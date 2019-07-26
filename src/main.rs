#![feature(async_await)]
#![deny(warnings)]

use serde::{Deserialize, Serialize};
use tide::{response, App, Context, EndpointResult};

use tokio::net::TcpStream;
use tokio::sync::lock::{Lock, LockGuard};

use tokio_postgres::tls::{NoTls, NoTlsStream};
use tokio_postgres::{Client, Config, Connection, Error};

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::{Future, FutureExt, Poll, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Message {
    contents: String,
}

async fn get_json(cx: Context<State>) -> EndpointResult {
    let cx = cx.state();

    let mut result = {
        let start = Instant::now();
        let mut client = cx.pool.get().await;
        println!("time elapsed waiting for lock: {:?}", start.elapsed());

        let start = Instant::now();
        let select = client.prepare("SELECT now()::TEXT").await.unwrap();
        println!("time elapsed preparing query: {:?}", start.elapsed());

        let res = client.query(&select, &[]).await.unwrap();

        drop(client);
        println!("dropped lock");

        res
    };

    let start = Instant::now();
    let res = if let Some(Ok(next)) = result.next().await {
        next.get::<_, &str>(0).to_string()
    } else {
        "failed to query".to_string()
    };
    println!("elapsed waiting for query: {:?}", start.elapsed());

    let message = Message { contents: res };

    //println!("{:?}", message);

    Ok(response::json(message))
}

async fn connect_raw(s: &str) -> Result<(Client, Connection<TcpStream, NoTlsStream>), Error> {
    let socket = TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        .await
        .unwrap();
    let config = s.parse::<Config>().unwrap();
    config.connect_raw(socket, NoTls).await
}

async fn connect(s: &str) -> Client {
    let (client, connection) = connect_raw(s).await.unwrap();
    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);
    client
}

struct SharedPool {
    connections: Vec<Lock<Client>>,
}

struct GetConnectionFuture {
    pool: Arc<SharedPool>,
}

impl Future for GetConnectionFuture {
    type Output = LockGuard<Client>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        for mut connection in self.pool.connections.iter().cloned() {
            if let Poll::Ready(val) = connection.poll_lock(cx) {
                println!("ready");
                return Poll::Ready(val);
            }
        }

        Poll::Pending
    }
}

struct Pool(Arc<SharedPool>);

impl Pool {
    async fn new() -> Self {
        let mut connections = Vec::new();

        for _ in 0..4usize {
            connections.push(Lock::new(connect("user=postgres").await));
        }

        Self(Arc::new(SharedPool { connections }))
    }

    fn get(&self) -> GetConnectionFuture {
        GetConnectionFuture {
            pool: self.0.clone(),
        }
    }
}

struct State {
    pool: Pool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = Pool::new().await;

    let mut app = App::with_state(State { pool });

    app.at("/json").get(get_json);

    app.run("127.0.0.1:8000")?;

    Ok(())
}
