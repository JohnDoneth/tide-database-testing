#![feature(async_await)]
#![deny(warnings)]

use serde::{Deserialize, Serialize};
use tide::{error::ResultExt, response, App, Context, EndpointResult};

use tokio::net::TcpStream;
use tokio::sync::lock::{Lock, LockGuard};

use tokio_postgres::tls::{NoTls, NoTlsStream};
use tokio_postgres::{Client, Config, Connection, Error};

use std::time::Instant;
use std::pin::Pin;
use std::sync::Arc;

use futures::{Future, FutureExt, Poll, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Message {
    contents: String,
}

async fn echo_json(mut cx: Context<State>) -> EndpointResult {
    let msg: Message = cx.body_json().await.client_err()?;
    println!("JSON: {:?}", msg);
    Ok(response::json(msg))
}

async fn get_json(cx: Context<State>) -> EndpointResult {
    let start = Instant::now();
    let cx = cx.state();

    let mut result = {
        let mut client = cx.pool.get().await;

        let select = client.prepare("SELECT now()::TEXT").await.unwrap();

        let res = client.query(&select, &[]).await.unwrap();

        drop(client);
        println!("dropped lock");

        res
    };
    println!("elapsed waiting for lock: {:?}", start.elapsed());

    let start = Instant::now();
    let res = if let Some(Ok(next)) = result.next().await {
        next.get::<_, &str>(0).to_string()
    } else {
        "failed to query".to_string()
    };
    println!("elapsed waiting for query: {:?}", start.elapsed());

    let message = Message {
        contents: res,
    };

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
        for connection in self.pool.connections.iter() {
            if let Poll::Ready(val) = connection.clone().poll_lock(cx) {
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

        for _ in 0..10usize {
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

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    println!("starting");

    rt.block_on(async move {
        let mut client = connect("user=postgres").await;
        let _select = client.prepare("SELECT now()::TEXT").await.unwrap();

        let pool = Pool::new().await;

        let mut app = App::with_state(State { pool });

        app.at("/json").post(echo_json).get(get_json);

        app.run("127.0.0.1:8000").unwrap();
    });
}
