#![feature(async_await)]
#![deny(warnings)]
#![feature(duration_float)]

use tokio::net::TcpStream;
use tokio::sync::lock::{Lock, LockGuard};

use tokio_postgres::tls::{NoTls, NoTlsStream};
use tokio_postgres::{Statement, Client, Config, Connection, Error};

use std::pin::Pin;
use std::collections::HashMap;

use futures::{Future, FutureExt, Poll, StreamExt};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

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

struct MyConnection {
    client: Client,
    cache: StatementCache,
}

#[derive(Clone)]
struct SharedPool {
    connections: Vec<Lock<MyConnection>>,
}

#[derive(Clone)]
struct GetConnectionFuture(SharedPool);

impl Future for GetConnectionFuture {
    type Output = LockGuard<MyConnection>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        for connection in self.0.connections.iter_mut() {
            if let Poll::Ready(val) = connection.poll_lock(cx) {
                return Poll::Ready(val);
            }
        }

        Poll::Pending
    }
}

#[derive(Clone)]
struct Pool(SharedPool);

impl Pool {
    async fn new(num: usize) -> Self {
        let mut connections = Vec::new();

        for _ in 0..num {
            let connection = connect("user=postgres").await;

            connections.push(Lock::new(MyConnection {
                client: connection,
                cache: StatementCache::default(),
            }));
        }

        println!("opened {} database connections", num);

        Self(SharedPool { connections })
    }

    fn get(&self) -> GetConnectionFuture {
        GetConnectionFuture(self.0.clone())
    }
}

#[derive(Default)]
struct StatementCache {
    cache: HashMap<String, Statement>,
}

impl StatementCache {
    fn insert(&mut self, query: &str, statement: Statement) {
        self.cache.insert(query.to_string(), statement);
    }

    fn prepare(&mut self, query: &str) -> Option<Statement> {
        if self.cache.contains_key(query) {
            Some(self.cache[query].clone())
        } else {
            None
        }
    }
}

async fn hello(
    _: Request<Body>,
    connection: GetConnectionFuture,
) -> Result<Response<Body>, hyper::Error> {
    let mut result = {
        let mut connection = connection.await;

        let query = "SELECT now()::TEXT";

        let statement = match connection.cache.prepare(query) {
            Some(statement) => statement,
            None => {
                let statement = connection.client.prepare(query).await.unwrap();

                connection.cache.insert(query, statement.clone());

                statement
            }
        };

        let res = connection.client.query(&statement, &[]).await.unwrap();

        res
    };

    let res = if let Some(Ok(next)) = result.next().await {
        next.get::<_, &str>(0).to_string()
    } else {
        "failed to query".to_string()
    };
    Ok(Response::new(Body::from(res)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pretty_env_logger::init();

    let pool = Pool::new(8).await;

    let addr = "127.0.0.1:8000".parse().unwrap();

    let server = Server::bind(&addr).serve(make_service_fn(move |_| {
        let pool = pool.clone();

        // This is the `Service` that will handle the connection.
        // `service_fn` is a helper to convert a function that
        // returns a Response into a `Service`.
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                let connection = pool.get();

                hello(req, connection)
            }))
        }
    }));

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}
