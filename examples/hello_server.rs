//! hello_client and hello_server demos show how rpc process the message sent by client
//! and the usage of 'call_remote' and 'dispatch' APIs.
//!
//! You can try this example by running:
//!
//!     cargo run --example hello_server --features=disk-db
//!
//! And then start client in another terminal by running:
//!
//!     cargo run --example hello_client --features=disk-db

#![allow(unused)]
use async_trait::async_trait;
use log::debug;
use sealfs::rpc::server::{Handler, Server};
use std::sync::Arc;
use tokio::sync::Mutex;
pub struct HelloHandler {}

impl HelloHandler {
    pub fn new() -> Self {
        Self {}
    }
}

// lazy_static::lazy_static! {
//     static ref HELLO_COUNT: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
// }

#[async_trait]
impl Handler for HelloHandler {
    async fn dispatch(
        &self,
        _conn_id: u32,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        _metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        // debug!("dispatch, operation_type: {}", operation_type);
        // debug!("dispatch, path: {:?}", path);
        // debug!("dispatch, data: {:?}", data);
        match operation_type {
            0 => {
                // let mut count = HELLO_COUNT.lock().await;
                // let buf = format!("Hello, {}!", count).into_bytes();
                // *count += 1;
                Ok((0, 0, 4, 4, vec![1, 2, 3, 4], vec![5, 6, 7, 8]))
            }
            _ => {
                todo!()
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Info);
    builder.init();
    let server = Server::new(Arc::new(HelloHandler::new()), "127.0.0.1:50051");
    server.run().await?;
    Ok(())
}
