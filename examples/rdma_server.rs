//! cargo run --example rdma_server --features=disk-db
//!

use async_trait::async_trait;
use sealfs::rpc::{rdma::server::Server, server::Handler};
use std::sync::Arc;
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
        _path: Vec<u8>,
        _data: Vec<u8>,
        _metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        // println!("metadata: {:?}", metadata);
        // println!("data: {:?}", data);
        match operation_type {
            0 => Ok((0, 0, 4, 4, vec![1, 2, 3, 4], vec![5, 6, 7, 8])),
            _ => {
                todo!()
            }
        }
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let server = Server::new("127.0.0.1:7777".to_string(), Arc::new(HelloHandler::new())).await;
    server.run().await?;
    Ok(())
}
