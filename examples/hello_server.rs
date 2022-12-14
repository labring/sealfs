use sealfs::rpc::server::{Handler, Server};
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use log::debug;
pub struct HelloHandler {}

impl HelloHandler {
    pub fn new() -> Self {
        Self {}
    }
}

lazy_static::lazy_static! {
    static ref HELLO_COUNT: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
}

#[async_trait]
impl Handler for HelloHandler {
    async fn dispatch(
        &self,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        _metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)> {
        debug!("dispatch, operation_type: {}", operation_type);
        debug!("dispatch, path: {:?}", path);
        debug!("dispatch, data: {:?}", data);
        match operation_type {
            0 => {
                let mut count = HELLO_COUNT.lock().await;
                let buf = format!("Hello, {}!", count).into_bytes();
                *count += 1;
                Ok((0, 0, vec![], buf))
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
        .filter(None, log::LevelFilter::Debug);
    builder.init();

    let server = Server::new(Arc::new(HelloHandler::new()), "127.0.0.1:50051");
    server.run().await?;
    Ok(())
}
