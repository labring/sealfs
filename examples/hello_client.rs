//! hello_client and hello_server demos show how rpc process the message sent by client
//! and the usage of 'call_remote' and 'dispatch' APIs.
//!
//! After starting server:
//!
//!     cargo run --example hello_server --features=disk-db
//!
//! You can try this example by running:
//!
//!     cargo run --example hello_client --features=disk-db

use log::debug;
use sealfs::rpc::client::Client;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
pub async fn main() {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Info);
    builder.init();
    let total = 10000;
    let elapsed = cli(total).await;
    println!("elapsed: {:?}", elapsed);
}

pub async fn cli(total: u32) -> Duration {
    let client = Arc::new(Client::new());
    let server_address = "127.0.0.1:50051";
    client.add_connection(server_address).await;
    // sleep for 1 second to wait for server to start
    // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let mut handles = vec![];
    let start = tokio::time::Instant::now();
    for _ in 0..total {
        let new_client = client.clone();
        handles.push(tokio::spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![0u8; 4];
            let mut recv_data = vec![0u8; 4];
            debug!("call_remote, start");
            let result = new_client
                .call_remote(
                    server_address,
                    0,
                    0,
                    "",
                    &[],
                    &[0u8; 10],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut recv_data,
                )
                .await;
            debug!("call_remote, result: {:?}", result);
            match result {
                Ok(_) => {
                    if status == 0 {
                        // // print recv_metadata and recv_data
                        // println!(
                        //     "result: {}, recv_meta_data: {:?}, recv_data: {:?}",
                        //     i, recv_meta_data, recv_data
                        // );
                    } else {
                        println!("Error: {}", status);
                    }
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }));
    }
    for handle in handles {
        if let Err(e) = handle.await {
            println!("Error: {}", e);
        }
    }
    let elapsed = start.elapsed();
    client.close();
    elapsed
}
