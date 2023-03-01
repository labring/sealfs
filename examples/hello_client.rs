//! hello_client and hello_server demos show how rpc process the message sent by client
//! and the usage of 'call_remote' and 'dispatch' APIs.
//!
//! After starting server:
//!
//!     cargo run --example hello_server
//!
//! You can try this example by running:
//!
//!     cargo run --example hello_client

use log::debug;
use sealfs::rpc::client::Client;
use std::sync::Arc;

#[tokio::main]
pub async fn main() {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Debug);
    builder.init();

    let client = Arc::new(Client::new());
    let server_address = "127.0.0.1:50051";
    client.add_connection(server_address).await;
    for i in 0..3 {
        let new_client = client.clone();
        tokio::spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![0u8; 1024];
            debug!("call_remote, start");
            let result = new_client
                .call_remote(
                    server_address,
                    0,
                    0,
                    "",
                    &[],
                    &[],
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
                        let data = String::from_utf8(recv_data).unwrap();
                        println!("result: {}, data: {}", i, data);
                    } else {
                        println!("Error: {}", status);
                    }
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        });
    }
    std::thread::sleep(std::time::Duration::from_secs(60));
    client.close();
    println!("Done")
}
