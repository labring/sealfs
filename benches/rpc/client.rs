#![allow(unused)]

use sealfs::rpc::client::Client;
use std::sync::Arc;

const SIZE_1024: usize = 1024;
const SIZE_1024_4: usize = 1024 * 4;
const SIZE_1024_16: usize = 1024 * 16;
const SIZE_1024_64: usize = 1024 * 64;
const SIZE_1024_256: usize = 1024 * 256;
const SIZE_1024_1024: usize = 1024 * 1024;
const SIZE_1024_1024_4: usize = 1024 * 1024 * 4;
const SIZE_1024_1024_16: usize = 1024 * 1024 * 16;

pub fn cli(total: u32) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(run_cli_without_data(total));
}

pub fn cli_size(total: u32, size: usize) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(run_cli_with_data_size(total, size));
}

pub async fn run_cli_without_data(total: u32) {
    let rt = tokio::runtime::Handle::current();
    let mut handles = Vec::with_capacity(total as usize);

    let server_address = "127.0.0.1:50052";
    let client = Arc::new(Client::new());
    client.add_connection(server_address).await;

    let mut data = [0u8; 50];
    for i in 0..total {
        let new_client = client.clone();
        handles.push(rt.spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![];
            // debug!("call_remote, start");
            let result = new_client
                .call_remote(
                    server_address,
                    0,
                    i,
                    "",
                    &[],
                    &data,
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut recv_data,
                )
                .await;
            // debug!("call_remote, result: {:?}", result);
            match result {
                Ok(_) => {
                    if status == 0 {
                        // let data = String::from_utf8(recv_data).unwrap();
                        // println!("result: {}, data: {}", i, data);
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
        handle.await;
    }
    client.close();
}
async fn run_cli_with_data_size(total: u32, size: usize) {
    let rt = tokio::runtime::Handle::current();
    let mut handles = Vec::with_capacity(total as usize);

    let server_address = "127.0.0.1:50052";
    let client = Arc::new(Client::new());
    client.add_connection(server_address).await;
    let data: &[u8] = match size {
        SIZE_1024 => &[0u8; SIZE_1024],
        SIZE_1024_4 => &[0u8; SIZE_1024_4],
        SIZE_1024_16 => &[0u8; SIZE_1024_16],
        SIZE_1024_64 => &[0u8; SIZE_1024_64],
        SIZE_1024_256 => &[0u8; SIZE_1024_256],
        SIZE_1024_1024 => &[0u8; SIZE_1024_1024],
        SIZE_1024_1024_4 => &[0u8; SIZE_1024_1024_4],
        SIZE_1024_1024_16 => &[0u8; SIZE_1024_1024_16],
        _ => &[0u8; 1024],
    };
    for i in 0..total {
        let new_client = client.clone();
        handles.push(rt.spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![];
            // debug!("call_remote, start");
            let result = new_client
                .call_remote(
                    server_address,
                    0,
                    i,
                    "",
                    &[],
                    &data,
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut recv_data,
                )
                .await;
            // debug!("call_remote, result: {:?}", result);
            match result {
                Ok(_) => {
                    if status == 0 {
                        // let data = String::from_utf8(recv_data).unwrap();
                        // println!("result: {}, data: {}", i, data);
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
        handle.await;
    }
    client.close();
}
