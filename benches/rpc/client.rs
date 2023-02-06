#![allow(unused)]

use sealfs::rpc::client::Client;
use std::sync::Arc;

pub fn cli(total: u32) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let mut handles = Vec::with_capacity(50);

    let server_address = "127.0.0.1:50052";
    let client = Arc::new(Client::new());
    client.add_connection(server_address);
    let p = client.clone();

    std::thread::spawn(move || p.parse_response());

    let mut data = [0u8; 50];
    for i in 0..total {
        let new_client = client.clone();
        handles.push(rt.spawn(async move {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![0u8; 1];
            // debug!("call_remote, start");
            let result = new_client.call_remote(
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
            );
            // debug!("call_remote, result: {:?}", result);
            match result {
                Ok(_) => {
                    if status == 0 {
                        let data = String::from_utf8(recv_data).unwrap();
                        //println!("result: {}, data: {}", i, data);
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
        rt.block_on(handle).unwrap();
    }
}
