use std::sync::Arc;

use log::debug;
use rpc::client::Client;

pub fn main() {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Debug);
    builder.init();

    let server_address = "127.0.0.1:50051";
    let client = Arc::new(Client::new());
    client.add_connection(server_address);
    let p = client.clone();
    std::thread::spawn(move || p.parse_response());
    for i in 0..50 {
        let new_client = client.clone();
        std::thread::spawn(move || {
            let mut status = 0;
            let mut rsp_flags = 0;
            let mut recv_meta_data_length = 0;
            let mut recv_data_length = 0;
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![0u8; 1024];
            debug!("call_remote, start");
            let result = new_client.call_remote(
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
            );
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
    println!("Done")
}
