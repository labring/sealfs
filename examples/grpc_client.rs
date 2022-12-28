use std::io::Read;
use std::result;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;
use tonic::codegen::http::response;

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = GreeterClient::connect("http://[::1]:50051").await?;

    let count = Arc::new(Mutex::new(0));
    // println!("{:?}", std::env::current_exe());
    let mut file = std::fs::File::open("examples/w.txt").unwrap();
    let mut data = [0u8; 50];
    file.read_exact(&mut data).unwrap();
    let data = String::from_utf8(data.to_vec()).unwrap();
    let total = 5000;
    let start = Instant::now();
    for i in 0..total {
        let out = data.clone();
        let client_clone = client.clone();
        let cnt = count.clone();
        tokio::spawn(async move {
            let request = tonic::Request::new(HelloRequest {
                id: i,
                r#type: 0,
                flags: 0,
                filename: "".to_string(),
                meta_data: "".to_string(),
                data: out,
            });
            let response = client_clone.to_owned().say_hello(request).await;
            {
                let mut val = cnt.lock().unwrap();
                *val += 1;
                if *val == total {
                    println!("time: {}", start.elapsed().as_millis());
                }
            }
            // debug!("call_remote, result: {:?}", result);
            match response {
                Ok(_) => {
                    println!("success: {}", i);
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        });
    }
    std::thread::sleep(std::time::Duration::from_secs(600));
    Ok(())
}
