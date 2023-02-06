#![allow(unused)]

use hello_world::greeter_client::GreeterClient;
use hello_world::HelloRequest;

use tonic::{transport::Server, Request, Response, Status};

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

pub fn gcli(total: u32) {
    let mut rt = tokio::runtime::Runtime::new().unwrap();

    let client = rt
        .block_on(GreeterClient::connect("http://[::1]:50051"))
        .unwrap();
    let mut handles = Vec::with_capacity(50);

    let mut data = [0u8; 50];
    let data = String::from_utf8(data.to_vec()).unwrap();
    for i in 0..total {
        let out = data.clone();
        let client_clone = client.clone();
        handles.push(rt.spawn(async move {
            let request = tonic::Request::new(HelloRequest {
                id: i,
                r#type: 0,
                flags: 0,
                filename: "".to_string(),
                meta_data: "".to_string(),
                data: out,
            });
            let response = client_clone.to_owned().say_hello(request).await.unwrap();
            // debug!("call_remote, result: {:?}", result);
            let reply = response.into_inner();
            if reply.id != i || reply.status != 1 {
                println!("Error reply")
            }
        }));
    }
    for handle in handles {
        rt.block_on(handle).unwrap();
    }
}
