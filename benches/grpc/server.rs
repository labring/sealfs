#![allow(unused)]
use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};

use tonic::{transport::Server, Request, Response, Status};

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let message = request.into_inner();
        // println!("Got a request: {} {}", message.id, &message.data[0..5]);

        let reply = hello_world::HelloReply {
            id: message.id,
            status: 1,
            flags: 0,
            meta_data: "".to_string(),
            data: "".to_string(),
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
pub async fn server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
