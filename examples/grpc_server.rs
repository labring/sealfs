use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{HelloReply, HelloRequest};

pub mod hello_world {
    tonic::include_proto!("helloworld"); // 这里指定的字符串必须与proto的包名称一致
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>, // 接收以HelloRequest为类型的请求
    ) -> Result<Response<HelloReply>, Status> {
        // 返回以HelloReply为类型的示例作为响应
        let message = request.into_inner();
        // println!("Got a request: {} {}", message.id, &message.data[0..5]);

        let reply = hello_world::HelloReply {
            id: 0,
            status: 0,
            flags: 0,
            meta_data: "".to_string(),
            data: "".to_string(),
        };

        Ok(Response::new(reply)) // 发回格式化的问候语
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
