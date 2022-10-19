use server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::new("127.0.0.1:8080")?;
    loop {
        let (socket, _) = server.listener.accept().await?;
        server.run(socket).await?;
    }
}