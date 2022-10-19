#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = client::connect("127.0.0.1:8080").await?;
    let result = client::write(&mut stream, b"hello world\n").await;
    println!("wrote to stream; success = {:?}", result.is_ok());
    Ok(())
}