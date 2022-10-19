use tokio::{net::TcpStream, io::AsyncWriteExt};


pub async fn connect(addr: &str) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr).await?;
    Ok(stream)
}

pub async fn write(stream: &mut TcpStream, buf: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    stream.write_all(buf).await?;
    Ok(())
}