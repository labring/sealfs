use tokio::{net::{TcpSocket, TcpListener, TcpStream}, io::AsyncReadExt};

 pub struct Server {
    pub listener: TcpListener,
 }

 impl Server {
    pub fn new(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let socket_addr = addr.parse().unwrap();
        let socket = TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(socket_addr)?;

        let listener = socket.listen(1024)?;

        Ok(Server { listener })
    }

    pub async fn run(&self, mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                let _n = socket.read(&mut buf).await.unwrap();
                println!("{}", String::from_utf8(buf.to_vec()).unwrap());
            }
        });
        Ok(())
    }
}
