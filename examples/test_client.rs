use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Instant,
    vec,
};
struct Connect {
    stream: Arc<TcpStream>,
}
impl Connect {
    fn send_request(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        filename: &str,
        meta_data: &[u8],
        meta_data_length: usize,
        data: &[u8],
        data_length: usize,
    ) {
        let filename_length = filename.len();
        let total_length = filename_length + meta_data_length + data_length;
        let mut request = Vec::with_capacity(total_length + 28);
        request.extend_from_slice(&id.to_le_bytes());
        request.extend_from_slice(&operation_type.to_le_bytes());
        request.extend_from_slice(&flags.to_le_bytes());
        request.extend_from_slice(&(total_length as u32).to_le_bytes());
        request.extend_from_slice(&(filename_length as u32).to_le_bytes());
        request.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
        request.extend_from_slice(&(data_length as u32).to_le_bytes());
        request.extend_from_slice(filename.as_bytes());
        request.extend_from_slice(meta_data);
        request.extend_from_slice(&data);
        self.stream.as_ref().write(&request).unwrap();
    }
}

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:50051").unwrap();
    let count = Arc::new(Mutex::new(0));
    // println!("{:?}", std::env::current_exe());
    let connect = Arc::new(Connect {
        stream: Arc::new(stream),
    });
    let mut file = std::fs::File::open("examples/w.txt").unwrap();
    let mut data = [0u8; 50000];
    file.read_exact(&mut data).unwrap();

    let total = 200;
    let start = Instant::now();
    for i in 0..total {
        let con = connect.clone();
        let id = i as u32;
        let operation_type = 0u32;
        let flags = 0u32;
        let filename = "";
        let meta_data = &[];
        let filename_length = 0;
        let meta_data_length = 0;
        let data_length = 0;
        con.send_request(
            id,
            operation_type,
            flags,
            filename,
            meta_data,
            meta_data_length,
            &data,
            data_length,
        );
        println!("ok {}", i);
    }
    println!("time: {}", start.elapsed().as_millis());
    std::thread::sleep(std::time::Duration::from_secs(10));
}
