use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:50051").unwrap();
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        println!("connection established!");
        let mut count = 0;
        loop {
            count += 1;
            let mut buf = vec![0u8; 50028];
            stream.read_exact(&mut buf).unwrap();
            let data_length = 0;
            let meta_data_length = 0;
            let total_length = data_length + meta_data_length;
            let mut response = Vec::with_capacity(24 + total_length);
            response.extend_from_slice(&buf[0..4]);
            response.extend_from_slice(&0u32.to_le_bytes());
            response.extend_from_slice(&0u32.to_le_bytes());
            response.extend_from_slice(&(total_length as u32).to_le_bytes());
            response.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
            response.extend_from_slice(&(data_length as u32).to_le_bytes());
            response.extend_from_slice(&[]);
            response.extend_from_slice(&[]);
            stream.write_all(&response);
        }
    }
}
