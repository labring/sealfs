use std::{io::IoSlice, sync::Arc};

use ibv::connection::conn::{Conn, MyReceiver};
use log::debug;
use tokio::sync::mpsc::channel;

use ibv::connection::conn::run;

use crate::rpc::{
    protocol::{RequestHeader, REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE},
    server::Handler,
};
pub struct Server<H: Handler + std::marker::Sync + std::marker::Send + 'static> {
    pub addr: String,
    incoming: MyReceiver<Conn>,
    handler: Arc<H>,
}

impl<H: Handler + std::marker::Sync + std::marker::Send> Server<H>
where
    H: Handler + std::marker::Sync + std::marker::Send + 'static,
{
    pub async fn new(addr: String, handler: Arc<H>) -> Self {
        let (tx, rx) = channel(1000);
        let address = addr.clone();
        tokio::spawn(run(address, tx));
        let rx = MyReceiver::new(rx);
        Server {
            addr,
            incoming: rx,
            handler,
        }
    }

    pub async fn accept(&self) -> Conn {
        self.incoming.recv().await
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        loop {
            let conn = Arc::new(self.accept().await);
            println!("accept a connection");
            let handler = Arc::clone(&self.handler);
            tokio::spawn(receive(handler, conn));
        }
    }
}

pub async fn receive<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    conn: Arc<Conn>,
) {
    loop {
        let request: &[u8] = conn.recv_msg().await.unwrap();
        debug!("receive a request: {:?}", request);
        let (header, path, meta_data, data) = parse_request(request);
        conn.release(request).await;

        let handler = handler.clone();
        tokio::spawn(handle(handler, conn.clone(), header, path, meta_data, data));
    }
}

// parse_request_header(): parse the request header
// 1. parse the header from the request
// 2. return the header
pub fn parse_request_header(request: &[u8]) -> RequestHeader {
    let header = &request[0..REQUEST_HEADER_SIZE];
    let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let operation_type = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let flags: u32 = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let file_path_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
    let meta_data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
    let data_length = u32::from_le_bytes(header[28..32].try_into().unwrap());
    RequestHeader {
        batch,
        id,
        r#type: operation_type,
        flags,
        total_length,
        file_path_length,
        meta_data_length,
        data_length,
    }
}

pub fn parse_request(request: &[u8]) -> (RequestHeader, Vec<u8>, Vec<u8>, Vec<u8>) {
    let header = parse_request_header(request);
    debug!("parse_request, header: {:?}", header);
    let path =
        &request[REQUEST_HEADER_SIZE..REQUEST_HEADER_SIZE + header.file_path_length as usize];
    let metadata = &request[REQUEST_HEADER_SIZE + header.file_path_length as usize
        ..REQUEST_HEADER_SIZE
            + header.file_path_length as usize
            + header.meta_data_length as usize];
    let data = &request[REQUEST_HEADER_SIZE
        + header.file_path_length as usize
        + header.meta_data_length as usize
        ..REQUEST_HEADER_SIZE
            + header.file_path_length as usize
            + header.meta_data_length as usize
            + header.data_length as usize];
    (header, path.to_vec(), metadata.to_vec(), data.to_vec())
}

// handle(): handle the request
// 1. call the handler to handle the request
// 2. send the response back to the client
async fn handle<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    conn: Arc<Conn>,
    header: RequestHeader,
    path: Vec<u8>,
    metadata: Vec<u8>,
    data: Vec<u8>,
) {
    debug!("handle, id: {}", header.id);
    let response = handler
        .dispatch(0, header.r#type, header.flags, path, data, metadata)
        .await;
    debug!("handle, response: {:?}", response);
    match response {
        Ok(response) => {
            let result = send_response(
                conn.clone(),
                header.batch,
                header.id,
                response.0,
                response.1,
                &response.4[0..response.2],
                &response.5[0..response.3],
            )
            .await;
            match result {
                Ok(_) => {
                    // debug!("handle, send response success");
                }
                Err(e) => {
                    debug!("handle, send response error: {}", e);
                }
            }
        }
        Err(e) => {
            debug!("handle, dispatch error: {}", e);
        }
    }
}

pub async fn send_response(
    conn: Arc<Conn>,
    batch: u32,
    id: u32,
    status: i32,
    flags: u32,
    meta_data: &[u8],
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let data_length = data.len();
    let meta_data_length = meta_data.len();
    let total_length = data_length + meta_data_length;
    let mut response = Vec::with_capacity(RESPONSE_HEADER_SIZE + total_length);
    response.extend_from_slice(&batch.to_le_bytes());
    response.extend_from_slice(&id.to_le_bytes());
    response.extend_from_slice(&status.to_le_bytes());
    response.extend_from_slice(&flags.to_le_bytes());
    response.extend_from_slice(&(total_length as u32).to_le_bytes());
    response.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
    response.extend_from_slice(&(data_length as u32).to_le_bytes());
    let response = &[
        IoSlice::new(&response),
        IoSlice::new(meta_data),
        IoSlice::new(data),
    ];
    conn.send_msg(response).await?;
    Ok(())
}
