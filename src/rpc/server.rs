// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::net::{tcp::OwnedReadHalf, TcpListener};

use super::{connection::ServerConnection, protocol::RequestHeader};

#[async_trait]
pub trait Handler {
    async fn dispatch(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)>;
}

pub async fn handle<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    connection: Arc<ServerConnection>,
    header: RequestHeader,
    path: Vec<u8>,
    data: Vec<u8>,
    metadata: Vec<u8>,
) {
    debug!("handle, id: {}", header.id);
    let response = handler
        .dispatch(
            connection.id,
            header.r#type,
            header.flags,
            path,
            data,
            metadata,
        )
        .await;
    debug!("handle, response: {:?}", response);
    match response {
        Ok(response) => {
            let result = connection
                .send_response(
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
                    debug!("handle, send response success");
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

pub async fn test() {
    info!("test");
}

// receive(): handle the connection
// 1. read the request header
// 2. read the request data
// 3. spawn a handle thread to handle the request
// 4. loop to 1
pub async fn receive<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    connection: Arc<ServerConnection>,
    mut read_stream: OwnedReadHalf,
) {
    loop {
        {
            let id = connection.name_id();
            debug!("{:?} parse_request, start", id);
            let header = match connection.receive_request_header(&mut read_stream).await {
                Ok(header) => header,
                Err(e) => {
                    if e.to_string() == "early eof" {
                        warn!("connection {:?} is closed abnormally.", id);
                    } else {
                        error!("{:?} parse_request, header error: {}", id, e);
                    }
                    break;
                }
            };
            debug!("{:?} parse_request, header: {}", id, header.id);
            let data_result = connection.receive_request(&mut read_stream, &header).await;
            let (path, data, metadata) = match data_result {
                Ok(data) => data,
                Err(e) => {
                    error!("{:?} parse_request, data error: {}", id, e);
                    break;
                }
            };
            debug!("{:?} parse_request, data: {}", id, header.id);
            let handler = handler.clone();
            let connection = connection.clone();
            tokio::spawn(handle(handler, connection, header, path, data, metadata));
        }
    }
}

pub struct Server<H: Handler + std::marker::Sync + std::marker::Send + 'static> {
    // listener: TcpListener,
    bind_address: String,
    handler: Arc<H>,
}

impl<H: Handler + std::marker::Sync + std::marker::Send> Server<H> {
    pub fn new(handler: Arc<H>, bind_address: &str) -> Self {
        Self {
            handler,
            bind_address: String::from(bind_address),
        }
    }

    // run(): listen on the port and accept the connection
    // 1. accept the connection
    // 2. spawn a receive thread to handle the connection
    // 3. loop to 1
    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Listening on {:?}", self.bind_address);
        let listener = TcpListener::bind(&self.bind_address).await?;
        let mut id = 1u32;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let (read_stream, write_stream) = stream.into_split();
                    info!("Connection {id} accepted");
                    let handler = Arc::clone(&self.handler);
                    let name_id = format!("{},{}", self.bind_address, id);
                    let connection = Arc::new(ServerConnection::new(write_stream, name_id, id));
                    tokio::spawn(async move {
                        receive(handler, connection, read_stream).await;
                    });
                    id += 1;
                }
                Err(e) => {
                    panic!("Failed to create tcp stream, error is {}", e)
                }
            }
        }
    }
}
