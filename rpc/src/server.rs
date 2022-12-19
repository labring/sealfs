// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, info};
use tokio::net::{tcp::OwnedReadHalf, TcpListener};

use crate::connection::ServerConnection;

#[async_trait]
pub trait Handler {
    async fn dispatch(
        &self,
        operation_type: u32,
        flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)>;
}

#[allow(clippy::too_many_arguments)]
pub async fn handle<H: Handler + std::marker::Sync + std::marker::Send + 'static>(
    handler: Arc<H>,
    connection: Arc<ServerConnection>,
    id: u32,
    operation_type: u32,
    flags: u32,
    path: Vec<u8>,
    data: Vec<u8>,
    metadata: Vec<u8>,
) {
    debug!("handle, id: {}", id);
    let response = handler
        .dispatch(operation_type, flags, path, data, metadata)
        .await;
    debug!("handle, response: {:?}", response);
    match response {
        Ok(response) => {
            let result = connection
                .send_response(id, response.0, response.1, &response.2, &response.3)
                .await;
            match result {
                Ok(_) => {
                    debug!("parse_response, send response success");
                }
                Err(e) => {
                    debug!("parse_response, send response error: {}", e);
                }
            }
        }
        Err(e) => {
            debug!("parse_response, dispatch error: {}", e);
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
            debug!("parse_response, start");
            let header = match connection.receive_request_header(&mut read_stream).await {
                Ok(header) => header,
                Err(e) => {
                    debug!("parse_response, header error: {}", e);
                    continue;
                }
            };
            debug!("parse_response, header: {:?}", header.id);
            let data_result = connection.receive_request(&mut read_stream, &header).await;
            let (path, data, metadata) = match data_result {
                Ok(data) => data,
                Err(e) => {
                    debug!("parse_response, data error: {}", e);
                    continue;
                }
            };
            debug!("parse_response, data: {:?}", header.id);
            let handler = handler.clone();
            let connection = connection.clone();
            tokio::spawn(handle(
                handler,
                connection,
                header.id,
                header.r#type,
                header.flags,
                path,
                data,
                metadata,
            ));
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await; // due to the bug of tokio
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
        info!("Listening");
        let listener = TcpListener::bind(&self.bind_address).await?;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let (read_stream, write_stream) = stream.into_split();
                    info!("Connection accepted");
                    let handler = Arc::clone(&self.handler);
                    let connection = Arc::new(ServerConnection::new(write_stream));
                    tokio::spawn(async move {
                        receive(handler, connection, read_stream).await;
                    });
                }
                Err(e) => {
                    panic!("Failed to create tcp stream, error is {}", e)
                }
            }
        }
    }
}
