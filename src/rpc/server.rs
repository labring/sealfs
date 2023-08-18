// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UnixListener},
};

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

pub async fn handle<
    H: Handler + std::marker::Sync + std::marker::Send + 'static,
    W: AsyncWriteExt + Unpin,
    R: AsyncReadExt + Unpin,
>(
    handler: Arc<H>,
    connection: Arc<ServerConnection<W, R>>,
    header: RequestHeader,
    path: Vec<u8>,
    data: Vec<u8>,
    metadata: Vec<u8>,
) {
    let response = handler
        .dispatch(
            connection.id,
            header.r#type,
            header.flags,
            path.clone(),
            data,
            metadata,
        )
        .await;
    match response {
        Ok(response) => {
            if let Err(e) = connection
                .send_response(
                    header.batch,
                    header.id,
                    response.0,
                    response.1,
                    &response.4[0..response.2],
                    &response.5[0..response.3],
                )
                .await
            {
                error!("handle connection: {} , send response error: {}, batch: {}, id: {}, operation_type: {}, flags: {}, path: {:?}", connection.id, e, header.batch, header.id, header.r#type, header.flags, std::str::from_utf8(&path));
                let _ = connection.close().await;
            }
        }
        Err(e) => {
            error!(
                "handle connection: {} , dispatch error: {}",
                connection.id, e
            );
        }
    }
}

pub async fn receive<
    H: Handler + std::marker::Sync + std::marker::Send + 'static,
    W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
>(
    handler: Arc<H>,
    connection: Arc<ServerConnection<W, R>>,
    mut read_stream: R,
) {
    loop {
        {
            let id = connection.name_id();
            let header = match connection.receive_request_header(&mut read_stream).await {
                Ok(header) => header,
                Err(e) => {
                    if e == "early eof" || e == "Connection reset by peer (os error 104)" {
                        warn!("{:?} receive, connection closed", id);
                        break;
                    }
                    panic!("{:?} parse_request, header error: {}", id, e);
                }
            };
            let data_result = connection.receive_request(&mut read_stream, &header).await;
            let (path, data, metadata) = match data_result {
                Ok(data) => data,
                Err(e) => {
                    panic!("{:?} parse_request, data error: {}", id, e);
                }
            };
            let handler = handler.clone();
            let connection = connection.clone();
            tokio::spawn(handle(handler, connection, header, path, data, metadata));
        }
    }
}

pub struct RpcServer<H: Handler + std::marker::Sync + std::marker::Send + 'static> {
    // listener: TcpListener,
    bind_address: String,
    handler: Arc<H>,
}

impl<H: Handler + std::marker::Sync + std::marker::Send> RpcServer<H> {
    pub fn new(handler: Arc<H>, bind_address: &str) -> Self {
        Self {
            handler,
            bind_address: String::from(bind_address),
        }
    }

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

    pub async fn run_unix_stream(&self) -> anyhow::Result<()> {
        info!("Listening on {:?}", self.bind_address);
        let listener = match UnixListener::bind(&self.bind_address) {
            Ok(listener) => listener,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to create unix stream at {:?}, error is {}",
                    self.bind_address,
                    e
                ));
            }
        };
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
