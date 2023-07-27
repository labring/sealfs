// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::{
    callback::CallbackPool,
    connection::ClientConnection,
    protocol::{CONNECTION_RETRY_TIMES, SEND_RETRY_TIMES},
};
use async_trait::async_trait;
use dashmap::DashMap;
use log::{error, info, warn};
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[async_trait]
pub trait StreamCreator<
    R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
>
{
    async fn create_stream(server_address: &str) -> Result<(R, W), String>;
}

pub struct TcpStreamCreator;

#[async_trait]
impl StreamCreator<tokio::net::tcp::OwnedReadHalf, tokio::net::tcp::OwnedWriteHalf>
    for TcpStreamCreator
{
    async fn create_stream(
        server_address: &str,
    ) -> Result<
        (
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
        ),
        String,
    > {
        let stream = match tokio::net::TcpStream::connect(server_address).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(format!("connect to {} error: {}", server_address, e));
            }
        };
        Ok(stream.into_split())
    }
}

pub struct UnixStreamCreator;

#[async_trait]
impl StreamCreator<tokio::net::unix::OwnedReadHalf, tokio::net::unix::OwnedWriteHalf>
    for UnixStreamCreator
{
    async fn create_stream(
        server_address: &str,
    ) -> Result<
        (
            tokio::net::unix::OwnedReadHalf,
            tokio::net::unix::OwnedWriteHalf,
        ),
        String,
    > {
        let stream = match tokio::net::UnixStream::connect(server_address).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(format!("connect to {} error: {}", server_address, e));
            }
        };
        Ok(stream.into_split())
    }
}

pub struct RpcClient<
    R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    S: StreamCreator<R, W>,
> {
    connections: DashMap<String, Arc<ClientConnection<W, R>>>,
    pool: Arc<CallbackPool>,
    stream_creator: PhantomData<S>,
}

impl<
        R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        S: StreamCreator<R, W>,
    > Default for RpcClient<R, W, S>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<
        R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        S: StreamCreator<R, W>,
    > RpcClient<R, W, S>
{
    pub fn new() -> Self {
        let mut pool = CallbackPool::new();
        pool.init();
        let pool = Arc::new(pool);
        Self {
            connections: DashMap::new(),
            pool,
            stream_creator: PhantomData,
        }
    }

    pub fn close(&self) {
        self.pool.free();
    }

    pub async fn add_connection(&self, server_address: &str) -> Result<(), String> {
        for _ in 0..CONNECTION_RETRY_TIMES {
            match S::create_stream(server_address).await {
                Ok((read_stream, write_stream)) => {
                    if self.connections.contains_key(server_address) {
                        warn!("connection already exists: {}", server_address);
                        return Ok(());
                    }
                    let connection = Arc::new(ClientConnection::new(server_address, write_stream));
                    tokio::spawn(parse_response(
                        read_stream,
                        connection.clone(),
                        self.pool.clone(),
                    ));
                    self.connections
                        .insert(server_address.to_string(), connection);
                    info!("add connection to {} success", server_address);
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "connect to {} failed: {}, wait for a while",
                        server_address, e
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Err(format!(
            "connect to {} error: connection retry times exceed",
            server_address
        ))
    }

    async fn reconnect(&self, server_address: &str) -> Result<(), String> {
        match self.connections.get(server_address) {
            Some(connection) => {
                if connection.value().reconnect() {
                    match S::create_stream(server_address).await {
                        Ok((read_stream, write_stream)) => {
                            tokio::spawn(parse_response(
                                read_stream,
                                connection.clone(),
                                self.pool.clone(),
                            ));
                            connection.value().reset_connection(write_stream).await;
                            info!("reconnect to {} success", server_address);
                            return Ok(());
                        }
                        Err(e) => {
                            warn!(
                                "reconnect to {} failed: {}, wait for a while",
                                server_address, e
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            connection.value().reconnect_failed();
                        }
                    }
                }
                Ok(())
            }
            None => {
                panic!("connection not exists: {}", server_address);
            }
        }
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.connections.remove(server_address);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn call_remote(
        &self,
        server_address: &str,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data_length: &mut usize,
        recv_data_length: &mut usize,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
        timeout: Duration,
    ) -> Result<(), String> {
        for _ in 0..SEND_RETRY_TIMES {
            let connection = match self.connections.get(server_address) {
                Some(connection) => connection,
                None => {
                    error!("connection not exists: {}", server_address);
                    return Err(format!("connection not exists: {}", server_address));
                }
            };
            let (batch, id) = self
                .pool
                .register_callback(recv_meta_data, recv_data)
                .await?; // TODO: unregister callback when error

            if let Err(e) = connection
                .send_request(
                    batch,
                    id,
                    operation_type,
                    req_flags,
                    path,
                    send_meta_data,
                    send_data,
                )
                .await
            {
                error!("send request to {} failed: {}", server_address, e);
                if connection.disconnect() {
                    warn!("connection to {} disconnected", server_address);
                    match self.reconnect(server_address).await {
                        Ok(_) => {
                            warn!("reconnect to {} success", server_address);
                            continue;
                        }
                        Err(e) => {
                            error!("reconnect to {} failed: {}", server_address, e);
                            return Err(format!("reconnect to {} failed: {}", server_address, e));
                        }
                    }
                }
                continue;
            }
            match self.pool.wait_for_callback(id, timeout).await {
                Ok((s, f, meta_data_length, data_length)) => {
                    *status = s;
                    *rsp_flags = f;
                    *recv_meta_data_length = meta_data_length;
                    *recv_data_length = data_length;
                    return Ok(());
                }
                Err(e) => {
                    error!("wait for callback failed: {}", e);
                    continue;
                }
            }
        }
        Err(format!(
            "send request to {} error: send retry times exceed",
            server_address
        ))
    }
}

// parse_response
// try to get response from sequence of connections and write to callbacks
pub async fn parse_response<W: AsyncWriteExt + Unpin, R: AsyncReadExt + Unpin>(
    mut read_stream: R,
    connection: Arc<ClientConnection<W, R>>,
    pool: Arc<CallbackPool>,
) {
    loop {
        if !connection.is_connected() {
            break;
        }
        let header = match connection.receive_response_header(&mut read_stream).await {
            Ok(header) => header,
            Err(e) => {
                if e == "early eof" || e == "Connection reset by peer (os error 104)" {
                    warn!("{:?} disconnected: {}", connection.server_address, e);
                    break;
                }
                panic!(
                    "parse_response header from {:?} error: {}",
                    connection.server_address, e
                );
            }
        };
        let batch = header.batch;
        let id = header.id;
        let total_length = header.total_length;

        let result = {
            match pool.lock_if_not_timeout(batch, id) {
                Ok(_) => Ok(()),
                Err(_) => Err("lock timeout"),
            }
        };
        match result {
            Ok(_) => {}
            Err(e) => {
                error!("parse_response lock timeout: {}", e);
                let result = connection
                    .clean_response(&mut read_stream, total_length)
                    .await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        error!("parse_response clean_response error: {}", e);
                        break;
                    }
                }
                continue;
            }
        }

        if let Err(e) = connection
            .receive_response(
                &mut read_stream,
                pool.get_meta_data_ref(id, header.meta_data_length as usize),
                pool.get_data_ref(id, header.data_length as usize),
            )
            .await
        {
            error!("Error receiving response: {}", e);
            break;
        };
        if let Err(e) = pool
            .response(
                id,
                header.status,
                header.flags,
                header.meta_data_length as usize,
                header.data_length as usize,
            )
            .await
        {
            error!("Error writing response back: {}", e);
            break;
        };
    }
}
