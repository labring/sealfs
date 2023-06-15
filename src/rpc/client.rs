// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::{callback::CallbackPool, connection::ClientConnection};
use dashmap::DashMap;
use log::{error, info, warn};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct RpcClient<
    W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
> {
    connections: DashMap<String, Arc<ClientConnection<W, R>>>,
    pool: Arc<CallbackPool>,
}

impl<
        W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    > Default for RpcClient<W, R>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<
        W: AsyncWriteExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
        R: AsyncReadExt + Unpin + std::marker::Sync + std::marker::Send + 'static,
    > RpcClient<W, R>
{
    pub fn new() -> Self {
        let mut pool = CallbackPool::new();
        pool.init();
        let pool = Arc::new(pool);
        Self {
            connections: DashMap::new(),
            pool,
        }
    }

    pub fn close(&self) {
        self.pool.free();
    }

    async fn add_connection(&self, read_stream: R, write_stream: W, server_address: &str) -> bool {
        let connection = Arc::new(ClientConnection::new(
            server_address,
            Some(tokio::sync::Mutex::new(write_stream)),
        ));
        tokio::spawn(parse_response(
            read_stream,
            connection.clone(),
            self.pool.clone(),
        ));
        self.connections
            .insert(server_address.to_string(), connection);
        true
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
    ) -> Result<(), String> {
        let connection = match self.connections.get(server_address) {
            Some(connection) => connection,
            None => {
                error!("connection not found: {}", server_address);
                return Err("connection not found".into());
            }
        };
        let (batch, id) = self
            .pool
            .register_callback(recv_meta_data, recv_data)
            .await?;
        connection
            .send_request(
                batch,
                id,
                operation_type,
                req_flags,
                path,
                send_meta_data,
                send_data,
            )
            .await?;
        let (s, f, meta_data_length, data_length) = self.pool.wait_for_callback(id).await?;
        *status = s;
        *rsp_flags = f;
        *recv_meta_data_length = meta_data_length;
        *recv_data_length = data_length;
        Ok(())
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
        if !connection.is_connected().await {
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

pub async fn add_tcp_connection(
    address: &str,
    client: &RpcClient<tokio::net::tcp::OwnedWriteHalf, tokio::net::tcp::OwnedReadHalf>,
) {
    loop {
        match tokio::net::TcpStream::connect(address.to_owned()).await {
            Ok(stream) => {
                let (read_stream, write_stream) = stream.into_split();
                match client
                    .add_connection(read_stream, write_stream, address)
                    .await
                {
                    true => {
                        break;
                    }
                    false => {
                        warn!("add connection failed, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            Err(e) => {
                warn!("connect to manager failed, wait for a while, {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        info!("add connection to {} failed, retrying...", address);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn add_unix_connection(
    address: &str,
    client: &Arc<RpcClient<tokio::net::unix::OwnedWriteHalf, tokio::net::unix::OwnedReadHalf>>,
) {
    loop {
        match tokio::net::UnixStream::connect(address.to_owned()).await {
            Ok(stream) => {
                let (read_stream, write_stream) = stream.into_split();
                match client
                    .add_connection(read_stream, write_stream, address)
                    .await
                {
                    true => {
                        break;
                    }
                    false => {
                        warn!("add connection failed, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            Err(e) => {
                warn!("connect to manager failed, wait for a while, {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
        info!("add connection to {} failed, retrying...", address);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
