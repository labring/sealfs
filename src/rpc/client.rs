// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::{callback::CallbackPool, connection::ClientConnection};
use dashmap::DashMap;
use log::{debug, error, warn};
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;

pub struct Client {
    connections: DashMap<String, Arc<ClientConnection>>,
    pool: Arc<CallbackPool>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
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

    pub async fn add_connection(&self, server_address: &str) -> bool {
        let result = tokio::net::TcpStream::connect(server_address).await;
        let connection = match result {
            Ok(stream) => {
                debug!("connect {:?} success", server_address);
                let (read_stream, write_stream) = stream.into_split();
                let connection = Arc::new(ClientConnection::new(
                    server_address,
                    Some(tokio::sync::Mutex::new(write_stream)),
                ));
                tokio::spawn(parse_response(
                    read_stream,
                    connection.clone(),
                    self.pool.clone(),
                ));
                connection
            }
            Err(e) => {
                debug!("connect error: {}", e);
                return false;
            }
        };
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
    ) -> Result<(), Box<dyn std::error::Error>> {
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
        debug!(
            "call_remote on {:?}, batch {}, id: {}",
            server_address, batch, id
        );
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
        debug!(
            "call_remote success, id: {}, status: {}, flags: {}, meta_data_length: {}, data_length: {}",
            id, s, f, meta_data_length, data_length
        );
        *status = s;
        *rsp_flags = f;
        *recv_meta_data_length = meta_data_length;
        *recv_data_length = data_length;
        Ok(())
    }
}

// parse_response
// try to get response from sequence of connections and write to callbacks
pub async fn parse_response(
    mut read_stream: OwnedReadHalf,
    connection: Arc<ClientConnection>,
    pool: Arc<CallbackPool>,
) {
    loop {
        if !connection.is_connected().await {
            debug!(
                "parse_response: connection to {:?} closed",
                connection.server_address
            );
            break;
        }
        debug!("parse_response: {:?}", connection.server_address);
        let header = match connection.receive_response_header(&mut read_stream).await {
            Ok(header) => header,
            Err(e) => {
                if e.to_string() == "early eof" {
                    warn!(
                        "connection to {:?} is closed abnormally.",
                        connection.server_address
                    );
                } else {
                    error!(
                        "parse_response header from {:?} error: {}",
                        connection.server_address, e
                    );
                }
                break;
            }
        };
        let batch = header.batch;
        let id = header.id;
        let total_length = header.total_length;

        let result = {
            match pool.lock_if_not_timeout(batch, id) {
                Ok(_) => {
                    debug!("parse_response: lock success");
                    Ok(())
                }
                Err(_) => {
                    debug!("parse_response: lock timeout");
                    Err("lock timeout")
                }
            }
        };
        match result {
            Ok(_) => {}
            Err(e) => {
                debug!("Error locking callback: {}", e);
                let result = connection
                    .clean_response(&mut read_stream, total_length)
                    .await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        debug!("Error cleaning up response: {}", e);
                        break;
                    }
                }
                continue;
            }
        }

        let meta_data_result = {
            match pool.get_meta_data_ref(id, header.meta_data_length as usize) {
                Ok(meta_data_result) => Ok(meta_data_result),
                Err(_) => Err("meta_data_result error"),
            }
        };
        let data_result = {
            match pool.get_data_ref(id, header.data_length as usize) {
                Ok(data_result) => Ok(data_result),
                Err(_) => Err("data_result error"),
            }
        };
        if let (Ok(data), Ok(meta_data)) = (data_result, meta_data_result) {
            if let Err(e) = connection
                .receive_response(&mut read_stream, meta_data, data)
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
        } else if let Err(e) = connection
            .clean_response(&mut read_stream, total_length)
            .await
        {
            error!("Error cleaning up response: {}", e);
            break;
        }
    }
}
