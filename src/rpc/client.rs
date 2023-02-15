// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::{callback::CallbackPool, connection::ClientConnectionAsync};
use dashmap::DashMap;
use log::debug;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;

pub struct Client {
    connections: DashMap<String, Arc<ClientConnectionAsync>>,
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
                debug!("connect success");
                let (read_stream, write_stream) = stream.into_split();
                let connection = Arc::new(ClientConnectionAsync::new(
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
        debug!("call_remote on connection: {}", server_address);
        let connection = self.connections.get(server_address).unwrap();
        let id = self
            .pool
            .register_callback(recv_meta_data, recv_data)
            .await?;
        debug!(
            "call_remote: {:?} id: {:?}",
            connection.value().server_address,
            id
        );
        connection
            .send_request(
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
            "call_remote success, status: {}, flags: {}, meta_data_length: {}, data_length: {}",
            s, f, meta_data_length, data_length
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
    connection: Arc<ClientConnectionAsync>,
    pool: Arc<CallbackPool>,
) {
    loop {
        if !connection.is_connected() {
            debug!("parse_response: connection closed");
            break;
        }
        debug!("parse_response: {:?}", connection.server_address);
        let result = connection.receive_response_header(&mut read_stream).await;
        match result {
            Ok(header) => {
                let id = header.id;
                let total_length = header.total_length;
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

                match (data_result, meta_data_result) {
                    (Ok(data), Ok(meta_data)) => {
                        let response = connection
                            .receive_response(&mut read_stream, meta_data, data)
                            .await;
                        match response {
                            Ok(_) => {
                                let result = pool
                                    .response(
                                        id,
                                        header.status,
                                        header.flags,
                                        header.meta_data_length as usize,
                                        header.data_length as usize,
                                    )
                                    .await;
                                match result {
                                    Ok(_) => {
                                        debug!("Response success");
                                    }
                                    Err(e) => {
                                        debug!("Error writing response back: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("Error receiving response: {}", e);
                            }
                        }
                    }
                    _ => {
                        let result = connection
                            .clean_response(&mut read_stream, total_length)
                            .await;
                        match result {
                            Ok(_) => {}
                            Err(e) => {
                                debug!("Error cleaning up response: {}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Error parsing header: {}", e);
            }
        }
    }
}
