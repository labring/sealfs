// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::connection::{clean_up, CircularQueue, ClientConnectionAsync};
use dashmap::DashMap;
use log::debug;
use std::sync::Arc;
use tokio::{net::tcp::OwnedReadHalf, task::JoinHandle};

pub struct Client {
    connections: DashMap<String, Arc<ClientConnectionAsync>>,
    queue: Arc<CircularQueue>,
    job_handle: JoinHandle<()>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

// parse_response
// try to get response from sequence of connections and write to callbacks
pub async fn parse_response(
    mut read_stream: OwnedReadHalf,
    connection: Arc<ClientConnectionAsync>,
    queue: Arc<CircularQueue>,
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
                    match queue.get_meta_data_ref(id, header.meta_data_length as usize) {
                        Ok(meta_data_result) => Ok(meta_data_result),
                        Err(_) => Err("meta_data_result error"),
                    }
                };
                let data_result = {
                    match queue.get_data_ref(id, header.data_length as usize) {
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
                                let result = queue.response(
                                    id,
                                    header.status,
                                    header.flags,
                                    header.meta_data_length as usize,
                                    header.data_length as usize,
                                );
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

impl Client {
    pub fn new() -> Self {
        let mut queue = CircularQueue::new();
        queue.init();
        let queue = Arc::new(queue);
        let q1 = queue.clone();
        let job = tokio::spawn(clean_up(q1));

        Self {
            connections: DashMap::new(),
            queue,
            job_handle: job,
        }
    }

    pub async fn close(&self) {
        self.job_handle.abort()
    }

    pub async fn add_connection(&self, server_address: &str) {
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
                    self.queue.clone(),
                ));
                connection
            }
            Err(e) => {
                debug!("connect error: {}", e);
                Arc::new(ClientConnectionAsync::new(server_address, None))
            }
        };
        self.connections
            .insert(server_address.to_string(), connection);
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
        debug!(
            "call_remote on connection: {:?}",
            connection.value().server_address
        );
        let result = {
            match self.queue.register_callback(recv_meta_data, recv_data) {
                Ok(result) => Ok(result),
                Err(_) => Err("register_callback error"),
            }
        };

        debug!("call_remote on connection: {:?}", result);
        match result {
            Ok(id) => {
                let send_result = connection
                    .send_request(
                        id,
                        operation_type,
                        req_flags,
                        path,
                        send_meta_data,
                        send_data,
                    )
                    .await;
                match send_result {
                    Ok(_) => {
                        // todo: wait_for_callback will use async instead
                        let result =
                            tokio::task::block_in_place(move || self.queue.wait_for_callback(id));
                        match result {
                            Ok((s, f, meta_data_length, data_length)) => {
                                debug!("call_remote success, status: {}, flags: {}, meta_data_length: {}, data_length: {}", s, f, meta_data_length, data_length);
                                *status = s;
                                *rsp_flags = f;
                                *recv_meta_data_length = meta_data_length;
                                *recv_data_length = data_length;
                                Ok(())
                            }
                            Err(_) => {
                                self.queue.error(id)?;
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "Error waiting for callback",
                                )
                                .into())
                            }
                        }
                    }
                    Err(_) => {
                        self.queue.error(id)?;
                        Err(
                            std::io::Error::new(std::io::ErrorKind::Other, "Error sending request")
                                .into(),
                        )
                    }
                }
            }
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error registering callback",
            )
            .into()),
        }
    }
}
