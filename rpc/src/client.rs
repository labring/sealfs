// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::connection::{CircularQueue, ClientConnection, ClientConnectionAsync};
use dashmap::DashMap;
use log::debug;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
pub struct Client {
    connections: DashMap<i32, Arc<ClientConnection>>,
    queue: CircularQueue,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            connections: DashMap::new(),
            queue: CircularQueue::new(),
        }
    }
}

impl Client {
    pub fn new() -> Self {
        let mut queue = CircularQueue::new();
        queue.init();
        Self {
            connections: DashMap::new(),
            queue,
        }
    }

    pub fn add_connection(&self, connection_id: i32, server_address: &str) {
        let mut connection = ClientConnection::new(server_address);
        let result = connection.connect();
        match result {
            Ok(_) => {
                debug!("connect success");
            }
            Err(e) => {
                debug!("connect error: {}", e);
            }
        }
        let connection = Arc::new(connection);
        self.connections.insert(connection_id, connection);
    }

    pub fn remove_connection(&self, connection_id: i32) {
        self.connections.remove(&connection_id);
    }

    // pub fn get_connection(
    //     &self,
    //     connection_id: i32,
    // ) -> Result<Arc<ClientConnection>, Box<dyn std::error::Error>> {
    //     let connection = self.connections.get(&connection_id).unwrap();
    //     Ok(connection.value().clone())
    // }

    // parse_response
    // try to get response from sequence of connections and write to callbacks
    pub fn parse_response(&self) {
        loop {
            // TODO: use epoll to watch all connections
            for it in self.connections.iter() {
                let connection = it.value();
                debug!("parse_response: {:?}", connection.server_address);
                let result = connection.receive_response_header();
                match result {
                    Ok(header) => {
                        let id = header.id;
                        let total_length = header.total_length;
                        let meta_data_result = self
                            .queue
                            .get_meta_data_ref(id, header.meta_data_length as usize);
                        let data_result = self.queue.get_data_ref(id, header.data_length as usize);
                        match (data_result, meta_data_result) {
                            (Ok(data), Ok(meta_data)) => {
                                let response = connection.receive_response(data, meta_data);
                                match response {
                                    Ok(_) => {
                                        let result = self.queue.response(id, 0);
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
                                let result = connection.clean_response(total_length);
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
    }

    #[allow(clippy::too_many_arguments)]
    pub fn call_remote(
        &self,
        connection_index: i32,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("call_remote on connection: {}", connection_index);
        let connection = self.connections.get(&connection_index).unwrap();
        debug!(
            "call_remote on connection: {:?}",
            connection.value().server_address
        );
        let result = self.queue.register_callback(recv_meta_data, recv_data);
        debug!("call_remote on connection: {:?}", result);
        match result {
            Ok(id) => {
                let send_result = connection.send_request(
                    id,
                    operation_type,
                    req_flags,
                    path,
                    send_meta_data,
                    send_data,
                );
                match send_result {
                    Ok(_) => {
                        let result = self.queue.wait_for_callback(id);
                        match result {
                            Ok(_) => {
                                *status = self.queue.get_status(id).unwrap();
                                *rsp_flags = self.queue.get_rsp_flags(id).unwrap();
                                Ok(())
                            }
                            Err(_) => {
                                self.queue.error(id)?;
                                Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "Error waiting for callback",
                                )))
                            }
                        }
                    }
                    Err(_) => {
                        self.queue.error(id)?;
                        Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Error sending request",
                        )))
                    }
                }
            }
            Err(_) => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Error registering callback",
            ))),
        }
    }
}

pub struct ClientAsync {
    connections: DashMap<i32, Arc<ClientConnectionAsync>>,
    queue: CircularQueue,
}

impl Default for ClientAsync {
    fn default() -> Self {
        Self {
            connections: DashMap::new(),
            queue: CircularQueue::new(),
        }
    }
}

impl ClientAsync {
    pub fn new() -> Self {
        let mut queue = CircularQueue::new();
        queue.init();
        Self {
            connections: DashMap::new(),
            queue,
        }
    }
    // Client must be static to tokio::spawn
    pub async fn add_connection(&'static self, connection_id: i32, server_address: &str) {
        let result = tokio::net::TcpStream::connect(server_address).await;
        let connection = match result {
            Ok(stream) => {
                debug!("connect success");
                let (read_stream, write_stream) = stream.into_split();
                tokio::spawn(self.parse_response(connection_id, read_stream));
                Arc::new(ClientConnectionAsync::new(
                    server_address,
                    Some(tokio::sync::Mutex::new(write_stream)),
                ))
            }
            Err(e) => {
                debug!("connect error: {}", e);
                Arc::new(ClientConnectionAsync::new(server_address, None))
            }
        };
        self.connections.insert(connection_id, connection);
    }

    pub fn remove_connection(&self, connection_id: i32) {
        self.connections.remove(&connection_id);
    }

    // parse_response
    // try to get response from sequence of connections and write to callbacks
    pub async fn parse_response(&self, connection_id: i32, mut read_stream: OwnedReadHalf) {
        loop {
            let connection = match self.connections.get(&connection_id) {
                Some(connection) => connection,
                None => {
                    return;
                }
            };
            debug!("parse_response: {:?}", connection.server_address);
            let result = connection.receive_response_header(&mut read_stream).await;
            match result {
                Ok(header) => {
                    let id = header.id;
                    let total_length = header.total_length;
                    let meta_data_result = {
                        match self
                            .queue
                            .get_meta_data_ref(id, header.meta_data_length as usize)
                        {
                            Ok(meta_data_result) => Ok(meta_data_result),
                            Err(_) => Err("meta_data_result error"),
                        }
                    };
                    let data_result = {
                        match self.queue.get_data_ref(id, header.data_length as usize) {
                            Ok(data_result) => Ok(data_result),
                            Err(_) => Err("data_result error"),
                        }
                    };

                    match (data_result, meta_data_result) {
                        (Ok(data), Ok(meta_data)) => {
                            let response = connection
                                .receive_response(&mut read_stream, data, meta_data)
                                .await;
                            match response {
                                Ok(_) => {
                                    let result = self.queue.response(id, 0);
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

    #[allow(clippy::too_many_arguments)]
    pub async fn call_remote(
        &self,
        connection_index: i32,
        operation_type: u32,
        req_flags: u32,
        path: &str,
        send_meta_data: &[u8],
        send_data: &[u8],
        status: &mut i32,
        rsp_flags: &mut u32,
        recv_meta_data: &mut [u8],
        recv_data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("call_remote on connection: {}", connection_index);
        let connection = self.connections.get(&connection_index).unwrap();
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
                        let result = self.queue.wait_for_callback(id);
                        match result {
                            Ok(_) => {
                                *status = self.queue.get_status(id).unwrap();
                                *rsp_flags = self.queue.get_rsp_flags(id).unwrap();
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
