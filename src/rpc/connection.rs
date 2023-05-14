// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::io::IoSlice;

use crate::rpc::protocol::{
    RequestHeader, ResponseHeader, MAX_DATA_LENGTH, MAX_FILENAME_LENGTH, MAX_METADATA_LENGTH,
    REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE,
};
use log::{debug, error};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::{Mutex, RwLock},
};
// use tokio::{
//     io::{AsyncReadExt, AsyncWriteExt},
//     net::TcpStream,
// };
use anyhow::Result;

enum ConnectionStatus {
    Connected = 0,
    Disconnected = 1,
}

pub struct ClientConnection {
    pub server_address: String,
    write_stream: Option<Mutex<OwnedWriteHalf>>,
    status: RwLock<ConnectionStatus>,
    // lock for send_request
    // we need this lock because we will send multiple requests in parallel
    // and each request will be sent several data packets due to the partation of data and header.
    // now we simply copy the data and header to a buffer and send it in one write call,
    // so we do not need to lock the stream(linux kernel will do it for us).
    _send_lock: Mutex<()>,
}

impl ClientConnection {
    pub fn new(
        server_address: &str,
        write_stream: Option<tokio::sync::Mutex<OwnedWriteHalf>>,
    ) -> Self {
        Self {
            server_address: server_address.to_string(),
            write_stream,
            status: RwLock::new(ConnectionStatus::Connected),
            _send_lock: Mutex::new(()),
        }
    }

    pub async fn disconnect(&mut self) {
        self.write_stream = None;
        *self.status.write().await = ConnectionStatus::Disconnected;
    }

    pub async fn is_connected(&self) -> bool {
        match *self.status.read().await {
            ConnectionStatus::Connected => true,
            ConnectionStatus::Disconnected => false,
        }
    }

    // request
    // | batch | id | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
    #[allow(clippy::too_many_arguments)]
    pub async fn send_request(
        &self,
        batch: u32,
        id: u32,
        operation_type: u32,
        flags: u32,
        filename: &str,
        meta_data: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filename_length = filename.len();
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        let total_length = filename_length + meta_data_length + data_length;
        debug!(
            "send_request batch: {}, id: {}, type: {}, flags: {}, total_length: {}, filname_length: {}, meta_data_length, {}, data_length: {}, filename: {:?}, meta_data: {:?}",
            batch, id, operation_type, flags, total_length, filename_length, meta_data_length, data_length, filename, meta_data
        );
        let mut request = Vec::with_capacity(total_length + REQUEST_HEADER_SIZE);
        request.extend_from_slice(&batch.to_le_bytes());
        request.extend_from_slice(&id.to_le_bytes());
        request.extend_from_slice(&operation_type.to_le_bytes());
        request.extend_from_slice(&flags.to_le_bytes());
        request.extend_from_slice(&(total_length as u32).to_le_bytes());
        request.extend_from_slice(&(filename_length as u32).to_le_bytes());
        request.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
        request.extend_from_slice(&(data_length as u32).to_le_bytes());
        request.extend_from_slice(filename.as_bytes());
        let mut stream = self.write_stream.as_ref().unwrap().lock().await;
        let mut offset = 0;
        loop {
            if offset >= request.len() + meta_data_length + data_length {
                break;
            }
            if offset < request.len() {
                let bufs: &[_] = &[
                    IoSlice::new(&request[offset..]),
                    IoSlice::new(meta_data),
                    IoSlice::new(data),
                ];
                offset += stream.write_vectored(bufs).await?;
            } else if offset < request.len() + meta_data_length {
                let bufs: &[_] = &[
                    IoSlice::new(&meta_data[offset - request.len()..]),
                    IoSlice::new(data),
                ];
                offset += stream.write_vectored(bufs).await?;
            } else {
                let bufs: &[_] = &[IoSlice::new(
                    &data[offset - request.len() - meta_data_length..],
                )];
                offset += stream.write_vectored(bufs).await?;
            }
        }
        Ok(())
    }

    pub async fn receive_response_header(
        &self,
        read_stream: &mut OwnedReadHalf,
    ) -> Result<ResponseHeader, Box<dyn std::error::Error>> {
        let mut header = [0; RESPONSE_HEADER_SIZE];
        debug!(
            "waiting for response_header, length: {}",
            RESPONSE_HEADER_SIZE
        );
        self.receive(read_stream, &mut header).await?;
        let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let status = i32::from_le_bytes(header[8..12].try_into().unwrap());
        let flags = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let meta_data_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
        debug!(
            "received response_header batch: {}, id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}",
            batch, id, status, flags, total_length, meta_data_length, data_length
        );
        Ok(ResponseHeader {
            batch,
            id,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        })
    }

    pub async fn receive_response(
        &self,
        read_stream: &mut OwnedReadHalf,
        meta_data: &mut [u8],
        data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        debug!(
            "waiting for response_meta_data, length: {}",
            meta_data_length
        );
        self.receive(read_stream, &mut meta_data[0..meta_data_length])
            .await?;
        debug!("received reponse_meta_data, meta_data: {:?}", meta_data);
        debug!("waiting for response_data, length: {}", data_length);
        self.receive(read_stream, &mut data[0..data_length]).await?;
        debug!("received reponse_data");
        Ok(())
    }

    pub async fn receive(
        &self,
        read_stream: &mut OwnedReadHalf,
        data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let result = read_stream.read_exact(data).await;
        if let Err(e) = result {
            if e.to_string() != "early eof" {
                error!(
                    "failed to read data from server {:?}, error: {}",
                    self.server_address, e
                );
            }
            return Err(e.into());
        }
        Ok(())
    }

    pub async fn clean_response(
        &self,
        read_stream: &mut OwnedReadHalf,
        total_length: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = vec![0u8; total_length as usize];
        self.receive(read_stream, &mut buffer).await?;
        debug!("cleaned response, total_length: {}", total_length);
        Ok(())
    }
}

pub struct ServerConnection {
    pub id: u32,
    name_id: String,
    write_stream: Mutex<OwnedWriteHalf>,
    status: ConnectionStatus,
}

impl ServerConnection {
    pub fn new(write_stream: OwnedWriteHalf, name_id: String, id: u32) -> Self {
        ServerConnection {
            id,
            name_id,
            write_stream: Mutex::new(write_stream),
            status: ConnectionStatus::Connected,
        }
    }

    pub fn disconnect(&mut self) {
        self.status = ConnectionStatus::Disconnected;
    }

    pub fn is_connected(&self) -> bool {
        match self.status {
            ConnectionStatus::Connected => true,
            ConnectionStatus::Disconnected => false,
        }
    }

    pub fn name_id(&self) -> String {
        self.name_id.clone()
    }

    // response
    // | batch | id | status | flags | total_length | meta_data_lenght | data_length | meta_data | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 0~ |
    pub async fn send_response(
        &self,
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
        debug!(
            "{} response id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}, meta_data: {:?}",
            self.name_id, id, status, flags, total_length, meta_data_length, data_length, meta_data);
        let mut response = Vec::with_capacity(RESPONSE_HEADER_SIZE + total_length);
        response.extend_from_slice(&batch.to_le_bytes());
        response.extend_from_slice(&id.to_le_bytes());
        response.extend_from_slice(&status.to_le_bytes());
        response.extend_from_slice(&flags.to_le_bytes());
        response.extend_from_slice(&(total_length as u32).to_le_bytes());
        response.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
        response.extend_from_slice(&(data_length as u32).to_le_bytes());
        let mut stream = self.write_stream.lock().await;
        let mut offset = 0;
        loop {
            if offset >= response.len() + meta_data_length + data_length {
                break;
            }
            if offset < response.len() {
                let bufs: &[_] = &[
                    IoSlice::new(&response[offset..]),
                    IoSlice::new(meta_data),
                    IoSlice::new(data),
                ];
                offset += stream.write_vectored(bufs).await?;
            } else if offset < response.len() + meta_data_length {
                let bufs: &[_] = &[
                    IoSlice::new(&meta_data[offset - response.len()..]),
                    IoSlice::new(data),
                ];
                offset += stream.write_vectored(bufs).await?;
            } else {
                let bufs: &[_] = &[IoSlice::new(
                    &data[offset - response.len() - meta_data_length..],
                )];
                offset += stream.write_vectored(bufs).await?;
            }
        }
        Ok(())
    }

    pub async fn receive_request_header(
        &self,
        read_stream: &mut OwnedReadHalf,
    ) -> Result<RequestHeader, Box<dyn std::error::Error>> {
        let mut header = [0; REQUEST_HEADER_SIZE];
        debug!(
            "{} waiting for request_header, length: {}",
            self.name_id, REQUEST_HEADER_SIZE
        );
        self.receive(read_stream, &mut header).await?;
        let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let operation_type = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let flags: u32 = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let file_path_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let meta_data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
        let data_length = u32::from_le_bytes(header[28..32].try_into().unwrap());
        debug!(
            "{} received request header: batch: {}, id: {}, type: {}, flags: {}, total_length: {}, file_path_length: {}, meta_data_length: {}, data_length: {}",
            self.name_id, batch, id, operation_type, flags, total_length, file_path_length, meta_data_length, data_length
        );
        Ok(RequestHeader {
            batch,
            id,
            r#type: operation_type,
            flags,
            total_length,
            file_path_length,
            meta_data_length,
            data_length,
        })
    }

    pub async fn receive_request(
        &self,
        read_stream: &mut OwnedReadHalf,
        header: &RequestHeader,
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
        let path_length = u32::from_le_bytes(header.file_path_length.to_le_bytes());
        let meta_data_length = u32::from_le_bytes(header.meta_data_length.to_le_bytes());
        let data_length = u32::from_le_bytes(header.data_length.to_le_bytes());
        if path_length > MAX_FILENAME_LENGTH.try_into().unwrap()
            || data_length > MAX_DATA_LENGTH.try_into().unwrap()
            || meta_data_length > MAX_METADATA_LENGTH.try_into().unwrap()
        {
            return Err("path length or data length or meta data length is too long".into());
        }
        let mut path = vec![0u8; path_length as usize];
        let mut data = vec![0u8; data_length as usize];
        let mut meta_data = vec![0u8; meta_data_length as usize];

        debug!("{} waiting for path, length: {}", self.name_id, path_length);
        self.receive(read_stream, &mut path[0..path_length as usize])
            .await?;
        debug!("{} received path: {:?}", self.name_id, path);

        debug!(
            "{} waiting for meta_data, length: {}",
            self.name_id, meta_data_length
        );
        self.receive(read_stream, &mut meta_data[0..meta_data_length as usize])
            .await?;
        debug!("{} received meta_data: {:?}", self.name_id, meta_data);

        debug!("{} waiting for data, length: {}", self.name_id, data_length);
        self.receive(read_stream, &mut data[0..data_length as usize])
            .await?;
        debug!("{} received data", self.name_id);

        Ok((path, data, meta_data))
    }

    pub async fn receive(
        &self,
        read_stream: &mut OwnedReadHalf,
        data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let result = read_stream.read_exact(data).await;
        if let Err(e) = result {
            if e.to_string() != "early eof" {
                error!(
                    "{} failed to read data from stream, error: {}",
                    self.name_id, e
                );
            }
            return Err(e.into());
        }
        Ok(())
    }
}
