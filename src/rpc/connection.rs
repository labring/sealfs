// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::{io::IoSlice, marker::PhantomData, sync::atomic::AtomicU32};

use super::protocol::{
    RequestHeader, ResponseHeader, MAX_DATA_LENGTH, MAX_FILENAME_LENGTH, MAX_METADATA_LENGTH,
    REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE,
};
use log::{error, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

const CONNECTED: u32 = 0;
const DISCONNECTED: u32 = 1;

pub struct ClientConnection<W: AsyncWriteExt + Unpin, R: AsyncReadExt + Unpin> {
    pub server_address: String,
    write_stream: Mutex<Option<W>>,
    status: AtomicU32,
    reconneting_lock: Mutex<()>,

    phantom_data: PhantomData<R>,

    // lock for send_request
    // we need this lock because we will send multiple requests in parallel
    // and each request will be sent several data packets due to the partation of data and header.
    // now we simply copy the data and header to a buffer and send it in one write call,
    // so we do not need to lock the stream(linux kernel will do it for us).
    _send_lock: Mutex<()>,
}

impl<W: AsyncWriteExt + Unpin, R: AsyncReadExt + Unpin> ClientConnection<W, R> {
    pub fn new(server_address: &str, write_stream: W) -> Self {
        Self {
            server_address: server_address.to_string(),
            write_stream: Mutex::new(Some(write_stream)),
            status: AtomicU32::new(CONNECTED),
            reconneting_lock: Mutex::new(()),
            phantom_data: PhantomData,
            _send_lock: Mutex::new(()),
        }
    }

    pub fn disconnect(&self) -> bool {
        info!("disconnecting from server {}", self.server_address);
        self.status
            .compare_exchange(
                CONNECTED,
                DISCONNECTED,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }

    pub fn is_connected(&self) -> bool {
        self.status.load(std::sync::atomic::Ordering::Acquire) == CONNECTED
    }

    pub async fn get_reconnecting_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.reconneting_lock.lock().await
    }

    pub async fn reset_connection(&self, write_stream: W) {
        self.write_stream.lock().await.replace(write_stream);
        self.status
            .store(CONNECTED, std::sync::atomic::Ordering::SeqCst);
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
    ) -> Result<(), String> {
        if !self.is_connected() {
            return Err("connection is not connected".to_string());
        }
        let filename_length = filename.len();
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        let total_length = filename_length + meta_data_length + data_length;
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
        let mut stream = self.write_stream.lock().await;
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
                offset += stream
                    .as_mut()
                    .unwrap()
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            } else if offset < request.len() + meta_data_length {
                let bufs: &[_] = &[
                    IoSlice::new(&meta_data[offset - request.len()..]),
                    IoSlice::new(data),
                ];
                offset += stream
                    .as_mut()
                    .unwrap()
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                let bufs: &[_] = &[IoSlice::new(
                    &data[offset - request.len() - meta_data_length..],
                )];
                offset += stream
                    .as_mut()
                    .unwrap()
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    pub async fn receive_response_header(
        &self,
        read_stream: &mut R,
    ) -> Result<ResponseHeader, String> {
        let mut header = [0; RESPONSE_HEADER_SIZE];
        self.receive(read_stream, &mut header).await?;
        let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let status = i32::from_le_bytes(header[8..12].try_into().unwrap());
        let flags = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let meta_data_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
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
        read_stream: &mut R,
        meta_data: &mut [u8],
        data: &mut [u8],
    ) -> Result<(), String> {
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        self.receive(read_stream, &mut meta_data[0..meta_data_length])
            .await?;
        self.receive(read_stream, &mut data[0..data_length]).await?;
        Ok(())
    }

    pub async fn receive(&self, read_stream: &mut R, data: &mut [u8]) -> Result<(), String> {
        match read_stream.read_exact(data).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    pub async fn clean_response(
        &self,
        read_stream: &mut R,
        total_length: u32,
    ) -> Result<(), String> {
        let mut buffer = vec![0u8; total_length as usize];
        self.receive(read_stream, &mut buffer).await?;
        Ok(())
    }
}

pub struct ServerConnection<W: AsyncWriteExt + Unpin, R: AsyncReadExt + Unpin> {
    pub id: u32,
    name_id: String,
    write_stream: Mutex<W>,

    phantom_data: PhantomData<R>,
}

impl<W: AsyncWriteExt + Unpin, R: AsyncReadExt + Unpin> ServerConnection<W, R> {
    pub fn new(write_stream: W, name_id: String, id: u32) -> Self {
        ServerConnection {
            id,
            name_id,
            write_stream: Mutex::new(write_stream),

            phantom_data: PhantomData,
        }
    }

    pub fn name_id(&self) -> String {
        self.name_id.clone()
    }

    pub async fn close(&self) -> Result<(), String> {
        let mut stream = self.write_stream.lock().await;
        stream.shutdown().await.map_err(|e| e.to_string())?;
        info!("close connection {}", self.name_id);
        Ok(())
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
    ) -> Result<(), String> {
        let data_length = data.len();
        let meta_data_length = meta_data.len();
        let total_length = data_length + meta_data_length;
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
                offset += stream
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            } else if offset < response.len() + meta_data_length {
                let bufs: &[_] = &[
                    IoSlice::new(&meta_data[offset - response.len()..]),
                    IoSlice::new(data),
                ];
                offset += stream
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                let bufs: &[_] = &[IoSlice::new(
                    &data[offset - response.len() - meta_data_length..],
                )];
                offset += stream
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    pub async fn receive_request_header(
        &self,
        read_stream: &mut R,
    ) -> Result<RequestHeader, String> {
        let mut header = [0; REQUEST_HEADER_SIZE];
        self.receive(read_stream, &mut header).await?;
        let batch = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let id = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let operation_type = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let flags: u32 = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let total_length = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let file_path_length = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let meta_data_length = u32::from_le_bytes(header[24..28].try_into().unwrap());
        let data_length = u32::from_le_bytes(header[28..32].try_into().unwrap());
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
        read_stream: &mut R,
        header: &RequestHeader,
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), String> {
        if header.file_path_length as usize > MAX_FILENAME_LENGTH {
            error!("path length is too long: {}", header.file_path_length);
            return Err("path length is too long".into());
        }
        if header.data_length as usize > MAX_DATA_LENGTH {
            error!("data length is too long: {}", header.data_length);
            return Err("data length is too long".into());
        }
        if header.meta_data_length as usize > MAX_METADATA_LENGTH {
            error!("meta data length is too long: {}", header.meta_data_length);
            return Err("meta data length is too long".into());
        }
        let mut path = vec![0u8; header.file_path_length as usize];
        let mut data = vec![0u8; header.data_length as usize];
        let mut meta_data = vec![0u8; header.meta_data_length as usize];

        self.receive(read_stream, &mut path[0..header.file_path_length as usize])
            .await?;
        self.receive(
            read_stream,
            &mut meta_data[0..header.meta_data_length as usize],
        )
        .await?;
        self.receive(read_stream, &mut data[0..header.data_length as usize])
            .await?;

        Ok((path, data, meta_data))
    }

    pub async fn receive(&self, read_stream: &mut R, data: &mut [u8]) -> Result<(), String> {
        match read_stream.read_exact(data).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}
