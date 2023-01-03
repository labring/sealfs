// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use common::request::{
    RequestHeader, ResponseHeader, CLIENT_REQUEST_TIMEOUT, MAX_DATA_LENGTH, MAX_FILENAME_LENGTH,
    MAX_METADATA_LENGTH, REQUEST_HEADER_SIZE, REQUEST_QUEUE_LENGTH, RESPONSE_HEADER_SIZE,
};
use log::{debug, error};
use std::{
    io::{Read, Write},
    sync::{mpsc, Arc, Mutex, RwLock},
    vec,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
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
    stream: Option<std::net::TcpStream>,
    status: RwLock<ConnectionStatus>,
    // lock for send_request
    // we need this lock because we will send multiple requests in parallel
    // and each request will be sent several data packets due to the partation of data and header.
    // now we simply copy the data and header to a buffer and send it in one write call,
    // so we do not need to lock the stream(linux kernel will do it for us).
    _send_lock: Mutex<()>,
}

impl ClientConnection {
    pub fn new(server_address: &str) -> Self {
        Self {
            server_address: server_address.to_string(),
            stream: None,
            status: RwLock::new(ConnectionStatus::Disconnected),
            _send_lock: Mutex::new(()),
        }
    }

    pub fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = std::net::TcpStream::connect(self.server_address.clone())?;
        self.stream = Some(stream);
        *self.status.write().unwrap() = ConnectionStatus::Connected;
        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
        *self.status.write().unwrap() = ConnectionStatus::Disconnected;
    }

    pub fn is_connected(&self) -> bool {
        match *self.status.read().unwrap() {
            ConnectionStatus::Connected => true,
            ConnectionStatus::Disconnected => false,
        }
    }

    // request
    // | id | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
    pub fn send_request(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        filename: &str,
        meta_data: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let filename_length = filename.len();
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        let total_length = filename_length + meta_data_length + data_length;
        debug!(
            "total_length: {}, filename_length: {}, meta_data_length: {}, data_length: {}",
            total_length, filename_length, meta_data_length, data_length
        );
        let mut request = Vec::with_capacity(total_length + REQUEST_HEADER_SIZE);
        request.extend_from_slice(&id.to_le_bytes());
        request.extend_from_slice(&operation_type.to_le_bytes());
        request.extend_from_slice(&flags.to_le_bytes());
        request.extend_from_slice(&(total_length as u32).to_le_bytes());
        request.extend_from_slice(&(filename_length as u32).to_le_bytes());
        request.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
        request.extend_from_slice(&(data_length as u32).to_le_bytes());
        request.extend_from_slice(filename.as_bytes());
        request.extend_from_slice(meta_data);
        request.extend_from_slice(data); // Here we copy data to request instead of locking the stream, but it is not sufficient.
        debug!("request: {:?}", request);
        stream.write_all(&request)?;
        Ok(())
    }

    pub fn receive_response_header(&self) -> Result<ResponseHeader, Box<dyn std::error::Error>> {
        let mut header = [0; RESPONSE_HEADER_SIZE];
        self.receive(&mut header)?;
        debug!("header: {:?}", header);
        let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let status = i32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let flags = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let meta_data_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let data_length = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        debug!(
            "id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}",
            id, status, flags, total_length, meta_data_length, data_length
        );
        Ok(ResponseHeader {
            id,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        })
    }

    pub fn receive_response(
        &self,
        meta_data: &mut [u8],
        data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        self.receive(&mut meta_data[0..meta_data_length as usize])?;
        self.receive(&mut data[0..data_length as usize])?;
        Ok(())
    }

    pub fn receive(&self, data: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let mut buf_len = 0;
        debug!("waiting for response, data length: {}", data.len());

        // TODO: use epoll or async read
        while buf_len < data.len() {
            let result = stream.read(data);
            match result {
                Ok(len) => {
                    buf_len += len;
                    debug!("received {} bytes, total: {}", len, buf_len);
                }
                Err(_) => {
                    return Err("failed to receive response".into());
                }
            }
        }
        debug!("received response, data length: {}", buf_len);
        Ok(())
    }

    pub fn clean_response(&self, total_length: u32) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = vec![0u8; total_length as usize];
        self.receive(&mut buffer)?;
        Ok(())
    }
}

pub struct ClientConnectionAsync {
    pub server_address: String,
    write_stream: Option<tokio::sync::Mutex<OwnedWriteHalf>>,
    status: RwLock<ConnectionStatus>,
    // lock for send_request
    // we need this lock because we will send multiple requests in parallel
    // and each request will be sent several data packets due to the partation of data and header.
    // now we simply copy the data and header to a buffer and send it in one write call,
    // so we do not need to lock the stream(linux kernel will do it for us).
    _send_lock: Mutex<()>,
}

impl ClientConnectionAsync {
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

    pub fn disconnect(&mut self) {
        self.write_stream = None;
        *self.status.write().unwrap() = ConnectionStatus::Disconnected;
    }

    pub fn is_connected(&self) -> bool {
        match *self.status.read().unwrap() {
            ConnectionStatus::Connected => true,
            ConnectionStatus::Disconnected => false,
        }
    }

    // request
    // | id | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
    pub async fn send_request(
        &self,
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
            "total_length: {}, filename_length: {}, meta_data_length: {}, data_length: {}",
            total_length, filename_length, meta_data_length, data_length
        );
        let mut request = Vec::with_capacity(total_length + REQUEST_HEADER_SIZE);
        request.extend_from_slice(&id.to_le_bytes());
        request.extend_from_slice(&operation_type.to_le_bytes());
        request.extend_from_slice(&flags.to_le_bytes());
        request.extend_from_slice(&(total_length as u32).to_le_bytes());
        request.extend_from_slice(&(filename_length as u32).to_le_bytes());
        request.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
        request.extend_from_slice(&(data_length as u32).to_le_bytes());
        request.extend_from_slice(filename.as_bytes());
        request.extend_from_slice(meta_data);
        request.extend_from_slice(data); // Here we copy data to request instead of locking the stream, but it is not sufficient.
        debug!("request: {:?}", request);
        self.write_stream
            .as_ref()
            .unwrap()
            .lock()
            .await
            .write_all(&request)
            .await?;
        Ok(())
    }

    pub async fn receive_response_header(
        &self,
        read_stream: &mut OwnedReadHalf,
    ) -> Result<ResponseHeader> {
        let mut header = [0; RESPONSE_HEADER_SIZE];
        self.receive(read_stream, &mut header).await?;
        debug!("header: {:?}", header);
        let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let status = i32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let flags = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let meta_data_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let data_length = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        debug!(
            "id: {}, status: {}, flags: {}, total_length: {}, meta_data_length: {}, data_length: {}",
            id, status, flags, total_length, meta_data_length, data_length
        );
        Ok(ResponseHeader {
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
    ) -> Result<()> {
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        self.receive(read_stream, &mut meta_data[0..meta_data_length as usize])
            .await?;
        self.receive(read_stream, &mut data[0..data_length as usize])
            .await?;
        Ok(())
    }

    pub async fn receive(&self, read_stream: &mut OwnedReadHalf, data: &mut [u8]) -> Result<()> {
        debug!("waiting for response, data length: {}", data.len());
        let result = read_stream.read_exact(data).await;
        match result {
            Ok(len) => {
                debug!("received {} bytes", len);
            }
            Err(_) => {
                return Err(anyhow::anyhow!("failed to receive response"));
            }
        }
        Ok(())
    }

    pub async fn clean_response(
        &self,
        read_stream: &mut OwnedReadHalf,
        total_length: u32,
    ) -> Result<()> {
        let mut buffer = vec![0u8; total_length as usize];
        self.receive(read_stream, &mut buffer).await?;
        Ok(())
    }
}

pub struct ServerConnection {
    write_stream: tokio::sync::Mutex<OwnedWriteHalf>,
    status: ConnectionStatus,
}

impl ServerConnection {
    pub fn new(write_stream: OwnedWriteHalf) -> Self {
        ServerConnection {
            write_stream: tokio::sync::Mutex::new(write_stream),
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

    // response
    // | id | status | flags | total_length | meta_data_lenght | data_length | meta_data | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 0~ |
    pub async fn send_response(
        &self,
        id: u32,
        status: i32,
        flags: u32,
        meta_data: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("send response, id: {}", id);
        let response = {
            let data_length = data.len();
            let meta_data_length = meta_data.len();
            let total_length = data_length + meta_data_length;
            let mut response = Vec::with_capacity(RESPONSE_HEADER_SIZE + total_length);
            response.extend_from_slice(&id.to_le_bytes());
            response.extend_from_slice(&status.to_le_bytes());
            response.extend_from_slice(&flags.to_le_bytes());
            response.extend_from_slice(&(total_length as u32).to_le_bytes());
            response.extend_from_slice(&(meta_data_length as u32).to_le_bytes());
            response.extend_from_slice(&(data_length as u32).to_le_bytes());
            response.extend_from_slice(meta_data);
            response.extend_from_slice(data);
            response
        };
        debug!("response: {:?}", response);
        self.write_stream.lock().await.write_all(&response).await?;
        Ok(())
    }

    pub async fn receive_request_header(
        &self,
        read_stream: &mut OwnedReadHalf,
    ) -> Result<RequestHeader, Box<dyn std::error::Error>> {
        let mut header = [0; REQUEST_HEADER_SIZE];
        self.receive(read_stream, &mut header).await?;
        let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let operation_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let flags: u32 = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let file_path_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let meta_data_length = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        let data_length = u32::from_le_bytes([header[24], header[25], header[26], header[27]]);
        debug!(
            "received request header: id: {}, type: {}, flags: {}, total_length: {}, file_path_length: {}, meta_data_length: {}, data_length: {}",
            id, operation_type, flags, total_length, file_path_length, meta_data_length, data_length
        );
        Ok(RequestHeader {
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
        self.receive(read_stream, &mut path[0..path_length as usize])
            .await?;
        self.receive(read_stream, &mut meta_data[0..meta_data_length as usize])
            .await?;
        self.receive(read_stream, &mut data[0..data_length as usize])
            .await?;
        Ok((path, data, meta_data))
    }

    pub async fn receive(
        &self,
        read_stream: &mut OwnedReadHalf,
        data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("waiting for request, data length: {}", data.len());

        let result = read_stream.read_exact(data).await;
        match result {
            Ok(len) => {
                debug!("received data length: {}, data: {:?}", len, data);
            }
            Err(e) => {
                error!("failed to read data from stream, error: {}", e);
                return Err(e.into());
            }
        }
        Ok(())
    }
}

unsafe impl std::marker::Sync for ServerConnection {}
unsafe impl std::marker::Send for ServerConnection {}

pub enum CallbackState {
    Empty = 0,
    WaitingForResponse = 1,
    Done = 2,
    Error = 3,
}

pub struct OperationCallback {
    pub data: *const u8,
    pub meta_data: *const u8,
    pub data_length: usize,
    pub meta_data_length: usize,
    pub state: CallbackState,
    pub request_status: libc::c_int,
    pub flags: u32,
    pub channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
}

unsafe impl std::marker::Sync for OperationCallback {}
unsafe impl std::marker::Send for OperationCallback {}

impl Default for OperationCallback {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            meta_data: std::ptr::null(),
            data_length: 0,
            meta_data_length: 0,
            state: CallbackState::Empty,
            request_status: 0,
            flags: 0,
            channel: mpsc::channel(),
        }
    }
}

impl OperationCallback {
    pub fn new() -> Self {
        Self {
            data: std::ptr::null(),
            meta_data: std::ptr::null(),
            data_length: 0,
            meta_data_length: 0,
            state: CallbackState::Empty,
            request_status: 0,
            flags: 0,
            channel: mpsc::channel(),
        }
    }
}

pub struct CircularQueue {
    callbacks: Vec<*const OperationCallback>,
    start_index: Arc<Mutex<u32>>, // maybe we can use a lock-free queue
    end_index: Arc<Mutex<u32>>,
}

impl Default for CircularQueue {
    fn default() -> Self {
        Self {
            callbacks: vec![std::ptr::null(); REQUEST_QUEUE_LENGTH],
            start_index: Arc::new(Mutex::new(0)),
            end_index: Arc::new(Mutex::new(1)),
        }
    }
}

impl CircularQueue {
    pub fn new() -> Self {
        Self {
            callbacks: vec![std::ptr::null_mut(); REQUEST_QUEUE_LENGTH],
            start_index: Arc::new(Mutex::new(0)),
            end_index: Arc::new(Mutex::new(1)),
        }
    }

    pub fn init(&mut self) {
        for i in 0..self.callbacks.len() {
            // allocate memory for the callback on the heap to avoid thread context switch.
            // ensure that the callback is not moved by the compiler.
            // TODO: release the memory when the server is stopped
            self.callbacks[i] = Box::into_raw(Box::new(OperationCallback::new()));
        }
        *self.end_index.lock().unwrap() = 0;
    }

    pub fn register_callback(
        &self,
        rsp_meta_data: &mut [u8],
        rsp_data: &mut [u8],
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let id = {
            let mut end_index = self.end_index.lock().unwrap();
            let id = *end_index;
            *end_index = (*end_index + 1) % REQUEST_QUEUE_LENGTH as u32;
            id
        };
        unsafe {
            let callback = self.callbacks[id as usize];
            (*(callback as *mut OperationCallback)).state = CallbackState::WaitingForResponse;
            (*(callback as *mut OperationCallback)).data = rsp_data.as_ptr();
            (*(callback as *mut OperationCallback)).meta_data = rsp_meta_data.as_ptr();
            (*(callback as *mut OperationCallback)).data_length = rsp_data.len();
            (*(callback as *mut OperationCallback)).meta_data_length = rsp_meta_data.len();
        }
        Ok(id)
    }

    pub fn clean_up(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut start_index = self.start_index.lock().unwrap();
        let end_flag = *self.end_index.lock().unwrap();
        for i in *start_index..end_flag {
            unsafe {
                let callback = self.callbacks[i as usize];
                match (*callback).state {
                    CallbackState::Done => {
                        (*(callback as *mut OperationCallback)).state = CallbackState::Empty;
                    }
                    CallbackState::Error => {
                        (*(callback as *mut OperationCallback)).state = CallbackState::Empty;
                    }
                    CallbackState::WaitingForResponse => {
                        *start_index = i;
                        break;
                    }
                    _ => Err("Invalid callback state")?,
                }
            }
        }
        Ok(())
    }

    pub fn get_data_ref(
        &self,
        id: u32,
        data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).data as *mut u8,
                data_length,
            ))
        }
    }

    pub fn get_meta_data_ref(
        &self,
        id: u32,
        meta_data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).meta_data as *mut u8,
                meta_data_length,
            ))
        }
    }

    pub fn response(&self, id: u32, status: libc::c_int) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let callback = self.callbacks[id as usize];
            (*(callback as *mut OperationCallback)).state = CallbackState::Done;
            (*(callback as *mut OperationCallback)).request_status = status;
            (*(callback as *mut OperationCallback)).channel.0.send(())?;
        }
        self.clean_up()?;
        Ok(())
    }

    pub fn error(&self, id: u32) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let callback = self.callbacks[id as usize];
            (*(callback as *mut OperationCallback)).state = CallbackState::Error;
        }
        self.clean_up()?;
        Ok(())
    }

    // wait_for_callback
    // return: (status, flags, data_length, meta_data_length)
    pub fn wait_for_callback(
        &self,
        id: u32,
    ) -> Result<(i32, u32, usize, usize), Box<dyn std::error::Error>> {
        unsafe {
            let callback = self.callbacks[id as usize];
            let result = (*(callback as *mut OperationCallback))
                .channel
                .1
                .recv_timeout(CLIENT_REQUEST_TIMEOUT);
            match result {
                Ok(_) => {
                    (*(callback as *mut OperationCallback)).state = CallbackState::Done;
                    Ok((
                        (*(callback as *mut OperationCallback)).request_status,
                        (*(callback as *mut OperationCallback)).flags,
                        (*(callback as *mut OperationCallback)).data_length,
                        (*(callback as *mut OperationCallback)).meta_data_length,
                    ))
                }
                Err(_) => {
                    self.error(id)?;
                    Err("Timeout")?
                }
            }
        }
    }
}

unsafe impl std::marker::Sync for CircularQueue {}
unsafe impl std::marker::Send for CircularQueue {}
