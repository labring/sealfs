// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use common::request::{
    OperationType, ResponseHeader, CLIENT_REQUEST_TIMEOUT, MAX_DATA_LENGTH, MAX_METADATA_LENGTH,
    REQUEST_HEADER_SIZE, REQUEST_QUEUE_LENGTH, RESPONSE_HEADER_SIZE,
};
use log::info;
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{mpsc, Arc, Mutex},
};

enum ConnectionStatus {
    Connected,
    Disconnected,
}

pub struct Connection {
    pub host: String,
    pub port: u16,
    stream: Option<TcpStream>,
    status: ConnectionStatus,
    _send_lock: Arc<Mutex<()>>,
}

impl Connection {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            stream: None,
            status: ConnectionStatus::Disconnected,
            _send_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(format!("{}:{}", self.host, self.port))?;
        self.stream = Some(stream);
        self.status = ConnectionStatus::Connected;
        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.stream = None;
        self.status = ConnectionStatus::Disconnected;
    }

    pub fn is_connected(&self) -> bool {
        match self.status {
            ConnectionStatus::Connected => true,
            ConnectionStatus::Disconnected => false,
        }
    }

    // request
    // | id | type | flags | total_length | filename_length | filename | meta_data_length | meta_data | data_length | data |
    // | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 4Byte | 0~ | 4Byte | 0~ |
    pub fn send(
        &self,
        id: u32,
        operation_type: OperationType,
        flags: u32,
        filename: &str,
        meta_data: &[u8],
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let total_length = REQUEST_HEADER_SIZE + filename.len() + meta_data.len() + data.len();
        let filename_length = filename.len();
        let meta_data_length = meta_data.len();
        let data_length = data.len();
        let mut request = Vec::with_capacity(total_length);
        request.extend_from_slice(&id.to_be_bytes());
        request.extend_from_slice(&(operation_type as u32).to_be_bytes());
        request.extend_from_slice(&flags.to_be_bytes());
        request.extend_from_slice(&total_length.to_be_bytes());
        request.extend_from_slice(&filename_length.to_be_bytes());
        request.extend_from_slice(filename.as_bytes());
        request.extend_from_slice(&meta_data_length.to_be_bytes());
        request.extend_from_slice(meta_data);
        request.extend_from_slice(&data_length.to_be_bytes());
        request.extend_from_slice(data);
        stream.write_all(&request)?;
        Ok(())
    }

    pub fn receive_response_header(&self) -> Result<ResponseHeader, Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let mut header = [0; RESPONSE_HEADER_SIZE];
        stream.read_exact(&mut header)?;
        let id = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let status = i32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let flags = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let total_length = u32::from_be_bytes([header[12], header[13], header[14], header[15]]);
        Ok(ResponseHeader {
            id,
            status,
            flags,
            total_length,
        })
    }

    pub fn receive_response(
        &self,
        data: &mut [u8],
        meta_data: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let mut header = [0u8; RESPONSE_HEADER_SIZE];
        stream.read_exact(&mut header)?;
        let mut data_length_bytes = [0u8; 4];
        let mut meta_data_length_bytes = [0u8; 4];
        data_length_bytes.copy_from_slice(&header[0..4]);
        meta_data_length_bytes.copy_from_slice(&header[4..8]);
        let data_length = u32::from_be_bytes(data_length_bytes);
        let meta_data_length = u32::from_be_bytes(meta_data_length_bytes);
        if data_length > MAX_DATA_LENGTH.try_into().unwrap()
            || meta_data_length > MAX_METADATA_LENGTH.try_into().unwrap()
        {
            return Err("data length or meta data length is too long".into());
        }
        stream.read_exact(&mut data[0..data_length as usize])?;
        stream.read_exact(&mut meta_data[0..meta_data_length as usize])?;
        Ok(())
    }

    pub fn clean_response(&self, total_length: u32) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = self.stream.as_ref().unwrap();
        let mut buffer = vec![0u8; total_length as usize];
        stream.read_exact(&mut buffer)?;
        Ok(())
    }
}

pub enum CallbackState {
    Empty = 0,
    WaitingForResponse = 1,
    Done = 2,
    Timeout = 3,
}

pub struct OperationCallback {
    // Data and meta_data are used to store the response data
    // and meta_data from the server.
    // But it should be used only when the state is Done(In particular,
    // it should not be used util the request processing is completed)
    //
    // A better way is to use a mutable pointer to the data and meta_data
    // in unsafe code to avoid large memory usage and lifetime issues.
    // We should do this in the future.
    pub data: [u8; MAX_DATA_LENGTH],
    pub meta_data: [u8; MAX_METADATA_LENGTH],
    pub data_length: u32,
    pub meta_data_length: u32,
    pub state: CallbackState,
    pub request_status: libc::c_int,
    pub channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
}

unsafe impl std::marker::Sync for OperationCallback {}

impl Default for OperationCallback {
    fn default() -> Self {
        Self {
            data: [0u8; MAX_DATA_LENGTH],
            meta_data: [0u8; MAX_METADATA_LENGTH],
            data_length: 0,
            meta_data_length: 0,
            state: CallbackState::Empty,
            request_status: 0,
            channel: mpsc::channel(),
        }
    }
}

impl OperationCallback {
    pub fn new() -> Self {
        Self {
            data: [0u8; MAX_DATA_LENGTH],
            meta_data: [0u8; MAX_METADATA_LENGTH],
            data_length: 0,
            meta_data_length: 0,
            state: CallbackState::Empty,
            request_status: 0,
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
            end_index: Arc::new(Mutex::new(0)),
        }
    }
}

impl CircularQueue {
    pub fn new() -> Self {
        Self {
            callbacks: vec![std::ptr::null_mut(); REQUEST_QUEUE_LENGTH],
            start_index: Arc::new(Mutex::new(0)),
            end_index: Arc::new(Mutex::new(0)),
        }
    }

    pub fn init(&mut self) {
        for i in 0..self.callbacks.len() {
            self.callbacks[i] = Box::into_raw(Box::new(OperationCallback::new()));
        }
    }

    pub fn register_callback(
        &self,
        _data: &[u8],
        data_length: u32,
        _meta_data: &[u8],
        meta_data_length: u32,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let mut end_index = self.end_index.lock().unwrap();
        let id = *end_index;
        *end_index = (*end_index + 1) % REQUEST_QUEUE_LENGTH as u32;
        unsafe {
            let callback = self.callbacks[id as usize];
            (*(callback as *mut OperationCallback)).state = CallbackState::WaitingForResponse;
            // TODO: put data and meta_data to the callback
            // (*(callback as *mut OperationCallback)).data = data;
            // (*(callback as *mut OperationCallback)).meta_data = meta_data;
            (*(callback as *mut OperationCallback)).data_length = data_length;
            (*(callback as *mut OperationCallback)).meta_data_length = meta_data_length;
        }
        Ok(id)
    }

    pub fn clean_up(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut start_index = self.start_index.lock().unwrap();
        let end_index = self.end_index.lock().unwrap();
        let end_flag = *end_index;
        drop(end_index);
        for i in *start_index..end_flag {
            unsafe {
                let callback = self.callbacks[i as usize];
                match (*callback).state {
                    CallbackState::Done => {
                        (*(callback as *mut OperationCallback)).state = CallbackState::Empty;
                    }
                    CallbackState::Timeout => {
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

    pub fn get_data_ref(&self, id: u32) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            // if (*callback).status != CallbackState::WaitingForResponse {
            //     Err("Callback status not WaitingForResponse")?;
            // }
            // if data_length == 0 {
            //     Ok(&[])
            // } else if data_length > (*callback).data.len() as u32 {
            //     Err("Data length is too large")?;
            // }
            // Ok(&(*callback).data[0..data_length as usize])

            Ok(&mut (*(callback as *mut OperationCallback)).data
                [0..(*callback).data_length as usize])
        }
    }

    pub fn get_meta_data_ref(&self, id: u32) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            // if (*callback).status != CallbackState::WaitingForResponse {
            //     Err("Callback status not WaitingForResponse")?;
            // }
            // if meta_data_length == 0 {
            //     Ok(&[])
            // } else if meta_data_length > (*callback).meta_data.len() as u32 {
            //     Err("Data length is too large")?;
            // }
            // Ok(&(*callback).meta_data[0..meta_data_length as usize])

            Ok(&mut (*(callback as *mut OperationCallback)).meta_data
                [0..(*callback).meta_data_length as usize])
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

    pub fn timeout(&self, id: u32) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let callback = self.callbacks[id as usize];
            (*(callback as *mut OperationCallback)).state = CallbackState::Timeout;
        }
        self.clean_up()?;
        Ok(())
    }

    pub fn wait_for_callback(
        &self,
        id: u32,
    ) -> Result<*const OperationCallback, Box<dyn std::error::Error>> {
        unsafe {
            let callback = self.callbacks[id as usize];
            let result = (*(callback as *mut OperationCallback))
                .channel
                .1
                .recv_timeout(CLIENT_REQUEST_TIMEOUT);
            match result {
                Ok(_) => {
                    (*(callback as *mut OperationCallback)).state = CallbackState::Done;
                    Ok(callback)
                }
                Err(_) => {
                    (*(callback as *mut OperationCallback)).state = CallbackState::Timeout;
                    Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout",
                    )))
                }
            }
        }
    }
}

unsafe impl std::marker::Sync for CircularQueue {}
