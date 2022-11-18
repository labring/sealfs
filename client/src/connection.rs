// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use common::request::{
    OperationType, CLIENT_REQUEST_TIMEOUT, CLIENT_RESPONSE_TIMEOUT, REQUEST_QUEUE_LENGTH,
    RESPONSE_HEADER_SIZE,
};
use fuser::{FileAttr, FileType};
use libc::{EIO, EPERM};
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{mpsc, Arc, Mutex},
    time::{self, UNIX_EPOCH},
};

pub enum CallbackState {
    Empty = 0,
    WaitingForResponse = 1,
    Done = 2,
    TIMEOUT = 3,
}

pub struct OperationCallback {
    pub data: Vec<u8>,
    pub meta_data: Vec<u8>,
    pub data_length: u32,
    pub meta_data_length: u32,
    pub status: CallbackState,
    pub channel: (mpsc::Sender<()>, mpsc::Receiver<()>),
    pub timeout: time::SystemTime,
}

unsafe impl std::marker::Sync for OperationCallback {}

enum ConnectionStatus {
    Connected,
    Disconnected,
}

pub struct Connection {
    // ...
    // Path: client/src/connection/connection.rs
    pub host: String,
    pub port: u16,
    status: ConnectionStatus,

    callbacks: Vec<OperationCallback>,
    start_index: Arc<Mutex<u32>>, // maybe we can use a lock-free queue
    end_index: Arc<Mutex<u32>>,

    stream: TcpStream,
    send_lock: Mutex<()>,
    // ...
}

impl Connection {
    // ...
    // Path: client/src/connection/connection.rs
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host: host.clone(),
            port,
            status: ConnectionStatus::Disconnected,
            callbacks: Vec::new(),
            start_index: Arc::new(Mutex::new(0)),
            end_index: Arc::new(Mutex::new(0)),
            stream: TcpStream::connect(format!("{}:{}", host, port)).unwrap(),
            send_lock: Mutex::new(()),
        }
    }

    pub fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.status = ConnectionStatus::Connected;
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.status = ConnectionStatus::Disconnected;
        Ok(())
    }

    pub fn send_request(
        &mut self,
        header: &[u8],
        data: &[u8],
        meta_data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let lock = self.send_lock.lock().unwrap();
        let _ = self.stream.write_all(header);
        let _ = self.stream.write_all(data);
        let _ = self.stream.write_all(meta_data);
        drop(lock);
        Ok(())
    }

    pub fn parse_response(&mut self) {
        let mut buffer = [0; RESPONSE_HEADER_SIZE];
        loop {
            // wake up time out requests
            let now = time::SystemTime::now();
            while *self.start_index.lock().unwrap() != *self.end_index.lock().unwrap()
                && now > self.callbacks[*self.start_index.lock().unwrap() as usize].timeout
            {
                let index = *self.start_index.lock().unwrap();
                self.callbacks[index as usize].status = CallbackState::TIMEOUT;
                self.callbacks[index as usize].channel.0.send(()).unwrap();
                *self.start_index.lock().unwrap() = (index + 1) % REQUEST_QUEUE_LENGTH as u32;
            }

            // read response header
            let _ = self.stream.read(&mut buffer);
            let id = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            let _status = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
            let _flags = u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
            let total_length = u32::from_le_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]);

            // read response data

            // read response meta data

            // wake up the request
            let mut callback = &mut self.callbacks[id as usize];
            callback.status = CallbackState::Done;
            callback.data_length = total_length - callback.meta_data_length;
            callback.data.resize(callback.data_length as usize, 0);
            let _ = self.stream.read_exact(&mut callback.data);
            let _ = callback.channel.0.send(());
        }
    }

    pub fn init(&mut self) {
        // ...
        // Path: client/src/connection/connection.rs
        for _ in 0..REQUEST_QUEUE_LENGTH {
            self.callbacks.push(OperationCallback {
                data: Vec::new(),
                meta_data: Vec::new(),
                data_length: 0,
                meta_data_length: 0,
                status: CallbackState::Empty,
                channel: mpsc::channel(),
                timeout: UNIX_EPOCH,
            });
        }
        self.status = ConnectionStatus::Connected;

        // set read timeout, so that we can clean up outdated requests quickly
        self.stream
            .set_read_timeout(Some(CLIENT_RESPONSE_TIMEOUT))
            .unwrap();

        // TODO start a thread to handle responses
    }

    pub fn get_callback(&mut self) -> Option<u32> {
        let mut end_index = self.end_index.lock().unwrap();
        let id = *end_index;
        *end_index = (*end_index + 1) % REQUEST_QUEUE_LENGTH as u32;
        Some(id)
    }

    pub fn lookup(&mut self, _path: &str) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("lookup");
                let id = self.get_callback();
                if id.is_none() {
                    return Err(EIO);
                }
                let id = id.unwrap();
                //put id, OperationType::Lookup, 0, 4 into header buffer;
                let mut header = vec![0u8; 16];
                header[0..4].copy_from_slice(&id.to_le_bytes());
                header[4..8].copy_from_slice(&OperationType::Lookup.to_le_bytes());
                header[8..12].copy_from_slice(&0u32.to_le_bytes());
                header[12..16].copy_from_slice(&4u32.to_le_bytes());
                //TODO put path into data buffer;

                //send header buffer and data buffer;
                let _ = self.send_request(&header, &[], &[]);

                let mut callback = &mut self.callbacks[id as usize];

                callback.timeout = time::SystemTime::now() + CLIENT_REQUEST_TIMEOUT;

                //wait for response;
                callback.status = CallbackState::WaitingForResponse;
                let _ = callback.channel.1.recv();
                callback.status = CallbackState::Done;

                //TODO parse response;
                let attr = FileAttr {
                    ino: 0,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: FileType::RegularFile,
                    perm: 0o777,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    flags: 0,
                    blksize: 0,
                };
                Ok(attr)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn create(
        &self,
        _path: &str,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("create");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn getattr(&self, _path: &str) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => Err(EPERM),
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn read(&self, _path: &str, _offset: i64, _size: u32) -> Result<Vec<u8>, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("read");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn readdir(&self, _path: &str) -> Result<Vec<(u64, FileType, String)>, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("readdir");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn write(&self, _path: &str, _offset: i64, _data: &[u8]) -> Result<u32, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("write");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn mkdir(&self, _path: &str, _mode: u32) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("mkdir");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn open(&self, _path: &str, _flags: i32) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("open");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn unlink(&self, _path: &str) -> Result<(), i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("unlink");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    pub fn rmdir(&self, _path: &str) -> Result<(), i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("rmdir");
                Err(EPERM)
            }
            ConnectionStatus::Disconnected => Err(EIO),
        }
    }

    // ...
}
