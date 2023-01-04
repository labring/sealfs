// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;

use fuser::{FileAttr, FileType};
use libc::{EIO, EPERM};

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
    pub lock: std::sync::Arc<Mutex<()>>,
    // ...
}

impl Connection {
    // ...
    // Path: client/src/connection/connection.rs
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            status: ConnectionStatus::Disconnected,
            lock: std::sync::Arc::new(Mutex::new(())),
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let _lock = self.lock.lock();
        self.status = ConnectionStatus::Connected;
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let _lock = self.lock.lock();
        self.status = ConnectionStatus::Disconnected;
        Ok(())
    }

    pub fn lookup(&self, _path: &str) -> Result<FileAttr, i32> {
        match self.status {
            ConnectionStatus::Connected => {
                log::info!("lookup");
                Err(EPERM)
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
