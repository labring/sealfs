// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use lazy_static::lazy_static;
use libc::{__errno_location, iovec, O_CREAT, O_TRUNC, O_WRONLY};
use sealfs::common::serialization::OperationType;
use sealfs::rpc;
pub struct Client {
    // TODO replace with a thread safe data structure
    pub client: rpc::client::ClientAsync,
    pub inodes: DashMap<String, u64>,
    pub inodes_reverse: DashMap<u64, String>,
    runtime: tokio::runtime::Runtime,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            client: rpc::client::ClientAsync::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            client: rpc::client::ClientAsync::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn add_connection(&self, server_address: &str) {
        self.runtime
            .block_on(self.client.add_connection(server_address));
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.client.remove_connection(server_address);
    }

    pub fn get_connection_address(
        &self,
        _path: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // mock
        Ok("127.0.0.1:8085".to_string())
    }

    pub fn open_remote(&self, pathname: &str, flag: i32, mode: u32) -> Result<(), i32> {
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = match flag & O_CREAT {
            0 => {
                let mut data = Vec::with_capacity(4);
                data.extend_from_slice(&flag.to_le_bytes());
                data
            }
            _ => {
                let mut data = Vec::with_capacity(8);
                data.extend_from_slice(&flag.to_le_bytes());
                data.extend_from_slice(&mode.to_le_bytes());
                data
            }
        };

        // todo: Err -> errno
        if self
            .runtime
            .block_on(self.client.call_remote(
                &server_address,
                OperationType::OpenFile.into(),
                0,
                pathname,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            ))
            .is_err()
        {
            return Err(libc::EIO);
        }
        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn rename_remote(&self, _oldpath: &str, _newpath: &str) -> i32 {
        todo!()
    }

    pub fn truncate_remote(&self, _pathname: &str, _length: i64) -> i32 {
        todo!()
    }

    pub fn mkdir_remote(&self, _pathname: &str, _mode: u32) -> i32 {
        todo!();
    }

    pub fn rmdir_remote(&self, _pathname: &str) -> i32 {
        todo!();
    }

    pub fn getdents64_remote(&self, _pathname: &str, _dirp: &mut [u8]) -> isize {
        todo!();
    }

    pub fn unlink_remote(&self, _pathname: &str) -> i32 {
        todo!()
    }

    pub fn stat_remote(&self, _pathname: &str, _statbuf: &mut [u8]) -> i32 {
        todo!()
    }

    pub fn pread_remote(&self, _pathname: &str, _buf: &mut [u8], _offset: i64) -> isize {
        todo!()
    }

    pub fn preadv_remote(&self, _pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
        todo!()
    }

    pub fn pwrite_remote(&self, _pathname: &str, _buf: &[u8], _offset: i64) -> isize {
        todo!()
    }

    pub fn pwritev_remote(&self, _pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
        todo!()
    }

    pub fn getsize_remote(&self, _pathname: &str) -> i64 {
        todo!()
    }
}

lazy_static! {
    pub static ref CLIENT: Client = Client::new();
}
