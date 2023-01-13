// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::fuse::{Node, SubDirectory};
use crate::common::request::OperationType;
use crate::rpc;
use dashmap::DashMap;
use fuser::{
    FileAttr, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use lazy_static::lazy_static;
use log::debug;
use std::ffi::OsStr;
use std::ops::Deref;
use std::time::{Duration, UNIX_EPOCH};
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct Client {
    // TODO replace with a thread safe data structure
    pub client: rpc::client::Client,
    pub inodes: DashMap<String, Node>,
    pub inodes_reverse: DashMap<u64, String>,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            client: rpc::client::Client::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
        }
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            client: rpc::client::Client::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
        }
    }

    pub fn add_connection(&self, server_address: &str) {
        self.client.add_connection(server_address);
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

    pub fn temp_init(
        &'static self,
        server_address: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("temp init");
        debug!("add connection");
        self.add_connection(server_address);

        self.inodes_reverse.insert(1, "/".to_string());
        self.inodes.insert(
            "/".to_string(),
            Node::new(
                "/".to_string(),
                "/".to_string(),
                FileAttr {
                    ino: 1,
                    size: 0,
                    blocks: 0,
                    atime: UNIX_EPOCH,
                    mtime: UNIX_EPOCH,
                    ctime: UNIX_EPOCH,
                    crtime: UNIX_EPOCH,
                    kind: fuser::FileType::Directory,
                    perm: 0o755,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    flags: 0,
                    blksize: 0,
                },
            ),
        );

        debug!("spawn parse thread");
        // spawn a thread to handle the connection using self.parse_response
        std::thread::spawn(|| self.client.parse_response());
        Ok(())
    }

    pub fn lookup_remote(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        log::info!("lookup_remote");
        match self.inodes_reverse.get(&parent) {
            Some(parent_path) => {
                let path = format!("{}/{}", parent_path.deref(), name.to_str().unwrap());
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::Lookup.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        let file_attr = FileAttr {
                            ino: 0,
                            size: 0,
                            blocks: 0,
                            atime: UNIX_EPOCH,
                            mtime: UNIX_EPOCH,
                            ctime: UNIX_EPOCH,
                            crtime: UNIX_EPOCH,
                            kind: fuser::FileType::RegularFile,
                            perm: 0,
                            nlink: 0,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            flags: 0,
                            blksize: 0,
                        };

                        reply.entry(&TTL, &file_attr, 0);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("lookup_remote error");
            }
        }
    }

    pub fn create_remote(
        &self,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        log::info!("create_remote");
        match self.inodes_reverse.get(&parent) {
            Some(parent_path) => {
                let path = format!("{}/{}", parent_path.deref(), name.to_str().unwrap());
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let mut recv_meta_data = vec![0u8; 1024];

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::CreateFile.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        let file_attr = FileAttr {
                            ino: 0,
                            size: 0,
                            blocks: 0,
                            atime: UNIX_EPOCH,
                            mtime: UNIX_EPOCH,
                            ctime: UNIX_EPOCH,
                            crtime: UNIX_EPOCH,
                            kind: fuser::FileType::RegularFile,
                            perm: 0,
                            nlink: 0,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            flags: 0,
                            blksize: 0,
                        };

                        reply.created(&TTL, &file_attr, 0, 0, rsp_flags);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("create_remote error");
            }
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        log::info!("getattr_remote");
        match self.inodes_reverse.get(&ino) {
            Some(path) => {
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let mut recv_meta_data = vec![0u8; 1024];

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::GetFileAttr.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        debug!("getattr_remote recv_meta_data: {:?}", &recv_meta_data[..1]);
                        let kind = {
                            match recv_meta_data[0] {
                                b'f' => fuser::FileType::RegularFile,
                                b'd' => fuser::FileType::Directory,
                                b's' => fuser::FileType::Symlink,
                                _ => fuser::FileType::RegularFile,
                            }
                        };
                        let file_attr = FileAttr {
                            ino,
                            size: 0,
                            blocks: 0,
                            atime: UNIX_EPOCH,
                            mtime: UNIX_EPOCH,
                            ctime: UNIX_EPOCH,
                            crtime: UNIX_EPOCH,
                            kind,
                            perm: 0,
                            nlink: 0,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            flags: 0,
                            blksize: 0,
                        };
                        debug!("getattr_remote file_attr: {:?}", file_attr);
                        reply.attr(&TTL, &file_attr);
                        debug!("getattr_remote reply.attr");
                        self.inodes.insert(
                            path.clone(),
                            Node::new(path.clone(), path.clone(), file_attr),
                        );
                        self.inodes_reverse.insert(ino, path.clone());
                        debug!("getattr_remote success");
                    }
                    Err(_) => {
                        debug!("getattr_remote error");
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("getattr_remote error");
            }
        }
    }

    pub fn readdir_remote(&self, ino: u64, offset: i64, mut reply: ReplyDirectory) {
        log::info!("readdir_remote");
        if offset > 0 {
            // I'm not sure how to tell the user that there are no more entries. This is a temporary solution.
            reply.ok();
            return;
        }
        match self.inodes_reverse.get(&ino) {
            Some(path) => {
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let mut recv_data = vec![0u8; 1024];

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::ReadDir.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut recv_data,
                );
                match result {
                    Ok(_) => {
                        debug!(
                            "readdir_remote recv_data: {:?}",
                            &recv_data[..recv_data_length]
                        );
                        let entries: SubDirectory =
                            bincode::deserialize(&recv_data[..recv_data_length]).unwrap();
                        for entry in entries.sub_dir {
                            let kind = {
                                match entry.1.as_bytes()[0] {
                                    b'f' => fuser::FileType::RegularFile,
                                    b'd' => fuser::FileType::Directory,
                                    b's' => fuser::FileType::Symlink,
                                    _ => fuser::FileType::RegularFile,
                                }
                            };
                            debug!("readdir_remote entry: {:?}", entry);
                            let r = reply.add(1, 2, kind, entry.0);
                            if r {
                                break;
                            }
                        }

                        reply.ok();
                        debug!("readdir_remote success");
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("readdir_remote error");
            }
        }
    }

    pub fn read_remote(&self, ino: u64, _offset: i64, _size: u32, reply: ReplyData) {
        log::info!("read_remote");
        match self.inodes_reverse.get(&ino) {
            Some(path) => {
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::ReadFile.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        reply.data(&[]);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("read_remote error");
            }
        }
    }

    pub fn write_remote(&self, ino: u64, _offset: i64, _data: &[u8], reply: ReplyWrite) {
        log::info!("write_remote");
        match self.inodes_reverse.get(&ino) {
            Some(path) => {
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::WriteFile.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        reply.written(0);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("write_remote error");
            }
        }
    }

    pub fn mkdir_remote(&self, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        log::info!("mkdir_remote");
        match self.inodes_reverse.get(&parent) {
            Some(parent_path) => {
                let path = format!("{}/{}", parent_path.deref(), name.to_str().unwrap(),);
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::CreateDir.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        let file_attr = FileAttr {
                            ino: 0,
                            size: 0,
                            blocks: 0,
                            atime: UNIX_EPOCH,
                            mtime: UNIX_EPOCH,
                            ctime: UNIX_EPOCH,
                            crtime: UNIX_EPOCH,
                            kind: fuser::FileType::RegularFile,
                            perm: 0,
                            nlink: 0,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            flags: 0,
                            blksize: 0,
                        };

                        reply.entry(&TTL, &file_attr, 0);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("mkdir_remote error");
            }
        }
    }

    pub fn open_remote(&self, ino: u64, _flags: i32, reply: ReplyOpen) {
        log::info!("open_remote");
        match self.inodes_reverse.get(&ino) {
            Some(path) => {
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::OpenFile.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        reply.opened(0, 0);
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("open_remote error");
            }
        }
    }

    pub fn unlink_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink_remote");
        match self.inodes_reverse.get(&parent) {
            Some(parent_path) => {
                let path = format!("{}/{}", parent_path.deref(), name.to_str().unwrap());
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::DeleteFile.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        reply.ok();
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("unlink_remote error");
            }
        }
    }

    pub fn rmdir_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("rmdir_remote");
        match self.inodes_reverse.get(&parent) {
            Some(parent_path) => {
                let path = format!("{}/{}", parent_path.deref(), name.to_str().unwrap());
                let server_address = self.get_connection_address(&path).unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let result = self.client.call_remote(
                    &server_address,
                    OperationType::DeleteDir.into(),
                    0,
                    &path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                );
                match result {
                    Ok(_) => {
                        reply.ok();
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
                log::info!("rmdir_remote error");
            }
        }
    }
}

lazy_static! {
    pub static ref CLIENT: Client = Client::new();
}
