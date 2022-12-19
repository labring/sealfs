// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use common::fuse::Node;
use common::request::OperationType;
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

    pub fn add_connection(&self, id: i32, server_address: &str) {
        self.client.add_connection(id, server_address);
    }

    pub fn remove_connection(&self, id: i32) {
        self.client.remove_connection(id);
    }

    pub fn get_connection_index(&self, _path: &str) -> Option<i32> {
        // mock
        Some(0)
    }

    pub fn temp_init(
        &'static self,
        server_address: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        debug!("temp init");
        debug!("add connection");
        self.add_connection(0, server_address);

        self.inodes_reverse.insert(1, "/".to_string());
        self.inodes.insert(
            "/".to_string(),
            Node::new(
                1,
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
        if self.inodes_reverse.contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.get(&parent).unwrap().deref(),
                name.to_str().unwrap()
            );
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::Lookup.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
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
        } else {
            reply.error(libc::ENOENT);
            log::info!("lookup_remote error");
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
        if self.inodes_reverse.contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.get(&parent).unwrap().deref(),
                name.to_str().unwrap()
            );
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::CreateFile.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
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
        } else {
            reply.error(libc::ENOENT);
            log::info!("create_remote error");
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        log::info!("getattr_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let path = self.inodes_reverse.get(&ino).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::GetFileAttr.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
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

                    reply.attr(&TTL, &file_attr);
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("getattr_remote error");
        }
    }

    pub fn readdir_remote(&self, ino: u64, _offset: i64, reply: ReplyDirectory) {
        log::info!("readdir_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let path = self.inodes_reverse.get(&ino).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::ReadDir.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    let mut _offset = 0;
                    // let mut entries = Vec::new();
                    // while offset < data.len() {
                    //     let entry = DirectoryEntry::from_bytes(&data[offset..]);
                    //     offset += entry.size();
                    //     entries.push(entry);
                    // }

                    // for entry in entries {
                    //     reply.add(entry.ino, entry.offset, entry.kind, entry.name);
                    // }

                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("readdir_remote error");
        }
    }

    pub fn read_remote(&self, ino: u64, _offset: i64, _size: u32, reply: ReplyData) {
        log::info!("read_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let path = self.inodes_reverse.get(&ino).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::ReadFile.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    reply.data(&[]);
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("read_remote error");
        }
    }

    pub fn write_remote(&self, ino: u64, _offset: i64, _data: &[u8], reply: ReplyWrite) {
        log::info!("write_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let path = self.inodes_reverse.get(&ino).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::WriteFile.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    reply.written(0);
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("write_remote error");
        }
    }

    pub fn mkdir_remote(&self, parent: u64, _name: &OsStr, _mode: u32, reply: ReplyEntry) {
        log::info!("mkdir_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let path = self.inodes_reverse.get(&parent).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::CreateDir.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
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
        } else {
            reply.error(libc::ENOENT);
            log::info!("mkdir_remote error");
        }
    }

    pub fn open_remote(&self, ino: u64, _flags: i32, reply: ReplyOpen) {
        log::info!("open_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let path = self.inodes_reverse.get(&ino).unwrap();
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::OpenFile.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    reply.opened(0, 0);
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("open_remote error");
        }
    }

    pub fn unlink_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.get(&parent).unwrap().deref(),
                name.to_str().unwrap()
            );
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::DeleteFile.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("unlink_remote error");
        }
    }

    pub fn rmdir_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("rmdir_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.get(&parent).unwrap().deref(),
                name.to_str().unwrap()
            );
            let connection_index = self.get_connection_index(&path).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;

            let result = self.client.call_remote(
                connection_index,
                OperationType::DeleteDir.into(),
                0,
                &path,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut [],
                &mut [],
            );
            match result {
                Ok(_data) => {
                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        } else {
            reply.error(libc::ENOENT);
            log::info!("rmdir_remote error");
        }
    }
}

// pub struct MANAGER {
//     m: *mut Client,
// }

// impl Deref for MANAGER {
//     type Target = Client;
//     fn deref(&self) -> &Client {
//         unsafe { &*MANAGER::get() }
//     }
// }

// impl MANAGER {
//     pub fn get() -> *const Client {
//         unsafe {
//             info!("MANAGER::get");
//             if MANAGER.m.is_null() {
//                 info!("MANAGER::get is null");
//                 MANAGER.m = Box::into_raw(Box::new(Client::new()));
//                 info!("MANAGER::get is null end");
//                 // initialize operation_callbacks
//                 MANAGER.m.as_mut().unwrap().queue.init();
//                 info!("MANAGER::get is null end 2");
//             }
//             MANAGER.m as *const Client
//         }
//     }
// }

// impl DerefMut for MANAGER {
//     fn deref_mut(&mut self) -> &mut Client {
//         unsafe { &mut *(MANAGER::get() as *mut Client) }
//     }
// }

// pub static mut MANAGER: MANAGER = MANAGER {
//     m: std::ptr::null_mut(),
// };

lazy_static! {
    pub static ref MANAGER: Client = Client::new();
}
