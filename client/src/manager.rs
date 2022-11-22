// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::connection::{CircularQueue, Connection};
use crate::distribute_hash_table::hash;
use common::request::OperationType;
use dashmap::DashMap;
use fuser::{
    FileAttr, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use std::ffi::OsStr;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, UNIX_EPOCH};
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct Manager {
    // TODO replace with a thread safe data structure
    pub connections: DashMap<i32, Connection>,
    pub inodes: DashMap<String, FileAttr>,
    pub inodes_reverse: DashMap<u64, String>,

    pub queue: CircularQueue,
}

impl Default for Manager {
    fn default() -> Self {
        Self {
            connections: DashMap::new(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            queue: CircularQueue::new(),
        }
    }
}

impl Manager {
    pub fn new() -> Self {
        Self {
            connections: DashMap::new(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            queue: CircularQueue::new(),
        }
    }

    pub fn add_connection(&self, id: i32, connection: Connection) {
        self.connections.insert(id as i32, connection);
    }

    pub fn remove_connection(&self, id: i32) {
        self.connections.remove(&(id as i32));
    }

    pub fn get_connection_index(&self, path: &str) -> Option<i32> {
        let hash = hash(path);
        let index = hash % self.connections.len() as u64;
        Some(index as i32)
    }

    pub fn temp_init(&'static self, host: &str, port: u16) {
        let connection = Connection::new(host.to_string(), port);
        self.add_connection(0, connection);

        // spawn a thread to handle the connection using self.parse_response
        std::thread::spawn(|| self.parse_response());
    }

    // parse_response
    // try to get response from sequence of connections and write to callbacks
    pub fn parse_response(&self) {
        loop {}
    }

    pub fn lookup_remote(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        log::info!("lookup_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = format!(
                        "{}/{}",
                        self.inodes_reverse.get(&parent).unwrap().deref(),
                        name.to_str().unwrap()
                    );
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    // connection.send(
                    // &mut self,
                    // id: u32,
                    // operation_type: OperationType,
                    // flags: u32,
                    // filename: &str,
                    // meta_data: &[u8],
                    // data: &[u8],)
                    let send_result =
                        connection.send(id, OperationType::Lookup, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
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
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("lookup_remote end");
        //let result = connection.lookup(&path).await;
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
        flags: i32,
        reply: ReplyCreate,
    ) {
        log::info!("create_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = format!(
                        "{}/{}",
                        self.inodes_reverse.get(&parent).unwrap().deref(),
                        name.to_str().unwrap()
                    );
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result = connection.send(
                        id,
                        OperationType::CreateFile,
                        flags as u32,
                        &path,
                        &[],
                        &[],
                    );
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
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
                                    reply.created(&TTL, &file_attr, 0, 0, 0);
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("create_remote end");
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        log::info!("getattr_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&ino).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::GetFileAttr, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
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
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("getattr_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("getattr_remote error");
        }
    }

    pub fn readdir_remote(&self, ino: u64, _offset: i64, reply: ReplyDirectory) {
        log::info!("readdir_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&ino).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::ReadDir, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_data) => {
                                    reply.ok();
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("readdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("readdir_remote error");
        }
    }

    pub fn read_remote(&self, ino: u64, _offset: i64, _size: u32, reply: ReplyData) {
        log::info!("read_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&ino).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::ReadFile, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_data) => {
                                    reply.error(libc::EPERM);
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("read_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("read_remote error");
        }
    }

    pub fn write_remote(&self, ino: u64, _offset: i64, _data: &[u8], reply: ReplyWrite) {
        log::info!("write_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&ino).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::WriteFile, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_data) => {
                                    reply.error(libc::EPERM);
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("write_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("write_remote error");
        }
    }

    pub fn mkdir_remote(&self, parent: u64, _name: &OsStr, _mode: u32, reply: ReplyEntry) {
        log::info!("mkdir_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&parent).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::CreateDir, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_data) => {
                                    reply.error(libc::EPERM);
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("mkdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("mkdir_remote error");
        }
    }

    pub fn open_remote(&self, ino: u64, _flags: i32, reply: ReplyOpen) {
        log::info!("open_remote");
        if self.inodes_reverse.contains_key(&ino) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&ino).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::OpenFile, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_data) => {
                                    reply.error(libc::EPERM);
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("open_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("open_remote error");
        }
    }

    pub fn unlink_remote(&self, parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&parent).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::DeleteFile, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_) => {
                                    reply.ok();
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("unlink_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("unlink_remote error");
        }
    }

    pub fn rmdir_remote(&self, parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        log::info!("rmdir_remote");
        if self.inodes_reverse.contains_key(&parent) {
            let result = self.queue.register_callback();
            match result {
                Ok(id) => {
                    let path = self.inodes_reverse.get(&parent).unwrap();
                    let index = self.get_connection_index(&path).unwrap();
                    let connection = self.connections.get(&index).unwrap();
                    let send_result =
                        connection.send(id, OperationType::DeleteDir, 0, &path, &[], &[]);
                    match send_result {
                        Ok(_) => {
                            let result = self.queue.wait_for_callback(id);
                            match result {
                                Ok(_) => {
                                    reply.ok();
                                }
                                Err(_) => {
                                    reply.error(libc::EIO);
                                }
                            }
                        }
                        Err(_) => {
                            reply.error(libc::EIO);
                        }
                    }
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
            log::info!("rmdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("rmdir_remote error");
        }
    }
}

pub struct MANAGER {
    m: *mut Manager,
}

impl Deref for MANAGER {
    type Target = Manager;
    fn deref(&self) -> &Manager {
        unsafe { &*MANAGER::get() }
    }
}

impl MANAGER {
    pub fn get() -> *const Manager {
        unsafe {
            if MANAGER.m.is_null() {
                MANAGER.m = Box::into_raw(Box::new(Manager::new()));
                // initialize operation_callbacks
                MANAGER.m.as_mut().unwrap().queue.init();
            }
            MANAGER.m as *const MANAGER as *const Manager
        }
    }
}

impl DerefMut for MANAGER {
    fn deref_mut(&mut self) -> &mut Manager {
        unsafe { &mut *(MANAGER::get() as *mut Manager) }
    }
}

pub static mut MANAGER: MANAGER = MANAGER {
    m: std::ptr::null_mut(),
};
