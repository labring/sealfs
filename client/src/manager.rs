// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::connection::Connection;
use common::distribute_hash_table::hash;
use fuser::{
    FileAttr, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use lazy_static::lazy_static;
use std::ffi::OsStr;
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct Manager {
    // TODO replace with a thread safe data structure
    pub connections: Arc<RwLock<HashMap<i32, Connection>>>,
    pub inodes: Arc<Mutex<HashMap<String, FileAttr>>>,
    pub inodes_reverse: Arc<Mutex<HashMap<u64, String>>>,
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

impl Manager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            inodes: Arc::new(Mutex::new(HashMap::new())),
            inodes_reverse: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_connection(&self, id: i32, connection: Connection) {
        self.connections
            .write()
            .unwrap()
            .insert(id as i32, connection);
    }

    pub fn remove_connection(&self, id: i32) {
        self.connections.write().unwrap().remove(&(id as i32));
    }

    pub fn get_connection_index(&self, path: &str) -> Option<i32> {
        let hash = hash(path);
        let index = hash % self.connections.read().unwrap().len() as u64;
        Some(index as i32)
    }

    pub fn lookup_remote(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        log::info!("lookup_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.lock().unwrap().get(&parent).unwrap(),
                name.to_str().unwrap()
            );
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .lookup(&path);
            //let result = connection.lookup(&path).await;
            match result {
                Ok(attr) => {
                    let inode = attr.ino;
                    self.inodes.lock().unwrap().insert(path.clone(), attr);
                    self.inodes_reverse.lock().unwrap().insert(inode, path);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("lookup_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("lookup_remote error");
        }
    }

    pub fn create_remote(
        &self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        log::info!("create_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.lock().unwrap().get(&parent).unwrap(),
                name.to_str().unwrap()
            );
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .create(&path, mode, umask, flags);
            match result {
                Ok(attr) => {
                    let inode = attr.ino;
                    self.inodes.lock().unwrap().insert(path.clone(), attr);
                    self.inodes_reverse.lock().unwrap().insert(inode, path);
                    reply.created(&TTL, &attr, 0, 0, 0);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("create_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("create_remote error");
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        log::info!("getattr_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&ino) {
            let path: String = self
                .inodes_reverse
                .lock()
                .unwrap()
                .get(&ino)
                .unwrap()
                .to_string();
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .getattr(&path);
            match result {
                Ok(attr) => {
                    reply.attr(&TTL, &attr);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("getattr_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("getattr_remote error");
        }
    }

    pub fn readdir_remote(&self, ino: u64, _offset: i64, mut reply: ReplyDirectory) {
        log::info!("readdir_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&ino) {
            let path: String = self
                .inodes_reverse
                .lock()
                .unwrap()
                .get(&ino)
                .unwrap()
                .to_string();
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .readdir(&path);
            match result {
                Ok(entries) => {
                    for entry in entries {
                        if reply.add(entry.0, 0, entry.1, entry.2) {
                            break;
                        }
                    }
                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("readdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("readdir_remote error");
        }
    }

    pub fn read_remote(&self, ino: u64, offset: i64, size: u32, reply: ReplyData) {
        log::info!("read_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&ino) {
            let path: String = self
                .inodes_reverse
                .lock()
                .unwrap()
                .get(&ino)
                .unwrap()
                .to_string();
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .read(&path, offset, size);
            match result {
                Ok(data) => {
                    reply.data(&data);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("read_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("read_remote error");
        }
    }

    pub fn write_remote(&self, ino: u64, offset: i64, data: &[u8], reply: ReplyWrite) {
        log::info!("write_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&ino) {
            let path: String = self
                .inodes_reverse
                .lock()
                .unwrap()
                .get(&ino)
                .unwrap()
                .to_string();
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .write(&path, offset, data);
            match result {
                Ok(size) => {
                    reply.written(size);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("write_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("write_remote error");
        }
    }

    pub fn mkdir_remote(&self, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        log::info!("mkdir_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.lock().unwrap().get(&parent).unwrap(),
                name.to_str().unwrap()
            );
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .mkdir(&path, mode);
            match result {
                Ok(attr) => {
                    let inode = attr.ino;
                    self.inodes.lock().unwrap().insert(path.clone(), attr);
                    self.inodes_reverse.lock().unwrap().insert(inode, path);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("mkdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("mkdir_remote error");
        }
    }

    pub fn open_remote(&self, ino: u64, flags: i32, reply: ReplyOpen) {
        log::info!("open_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&ino) {
            let path: String = self
                .inodes_reverse
                .lock()
                .unwrap()
                .get(&ino)
                .unwrap()
                .to_string();
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .open(&path, flags);
            match result {
                Ok(fh) => {
                    reply.opened(fh.ino, 0);
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("open_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("open_remote error");
        }
    }

    pub fn unlink_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.lock().unwrap().get(&parent).unwrap(),
                name.to_str().unwrap()
            );
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .unlink(&path);
            match result {
                Ok(_) => {
                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("unlink_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("unlink_remote error");
        }
    }

    pub fn rmdir_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("rmdir_remote");
        if self.inodes_reverse.lock().unwrap().contains_key(&parent) {
            let path = format!(
                "{}/{}",
                self.inodes_reverse.lock().unwrap().get(&parent).unwrap(),
                name.to_str().unwrap()
            );
            let index = self.get_connection_index(&path).unwrap();
            let result = self
                .connections
                .read()
                .unwrap()
                .get(&(index as i32))
                .unwrap()
                .rmdir(&path);
            match result {
                Ok(_) => {
                    reply.ok();
                }
                Err(_) => {
                    reply.error(libc::ENOENT);
                }
            }
            log::info!("rmdir_remote end");
        } else {
            reply.error(libc::ENOENT);
            log::info!("rmdir_remote error");
        }
    }
}

lazy_static! {
    pub static ref CLIENT: Manager = Manager::new();
}
