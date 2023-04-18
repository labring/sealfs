// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::hash_ring::HashRing;
use crate::common::serialization::{
    ClusterStatus, CreateDirSendMetaData, CreateFileSendMetaData, FileAttrSimple,
    GetClusterStatusRecvMetaData, GetHashRingInfoRecvMetaData, ManagerOperationType,
    OpenFileSendMetaData, OperationType, ReadDirSendMetaData, ReadFileSendMetaData,
    WriteFileSendMetaData,
};
use crate::rpc;
use dashmap::DashMap;
use fuser::{
    FileAttr, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use lazy_static::lazy_static;
use libc::{mode_t, DT_DIR, DT_LNK, DT_REG};
use log::{debug, info};
use std::ffi::OsStr;
use std::ops::Deref;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct Client {
    // TODO replace with a thread safe data structure
    pub client: rpc::client::Client,
    pub inodes: DashMap<String, u64>,
    pub inodes_reverse: DashMap<u64, String>,
    pub inode_counter: std::sync::atomic::AtomicU64,
    pub fd_counter: std::sync::atomic::AtomicU64,
    pub handle: tokio::runtime::Handle,
    pub cluster_status: AtomicI32,
    pub hash_ring: Arc<RwLock<Option<HashRing>>>,
    pub new_hash_ring: Arc<RwLock<Option<HashRing>>>,
    pub manager_address: Arc<tokio::sync::Mutex<String>>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            client: rpc::client::Client::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            inode_counter: std::sync::atomic::AtomicU64::new(0),
            fd_counter: std::sync::atomic::AtomicU64::new(0),
            handle: tokio::runtime::Handle::current(),
            cluster_status: AtomicI32::new(ClusterStatus::Init.into()),
            hash_ring: Arc::new(RwLock::new(None)),
            new_hash_ring: Arc::new(RwLock::new(None)),
            manager_address: Arc::new(tokio::sync::Mutex::new("".to_string())),
        }
    }

    pub async fn add_connection(&self, server_address: &str) {
        loop {
            if self.client.add_connection(server_address).await {
                break;
            }
        }
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.client.remove_connection(server_address);
    }

    pub fn get_address(&self, path: &str) -> String {
        self.hash_ring
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    pub fn get_new_address(&self, path: &str) -> String {
        self.new_hash_ring
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    pub fn get_connection_address(&self, path: &str) -> String {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return self.get_address(path);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::SyncNewHashRing => self.get_address(path),
            ClusterStatus::PreTransfer => self.get_address(path),
            ClusterStatus::Transferring => self.get_address(path),
            ClusterStatus::PreFinish => self.get_new_address(path),
            s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }

    pub fn get_new_inode(&self) -> u64 {
        debug!(
            "get new inode, inode_counter: {}",
            self.inode_counter
                .load(std::sync::atomic::Ordering::Acquire)
        );
        self.inode_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    pub fn get_new_fd(&self) -> u64 {
        self.fd_counter
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    pub async fn init(&'static self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("temp init");

        self.inodes_reverse.insert(1, "/".to_string());
        self.inodes.insert("/".to_string(), 1);

        self.inode_counter
            .fetch_add(2, std::sync::atomic::Ordering::AcqRel);
        self.fd_counter
            .fetch_add(2, std::sync::atomic::Ordering::AcqRel);

        debug!("add connection");

        loop {
            match self
                .client
                .add_connection(&self.manager_address.lock().await)
                .await
            {
                true => {
                    break;
                }
                false => {
                    debug!("add connection failed, wait for a while");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        let result = async {
            loop {
                let result = self.get_cluster_status().await;
                match result {
                    Ok(status) => match status.try_into().unwrap() {
                        ClusterStatus::Idle => {
                            self.cluster_status.store(status, Ordering::Release);
                            return self.get_hash_ring_info().await;
                        }
                        ClusterStatus::Init => {
                            info!("cluster is initalling, wait for a while");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        ClusterStatus::PreFinish => {
                            info!("cluster is initalling, wait for a while");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        s => {
                            return Err(format!("invalid cluster status: {}", s).into());
                        }
                    },
                    Err(e) => return Err(e),
                }
            }
        }
        .await;

        match result {
            Ok(all_servers_address) => {
                for server_address in &all_servers_address {
                    self.add_connection(&server_address.0).await;
                }
                self.hash_ring
                    .write()
                    .unwrap()
                    .replace(HashRing::new(all_servers_address));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_cluster_status(&self) -> Result<i32, Box<dyn std::error::Error>> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 4];

        let result = self
            .client
            .call_remote(
                &self.manager_address.lock().await,
                ManagerOperationType::GetClusterStatus.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get cluster status failed: status {}", status).into());
                }
                let cluster_status_meta_data: GetClusterStatusRecvMetaData =
                    bincode::deserialize(&recv_meta_data).unwrap();
                Ok(cluster_status_meta_data.status as i32)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_hash_ring_info(
        &self,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                &self.manager_address.lock().await,
                ManagerOperationType::GetHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get hash ring failed: status {}", status).into());
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_new_hash_ring_info(
        &self,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                &self.manager_address.lock().await,
                ManagerOperationType::GetNewHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get new hash ring failed: status {}", status).into());
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_full_path(&self, parent: &str, name: &OsStr) -> String {
        if parent == "/" {
            return format!("/{}", name.to_str().unwrap());
        }
        let path = format!("{}/{}", parent, name.to_str().unwrap());
        path
    }

    pub fn lookup_remote(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        log::info!(
            "lookup_remote, parent: {}, name: {}",
            parent,
            name.to_str().unwrap()
        );
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                log::info!("lookup_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self.handle.block_on(self.client.call_remote(
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
        ));
        match result {
            Ok(_) => {
                debug!("lookup_remote status: {}", status);
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "lookup_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                let mut file_attr: FileAttr = {
                    let file_attr_simple: FileAttrSimple =
                        bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                    file_attr_simple.into()
                };

                if self.inodes.contains_key(&path) {
                    file_attr.ino = *self.inodes.get(&path).unwrap().value();
                } else {
                    file_attr.ino = self.get_new_inode();
                    self.inodes.insert(path.clone(), file_attr.ino);
                    self.inodes_reverse.insert(file_attr.ino, path.clone());
                }

                reply.entry(&TTL, &file_attr, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
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
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                log::info!("create_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let send_meta_data =
            bincode::serialize(&CreateFileSendMetaData { mode, umask, flags }).unwrap();

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::CreateFile.into(),
            0,
            &path,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        ));
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "create_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                let mut file_attr: FileAttr = {
                    let file_attr_simple: FileAttrSimple =
                        bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                    file_attr_simple.into()
                };

                file_attr.ino = self.get_new_inode();

                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path.clone());

                reply.created(&TTL, &file_attr, 0, 0, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        log::info!("getattr_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                log::info!("getattr_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self.handle.block_on(self.client.call_remote(
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
        ));
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "getattr_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                let mut file_attr: FileAttr = {
                    let file_attr_simple: FileAttrSimple =
                        bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                    file_attr_simple.into()
                };
                debug!("getattr_remote file_attr: {:?}", file_attr);
                if self.inodes.contains_key(&path) {
                    file_attr.ino = *self.inodes.get(&path).unwrap().value();
                } else {
                    file_attr.ino = self.get_new_inode();
                    self.inodes.insert(path.clone(), file_attr.ino);
                    self.inodes_reverse.insert(file_attr.ino, path.clone());
                }
                reply.attr(&TTL, &file_attr);
                debug!("getattr_remote success");
            }
            Err(_) => {
                debug!("getattr_remote error");
                reply.error(libc::EIO);
            }
        }
    }

    pub fn readdir_remote(&self, ino: u64, offset: i64, mut reply: ReplyDirectory) {
        log::info!("readdir_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                log::info!("readdir_remote error");
                return;
            }
        };
        let size = 1024;

        let server_address = self.get_connection_address(&path);
        let md = ReadDirSendMetaData {
            offset,
            size: size as u32,
        };
        let send_meta_data = bincode::serialize(&md).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_data = vec![0u8; size];

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::ReadDir.into(),
            0,
            &path,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut recv_data,
        ));
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "readdir_remote recv_data: {:?}",
                    &recv_data[..recv_data_length]
                );
                let mut total = 0;
                let mut offset = offset;
                while total < recv_data_length {
                    let r#type = u8::from_le_bytes(recv_data[total..total + 1].try_into().unwrap());
                    let name_len =
                        u16::from_le_bytes(recv_data[total + 1..total + 3].try_into().unwrap());
                    let name = String::from_utf8(
                        recv_data[total + 3..total + 3 + name_len as usize]
                            .try_into()
                            .unwrap(),
                    )
                    .unwrap();
                    let kind = match r#type {
                        DT_REG => fuser::FileType::RegularFile,
                        DT_DIR => fuser::FileType::Directory,
                        DT_LNK => fuser::FileType::Symlink,
                        _ => fuser::FileType::RegularFile,
                    };
                    offset += 1;
                    let r = reply.add(1, offset, kind, name);
                    if r {
                        break;
                    }

                    total += 3 + name_len as usize;
                }

                reply.ok();
                debug!("readdir_remote success");
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn read_remote(&self, ino: u64, offset: i64, size: u32, reply: ReplyData) {
        log::info!("read_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                log::info!("read_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);

        let meta_data = bincode::serialize(&ReadFileSendMetaData { offset, size }).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_data = vec![0u8; size as usize];

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::ReadFile.into(),
            0,
            &path,
            &meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut recv_data,
        ));
        match result {
            Ok(()) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "read_remote success recv_data: {:?}",
                    &recv_data[..recv_data_length]
                );
                reply.data(&recv_data);
            }
            Err(e) => {
                debug!("read_remote error: {:?}", e);
                reply.error(libc::EIO);
            }
        }
    }

    pub fn write_remote(&self, ino: u64, offset: i64, data: &[u8], reply: ReplyWrite) {
        log::info!("write_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                log::info!("write_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let send_meta_data = bincode::serialize(&WriteFileSendMetaData { offset }).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 4];

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::WriteFile.into(),
            0,
            &path,
            &send_meta_data,
            data,
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        ));
        match result {
            Ok(()) => {
                let size: u32 =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                debug!("write_remote success, size: {}", size);
                reply.written(size);
            }
            Err(_) => {
                debug!("write_remote error");
                reply.error(libc::EIO);
            }
        }
    }

    pub fn mkdir_remote(&self, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        log::info!("mkdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                log::info!("mkdir_remote error");
                return;
            }
        };
        debug!("mkdir_remote ,path: {:?}", &path);
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let mode: mode_t = 0o755;
        let send_meta_data = bincode::serialize(&CreateDirSendMetaData { mode }).unwrap();

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::CreateDir.into(),
            0,
            &path,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        ));
        match result {
            Ok(_) => {
                if status != 0 {
                    reply.error(status);
                    return;
                }
                debug!(
                    "create_remote recv_meta_data: {:?}",
                    &recv_meta_data[..recv_meta_data_length]
                );
                let mut file_attr: FileAttr = {
                    let file_attr_simple: FileAttrSimple =
                        bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                    file_attr_simple.into()
                };

                file_attr.ino = self.get_new_inode();

                reply.entry(&TTL, &file_attr, 0);

                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path.clone());
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn open_remote(&self, ino: u64, flags: i32, reply: ReplyOpen) {
        log::info!("open_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                log::info!("open_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mode: mode_t = if flags & libc::O_WRONLY != 0 {
            libc::S_IWUSR
        } else if flags & libc::O_RDWR != 0 {
            libc::S_IWUSR | libc::S_IRUSR
        } else {
            libc::S_IRUSR
        };

        let send_meta_data = bincode::serialize(&OpenFileSendMetaData { flags, mode }).unwrap();

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::OpenFile.into(),
            0,
            &path,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut [],
        ));
        match result {
            Ok(()) => {
                reply.opened(self.get_new_fd(), 0);
            }
            Err(e) => {
                debug!("open_remote error: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    pub fn unlink_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("unlink_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                log::info!("unlink_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self.handle.block_on(self.client.call_remote(
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
        ));
        match result {
            Ok(_) => {
                reply.ok();
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn rmdir_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        log::info!("rmdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                log::info!("rmdir_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self.handle.block_on(self.client.call_remote(
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
        ));
        match result {
            Ok(_) => {
                reply.ok();
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }
}

lazy_static! {
    pub static ref CLIENT: Client = Client::new();
}
