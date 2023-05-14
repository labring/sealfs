// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::hash_ring::HashRing;
use crate::common::sender;
use crate::common::serialization::{
    ClusterStatus, CreateDirSendMetaData, CreateFileSendMetaData, DeleteDirSendMetaData,
    DeleteFileSendMetaData, FileAttrSimple, OpenFileSendMetaData, OperationType,
    ReadDirSendMetaData, ReadFileSendMetaData, WriteFileSendMetaData,
};
use crate::rpc;
use dashmap::DashMap;
use fuser::{
    FileAttr, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite,
};
use lazy_static::lazy_static;
use libc::{mode_t, DT_DIR, DT_LNK, DT_REG};
use log::{debug, info, warn};
use spin::RwLock;
use std::ffi::OsStr;
use std::ops::Deref;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
const TTL: Duration = Duration::from_secs(1); // 1 second

pub struct Client {
    // TODO replace with a thread safe data structure
    pub client: Arc<rpc::client::Client>,
    pub volume_name: RwLock<Option<String>>,
    pub sender: Arc<sender::Sender>,
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
        let client = Arc::new(rpc::client::Client::default());
        Self {
            client: client.clone(),
            volume_name: RwLock::new(None),
            sender: Arc::new(sender::Sender::new(client)),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            inode_counter: std::sync::atomic::AtomicU64::new(0),
            fd_counter: std::sync::atomic::AtomicU64::new(0),
            handle: tokio::runtime::Handle::current(),
            cluster_status: AtomicI32::new(ClusterStatus::Initializing.into()),
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
        let volume_name = self.volume_name.read().clone();
        if let Some(volume_name) = volume_name {
            match self.sender.init_volume(server_address, &volume_name).await {
                Ok(_) => {
                    info!("init volume {} success", volume_name);
                }
                Err(e) => {
                    panic!("init volume {} failed, error: {}", volume_name, e);
                }
            }
        }
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.client.remove_connection(server_address);
    }

    pub fn get_address(&self, path: &str) -> String {
        self.hash_ring
            .read()
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    pub fn get_new_address(&self, path: &str) -> String {
        match self.new_hash_ring.read().as_ref() {
            Some(hash_ring) => hash_ring.get(path).unwrap().address.clone(),
            None => self.get_address(path),
        }
    }

    pub fn get_connection_address(&self, path: &str) -> String {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return self.get_address(path);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::Initializing => panic!("cluster status is not ready"),
            ClusterStatus::Idle => todo!(),
            ClusterStatus::NodesStarting => self.get_address(path),
            ClusterStatus::SyncNewHashRing => self.get_address(path),
            ClusterStatus::PreTransfer => self.get_address(path),
            ClusterStatus::Transferring => self.get_address(path),
            ClusterStatus::PreFinish => self.get_new_address(path),
            ClusterStatus::Finishing => self.get_address(path),
            ClusterStatus::StatusError => todo!(),
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

    pub async fn sync_cluster_infos(&self) {
        loop {
            {
                let result = self.get_cluster_status().await;
                match result {
                    Ok(status) => {
                        if self.cluster_status.load(Ordering::Relaxed) != status {
                            self.cluster_status.store(status, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        info!("sync server infos failed, error = {}", e);
                    }
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn watch_status(&self) {
        loop {
            match self
                .cluster_status
                .load(Ordering::Relaxed)
                .try_into()
                .unwrap()
            {
                ClusterStatus::SyncNewHashRing => {
                    // here we write a long code block to deal with the process from SyncNewHashRing to new Idle status.
                    // this is because we don't make persistent flags for status, so we could not check a status is finished or not.
                    // so we have to check the status in a long code block, and we could not use a loop to check the status.
                    // in the future, we will make persistent flags for status, and we separate the code block for each status.

                    info!("Transfer: start to sync new hash ring");
                    let all_servers_address = match self.get_new_hash_ring_info().await {
                        Ok(value) => value,
                        Err(e) => {
                            panic!("Get Hash Ring Info Failed. Error = {}", e);
                        }
                    };
                    info!("Transfer: get new hash ring info");

                    for value in all_servers_address.iter() {
                        if self.hash_ring.read().as_ref().unwrap().contains(&value.0) {
                            continue;
                        }
                        self.add_connection(&value.0).await;
                    }
                    self.new_hash_ring
                        .write()
                        .replace(HashRing::new(all_servers_address));
                    info!("Transfer: sync new hash ring finished");

                    // wait for all servers to be PreTransfer

                    while <i32 as TryInto<ClusterStatus>>::try_into(
                        self.cluster_status.load(Ordering::Relaxed),
                    )
                    .unwrap()
                        == ClusterStatus::SyncNewHashRing
                    {
                        sleep(Duration::from_secs(1)).await;
                    }
                    assert!(
                        <i32 as TryInto<ClusterStatus>>::try_into(
                            self.cluster_status.load(Ordering::Relaxed)
                        )
                        .unwrap()
                            == ClusterStatus::PreTransfer
                    );

                    while <i32 as TryInto<ClusterStatus>>::try_into(
                        self.cluster_status.load(Ordering::Relaxed),
                    )
                    .unwrap()
                        == ClusterStatus::PreTransfer
                    {
                        sleep(Duration::from_secs(1)).await;
                    }
                    assert!(
                        <i32 as TryInto<ClusterStatus>>::try_into(
                            self.cluster_status.load(Ordering::Relaxed)
                        )
                        .unwrap()
                            == ClusterStatus::Transferring
                    );

                    while <i32 as TryInto<ClusterStatus>>::try_into(
                        self.cluster_status.load(Ordering::Relaxed),
                    )
                    .unwrap()
                        == ClusterStatus::Transferring
                    {
                        sleep(Duration::from_secs(1)).await;
                    }
                    assert!(
                        <i32 as TryInto<ClusterStatus>>::try_into(
                            self.cluster_status.load(Ordering::Relaxed)
                        )
                        .unwrap()
                            == ClusterStatus::PreFinish
                    );

                    let _old_hash_ring = self
                        .hash_ring
                        .write()
                        .replace(self.new_hash_ring.read().as_ref().unwrap().clone());

                    while <i32 as TryInto<ClusterStatus>>::try_into(
                        self.cluster_status.load(Ordering::Relaxed),
                    )
                    .unwrap()
                        == ClusterStatus::PreFinish
                    {
                        sleep(Duration::from_secs(1)).await;
                    }
                    assert!(
                        <i32 as TryInto<ClusterStatus>>::try_into(
                            self.cluster_status.load(Ordering::Relaxed)
                        )
                        .unwrap()
                            == ClusterStatus::Finishing
                    );

                    let _ = self.new_hash_ring.write().take();
                    // here we should close connections to old servers, but now we just wait for remote servers to close connections and do nothing

                    while <i32 as TryInto<ClusterStatus>>::try_into(
                        self.cluster_status.load(Ordering::Relaxed),
                    )
                    .unwrap()
                        == ClusterStatus::Finishing
                    {
                        sleep(Duration::from_secs(1)).await;
                    }
                    assert!(
                        <i32 as TryInto<ClusterStatus>>::try_into(
                            self.cluster_status.load(Ordering::Relaxed)
                        )
                        .unwrap()
                            == ClusterStatus::Idle
                    );

                    info!("transferring data finished");
                }
                ClusterStatus::Idle => {
                    sleep(Duration::from_secs(1)).await;
                }
                ClusterStatus::Initializing => {
                    sleep(Duration::from_secs(1)).await;
                }
                ClusterStatus::NodesStarting => {
                    sleep(Duration::from_secs(1)).await;
                }
                e => {
                    panic!("cluster status error: {:?}", e as u32);
                }
            }
        }
    }

    pub async fn connect_to_manager(&self, manager_address: &str) {
        self.manager_address.lock().await.push_str(manager_address);

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
                    warn!("add connection failed, wait for a while");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn connect_servers(&'static self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("init");

        let result = async {
            loop {
                match self
                    .cluster_status
                    .load(Ordering::Acquire)
                    .try_into()
                    .unwrap()
                {
                    ClusterStatus::Idle => {
                        return self.get_hash_ring_info().await;
                    }
                    ClusterStatus::Initializing => {
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
                    .replace(HashRing::new(all_servers_address.clone()));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn init_volume(&self, volume_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.inodes_reverse.insert(1, volume_name.to_string());
        self.inodes.insert(volume_name.to_string(), 1);

        self.inode_counter
            .fetch_add(2, std::sync::atomic::Ordering::AcqRel);
        self.fd_counter
            .fetch_add(2, std::sync::atomic::Ordering::AcqRel);
        let _ = self.volume_name.write().replace(volume_name.to_string());
        Ok(())
    }

    pub async fn add_new_servers(
        &self,
        new_servers_info: Vec<(String, usize)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .add_new_servers(&self.manager_address.lock().await, new_servers_info)
            .await
    }

    pub async fn delete_servers(
        &self,
        servers_info: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .delete_servers(&self.manager_address.lock().await, servers_info)
            .await
    }

    pub async fn get_cluster_status(&self) -> Result<i32, Box<dyn std::error::Error>> {
        self.sender
            .get_cluster_status(&self.manager_address.lock().await)
            .await
    }

    pub async fn get_hash_ring_info(
        &self,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
        self.sender
            .get_hash_ring_info(&self.manager_address.lock().await)
            .await
    }

    pub async fn get_new_hash_ring_info(
        &self,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
        self.sender
            .get_new_hash_ring_info(&self.manager_address.lock().await)
            .await
    }

    pub fn get_full_path(&self, parent: &str, name: &OsStr) -> String {
        let path = format!("{}/{}", parent, name.to_str().unwrap());
        path
    }

    pub async fn create_volume(
        &self,
        name: &str,
        size: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .create_volume(&self.get_connection_address(name), name, size)
            .await
    }

    pub fn lookup_remote(&self, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!(
            "lookup_remote, parent: {}, name: {}",
            parent,
            name.to_str().unwrap()
        );
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => self.get_full_path(parent_path.deref(), name),
            None => {
                reply.error(libc::ENOENT);
                info!("lookup_remote error");
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
        info!("create_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("create_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let send_meta_data = bincode::serialize(&CreateFileSendMetaData {
            mode,
            umask,
            flags,
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

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

                let path = self.get_full_path(&path, name);
                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path);

                reply.created(&TTL, &file_attr, 0, 0, 0);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn getattr_remote(&self, ino: u64, reply: ReplyAttr) {
        info!("getattr_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("getattr_remote error");
                return;
            }
        };
        info!("getattr_remote path: {:?}", path);
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
        info!("readdir_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("readdir_remote error");
                return;
            }
        };
        let size = 2048;

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
        info!("read_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("read_remote error");
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
        info!("write_remote");
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("write_remote error");
                return;
            }
        };
        info!("write_remote path: {:?}", path);
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
        info!("mkdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("mkdir_remote error");
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
        let send_meta_data = bincode::serialize(&CreateDirSendMetaData {
            mode,
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

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

                let path = self.get_full_path(&path, name);
                self.inodes.insert(path.clone(), file_attr.ino);
                self.inodes_reverse.insert(file_attr.ino, path);
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn open_remote(&self, ino: u64, flags: i32, reply: ReplyOpen) {
        info!("open_remote");
        if flags & libc::O_CREAT != 0 {
            todo!("open_remote O_CREAT")
        }
        let path = match self.inodes_reverse.get(&ino) {
            Some(path) => path.clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("open_remote error");
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
        info!("unlink_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("unlink_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = bincode::serialize(&DeleteFileSendMetaData {
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::DeleteFile.into(),
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
            Ok(_) => {
                let path = self.get_full_path(&path, name);
                self.inodes_reverse
                    .remove(self.inodes.get(&path).as_deref().unwrap());
                self.inodes.remove(&path);
                reply.ok();
            }
            Err(_) => {
                reply.error(libc::EIO);
            }
        }
    }

    pub fn rmdir_remote(&self, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        info!("rmdir_remote");
        let path = match self.inodes_reverse.get(&parent) {
            Some(parent_path) => parent_path.deref().clone(),
            None => {
                reply.error(libc::ENOENT);
                info!("rmdir_remote error");
                return;
            }
        };
        let server_address = self.get_connection_address(&path);
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = bincode::serialize(&DeleteDirSendMetaData {
            name: name.to_str().unwrap().to_owned(),
        })
        .unwrap();

        let result = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::DeleteDir.into(),
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
            Ok(_) => {
                let path = self.get_full_path(&path, name);
                self.inodes_reverse
                    .remove(self.inodes.get(&path).as_deref().unwrap());
                self.inodes.remove(&path);
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
