use super::storage_engine::meta_engine::MetaEngine;
use super::storage_engine::StorageEngine;
use super::transfer_manager::TransferManager;
use super::volume::Volume;
use super::EngineError;
use crate::common::byte::CHUNK_SIZE;
use crate::common::hash_ring::HashRing;
use crate::common::sender::Sender;
use crate::common::serialization::{
    CheckDirSendMetaData, CheckFileSendMetaData, ClusterStatus, CreateDirSendMetaData,
    CreateFileSendMetaData, FileAttrSimple, FileTypeSimple, ManagerOperationType,
    ReadFileSendMetaData, ServerStatus, WriteFileSendMetaData,
};
use crate::common::serialization::{DirectoryEntrySendMetaData, OperationType};

use crate::common::util::get_full_path;
use crate::rpc::client::Client;
use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;
use libc::{O_CREAT, O_DIRECTORY, O_EXCL};
use log::{debug, error, info};
use nix::fcntl::OFlag;
use rocksdb::IteratorMode;
use spin::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::{sync::Arc, vec};
use tokio::sync::Mutex;
use tokio::time::Duration;

pub struct DistributedEngine<Storage: StorageEngine> {
    pub address: String,
    pub storage_engine: Arc<Storage>,
    pub meta_engine: Arc<MetaEngine>,
    pub client: Arc<Client>,
    pub sender: Sender,

    pub cluster_status: AtomicI32,

    pub hash_ring: Arc<RwLock<Option<HashRing>>>,
    pub new_hash_ring: Arc<RwLock<Option<HashRing>>>,

    pub manager_address: Arc<Mutex<String>>,

    pub file_locks: DashMap<String, HashMap<String, u32>>,
    pub volumes: DashMap<String, Volume>,
    pub volume_lock: spin::Mutex<()>,
    pub transfer_manager: TransferManager,
    pub volume_indexes: DashMap<u32, String>,
}

impl<Storage> DistributedEngine<Storage>
where
    Storage: StorageEngine,
{
    pub fn new(
        address: String,
        storage_engine: Arc<Storage>,
        meta_engine: Arc<MetaEngine>,
    ) -> Self {
        let file_locks = DashMap::new();
        let client = Arc::new(Client::new());
        Self {
            address,
            storage_engine,
            meta_engine,
            client: client.clone(),
            sender: Sender::new(client),
            cluster_status: AtomicI32::new(ClusterStatus::Initializing.into()),
            hash_ring: Arc::new(RwLock::new(None)),
            new_hash_ring: Arc::new(RwLock::new(None)),
            manager_address: Arc::new(Mutex::new("".to_string())),
            file_locks,
            volumes: DashMap::new(),
            volume_lock: spin::Mutex::new(()),
            transfer_manager: TransferManager::new(),
            volume_indexes: DashMap::new(),
        }
    }

    pub async fn add_connection(&self, address: String) {
        loop {
            if self.client.add_connection(&address).await {
                info!("add connection to {}", address);
                break;
            }
            info!("add connection to {} failed, retrying...", address);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.sender.init_volume(&address, "").await.unwrap();
    }

    pub async fn add_server_connection(&self, address: String) {
        loop {
            if self.client.add_connection(&address).await {
                info!("add server connection to {}", address);
                match self.sender.init_volume(&address, "").await {
                    Ok(_) => {
                        info!("init volume success");
                        break;
                    }
                    Err(_) => {
                        info!("init volume failed, retrying...");
                    }
                }
            }
            info!("add server connection to {} failed, retrying...", address);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn lock_file(
        &self,
        path: &str,
    ) -> Result<Ref<String, HashMap<std::string::String, u32>>, EngineError> {
        match self.file_locks.get(path) {
            Some(lock) => Ok(lock),
            None => {
                info!("lock file error: {}", path);
                Err(EngineError::NoEntry)
            }
        }
    }

    pub fn lock_file_mut(
        &self,
        path: &str,
    ) -> Result<RefMut<String, HashMap<std::string::String, u32>>, EngineError> {
        match self.file_locks.get_mut(path) {
            Some(lock) => Ok(lock),
            None => {
                error!("lock file error: {}", path);
                Err(EngineError::NoEntry)
            }
        }
    }

    pub fn make_up_file_map(&self) -> Vec<String> {
        let mut file_map = Vec::new();
        self.meta_engine
            .file_attr_db
            .db
            .iterator(IteratorMode::Start)
            .for_each(|result| {
                let (k, _) = result.unwrap();
                let k = String::from_utf8(k.to_vec()).unwrap();
                if self.get_new_address(&k) != self.address {
                    file_map.push(k);
                }
            });
        self.transfer_manager.make_up_files(&file_map);
        file_map
    }

    pub async fn create_file_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path);
        let send_meta_data = bincode::serialize(&CreateFileSendMetaData {
            mode: 0o777,
            umask: 0,
            flags: OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits(),
            name: "".to_string(),
        })
        .unwrap();

        self.sender
            .create_no_parent(
                &address,
                OperationType::CreateFileNoParent,
                path,
                &send_meta_data,
            )
            .await?;
        Ok(())
    }

    pub async fn write_file_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path);

        let file_attr = self.meta_engine.get_file_attr(path).unwrap();

        let mut idx = 0;
        let end_idx = file_attr.size as i64;
        let mut chunk_left = 0;
        let mut chunk_right = std::cmp::min((idx + 1) * CHUNK_SIZE, end_idx);
        let mut _result = 0;
        while chunk_left < end_idx {
            // let file_path = format!("{}_{}", pathname, idx);
            // println!("write: {} {}", file_path, address);

            let send_meta_data =
                bincode::serialize(&WriteFileSendMetaData { offset: chunk_left }).unwrap();
            let mut status = 0i32;
            let mut rsp_flags = 0u32;
            let chunk_buf = self
                .storage_engine
                .read_file(path, CHUNK_SIZE as u32, chunk_left)
                .unwrap();
            let mut recv_meta_data_length = 0usize;
            let mut recv_data_length = 0usize;

            let mut recv_meta_data = [0u8; std::mem::size_of::<isize>()];
            if let Err(e) = self
                .client
                .call_remote(
                    &address,
                    OperationType::WriteFile.into(),
                    0,
                    path,
                    &send_meta_data,
                    &chunk_buf,
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut [],
                )
                .await
            {
                error!("write file failed with error: {}", e);
                return Err(EngineError::IO);
            }
            if status != 0 {
                error!("write file failed, status: {}", status);
                return Err(EngineError::IO);
            }
            let size = isize::from_le_bytes(recv_meta_data);
            idx += 1;
            chunk_left = chunk_right;
            chunk_right = std::cmp::min(chunk_right + CHUNK_SIZE, end_idx);
            _result += size;
        }
        Ok(())
    }

    pub async fn check_file_remote(&self, path: &str) -> Result<(), EngineError> {
        let file_attr = self.meta_engine.get_file_attr(path).unwrap();
        let server_address = self.get_new_address(path);
        // println!("check: {} {}", file_path, server_address);

        let send_meta_data = bincode::serialize(&CheckFileSendMetaData { file_attr }).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;
        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        if let Err(e) = self
            .client
            .call_remote(
                &server_address,
                OperationType::CheckFile.into(),
                0,
                path,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            )
            .await
        {
            error!("check file failed with error: {}", e);
            return Err(EngineError::IO);
        }
        if status != 0 {
            error!("check file failed, status: {}", status);
            return Err(EngineError::IO);
        }

        self.delete_file_no_parent(path)
    }

    pub async fn create_dir_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path);

        let send_meta_data = bincode::serialize(&CreateDirSendMetaData {
            mode: 0o777,
            name: "".to_string(),
        })
        .unwrap();

        self.sender
            .create_no_parent(
                &address,
                OperationType::CreateDirNoParent,
                path,
                &send_meta_data,
            )
            .await?;
        Ok(())
    }

    pub async fn add_subdirs_remote(&self, path: &str) -> Result<(), EngineError> {
        if !path.contains('/') {
            // root directory of a volume
            return Ok(());
        }
        let address = self.get_new_address(path);

        for item in self.meta_engine.dir_db.db.iterator(IteratorMode::From(
            format!("{}-", path).as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            // let file_path = format!("{}_{}", pathname, idx);
            // println!("write: {} {}", file_path, server_address);

            let (key, value) = item.unwrap();
            let file_name = String::from_utf8(value.to_vec()).unwrap();
            let file_type = *key.last().unwrap();

            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type,
                file_name,
            })
            .unwrap();

            self.sender
                .directory_add_entry(&address, path, &send_meta_data)
                .await?;
        }
        Ok(())
    }

    pub async fn check_dir_remote(&self, path: &str) -> Result<(), EngineError> {
        let file_attr = self.meta_engine.get_file_attr(path).unwrap();
        let server_address = self.get_new_address(path);
        // println!("check: {} {}", file_path, server_address);

        let send_meta_data = bincode::serialize(&CheckDirSendMetaData { file_attr }).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;
        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        if let Err(e) = self
            .client
            .call_remote(
                &server_address,
                OperationType::CheckDir.into(),
                0,
                path,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            )
            .await
        {
            error!("check dir failed with error: {}", e);
            return Err(EngineError::IO);
        }
        if status != 0 {
            error!("check dir failed, status: {}", status);
            return Err(EngineError::IO);
        }

        self.delete_dir_no_parent_force(path)
    }

    pub async fn transfer_files(&self, file_map: Vec<String>) -> Result<(), EngineError> {
        // transfer all files ,and set the flag as true
        info!("transfer_files: {:?}", file_map);
        for k in file_map {
            let _lock = self.transfer_manager.get_wlock(&k).await;
            if self.transfer_manager.status(&k).unwrap() {
                continue;
            }
            match self.meta_engine.is_dir(&k) {
                Ok(true) => {
                    self.create_dir_remote(&k).await?;
                    self.add_subdirs_remote(&k).await?;
                    self.check_dir_remote(&k).await?;
                }
                Ok(false) => {
                    self.create_file_remote(&k).await?;
                    self.write_file_remote(&k).await?;
                    self.check_file_remote(&k).await?;
                }
                Err(EngineError::NoEntry) => {
                    // file has been deleted before transfering
                    continue;
                }
                Err(e) => {
                    error!("transfer_files: {}", e);
                    return Err(e);
                }
            }
            info!("transfer_files: {} done", k);
            self.transfer_manager.set_status(&k, true);
        }
        Ok(())
    }

    pub fn remove_connection(&self, address: String) {
        self.client.remove_connection(&address);
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
            Some(ring) => ring.get(path).unwrap().address.clone(),
            None => self.get_address(path),
        }
    }

    pub async fn rlock_in_transfer_map(&self, path: &str) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.transfer_manager.get_rlock(path).await
    }

    pub fn get_server_address(&self, path: &str) -> (String, bool) {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return (self.get_address(path), false);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::Initializing => todo!(),
            ClusterStatus::Idle => todo!(),
            ClusterStatus::NodesStarting => (self.get_address(path), false),
            ClusterStatus::SyncNewHashRing => (self.get_address(path), false),
            ClusterStatus::PreTransfer => {
                let address = self.get_address(path);
                if address != self.address {
                    (address, false)
                } else {
                    let new_address = self.get_new_address(path);
                    if new_address != self.address {
                        // the most efficient way is to check the operation_type
                        // if operation_type is Create, forward the request to the new node
                        // here is a temporary solution
                        match self.meta_engine.is_exist(path) {
                            Ok(true) => (address, false),
                            Ok(false) => (new_address, false),
                            Err(e) => {
                                error!("get forward address failed, error: {}", e);
                                (new_address, false) // local db error, attempt to forward. but it may cause inconsistency
                            }
                        }
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::Transferring => {
                let address = self.get_address(path);
                if address != self.address {
                    (address, false)
                } else {
                    let new_address = self.get_new_address(path);
                    if new_address != self.address {
                        match self.transfer_manager.status(path) {
                            Some(true) => (new_address, false),
                            Some(false) => (address, false),
                            None => (new_address, false),
                        }
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::PreFinish => {
                let address = self.get_address(path);
                if address != self.address {
                    (address, false)
                } else {
                    let new_address = self.get_new_address(path);
                    if new_address != self.address {
                        (new_address, false)
                    } else {
                        (address, false)
                    }
                }
            }
            ClusterStatus::Finishing => (self.get_address(path), false),
            ClusterStatus::StatusError => todo!(),
            //s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }

    pub fn get_forward_address(&self, path: &str) -> (Option<String>, bool) {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            // assert!(self.address == self.get_address(path));
            return (None, false);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::NodesStarting => (None, false),
            ClusterStatus::SyncNewHashRing => (None, false),
            ClusterStatus::PreTransfer => {
                let address = self.get_new_address(path);
                if address != self.address {
                    // the most efficient way is to check the operation_type
                    // if operation_type is Create, forward the request to the new node
                    // here is a temporary solution
                    match self.meta_engine.is_exist(path) {
                        Ok(true) => (None, false),
                        Ok(false) => (Some(address), false),
                        Err(e) => {
                            error!("get forward address failed, error: {}", e);
                            (Some(address), false) // local db error, attempt to forward. but it may cause inconsistency
                        }
                    }
                } else {
                    (None, false)
                }
            }
            ClusterStatus::Transferring => {
                let address = self.get_new_address(path);
                if address != self.address {
                    match self.transfer_manager.status(path) {
                        Some(true) => (Some(address), false),
                        Some(false) => (None, false),
                        None => (Some(address), false),
                    }
                } else {
                    (None, false)
                }
            }
            ClusterStatus::PreFinish => {
                let address = self.get_new_address(path);
                if address != self.address {
                    (Some(address), false)
                } else {
                    (None, false)
                }
            }
            ClusterStatus::Finishing => (None, false),
            ClusterStatus::Initializing => (None, false),
            s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }

    pub async fn update_server_status(
        &self,
        server_status: ServerStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let send_meta_data = bincode::serialize(&server_status).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                &self.manager_address.lock().await,
                ManagerOperationType::UpdateServerStatus.into(),
                0,
                &self.address,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get cluster status failed, status: {}", status).into());
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_cluster_status(&self) -> Result<ClusterStatus, Box<dyn std::error::Error>> {
        Ok(self
            .sender
            .get_cluster_status(&self.manager_address.lock().await)
            .await?
            .try_into()?)
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

    #[inline]
    pub async fn forward_request(
        &self,
        address: String,
        operation_type: u32,
        flags: u32,
        path: &str,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>), EngineError> {
        let (
            mut status,
            mut rsp_flags,
            mut recv_meta_data_length,
            mut recv_data_length,
            mut recv_meta_data,
            mut recv_data,
        ) = match operation_type.try_into().unwrap() {
            OperationType::Unkown => todo!(),
            OperationType::Lookup => todo!(),
            OperationType::CreateFile => (0, 0, 0, 0, vec![0; 1024], vec![]),
            OperationType::CreateDir => (0, 0, 0, 0, vec![0; 1024], vec![]),
            OperationType::GetFileAttr => (0, 0, 0, 0, vec![0; 1024], vec![]),
            OperationType::ReadDir => (0, 0, 0, 0, vec![0; 2048], vec![]),
            OperationType::OpenFile => (0, 0, 0, 0, vec![], vec![]),
            OperationType::ReadFile => {
                let unwraped_meta_data =
                    bincode::deserialize::<ReadFileSendMetaData>(&metadata).unwrap();
                (
                    0,
                    0,
                    0,
                    0,
                    vec![],
                    vec![0; unwraped_meta_data.size as usize],
                )
            }
            OperationType::WriteFile => (0, 0, 0, 0, vec![0; 4], vec![]),
            OperationType::DeleteFile => (0, 0, 0, 0, vec![], vec![]),
            OperationType::DeleteDir => (0, 0, 0, 0, vec![], vec![]),
            OperationType::DirectoryAddEntry => (0, 0, 0, 0, vec![], vec![]),
            OperationType::DirectoryDeleteEntry => (0, 0, 0, 0, vec![], vec![]),
            OperationType::TruncateFile => (0, 0, 0, 0, vec![], vec![]),
            OperationType::CheckDir => (0, 0, 0, 0, vec![], vec![]),
            OperationType::CheckFile => (0, 0, 0, 0, vec![], vec![]),
            OperationType::CreateDirNoParent => (0, 0, 0, 0, vec![0; 1024], vec![]),
            OperationType::CreateFileNoParent => (0, 0, 0, 0, vec![0; 1024], vec![]),
            OperationType::DeleteDirNoParent => (0, 0, 0, 0, vec![], vec![]),
            OperationType::DeleteFileNoParent => (0, 0, 0, 0, vec![], vec![]),
            OperationType::CreateVolume => (0, 0, 0, 0, vec![], vec![]),
            OperationType::InitVolume => todo!(),
        };
        let result = self
            .client
            .call_remote(
                &address,
                operation_type,
                flags,
                path,
                &metadata,
                &data,
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut recv_data,
            )
            .await;

        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status.into());
                }
            }
            Err(e) => {
                error!("forward request failed: {:?}", e);
                return Err(EngineError::StdIo(std::io::Error::from(
                    std::io::ErrorKind::NotConnected,
                )));
            }
        };
        Ok((
            status,
            rsp_flags,
            recv_meta_data_length,
            recv_data_length,
            recv_meta_data,
            recv_data,
        ))
    }

    pub fn create_dir_no_parent(&self, path: &str, mode: u32) -> Result<Vec<u8>, EngineError> {
        match self.file_locks.insert(path.to_owned(), HashMap::new()) {
            Some(_) => Err(EngineError::Exist),
            None => self.meta_engine.create_directory(path, mode),
        }
    }

    pub async fn create_dir(
        &self,
        send_meta_data: Vec<u8>,
        parent: &str,
        name: &str,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        {
            let mut file_lock = self.lock_file_mut(parent)?;
            if file_lock.contains_key(name) {
                return Err(EngineError::Exist); // this may indicate that the file is being created or deleted
            }
            file_lock.insert(name.to_owned(), 0);
        }

        let path = get_full_path(parent, name);
        let (address, _lock) = self.get_server_address(&path);
        let result = if self.address == address {
            info!(
                "local create dir, parent_dir: {}, file_name: {}",
                parent, name
            );
            self.create_dir_no_parent(&path, mode)
        } else {
            self.sender
                .create_no_parent(
                    &address,
                    OperationType::CreateDirNoParent,
                    &path,
                    &send_meta_data,
                )
                .await
        };

        if result.is_ok() {
            self.meta_engine
                .directory_add_entry(parent, name, FileTypeSimple::Directory.into())?;
        }

        let mut file_lock = self.lock_file_mut(parent)?;
        file_lock.remove(name);
        drop(file_lock);

        match result {
            Ok(attr) => Ok(attr),
            Err(e) => Err(e),
        }
    }

    pub fn delete_dir_no_parent(&self, path: &str) -> Result<(), EngineError> {
        match self.file_locks.get_mut(path) {
            Some(value) => {
                self.meta_engine.delete_directory(path)?;
                drop(value);
                self.file_locks.remove(path);
                Ok(())
            }
            None => Err(EngineError::NoEntry),
        }
    }

    pub fn delete_dir_no_parent_force(&self, path: &str) -> Result<(), EngineError> {
        match self.file_locks.get_mut(path) {
            Some(value) => {
                self.meta_engine.delete_directory_force(path)?;
                drop(value);
                self.file_locks.remove(path);
                Ok(())
            }
            None => Err(EngineError::NoEntry),
        }
    }

    pub async fn delete_dir(
        &self,
        send_meta_data: Vec<u8>,
        parent: &str,
        name: &str,
    ) -> Result<(), EngineError> {
        {
            let mut file_lock = self.lock_file_mut(parent)?;
            if file_lock.contains_key(name) {
                return Err(EngineError::NoEntry); // this may indicate that the file is being created or deleted
            }
            file_lock.insert(name.to_owned(), 0);
        }

        let path = get_full_path(parent, name);
        let (address, _lock) = self.get_server_address(&path);
        let result = if self.address == address {
            info!(
                "local create dir, parent_dir: {}, file_name: {}",
                parent, name
            );
            match self.delete_dir_no_parent(&path) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        } else {
            self.sender
                .delete_no_parent(
                    &address,
                    OperationType::DeleteDirNoParent,
                    &path,
                    &send_meta_data,
                )
                .await
        };

        if result.is_ok() {
            self.meta_engine.directory_delete_entry(
                parent,
                name,
                FileTypeSimple::Directory.into(),
            )?;
        }

        let mut file_lock = self.lock_file_mut(parent)?;
        file_lock.remove(name);
        drop(file_lock);

        match result {
            Ok(attr) => Ok(attr),
            Err(e) => Err(e),
        }
    }

    pub async fn read_dir(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        let _file_lock = self.lock_file(path)?;
        self.meta_engine.read_directory(path, size, offset)
    }

    pub fn create_file_no_parent(
        &self,
        path: &str,
        oflag: i32,
        umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        match self.file_locks.insert(path.to_owned(), HashMap::new()) {
            Some(_) => Err(EngineError::Exist),
            None => {
                info!("local create file, path: {}", path);
                self.storage_engine.create_file(path, oflag, umask, mode)
            }
        }
    }

    pub async fn call_get_attr_remote_or_local(&self, path: &str) -> Result<Vec<u8>, EngineError> {
        let (address, _lock) = self.get_server_address(path);
        if self.address == address {
            info!("local get attr, path: {}", path);
            self.meta_engine.get_file_attr_raw(path)
        } else {
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let mut recv_meta_data = vec![0; 1024];
            match self
                .client
                .call_remote(
                    &address,
                    OperationType::GetFileAttr as u32,
                    0,
                    path,
                    &[],
                    &[],
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut recv_meta_data,
                    &mut [],
                )
                .await
            {
                Ok(_) => {
                    if status != 0 {
                        Err(status.into())
                    } else {
                        Ok(recv_meta_data)
                    }
                }
                Err(e) => {
                    error!("Get attr failed: {} ,{:?}", path, e);
                    Err(EngineError::IO)
                }
            }
        }
    }

    pub async fn create_file(
        &self,
        send_meta_data: Vec<u8>,
        parent: &str,
        name: &str,
        oflag: i32,
        umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        let path = get_full_path(parent, name);
        {
            let mut file_lock = self.lock_file_mut(parent)?;
            if file_lock.contains_key(name) {
                if (oflag & O_EXCL) != 0 {
                    return Err(EngineError::Exist); // this may indicate that the file is being created or deleted
                } else {
                    drop(file_lock);
                    match self.call_get_attr_remote_or_local(&path).await {
                        Ok(attr) => return Ok(attr),
                        Err(EngineError::NoEntry) => {
                            return Ok(bincode::serialize(&FileAttrSimple::new(
                                FileTypeSimple::RegularFile,
                            ))
                            .unwrap()); // this may indicate that the file is creating or deleting
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
            file_lock.insert(name.to_owned(), 0);
        }

        let (address, _lock) = self.get_server_address(&path);
        let result = if self.address == address {
            info!(
                "local create file, parent_file: {}, file_name: {}",
                parent, name
            );
            match self.create_file_no_parent(&path, oflag, umask, mode) {
                Ok(attr) => Ok(attr),
                Err(EngineError::Exist) => {
                    if (oflag & O_EXCL) != 0 {
                        return Err(EngineError::Exist); // this may indicate that the file is being created or deleted
                    } else {
                        return self.call_get_attr_remote_or_local(&path).await;
                    }
                }
                Err(e) => {
                    error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                    Err(e)
                }
            }
        } else {
            self.sender
                .create_no_parent(
                    &address,
                    OperationType::CreateFileNoParent,
                    &path,
                    &send_meta_data,
                )
                .await
        };

        let mut file_lock = self.lock_file_mut(parent)?;
        if result.is_ok() {
            self.meta_engine.directory_add_entry(
                parent,
                name,
                FileTypeSimple::RegularFile.into(),
            )?;
        }
        file_lock.remove(name);
        drop(file_lock);

        match result {
            Ok(attr) => Ok(attr),
            Err(e) => Err(e),
        }
    }

    pub fn delete_file_no_parent(&self, path: &str) -> Result<(), EngineError> {
        match self.file_locks.get_mut(path) {
            Some(value) => {
                self.storage_engine.delete_file(path)?;
                drop(value);
                self.file_locks.remove(path);
                Ok(())
            }
            None => Err(EngineError::NoEntry),
        }
    }

    pub async fn delete_file(
        &self,
        send_meta_data: Vec<u8>,
        parent: &str,
        name: &str,
    ) -> Result<(), EngineError> {
        {
            let mut file_lock = self.lock_file_mut(parent)?;
            if file_lock.contains_key(name) {
                return Err(EngineError::NoEntry); // this may indicate that the file is being created or deleted
            }
            file_lock.insert(name.to_owned(), 0);
        }

        let path = get_full_path(parent, name);
        let (address, _lock) = self.get_server_address(&path);
        let result = if self.address == address {
            debug!(
                "local create file, parent_file: {}, file_name: {}",
                parent, name
            );
            match self.delete_file_no_parent(&path) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        } else {
            self.sender
                .delete_no_parent(
                    &address,
                    OperationType::DeleteFileNoParent,
                    &path,
                    &send_meta_data,
                )
                .await
        };

        let mut file_lock = self.lock_file_mut(parent)?;
        if result.is_ok() {
            self.meta_engine.directory_delete_entry(
                parent,
                name,
                FileTypeSimple::RegularFile.into(),
            )?;
        }
        file_lock.remove(name);
        drop(file_lock);

        match result {
            Ok(attr) => Ok(attr),
            Err(e) => Err(e),
        }
    }

    pub async fn truncate_file(&self, path: &str, length: i64) -> Result<(), EngineError> {
        // a temporary implementation
        let _file_lock = self.lock_file(path)?;
        self.storage_engine.truncate_file(path, length)
    }

    pub async fn read_file(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        let _file_lock = self.lock_file(path)?;
        self.storage_engine.read_file(path, size, offset)
    }

    pub async fn write_file(
        &self,
        path: &str,
        data: &[u8],
        offset: i64,
    ) -> Result<usize, EngineError> {
        let _file_lock = self.lock_file(path)?;
        self.storage_engine.write_file(path, data, offset)
    }

    pub async fn get_file_attr(&self, path: &str) -> Result<Vec<u8>, EngineError> {
        let _file_lock = self.lock_file(path)?;
        self.meta_engine.get_file_attr_raw(path)
    }

    pub async fn open_file(&self, path: &str, flag: i32, mode: u32) -> Result<(), EngineError> {
        if (flag & O_CREAT) != 0 {
            todo!("create file should be converted at client side")
        } else if (flag & O_DIRECTORY) != 0 {
            Ok(())
        } else {
            let _file_lock = self.lock_file(path)?;
            self.storage_engine.open_file(path, flag, mode)
        }
    }

    pub async fn directory_add_entry(&self, path: &str, file_name: String, file_type: u8) -> i32 {
        let _lock = match self.lock_file(path) {
            Ok(lock) => lock,
            Err(e) => {
                error!("directory add entry, lock file failed: {:?}", e);
                return e.into();
            }
        };
        match self
            .meta_engine
            .directory_add_entry(path, &file_name, file_type)
        {
            Ok(()) => {
                debug!("{} Directory Add Entry success", self.address);
                0
            }
            Err(value) => {
                debug!("{} Directory Add Entry error: {:?}", self.address, value);
                value.into()
            }
        }
    }

    pub async fn check_file(
        &self,
        path: &str,
        file_attr: FileAttrSimple,
    ) -> Result<(), EngineError> {
        self.meta_engine.complete_transfer_file(path, file_attr)
    }

    pub async fn check_dir(
        &self,
        path: &str,
        file_attr: FileAttrSimple,
    ) -> Result<(), EngineError> {
        self.meta_engine.complete_transfer_file(path, file_attr)
    }

    pub async fn directory_delete_entry(
        &self,
        path: &str,
        file_name: String,
        file_type: u8,
    ) -> i32 {
        let _lock = match self.lock_file(path) {
            Ok(lock) => lock,
            Err(e) => {
                error!("directory delete entry, lock file failed: {:?}", e);
                return e.into();
            }
        };
        match self
            .meta_engine
            .directory_delete_entry(path, &file_name, file_type)
        {
            Ok(()) => 0,
            Err(value) => {
                debug!("{} Directory Delete Entry error: {:?}", self.address, value);
                value.into()
            }
        }
    }

    pub fn create_volume(&self, name: &str) -> Result<(), EngineError> {
        let _vlock = {
            let _lock = self.volume_lock.lock();
            if self.volumes.contains_key(name) {
                return Err(EngineError::Exist);
            }
            self.volumes.insert(
                name.to_owned(),
                Volume {
                    name: name.to_owned(),
                    size: 100000000,
                    used_size: 0,
                },
            );
            self.volumes.get(name).unwrap()
        };
        match self.create_dir_no_parent(name, 0o755) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
