use super::storage_engine::meta_engine::MetaEngine;
use super::EngineError;
use super::{path_split, storage_engine::StorageEngine};
use crate::common::byte::CHUNK_SIZE;
use crate::common::hash_ring::HashRing;
use crate::common::serialization::{
    CheckDirSendMetaData, CheckFileSendMetaData, ClusterStatus, CreateDirSendMetaData,
    CreateFileSendMetaData, FileAttrSimple, FileTypeSimple, GetClusterStatusRecvMetaData,
    GetHashRingInfoRecvMetaData, ManagerOperationType, ServerStatus, WriteFileSendMetaData,
};
use crate::common::serialization::{DirectoryEntrySendMetaData, OperationType};

use crate::rpc::client::Client;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use libc::{O_CREAT, O_DIRECTORY, O_EXCL};
use log::{debug, error, info};
use nix::fcntl::OFlag;
use rocksdb::IteratorMode;
use std::sync::atomic::{AtomicI32, Ordering};
use std::{sync::Arc, vec};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;
pub struct DistributedEngine<Storage: StorageEngine> {
    pub address: String,
    pub storage_engine: Arc<Storage>,
    pub meta_engine: Arc<MetaEngine>,
    pub client: Client,

    pub cluster_status: AtomicI32,

    pub hash_ring: Arc<RwLock<Option<HashRing>>>,
    pub new_hash_ring: Arc<RwLock<Option<HashRing>>>,

    pub transfering_files: DashMap<String, bool>,

    pub manager_address: Arc<Mutex<String>>,
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
        Self {
            address,
            storage_engine,
            meta_engine,
            client: Client::new(),
            cluster_status: AtomicI32::new(ClusterStatus::Init.into()),
            hash_ring: Arc::new(RwLock::new(None)),
            new_hash_ring: Arc::new(RwLock::new(None)),
            transfering_files: DashMap::new(),
            manager_address: Arc::new(Mutex::new("".to_string())),
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
    }

    pub async fn make_up_file_map(&self) -> Result<(), EngineError> {
        match self.meta_engine.get_file_map() {
            Ok(file_map) => {
                self.transfering_files.clear();
                for path in file_map {
                    if self.get_new_address(&path).await != self.address {
                        self.transfering_files.insert(path, false);
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!("make_up_file_map: {}", e);
                Err(e)
            }
        }
    }

    pub async fn create_file_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path).await;

        let send_meta_data = bincode::serialize(&CreateFileSendMetaData {
            mode: 0o777,
            umask: 0,
            flags: OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits(),
        })
        .unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
            .client
            .call_remote(
                &address,
                OperationType::CreateFile.into(),
                0,
                path,
                &send_meta_data,
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
                    error!("create file failed, status: {}", status);
                    Err(EngineError::IO)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub async fn write_file_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path).await;

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
        let server_address = self.get_new_address(path).await;
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
        Ok(())
    }

    pub async fn create_dir_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path).await;

        let send_meta_data = bincode::serialize(&CreateDirSendMetaData { mode: 0o777 }).unwrap();

        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
            .client
            .call_remote(
                &address,
                OperationType::CreateDir.into(),
                0,
                path,
                &send_meta_data,
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
                    error!("create dir failed, status: {}", status);
                    Err(EngineError::IO)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("create dir failed with error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub async fn add_subdirs_remote(&self, path: &str) -> Result<(), EngineError> {
        let address = self.get_new_address(path).await;

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
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let result = self
                .client
                .call_remote(
                    &address,
                    OperationType::DirectoryAddEntry as u32,
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
                .await;
            match result {
                Ok(_) => {
                    if status != 0 {
                        return Err(status.into());
                    }
                }
                e => {
                    error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )));
                }
            };
        }
        Ok(())
    }

    pub async fn check_dir_remote(&self, path: &str) -> Result<(), EngineError> {
        let file_attr = self.meta_engine.get_file_attr(path).unwrap();
        let server_address = self.get_new_address(path).await;
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
        Ok(())
    }

    pub async fn transfer_files(&self) -> Result<(), EngineError> {
        let iter = self.transfering_files.iter_mut();
        // transfer all files ,and set the flag as true
        for mut kv in iter {
            if *kv.value() {
                continue;
            }
            let path = kv.key();

            match self.meta_engine.is_dir(path) {
                Ok(true) => {
                    self.create_dir_remote(path).await?;
                    self.add_subdirs_remote(path).await?;
                    self.check_dir_remote(path).await?;
                }
                Ok(false) => {
                    self.create_file_remote(path).await?;
                    self.write_file_remote(path).await?;
                    self.check_file_remote(path).await?;
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
            *kv.value_mut() = true;
        }
        Ok(())
    }

    pub fn remove_connection(&self, address: String) {
        self.client.remove_connection(&address);
    }

    pub async fn get_address(&self, path: &str) -> String {
        self.hash_ring
            .read()
            .await
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    pub async fn get_new_address(&self, path: &str) -> String {
        self.new_hash_ring
            .read()
            .await
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    pub fn rlock_in_transfer_map(&self, path: &str) -> Option<Ref<String, bool>> {
        self.transfering_files.get(path)
    }

    pub async fn get_server_address(&self, path: &str) -> (String, Option<Ref<String, bool>>) {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return (self.get_address(path).await, None);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::SyncNewHashRing => (self.get_address(path).await, None),
            ClusterStatus::PreTransfer => {
                let address = self.get_new_address(path).await;
                if address != self.address {
                    (address, None)
                } else {
                    (self.get_address(path).await, None)
                }
            }
            ClusterStatus::Transferring => {
                let address = self.get_new_address(path).await;
                if address != self.address {
                    if let Some(lock) = self.rlock_in_transfer_map(path) {
                        (address, Some(lock))
                    } else {
                        (address, None)
                    }
                } else {
                    (self.get_address(path).await, None)
                }
            }
            ClusterStatus::PreFinish => (self.get_new_address(path).await, None),
            s => panic!("get forward address failed, invalid cluster status: {}", s),
        }
    }

    pub async fn get_forward_address(
        &self,
        path: &str,
    ) -> (Option<String>, Option<Ref<String, bool>>) {
        let cluster_status = self.cluster_status.load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return (None, None);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::StartNodes => (None, None),
            ClusterStatus::SyncNewHashRing => (None, None),
            ClusterStatus::PreTransfer => {
                let address = self.get_new_address(path).await;
                if address != self.address {
                    (Some(address), None)
                } else {
                    (None, None)
                }
            }
            ClusterStatus::Transferring => {
                let address = self.get_new_address(path).await;
                if address != self.address {
                    if let Some(lock) = self.rlock_in_transfer_map(path) {
                        (None, Some(lock))
                    } else {
                        (Some(address), None)
                    }
                } else {
                    (None, None)
                }
            }
            ClusterStatus::PreFinish => (Some(self.get_new_address(path).await), None),
            ClusterStatus::CloseNodes => (None, None),
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
                Ok(cluster_status_meta_data.status)
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
            Err(e) => {
                error!("get hash ring failed: {:?}", e);
                Err(e)
            }
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
            Err(e) => {
                error!("get new hash ring failed: {:?}", e);
                Err(e)
            }
        }
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
    ) -> Result<(i32, u32, Vec<u8>, Vec<u8>), EngineError> {
        let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
            (0, 0, 0, 0);
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![];
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
        Ok((status, rsp_flags, recv_meta_data, recv_data))
    }

    pub async fn create_dir(&self, path: &str, mode: u32) -> Result<Vec<u8>, EngineError> {
        if self.meta_engine.is_exist(path)? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path)?;
        let (address, _lock) = self.get_server_address(&parent_dir).await;
        if self.address == address {
            info!(
                "local create dir, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
            self.meta_engine.directory_add_entry(
                parent_dir.as_str(),
                file_name.as_str(),
                FileTypeSimple::Directory.into(),
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: FileTypeSimple::Directory.into(),
                file_name,
            })
            .unwrap();
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![];
            let result = self
                .client
                .call_remote(
                    &address,
                    OperationType::DirectoryAddEntry as u32,
                    0,
                    &parent_dir,
                    &send_meta_data,
                    &[],
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
                    error!("Create dir: DirectoryAddEntry failed: {} ,{:?}", path, e);
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )));
                }
            };
        }

        self.meta_engine.create_directory(path, mode)
    }

    pub async fn delete_dir(&self, path: &str) -> Result<(), EngineError> {
        if !self.meta_engine.is_exist(path)? {
            return Ok(());
        }
        let (parent_dir, file_name) = path_split(path)?;
        let (address, _lock) = self.get_server_address(&parent_dir).await;
        if self.address == address {
            info!(
                "local delete dir, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
            self.meta_engine.directory_delete_entry(
                &parent_dir,
                &file_name,
                FileTypeSimple::Directory.into(),
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: FileTypeSimple::Directory.into(),
                file_name,
            })
            .unwrap();
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let mut recv_meta_data = vec![];
            let mut recv_data = vec![];
            let result = self
                .client
                .call_remote(
                    &address,
                    OperationType::DirectoryDeleteEntry as u32,
                    0,
                    &parent_dir,
                    &send_meta_data,
                    &[],
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
                e => {
                    error!("Delete dir: DirectoryDeleteEntry failed: {} ,{:?}", path, e);
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )));
                }
            };
        }
        debug!("delete_directory: {}", path);
        self.meta_engine.delete_directory(path)
    }

    pub async fn read_dir(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        self.meta_engine.read_directory(path, size, offset)
    }

    pub async fn create_file(
        &self,
        path: &str,
        oflag: i32,
        umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        debug!("create file: {}", path);
        if self.meta_engine.is_exist(path)? {
            if (oflag & O_EXCL) != 0 {
                return Err(EngineError::Exist);
            } else {
                return self.get_file_attr(path).await;
            }
        }
        let (parent_dir, file_name) = path_split(path)?;
        let (address, _lock) = self.get_server_address(&parent_dir).await;
        if self.address == address {
            info!(
                "local create file, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
            self.meta_engine.directory_add_entry(
                &parent_dir,
                &file_name,
                FileTypeSimple::RegularFile.into(),
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: FileTypeSimple::RegularFile.into(),
                file_name,
            })
            .unwrap();
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let result = self
                .client
                .call_remote(
                    &address,
                    OperationType::DirectoryAddEntry as u32,
                    0,
                    &parent_dir,
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
                        return Err(status.into());
                    }
                }
                e => {
                    error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )));
                }
            };
        }

        self.storage_engine.create_file(path, oflag, umask, mode)
    }

    pub async fn delete_file(&self, path: &str) -> Result<(), EngineError> {
        if !self.meta_engine.is_exist(path)? {
            return Ok(());
        }

        let (parent_dir, file_name) = path_split(path)?;
        let (address, _lock) = self.get_server_address(&parent_dir).await;
        if self.address == address {
            info!(
                "local delete file, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
            self.meta_engine.directory_delete_entry(
                &parent_dir,
                &file_name,
                FileTypeSimple::RegularFile.into(),
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: FileTypeSimple::RegularFile.into(),
                file_name,
            })
            .unwrap();
            let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
                (0, 0, 0, 0);
            let result = self
                .client
                .call_remote(
                    &address,
                    OperationType::DirectoryDeleteEntry as u32,
                    0,
                    &parent_dir,
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
                        return Err(status.into());
                    }
                }
                e => {
                    error!(
                        "Delete file: DirectoryDeleteEntry failed: {} ,{:?}",
                        path, e
                    );
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )));
                }
            };
        }

        self.storage_engine.delete_file(path)
    }

    pub async fn truncate_file(&self, path: &str, length: i64) -> Result<(), EngineError> {
        // a temporary implementation
        self.storage_engine.truncate_file(path, length)
    }

    pub async fn read_file(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        self.storage_engine.read_file(path, size, offset)
    }

    pub async fn write_file(
        &self,
        path: &str,
        data: &[u8],
        offset: i64,
    ) -> Result<usize, EngineError> {
        self.storage_engine.write_file(path, data, offset)
    }

    pub async fn get_file_attr(&self, path: &str) -> Result<Vec<u8>, EngineError> {
        self.meta_engine.get_file_attr_raw(path)
    }

    pub async fn open_file(&self, path: &str, flag: i32, mode: u32) -> Result<(), EngineError> {
        if (flag & O_CREAT) != 0 {
            self.create_file(path, flag, 0, mode).await.map(|_v| ())
        } else if (flag & O_DIRECTORY) != 0 {
            Ok(())
        } else {
            self.storage_engine.open_file(path, flag, mode)
        }
    }

    pub async fn directory_add_entry(&self, path: &str, file_name: String, file_type: u8) -> i32 {
        match self
            .meta_engine
            .directory_add_entry(path, &file_name, file_type)
        {
            Ok(()) => 0,
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
}

// #[cfg(test)]
// mod tests {
//     // use super::{enginerpc::enginerpc_client::EnginerpcClient, RPCService};
//     use crate::rpc::server::Server;
//     use crate::server::storage_engine::meta_engine::MetaEngine;
//     use crate::server::storage_engine::{file_engine::FileEngine, StorageEngine};
//     use crate::server::{DistributedEngine, EngineError, FileRequestHandler};
//     use libc::mode_t;
//     use std::sync::Arc;
//     use tokio::time::sleep;

//     async fn add_server(
//         database_path: String,
//         storage_path: String,
//         server_address: String,
//     ) -> Arc<DistributedEngine<FileEngine>> {
//         let meta_engine = Arc::new(MetaEngine::new(
//             &database_path,
//             128 << 20,
//             128 * 1024 * 1024,
//         ));
//         let local_storage = Arc::new(FileEngine::new(&storage_path, Arc::clone(&meta_engine)));
//         local_storage.init();

//         let engine = Arc::new(DistributedEngine::new(
//             server_address.clone(),
//             local_storage,
//             meta_engine,
//         ));
//         let handler = Arc::new(FileRequestHandler::new(engine.clone()));
//         let server = Server::new(handler, &server_address);
//         tokio::spawn(async move {
//             server.run().await.unwrap();
//         });
//         sleep(std::time::Duration::from_millis(3000)).await;
//         engine
//     }

//     async fn test_file(engine0: Arc<DistributedEngine<FileEngine>>) {
//         let mode: mode_t = 0o777;
//         let oflag = libc::O_CREAT | libc::O_RDWR;
//         // end with '/', not expected
//         // match engine0.create_file("/test/".into(), mode).await {
//         //     Err(EngineError::IsDir) => assert!(true),
//         //     _ => assert!(false),
//         // };
//         match engine0.create_file("/test".into(), oflag, 0, mode).await {
//             Ok(_) => assert!(true),
//             _ => assert!(false),
//         };
//         // repeat the same file
//         match engine0.create_file("/test".into(), oflag, 0, mode).await {
//             Ok(_) => assert!(true),
//             _ => assert!(false),
//         };
//         match engine0.delete_file("/test".into()).await {
//             Ok(_) => assert!(true),
//             _ => assert!(false),
//         };
//         println!("OK test_file");
//     }

//     async fn test_dir(
//         engine0: Arc<DistributedEngine<FileEngine>>,
//         engine1: Arc<DistributedEngine<FileEngine>>,
//     ) {
//         let mode: mode_t = 0o777;
//         let oflag = libc::O_CREAT | libc::O_RDWR;
//         // not end with '/'
//         match engine1.create_dir("/test".into(), mode).await {
//             Ok(_) => assert!(true),
//             _ => assert!(false),
//         };
//         // repeat the same dir
//         match engine1.create_dir("/test".into(), mode).await {
//             Err(EngineError::Exist) => assert!(true),
//             _ => assert!(false),
//         };
//         // dir add file
//         match engine0.create_file("/test/t1".into(), oflag, 0, mode).await {
//             Ok(_) => assert!(true),
//             _ => assert!(false),
//         };
//         // dir has file
//         match engine1.delete_dir("/test".into()).await {
//             Err(EngineError::NotEmpty) => assert!(true),
//             _ => assert!(false),
//         };
//         match engine0.delete_file("/test/t1".into()).await {
//             Ok(()) => assert!(true),
//             _ => assert!(false),
//         };
//         match engine1.delete_dir("/test".into()).await {
//             Ok(()) => assert!(true),
//             _ => assert!(false),
//         };
//         println!("OK test_dir");
//     }

//     #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//     async fn test_all() {
//         let address0 = "127.0.0.1:18080".to_string();
//         let address1 = "127.0.0.1:18081".to_string();
//         let engine0 = add_server(
//             "/tmp/test_file_db0".into(),
//             "/tmp/test0".into(),
//             address0.clone(),
//         )
//         .await;
//         let engine1 = add_server(
//             "/tmp/test_file_db1".into(),
//             "/tmp/test1".into(),
//             address1.clone(),
//         )
//         .await;
//         println!("add_server success!");
//         engine0.add_connection(address1).await;
//         engine1.add_connection(address0).await;
//         println!("add_connection success!");
//         test_file(engine0.clone()).await;
//         test_dir(engine0.clone(), engine1.clone()).await;
//         engine0.client.close();
//         engine1.client.close();
//     }
// }
