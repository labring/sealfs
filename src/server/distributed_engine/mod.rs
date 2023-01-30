pub mod engine_rpc;

use super::storage_engine::StorageEngine;
use super::EngineError;
use crate::common::{
    distribute_hash_table::{hash, index_selector},
    request::OperationType,
};

use crate::rpc::client::ClientAsync;
use log::debug;
use nix::sys::stat::Mode;
use std::{sync::Arc, vec};
pub struct DistributedEngine<Storage: StorageEngine> {
    pub address: String,
    pub local_storage: Arc<Storage>,
    pub client: ClientAsync,
}

fn path_split(path: String) -> Result<(String, String), EngineError> {
    if !path.ends_with('/') {
        let index = match path.rfind('/') {
            Some(value) => value,
            None => return Err(EngineError::Path),
        };
        Ok((path[..=index].into(), path[(index + 1)..].into()))
    } else {
        let index = match path[..path.len() - 1].rfind('/') {
            Some(value) => value,
            None => return Err(EngineError::Path),
        };
        Ok((path[..=index].into(), path[(index + 1)..].into()))
    }
}

pub fn get_connection_address(path: &str) -> Option<String> {
    Some(index_selector(hash(path)))
}

impl<Storage> DistributedEngine<Storage>
where
    Storage: StorageEngine,
{
    pub fn new(address: String, local_storage: Arc<Storage>) -> Self {
        Self {
            address,
            local_storage,
            client: ClientAsync::new(),
        }
    }

    pub async fn add_connection(&self, address: String) {
        self.client.add_connection(&address).await;
    }

    pub fn remove_connection(&self, address: String) {
        self.client.remove_connection(&address);
    }

    pub async fn create_dir(&self, path: String, mode: Mode) -> Result<(), EngineError> {
        if !path.ends_with('/') {
            return Err(EngineError::NotDir);
        }
        if self.local_storage.is_exist(path.clone())? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_add_entry(parent_dir, file_name)?;
        } else {
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
                    file_name.as_bytes(),
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
                        return Err(EngineError::from(status as u32));
                    }
                }
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
        }

        self.local_storage.create_directory(path, mode)
    }

    pub async fn delete_dir(&self, path: String) -> Result<(), EngineError> {
        if !path.ends_with('/') {
            return Err(EngineError::NotDir);
        }
        if !self.local_storage.is_exist(path.clone())? {
            return Ok(());
        }
        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_delete_entry(parent_dir.clone(), file_name.clone())?;
        } else {
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
                    file_name.as_bytes(),
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
                        return Err(EngineError::from(status as u32));
                    }
                }
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
        }
        self.local_storage.delete_directory(path.clone())
    }

    pub async fn read_dir(&self, path: String) -> Result<Vec<u8>, EngineError> {
        self.local_storage.read_directory(path)
    }

    pub async fn create_file(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError> {
        debug!("create file: {}", path);
        if path.ends_with('/') {
            return Err(EngineError::IsDir);
        }
        if self.local_storage.is_exist(path.clone())? {
            return Err(EngineError::Exist);
        }
        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            debug!("create file local: {}", path);
            self.local_storage
                .directory_add_entry(parent_dir, file_name)?;
        } else {
            debug!("create file remote: {}", path);
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
                    file_name.as_bytes(),
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
                        return Err(EngineError::from(status as u32));
                    }
                }
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
        }

        self.local_storage.create_file(path, mode)
    }

    pub async fn delete_file(&self, path: String) -> Result<(), EngineError> {
        if path.ends_with('/') {
            return Err(EngineError::IsDir);
        }
        if !self.local_storage.is_exist(path.clone())? {
            return Ok(());
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_delete_entry(parent_dir, file_name)?;
        } else {
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
                    file_name.as_bytes(),
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
                        return Err(EngineError::from(status as u32));
                    }
                }
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
        }

        self.local_storage.delete_file(path)
    }

    pub async fn read_file(
        &self,
        path: String,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        self.local_storage.read_file(path, size, offset)
    }

    pub async fn write_file(
        &self,
        path: String,
        data: &[u8],
        offset: i64,
    ) -> Result<usize, EngineError> {
        self.local_storage.write_file(path, data, offset)
    }

    pub async fn get_file_attr(&self, path: String) -> Result<Vec<u8>, EngineError> {
        self.local_storage.get_file_attributes(path)
    }

    pub async fn open_file(&self, path: String, mode: Mode) -> Result<(), EngineError> {
        self.local_storage.open_file(path, mode)
    }
}
