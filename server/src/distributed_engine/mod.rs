pub mod engine_rpc;

use crate::storage_engine::StorageEngine;
use crate::EngineError;
use common::distribute_hash_table::{hash, index_selector};

use dashmap::DashMap;
use engine_rpc::enginerpc::{enginerpc_client::EnginerpcClient, EngineRequest};
use std::sync::Arc;
use tonic::transport::Channel;

pub struct DistributedEngine<Storage: StorageEngine> {
    pub address: String,
    pub local_storage: Arc<Storage>,
    pub connections: DashMap<String, EnginerpcClient<Channel>>,
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

impl<Storage> DistributedEngine<Storage>
where
    Storage: StorageEngine,
{
    pub fn new(address: String, local_storage: Arc<Storage>) -> Self {
        Self {
            address,
            local_storage,
            connections: DashMap::new(),
        }
    }

    pub fn add_connection(&self, address: String, connection: EnginerpcClient<Channel>) {
        self.connections.insert(address, connection);
    }

    pub fn remove_connection(&self, address: String) {
        self.connections.remove(&address);
    }

    pub fn get_connection_address(&self, path: &str) -> Option<String> {
        Some(index_selector(hash(path)))
    }

    fn get_connection(&self, address: String) -> Result<EnginerpcClient<Channel>, EngineError> {
        let connect = { self.connections.get(&address) };
        match connect {
            Some(value) => Ok(value.value().clone()),
            None => Err(EngineError::StdIo(std::io::Error::from(
                std::io::ErrorKind::NotConnected,
            ))),
        }
    }

    pub async fn create_dir(&self, path: String) -> Result<(), EngineError> {
        if !path.ends_with('/') {
            return Err(EngineError::NotDir);
        }
        if self.local_storage.is_exist(path.clone())? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = self.get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_add_entry(parent_dir, file_name)?;
        } else {
            let request = tonic::Request::new(EngineRequest {
                parentdir: parent_dir,
                filename: file_name,
            });
            let mut connect = self.get_connection(address)?;
            let response = connect.directory_add_entry(request).await;

            let status = match response {
                Ok(value) => value.get_ref().status,
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
            if status != 0 {
                return Err(EngineError::from(status));
            }
        }

        self.local_storage.create_directory(path)
    }

    pub async fn delete_dir(&self, path: String) -> Result<(), EngineError> {
        if !path.ends_with('/') {
            return Err(EngineError::NotDir);
        }
        if !self.local_storage.is_exist(path.clone())? {
            return Ok(());
        }
        self.local_storage.delete_directory(path.clone())?;
        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = self.get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_delete_entry(parent_dir.clone(), file_name.clone())?;
        } else {
            let request = tonic::Request::new(EngineRequest {
                parentdir: parent_dir.clone(),
                filename: file_name.clone(),
            });
            let mut connect = self.get_connection(address.clone())?;
            let response = connect.directory_delete_entry(request).await;

            let status = match response {
                Ok(value) => value.get_ref().status,
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
            if status != 0 {
                self.local_storage.create_directory(path)?;
                return Err(EngineError::from(status));
            }
        }
        Ok(())
    }

    pub async fn read_dir(&self, path: String) -> Result<Vec<String>, EngineError> {
        // a temporary implementation
        self.local_storage.read_directory(path)
    }

    pub async fn create_file(&self, path: String) -> Result<(), EngineError> {
        if path.ends_with('/') {
            return Err(EngineError::IsDir);
        }
        if self.local_storage.is_exist(path.clone())? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = self.get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_add_entry(parent_dir, file_name)?;
        } else {
            let request = tonic::Request::new(EngineRequest {
                parentdir: parent_dir,
                filename: file_name,
            });
            let mut connect = self.get_connection(address)?;
            let response = connect.directory_add_entry(request).await;

            let status = match response {
                Ok(value) => value.get_ref().status,
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
            if status != 0 {
                return Err(EngineError::from(status));
            }
        }

        self.local_storage.create_file(path)
    }

    pub async fn delete_file(&self, path: String) -> Result<(), EngineError> {
        if path.ends_with('/') {
            return Err(EngineError::IsDir);
        }
        if !self.local_storage.is_exist(path.clone())? {
            return Ok(());
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = self.get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.local_storage
                .directory_delete_entry(parent_dir, file_name)?;
        } else {
            let request = tonic::Request::new(EngineRequest {
                parentdir: parent_dir,
                filename: file_name,
            });
            let mut connect = self.get_connection(address)?;
            let response = connect.directory_delete_entry(request).await;

            let status = match response {
                Ok(value) => value.get_ref().status,
                _ => {
                    return Err(EngineError::StdIo(std::io::Error::from(
                        std::io::ErrorKind::NotConnected,
                    )))
                }
            };
            if status != 0 {
                return Err(EngineError::from(status));
            }
        }

        self.local_storage.delete_file(path)
    }

    pub async fn read_file(&self, path: String) -> Result<Vec<u8>, EngineError> {
        // a temporary implementation
        self.local_storage.read_file(path)
    }

    pub async fn write_file(&self, path: String, data: &[u8]) -> Result<(), EngineError> {
        // a temporary implementation
        self.local_storage.write_file(path, data)
    }

    pub async fn get_file_attr(&self, path: String) -> Result<Option<Vec<String>>, EngineError> {
        // a temporary implementation
        self.local_storage.get_file_attributes(path)
    }
}
