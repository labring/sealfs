use super::storage_engine::meta_engine::MetaEngine;
use super::EngineError;
use super::{path_split, storage_engine::StorageEngine};
use crate::common::{
    distribute_hash_table::{hash, index_selector},
    serialization::{DirectoryEntrySendMetaData, OperationType},
};

use crate::rpc::client::Client;
use libc::{O_CREAT, O_DIRECTORY, O_EXCL};
use log::debug;
use std::{sync::Arc, vec};
use tokio::time::Duration;
pub struct DistributedEngine<Storage: StorageEngine> {
    pub address: String,
    pub storage_engine: Arc<Storage>,
    pub meta_engine: Arc<MetaEngine>,
    pub client: Client,
}

pub fn get_connection_address(path: &str) -> Option<String> {
    Some(index_selector(hash(path)))
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
        }
    }

    pub async fn add_connection(&self, address: String) {
        loop {
            if self.client.add_connection(&address).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn remove_connection(&self, address: String) {
        self.client.remove_connection(&address);
    }

    pub async fn create_dir(&self, path: String, mode: u32) -> Result<Vec<u8>, EngineError> {
        if self.meta_engine.is_exist(path.as_str())? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            debug!(
                "local create dir, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
            self.meta_engine.directory_add_entry(
                parent_dir.as_str(),
                file_name.as_str(),
                fuser::FileType::Directory as u8,
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: fuser::FileType::Directory as u8,
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

        self.meta_engine.create_directory(&path, mode)
    }

    pub async fn delete_dir(&self, path: String) -> Result<(), EngineError> {
        if !self.meta_engine.is_exist(&path)? {
            return Ok(());
        }
        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.meta_engine.directory_delete_entry(
                &parent_dir,
                &file_name,
                fuser::FileType::Directory as u8,
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: fuser::FileType::Directory as u8,
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
        debug!("delete_directory: {}", path);
        self.meta_engine.delete_directory(&path)
    }

    pub async fn read_dir(
        &self,
        path: String,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        self.meta_engine.read_directory(&path, size, offset)
    }

    pub async fn create_file(
        &self,
        path: String,
        oflag: i32,
        umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        debug!("create file: {}", path);
        if self.meta_engine.is_exist(&path)? {
            if (oflag & O_EXCL) != 0 {
                return Err(EngineError::Exist);
            } else {
                return self.get_file_attr(path).await;
            }
        }
        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            debug!("create file local: {}", path);
            self.meta_engine.directory_add_entry(
                &parent_dir,
                &file_name,
                fuser::FileType::RegularFile as u8,
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: fuser::FileType::RegularFile as u8,
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

        self.storage_engine.create_file(path, oflag, umask, mode)
    }

    pub async fn delete_file(&self, path: String) -> Result<(), EngineError> {
        if !self.meta_engine.is_exist(&path)? {
            return Ok(());
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            self.meta_engine.directory_delete_entry(
                &parent_dir,
                &file_name,
                fuser::FileType::RegularFile as u8,
            )?;
        } else {
            let send_meta_data = bincode::serialize(&DirectoryEntrySendMetaData {
                file_type: fuser::FileType::RegularFile as u8,
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

        self.storage_engine.delete_file(path)
    }

    pub async fn truncate_file(&self, path: String, length: i64) -> Result<(), EngineError> {
        // a temporary implementation
        self.storage_engine.truncate_file(path, length)
    }

    pub async fn read_file(
        &self,
        path: String,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        self.storage_engine.read_file(path, size, offset)
    }

    pub async fn write_file(
        &self,
        path: String,
        data: &[u8],
        offset: i64,
    ) -> Result<usize, EngineError> {
        self.storage_engine.write_file(path, data, offset)
    }

    pub async fn get_file_attr(&self, path: String) -> Result<Vec<u8>, EngineError> {
        self.meta_engine.get_file_attr_raw(&path)
    }

    pub async fn open_file(&self, path: String, flag: i32, mode: u32) -> Result<(), EngineError> {
        if (flag & O_CREAT) != 0 {
            self.create_file(path, flag, 0, mode).await.map(|_v| ())
        } else if (flag & O_DIRECTORY) != 0 {
            Ok(())
        } else {
            self.storage_engine.open_file(path, flag, mode)
        }
    }

    pub async fn directory_add_entry(&self, path: String, file_name: String, file_type: u8) -> i32 {
        let status: u32 = match self
            .meta_engine
            .directory_add_entry(&path, &file_name, file_type)
        {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        status as i32
    }

    pub async fn directory_delete_entry(
        &self,
        path: String,
        file_name: String,
        file_type: u8,
    ) -> i32 {
        let status: u32 = match self
            .meta_engine
            .directory_delete_entry(&path, &file_name, file_type)
        {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        status as i32
    }
}

#[cfg(test)]
mod tests {
    // use super::{enginerpc::enginerpc_client::EnginerpcClient, RPCService};
    use crate::common::distribute_hash_table::build_hash_ring;
    use crate::rpc::server::Server;
    use crate::server::storage_engine::meta_engine::MetaEngine;
    use crate::server::storage_engine::{file_engine::FileEngine, StorageEngine};
    use crate::server::{DistributedEngine, EngineError, FileRequestHandler};
    use libc::mode_t;
    use std::sync::Arc;
    use tokio::time::sleep;

    async fn add_server(
        database_path: String,
        storage_path: String,
        server_address: String,
    ) -> Arc<DistributedEngine<FileEngine>> {
        let meta_engine = Arc::new(MetaEngine::new(
            &database_path,
            128 << 20,
            128 * 1024 * 1024,
        ));
        let local_storage = Arc::new(FileEngine::new(&storage_path, Arc::clone(&meta_engine)));
        local_storage.init();

        let engine = Arc::new(DistributedEngine::new(
            server_address.clone(),
            local_storage,
            meta_engine,
        ));
        let handler = Arc::new(FileRequestHandler::new(engine.clone()));
        let server = Server::new(handler, &server_address);
        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        sleep(std::time::Duration::from_millis(3000)).await;
        engine
    }

    async fn test_file(engine0: Arc<DistributedEngine<FileEngine>>) {
        let mode: mode_t = 0o777;
        let oflag = libc::O_CREAT | libc::O_RDWR;
        // end with '/', not expected
        // match engine0.create_file("/test/".into(), mode).await {
        //     Err(EngineError::IsDir) => assert!(true),
        //     _ => assert!(false),
        // };
        match engine0.create_file("/test".into(), oflag, 0, mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // repeat the same file
        match engine0.create_file("/test".into(), oflag, 0, mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_file("/test".into()).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        println!("OK test_file");
    }

    async fn test_dir(
        engine0: Arc<DistributedEngine<FileEngine>>,
        engine1: Arc<DistributedEngine<FileEngine>>,
    ) {
        let mode: mode_t = 0o777;
        let oflag = libc::O_CREAT | libc::O_RDWR;
        // not end with '/'
        match engine1.create_dir("/test".into(), mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // repeat the same dir
        match engine1.create_dir("/test".into(), mode).await {
            Err(EngineError::Exist) => assert!(true),
            _ => assert!(false),
        };
        // dir add file
        match engine0.create_file("/test/t1".into(), oflag, 0, mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // dir has file
        match engine1.delete_dir("/test".into()).await {
            Err(EngineError::NotEmpty) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_file("/test/t1".into()).await {
            Ok(()) => assert!(true),
            _ => assert!(false),
        };
        match engine1.delete_dir("/test".into()).await {
            Ok(()) => assert!(true),
            _ => assert!(false),
        };
        println!("OK test_dir");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_all() {
        let address0 = "127.0.0.1:18080".to_string();
        let address1 = "127.0.0.1:18081".to_string();
        build_hash_ring(vec![address0.clone(), address1.clone()]);
        let engine0 = add_server(
            "/tmp/test_file_db0".into(),
            "/tmp/test0".into(),
            address0.clone(),
        )
        .await;
        let engine1 = add_server(
            "/tmp/test_file_db1".into(),
            "/tmp/test1".into(),
            address1.clone(),
        )
        .await;
        println!("add_server success!");
        engine0.add_connection(address1).await;
        engine1.add_connection(address0).await;
        println!("add_connection success!");
        test_file(engine0.clone()).await;
        test_dir(engine0.clone(), engine1.clone()).await;
        engine0.client.close();
        engine1.client.close();
    }
}
