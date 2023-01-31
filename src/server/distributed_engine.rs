use super::storage_engine::StorageEngine;
use super::EngineError;
use crate::common::{
    distribute_hash_table::{hash, index_selector},
    serialization::OperationType,
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

//  path_split: the path should not be empty, and it does not end with a slash unless it is the root directory.
pub fn path_split(path: String) -> Result<(String, String), EngineError> {
    if path.is_empty() {
        return Err(EngineError::Path);
    }
    if path == "/" {
        return Err(EngineError::Path);
    }
    if path.ends_with('/') {
        return Err(EngineError::Path);
    }
    let index = match path.rfind('/') {
        Some(value) => value,
        None => return Err(EngineError::Path),
    };
    match index {
        0 => Ok(("/".into(), path[1..].into())),
        _ => Ok((path[..index].into(), path[(index + 1)..].into())),
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

    pub async fn create_dir(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError> {
        if self.local_storage.is_exist(path.clone())? {
            return Err(EngineError::Exist);
        }

        let (parent_dir, file_name) = path_split(path.clone())?;
        let address = get_connection_address(parent_dir.as_str()).unwrap();
        if self.address == address {
            debug!(
                "local create dir, path: {}, parent_dir: {}, file_name: {}",
                path, parent_dir, file_name
            );
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
                    &path,
                    &[],
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
                    &path,
                    &[],
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
                    &path,
                    &[],
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
                    &path,
                    &[],
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

    pub async fn directory_add_entry(&self, path: String) -> i32 {
        let status = match path_split(path) {
            Ok((parentdir, filename)) => {
                match self.local_storage.directory_add_entry(parentdir, filename) {
                    Ok(()) => 0,
                    Err(value) => value.into(),
                }
            }
            Err(value) => value.into(),
        };
        status as i32
    }

    pub async fn directory_delete_entry(&self, path: String) -> i32 {
        let status = match path_split(path) {
            Ok((parentdir, filename)) => {
                match self
                    .local_storage
                    .directory_delete_entry(parentdir, filename)
                {
                    Ok(()) => 0,
                    Err(value) => value.into(),
                }
            }
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
    use crate::server::storage_engine::{default_engine::DefaultEngine, StorageEngine};
    use crate::server::{DistributedEngine, EngineError, FileRequestHandler};
    use nix::sys::stat::Mode;
    use std::sync::Arc;
    use tokio::time::sleep;

    async fn add_server(
        database_path: String,
        storage_path: String,
        server_address: String,
    ) -> Arc<DistributedEngine<DefaultEngine>> {
        let local_storage = Arc::new(DefaultEngine::new(&database_path, &storage_path));
        local_storage.init();

        let engine = Arc::new(DistributedEngine::new(
            server_address.clone(),
            local_storage,
        ));
        let handler = Arc::new(FileRequestHandler::new(engine.clone()));
        let server = Server::new(handler, &server_address);
        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        sleep(std::time::Duration::from_millis(3000)).await;
        engine
    }

    async fn test_file(engine0: Arc<DistributedEngine<DefaultEngine>>) {
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        // end with '/', not expected
        // match engine0.create_file("/test/".into(), mode).await {
        //     Err(EngineError::IsDir) => assert!(true),
        //     _ => assert!(false),
        // };
        match engine0.create_file("/test".into(), mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // repeat the same file
        match engine0.create_file("/test".into(), mode).await {
            Err(EngineError::Exist) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_file("/test".into()).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        println!("OK test_file");
    }

    async fn test_dir(
        engine0: Arc<DistributedEngine<DefaultEngine>>,
        engine1: Arc<DistributedEngine<DefaultEngine>>,
    ) {
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
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
        match engine0.create_file("/test/t1".into(), mode).await {
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
        test_dir(engine0, engine1).await;
    }
}
