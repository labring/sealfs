use crate::storage_engine::StorageEngine;
use common::request::OperationType;
use rpc::server::Handler;
use std::sync::Arc;
pub struct DistributedHandler<Storage: StorageEngine> {
    pub local_storage: Arc<Storage>,
}

impl<Storage: StorageEngine> DistributedHandler<Storage> {
    pub fn new(local_storage: Arc<Storage>) -> Self {
        Self { local_storage }
    }
    fn directory_add_entry(&self, parentdir: String, filename: String) -> i32 {
        let result = self.local_storage.directory_add_entry(parentdir, filename);
        let status = match result {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        status as i32
    }

    fn directory_delete_entry(&self, parentdir: String, filename: String) -> i32 {
        let result = self
            .local_storage
            .directory_delete_entry(parentdir, filename);
        let status = match result {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        status as i32
    }
}

#[tonic::async_trait]
impl<Storage: StorageEngine + std::marker::Send + std::marker::Sync + 'static> Handler
    for DistributedHandler<Storage>
{
    async fn dispatch(
        &self,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)> {
        let operation_type = OperationType::try_from(operation_type).unwrap();
        match operation_type {
            OperationType::DirectoryAddEntry => {
                let parentdir = String::from_utf8(path).unwrap();
                let filename = String::from_utf8(metadata).unwrap();
                Ok((
                    self.directory_add_entry(parentdir, filename),
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::DirectoryDeleteEntry => {
                let parentdir = String::from_utf8(path).unwrap();
                let filename = String::from_utf8(metadata).unwrap();
                Ok((
                    self.directory_delete_entry(parentdir, filename),
                    0,
                    vec![],
                    vec![],
                ))
            }
            _ => Ok((-1, 0, vec![], vec![])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DistributedHandler;
    // use super::{enginerpc::enginerpc_client::EnginerpcClient, RPCService};
    use crate::storage_engine::{default_engine::DefaultEngine, StorageEngine};
    use crate::{DistributedEngine, EngineError};
    use common::distribute_hash_table::build_hash_ring;
    use nix::sys::stat::Mode;
    use std::sync::Arc;
    use tokio::time::sleep;

    async fn add_server(
        database_path: String,
        storage_path: String,
        local_distributed_address: String,
    ) -> Arc<DistributedEngine<DefaultEngine>> {
        let local_storage = Arc::new(DefaultEngine::new(&database_path, &storage_path));
        local_storage.init();

        let service = local_storage.clone();
        let local = local_distributed_address.clone();
        tokio::spawn(async move {
            let server =
                rpc::server::Server::new(Arc::new(DistributedHandler::new(service)), &local);
            server.run().await.unwrap();
        });
        sleep(std::time::Duration::from_millis(3000)).await;
        Arc::new(DistributedEngine::new(
            local_distributed_address,
            local_storage,
        ))
    }

    async fn test_file(engine0: Arc<DistributedEngine<DefaultEngine>>) {
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        // end with '/'
        match engine0.create_file("/test/".into(), mode).await {
            Err(EngineError::IsDir) => assert!(true),
            _ => assert!(false),
        };
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
            Err(EngineError::NotDir) => assert!(true),
            _ => assert!(false),
        };
        match engine1.create_dir("/test/".into(), mode).await {
            core::result::Result::Ok(()) => assert!(true),
            _ => assert!(false),
        };
        // repeat the same dir
        match engine1.create_dir("/test/".into(), mode).await {
            Err(EngineError::Exist) => assert!(true),
            _ => assert!(false),
        };
        // dir add file
        match engine0.create_file("/test/t1".into(), mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // dir has file
        match engine1.delete_dir("/test/".into()).await {
            Err(EngineError::NotEmpty) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_file("/test/t1".into()).await {
            Ok(()) => assert!(true),
            _ => assert!(false),
        };
        match engine1.delete_dir("/test/".into()).await {
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
