use self::enginerpc::{
    enginerpc_server::Enginerpc, enginerpc_server::EnginerpcServer, EngineRequest, EngineResponse,
};

use crate::storage_engine::StorageEngine;
use std::sync::Arc;
use tonic::{Request, Response, Status};
pub mod enginerpc {
    tonic::include_proto!("enginerpc");
}

pub struct RPCService<Storage: StorageEngine> {
    pub local_storage: Arc<Storage>,
}

pub fn new_manager_service<
    Storage: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
>(
    service: RPCService<Storage>,
) -> EnginerpcServer<RPCService<Storage>> {
    EnginerpcServer::new(service)
}
impl<Storage: StorageEngine> RPCService<Storage> {
    pub fn new(local_storage: Arc<Storage>) -> Self {
        Self { local_storage }
    }
}
#[tonic::async_trait]
impl<Storage: StorageEngine + std::marker::Send + std::marker::Sync + 'static> Enginerpc
    for RPCService<Storage>
{
    async fn directory_add_entry(
        &self,
        request: Request<EngineRequest>,
    ) -> Result<Response<EngineResponse>, Status> {
        let message = request.get_ref();
        println!("add_entry {:?}", message);
        let result = self
            .local_storage
            .directory_add_entry(message.parentdir.clone(), message.filename.clone());
        let status = match result {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        let response = EngineResponse { status };
        Ok(Response::new(response))
    }
    async fn directory_delete_entry(
        &self,
        request: Request<EngineRequest>,
    ) -> Result<Response<EngineResponse>, Status> {
        let message = request.get_ref();
        println!("del_entry {:?}", message);
        let result = self
            .local_storage
            .directory_delete_entry(message.parentdir.clone(), message.filename.clone());
        let status = match result {
            Ok(()) => 0,
            Err(value) => value.into(),
        };
        let response = EngineResponse { status };
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::{enginerpc::enginerpc_client::EnginerpcClient, RPCService};
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
        let service = RPCService::new(local_storage.clone());
        let local = local_distributed_address.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(super::new_manager_service(service))
                .serve((&local).parse().unwrap())
                .await
        });

        Arc::new(DistributedEngine::new(
            local_distributed_address,
            local_storage,
        ))
    }

    async fn test_file(address0: String, address1: String) {
        let engine0 = add_server(
            "/tmp/test_file_db0".into(),
            "/tmp/test0".into(),
            address0.clone(),
        )
        .await;
        add_server(
            "/tmp/test_file_db1".into(),
            "/tmp/test1".into(),
            address1.clone(),
        )
        .await;
        let arc_engine0 = engine0.clone();
        tokio::spawn(async move {
            let connection = EnginerpcClient::connect(format!("http://{}", address1.clone()))
                .await
                .unwrap();
            arc_engine0.add_connection(address1.clone(), connection);
            println!("connected");
        });
        sleep(std::time::Duration::from_millis(5000)).await;
        engine0.delete_file("/test".into()).await.unwrap();

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
    }

    async fn test_dir(address0: String, address1: String) {
        let engine0 = add_server(
            "/tmp/test_dir_db0".into(),
            "/tmp/test2".into(),
            address0.clone(),
        )
        .await;
        add_server(
            "/tmp/test_dir_db1".into(),
            "/tmp/test3".into(),
            address1.clone(),
        )
        .await;
        let arc_engine0 = engine0.clone();
        tokio::spawn(async move {
            let connection = EnginerpcClient::connect(format!("http://{}", address1.clone()))
                .await
                .unwrap();
            arc_engine0.add_connection(address1.clone(), connection);
            println!("connected");
        });
        sleep(std::time::Duration::from_millis(5000)).await;
        engine0.delete_file("/test/t1".into()).await.unwrap();
        engine0.delete_dir("/test/".into()).await.unwrap();

        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        // not end with '/'
        match engine0.create_dir("/test".into(), mode).await {
            Err(EngineError::NotDir) => assert!(true),
            _ => assert!(false),
        };
        match engine0.create_dir("/test/".into(), mode).await {
            core::result::Result::Ok(()) => assert!(true),
            _ => assert!(false),
        };
        // repeat the same dir
        match engine0.create_dir("/test/".into(), mode).await {
            Err(EngineError::Exist) => assert!(true),
            _ => assert!(false),
        };
        // dir add file
        match engine0.create_file("/test/t1".into(), mode).await {
            Ok(_) => assert!(true),
            _ => assert!(false),
        };
        // dir has file
        match engine0.delete_dir("/test/".into()).await {
            Err(EngineError::NotEmpty) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_file("/test/t1".into()).await {
            Ok(()) => assert!(true),
            _ => assert!(false),
        };
        match engine0.delete_dir("/test/".into()).await {
            Ok(()) => assert!(true),
            _ => assert!(false),
        };
    }

    #[tokio::test]
    async fn test_all() {
        let address0 = "127.0.0.1:8080".to_string();
        let address1 = "127.0.0.1:8081".to_string();
        build_hash_ring(vec![address0.clone(), address1.clone()]);
        test_file(address0.clone(), address1.clone()).await;
        test_dir(address0, address1).await;
    }
}
