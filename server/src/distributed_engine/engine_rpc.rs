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
