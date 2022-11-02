use self::manager::{
    manager_server::Manager, manager_server::ManagerServer, HeartRequest, HeartResponse,
};
use log::debug;
use tonic::{Request, Response, Status};

pub mod manager {
    tonic::include_proto!("manager");
}

#[derive(Default)]
pub struct ManagerService {}

pub fn new_manager_service(service: ManagerService) -> ManagerServer<ManagerService> {
    ManagerServer::new(service)
}

#[tonic::async_trait]
impl Manager for ManagerService {
    async fn send_heart(
        &self,
        request: Request<HeartRequest>,
    ) -> Result<Response<HeartResponse>, Status> {
        debug!("Got a request from {:?}", request.remote_addr());
        let response = HeartResponse { status: 0 };
        Ok(Response::new(response))
    }
}
