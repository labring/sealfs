use self::manager::{
    manager_server::Manager, manager_server::ManagerServer, HeartRequest, HeartResponse,
    MetadataRequest, MetadataResponse,
};
use super::heart::Heart;
use tonic::{Request, Response, Status};

pub mod manager {
    tonic::include_proto!("manager");
}

#[derive(Default)]
pub struct ManagerService {
    pub heart: Heart,
}

pub fn new_manager_service(service: ManagerService) -> ManagerServer<ManagerService> {
    ManagerServer::new(service)
}

#[tonic::async_trait]
impl Manager for ManagerService {
    async fn send_heart(
        &self,
        request: Request<HeartRequest>,
    ) -> Result<Response<HeartResponse>, Status> {
        let message = request.get_ref();
        self.heart
            .register_server(message.address.clone(), message.lifetime.clone())
            .await;
        let response = HeartResponse { status: 0 };
        Ok(Response::new(response))
    }

    async fn get_metadata(
        &self,
        _request: Request<MetadataRequest>,
    ) -> Result<Response<MetadataResponse>, Status> {
        let mut vec: Vec<String> = vec![];
        self.heart.instances.iter().for_each(|instance| {
            let key = instance.key();
            vec.push(key.clone());
        });
        let response = MetadataResponse { instances: vec };
        Ok(Response::new(response))
    }
}
