use super::heart::Heart;

use crate::{common::request::OperationType, rpc::server::Handler};
use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};

#[derive(Default)]
pub struct ManagerService {
    pub heart: Heart,
}

#[derive(Serialize, Deserialize)]
pub struct SendHeartRequest {
    pub address: String,
    pub flags: u32,
    pub lifetime: String,
}

#[derive(Serialize, Deserialize)]
pub struct MetadataRequest {
    pub flags: u32,
}

#[derive(Default, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub instances: Vec<String>,
}

#[async_trait]
impl Handler for ManagerService {
    async fn dispatch(
        &self,
        operation_type: u32,
        _flags: u32,
        _path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)> {
        let r#type = OperationType::try_from(operation_type).unwrap();
        match r#type {
            OperationType::SendHeart => {
                let request: SendHeartRequest = bincode::deserialize(&metadata).unwrap();
                debug!("{}", request.lifetime);
                self.heart
                    .register_server(request.address, request.lifetime)
                    .await;

                Ok((0, 0, Vec::new(), Vec::new()))
            }
            OperationType::GetMetadata => {
                let _request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                Ok((0, 0, bincode::serialize(&response).unwrap(), Vec::new()))
            }
            _ => todo!(),
        }
    }
}
