use crate::{common::serialization::ManagerOperationType, rpc::server::Handler};

use super::{heart::Heart, manager::Manager};

use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};

pub struct ManagerService {
    pub heart: Heart,
    manager: Manager,
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

impl ManagerService {
    pub fn new(servers: Vec<(String, usize)>) -> Self {
        let heart = Heart::default();
        let manager = Manager::new(servers);
        ManagerService { heart, manager }
    }
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
        let r#type = ManagerOperationType::try_from(operation_type).unwrap();
        match r#type {
            ManagerOperationType::SendHeart => {
                let request: SendHeartRequest = bincode::deserialize(&metadata).unwrap();
                debug!("{}", request.lifetime);
                self.heart
                    .register_server(request.address, request.lifetime)
                    .await;

                Ok((0, 0, Vec::new(), Vec::new()))
            }
            ManagerOperationType::GetMetadata => {
                let _request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                Ok((0, 0, bincode::serialize(&response).unwrap(), Vec::new()))
            }
            ManagerOperationType::GetClusterStatus => {
                let status = self.manager.get_cluster_status();
                Ok((0, 0, bincode::serialize(&status).unwrap(), Vec::new()))
            }
            ManagerOperationType::GetHashRing => {
                let hashring = self.manager.get_hash_ring_info();
                Ok((0, 0, bincode::serialize(&hashring).unwrap(), Vec::new()))
            }
            ManagerOperationType::AddNodes => {
                let request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                Ok((0, 0, bincode::serialize(&response).unwrap(), Vec::new()))
            }
            ManagerOperationType::RemoveNodes => {
                let request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                Ok((0, 0, bincode::serialize(&response).unwrap(), Vec::new()))
            }
            ManagerOperationType::PreFinishServer => {
                let request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                Ok((0, 0, bincode::serialize(&response).unwrap(), Vec::new()))
            }
            ManagerOperationType::FinishServer => {
                let request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
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
