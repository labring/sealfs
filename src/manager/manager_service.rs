use crate::{
    common::serialization::{
        AddNodesSendMetaData, DeleteNodesSendMetaData, GetClusterStatusRecvMetaData,
        GetHashRingInfoRecvMetaData, ManagerOperationType,
    },
    rpc::server::Handler,
};

use super::{core::Manager, heart::Heart};

use async_trait::async_trait;
use log::{debug, error};
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
        _id: u32,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        let r#type = ManagerOperationType::try_from(operation_type).unwrap();
        match r#type {
            ManagerOperationType::SendHeart => {
                let request: SendHeartRequest = bincode::deserialize(&metadata).unwrap();
                debug!("{}", request.lifetime);
                self.heart
                    .register_server(request.address, request.lifetime)
                    .await;

                Ok((0, 0, 0, 0, Vec::new(), Vec::new()))
            }
            ManagerOperationType::GetMetadata => {
                let _request: MetadataRequest = bincode::deserialize(&metadata).unwrap();
                let mut response = MetadataResponse::default();
                self.heart.instances.iter().for_each(|instance| {
                    let key = instance.key();
                    response.instances.push(key.to_owned());
                });
                let response_meta_data = bincode::serialize(&response).unwrap();
                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            ManagerOperationType::GetClusterStatus => {
                let status = self.manager.get_cluster_status();
                let response_meta_data =
                    bincode::serialize(&GetClusterStatusRecvMetaData { status }).unwrap();
                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            ManagerOperationType::GetHashRing => {
                let hash_ring_info = self.manager.get_hash_ring_info();
                let response_meta_data =
                    bincode::serialize(&GetHashRingInfoRecvMetaData { hash_ring_info }).unwrap();
                Ok((
                    0,
                    0,
                    response_meta_data.len(),
                    0,
                    response_meta_data,
                    Vec::new(),
                ))
            }
            ManagerOperationType::GetNewHashRing => match self.manager.get_new_hash_ring_info() {
                Ok(hash_ring_info) => {
                    let response_meta_data =
                        bincode::serialize(&GetHashRingInfoRecvMetaData { hash_ring_info })
                            .unwrap();
                    Ok((
                        0,
                        0,
                        response_meta_data.len(),
                        0,
                        response_meta_data,
                        Vec::new(),
                    ))
                }
                Err(e) => {
                    error!("get new hash ring error: {}", e);
                    Ok((libc::ENOENT, 0, 0, 0, Vec::new(), Vec::new()))
                }
            },
            ManagerOperationType::AddNodes => {
                let new_servers_info = bincode::deserialize::<AddNodesSendMetaData>(&metadata)
                    .unwrap()
                    .new_servers_info;
                match self.manager.add_nodes(new_servers_info) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("add nodes error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::RemoveNodes => {
                let deleted_servers_info =
                    bincode::deserialize::<DeleteNodesSendMetaData>(&metadata)
                        .unwrap()
                        .deleted_servers_info;
                match self.manager.delete_nodes(deleted_servers_info) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("remove nodes error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::UpdateServerStatus => {
                match self.manager.set_server_status(
                    String::from_utf8(path).unwrap(),
                    bincode::deserialize(&metadata).unwrap(),
                ) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("update server status error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            _ => todo!(),
        }
    }
}
