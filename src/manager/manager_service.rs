// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use crate::{
    common::serialization::{
        AddNodesSendMetaData, ClusterStatus, DeleteNodesSendMetaData, GetClusterStatusRecvMetaData,
        GetHashRingInfoRecvMetaData, ManagerOperationType, ServerStatus,
    },
    rpc::server::Handler,
};

use super::core::Manager;

use async_trait::async_trait;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};

pub struct ManagerService {
    pub manager: Arc<Manager>,
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

pub async fn update_server_status(manager: Arc<Manager>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if manager.closed.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let status = *manager.cluster_status.lock().unwrap();
        debug!("current cluster status is {:?}", status);
        match status {
            ClusterStatus::Idle => {}
            ClusterStatus::NodesStarting => {
                // if all servers is ready, change the cluster status to SyncNewHashRing
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Finished);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::SyncNewHashRing;
                    info!("all servers is ready, change the cluster status to SyncNewHashRing");
                };
            }
            ClusterStatus::SyncNewHashRing => {
                // if all servers is ready, change the cluster status to PreTransfer
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::PreTransfer);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::PreTransfer;
                    info!("all servers is ready, change the cluster status to PreTransfer");
                }
            }
            ClusterStatus::PreTransfer => {
                // if all servers is ready, change the cluster status to Transferring
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Transferring);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::Transferring;
                    info!("all servers is ready, change the cluster status to Transferring");
                }
            }
            ClusterStatus::Transferring => {
                // if all servers is ready, change the cluster status to PreFinish
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::PreFinish);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::PreFinish;
                    info!("all servers is ready, change the cluster status to PreFinish");
                }
            }
            ClusterStatus::PreFinish => {
                // if all servers is ready, change the cluster status to Finishing
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Finishing);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let _ = manager
                        .hashring
                        .write()
                        .unwrap()
                        .replace(manager.new_hashring.read().unwrap().clone().unwrap());
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::Finishing;
                    info!("all servers is ready, change the cluster status to Finishing");
                }
            }
            ClusterStatus::Finishing => {
                // if all servers is ready, change the cluster status to Idle
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Finished);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let mut new_hashring = manager.new_hashring.write().unwrap();
                    manager
                        .servers
                        .lock()
                        .unwrap()
                        .retain(|k, _| new_hashring.as_ref().unwrap().contains(k));
                    // move new_hashring to hashring
                    let _ = new_hashring.take().unwrap();
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::Idle;
                    info!("all servers is ready, change the cluster status to Idle");
                }
            }
            ClusterStatus::Initializing => {
                // if all servers is ready, change the cluster status to Idle
                let flag = manager
                    .servers
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Finished);
                if flag {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    *manager.cluster_status.lock().unwrap() = ClusterStatus::Idle;
                    info!("all servers is ready, change the cluster status to Idle");
                }
            }
            s => panic!("update server status failed, invalid cluster status: {}", s),
        }
    }
}

impl ManagerService {
    pub fn new(servers: Vec<(String, usize)>) -> Self {
        let manager = Arc::new(Manager::new(servers));
        ManagerService { manager }
    }
}

#[async_trait]
impl Handler for ManagerService {
    async fn dispatch(
        &self,
        id: u32,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        let r#type = ManagerOperationType::try_from(operation_type).unwrap();
        match r#type {
            ManagerOperationType::GetClusterStatus => {
                let status = self.manager.get_cluster_status();
                let response_meta_data =
                    bincode::serialize(&GetClusterStatusRecvMetaData { status }).unwrap();

                debug!("connection {} get cluster status: {:?}", id, status);

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

                info!("connection {} get hash ring: {:?}", id, hash_ring_info);

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
                    info!("connection {} get new hash ring: {:?}", id, hash_ring_info);
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
                info!("connection {} add nodes: {:?}", id, new_servers_info);
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
                info!("connection {} remove nodes: {:?}", id, deleted_servers_info);
                match self.manager.delete_nodes(deleted_servers_info) {
                    None => Ok((0, 0, 0, 0, Vec::new(), Vec::new())),
                    Some(e) => {
                        error!("remove nodes error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, Vec::new(), Vec::new()))
                    }
                }
            }
            ManagerOperationType::UpdateServerStatus => {
                info!("connection {} update server status", id);
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
