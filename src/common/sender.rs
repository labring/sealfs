// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// sender is used to send requests to the other sealfs servers

use std::{sync::Arc, time::Duration};

use log::error;

use crate::{
    common::errors::CONNECTION_ERROR,
    rpc::client::{RpcClient, TcpStreamCreator},
};

use super::serialization::{
    AddNodesSendMetaData, ClusterStatus, CreateVolumeSendMetaData, DeleteNodesSendMetaData,
    GetClusterStatusRecvMetaData, GetHashRingInfoRecvMetaData, ManagerOperationType, OperationType,
    Volume,
};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Sender {
    pub client: Arc<
        RpcClient<
            tokio::net::tcp::OwnedReadHalf,
            tokio::net::tcp::OwnedWriteHalf,
            TcpStreamCreator,
        >,
    >,
}

impl Sender {
    pub fn new(
        client: Arc<
            RpcClient<
                tokio::net::tcp::OwnedReadHalf,
                tokio::net::tcp::OwnedWriteHalf,
                TcpStreamCreator,
            >,
        >,
    ) -> Self {
        Sender { client }
    }

    pub async fn add_new_servers(
        &self,
        manager_address: &str,
        new_servers_info: Vec<(String, usize)>,
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data =
            bincode::serialize(&AddNodesSendMetaData { new_servers_info }).unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::AddNodes.into(),
                0,
                "",
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("add new servers failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_servers(
        &self,
        manager_address: &str,
        deleted_servers_info: Vec<String>,
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data = bincode::serialize(&DeleteNodesSendMetaData {
            deleted_servers_info,
        })
        .unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::RemoveNodes.into(),
                0,
                "",
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("delete servers failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_cluster_status(&self, manager_address: &str) -> Result<ClusterStatus, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 4];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetClusterStatus.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    let cluster_status_meta_data: GetClusterStatusRecvMetaData =
                        bincode::deserialize(&recv_meta_data).unwrap();
                    Ok(cluster_status_meta_data.status)
                }
            }
            Err(e) => {
                error!("get cluster status failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => {
                error!("get hash ring info failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn get_new_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                manager_address,
                ManagerOperationType::GetNewHashRing.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => {
                error!("get new hash ring info failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn list_volumes(&self, address: &str) -> Result<Vec<Volume>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 65535];

        let result = self
            .client
            .call_remote(
                address,
                OperationType::ListVolumes.into(),
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                let volumes: Vec<Volume> =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(volumes)
            }
            Err(e) => {
                error!("list volumes failed: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn create_volume(&self, address: &str, name: &str, size: u64) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data = bincode::serialize(&CreateVolumeSendMetaData { size }).unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                address,
                OperationType::CreateVolume.into(),
                0,
                name,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("create volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_volume(&self, address: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                address,
                OperationType::DeleteVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("delete volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn clean_volume(&self, address: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                address,
                OperationType::CleanVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("clean volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn init_volume(&self, address: &str, name: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                address,
                OperationType::InitVolume.into(),
                0,
                name,
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(())
            }
            Err(e) => {
                error!("init volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn create_no_parent(
        &self,
        address: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<Vec<u8>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
            .client
            .call_remote(
                address,
                operation_type.into(),
                0,
                parent,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(recv_meta_data[..recv_meta_data_length].to_vec())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn delete_no_parent(
        &self,
        address: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];

        let result = self
            .client
            .call_remote(
                address,
                operation_type.into(),
                0,
                parent,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn directory_add_entry(
        &self,
        address: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
            (0, 0, 0, 0);
        let result = self
            .client
            .call_remote(
                address,
                OperationType::DirectoryAddEntry as u32,
                0,
                path,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn directory_delete_entry(
        &self,
        address: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), i32> {
        let (mut status, mut rsp_flags, mut recv_meta_data_length, mut recv_data_length) =
            (0, 0, 0, 0);
        let result = self
            .client
            .call_remote(
                address,
                OperationType::DirectoryDeleteEntry as u32,
                0,
                path,
                send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut [],
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status)
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(CONNECTION_ERROR)
            }
        }
    }
}
