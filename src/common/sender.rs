use std::sync::Arc;

use log::error;

use crate::{rpc::client::Client, server::EngineError};

use super::serialization::{
    AddNodesSendMetaData, CreateVolumeSendMetaData, DeleteNodesSendMetaData,
    GetClusterStatusRecvMetaData, GetHashRingInfoRecvMetaData, ManagerOperationType, OperationType,
};

pub struct Sender {
    pub client: Arc<Client>,
}

impl Sender {
    pub fn new(client: Arc<Client>) -> Self {
        Sender { client }
    }

    pub async fn add_new_servers(
        &self,
        manager_address: &str,
        new_servers_info: Vec<(String, usize)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get cluster status failed: status {}", status).into());
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn delete_servers(
        &self,
        manager_address: &str,
        deleted_servers_info: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get cluster status failed: status {}", status).into());
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_cluster_status(
        &self,
        manager_address: &str,
    ) -> Result<i32, Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get cluster status failed: status {}", status).into());
                }
                let cluster_status_meta_data: GetClusterStatusRecvMetaData =
                    bincode::deserialize(&recv_meta_data).unwrap();
                Ok(cluster_status_meta_data.status as i32)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get hash ring failed: status {}", status).into());
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_new_hash_ring_info(
        &self,
        manager_address: &str,
    ) -> Result<Vec<(String, usize)>, Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(format!("get new hash ring failed: status {}", status).into());
                }
                let hash_ring_meta_data: GetHashRingInfoRecvMetaData =
                    bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
                Ok(hash_ring_meta_data.hash_ring_info)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn create_volume(
        &self,
        address: &str,
        name: &str,
        size: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let send_meta_data = bincode::serialize(&CreateVolumeSendMetaData {
            name: name.to_string(),
            size,
        })
        .unwrap();

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                address,
                OperationType::CreateVolume.into(),
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    error!("create volume failed: status {}", status);
                    return Err(format!("create volume failed: status {}", status).into());
                }
                Ok(())
            }
            Err(e) => {
                error!("create volume failed: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn init_volume(
        &self,
        address: &str,
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    error!("init volume failed: status {}", status);
                    return Err(format!("init volume failed: status {}", status).into());
                }
                Ok(())
            }
            Err(e) => {
                error!("init volume failed: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn create_no_parent(
        &self,
        address: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<Vec<u8>, EngineError> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    error!("create file failed, status: {}", status);
                    Err(EngineError::IO)
                } else {
                    Ok(recv_meta_data[..recv_meta_data_length].to_vec())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub async fn delete_no_parent(
        &self,
        address: &str,
        operation_type: OperationType,
        parent: &str,
        send_meta_data: &[u8],
    ) -> Result<(), EngineError> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    error!("create file failed, status: {}", status);
                    Err(EngineError::IO)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                error!("create file failed with error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub async fn directory_add_entry(
        &self,
        address: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), EngineError> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status.into())
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(EngineError::StdIo(std::io::Error::from(
                    std::io::ErrorKind::NotConnected,
                )))
            }
        }
    }

    pub async fn directory_delete_entry(
        &self,
        address: &str,
        path: &str,
        send_meta_data: &[u8],
    ) -> Result<(), EngineError> {
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
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    Err(status.into())
                } else {
                    Ok(())
                }
            }
            e => {
                error!("Create file: DirectoryAddEntry failed: {} ,{:?}", path, e);
                Err(EngineError::StdIo(std::io::Error::from(
                    std::io::ErrorKind::NotConnected,
                )))
            }
        }
    }
}
