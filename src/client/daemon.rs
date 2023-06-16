// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use fuser::{BackgroundSession, MountOption};
use log::{error, info};

use crate::{
    common::errors::CONNECTION_ERROR,
    rpc::{
        client::{add_unix_connection, RpcClient},
        server::Handler,
    },
};

use super::{fuse_client::Client, SealFS};
const MOUNT: u32 = 1;
const PROBE: u32 = 2;
const UMOUNT: u32 = 3;

pub struct SealfsFused {
    pub client: Arc<Client>,
    pub mount_points: DashMap<String, BackgroundSession>,
}

// TODO: remove this
// replace fuser with other fuse library which supports async.
// better for performance too.
unsafe impl std::marker::Sync for SealfsFused {}
unsafe impl std::marker::Send for SealfsFused {}

impl SealfsFused {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            mount_points: DashMap::new(),
        }
    }
}

#[async_trait]
impl Handler for SealfsFused {
    async fn dispatch(
        &self,
        _id: u32,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        _data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        match operation_type {
            MOUNT => {
                let mountpoint = std::str::from_utf8(&path).unwrap();
                let volume_name = std::str::from_utf8(&metadata).unwrap();
                let mut options = vec![MountOption::RW, MountOption::FSName("seal".to_string())];
                options.push(MountOption::AutoUnmount);
                options.push(MountOption::AllowRoot);
                let result = self.client.init_volume(volume_name).await;
                match result {
                    Ok(inode) => {
                        info!("mounting volume {} to {}", volume_name, mountpoint);
                        match fuser::spawn_mount2(
                            SealFS::new(self.client.clone(), inode),
                            mountpoint,
                            &options,
                        ) {
                            Ok(session) => {
                                info!("mount success");
                                self.mount_points.insert(mountpoint.to_string(), session);
                                Ok((0, 0, 0, 0, vec![], vec![]))
                            }
                            Err(e) => {
                                error!("mount error: {}", e);
                                Ok((libc::EIO, 0, 0, 0, vec![], vec![]))
                            }
                        }
                    }
                    Err(e) => {
                        error!("mount error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            UMOUNT => {
                let mountpoint = std::str::from_utf8(&path).unwrap();
                info!("unmounting volume {}", mountpoint);
                match self.mount_points.remove(mountpoint) {
                    Some(_) => Ok((0, 0, 0, 0, vec![], vec![])),
                    None => {
                        error!("mountpoint not found: {}", mountpoint);
                        Ok((libc::ENOENT, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            PROBE => {
                info!("probe");
                Ok((0, 0, 0, 0, vec![], vec![]))
            }
            _ => {
                error!("operation_type not found: {}", operation_type);
                Err(anyhow::anyhow!("operation_type not found"))
            }
        }
    }
}

pub struct LocalCli {
    pub client: Arc<RpcClient<tokio::net::unix::OwnedWriteHalf, tokio::net::unix::OwnedReadHalf>>,
    path: String,
}

impl LocalCli {
    pub fn new(path: String) -> Self {
        Self {
            client: Arc::new(RpcClient::new()),
            path,
        }
    }

    pub async fn add_connection(&self, path: &str) {
        add_unix_connection(path, &self.client).await
    }

    pub async fn mount(&self, volume_name: &str, mount_point: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                &self.path,
                MOUNT,
                0,
                mount_point,
                volume_name.as_bytes(),
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
                error!("mount volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn umount(&self, mount_point: &str) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                &self.path,
                UMOUNT,
                0,
                mount_point,
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
                error!("umount volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn probe(&self) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let result = self
            .client
            .call_remote(
                &self.path,
                PROBE,
                0,
                "",
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
                error!("probe failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }
}
