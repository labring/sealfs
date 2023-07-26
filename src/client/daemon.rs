// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{Read, Write},
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use fuser::{BackgroundSession, MountOption};
use log::{error, info};

use crate::{
    common::{
        errors::CONNECTION_ERROR, sender::REQUEST_TIMEOUT, serialization::MountVolumeSendMetaData,
    },
    rpc::{
        client::{RpcClient, UnixStreamCreator},
        server::Handler,
    },
};

use super::{fuse_client::Client, SealFS};
const MOUNT: u32 = 1;
const PROBE: u32 = 2;
const UMOUNT: u32 = 3;
const LIST_MOUNTPOINTS: u32 = 4;

pub struct SealfsFused {
    pub client: Arc<Client>,
    pub mount_points: DashMap<String, (String, bool, BackgroundSession)>,
    pub index_file: String,
}

// TODO: remove this
// replace fuser with other fuse library which supports async.
// better for performance too.
unsafe impl std::marker::Sync for SealfsFused {}
unsafe impl std::marker::Send for SealfsFused {}

impl SealfsFused {
    pub fn new(index_file: String, client: Arc<Client>) -> Self {
        Self {
            client,
            mount_points: DashMap::new(),
            index_file,
        }
    }

    pub async fn mount(
        &self,
        mountpoint: String,
        volume_name: String,
        read_only: bool,
    ) -> Result<(), i32> {
        let mount_mode = if read_only {
            MountOption::RO
        } else {
            MountOption::RW
        };
        let mut options = vec![mount_mode, MountOption::FSName("seal".to_string())];
        options.push(MountOption::AutoUnmount);
        options.push(MountOption::AllowRoot);
        let result = self.client.init_volume(&volume_name).await;
        match result {
            Ok(inode) => {
                info!("mounting volume {} to {}", volume_name, mountpoint);
                match fuser::spawn_mount2(
                    SealFS::new(self.client.clone(), inode),
                    &mountpoint,
                    &options,
                ) {
                    Ok(session) => {
                        info!("mount success");
                        self.mount_points
                            .insert(mountpoint, (volume_name, read_only, session));
                        Ok(())
                    }
                    Err(e) => {
                        error!("mount error: {}", e);
                        Err(CONNECTION_ERROR)
                    }
                }
            }
            Err(e) => {
                error!("mount error: {}", e);
                Err(e)
            }
        }
    }

    pub fn unmount(&self, mountpoint: &str) -> Result<(), i32> {
        match self.mount_points.remove(mountpoint) {
            Some(_) => Ok(()),
            None => {
                error!("mountpoint {} not found", mountpoint);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub fn list_mountpoints(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        for k in self.mount_points.iter() {
            result.push((k.key().clone(), k.value().0.clone()));
        }
        result
    }

    // remove old index file and sync mount points to index file
    pub fn sync_index_file(&self) {
        std::fs::remove_file(&self.index_file).unwrap_or(());
        let mut file = std::fs::File::create(&self.index_file).unwrap();
        for k in self.mount_points.iter() {
            let line = format!("{}\n{}\n{}\n\n", k.key(), k.value().0, k.value().1);
            file.write_all(line.as_bytes()).unwrap();
        }
    }

    // read index file and mount all volumes
    pub async fn init(&self) -> Result<(), i32> {
        let mut file = match std::fs::File::open(&self.index_file) {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return Ok(());
                }
                error!("open index file {} error: {}", &self.index_file, e);
                return Err(libc::EIO);
            }
        };
        let mut content = String::new();
        match file.read_to_string(&mut content) {
            Ok(_) => (),
            Err(e) => {
                error!("read index file {} error: {}", &self.index_file, e);
                return Ok(());
            }
        }
        let lines: Vec<&str> = content.split('\n').collect();
        for i in 0..lines.len() / 4 {
            let mountpoint = lines[i * 4].to_owned();
            let volume_name = lines[i * 4 + 1].to_owned();
            let read_only = lines[i * 4 + 2].parse::<bool>().unwrap();
            info!("mounting volume {} to {}", volume_name, mountpoint);
            match self.mount(mountpoint, volume_name.clone(), read_only).await {
                Ok(_) => {
                    info!("mount success");
                }
                Err(e) => {
                    if e == libc::ENOENT {
                        error!("volume {} not found", volume_name);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Ok(())
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
                let send_meta_data: MountVolumeSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                match self
                    .mount(
                        send_meta_data.mount_point,
                        send_meta_data.volume_name,
                        send_meta_data.read_only,
                    )
                    .await
                {
                    Ok(_) => {
                        self.sync_index_file();
                        Ok((0, 0, 0, 0, vec![], vec![]))
                    }
                    Err(e) => {
                        error!("mount error: {}", e);
                        Ok((e, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            UMOUNT => {
                let mountpoint = std::str::from_utf8(&path).unwrap();
                info!("unmounting volume {}", mountpoint);
                match self.mount_points.remove(mountpoint) {
                    Some(_) => {
                        self.sync_index_file();
                        Ok((0, 0, 0, 0, vec![], vec![]))
                    }
                    None => {
                        error!("mountpoint not found: {}", mountpoint);
                        Ok((libc::ENOENT, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            LIST_MOUNTPOINTS => {
                let result = self.list_mountpoints();
                Ok((0, 0, 0, 0, vec![], bincode::serialize(&result).unwrap()))
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
    pub client: Arc<
        RpcClient<
            tokio::net::unix::OwnedReadHalf,
            tokio::net::unix::OwnedWriteHalf,
            UnixStreamCreator,
        >,
    >,
    path: String,
}

impl LocalCli {
    pub fn new(path: String) -> Self {
        Self {
            client: Arc::new(RpcClient::new()),
            path,
        }
    }

    pub async fn add_connection(&self, path: &str) -> Result<(), i32> {
        self.client.add_connection(path).await.map_err(|e| {
            error!("add connection failed: {:?}", e);
            CONNECTION_ERROR
        })
    }

    pub async fn mount(
        &self,
        volume_name: &str,
        mount_point: &str,
        read_only: bool,
    ) -> Result<(), i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = bincode::serialize(&MountVolumeSendMetaData {
            volume_name: volume_name.to_string(),
            mount_point: mount_point.to_string(),
            read_only,
        })
        .unwrap();

        let result = self
            .client
            .call_remote(
                &self.path,
                MOUNT,
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
                error!("umount volume failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }

    pub async fn list_mountpoints(&self) -> Result<Vec<(String, String)>, i32> {
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut mountpoints = vec![];

        let result = self
            .client
            .call_remote(
                &self.path,
                LIST_MOUNTPOINTS,
                0,
                "",
                &[],
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut [],
                &mut mountpoints,
                REQUEST_TIMEOUT,
            )
            .await;
        match result {
            Ok(_) => {
                if status != 0 {
                    return Err(status);
                }
                Ok(bincode::deserialize(&mountpoints).unwrap())
            }
            Err(e) => {
                error!("list mountpoints failed: {:?}", e);
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
                error!("probe failed: {:?}", e);
                Err(CONNECTION_ERROR)
            }
        }
    }
}
