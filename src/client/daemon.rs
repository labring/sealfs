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
use log::{error, info, warn};

use crate::{
    common::{
        errors::{status_to_string, CONNECTION_ERROR},
        sender::REQUEST_TIMEOUT,
        serialization::MountVolumeSendMetaData,
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
    pub mount_lock: tokio::sync::Mutex<()>,
}

// TODO: remove this
// replace fuser with other fuse library which supports async.
// better for performance too.
unsafe impl Sync for SealfsFused {}
unsafe impl Send for SealfsFused {}

impl SealfsFused {
    pub fn new(index_file: String, client: Arc<Client>) -> Self {
        Self {
            client,
            mount_points: DashMap::new(),
            index_file,
            mount_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn mount(
        &self,
        mountpoint: String,
        volume_name: String,
        read_only: bool,
    ) -> Result<(), String> {
        let _lock = self.mount_lock.lock().await;
        let mount_mode = if read_only {
            MountOption::RO
        } else {
            MountOption::RW
        };
        let mut options = vec![mount_mode, MountOption::FSName("seal".to_string())];
        options.push(MountOption::AutoUnmount);
        options.push(MountOption::AllowRoot);
        options.push(MountOption::CUSTOM("nonempty".to_string()));
        let result = self.client.init_volume(&volume_name).await;
        match result {
            Ok(inode) => {
                info!("volume {} inited, now mount", volume_name);

                // check if already mounted
                if self.mount_points.contains_key(&mountpoint) {
                    warn!("mountpoint {} already mounted", mountpoint);
                    if self.mount_points.get(&mountpoint).unwrap().0 != volume_name {
                        return Err(format!("mountpoint {} already mounted with different volume", mountpoint));
                    }
                    if self.mount_points.get(&mountpoint).unwrap().1 != read_only {
                        return Err(format!("mountpoint {} already mounted with different mode", mountpoint));
                    }
                    return Ok(());
                }

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
                        Err(format!("mount error: {}", e))
                    }
                }
            }
            Err(e) => {
                return Err(format!("mount error: {}", status_to_string(e)))
            }
        }
    }

    pub async fn unmount(&self, mountpoint: &str) -> Result<(), String> {
        let _lock = self.mount_lock.lock().await;
        match self.mount_points.remove(mountpoint) {
            Some(_) => Ok(()),
            None => {
                Err(format!("mountpoint {} not found", mountpoint))
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

        // write to swap file first
        let mut file = std::fs::File::create(format!("{}.swap", &self.index_file)).unwrap();
        for k in self.mount_points.iter() {
            let line = format!("{}\n{}\n{}\n\n", k.key(), k.value().0, k.value().1);
            file.write_all(line.as_bytes()).unwrap();
        }
        // write a $ to indicate the end of file
        file.write_all(b"$\n").unwrap();
        file.sync_all().unwrap();
        drop(file);

        std::fs::remove_file(&self.index_file).unwrap_or(());
        let mut file = std::fs::File::create(&self.index_file).unwrap();
        for k in self.mount_points.iter() {
            let line = format!("{}\n{}\n{}\n\n", k.key(), k.value().0, k.value().1);
            file.write_all(line.as_bytes()).unwrap();
        }
        // write a $ to indicate the end of file
        file.write_all(b"$\n").unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::remove_file(format!("{}.swap", &self.index_file)).unwrap_or(());
    }

    pub fn read_index_file(&self, index_file_name: &str, allow_nonexist: bool) -> Result<Vec<(String, String, bool)>, String> {
        let mut result = Vec::new();
        let mut file = match std::fs::File::open(index_file_name) {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    if allow_nonexist {
                        return Ok(result);
                    }
                    return Err(format!("index file {} not found", index_file_name));
                }
                return Err(format!("open index file {} error: {}", index_file_name, e));
            }
        };
        let mut content = String::new();
        match file.read_to_string(&mut content) {
            Ok(_) => (),
            Err(e) => {
                error!("read index file {} error: {}", index_file_name, e);
                return Err(format!("read index file {} error: {}", index_file_name, e));
            }
        }
        let lines: Vec<&str> = content.split('\n').collect();
        for i in (0..lines.len()).step_by(4) {
            if i + 3 >= lines.len() {
                println!("{}", lines[i]);
                if lines[i] == "$" {
                    break
                }
                return Err(format!("index file {} format error", index_file_name));
            }
            result.push((
                lines[i].to_string(),
                lines[i + 1].to_string(),
                lines[i + 2].parse::<bool>().unwrap(),
            ));
        }
        Ok(result)
    }

    // read index file and mount all volumes
    pub async fn init(&self) -> Result<(), String> {
        let volumes = {
            match self.read_index_file(format!("{}.swap", &self.index_file).as_str(), false) {
                Ok(result) => result,
                Err(e) => {
                    warn!("read swap index file error: {}", e);
                    match self.read_index_file(&self.index_file, true) {
                        Ok(result) => result,
                        Err(e) => {
                            return Err(format!("read index file error: {}", e));
                        }
                    }
                }
            }
        };

        for (mountpoint, volume_name, read_only) in volumes {
            match self.mount(mountpoint, volume_name.clone(), read_only).await {
                Ok(_) => {},
                Err(e) => {
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
                info!(
                    "mounting volume {} to {}",
                    send_meta_data.volume_name, send_meta_data.mount_point
                );
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
                        Ok((libc::EIO, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            UMOUNT => {
                let mountpoint = std::str::from_utf8(&path).unwrap();
                info!("unmounting volume {}", mountpoint);
                match self.unmount(mountpoint).await {
                    Ok(()) => {
                        self.sync_index_file();
                        Ok((0, 0, 0, 0, vec![], vec![]))
                    }
                    Err(e) => {
                        error!("unmount error: {}", e);
                        Ok((libc::EIO, 0, 0, 0, vec![], vec![]))
                    }
                }
            }
            LIST_MOUNTPOINTS => {
                info!("list_mountpoints");
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
