// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod storage_engine;
mod transfer_manager;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_trait::async_trait;
use log::{debug, error, info};
use storage_engine::StorageEngine;
use tokio::time::sleep;

use crate::{
    common::{
        errors::status_to_string,
        hash_ring::HashRing,
        serialization::{
            bytes_as_file_attr, ClusterStatus, CreateDirSendMetaData, CreateFileSendMetaData,
            CreateVolumeSendMetaData, DeleteDirSendMetaData, DeleteFileSendMetaData,
            DirectoryEntrySendMetaData, OpenFileSendMetaData, OperationType, ReadDirSendMetaData,
            ServerStatus, TruncateFileSendMetaData,
        },
        serialization::{ReadFileSendMetaData, WriteFileSendMetaData},
    },
    rpc::server::{Handler, RpcServer},
    server::storage_engine::meta_engine::MetaEngine,
};
use distributed_engine::DistributedEngine;
use storage_engine::file_engine::FileEngine;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServerError {
    #[error("ParseHeaderError")]
    ParseHeaderError,
}

pub async fn sync_cluster_status(engine: Arc<DistributedEngine<FileEngine>>) {
    loop {
        {
            let result = engine.get_cluster_status().await;
            match result {
                Ok(status) => {
                    let status: i32 = status.into();
                    if engine.cluster_status.load(Ordering::Relaxed) != status {
                        engine.cluster_status.store(status, Ordering::Relaxed);
                        debug!("sync server status, status = {}", status);
                    }
                }
                Err(e) => {
                    error!("sync server status failed, error = {}", e);
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub async fn watch_status(engine: Arc<DistributedEngine<FileEngine>>) {
    loop {
        if engine.closed.load(Ordering::Relaxed) {
            error!("watch status: server closed");
            break;
        }
        match engine
            .cluster_status
            .load(Ordering::Relaxed)
            .try_into()
            .unwrap()
        {
            ClusterStatus::SyncNewHashRing => {
                info!("watch status: start to sync new hash ring");
                let all_servers_address = match engine.get_new_hash_ring_info().await {
                    Ok(value) => value,
                    Err(e) => {
                        panic!("Get Hash Ring Info Failed. Error = {}", e);
                    }
                };
                info!("watch status: get new hash ring info");
                for value in all_servers_address.iter() {
                    if engine.address == value.0
                        || engine.hash_ring.read().as_ref().unwrap().contains(&value.0)
                    {
                        continue;
                    }
                    if let Err(e) = engine.add_connection(value.0.clone()).await {
                        // TODO: rollback the transfer process
                        panic!("watch status: add connection failed, error = {}", e);
                    }
                }
                engine
                    .new_hash_ring
                    .write()
                    .replace(HashRing::new(all_servers_address));
                info!("watch status: sync new hash ring finished");
                match engine.update_server_status(ServerStatus::PreTransfer).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    engine.cluster_status.load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::SyncNewHashRing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        engine.cluster_status.load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::PreTransfer
                );

                let file_map = engine.make_up_file_map();

                info!("watch status: start to transfer files");
                match engine
                    .update_server_status(ServerStatus::Transferring)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }
                while <i32 as TryInto<ClusterStatus>>::try_into(
                    engine.cluster_status.load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::PreTransfer
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        engine.cluster_status.load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Transferring
                );

                if let Err(e) = engine.transfer_files(file_map).await {
                    panic!("transfer files failed, error = {}", e);
                }

                info!("watch status: transfer files finished");
                match engine.update_server_status(ServerStatus::PreFinish).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    engine.cluster_status.load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::Transferring
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        engine.cluster_status.load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::PreFinish
                );

                let _old_hash_ring = engine
                    .hash_ring
                    .write()
                    .replace(engine.new_hash_ring.read().clone().unwrap()); // TODO: _old_hash_ring should be used to rollback the transfer process

                info!("watch status: start to finishing");
                match engine.update_server_status(ServerStatus::Finishing).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    engine.cluster_status.load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::PreFinish
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        engine.cluster_status.load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Finishing
                );

                let _ = engine.new_hash_ring.write().take();
                // here we should close connections to old servers, but now we just wait for remote servers to close connections and do nothing

                info!("watch status: start to finishing");
                match engine.update_server_status(ServerStatus::Finished).await {
                    Ok(_) => {}
                    Err(e) => {
                        panic!("update server status failed, error = {}", e);
                    }
                }

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    engine.cluster_status.load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::Finishing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        engine.cluster_status.load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Idle
                );

                info!("watch status: transferring data finished");
            }
            ClusterStatus::Idle => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::Initializing => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::NodesStarting => {
                sleep(Duration::from_secs(1)).await;
            }
            e => {
                panic!("cluster status error: {:?}", e as u32);
            }
        }
    }
}

pub async fn run(
    database_path: String,
    storage_path: String,
    server_address: String,
    manager_address: String,
    #[cfg(feature = "disk-db")] cache_capacity: usize,
    #[cfg(feature = "disk-db")] write_buffer_size: usize,
) -> anyhow::Result<()> {
    debug!("run server");
    let meta_engine = Arc::new(MetaEngine::new(
        &database_path,
        #[cfg(feature = "disk-db")]
        cache_capacity,
        #[cfg(feature = "disk-db")]
        write_buffer_size,
    ));
    let storage_engine = Arc::new(FileEngine::new(&storage_path, Arc::clone(&meta_engine)));
    storage_engine.init();
    info!("Init: Storage Engine Init Finished");

    let engine = Arc::new(DistributedEngine::new(
        server_address.clone(),
        storage_engine,
        meta_engine,
    ));

    info!("Init: Connect To Manager: {}", manager_address);
    if let Err(e) = engine.client.add_connection(&manager_address).await {
        panic!("Connect To Manager Failed, Error = {}", e);
    }
    *engine.manager_address.lock().await = manager_address;

    tokio::spawn(sync_cluster_status(Arc::clone(&engine)));

    while <i32 as TryInto<ClusterStatus>>::try_into(engine.cluster_status.load(Ordering::Relaxed))
        .unwrap()
        == ClusterStatus::Unkown
    {
        sleep(Duration::from_secs(1)).await;
    }

    let handler = Arc::new(FileRequestHandler::new(engine.clone()));
    let server = RpcServer::new(handler, &server_address);

    let engine_clone = Arc::clone(&engine);

    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("Server Run Failed, Error = {}", e);
            engine_clone.closed.store(true, Ordering::Relaxed);
        }
    });

    info!("Init: Add connections and update Server Status");

    let all_servers_address = match engine.get_hash_ring_info().await {
        Ok(value) => value,
        Err(_) => {
            panic!("Get Hash Ring Info Failed.");
        }
    };
    info!("Init: Hash Ring Info: {:?}", all_servers_address);
    for value in all_servers_address.iter() {
        if server_address == value.0 {
            continue;
        }
        if let Err(e) = engine.add_connection(value.0.clone()).await {
            panic!("Init: Add Connection Failed. Error = {}", e);
        }
    }
    info!("Init: Add Connections Success.");
    engine
        .hash_ring
        .write()
        .replace(HashRing::new(all_servers_address));
    info!("Init: Update Hash Ring Success.");

    match <i32 as TryInto<ClusterStatus>>::try_into(engine.cluster_status.load(Ordering::Relaxed))
        .unwrap()
    {
        ClusterStatus::Initializing => {
            match engine.update_server_status(ServerStatus::Finished).await {
                Ok(_) => {
                    info!("Update Server Status to Finish Success.");
                }
                Err(e) => {
                    panic!("Update Server Status to Finish Failed. Error = {}", e);
                }
            }
        }
        ClusterStatus::Idle => {}
        e => {
            panic!("Cluster Status Unexpected. Status = {:?}", e as u32);
        }
    }
    info!("Init: Start Transferring Data.");
    watch_status(engine.clone()).await;

    Ok(())
}

pub struct FileRequestHandler<S: StorageEngine + std::marker::Send + std::marker::Sync + 'static> {
    engine: Arc<DistributedEngine<S>>,
}

impl<S: StorageEngine> FileRequestHandler<S>
where
    S: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
{
    pub fn new(engine: Arc<DistributedEngine<S>>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl<S: StorageEngine> Handler for FileRequestHandler<S>
where
    S: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
{
    // dispatch is the main function to handle the request from client
    // the return value is a tuple of (i32, u32, Vec<u8>, Vec<u8>)
    // the first i32 is the status of the function
    // the second u32 is the reserved field flags
    // the third Vec<u8> is the metadata of the function
    // the fourth Vec<u8> is the data of the function
    async fn dispatch(
        &self,
        id: u32,
        operation_type: u32,
        flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, usize, usize, Vec<u8>, Vec<u8>)> {
        let r#type = match OperationType::try_from(operation_type) {
            Ok(value) => value,
            Err(e) => {
                error!("Operation Type Error: {:?}", e);
                return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
            }
        };

        let file_path = unsafe { std::str::from_utf8_unchecked(&path) };

        // this lock is deprecated, and always return false
        let _lock =
            match self.engine.get_forward_address(file_path) {
                (Some(address), _) => {
                    match self
                        .engine
                        .forward_request(address, operation_type, flags, file_path, data, metadata)
                        .await
                    {
                        Ok(value) => {
                            return Ok(value);
                        }
                        Err(e) => {
                            debug!(
                            "Forward Request Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            return Ok((e, 0, 0, 0, Vec::new(), Vec::new()));
                        }
                    }
                }
                (None, lock) => lock,
            };

        match r#type {
            OperationType::Unkown => {
                error!("Unkown Operation Type: path: {}", file_path);
                Ok((-1, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::Lookup => {
                error!("{} Lookup not implemented", self.engine.address);
                Ok((-1, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateFile => {
                debug!("{} Create File: path: {}", self.engine.address, file_path);
                let meta_data_unwraped: CreateFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_file(
                        metadata,
                        file_path,
                        &meta_data_unwraped.name,
                        meta_data_unwraped.flags,
                        meta_data_unwraped.umask,
                        meta_data_unwraped.mode,
                    )
                    .await
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::CreateDir => {
                debug!("{} Create Dir: path: {}", self.engine.address, file_path);
                let meta_data_unwraped: CreateDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_dir(
                        metadata,
                        file_path,
                        &meta_data_unwraped.name,
                        meta_data_unwraped.mode,
                    )
                    .await
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            e, file_path, operation_type, flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::GetFileAttr => {
                debug!("{} Get File Attr: path: {}", self.engine.address, file_path);
                let (return_meta_data, status) =
                    match self.engine.get_file_attr(file_path) {
                        Ok(value) => (value, 0),
                        Err(e) => {
                            debug!(
                            "Get File Attr Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            (Vec::new(), e)
                        }
                    };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::OpenFile => {
                debug!("{} Open File {}", self.engine.address, file_path);
                let meta_data_unwraped: OpenFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self.engine.open_file(
                    file_path,
                    meta_data_unwraped.flags,
                    meta_data_unwraped.mode,
                ) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Open File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::ReadDir => {
                debug!("{} Read Dir: {}", self.engine.address, file_path);
                let md: ReadDirSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (data, status) = match self.engine.read_dir(file_path, md.size, md.offset) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Read Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((status, 0, 0, data.len(), Vec::new(), data))
            }
            OperationType::ReadFile => {
                debug!("{} Read File: {}", self.engine.address, file_path);
                let md: ReadFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (data, status) = match self.engine.read_file(file_path, md.size, md.offset) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Read File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((status, 0, 0, data.len(), Vec::new(), data))
            }
            OperationType::WriteFile => {
                debug!("{} Write File: {}", self.engine.address, file_path);
                let md: WriteFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (status, size) =
                    match self
                        .engine
                        .write_file(file_path, data.as_slice(), md.offset)
                    {
                        Ok(size) => (0, size as u32),
                        Err(e) => {
                            debug!(
                                "Write File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                                status_to_string(e),
                                file_path,
                                operation_type,
                                flags
                            );
                            (e, 0)
                        }
                    };
                Ok((
                    status,
                    0,
                    size.to_le_bytes().len(),
                    0,
                    size.to_le_bytes().to_vec(),
                    Vec::new(),
                ))
            }
            OperationType::DeleteFile => {
                debug!("{} Delete File: {}", self.engine.address, file_path);
                let meta_data_unwraped: DeleteFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self
                    .engine
                    .delete_file(metadata, file_path, &meta_data_unwraped.name)
                    .await
                {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DeleteDir => {
                debug!("{} Delete Dir: {}", self.engine.address, file_path);
                let meta_data_unwraped: DeleteDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let status = match self
                    .engine
                    .delete_dir(metadata, file_path, &meta_data_unwraped.name)
                    .await
                {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DirectoryAddEntry => {
                debug!("{} Directory Add Entry: {}", self.engine.address, file_path);
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_add_entry(file_path, md.file_name, md.file_type),
                    0,
                    0,
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::DirectoryDeleteEntry => {
                debug!(
                    "{} Directory Delete Entry: {}",
                    self.engine.address, file_path
                );
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_delete_entry(file_path, md.file_name, md.file_type),
                    0,
                    0,
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::TruncateFile => {
                debug!("{} Truncate File: {}", self.engine.address, file_path);
                let md: TruncateFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let status =
                    match self.engine.truncate_file(file_path, md.length) {
                        Ok(()) => 0,
                        Err(e) => {
                            debug!(
                            "Truncate File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CheckFile => {
                info!("{} Checkout File: {}", self.engine.address, file_path);
                let file_attr = bytes_as_file_attr(&metadata);
                let status =
                    match self.engine.check_file(file_path, file_attr) {
                        Ok(()) => 0,
                        Err(e) => {
                            info!(
                            "Checkout File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CheckDir => {
                info!("{} Checkout Dir: {}", self.engine.address, file_path);
                let file_attr = bytes_as_file_attr(&metadata);
                let status =
                    match self.engine.check_dir(file_path, file_attr) {
                        Ok(()) => 0,
                        Err(e) => {
                            info!(
                            "Checkout Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e), file_path, operation_type, flags
                        );
                            e
                        }
                    };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateDirNoParent => {
                debug!(
                    "{} Create Dir no Parent: path: {}",
                    self.engine.address, file_path
                );
                let meta_data_unwraped: CreateDirSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self
                    .engine
                    .create_dir_no_parent(file_path, meta_data_unwraped.mode)
                {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::CreateFileNoParent => {
                debug!(
                    "{} Create File no Parent: path: {}",
                    self.engine.address, file_path
                );
                let meta_data_unwraped: CreateFileSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                let (return_meta_data, status) = match self.engine.create_file_no_parent(
                    file_path,
                    meta_data_unwraped.flags,
                    meta_data_unwraped.umask,
                    meta_data_unwraped.mode,
                ) {
                    Ok(value) => (value, 0),
                    Err(e) => {
                        debug!(
                            "Create File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        (Vec::new(), e)
                    }
                };
                Ok((
                    status,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ))
            }
            OperationType::DeleteDirNoParent => {
                debug!(
                    "{} Delete Dir no Parent: {}",
                    self.engine.address, file_path
                );
                let status = match self.engine.delete_dir_no_parent(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete Dir Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::DeleteFileNoParent => {
                debug!(
                    "{} Delete File no Parent: {}",
                    self.engine.address, file_path
                );
                let status = match self.engine.delete_file_no_parent(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        debug!(
                            "Delete File Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            file_path,
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                Ok((status, 0, 0, 0, Vec::new(), Vec::new()))
            }
            OperationType::CreateVolume => {
                info!("{} Create Volume", self.engine.address);
                let meta_data_unwraped: CreateVolumeSendMetaData =
                    bincode::deserialize(&metadata).unwrap();
                info!("Create Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self
                    .engine
                    .create_volume(file_path, meta_data_unwraped.size)
                {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Create Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::InitVolume => {
                info!(
                    "{} Init Volume: {}, id: {}",
                    self.engine.address, file_path, id
                );
                if !file_path.is_empty()
                    && self.engine.get_address(file_path) == self.engine.address
                    && self.engine.meta_engine.init_volume(file_path).is_err()
                {
                    error!(
                        "Volume not Exists: id: {}, file_path: {}, address {}, self_address {}",
                        id,
                        file_path,
                        self.engine.get_address(file_path),
                        self.engine.address
                    );
                    return Ok((libc::ENOENT, 0, 0, 0, vec![], vec![]));
                }
                //self.engine.volume_indexes.insert(id, file_path);
                return Ok((0, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::ListVolumes => {
                info!("{} List Volume", self.engine.address);
                let return_meta_data = self.engine.meta_engine.list_volumes().unwrap();
                return Ok((
                    0,
                    0,
                    return_meta_data.len(),
                    0,
                    return_meta_data,
                    Vec::new(),
                ));
            }
            OperationType::DeleteVolume => {
                info!("{} Delete Volume", self.engine.address);
                info!("Delete Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self.engine.delete_volume(file_path).await {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Delete Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
            OperationType::CleanVolume => {
                info!("{} Clean Volume", self.engine.address);
                info!("Clean Volume: {:?}, id: {}", file_path, id);
                if file_path.is_empty()
                    || file_path.len() > 255
                    || file_path.contains('\0')
                    || file_path.contains('/')
                {
                    return Ok((libc::EINVAL, 0, 0, 0, vec![], vec![]));
                }
                let status = match self.engine.clean_volume(file_path) {
                    Ok(()) => 0,
                    Err(e) => {
                        info!(
                            "Clean Volume Failed: {:?}, path: {}, operation_type: {}, flags: {}",
                            status_to_string(e),
                            std::str::from_utf8(path.as_slice()).unwrap(),
                            operation_type,
                            flags
                        );
                        e
                    }
                };
                return Ok((status, 0, 0, 0, Vec::new(), Vec::new()));
            }
        }
    }
}
