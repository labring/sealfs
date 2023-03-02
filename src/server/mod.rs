// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod storage_engine;

use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use nix::sys::stat::Mode;
use storage_engine::StorageEngine;

use crate::{
    common::{
        distribute_hash_table::build_hash_ring,
        serialization::{DirectoryEntrySendMetaData, OperationType},
        serialization::{ReadFileSendMetaData, WriteFileSendMetaData},
    },
    rpc::server::{Handler, Server},
};
use distributed_engine::DistributedEngine;
use storage_engine::default_engine::DefaultEngine;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("ENOENT")]
    NoEntry,

    #[error("ENOTDIR")]
    NotDir,

    #[error("EISDIR")]
    IsDir,

    #[error("EEXIST")]
    Exist,

    #[error("EIO")]
    IO,

    #[error("EPATH")]
    Path,

    #[error("ENOTEMPTY")]
    NotEmpty,

    #[error("EBLOCKINFO")]
    BlockInfo,

    #[error(transparent)]
    StdIo(#[from] std::io::Error),

    #[error(transparent)]
    Nix(#[from] nix::Error),

    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
}
impl From<u32> for EngineError {
    fn from(status: u32) -> Self {
        match status {
            1 => EngineError::NoEntry,
            _ => EngineError::IO,
        }
    }
}

impl From<EngineError> for u32 {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::NoEntry => 1,
            EngineError::IO => 2,
            _ => 3,
        }
    }
}

pub async fn run(
    database_path: String,
    storage_path: String,
    server_address: String,
    all_servers_address: Vec<String>,
) -> anyhow::Result<()> {
    debug!("run server");
    let local_storage = Arc::new(DefaultEngine::new(&database_path, &storage_path));
    local_storage.init();
    build_hash_ring(all_servers_address.clone());

    let engine = Arc::new(DistributedEngine::new(
        server_address.clone(),
        local_storage.clone(),
    ));
    for value in all_servers_address.iter() {
        if &server_address == value {
            continue;
        }
        let arc_engine = engine.clone();
        let key_address = value.clone();
        tokio::spawn(async move {
            arc_engine.add_connection(key_address).await;
        });
    }
    let handler = Arc::new(FileRequestHandler::new(engine));
    let server = Server::new(handler, &server_address);
    server.run().await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServerError {
    #[error("ParseHeaderError")]
    ParseHeaderError,
}

macro_rules! EngineErr2Status {
    ($e:expr) => {
        match $e {
            EngineError::IO => libc::EIO,
            EngineError::NoEntry => libc::ENOENT,
            EngineError::NotDir => libc::ENOTDIR,
            EngineError::IsDir => libc::EISDIR,
            EngineError::Exist => libc::EEXIST,
            EngineError::NotEmpty => libc::ENOTEMPTY,
            // todo
            // other Error
            _ => libc::EIO,
        }
    };
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
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        metadata: Vec<u8>,
    ) -> anyhow::Result<(i32, u32, Vec<u8>, Vec<u8>)> {
        let r#type = OperationType::try_from(operation_type).unwrap();
        let file_path = String::from_utf8(path).unwrap();
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        match r#type {
            OperationType::Unkown => {
                debug!("Unkown");
                Ok((-1, 0, Vec::new(), Vec::new()))
            }
            OperationType::Lookup => {
                debug!("Lookup");
                todo!()
            }
            OperationType::CreateFile => {
                debug!("Create File");
                let (meta_data, status) = match self.engine.create_file(file_path, mode).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as i32),
                };
                Ok((status, 0, meta_data, Vec::new()))
            }
            OperationType::CreateDir => {
                debug!("Create Dir");
                let (meta_data, status) = match self.engine.create_dir(file_path, mode).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as i32),
                };
                Ok((status, 0, meta_data, Vec::new()))
            }
            OperationType::GetFileAttr => {
                debug!("Get File Attr");
                let (meta_data, status) = match self.engine.get_file_attr(file_path).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as i32),
                };
                Ok((status, 0, meta_data, Vec::new()))
            }
            OperationType::OpenFile => {
                debug!("Open File");
                let status = match self.engine.open_file(file_path, mode).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as i32,
                };
                Ok((status, 0, Vec::new(), Vec::new()))
            }
            OperationType::ReadDir => {
                debug!("Read Dir");
                let (data, status) = match self.engine.read_dir(file_path).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as i32),
                };
                Ok((status, 0, Vec::new(), data))
            }
            OperationType::ReadFile => {
                debug!("Read File");
                let md: ReadFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (data, status) =
                    match self.engine.read_file(file_path, md.size, md.offset).await {
                        Ok(value) => (value, 0),
                        Err(e) => (Vec::new(), EngineErr2Status!(e) as i32),
                    };
                Ok((status, 0, Vec::new(), data))
            }
            OperationType::WriteFile => {
                debug!("Write File, data len: {}", data.len());
                let md: WriteFileSendMetaData = bincode::deserialize(&metadata).unwrap();
                let (status, size) = match self
                    .engine
                    .write_file(file_path, data.as_slice(), md.offset)
                    .await
                {
                    Ok(size) => (0, size as u32),
                    Err(e) => (EngineErr2Status!(e) as i32, 0),
                };
                Ok((status, 0, size.to_le_bytes().to_vec(), Vec::new()))
            }
            OperationType::DeleteFile => {
                debug!("Delete File");
                let status = match self.engine.delete_file(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as i32,
                };
                Ok((status, 0, Vec::new(), Vec::new()))
            }
            OperationType::DeleteDir => {
                debug!("Delete Dir");
                let status = match self.engine.delete_dir(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as i32,
                };
                Ok((status, 0, Vec::new(), Vec::new()))
            }
            OperationType::DirectoryAddEntry => {
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_add_entry(file_path, md.file_type)
                        .await,
                    0,
                    vec![],
                    vec![],
                ))
            }
            OperationType::DirectoryDeleteEntry => {
                let md: DirectoryEntrySendMetaData = bincode::deserialize(&metadata).unwrap();
                Ok((
                    self.engine
                        .directory_delete_entry(file_path, md.file_type)
                        .await,
                    0,
                    vec![],
                    vec![],
                ))
            }
            _ => todo!(),
        }
    }
}
