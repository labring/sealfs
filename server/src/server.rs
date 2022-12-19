use std::sync::Arc;

use crate::EngineError;
use async_trait::async_trait;
use common::request::OperationType;
use log::debug;
use nix::sys::stat::Mode;
use rpc::server::Handler;

use crate::{distributed_engine::DistributedEngine, storage_engine::StorageEngine};

macro_rules! EngineErr2Status {
    ($e:expr) => {
        match $e {
            EngineError::IO => libc::EIO,
            EngineError::NoEntry => libc::ENOENT,
            EngineError::NotDir => libc::ENOTDIR,
            EngineError::IsDir => libc::EISDIR,
            EngineError::Exist => libc::EEXIST,
            EngineError::NotEmpty => libc::ENOTEMPTY,
            // temp
            EngineError::Path => -1,
            EngineError::StdIo(_) => -1,
            EngineError::Nix(_) => -1,
            EngineError::Rocksdb(_) => -1,
            // todo
            // other Error
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
    async fn dispatch(
        &self,
        operation_type: u32,
        _flags: u32,
        path: Vec<u8>,
        data: Vec<u8>,
        _metadata: Vec<u8>,
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
            },
            OperationType::Lookup => {
                debug!("Lookup");
                todo!()
            },
            OperationType::CreateFile => {
                debug!("Create File");
                let status = match self.engine.create_file(file_path, mode).await {
                    Ok(_) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                Ok((0, status, Vec::new(), Vec::new()))
            }
            OperationType::CreateDir => {
                debug!("Create Dir");
                let status = match self.engine.create_dir(file_path, mode).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                Ok((0, status, Vec::new(), Vec::new()))
            }
            OperationType::GetFileAttr => {
                debug!("Get File Attr");
                let (data, status) = match self.engine.get_file_attr(file_path).await {
                    Ok(value) => (value, 0),
                    Err(e) => (None, EngineErr2Status!(e)),
                };
                let data_bytes = if let Some(value) = data {
                    bincode::serialize(&value[..]).unwrap()
                } else {
                    Vec::new()
                };
                Ok((0, status as u32, Vec::new(), data_bytes))
            }
            OperationType::OpenFile => {
                debug!("Open File");
                todo!()
            }
            OperationType::ReadDir => {
                debug!("Read Dir");
                let (data, status) = match self.engine.read_dir(file_path).await {
                    Ok(value) => (value, 0u32),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as u32),
                };
                let data_bytes = bincode::serialize(&data[..])?;
                Ok((0, status, Vec::new(), data_bytes))
            }
            OperationType::ReadFile => {
                debug!("Read File");
                let (data, status) = match self.engine.read_file(file_path, 0, 0).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e)),
                };
                Ok((0, status as u32, Vec::new(), data))
            }
            OperationType::WriteFile => {
                debug!("Write File");
                let status = match self.engine.write_file(file_path, data.as_slice(), 0).await {
                    Ok(_) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                Ok((0, status, Vec::new(), Vec::new()))
            }
            OperationType::DeleteFile => {
                debug!("Delete File");
                let status = match self.engine.delete_file(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                Ok((0, status, Vec::new(), Vec::new()))
            }
            OperationType::DeleteDir => {
                debug!("Delete Dir");
                let status = match self.engine.delete_dir(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                Ok((0, status, Vec::new(), Vec::new()))
            }
        }
    }
}
