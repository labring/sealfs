// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod storage_engine;

use std::sync::Arc;

use log::{debug, info};
use nix::sys::stat::Mode;
use storage_engine::StorageEngine;

use crate::common::{
    byte::array2u32,
    distribute_hash_table::build_hash_ring,
    request::{
        OperationType, RequestHeader, REQUEST_DATA_LENGTH_SIZE, REQUEST_FILENAME_LENGTH_SIZE,
        REQUEST_HEADER_SIZE, REQUEST_METADATA_LENGTH_SIZE,
    },
};
use distributed_engine::DistributedEngine;
use storage_engine::default_engine::DefaultEngine;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

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
    address: String,
    database_path: String,
    storage_path: String,
    local_distributed_address: String,
    all_servers_address: Vec<String>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&address).await?;
    let local_storage = Arc::new(DefaultEngine::new(&database_path, &storage_path));
    local_storage.init();
    build_hash_ring(all_servers_address.clone());

    let engine = Arc::new(DistributedEngine::new(
        local_distributed_address.clone(),
        local_storage.clone(),
    ));
    for value in all_servers_address.iter() {
        if &local_distributed_address == value {
            continue;
        }
        let arc_engine = engine.clone();
        let key_address = value.clone();
        tokio::spawn(async move {
            arc_engine.add_connection(key_address).await;
        });
    }
    let mut server = Server::new(address, listener, engine);
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
            EngineError::IO => -libc::EIO,
            EngineError::NoEntry => -libc::ENOENT,
            EngineError::NotDir => -libc::ENOTDIR,
            EngineError::IsDir => -libc::EISDIR,
            EngineError::Exist => -libc::EEXIST,
            EngineError::NotEmpty => -libc::ENOTEMPTY,
            // todo
            // other Error
            _ => 0,
        }
    };
}

pub struct Server<S: StorageEngine + std::marker::Send + std::marker::Sync + 'static> {
    pub address: String,
    listener: TcpListener,
    engine: Arc<DistributedEngine<S>>,
}

impl<S> Server<S>
where
    S: StorageEngine + std::marker::Send + std::marker::Sync + 'static,
{
    pub fn new(address: String, listener: TcpListener, engine: Arc<DistributedEngine<S>>) -> Self
    where
        S: StorageEngine,
    {
        Self {
            address,
            listener,
            engine,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("Listening");
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    let mut handler = Handler {
                        stream,
                        send_lock: Mutex::new(()),
                    };
                    let e = Arc::clone(&self.engine);
                    tokio::spawn(async move {
                        let header_result = handler.parse_header().await;
                        let header = match header_result {
                            Ok(header) => header.unwrap_or_else(|| {
                                todo!("response to client, request header is null");
                            }),
                            Err(e) => {
                                todo!("response to client, error is {}", e);
                            }
                        };
                        match handler.dispatch(header, e).await {
                            Ok(_) => {}
                            Err(e) => {
                                todo!("handle response error, error is {}", e);
                            }
                        };
                    });
                }
                Err(e) => {
                    panic!("Failed to create tcp stream, error is {}", e)
                }
            }
        }
    }
}

pub struct Handler {
    stream: TcpStream,
    send_lock: Mutex<()>,
}

impl Handler {
    pub async fn parse_header(&mut self) -> Result<Option<RequestHeader>, ServerError> {
        let mut header_buf = [0u8; REQUEST_HEADER_SIZE];
        let header = match self.stream.read(&mut header_buf).await {
            Ok(n) if n == REQUEST_HEADER_SIZE => {
                let header = RequestHeader {
                    id: array2u32(&header_buf[0..4]),
                    r#type: array2u32(&header_buf[4..8]),
                    flags: array2u32(&header_buf[8..12]),
                    total_length: array2u32(&header_buf[12..16]),
                    file_path_length: array2u32(&header_buf[16..20]),
                    meta_data_length: array2u32(&header_buf[20..24]),
                    data_length: array2u32(&header_buf[24..28]),
                };
                Some(header)
            }
            Ok(_n) => None,
            Err(_e) => return Err(ServerError::ParseHeaderError),
        };
        Ok(header)
    }

    pub async fn read_body(&mut self, length: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0; length];
        self.stream.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub async fn parse_data(
        &mut self,
        raw_data: &[u8],
        start: usize,
    ) -> anyhow::Result<(usize, Vec<u8>)> {
        let data_length = array2u32(&raw_data[start..REQUEST_DATA_LENGTH_SIZE]) as usize;
        Ok((
            data_length,
            raw_data[start + REQUEST_DATA_LENGTH_SIZE..start + data_length].to_vec(),
        ))
    }

    pub async fn parse_metadata(
        &mut self,
        raw_data: &[u8],
        start: usize,
    ) -> anyhow::Result<(usize, Vec<u8>)> {
        let metadata_length = array2u32(&raw_data[start..REQUEST_METADATA_LENGTH_SIZE]) as usize;
        Ok((
            metadata_length,
            raw_data[start + REQUEST_METADATA_LENGTH_SIZE..start + metadata_length].to_vec(),
        ))
    }

    pub async fn parse_path(&mut self, raw_data: &[u8]) -> Result<Option<String>, ServerError> {
        let filename_length = array2u32(&raw_data[0..REQUEST_FILENAME_LENGTH_SIZE]) as usize;
        let file_name = String::from_utf8(
            raw_data[REQUEST_FILENAME_LENGTH_SIZE..REQUEST_FILENAME_LENGTH_SIZE + filename_length]
                .to_vec(),
        )
        .unwrap();
        Ok(Some(file_name))
    }

    pub async fn dispatch<S: StorageEngine>(
        &mut self,
        header: RequestHeader,
        engine: Arc<DistributedEngine<S>>,
    ) -> anyhow::Result<()> {
        let buf = self
            .read_body(header.total_length as usize - REQUEST_HEADER_SIZE)
            .await?;
        // a temporary implementation
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let operation_type = OperationType::try_from(header.r#type).unwrap();
        match operation_type {
            OperationType::Unkown => todo!(),
            OperationType::Lookup => todo!(),
            OperationType::CreateFile => {
                debug!("Create File");
                let file_path = self.parse_path(&buf).await?.unwrap();
                let (_fd, status) = match engine.create_file(file_path, mode).await {
                    Ok(fd) => (fd, 0),
                    Err(e) => (-1, EngineErr2Status!(e) as u32),
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::CreateDir => {
                debug!("Create Dir");
                let dir_path = self.parse_path(&buf).await?.unwrap();
                let status = match engine.create_dir(dir_path, mode).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::GetFileAttr => {
                debug!("Get File Attr");
                let file_path = self.parse_path(&buf).await?.unwrap();
                let (data, status) = match engine.get_file_attr(file_path).await {
                    Ok(value) => (value, 0),
                    Err(e) => (None, EngineErr2Status!(e)),
                };
                let data_bytes = if let Some(value) = data {
                    bincode::serialize(&value[..]).unwrap()
                } else {
                    Vec::new()
                };
                self.response(
                    header.id,
                    status as u32,
                    0,
                    16,
                    Some(data_bytes.len() as u32),
                    None,
                    Some(data_bytes.as_slice()),
                    None,
                )
                .await?;
            }
            OperationType::OpenFile => {
                debug!("Open File");
                todo!()
            }
            OperationType::ReadDir => {
                debug!("Read Dir");
                let dir_path = self.parse_path(&buf).await?.unwrap();
                // need to be parse size
                let (data, status) = match engine.read_dir(dir_path).await {
                    Ok(value) => (value, 0u32),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as u32),
                };
                let data_bytes = bincode::serialize(&data[..])?;
                self.response(
                    header.id,
                    status,
                    0,
                    16,
                    None,
                    Some(data_bytes.len() as u32),
                    None,
                    Some(data_bytes.as_slice()),
                )
                .await?;
            }
            OperationType::ReadFile => {
                debug!("Read File");
                let file_path = self.parse_path(&buf).await?.unwrap();
                // need to parse size and offset from request
                let (data, status) = match engine.read_file(file_path, 100, 0).await {
                    Ok(value) => (value, 0),
                    Err(e) => (Vec::new(), EngineErr2Status!(e)),
                };
                self.response(
                    header.id,
                    status as u32,
                    0,
                    16,
                    None,
                    Some(data.len() as u32),
                    None,
                    Some(data.as_slice()),
                )
                .await?;
            }
            OperationType::WriteFile => {
                debug!("Write File");
                let file_path = self.parse_path(&buf).await?.unwrap();
                let mut start = file_path.len();
                let (metadata_length, _metadata) = self.parse_metadata(&buf, start).await?;
                start = start + metadata_length + REQUEST_METADATA_LENGTH_SIZE;
                let (_data_length, data) = self.parse_data(&buf, start).await?;
                // need to be parse offset
                let (_size, status) = match engine.write_file(file_path, data.as_slice(), 0).await {
                    Ok(size) => (size, 0),
                    Err(e) => (0, EngineErr2Status!(e) as u32),
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::DeleteFile => {
                debug!("Delete File");
                let file_path = self.parse_path(&buf).await?.unwrap();
                let status = match engine.delete_file(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::DeleteDir => {
                debug!("Delete Dir");
                let file_path = self.parse_path(&buf).await?.unwrap();
                let status = match engine.delete_dir(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::DirectoryAddEntry => {}
            OperationType::DirectoryDeleteEntry => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn response(
        &mut self,
        id: u32,
        status: u32,
        flags: u32,
        total_length: u32,
        meta_data_length: Option<u32>,
        data_length: Option<u32>,
        meta_data: Option<&[u8]>,
        data: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let buf = [id, status, flags, total_length];
        let buf = unsafe { std::mem::transmute_copy::<[u32; 4], [u8; 16]>(&buf) };

        let _ = self.send_lock.lock().await;
        // send response header
        self.stream.write_all(&buf).await?;
        // send metadata length
        if let Some(meta_data_length) = meta_data_length {
            self.stream.write_u32(meta_data_length).await?;
        }
        if let Some(meta_data) = meta_data {
            self.stream.write_all(meta_data).await?;
        }
        // send data length
        if let Some(data_length) = data_length {
            self.stream.write_u32(data_length).await?;
        }
        if let Some(data) = data {
            self.stream.write_all(data).await?;
        }
        Ok(())
    }
}
