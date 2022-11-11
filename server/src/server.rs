use std::sync::Arc;

use crate::EngineError;
use common::{
    byte::array2u32,
    request::{OperationType, RequestHeader, REQUEST_FILENAME_LENGTH, REQUEST_HEADER_SIZE},
};
use log::{debug, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{distributed_engine::DistributedEngine, storage_engine::StorageEngine, ServerError};

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
                    r#type: array2u32(&header_buf[4..8]).try_into().unwrap(),
                    flags: array2u32(&header_buf[8..12]),
                    total_length: array2u32(&header_buf[12..16]),
                };
                Some(header)
            }
            Ok(_n) => None,
            Err(_e) => return Err(ServerError::ParseHeaderError),
        };
        Ok(header)
    }

    pub async fn read_body(&mut self, length: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(length);
        self.stream.read_exact(&mut buf).await?;
        Ok(buf)
    }

    pub async fn parse_path(&mut self, raw_data: Vec<u8>) -> Result<Option<String>, ServerError> {
        let filename_length = array2u32(&raw_data[0..REQUEST_FILENAME_LENGTH]) as usize;
        let file_name = String::from_utf8(
            raw_data[REQUEST_FILENAME_LENGTH..REQUEST_FILENAME_LENGTH + filename_length].to_vec(),
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
        match header.r#type {
            OperationType::CreateFile => {
                debug!("Create File");
                let file_path = self.parse_path(buf).await?.unwrap();
                let status = match engine.create_file(file_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::CreateDir => {
                debug!("Create Dir");
                let dir_path = self.parse_path(buf).await?.unwrap();
                let status = match engine.create_dir(dir_path).await {
                    Ok(()) => 0,
                    Err(e) => EngineErr2Status!(e) as u32,
                };
                self.response(header.id, status, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::GetFileAttr => {
                debug!("Get File Attr");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::OpenFile => {
                debug!("Open File");
                todo!()
            }
            OperationType::ReadDir => {
                debug!("Read Dir");
                let dir_path = self.parse_path(buf).await?.unwrap();
                let (data, status) = match engine.read_dir(dir_path).await {
                    Ok(value) => (value, 0u32),
                    Err(e) => (Vec::new(), EngineErr2Status!(e) as u32),
                };
                let l: usize = data.iter().map(|s| s.as_bytes().len()).sum();
                let data_byte = bincode::serialize(&data[..])?;
                self.response(
                    header.id,
                    status,
                    0,
                    16,
                    None,
                    Some(l as u32),
                    None,
                    Some(data_byte.as_slice()),
                )
                .await?;
            }
            OperationType::ReadFile => {
                debug!("Read File");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::WriteFile => {
                debug!("Write File");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
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
