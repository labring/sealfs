use common::{
    byte::array2u32,
    request::{OperationType, RequestHeader, REQUEST_HEADER_SIZE},
};
use log::{debug, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::ServerError;

pub struct Server {
    pub address: String,
    listener: TcpListener,
}

impl Server {
    pub fn new(address: String, listener: TcpListener) -> Self {
        Self { address, listener }
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
                        match handler.dispatch(header).await {
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

    pub async fn dispatch(&mut self, header: RequestHeader) -> anyhow::Result<()> {
        match header.r#type {
            OperationType::Lookup => {
                debug!("Lookup");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::CreateFile => {
                debug!("Create File");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::CreateDir => {
                debug!("Create Dir");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::GetFileAttr => {
                debug!("Get File Attr");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::OpenFile => {
                debug!("Open File");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::ReadDir => {
                debug!("Read Dir");
                self.response(header.id, 0, 0, 16, None, None, None, None)
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
            OperationType::DeleteFile => {
                debug!("Remove File");
                self.response(header.id, 0, 0, 16, None, None, None, None)
                    .await?;
            }
            OperationType::DeleteDir => {
                debug!("Remove Dir");
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
