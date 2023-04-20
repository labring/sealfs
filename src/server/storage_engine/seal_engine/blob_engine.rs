#![allow(unused)]
use std::sync::{Arc, Mutex};

use async_spdk::{
    blob::{Blob, BlobId, Blobstore, IoChannel},
    blob_bdev::BlobStoreBDev,
};
use log::info;

use crate::server::EngineError;

use super::Result;

pub struct BlobEngine {
    pub name: String,
    pub core: u32,
    pub bs: Arc<Mutex<Blobstore>>,
}

impl std::fmt::Display for BlobEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BlobEngine INFO:\n\tname: {:?}\n\tcore: {}\n",
            self.name.clone(),
            self.core,
        )
    }
}

impl BlobEngine {
    pub fn new(name: String, core: u32, bs: Arc<Mutex<Blobstore>>) -> Self {
        Self {
            name: name.to_string(),
            core,
            bs,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_core_id(&self) -> u32 {
        self.core
    }

    pub async fn create_blob(&self) -> Result<BlobId> {
        let bid = self
            .bs
            .lock()
            .map_err(|e| EngineError::SyncPoison)?
            .create_blob()
            .await?;
        info!("Create Blob");
        Ok(bid)
    }

    pub async fn open_blob(&self, bid: BlobId) -> Result<Blob> {
        let blob = self
            .bs
            .lock()
            .map_err(|e| EngineError::SyncPoison)?
            .open_blob(bid)
            .await?;
        Ok(blob)
    }

    pub async fn close_blob(&self, blob: Blob) -> Result<()> {
        blob.close().await;
        Ok(())
    }

    pub async fn unload(&self) -> Result<()> {
        self.bs
            .lock()
            .map_err(|e| EngineError::SyncPoison)?
            .unload()
            .await;
        Ok(())
    }

    pub async fn read(&self, offset: u64, bid: BlobId, buf: &mut [u8]) -> Result<()> {
        let blob = self.open_blob(bid).await?;
        let channel = self
            .bs
            .lock()
            .map_err(|e| EngineError::SyncPoison)?
            .alloc_io_channel()?;
        blob.read(&channel, offset, buf).await?;
        blob.close().await?;
        drop(channel);
        Ok(())
    }

    pub async fn write(&self, offset: u64, bid: BlobId, buf: &[u8]) -> Result<()> {
        let blob = self.open_blob(bid).await?;
        let channel = self
            .bs
            .lock()
            .map_err(|e| EngineError::SyncPoison)?
            .alloc_io_channel()?;
        blob.write(&channel, offset, buf).await?;
        blob.close().await?;
        drop(channel);
        Ok(())
    }
}
