mod blob_engine;

use std::sync::{Arc, Mutex};

use async_spdk::{
    blob::{self, Blobstore},
    blob_bdev, event,
};
use log::info;

use self::blob_engine::BlobEngine;

type Result<T> = std::result::Result<T, super::EngineError>;

pub struct SealEngine {
    pub blob_engine: Arc<BlobEngine>,
    pub blobstore: Arc<Mutex<Blobstore>>,
}

impl SealEngine {
    pub async fn new(config_file: String, bs_dev: &str, bs_core: u32, is_reload: bool) -> Self {
        let bs = event::AppOpts::new()
            .name("seal_engine")
            .config_file(&config_file)
            .block_on(Self::start_spdk_helper(bs_dev, is_reload))
            .unwrap();

        let bs_arc = Arc::new(Mutex::new(bs));
        let blob_engine = BlobEngine::new(bs_dev.to_string(), bs_core, Arc::clone(&bs_arc));
        info!("SealEngine initialization complete");

        let seal_engine = SealEngine {
            blob_engine: Arc::new(blob_engine),
            blobstore: bs_arc,
        };
        seal_engine
    }

    pub async fn get_blob_engine(&self) -> Arc<BlobEngine> {
        Arc::clone(&self.blob_engine)
    }

    async fn start_spdk_helper(bs_dev: &str, is_reload: bool) -> Result<Blobstore> {
        let mut blob_store = blob_bdev::BlobStoreBDev::create(&bs_dev).unwrap();
        if !is_reload {
            Ok(blob::Blobstore::init(&mut blob_store).await.unwrap())
        } else {
            Ok(blob::Blobstore::load(&mut blob_store).await.unwrap())
        }
    }
}
