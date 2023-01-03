// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod server;
pub mod storage_engine;

use std::sync::Arc;

use crate::storage_engine::StorageEngine;

use crate::server::Server;
use common::distribute_hash_table::build_hash_ring;
use distributed_engine::DistributedEngine;
use storage_engine::default_engine::DefaultEngine;
use tokio::net::TcpListener;

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
