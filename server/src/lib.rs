// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod server;
pub mod storage_engine;

use std::sync::Arc;

use crate::storage_engine::StorageEngine;

use crate::server::Server;
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
    Rocksdb(#[from] rocksdb::Error),
}

pub async fn run(
    address: String,
    database_path: String,
    storage_path: String,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&address).await?;
    // let engine = Arc::new(Mutex::new(DefaultEngine::new(
    //     &database_path,
    //     &storage_path,
    // )));
    let local_storage = DefaultEngine::new(&database_path, &storage_path);
    let engine = Arc::new(DistributedEngine::new(local_storage));
    let mut server = Server::new(address, listener, engine);
    server.run().await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServerError {
    #[error("ParseHeaderError")]
    ParseHeaderError,
}
