// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod server;
pub mod storage_engine;

use crate::server::Server;
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

pub async fn run(address: String) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&address).await?;
    let mut server = Server::new(address, listener);
    server.run().await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ServerError {
    #[error("ParseHeaderError")]
    ParseHeaderError,
}
