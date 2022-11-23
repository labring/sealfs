// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod distributed_engine;
pub mod server;
pub mod storage_engine;

use std::sync::Arc;

use crate::storage_engine::StorageEngine;

use crate::server::Server;
use distributed_engine::{
    engine_rpc::{self, enginerpc::enginerpc_client::EnginerpcClient, RPCService},
    DistributedEngine,
};
use storage_engine::default_engine::DefaultEngine;
use tokio::net::TcpListener;
use tokio::time;
use tokio::time::MissedTickBehavior;

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

    let service = RPCService::new(local_storage.clone());
    let local = local_distributed_address.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(engine_rpc::new_manager_service(service))
            .serve((&local).parse().unwrap())
            .await
    });

    let engine = Arc::new(DistributedEngine::new(
        local_distributed_address.clone(),
        local_storage.clone(),
    ));
    for value in all_servers_address.clone().into_iter() {
        engine.add_connection(value, None);
    }
    for value in all_servers_address.clone().into_iter() {
        if local_distributed_address == value.clone() {
            continue;
        }
        let server_address = format!("http://{}", value);
        let arc_engine = engine.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(time::Duration::from_secs(5));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let result = EnginerpcClient::connect(server_address.clone()).await;
                match result {
                    Ok(connection) => {
                        arc_engine.add_connection(value.clone(), Some(connection));
                        break;
                    }
                    Err(_) => interval.tick().await,
                };
            }
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
