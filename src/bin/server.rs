// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use log::info;
use sealfs::common::request::OperationType;
use sealfs::rpc::client::ClientAsync;
use sealfs::server;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::time;
use tokio::time::MissedTickBehavior;

const SERVER_FLAG: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    manager_address: String,
    server_address: String,
    lifetime: String,
    database_path: String,
    storage_path: String,
    // it will merge into 'server_address' in the future
    local_distributed_address: String,
    all_distributed_address: Vec<String>,
    heartbeat: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Debug);
    builder.init();

    //todo
    //read from command line.

    //read from yaml
    let yaml_str = include_str!("../../examples/server.yaml");
    let properties: Properties = serde_yaml::from_str(yaml_str).expect("server.yaml read failed!");
    let manager_address = properties.manager_address;
    let _server_address = properties.server_address.clone();
    //connect to manager

    if properties.heartbeat {
        info!("Connect To Manager.");
        let client = ClientAsync::new();
        client.add_connection(&manager_address).await;

        //begin scheduled task
        tokio::spawn(begin_heartbeat_report(
            client,
            manager_address,
            properties.server_address.clone(),
            properties.lifetime.clone(),
        ));
    }

    //todo
    //start server
    // let fs_service = FsService::default();
    // let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    // health_reporter
    //     .set_serving::<RemoteFsServer<FsService>>()
    //     .await;
    info!("Start Server");
    server::run(
        properties.server_address.clone(),
        properties.database_path.clone(),
        properties.storage_path.clone(),
        properties.local_distributed_address.clone(),
        properties.all_distributed_address.clone(),
    )
    .await?;
    // Server::builder()
    //     .add_service(health_service)
    //     .add_service(service::new_fs_service(fs_service))
    //     .serve(properties.server_address.parse().unwrap())
    //     .await?;

    Ok(())
}

async fn begin_heartbeat_report(
    client: ClientAsync,
    manager_address: String,
    server_address: String,
    lifetime: String,
) {
    let mut interval = time::interval(time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let mut data = Vec::with_capacity(4 + lifetime.len());
        data.extend_from_slice(&(lifetime.len() as u32).to_le_bytes());
        data.extend_from_slice(lifetime.as_bytes());
        let mut status = 0i32;
        let mut rsp_flags = 0u32;
        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;
        {
            let result = client
                .call_remote(
                    &manager_address,
                    OperationType::SendHeart.into(),
                    SERVER_FLAG,
                    &server_address,
                    &[],
                    &data,
                    &mut status,
                    &mut rsp_flags,
                    &mut recv_meta_data_length,
                    &mut recv_data_length,
                    &mut [],
                    &mut [],
                )
                .await;
            if result.is_err() {
                panic!("send heartbeat error. {:?}", result);
            }
        }
        interval.tick().await;
    }
}
