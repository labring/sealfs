// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use log::info;
use manager_service::{manager_client::ManagerClient, HeartRequest};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::time;
use tokio::time::MissedTickBehavior;
use tonic::transport::Channel;

const SERVER_FLAG: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    manager_address: String,
    server_address: String,
    lifetime: String,
    database_path: String,
    storage_path: String,
    local_distributed_address: String,
    all_distributed_address: Vec<String>,
}

pub mod manager_service {
    tonic::include_proto!("manager");
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    //todo
    //read from command line.

    //read from yaml
    let yaml_str = include_str!("../../server.yaml");
    let properties: Properties = serde_yaml::from_str(yaml_str).expect("server.yaml read failed!");
    let manager_address = properties.manager_address;
    let http_manager_address = format!("http://{}", manager_address);
    let _server_address = properties.server_address.clone();

    //connect to manager
    info!("Connect To Manager.");
    let client = ManagerClient::connect(http_manager_address).await?;

    //begin scheduled task
    tokio::spawn(begin_heartbeat_report(
        client,
        properties.server_address.clone(),
        properties.lifetime.clone(),
    ));

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
    mut client: ManagerClient<Channel>,
    server_address: String,
    lifetime: String,
) {
    let mut interval = time::interval(time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let request = tonic::Request::new(HeartRequest {
            address: server_address.clone(),
            flag: SERVER_FLAG,
            lifetime: lifetime.clone(),
        });
        let result = client.send_heart(request).await;
        if result.is_err() {
            panic!("send heartbeat error.");
        }
        interval.tick().await;
    }
}
