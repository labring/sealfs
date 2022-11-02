// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use sealfs_rust::manager_service::{self, ManagerService};
use serde::{Deserialize, Serialize};
use tonic::transport::Server;

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    port: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //read from yaml.
    let yaml_str = include_str!("../../manager.yaml");
    let properties: Properties = serde_yaml::from_str(yaml_str).expect("manager.yaml read failed!");
    let address = properties.port;
    let service = ManagerService::default();

    //build rpc server.
    Server::builder()
        .add_service(manager_service::new_manager_service(service))
        .serve(address.parse().unwrap())
        .await?;

    Ok(())
}
