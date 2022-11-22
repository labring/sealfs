// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use common::manager_service::{self, ManagerService};
use manager::heart::healthy_check;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tonic::transport::Server;

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    address: String,
    protect_threshold: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //read from yaml.
    let yaml_str = include_str!("../../manager.yaml");
    let properties: Properties = serde_yaml::from_str(yaml_str).expect("manager.yaml read failed!");
    let address = properties.address;
    let service = ManagerService::default();

    tokio::spawn(async {
        healthy_check().await;
    });

    //build rpc server.
    Server::builder()
        .add_service(manager_service::new_manager_service(service))
        .serve(address.parse().unwrap())
        .await?;

    Ok(())
}
