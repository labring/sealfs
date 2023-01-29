// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use sealfs::{manager::manager_service::ManagerService, rpc::server::Server};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, sync::Arc};

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    address: String,
    protect_threshold: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::Debug);
    builder.init();
    //read from yaml.
    let yaml_str = include_str!("../../examples/manager.yaml");
    let properties: Properties = serde_yaml::from_str(yaml_str).expect("manager.yaml read failed!");
    let address = properties.address;

    let server = Server::new(Arc::new(ManagerService::default()), &address);
    server.run().await?;
    Ok(())
}
