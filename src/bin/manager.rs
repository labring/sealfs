// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use sealfs::{manager::manager_service::ManagerService, rpc::server::Server};
use serde::{Deserialize, Serialize};
use std::fs;
use std::{fmt::Debug, sync::Arc};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    protect_threshold: Option<String>,
    #[arg(long)]
    config_file: Option<String>,
}

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
    let mut properties: Properties;
    // read from command line.
    let args = Args::parse();
    // if the user provides the config file, parse it and use the arguments from the config file.
    match args.config_file {
        None => {
            //read from default configuration.
            let yaml_str = include_str!("../../examples/manager.yaml");
            properties = serde_yaml::from_str(yaml_str).expect("manager.yaml read failed!");
        }
        Some(c) => {
            // read from user-provided config file
            let yaml_str = fs::read_to_string(c).expect("Couldn't read from file. The file is either missing or you don't have enough permissions!");
            properties = serde_yaml::from_str(&yaml_str).expect("server.yaml read failed!");
        }
    }
    properties.address = if let Some(addr) = args.address {
        addr
    } else {
        properties.address
    };
    properties.protect_threshold = if let Some(t) = args.protect_threshold {
        t
    } else {
        properties.protect_threshold
    };

    let address = properties.address;

    let server = Server::new(Arc::new(ManagerService::default()), &address);
    server.run().await?;
    Ok(())
}
