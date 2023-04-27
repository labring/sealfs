// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use log::warn;
use sealfs::{manager::manager_service::ManagerService, rpc::server::Server};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
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
    /// To use customized configuration or not. If this flag is used, please provide a config file through --config_file <path>
    #[arg(long)]
    use_config_file: bool,
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

    // read from default configuration.
    let config_path = std::env::var("SEALFS_CONFIG_PATH").unwrap_or("~".to_string());
    let mut config_file = std::fs::File::open(format!("{}/{}", config_path, "manager.yaml"))
        .expect("manager.yaml open failed!");
    let mut config_str = String::new();
    config_file
        .read_to_string(&mut config_str)
        .expect("manager.yaml read failed!");
    let default_properties: Properties =
        serde_yaml::from_str(&config_str).expect("manager.yaml serializa failed!");

    // read from command line.
    let args: Args = Args::parse();
    let properties: Properties = match args.use_config_file {
        true => {
            // read from user-provided config file
            match args.config_file {
                Some(c) => {
                    let yaml_str = fs::read_to_string(c).expect("Couldn't read from file. The file is either missing or you don't have enough permissions!");
                    let result: Properties =
                        serde_yaml::from_str(&yaml_str).expect("manager.yaml read failed!");
                    result
                }
                _ => {
                    warn!(
                        "No custom configuration provided, fallback to the default configuration."
                    );
                    default_properties
                }
            }
        }
        false => Properties {
            address: args.address.unwrap_or(default_properties.address),
            protect_threshold: args
                .protect_threshold
                .unwrap_or(default_properties.protect_threshold),
        },
    };

    let address = properties.address;

    let server = Server::new(Arc::new(ManagerService::default()), &address);
    server.run().await?;
    Ok(())
}
