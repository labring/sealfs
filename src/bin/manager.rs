// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use env_logger::fmt;
use log::{error, info, warn};
use sealfs::manager::manager_service::update_server_status;
use sealfs::{manager::manager_service::ManagerService, rpc::server::RpcServer};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::str::FromStr;
use std::{fmt::Debug, sync::Arc};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    address: Option<String>,
    #[arg(long)]
    config_file: Option<String>,
    /// To use customized configuration or not. If this flag is used, please provide a config file through --config_file <path>
    #[arg(long)]
    use_config_file: bool,
    #[arg(long)]
    log_level: Option<String>,
    #[arg(long)]
    all_servers_address: Option<Vec<String>>,
    #[arg(long)]
    virtual_nodes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    address: String,
    all_servers_address: Vec<String>,
    virtual_nodes: usize,
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut builder = env_logger::Builder::from_default_env();

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
                    let mut result: Properties =
                        serde_yaml::from_str(&yaml_str).expect("manager.yaml read failed!");
                    if args.log_level.is_some() {
                        result.log_level = args.log_level.unwrap();
                    }
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
            all_servers_address: args
                .all_servers_address
                .unwrap_or(default_properties.all_servers_address),
            virtual_nodes: args
                .virtual_nodes
                .unwrap_or(default_properties.virtual_nodes),
            log_level: args.log_level.unwrap_or(default_properties.log_level),
        },
    };

    builder
        .format_timestamp(Some(fmt::TimestampPrecision::Millis))
        .filter(
            None,
            log::LevelFilter::from_str(&properties.log_level).unwrap(),
        );
    builder.init();

    info!("Starting manager with log level: {}", properties.log_level);

    let address = properties.address;

    let servers_address = properties
        .all_servers_address
        .iter()
        .map(|s| (s.to_string(), properties.virtual_nodes))
        .collect::<Vec<(String, usize)>>();

    info!("All servers address: {:?}", servers_address);

    let manager = Arc::new(ManagerService::new(servers_address.clone()));

    let server = Arc::new(RpcServer::new(manager.clone(), &address));

    info!("Manager started at {}", address);

    let new_manager = manager.clone();

    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("Manager server error: {}", e);
            new_manager
                .manager
                .closed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });

    update_server_status(manager.manager.clone()).await;

    Ok(())
}
