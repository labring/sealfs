// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use log::info;
use sealfs::common::serialization::OperationType;
use sealfs::manager::manager_service::SendHeartRequest;
use sealfs::rpc::client::Client;
use sealfs::server;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
use std::io::Read;
use std::str::FromStr;
use tokio::time;
use tokio::time::MissedTickBehavior;

const SERVER_FLAG: u32 = 1;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    manager_address: Option<String>,
    #[arg(long)]
    server_address: Option<String>,
    #[arg(long)]
    all_servers_address: Option<Vec<String>>,
    #[arg(long)]
    lifetime: Option<String>,
    #[arg(long)]
    database_path: Option<String>,
    #[arg(long)]
    storage_path: Option<String>,
    #[arg(long)]
    heartbeat: Option<bool>,
    #[arg(long)]
    log_level: Option<String>,
    /// The path of the configuration file
    #[arg(long)]
    config_file: Option<String>,
    /// To use customized configuration or not. If this flag is used, please provide a config file through --config_file <path>
    #[arg(long)]
    use_config_file: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    manager_address: String,
    server_address: String,
    all_servers_address: Vec<String>,
    lifetime: String,
    database_path: String,
    storage_path: String,
    heartbeat: bool,
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    // read from default configuration.
    let config_path = std::env::var("SEALFS_CONFIG_PATH").unwrap_or_else(|_| "~".to_string());
    let mut config_file = std::fs::File::open(format!("{}/{}", config_path, "server.yaml"))
        .expect("server.yaml open failed!");
    let mut config_str = String::new();
    config_file
        .read_to_string(&mut config_str)
        .expect("server.yaml read failed!");
    let default_properties: Properties =
        serde_yaml::from_str(&config_str).expect("server.yaml serializa failed!");
    // read from command line.
    let args: Args = Args::parse();
    // if the user provides the config file, parse it and use the arguments from the config file.
    let properties: Properties = match args.use_config_file {
        true => {
            // read from default configuration.
            match args.config_file {
                Some(c) => {
                    // read from user-provided config file
                    let yaml_str = fs::read_to_string(c).expect("Couldn't read from file. The file is either missing or you don't have enough permissions!");
                    let result: Properties =
                        serde_yaml::from_str(&yaml_str).expect("server.yaml read failed!");
                    result
                }
                _ => {
                    println!(
                        "No custom configuration provided, fallback to the default configuration."
                    );
                    default_properties
                }
            }
        }
        false => Properties {
            manager_address: args
                .manager_address
                .unwrap_or(default_properties.manager_address),
            server_address: args
                .server_address
                .unwrap_or(default_properties.server_address),
            all_servers_address: args
                .all_servers_address
                .unwrap_or(default_properties.all_servers_address),

            lifetime: args.lifetime.unwrap_or(default_properties.lifetime),
            database_path: args
                .database_path
                .unwrap_or(default_properties.database_path),
            storage_path: args.storage_path.unwrap_or(default_properties.storage_path),
            heartbeat: args.heartbeat.unwrap_or(default_properties.heartbeat),
            log_level: args.log_level.unwrap_or(default_properties.log_level),
        },
    };

    let mut builder = env_logger::Builder::from_default_env();
    builder.format_timestamp(None).filter(
        None,
        log::LevelFilter::from_str(&properties.log_level).unwrap(),
    );
    builder.init();

    let manager_address = properties.manager_address;
    let server_address = properties.server_address.clone();
    //connect to manager
    info!("server_address: {}", server_address.clone());
    if properties.heartbeat {
        info!("Connect To Manager.");
        let client = Client::new();
        client.add_connection(&manager_address).await;

        //begin scheduled task
        tokio::spawn(begin_heartbeat_report(
            client,
            manager_address,
            server_address.clone(),
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
        properties.database_path,
        properties.storage_path,
        server_address,
        properties.all_servers_address.clone(),
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
    client: Client,
    manager_address: String,
    server_address: String,
    lifetime: String,
) {
    let mut interval = time::interval(time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let request = SendHeartRequest {
            address: server_address.clone(),
            flags: SERVER_FLAG,
            lifetime: lifetime.clone(),
        };
        let mut status = 0i32;
        let mut rsp_flags = 0u32;
        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;
        {
            let result = client
                .call_remote(
                    &manager_address,
                    OperationType::SendHeart.into(),
                    0,
                    &server_address,
                    &bincode::serialize(&request).unwrap(),
                    &[],
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
