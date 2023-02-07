// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use log::info;
use sealfs::common::serialization::OperationType;
use sealfs::manager::manager_service::SendHeartRequest;
use sealfs::rpc::client::ClientAsync;
use sealfs::server;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs;
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
    config_file: Option<String>,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
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
            // read from default configuration.
            let yaml_str = include_str!("../../examples/server.yaml");
            properties = serde_yaml::from_str(yaml_str).expect("server.yaml read failed!");
        }
        Some(c) => {
            // read from user-provided config file
            let yaml_str = fs::read_to_string(c).expect("Couldn't read from file. The file is either missing or you don't have enough permissions!");
            properties = serde_yaml::from_str(&yaml_str).expect("server.yaml read failed!");
        }
    }
    // if the user provides/initialize arguments through the command line, replace the corresponding ones.
    properties.manager_address = if let Some(addr) = args.manager_address {
        addr
    } else {
        properties.manager_address
    };
    properties.server_address = if let Some(addr) = args.server_address {
        addr
    } else {
        properties.server_address
    };
    properties.all_servers_address = if let Some(addr) = args.all_servers_address {
        addr
    } else {
        properties.all_servers_address
    };
    properties.lifetime = if let Some(l) = args.lifetime {
        l
    } else {
        properties.lifetime
    };
    properties.database_path = if let Some(p) = args.database_path {
        p
    } else {
        properties.database_path
    };
    properties.storage_path = if let Some(p) = args.storage_path {
        p
    } else {
        properties.storage_path
    };
    properties.heartbeat = if let Some(h) = args.heartbeat {
        h
    } else {
        properties.heartbeat
    };

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
        properties.database_path.clone(),
        properties.storage_path.clone(),
        properties.server_address.clone(),
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
    client: ClientAsync,
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
