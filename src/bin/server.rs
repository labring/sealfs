// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use log::{info, warn};
use sealfs::common::serialization::ManagerOperationType;
use sealfs::manager::manager_service::SendHeartRequest;
use sealfs::rpc::client::Client;
use sealfs::server;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;
use tokio::time;
use tokio::time::MissedTickBehavior;

const _SERVER_FLAG: u32 = 1;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    manager_address: Option<String>,
    #[arg(required = true, long)]
    server_address: Option<String>,
    #[arg(required = true, long)]
    database_path: Option<String>,
    #[arg(long)]
    cache_capacity: Option<usize>,
    #[arg(long)]
    write_buffer_size: Option<usize>,
    #[arg(required = true, long)]
    storage_path: Option<String>,
    #[arg(long)]
    heartbeat: Option<bool>,
    #[arg(long)]
    log_level: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Properties {
    manager_address: String,
    server_address: String,
    database_path: String,
    cache_capacity: usize,
    write_buffer_size: usize,
    storage_path: String,
    heartbeat: bool,
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), Box<dyn std::error::Error>> {
    // read from command line.
    let args: Args = Args::parse();
    // if the user provides the config file, parse it and use the arguments from the config file.
    let properties: Properties = Properties {
        manager_address: args.manager_address.unwrap_or("127.0.0.1:8081".to_owned()),
        server_address: args.server_address.unwrap(),
        database_path: args.database_path.unwrap(),
        cache_capacity: args.cache_capacity.unwrap_or(13421772),
        write_buffer_size: args.write_buffer_size.unwrap_or(0x4000000),
        storage_path: args.storage_path.unwrap(),
        heartbeat: args.heartbeat.unwrap_or(false),
        log_level: args.log_level.unwrap_or("warn".to_owned()),
    };

    let mut builder = env_logger::Builder::from_default_env();
    builder.format_timestamp(None).filter(
        None,
        match log::LevelFilter::from_str(&properties.log_level) {
            Ok(level) => level,
            Err(_) => {
                warn!("Invalid log level, use default level: warn");
                log::LevelFilter::Warn
            }
        },
    );
    builder.init();

    let manager_address = properties.manager_address;
    let server_address = properties.server_address.clone();

    //connect to manager

    info!("server_address: {}", server_address.clone());
    // if properties.heartbeat {

    //     //begin scheduled task
    //     tokio::spawn(begin_heartbeat_report(
    //         client,
    //         manager_address,
    //         server_address.clone(),
    //         properties.lifetime.clone(),
    //     ));
    // }

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
        manager_address,
        properties.cache_capacity,
        properties.write_buffer_size,
    )
    .await?;
    // Server::builder()
    //     .add_service(health_service)
    //     .add_service(service::new_fs_service(fs_service))
    //     .serve(properties.server_address.parse().unwrap())
    //     .await?;
    Ok(())
}

async fn _begin_heartbeat_report(
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
            flags: _SERVER_FLAG,
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
                    ManagerOperationType::SendHeart.into(),
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
