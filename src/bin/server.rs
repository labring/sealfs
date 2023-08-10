// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use log::info;
use sealfs::server;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;

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
            Err(_) => log::LevelFilter::Warn,
        },
    );
    builder.init();

    info!("start server with properties: {:?}", properties);

    let manager_address = properties.manager_address;
    let server_address = properties.server_address.clone();

    server::run(
        properties.database_path,
        properties.storage_path,
        server_address,
        manager_address,
        properties.cache_capacity,
        properties.write_buffer_size,
    )
    .await?;
    Ok(())
}
