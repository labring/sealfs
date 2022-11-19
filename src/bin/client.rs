// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use sealfs_rust::manager_service::manager::manager_client::ManagerClient;
use sealfs_rust::manager_service::manager::MetadataRequest;
use serde::{Deserialize, Serialize};

const CLIENT_FLAG: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    manager_address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let attr = include_str!("../../client.yaml");
    let config: Config = serde_yaml::from_str(attr).expect("client.yaml read failed!");
    let manager_address = config.manager_address;
    let http_manager_address = format!("http://{}", manager_address);

    tokio::spawn(async {
        let mut client = ManagerClient::connect(http_manager_address).await.unwrap();
        let request = tonic::Request::new(MetadataRequest { flag: CLIENT_FLAG });
        let result = client.get_metadata(request).await;
        if result.is_err() {
            panic!("get metadata error.");
        }
    })
    .await?;

    let result = client::init_fs_client();
    println!("client stopped. success = {:?}", result.is_ok());
    Ok(())
}
