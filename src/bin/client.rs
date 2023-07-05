// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use sealfs::client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = client::run_command().await {
        println!("Error: {}", e);
        return Err(e);
    }
    Ok(())
}
