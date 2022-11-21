// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("test");
    let result = client::init_fs_client();
    println!("client stopped. success = {:?}", result.is_ok());
    Ok(())
}
