// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(r"cargo:rustc-link-search=native=./lib");
    Ok(())
}
