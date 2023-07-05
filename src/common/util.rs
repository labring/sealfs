// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub fn get_full_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        return format!("/{}", name);
    }
    let path = format!("{}/{}", parent, name);
    path
}
