// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub trait BlockStorage {
    /**
     * Default aio.
     */
    fn write(&self, file_name: String, data: &[u8], offset: i64);

    /**
     * Default aio.  
     */
    fn read(&self, file_name: String, data: &[u8], offset: i64);
}
