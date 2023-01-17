// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub trait BlockStorage {
    /**
     * Default aio.
     */
    fn write_file(&self, file_name: String, data: &[u8], offset: i64);

    /**
     * Default aio.  
     */
    fn read_file(&self, file_name: String, data: &[u8], offset: i64);

    fn create_file(&self, file_name: String);

    fn delete_file(&self, file_name: String);
}
