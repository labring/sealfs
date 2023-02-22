// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time;

pub const MAX_FILENAME_LENGTH: usize = 4096;
pub const MAX_DATA_LENGTH: usize = 65535;
pub const MAX_METADATA_LENGTH: usize = 4096;

// request
// | uid | index | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
// | 8Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
pub const REQUEST_HEADER_SIZE: usize = 8 + 4 * 7;
pub const REQUEST_FILENAME_LENGTH_SIZE: usize = 4;
pub const REQUEST_METADATA_LENGTH_SIZE: usize = 4;
pub const REQUEST_DATA_LENGTH_SIZE: usize = 4;

pub const REQUEST_POOL_SIZE: usize = 65536;
pub const CLIENT_REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(10);

/* receive operation response and wake up the operation thread using condition variable
    response
    | uid | index | status | flags | total_length | meta_data_lenght | data_length | meta_data | data |
    | 8Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 0~ |
*/
pub const RESPONSE_HEADER_SIZE: usize = 8 + 4 * 6;

// pub const CLIENT_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_micros(300); // timeout for client response loop

pub struct RequestHeader {
    pub uid: u64,
    pub index: u32,
    pub r#type: u32,
    pub flags: u32,
    pub total_length: u32, // we use u32 because of the protocol consistency
    pub file_path_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl RequestHeader {
    pub fn new(
        uid: u64,
        index: u32,
        r#type: u32,
        flags: u32,
        total_length: u32,
        file_path_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
            uid,
            index,
            r#type,
            flags,
            total_length,
            file_path_length,
            meta_data_length,
            data_length,
        }
    }
}

pub struct ResponseHeader {
    pub uid: u64,
    pub index: u32,
    pub status: i32,
    pub flags: u32,
    pub total_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl ResponseHeader {
    pub fn new(
        uid: u64,
        index: u32,
        status: i32,
        flags: u32,
        total_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
            uid,
            index,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        }
    }
}
