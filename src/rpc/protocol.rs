// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub const MAX_FILENAME_LENGTH: usize = 4096;
pub const MAX_DATA_LENGTH: usize = 65536 * 128;
pub const MAX_METADATA_LENGTH: usize = 4096;
pub const MAX_COPY_LENGTH: usize = 1024 * 8;

pub const CONNECTION_RETRY_TIMES: i32 = 20;
pub const SEND_RETRY_TIMES: i32 = 3;

// request
// | batch | id | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
// | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
pub const REQUEST_HEADER_SIZE: usize = 4 * 8;
pub const REQUEST_FILENAME_LENGTH_SIZE: usize = 4;
pub const REQUEST_METADATA_LENGTH_SIZE: usize = 4;
pub const REQUEST_DATA_LENGTH_SIZE: usize = 4;

pub const REQUEST_POOL_SIZE: usize = 65536;

/* receive operation response and wake up the operation thread using condition variable
    response
    | batch | id | status | flags | total_length | meta_data_lenght | data_length | meta_data | data |
    | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 0~ |
*/
pub const RESPONSE_HEADER_SIZE: usize = 4 * 7;

// pub const CLIENT_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_micros(300); // timeout for client response loop

#[derive(Debug)]
pub struct RequestHeader {
    pub batch: u32,
    pub id: u32,
    pub r#type: u32,
    pub flags: u32,
    pub total_length: u32, // we use u32 because of the protocol consistency
    pub file_path_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl RequestHeader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch: u32,
        id: u32,
        r#type: u32,
        flags: u32,
        total_length: u32,
        file_path_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
            batch,
            id,
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
    pub batch: u32,
    pub id: u32,
    pub status: i32,
    pub flags: u32,
    pub total_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl ResponseHeader {
    pub fn new(
        batch: u32,
        id: u32,
        status: i32,
        flags: u32,
        total_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
            batch,
            id,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        }
    }
}
