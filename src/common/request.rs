// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time;

use log::debug;

pub const MAX_FILENAME_LENGTH: usize = 4096;
pub const MAX_DATA_LENGTH: usize = 65535;
pub const MAX_METADATA_LENGTH: usize = 4096;

// request
// | id | type | flags | total_length | file_path_length | meta_data_length | data_length | filename | meta_data | data |
// | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 0~ | 0~ |
pub const REQUEST_HEADER_SIZE: usize = 4 * 7;
pub const REQUEST_FILENAME_LENGTH_SIZE: usize = 4;
pub const REQUEST_METADATA_LENGTH_SIZE: usize = 4;
pub const REQUEST_DATA_LENGTH_SIZE: usize = 4;

pub const REQUEST_QUEUE_LENGTH: usize = 65535;
pub const CLIENT_REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(10);

/* receive operation response and wake up the operation thread using condition variable
    response
    | id | status | flags | total_length | meta_data_lenght | data_length | meta_data | data |
    | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 0~ |
*/
pub const RESPONSE_HEADER_SIZE: usize = 4 * 6;

// pub const CLIENT_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_micros(300); // timeout for client response loop

pub struct RequestHeader {
    pub id: u32,
    pub r#type: u32,
    pub flags: u32,
    pub total_length: u32, // we use u32 because of the protocol consistency
    pub file_path_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl RequestHeader {
    pub fn new(
        id: u32,
        r#type: u32,
        flags: u32,
        total_length: u32,
        file_path_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
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
    pub id: u32,
    pub status: i32,
    pub flags: u32,
    pub total_length: u32,
    pub meta_data_length: u32,
    pub data_length: u32,
}

impl ResponseHeader {
    pub fn new(
        id: u32,
        status: i32,
        flags: u32,
        total_length: u32,
        meta_data_length: u32,
        data_length: u32,
    ) -> Self {
        Self {
            id,
            status,
            flags,
            total_length,
            meta_data_length,
            data_length,
        }
    }
}

pub enum OperationType {
    Unkown = 0,
    Lookup = 1,
    CreateFile = 2,
    CreateDir = 3,
    GetFileAttr = 4,
    ReadDir = 5,
    OpenFile = 6,
    ReadFile = 7,
    WriteFile = 8,
    DeleteFile = 9,
    DeleteDir = 10,
    DirectoryAddEntry = 11,
    DirectoryDeleteEntry = 12,
}

impl TryFrom<u32> for OperationType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        debug!("OperationType::try_from: value = {}", value);
        match value {
            0 => Ok(OperationType::Unkown),
            1 => Ok(OperationType::Lookup),
            2 => Ok(OperationType::CreateFile),
            3 => Ok(OperationType::CreateDir),
            4 => Ok(OperationType::GetFileAttr),
            5 => Ok(OperationType::ReadDir),
            6 => Ok(OperationType::OpenFile),
            7 => Ok(OperationType::ReadFile),
            8 => Ok(OperationType::WriteFile),
            9 => Ok(OperationType::DeleteFile),
            10 => Ok(OperationType::DeleteDir),
            11 => Ok(OperationType::DirectoryAddEntry),
            12 => Ok(OperationType::DirectoryDeleteEntry),
            _ => panic!("Unkown value: {}", value),
        }
    }
}

impl OperationType {
    pub fn to_le_bytes(&self) -> [u8; 4] {
        match self {
            OperationType::Unkown => 0u32.to_le_bytes(),
            OperationType::Lookup => 1u32.to_le_bytes(),
            OperationType::CreateFile => 2u32.to_le_bytes(),
            OperationType::CreateDir => 3u32.to_le_bytes(),
            OperationType::GetFileAttr => 4u32.to_le_bytes(),
            OperationType::ReadDir => 5u32.to_le_bytes(),
            OperationType::OpenFile => 6u32.to_le_bytes(),
            OperationType::ReadFile => 7u32.to_le_bytes(),
            OperationType::WriteFile => 8u32.to_le_bytes(),
            OperationType::DeleteFile => 9u32.to_le_bytes(),
            OperationType::DeleteDir => 10u32.to_le_bytes(),
            OperationType::DirectoryAddEntry => 11u32.to_le_bytes(),
            OperationType::DirectoryDeleteEntry => 12u32.to_le_bytes(),
        }
    }
}
