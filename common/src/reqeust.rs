// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time;

// request
// | id | type | flags | total_length | filename_length | filename | meta_data_length | meta_data | data_length | data |
// | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 4Byte | 0~ | 4Byte | 0~ |
pub const REQUEST_HEADER_SIZE: usize = 16;

pub const REQUEST_QUEUE_LENGTH: usize = 65535;
pub const CLIENT_REQUEST_TIMEOUT: time::Duration = time::Duration::from_secs(3);

/* receive operation response and wake up the operation thread using condition variable
    response
    | id | status | flags | total_length | meta_data_lenght | meta_data | data_length | data |
    | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 4Byte | 0~ |
*/
pub const RESPONSE_HEADER_SIZE: usize = 16;

pub const CLIENT_RESPONSE_TIMEOUT: time::Duration = time::Duration::from_micros(300); // timeout for client response loop

pub struct RequestHeader {
    pub id: u32,
    pub r#type: OperationType,
    pub flags: u32,
    pub total_length: u32,
}

impl RequestHeader {
    pub fn new(id: u32, r#type: OperationType, flags: u32, total_length: u32) -> Self {
        Self {
            id,
            r#type,
            flags,
            total_length,
        }
    }
}

pub enum OperationType {
    Lookup = 0,
    CreateFile = 1,
    CreateDir = 2,
    GetFileAttr = 3,
    ReadDir = 4,
    OpenFile = 5,
    ReadFile = 6,
    WriteFile = 7,
}

impl TryFrom<u32> for OperationType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OperationType::CreateFile),
            2 => Ok(OperationType::CreateDir),
            3 => Ok(OperationType::GetFileAttr),
            4 => Ok(OperationType::ReadDir),
            5 => Ok(OperationType::OpenFile),
            6 => Ok(OperationType::ReadFile),
            7 => Ok(OperationType::WriteFile),
            _ => panic!("Unkown value: {}", value),
        }
    }
}

impl OperationType {
    pub fn to_le_bytes(&self) -> [u8; 4] {
        match self {
            OperationType::Lookup => 0u32.to_le_bytes(),
            OperationType::CreateFile => 1u32.to_le_bytes(),
            OperationType::CreateDir => 2u32.to_le_bytes(),
            OperationType::GetFileAttr => 3u32.to_le_bytes(),
            OperationType::ReadDir => 4u32.to_le_bytes(),
            OperationType::OpenFile => 5u32.to_le_bytes(),
            OperationType::ReadFile => 6u32.to_le_bytes(),
            OperationType::WriteFile => 7u32.to_le_bytes(),
        }
    }
}
