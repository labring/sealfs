// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use nix::sys::stat::Mode;

use super::EngineError;

pub mod block_device;
pub mod default_engine;
pub trait StorageEngine {
    fn new(db_path: &str, root: &str) -> Self;

    fn init(&self);

    fn get_file_attributes(&self, path: String) -> Result<Vec<u8>, EngineError>;

    fn read_directory(
        &self,
        path: String,
        size: u32,
        offset: i64,
    ) -> Result<(Vec<u8>, Vec<u8>), EngineError>;

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError>;

    fn open_file(&self, path: String, mode: Mode) -> Result<(), EngineError>;

    fn write_file(&self, path: String, data: &[u8], offset: i64) -> Result<usize, EngineError>;

    fn delete_directory_recursive(&self, path: String) -> Result<(), EngineError>;

    fn is_exist(&self, path: String) -> Result<bool, EngineError>;

    fn directory_add_entry(
        &self,
        parent_dir: String,
        file_name: String,
        file_type: u8,
    ) -> Result<(), EngineError>;

    fn directory_delete_entry(
        &self,
        parent_dir: String,
        file_name: String,
        file_type: u8,
    ) -> Result<(), EngineError>;

    fn create_file(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError>;

    fn delete_file(&self, path: String) -> Result<(), EngineError>;

    fn truncate_file(&self, path: String, length: i64) -> Result<(), EngineError>;

    fn create_directory(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError>;

    fn delete_directory(&self, path: String) -> Result<(), EngineError>;
}
