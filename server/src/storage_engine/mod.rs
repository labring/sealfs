// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::EngineError;

pub mod default_engine;
pub trait StorageEngine {
    fn new(db_path: &str, root: &str) -> Self;

    fn init(&self);

    fn get_file_attributes(&self, path: String) -> Result<Option<Vec<String>>, EngineError>;

    fn read_directory(&self, path: String) -> Result<Vec<String>, EngineError>;

    fn read_file(&self, path: String) -> Result<Vec<u8>, EngineError>;

    fn write_file(&self, path: String, data: &[u8]) -> Result<(), EngineError>;

    fn delete_directory_recursive(&self, path: String) -> Result<(), EngineError>;

    fn is_exist(&self, path: String) -> Result<bool, EngineError>;

    fn directory_add_entry(&self, parent_dir: String, file_name: String)
        -> Result<(), EngineError>;

    fn directory_delete_entry(
        &self,
        parent_dir: String,
        file_name: String,
    ) -> Result<(), EngineError>;

    fn create_file(&self, path: String) -> Result<(), EngineError>;

    fn delete_file(&self, path: String) -> Result<(), EngineError>;

    fn create_directory(&self, path: String) -> Result<(), EngineError>;

    fn delete_directory(&self, path: String) -> Result<(), EngineError>;
}
