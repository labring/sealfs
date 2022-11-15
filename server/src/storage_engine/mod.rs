// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::EngineError;

pub mod default_engine;
pub trait StorageEngine {
    fn new(db_path: &str, root: &str) -> Self;

    fn init(&self);

    fn create_file(&self, path: String) -> Result<(), EngineError>;

    fn create_directory(&self, path: String) -> Result<(), EngineError>;

    fn get_file_attributes(&self, path: String) -> Result<Option<Vec<String>>, EngineError>;

    fn read_directory(&self, path: String) -> Result<Vec<String>, EngineError>;

    fn read_file(&self, path: String) -> Result<Vec<u8>, EngineError>;

    fn write_file(&self, path: String, data: &[u8]) -> Result<(), EngineError>;

    fn delete_file(&self, path: String) -> Result<(), EngineError>;

    fn delete_directory(&self, path: String) -> Result<(), EngineError>;

    fn delete_directory_recursive(&self, path: String) -> Result<(), EngineError>;
}
