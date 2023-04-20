// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use self::meta_engine::MetaEngine;

use super::EngineError;

pub mod block_engine;
pub mod file_engine;
pub mod meta_engine;
pub mod seal_engine;

pub trait StorageEngine {
    fn new(root: &str, meta_engine: Arc<MetaEngine>) -> Self;

    fn init(&self);

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError>;

    fn open_file(&self, path: String, flag: i32, mode: u32) -> Result<(), EngineError>;

    fn write_file(&self, path: String, data: &[u8], offset: i64) -> Result<usize, EngineError>;

    fn create_file(
        &self,
        path: String,
        oflag: i32,
        umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError>;

    fn delete_file(&self, path: String) -> Result<(), EngineError>;

    fn truncate_file(&self, path: String, length: i64) -> Result<(), EngineError>;
}
