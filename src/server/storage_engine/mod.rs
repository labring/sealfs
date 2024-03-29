// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use self::meta_engine::MetaEngine;

pub mod block_engine;
pub mod file_engine;
pub mod meta_engine;

pub trait StorageEngine {
    fn new(root: &str, meta_engine: Arc<MetaEngine>) -> Self;

    fn init(&self);

    fn read_file(&self, path: &str, size: u32, offset: i64) -> Result<Vec<u8>, i32>;

    fn open_file(&self, path: &str, flag: i32, mode: u32) -> Result<(), i32>;

    fn write_file(&self, path: &str, data: &[u8], offset: i64) -> Result<usize, i32>;

    fn create_file(&self, path: &str, oflag: i32, umask: u32, mode: u32) -> Result<Vec<u8>, i32>;

    fn delete_file(&self, path: &str) -> Result<(), i32>;

    fn truncate_file(&self, path: &str, length: i64) -> Result<(), i32>;
}
