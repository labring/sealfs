// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod allocator;
pub mod index;
pub mod io;

use std::sync::Arc;

use crate::server::storage_engine::StorageEngine;
use crate::server::EngineError;
use nix::sys::stat::Mode;

use allocator::{Allocator, BlockAllocator};
use index::FileIndex;
use io::Storage;

use super::meta_engine::MetaEngine;

/**
*block device is use to bypass filesystem aimed to attain higher performance.
*/
#[allow(unused)]
pub struct BlockEngine {
    allocator: BlockAllocator,
    index: FileIndex,
    storage: Storage,
}

#[derive(Clone, Copy)]
pub(crate) struct AllocatorEntry {
    begin: u64,
    length: u64,
}

impl StorageEngine for BlockEngine {
    fn new(root: &str, _meta: Arc<MetaEngine>) -> Self {
        let index = FileIndex::new();
        let storage = Storage::new(root);
        let allocator = BlockAllocator::new(root);
        Self {
            allocator,
            index,
            storage,
        }
    }

    fn init(&self) {}

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        let index_vec = self.index.search(path.as_str());
        if index_vec.is_empty() {
            return Err(EngineError::IO);
        }
        let iter = index_vec.iter();
        let mut temp_offset = offset;
        let mut real_offset = 0;
        for index in iter {
            temp_offset -= index.length as i64;
            if temp_offset < 0 {
                real_offset = (index.begin + index.length) as i64 + temp_offset;
                break;
            }
        }
        self.storage.read(size, real_offset)
    }

    fn open_file(&self, _path: String, _mode: Mode) -> Result<(), EngineError> {
        todo!()
    }

    fn write_file(&self, path: String, data: &[u8], _offset: i64) -> Result<usize, EngineError> {
        let allocator_vec = self.allocator.allocator_space(data.len() as u64);
        if allocator_vec.is_empty() {
            return Err(EngineError::Space);
        }
        self.index
            .update_index(path.as_str(), allocator_vec.clone());
        let iter = allocator_vec.iter();
        let mut result_size = 0;
        for alloc in iter {
            let size = self.storage.write(data, alloc.begin as i64);
            match size {
                Ok(size) => result_size += size,
                Err(_) => return Err(EngineError::IO),
            };
        }
        Ok(result_size)
    }

    fn create_file(&self, _path: String, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        todo!()
    }

    fn delete_file(&self, _path: String) -> Result<(), EngineError> {
        todo!()
    }

    fn truncate_file(&self, _path: String, _length: i64) -> Result<(), EngineError> {
        todo!()
    }
}

#[cfg(feature = "block_test")]
#[cfg(test)]
mod tests {
    use crate::server::storage_engine::StorageEngine;

    use super::BlockEngine;
    use super::MetaEngine;
    use std::{process::Command, sync::Arc};
    #[test]
    fn write_and_read_test() {
        Command::new("bash")
            .arg("-c")
            .arg("dd if=/dev/zero of=node1 bs=4M count=1")
            .output()
            .unwrap();
        Command::new("bash")
            .arg("-c")
            .arg("losetup /dev/loop8 node1")
            .output()
            .unwrap();
        let engine = BlockEngine::new("/dev/loop8", Arc::new(MetaEngine::new("")));
        let write_size = engine
            .write_file("test".to_string(), &b"some bytes"[..], 0)
            .unwrap();
        assert_eq!(write_size, 10);
        let read = engine.read_file("test".to_string(), 10, 0).unwrap();
        assert_eq!(read, &b"some bytes"[..]);
        Command::new("bash")
            .arg("-c")
            .arg("losetup -d /dev/loop8")
            .output()
            .unwrap();
        Command::new("bash")
            .arg("-c")
            .arg("rm node1")
            .output()
            .unwrap();
    }
}
