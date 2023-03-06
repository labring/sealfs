// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::server::storage_engine::StorageEngine;
use crate::server::EngineError;
use nix::sys::stat::Mode;

use super::allocator::{Allocator, BitmapAllocator, CHUNK};
//use super::allocator::SkipListAllocator;
use super::index::FileIndex;
use super::io::Storage;

#[allow(unused)]
pub struct BlockEngine {
    allocator: BitmapAllocator,
    index: FileIndex,
    storage: Storage,
}

impl StorageEngine for BlockEngine {
    fn new(_db_path: &str, root: &str) -> Self {
        let index = FileIndex::new();
        let storage = Storage::new(root);
        let allocator = BitmapAllocator::new(root);
        Self {
            allocator,
            index,
            storage,
        }
    }

    fn init(&self) {}

    fn get_file_attributes(&self, _path: String) -> Result<Vec<u8>, EngineError> {
        todo!()
    }

    fn read_directory(&self, _path: String) -> Result<Vec<u8>, EngineError> {
        todo!()
    }

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        let index_vec = self.index.search(path.as_str());
        let real_offset_index = offset as u64 / CHUNK;
        let real_offset = index_vec.get(real_offset_index as usize);
        match real_offset {
            Some(real_offset) => self.storage.read(size, *real_offset as i64),
            None => Err(EngineError::IO),
        }
    }

    fn open_file(&self, _path: String, _mode: Mode) -> Result<(), EngineError> {
        todo!()
    }

    fn write_file(&self, path: String, data: &[u8], _offset: i64) -> Result<usize, EngineError> {
        let pos = self.allocator.allocator_space(data.len() as u64);
        let index_value_vec = self.index.search(path.as_str());
        let mut vec = Vec::new();
        let mut length = (data.len() as u64) / CHUNK;
        if data.len() as u64 - length * CHUNK > 0 {
            length += 1;
        }
        for n in 0..length {
            vec.push(pos + n * CHUNK);
        }
        self.index.update_index(path.as_str(), vec);
        match index_value_vec.last() {
            Some(last) => self.storage.write(data, (last + pos) as i64),
            None => self.storage.write(data, pos as i64),
        }
    }

    fn delete_directory_recursive(&self, _path: String) -> Result<(), EngineError> {
        todo!()
    }

    fn is_exist(&self, _path: String) -> Result<bool, EngineError> {
        todo!()
    }

    fn directory_add_entry(
        &self,
        _parent_dir: String,
        _file_name: String,
        _file_type: u8,
    ) -> Result<(), EngineError> {
        todo!()
    }

    fn directory_delete_entry(
        &self,
        _parent_dir: String,
        _file_name: String,
        _file_type: u8,
    ) -> Result<(), EngineError> {
        todo!()
    }

    fn create_file(&self, _path: String, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        todo!()
    }

    fn delete_file(&self, _path: String) -> Result<(), EngineError> {
        todo!()
    }

    fn create_directory(&self, _path: String, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        todo!()
    }

    fn delete_directory(&self, _path: String) -> Result<(), EngineError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    // use crate::server::storage_engine::StorageEngine;

    // use super::BlockEngine;

    // #[test]
    // fn write_and_read_test() {
    //     let engine = BlockEngine::new("", "/dev/sda14");
    //          let write_size = engine
    //         .write_file("test".to_string(), &b"some bytes"[..], 0)
    //         .unwrap();
    //     assert_eq!(write_size, 10);
    //     let read = engine.read_file("test".to_string(), 10, 0).unwrap();
    //     assert_eq!(read, &b"some bytes"[..]);
    // }
}
