// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub mod allocator;
/**
*block device is use to bypass filesystem aimed to attain higher performance.
*/
pub mod index;
pub mod io;

use std::sync::Arc;

use crate::server::storage_engine::StorageEngine;

use allocator::{Allocator, BitmapAllocator, CHUNK};
use index::FileIndex;
use io::Storage;

use super::meta_engine::MetaEngine;

#[allow(unused)]
pub struct BlockEngine {
    allocator: BitmapAllocator,
    index: FileIndex,
    storage: Storage,
}

impl StorageEngine for BlockEngine {
    fn new(root: &str, _meta: Arc<MetaEngine>) -> Self {
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

    fn read_file(&self, path: &str, _size: u32, offset: i64) -> Result<Vec<u8>, i32> {
        let index_vec = self.index.search(path);
        let real_offset_index = offset as u64 / CHUNK;
        let real_offset = index_vec.get(real_offset_index as usize);
        match real_offset {
            Some(_real_offset) => todo!(), // self.storage.read(size, *real_offset as i64),
            None => todo!(),               // Err(libc::EIO),
        }
    }

    fn open_file(&self, _path: &str, _flag: i32, _mode: u32) -> Result<(), i32> {
        todo!()
    }

    fn write_file(&self, path: &str, data: &[u8], _offset: i64) -> Result<usize, i32> {
        let pos = self.allocator.allocator_space(data.len() as u64);
        let index_value_vec = self.index.search(path);
        let mut vec = Vec::new();
        let mut length = (data.len() as u64) / CHUNK;
        if data.len() as u64 - length * CHUNK > 0 {
            length += 1;
        }
        for n in 0..length {
            vec.push(pos + n * CHUNK);
        }
        self.index.update_index(path, vec);
        match index_value_vec.last() {
            Some(_last) => todo!(), // self.storage.write(data, (last + pos) as i64),
            None => todo!(),        // self.storage.write(data, pos as i64),
        }
    }

    fn create_file(
        &self,
        _path: &str,
        _oflag: i32,
        _umask: u32,
        _mode: u32,
    ) -> Result<Vec<u8>, i32> {
        todo!()
    }

    fn delete_file(&self, _path: &str) -> Result<(), i32> {
        todo!()
    }

    fn truncate_file(&self, _path: &str, _length: i64) -> Result<(), i32> {
        todo!()
    }
}

#[cfg(feature = "block_test")]
#[cfg(test)]
mod tests {
    use crate::server::storage_engine::StorageEngine;

    use super::BlockEngine;
    use std::process::Command;
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
        let engine = BlockEngine::new("", "/dev/loop8");
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
