// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use libc::ioctl;
use nix::fcntl::{open, OFlag};
use parking_lot::Mutex;

use crate::server::EngineError;

//#define BLKGETSIZE _IO(0x12,96)	/* return device size /512 (long *arg) */
const BLOCKGETSIZE: u64 = 0x1260;

pub const CHUNK: u64 = 512 * 8;
const SECTOR: u64 = 512;

pub(crate) trait Allocator {
    fn new(path: &str) -> Self;
    fn allocator_space(&self, lenth: u64) -> u64;
}

/*
 * This Allocator use for memory.
 */
#[allow(unused)]
pub(crate) struct BitmapAllocator {
    block_space: Arc<Mutex<u64>>,
    total_aspce: u64,
}

impl Allocator for BitmapAllocator {
    fn new(path: &str) -> Self {
        let blockdevice = BlockDevice::new(path).unwrap();
        Self {
            block_space: Arc::new(Mutex::new(0)),
            total_aspce: blockdevice.chunk_num,
        }
    }

    fn allocator_space(&self, lenth: u64) -> u64 {
        // todo reduce allocatorc size.
        // todo exent space manager.
        let mut chunk_size = lenth / CHUNK;
        if lenth - chunk_size * CHUNK > 0 {
            chunk_size += 1;
        }
        let mut mutex = self.block_space.lock();
        let begin_allocator_pos = *mutex;
        *mutex += chunk_size;
        begin_allocator_pos
    }
}

// Block device info.
struct BlockDevice {
    chunk_num: u64,
}

impl BlockDevice {
    fn new(path: &str) -> Result<BlockDevice, EngineError> {
        let block_num = Self::get_block_info(path)?;
        let chunk_num = block_num / (CHUNK / SECTOR);
        Ok(BlockDevice { chunk_num })
    }

    fn get_block_info(path: &str) -> Result<u64, EngineError> {
        let fd = open(path, OFlag::O_DIRECT, nix::sys::stat::Mode::S_IRWXU);
        if fd? < 0 {
            return Err(EngineError::Exist);
        }
        let block_num = 0;
        unsafe {
            let result = ioctl(fd?, BLOCKGETSIZE, &block_num);
            if result < 0 {
                return Err(EngineError::BlockInfo);
            }
        }
        Ok(block_num)
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::{Allocator, BitmapAllocator, BlockDevice};

    #[test]
    fn block_info_test() {
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
        let block_num = BlockDevice::get_block_info("/dev/loop8");
        assert_eq!(8192, block_num.unwrap());
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

    #[test]
    fn allocator_test() {
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
        let allocator = BitmapAllocator::new("/dev/loop8");
        let length = allocator.allocator_space(512 * 8 * 8);
        assert_eq!(length + 8, 8);
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
