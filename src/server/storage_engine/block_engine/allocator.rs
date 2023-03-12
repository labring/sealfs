// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use libc::ioctl;
use nix::fcntl::{open, OFlag};
use std::collections::HashSet;

use crossbeam_queue::ArrayQueue;

use super::AllocatorEntry;
use crate::server::EngineError;
extern crate num_cpus;

//#define BLKGETSIZE _IO(0x12,96)   /* return device size /512 (long *arg) */
const BLOCKGETSIZE: u64 = 0x1260;

pub const CHUNK: u64 = 512 * 8;
const SECTOR: u64 = 512;

pub(crate) trait Allocator {
    fn new(path: &str) -> Self;
    fn allocator_space(&self, lenth: u64) -> Vec<AllocatorEntry>;
    fn release_space(&self, lenth: u64, begin: u64) -> bool;
}

/*
 *This Allocator use for memory.
 */
#[allow(unused)]
pub(crate) struct BlockAllocator {
    block_group_list: ArrayQueue<BlockGroup>,
}

struct BlockGroup {
    block_space_point: u64,
    block_space: u64,
    slice_space_manager: Vec<HashSet<u64>>,
    slice_space: u64,
}

impl Allocator for BlockAllocator {
    fn new(path: &str) -> Self {
        let block_device = BlockDevice::new(path).unwrap();
        let num_cpus = num_cpus::get();
        let block_group_list = ArrayQueue::new(num_cpus);
        for index in 1..num_cpus + 1 {
            let block_allocator =
                BlockGroup::new(block_device.chunk_num / num_cpus as u64, index as u64);
            match block_group_list.push(block_allocator) {
                Ok(_) => {}
                Err(_) => {
                    todo!()
                }
            };
        }
        Self { block_group_list }
    }

    fn allocator_space(&self, lenth: u64) -> Vec<AllocatorEntry> {
        let mut block_group_count = 0;
        loop {
            let block_group = self.block_group_list.pop();
            match block_group {
                Some(mut block_group) => {
                    let result_vec = block_group.allocator_space(lenth);
                    match self.block_group_list.push(block_group) {
                        Ok(_) => {}
                        Err(_) => {
                            todo!()
                        }
                    };
                    if block_group_count >= num_cpus::get() {
                        return Vec::new();
                    }
                    if result_vec.is_empty() {
                        block_group_count += 1;
                        continue;
                    }
                    return result_vec;
                }
                None => {
                    continue;
                }
            }
        }
    }

    fn release_space(&self, _lenth: u64, _begin: u64) -> bool {
        // todo
        true
    }
}

impl BlockGroup {
    fn new(size: u64, block_group_index: u64) -> Self {
        let mut slice_space_vec = Vec::new();
        for _ in 0..11 {
            slice_space_vec.push(HashSet::new());
        }
        Self {
            block_space_point: (block_group_index - 1) * size,
            block_space: size,
            slice_space_manager: slice_space_vec,
            slice_space: 0,
        }
    }

    fn allocator_space(&mut self, lenth: u64) -> Vec<AllocatorEntry> {
        let mut chunk_size = lenth / CHUNK;
        if lenth - chunk_size * CHUNK > 0 {
            chunk_size += 1;
        }
        let mut result_vec = Vec::new();

        if lenth < (self.block_space - self.block_space_point) * CHUNK {
            let begin_allocator_pos = self.block_space_point;
            self.block_space_point += chunk_size;
            let alloc = AllocatorEntry {
                begin: begin_allocator_pos,
                length: chunk_size,
            };
            result_vec.push(alloc);
        } else {
            // Block space is full.
            if self.slice_space < chunk_size {
                return Vec::new();
            }
            let slice_space_vec = &mut self.slice_space_manager;
            let mut borrow_size = 0;
            let mut if_need_borrow_from_smaller = false;
            loop {
                match chunk_size {
                    size if size < 10 => {
                        if slice_space_vec[chunk_size as usize].is_empty() {
                            // There is no space of this chunk,
                            // need to borrow from bigger slice or smaller slice.
                            match borrow_size {
                                0 => {
                                    borrow_size = chunk_size;
                                    chunk_size += 1;
                                }
                                _ => {
                                    if if_need_borrow_from_smaller {
                                        chunk_size -= 1;
                                    } else {
                                        chunk_size += 1;
                                    }
                                }
                            }
                        } else {
                            let mut iter = slice_space_vec[chunk_size as usize].iter();
                            let mut begin = 0;
                            if let Some(slice) = iter.next(){
                                begin = *slice;
                            };
                            if borrow_size != 0 {
                                if chunk_size - borrow_size > 0 {
                                    let alloc = AllocatorEntry {
                                        begin,
                                        length: borrow_size,
                                    };
                                    result_vec.push(alloc);
                                    slice_space_vec[chunk_size as usize].insert(begin);
                                    let mut index = (chunk_size - borrow_size) as usize;
                                    loop {
                                        if slice_space_vec[index].contains(&(begin - index as u64))
                                        {
                                            slice_space_vec[index].remove(&(begin - index as u64));
                                            index *= 2;
                                        } else {
                                            slice_space_vec[index].insert(begin);
                                            break;
                                        }
                                    }
                                } else {
                                    loop {
                                        if slice_space_vec[chunk_size as usize].is_empty() {
                                            if chunk_size == 1 {
                                                return Vec::new();
                                            }
                                            break;
                                        } else {
                                            let mut iter = slice_space_vec[chunk_size as usize].iter();
                                            let mut begin = 0;
                                            if let Some(slice) = iter.next(){
                                                begin = *slice;
                                            };
                                            let alloc = AllocatorEntry {
                                                begin,
                                                length: chunk_size,
                                            };
                                            result_vec.push(alloc);
                                            slice_space_vec[chunk_size as usize].remove(&begin);
                                            if borrow_size < chunk_size {
                                                break;
                                            }
                                            borrow_size -= chunk_size;
                                        }
                                    }
                                }
                            } else {
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: chunk_size,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[chunk_size as usize].remove(&begin);
                                break;
                            }
                        }
                    }

                    size if size == 10 => {
                        if slice_space_vec[chunk_size as usize].is_empty() {
                            // Need to borrow from smaller slice.
                            if borrow_size == 0 {
                                borrow_size = chunk_size;
                                chunk_size -= 1;
                            } else {
                                chunk_size = borrow_size - 1;
                                if_need_borrow_from_smaller = true;
                            }
                        } else {
                            let mut iter = slice_space_vec[chunk_size as usize].iter();
                            let mut begin = 0;
                            if let Some(slice) = iter.next(){
                                begin = *slice;
                            }
                            if borrow_size != 0 {
                                if size >= borrow_size {
                                    self.release_space(borrow_size, begin);
                                    self.release_space(chunk_size - borrow_size, begin);
                                    let alloc = AllocatorEntry {
                                        begin,
                                        length: borrow_size,
                                    };
                                    result_vec.push(alloc);
                                }
                            } else {
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: chunk_size,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[chunk_size as usize].remove(&begin);
                            }
                            break;
                        }
                    }
                    _ => {
                        let mut size = chunk_size;
                        let mut allocator_size = 10;
                        loop {
                            if size - allocator_size <= 10 {
                                break;
                            }
                            if slice_space_vec[allocator_size as usize].is_empty() {
                                allocator_size -= 1;
                            } else {
                                let mut iter = slice_space_vec[allocator_size as usize].iter();
                                let mut begin = 0;
                                if let Some(slice) = iter.next() {
                                    begin = *slice;
                                }
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: allocator_size,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[allocator_size as usize].remove(&begin);
                                size -= allocator_size;
                            }
                        }
                    }
                }
            }
        }
        result_vec
    }

    fn release_space(&mut self, lenth: u64, begin: u64) -> bool {
        let mut chunk_size = lenth / CHUNK;
        if lenth - chunk_size * CHUNK > 0 {
            chunk_size += 1;
        }

        let slice_space_vec = &mut self.slice_space_manager;
        loop {
            match chunk_size {
                size if size <= 10 => {
                    if slice_space_vec[chunk_size as usize].contains(&(begin + chunk_size)) {
                        //Merge space to reduce allocator.
                        slice_space_vec[chunk_size as usize].remove(&(begin - chunk_size));
                        chunk_size += chunk_size;
                    } else {
                        slice_space_vec[chunk_size as usize].insert(begin);
                        self.slice_space += chunk_size;
                        break;
                    }
                }
                _ => {
                    let num = chunk_size / 10;
                    for index in 0..num {
                        slice_space_vec[10].insert(begin * (index - 1));
                        self.block_space += chunk_size;
                    }
                    if chunk_size - num * 10 == 0 {
                        break;
                    }
                }
            }
        }
        true
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

#[cfg(feature = "block_test")]
#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::{Allocator, BlockAllocator, BlockDevice};
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
    fn allocator_and_release_test() {
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
        let allocator = BlockAllocator::new("/dev/loop8");
        let vec = allocator.allocator_space(512 * 8 * 8);
        assert_eq!(1, vec.len());
        let re = allocator.release_space(512 * 8 * 8, 0);
        assert_eq!(re, true);
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
