// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use libc::ioctl;
use nix::fcntl::{open, OFlag};
use std::collections::{HashMap, HashSet};

use crossbeam_queue::ArrayQueue;

use super::AllocatorEntry;
use crate::server::EngineError;
extern crate num_cpus;

//Copy from Linux.
//#define BLKGETSIZE _IO(0x12,96)   /* return device size /512 (long *arg) */
const BLOCKGETSIZE: u64 = 0x1260;

pub const CHUNK: u64 = 512 * 8;
const SECTOR: u64 = 512;
const BASE: i32 = 2;
const NOBORROW: u64 = 16;

pub(crate) trait Allocator {
    fn new(path: &str) -> Self;
    fn allocator_space(&self, lenth: u64) -> Vec<AllocatorEntry>;
    fn recycle_space(&self, entry_vec: Vec<AllocatorEntry>) -> bool;
}

/*
 *This Allocator use for memory.
 */
#[allow(unused)]
pub(crate) struct BlockAllocator {
    block_group_list: ArrayQueue<BlockGroup>,
    block_size: u64,
}

impl Allocator for BlockAllocator {
    fn new(path: &str) -> Self {
        let block_device = BlockDevice::new(path).unwrap();
        let num_cpus = num_cpus::get();
        let block_group_list = ArrayQueue::new(num_cpus);
        let block_size = block_device.chunk_num / num_cpus as u64;
        for index in 1..num_cpus + 1 {
            let block_allocator = BlockGroup::new(block_size, index as u64);
            match block_group_list.push(block_allocator) {
                Ok(_) => {}
                Err(_) => {
                    todo!()
                }
            };
        }
        Self {
            block_group_list,
            block_size,
        }
    }

    fn allocator_space(&self, len: u64) -> Vec<AllocatorEntry> {
        let mut block_group_count = 0;
        loop {
            let block_group = self.block_group_list.pop();
            match block_group {
                Some(mut block_group) => {
                    let result_vec = block_group.allocator_space(len);
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

    fn recycle_space(&self, recycle_list: Vec<AllocatorEntry>) -> bool {
        let mut map: HashMap<u64, Vec<AllocatorEntry>> = HashMap::new();
        for entry in recycle_list {
            let begin = entry.begin;
            let index = begin / self.block_size + 1;
            let key_exist = map.contains_key(&index);
            if !key_exist {
                map.insert(index, Vec::new());
            }
            let value = map.get_mut(&index).unwrap();
            value.push(entry);
        }
        loop {
            if map.is_empty() {
                break;
            }
            let block_group = self.block_group_list.pop();
            match block_group {
                Some(mut block_group) => {
                    if map.contains_key(&block_group.index) {
                        let vec = map.get(&block_group.index).unwrap();
                        for entry in vec {
                            block_group.recycle_space(entry.length, entry.begin);
                        }
                        map.remove(&block_group.index);
                        match self.block_group_list.push(block_group) {
                            Ok(_) => {}
                            Err(_) => {
                                todo!()
                            }
                        };
                    }
                }
                None => {
                    continue;
                }
            }
        }
        true
    }
}

struct BlockGroup {
    index: u64,
    block_space_point: u64,
    block_space: u64,
    slice_space_manager: Vec<HashSet<u64>>,
    slice_space: u64,
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
            slice_space_manager: slice_space_vec.clone(),
            slice_space: 0,
            index: block_group_index,
        }
    }

    fn allocator_space(&mut self, len: u64) -> Vec<AllocatorEntry> {
        let mut chunk_size = len / CHUNK;
        if len - chunk_size * CHUNK > 0 {
            chunk_size += 1;
        }
        let mut result_vec = Vec::new();

        if len < (self.block_space - self.block_space_point) * CHUNK {
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
            let chunk_size_vec = calulate_binary(len);
            for chunk_size in chunk_size_vec {
                let mut vec = self.allocator_slice_space(chunk_size);
                result_vec.append(&mut vec);
            }
        }
        result_vec
    }

    fn allocator_slice_space(&mut self, chunk_size: u64) -> Vec<AllocatorEntry> {
        let mut result_vec = Vec::new();
        let mut chunk_size_vec = vec![chunk_size];
        let slice_space_vec = &mut self.slice_space_manager;
        let mut borrow_size = NOBORROW;
        let mut if_need_borrow_from_smaller = false;
        loop {
            if chunk_size_vec.is_empty() {
                break;
            }
            let mut chunk_size = chunk_size_vec.pop().unwrap();
            loop {
                match chunk_size {
                    size if size < 10 => {
                        if slice_space_vec[chunk_size as usize].is_empty() {
                            // There is no space of this chunk,
                            // need to borrow from bigger slice or smaller slice.
                            match borrow_size {
                                NOBORROW => {
                                    borrow_size = chunk_size;
                                    chunk_size += 1;
                                }
                                _ => {
                                    if if_need_borrow_from_smaller {
                                        if chunk_size == 0 {
                                            return Vec::new();
                                        }
                                        chunk_size -= 1;
                                    } else {
                                        chunk_size += 1;
                                    }
                                }
                            }
                        } else {
                            let mut iter = slice_space_vec[chunk_size as usize].iter();
                            let mut begin = 0;
                            if let Some(slice) = iter.next() {
                                begin = *slice;
                            };
                            if borrow_size != NOBORROW {
                                if chunk_size > borrow_size {
                                    let alloc = AllocatorEntry {
                                        begin,
                                        length: BASE.pow(borrow_size as u32) as u64,
                                    };
                                    result_vec.push(alloc);
                                    slice_space_vec[chunk_size as usize].remove(&begin);
                                    self.slice_space -= BASE.pow(chunk_size as u32) as u64;
                                    let mut vec = calulate_binary(
                                        (BASE.pow(chunk_size as u32) - BASE.pow(borrow_size as u32))
                                            as u64,
                                    );
                                    loop {
                                        if vec.is_empty() {
                                            break;
                                        }
                                        let chunk_size = vec.pop().unwrap();
                                        if slice_space_vec[chunk_size as usize]
                                            .contains(&(begin + BASE.pow(size as u32) as u64))
                                        {
                                            slice_space_vec[chunk_size as usize]
                                                .remove(&(begin + BASE.pow(size as u32) as u64));
                                            slice_space_vec[chunk_size as usize + 1].insert(begin);
                                            self.slice_space += BASE.pow(size as u32 + 1) as u64;
                                            continue;
                                        }
                                        if begin >= BASE.pow(size as u32) as u64
                                            && slice_space_vec[chunk_size as usize]
                                                .contains(&(begin - BASE.pow(size as u32) as u64))
                                        {
                                            slice_space_vec[chunk_size as usize]
                                                .remove(&(begin - BASE.pow(size as u32) as u64));
                                            slice_space_vec[chunk_size as usize + 1].insert(begin);
                                            self.slice_space += BASE.pow(size as u32 + 1) as u64;
                                            continue;
                                        }
                                        slice_space_vec[chunk_size as usize].insert(begin);
                                        self.slice_space += BASE.pow(size as u32) as u64;
                                    }
                                } else {
                                    let alloc = AllocatorEntry {
                                        begin,
                                        length: BASE.pow(chunk_size as u32) as u64,
                                    };
                                    result_vec.push(alloc);
                                    slice_space_vec[chunk_size as usize].remove(&begin);
                                    let mut vec = calulate_binary(
                                        (BASE.pow(borrow_size as u32) - BASE.pow(chunk_size as u32))
                                            as u64,
                                    );
                                    chunk_size_vec.append(&mut vec);
                                }
                            } else {
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: BASE.pow(chunk_size as u32) as u64,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[chunk_size as usize].remove(&begin);
                                self.slice_space -= BASE.pow(chunk_size as u32) as u64;
                            }
                            break;
                        }
                    }

                    size if size == 10 => {
                        if slice_space_vec[chunk_size as usize].is_empty() {
                            // Need to borrow from smaller slice.
                            if borrow_size == NOBORROW {
                                borrow_size = chunk_size;
                                chunk_size -= 1;
                            } else {
                                chunk_size = borrow_size - 1;
                            }
                            if_need_borrow_from_smaller = true;
                        } else {
                            let mut iter = slice_space_vec[chunk_size as usize].iter();
                            let mut begin = 0;
                            if let Some(slice) = iter.next() {
                                begin = *slice;
                            }
                            if borrow_size != NOBORROW {
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: BASE.pow(borrow_size as u32) as u64,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[chunk_size as usize].remove(&begin);
                                self.slice_space -= BASE.pow(chunk_size as u32) as u64;
                                let mut vec = calulate_binary(
                                    (BASE.pow(chunk_size as u32) - BASE.pow(borrow_size as u32))
                                        as u64,
                                );
                                loop {
                                    if vec.is_empty() {
                                        break;
                                    }
                                    let chunk_size = vec.pop().unwrap();
                                    if slice_space_vec[chunk_size as usize]
                                        .contains(&(begin + BASE.pow(size as u32) as u64))
                                    {
                                        slice_space_vec[chunk_size as usize]
                                            .remove(&(begin + BASE.pow(size as u32) as u64));
                                        slice_space_vec[chunk_size as usize + 1].insert(begin);
                                        self.slice_space += BASE.pow(size as u32 + 1) as u64;
                                        continue;
                                    }
                                    if begin >= BASE.pow(size as u32 + 1) as u64
                                        && slice_space_vec[chunk_size as usize]
                                            .contains(&(begin - BASE.pow(size as u32) as u64))
                                    {
                                        slice_space_vec[chunk_size as usize]
                                            .remove(&(begin - BASE.pow(size as u32) as u64));
                                        slice_space_vec[chunk_size as usize + 1].insert(begin);
                                        self.slice_space += BASE.pow(size as u32 + 1) as u64;
                                        continue;
                                    }
                                    slice_space_vec[chunk_size as usize].insert(begin);
                                    self.slice_space += BASE.pow(size as u32) as u64;
                                }
                            } else {
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: BASE.pow(chunk_size as u32) as u64,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[chunk_size as usize].remove(&begin);
                                self.slice_space -= BASE.pow(chunk_size as u32) as u64;
                            }
                            break;
                        }
                    }
                    _ => {
                        let mut allocator_size = 10;
                        loop {
                            if slice_space_vec[allocator_size as usize].is_empty() {
                                if allocator_size == 0 {
                                    return Vec::new();
                                }
                                allocator_size -= 1;
                            } else {
                                let mut iter = slice_space_vec[allocator_size as usize].iter();
                                let mut begin = 0;
                                if let Some(slice) = iter.next() {
                                    begin = *slice;
                                }
                                let alloc = AllocatorEntry {
                                    begin,
                                    length: BASE.pow(allocator_size as u32) as u64,
                                };
                                result_vec.push(alloc);
                                slice_space_vec[allocator_size as usize].remove(&begin);
                                self.slice_space -= BASE.pow(allocator_size as u32) as u64;
                                let mut vec = calulate_binary(
                                    (BASE.pow(chunk_size as u32) - BASE.pow(allocator_size as u32))
                                        as u64,
                                );
                                chunk_size_vec.append(&mut vec);
                                break;
                            }
                        }
                        break;
                    }
                }
            }
        }
        result_vec
    }

    fn recycle_space(&mut self, len: u64, begin: u64) {
        let mut chunk_size_vec = calulate_binary(len);
        let slice_space_vec = &mut self.slice_space_manager;
        loop {
            if chunk_size_vec.is_empty() {
                break;
            }
            let chunk_size = chunk_size_vec.pop().unwrap();
            match chunk_size {
                size if size < 10 => {
                    if slice_space_vec[chunk_size as usize]
                        .contains(&(begin + BASE.pow(size as u32) as u64))
                    {
                        //Merge space to reduce allocator.
                        slice_space_vec[chunk_size as usize]
                            .remove(&(begin + BASE.pow(size as u32) as u64));
                        slice_space_vec[chunk_size as usize + 1].insert(begin);
                        self.slice_space += BASE.pow(size as u32 + 1) as u64;
                        continue;
                    }
                    if begin >= BASE.pow(size as u32) as u64
                        && slice_space_vec[chunk_size as usize]
                            .contains(&(begin - BASE.pow(size as u32) as u64))
                    {
                        slice_space_vec[chunk_size as usize]
                            .remove(&(begin - BASE.pow(size as u32) as u64));
                        slice_space_vec[chunk_size as usize + 1].insert(begin);
                        self.slice_space += BASE.pow(size as u32 + 1) as u64;
                        continue;
                    }
                    slice_space_vec[chunk_size as usize].insert(begin);
                    self.slice_space += BASE.pow(size as u32) as u64;
                }
                size if size == 10 => {
                    slice_space_vec[chunk_size as usize].insert(begin);
                    self.slice_space += BASE.pow(size as u32) as u64;
                }
                _ => {
                    let num = chunk_size / 10;
                    for index in 1..BASE.pow(num as u32) as u64 + 1 {
                        slice_space_vec[10].insert(begin + (index - 1) * BASE.pow(10) as u64);
                        self.slice_space += BASE.pow(10) as u64;
                    }
                }
            }
        }
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

fn calulate_binary(mut len: u64) -> Vec<u64> {
    let mut binary_vec = Vec::new();
    let mut count = 0;
    loop {
        let remainder = len % 2;
        if remainder == 1 {
            binary_vec.push(count);
        }
        count += 1;
        len /= 2;
        if len == 0 {
            break;
        }
    }
    binary_vec
}

#[cfg(feature = "block_test")]
#[cfg(test)]
mod tests {
    use std::process::Command;

    use crate::server::storage_engine::block_engine::AllocatorEntry;

    use super::{calulate_binary, Allocator, BlockAllocator, BlockDevice, BlockGroup};
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
        let alloc = AllocatorEntry {
            begin: 0,
            length: 512 * 8 * 8,
        };
        let mut vec = Vec::new();
        vec.push(alloc);
        let re = allocator.recycle_space(vec);
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

    #[test]
    fn allocator_and_recycle_slice_test() {
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
        let mut allocator = BlockGroup::new(1024, 1);
        allocator.recycle_space(1024, 0);
        allocator.recycle_space(1024, 1024);
        allocator.recycle_space(1024, 2048);
        let size = allocator.slice_space;
        assert_eq!(size, 3072);
        let vec = allocator.allocator_slice_space(6);
        for alloc in vec {
            assert_eq!(alloc.length, 64);
        }
        let vec = allocator.allocator_slice_space(11);
        let alloc = vec.get(0).unwrap();
        assert_eq!(alloc.length, 1024);
        let alloc = vec.get(1).unwrap();
        assert_eq!(alloc.length, 1024);
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
    fn calulate_binary_test() {
        let vec = calulate_binary(8);
        for binary in vec {
            assert_eq!(binary, 3);
        }
    }
}
