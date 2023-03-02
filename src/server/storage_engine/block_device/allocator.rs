// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::AtomicU64;

use crate::server::storage_engine::block_device::skiplist::SkipList;
use thiserror::Error;

pub(crate) trait Allocator {
    fn new(size: u64) -> Self;
}

#[derive(Clone, Copy)]
pub(crate) struct AllocatorEntry {
    pub begin: u64,
    pub end: u64,
}

/*
 * This Allocator use for memory.
 */
#[allow(unused)]
pub(crate) struct SkipListAllocator {
    block_space: AtomicU64,
    exent_space: SkipList,
    total_space: u64,
}

impl Allocator for SkipListAllocator {
    fn new(size: u64) -> SkipListAllocator {
        // Block device space manager.
        let pos_space = AtomicU64::new(0);

        // Extent space manager.
        let skiplist = SkipList::new();
        Self {
            block_space: pos_space,
            exent_space: skiplist,
            total_space: size,
        }
    }
}

#[derive(Error, Debug)]
enum AllocatorError {
    #[error("There is no enough space.")]
    _Space,
}
