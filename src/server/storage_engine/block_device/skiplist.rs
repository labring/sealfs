// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;

use super::allocator::AllocatorEntry;
use crossbeam::epoch::{self, Atomic, Owned};
use rand::Rng;

const MAX_LEVEL: i32 = 32;

struct Node {
    entry: Atomic<AllocatorEntry>,
    next: Atomic<Node>,
    down: Atomic<Node>,
    level: i32,
}

impl Node {
    fn new(level: i32, begin: u64, end: u64, next: Atomic<Node>, down: Atomic<Node>) -> Self {
        Node {
            entry: Atomic::new(AllocatorEntry { begin, end }),
            next,
            down,
            level,
        }
    }
}

pub(crate) struct SkipList {
    head_node: Atomic<Node>,
}

unsafe impl Sync for SkipList {}
unsafe impl Send for SkipList {}
impl SkipList {
    pub(crate) fn new() -> Self {
        let head_node = Node::new(1, 0, 0, Atomic::null(), Atomic::null());

        Self {
            head_node: Atomic::new(head_node),
        }
    }

    #[allow(unused)]
    fn insert(&self, begin: u64, end: u64) {
        // Index
        let mut index_level = 1;
        while rand::thread_rng().gen_range(0..2) >= 1 && index_level < MAX_LEVEL {
            index_level += 1;
        }

        let guard = epoch::pin();
        // Add index for node.
        let mut down_node: Atomic<Node> = Atomic::null();
        for level in 1..=index_level {
            unsafe {
                loop {
                    // If head level less than less,create new head index.
                    let head = &self.head_node;
                    // Level exist,which will not cause panic.
                    let head_level = head.load(Ordering::Acquire, &guard).as_ref().unwrap().level;
                    if head_level < level {
                        let head_node_snapshot = self.head_node.load(Ordering::Acquire, &guard);
                        let new_head = Owned::new(Node::new(
                            level,
                            0,
                            0,
                            Atomic::null(),
                            self.head_node.clone(),
                        ));
                        match self.head_node.compare_exchange(
                            head_node_snapshot,
                            new_head,
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        ) {
                            Ok(_) => break,
                            Err(_) => continue,
                        };
                    }
                    break;
                }
                loop {
                    let pre_node = self.find_pre_node(begin, level);
                    let snapshot = pre_node.load(Ordering::Acquire, &guard);

                    let entry = &(*snapshot.as_raw()).entry;
                    let node_begin = entry
                        .load(Ordering::Acquire, &guard)
                        .as_ref()
                        .unwrap()
                        .begin;
                    let node_end = entry.load(Ordering::Acquire, &guard).as_ref().unwrap().end;
                    let node = Atomic::new(Node::new(
                        level,
                        begin,
                        end,
                        (*snapshot.as_raw()).next.clone(),
                        down_node.clone(),
                    ));
                    let new_node = Owned::new(Node::new(
                        level,
                        node_begin,
                        node_end,
                        node.clone(),
                        (*snapshot.as_raw()).down.clone(),
                    ));
                    down_node = node;
                    match self
                        .find_pre_node(begin, level)
                        .compare_exchange(
                            snapshot,
                            new_node,
                            Ordering::Release,
                            Ordering::Relaxed,
                            &guard,
                        )
                        .ok()
                    {
                        Some(_) => {
                            break;
                        }
                        None => continue,
                    };
                }
            }
        }
    }

    #[allow(unused)]
    fn find_pre_node(&self, begin: u64, level: i32) -> &Atomic<Node> {
        unsafe {
            let guard = epoch::pin();
            let mut node = &self.head_node;
            loop {
                match &(*node.load(Ordering::Acquire, &guard).as_raw())
                    .next
                    .load(Ordering::Acquire, &guard)
                    .as_ref()
                {
                    Some(n) => {
                        let current_begin = n
                            .entry
                            .load(Ordering::Acquire, &guard)
                            .as_ref()
                            .unwrap()
                            .begin;
                        let current_level = n.level;
                        match begin {
                            begin if begin == current_begin => {
                                if current_level == level {
                                    return node;
                                }
                                node = &(*node.load(Ordering::Acquire, &guard).as_raw()).down;
                            }
                            begin if begin > current_begin => {
                                node = &(*node.load(Ordering::Acquire, &guard).as_raw()).next;
                            }
                            // if begin < current_begin
                            _ => {
                                if current_level == level {
                                    return node;
                                }
                                node = &(*node.load(Ordering::Acquire, &guard).as_raw()).down;
                            }
                        }
                    }
                    None => {
                        if (*node.load(Ordering::Acquire, &guard).as_raw()).level == level {
                            return node;
                        }
                        node = &(*node.load(Ordering::Acquire, &guard).as_raw()).down;
                    }
                }
            }
        }
    }

    #[allow(unused)]
    fn find_top_node(&self, begin: u64) -> &Atomic<Node> {
        unsafe {
            let guard = epoch::pin();
            let mut node = &self.head_node;
            loop {
                match &(*node.load(Ordering::Acquire, &guard).as_raw())
                    .next
                    .load(Ordering::Acquire, &guard)
                    .as_ref()
                {
                    Some(n) => {
                        let current_begin = n
                            .entry
                            .load(Ordering::Acquire, &guard)
                            .as_ref()
                            .unwrap()
                            .begin;
                        match begin {
                            begin if begin == current_begin => {
                                return &(*node.load(Ordering::Acquire, &guard).as_raw()).next
                            }
                            begin if begin > current_begin => {
                                node = &(*node.load(Ordering::Acquire, &guard).as_raw()).next
                            }
                            // begin < current_begin
                            _ => node = &(*node.load(Ordering::Acquire, &guard).as_raw()).down,
                        }
                    }
                    None => node = &(*node.load(Ordering::Acquire, &guard).as_raw()).down,
                }
            }
        }
    }

    #[allow(unused)]
    fn remove(&self, begin: u64) {
        unsafe {
            let guard = epoch::pin();
            loop {
                let node = self.find_top_node(begin);
                let node_snapshot = node.load(Ordering::Acquire, &guard);
                let level = (*node_snapshot.as_raw()).level;
                let next_snapshot = (*node_snapshot.as_raw())
                    .next
                    .load(Ordering::Acquire, &guard);
                match self.find_top_node(begin).compare_exchange(
                    node_snapshot,
                    next_snapshot,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                ) {
                    Ok(_) => {
                        guard.defer_destroy(node_snapshot);
                        if level == 1 {
                            break;
                        }
                    }
                    Err(_) => continue,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic;

    use crossbeam::epoch;

    use super::SkipList;

    #[test]
    // Test add and find pre index in singe thread.
    fn skiplist_add_find_pre_node_test() {
        let guard = epoch::pin();
        let skiplist = SkipList::new();
        for n in 1..=3 {
            skiplist.insert(n as u64, 0);
        }

        for n in 1..=3 {
            let node = skiplist.find_pre_node(n + 1, 1);
            unsafe {
                let entry = &(*node.load(atomic::Ordering::Acquire, &guard).as_raw()).entry;
                let begin = &(*entry.load(atomic::Ordering::Acquire, &guard).as_raw()).begin;
                assert_eq!(&n, begin);
            }
        }
    }

    #[test]
    // Test remove and find top node in single thread.
    fn skiplist_remove_test() {
        let guard = epoch::pin();
        let skiplist = SkipList::new();
        for n in 1..=3 {
            skiplist.insert(n as u64, 0);
        }

        let node = skiplist.find_pre_node(2, 1);
        unsafe {
            let entry = &(*node.load(atomic::Ordering::Acquire, &guard).as_raw()).entry;
            let begin = &(*entry.load(atomic::Ordering::Acquire, &guard).as_raw()).begin;
            assert_eq!(&1, begin);
        }

        skiplist.remove(1);
        let node = skiplist.find_pre_node(2, 1);
        unsafe {
            let entry = &(*node.load(atomic::Ordering::Acquire, &guard).as_raw()).entry;
            let begin = &(*entry.load(atomic::Ordering::Acquire, &guard).as_raw()).begin;
            assert_eq!(&0, begin);
        }
    }
}
