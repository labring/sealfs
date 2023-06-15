// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{marker::PhantomData, mem, ptr::NonNull};

#[derive(Copy, Clone)]
struct NodePointer<T>(Option<NonNull<Node<T>>>);

unsafe impl<T> Send for NodePointer<T> {}
unsafe impl<T> Sync for NodePointer<T> {}

impl<T> Default for NodePointer<T> {
    fn default() -> Self {
        NodePointer(None)
    }
}

pub struct Node<T> {
    val: T,
    next: Mutex<NodePointer<T>>,
    prev: Mutex<NodePointer<T>>,
}

impl<T> Node<T> {
    fn new(val: T) -> Self {
        Self {
            val,
            next: Mutex::new(NodePointer::default()),
            prev: Mutex::new(NodePointer::default()),
        }
    }

    fn into_val(self) -> T {
        self.val
    }
}

pub struct LinkedList<T>
where
    T: std::fmt::Debug,
{
    length: AtomicUsize,
    head: Mutex<NodePointer<T>>,
    tail: Mutex<NodePointer<T>>,
    _marker: PhantomData<Box<Node<T>>>,
}

impl<T> Default for LinkedList<T>
where
    T: std::fmt::Debug,
{
    fn default() -> Self {
        Self {
            length: 0.into(),
            head: Mutex::new(NodePointer::default()),
            tail: Mutex::new(NodePointer::default()),
            _marker: PhantomData,
        }
    }
}

impl<T> LinkedList<T>
where
    T: std::fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            length: 0.into(),
            head: Mutex::new(NodePointer::default()),
            tail: Mutex::new(NodePointer::default()),
            _marker: PhantomData,
        }
    }

    pub fn insert_front(&self, val: T) {
        let node = Box::new(Node::new(val));
        let node = NonNull::new(Box::into_raw(node)).unwrap();
        self.insert_front_raw(node);
    }

    pub fn insert_front_raw(&self, mut node: NonNull<Node<T>>) {
        let mut head_locked = self.head.lock();
        unsafe {
            node.as_mut().next.lock().0 = head_locked.0;
            node.as_mut().prev = Mutex::new(NodePointer::default());
        }

        match head_locked.0 {
            Some(head) => unsafe {
                (*head.as_ptr()).prev.lock().0 = Some(node);
            },
            None => {
                self.tail.lock().0 = Some(node);
            }
        }
        head_locked.0 = Some(node);
        self.length.fetch_add(1, Ordering::Relaxed);
    }

    pub fn remove(&self, mut node: NonNull<Node<T>>) -> T {
        let node_mut = unsafe { node.as_mut() };
        self.length.fetch_sub(1, Ordering::Relaxed);
        match node_mut.prev.lock().0 {
            Some(prev) => unsafe { (*prev.as_ptr()).next.lock().0 = node_mut.next.lock().0 },
            None => {
                self.head.lock().0 = node_mut.next.lock().0;
            }
        }
        match node_mut.next.lock().0 {
            Some(next) => unsafe { (*next.as_ptr()).prev.lock().0 = node_mut.prev.lock().0 },
            None => self.tail.lock().0 = node_mut.prev.lock().0,
        }
        unsafe {
            let n = Box::from_raw(node.as_ptr());
            n.into_val()
        }
    }

    pub fn reinsert_front(&self, mut node: NonNull<Node<T>>) {
        {
            let head_locked = self.head.lock();
            if head_locked.0 == Some(node) {
                return;
            }
        }
        let node_mut = unsafe { node.as_mut() };
        self.length.fetch_sub(1, Ordering::Relaxed);
        match node_mut.prev.lock().0 {
            Some(prev) => unsafe { (*prev.as_ptr()).next.lock().0 = node_mut.next.lock().0 },
            None => {
                self.head.lock().0 = node_mut.next.lock().0;
            }
        }
        match node_mut.next.lock().0 {
            Some(next) => unsafe { (*next.as_ptr()).prev.lock().0 = node_mut.prev.lock().0 },
            None => self.tail.lock().0 = node_mut.prev.lock().0,
        }
        self.insert_front_raw(node);
    }

    pub fn remove_tail(&self) -> Option<T> {
        let mut tail_locked = self.tail.lock();
        self.length.fetch_sub(1, Ordering::Relaxed);
        match tail_locked.0 {
            Some(tail) => unsafe {
                let node = Box::from_raw(tail.as_ptr());
                {
                    let prev_node_locked = node.prev.lock();
                    tail_locked.0 = prev_node_locked.0;
                    match tail_locked.0 {
                        Some(t) => {
                            (*t.as_ptr()).next.lock().0 = None;
                        }
                        None => {
                            self.head.lock().0 = None;
                        }
                    }
                }
                Some(node.into_val())
            },
            None => {
                let mut head_locked = self.head.lock();
                head_locked.0 = None;
                None
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            head: NodePointer(self.head.lock().0),
            len: self.length.load(Ordering::Relaxed),
            _marker: PhantomData,
        }
    }
}

impl<T> Drop for LinkedList<T>
where
    T: std::fmt::Debug,
{
    fn drop(&mut self) {
        struct DropGuard<'a, T>(&'a mut LinkedList<T>)
        where
            T: std::fmt::Debug;
        impl<'a, T> Drop for DropGuard<'a, T>
        where
            T: std::fmt::Debug,
        {
            fn drop(&mut self) {
                while self.0.remove_tail().is_some() {}
            }
        }

        while let Some(node) = self.remove_tail() {
            let guard = DropGuard(self);
            drop(node);
            mem::forget(guard);
        }
    }
}

pub struct Iter<'a, T: 'a> {
    head: NodePointer<T>,
    len: usize,
    _marker: PhantomData<&'a Node<T>>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            None
        } else {
            self.head.0.map(|node| {
                self.len -= 1;

                unsafe {
                    let node = &*node.as_ptr();
                    self.head = NodePointer(node.next.lock().0);
                    &node.val
                }
            })
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for LinkedList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for cur in self.iter() {
            write!(f, "{:?} ", cur)?;
        }
        Ok(())
    }
}

struct LRUEntry<T: std::fmt::Debug> {
    key: Vec<u8>,
    value: T,
}

impl<T> LRUEntry<T>
where
    T: std::fmt::Debug,
{
    pub fn new(key: &[u8], value: T) -> Self {
        Self {
            key: key.to_vec(),
            value,
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for LRUEntry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.value)?;
        Ok(())
    }
}

pub struct LRUCache<T>
where
    T: std::fmt::Debug,
{
    map: DashMap<Vec<u8>, NodePointer<LRUEntry<T>>>,
    list: LinkedList<LRUEntry<T>>,
    capacity: usize,
    lock: Mutex<()>,
}

impl<T> LRUCache<T>
where
    T: std::fmt::Debug,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: DashMap::new(),
            list: LinkedList::new(),
            capacity,
            lock: Mutex::new(()),
        }
    }

    pub fn insert(&self, key: &[u8], value: T) -> Option<T> {
        let _l = self.lock.lock();
        let new_node = LRUEntry::new(key, value);
        let new_node = Box::new(Node::new(new_node));
        let new_node = NonNull::new(Box::into_raw(new_node)).unwrap();

        let mut val = None;
        match self.map.get(key) {
            Some(entry) => {
                let entry = entry.0.unwrap();
                let value = self.list.remove(entry);
                val = Some(value.value);
                self.list.insert_front_raw(new_node);
            }
            None => {
                if self.list.length.load(Ordering::Relaxed) >= self.capacity {
                    // let removed_key = self.list.remove_tail();
                    if let Some(entry) = self.list.remove_tail() {
                        self.map.remove(&entry.key);
                        val = Some(entry.value);
                    }
                }
                self.list.insert_front_raw(new_node);
            }
        }
        self.map.insert(key.to_vec(), NodePointer(Some(new_node)));
        val
    }

    pub fn get(&self, key: &[u8]) -> Option<&T> {
        let _l = self.lock.lock();
        match self.map.get(key) {
            Some(node) => unsafe {
                let node = node.0.unwrap();
                let value = &node.as_ref().val.value;
                self.list.reinsert_front(node);
                Some(value)
            },
            None => None,
        }
    }

    pub fn remove(&self, key: &[u8]) {
        let _l = self.lock.lock();
        if let Some(node) = self.map.get(key) {
            self.list.remove(node.0.unwrap());
        }
        self.map.remove(key);
    }
}

#[cfg(test)]
mod test {
    mod test_linkedlist {
        use super::super::LinkedList;

        #[test]
        fn test_insert() {
            let list: LinkedList<i32> = LinkedList::new();
            list.insert_front(2);
            list.insert_front(3);
            list.insert_front(4);
            let result = format!("{:?}", list);
            assert_eq!("4 3 2 ", result);
        }
    }

    mod test_lru_cache {
        use std::sync::Arc;

        use super::super::LRUCache;
        use rand::prelude::*;

        #[test]
        fn test() {
            let lru = LRUCache::new(5);
            lru.insert(&5_i32.to_le_bytes(), 5);
            println!("{:?}", lru.list);
            lru.insert(&0_i32.to_le_bytes(), 0);
            println!("{:?}", lru.list);
            lru.insert(&2_i32.to_le_bytes(), 2);
            println!("{:?}", lru.list);
            lru.insert(&6_i32.to_le_bytes(), 6);
            println!("{:?}", lru.list);
            lru.insert(&1_i32.to_le_bytes(), 1);
            println!("{:?}", lru.list);
            lru.insert(&6_i32.to_le_bytes(), 6);
            println!("{:?}", lru.list);
            lru.insert(&8_i32.to_le_bytes(), 8);
            println!("{:?}", lru.list);
            lru.insert(&8_i32.to_le_bytes(), 8);
            println!("{:?}", lru.list);
            lru.insert(&7_i32.to_le_bytes(), 7);
            println!("{:?}", lru.list);
            lru.insert(&4_i32.to_le_bytes(), 4);
            println!("{:?}", lru.list);
            lru.insert(&0_i32.to_le_bytes(), 0);
            println!("{:?}", lru.list);
            lru.insert(&0_i32.to_le_bytes(), 0);
            lru.insert(&2_i32.to_le_bytes(), 2);
            lru.insert(&1_i32.to_le_bytes(), 1);
            lru.insert(&0_i32.to_le_bytes(), 0);
            lru.insert(&2_i32.to_le_bytes(), 2);
        }

        #[test]
        fn test_insert() {
            let lru = LRUCache::new(10);
            for _i in 0..50000_usize {
                let mut rng = rand::thread_rng();
                let n: usize = rng.gen::<usize>() % 100;
                print!("{n},");
                lru.insert(&n.to_le_bytes(), n);
            }
        }

        #[test]
        fn test_multithread() {
            let lru: Arc<LRUCache<usize>> = Arc::new(LRUCache::new(10));
            let mut thread_arr = Vec::new();
            for _i in 0..10usize {
                let lru_arc = Arc::clone(&lru);
                let handler = std::thread::spawn(move || {
                    for _i in 0..10000_usize {
                        let mut rng = rand::thread_rng();
                        let n = rng.gen::<usize>() % 100;
                        // println!("thread: {:?} {n}", std::thread::current().id());
                        lru_arc.insert(&n.to_le_bytes(), n);
                    }
                });
                thread_arr.push(handler);
            }
            for thread in thread_arr {
                thread.join().unwrap();
            }
        }
    }
}
