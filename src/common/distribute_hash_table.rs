// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

//todo configurable
const VIRTUAL_NODE_SIZE: i32 = 10;

lazy_static! {
    static ref INSTANCES: Arc<RwLock<BTreeMap<u64, String>>> =
        Arc::new(RwLock::new(BTreeMap::new()));
}

pub fn hash(path: &str) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let hash_builder = RandomState::with_seed(now as usize);
    hash_builder.hash_one(path)
}

pub fn index_selector(hash: u64) -> String {
    let instance_map = INSTANCES.read();
    if !instance_map.contains_key(&hash) {
        let entry_vec = Vec::from_iter(instance_map.iter());
        let vec = Vec::from_iter(instance_map.keys());
        let index = binary_search(vec, hash);
        let (_, value) = entry_vec[index as usize];
        return value.to_string();
    };
    instance_map.get(&hash).unwrap().to_string()
}

pub fn build_hash_ring(metadata: Vec<String>) {
    let mut instance_map = INSTANCES.write();
    metadata.iter().for_each(|ip| {
        for i in 0..VIRTUAL_NODE_SIZE {
            let name = format!("{}{}", ip, i);
            (*instance_map).insert(hash(name.as_str()), ip.to_string());
        }
    })
}

fn binary_search(hash_vec: Vec<&u64>, file_hash: u64) -> i32 {
    let mut begin = 0;
    let mut end = hash_vec.len() as i32 - 1;
    while begin <= end {
        let mid = (begin + end) >> 1;
        let val = hash_vec[mid as usize];
        match val {
            val if val < &file_hash => begin += 1,
            val if val > &file_hash => end -= 1,
            _ => return mid,
        };
    }
    if begin > (hash_vec.len() - 1) as i32 {
        return 0;
    }
    begin
}

#[cfg(test)]
mod tests {
    use crate::common::distribute_hash_table::binary_search;
    use std::collections::BTreeMap;

    #[test]
    fn index_selector_test() {
        let mut map: BTreeMap<u64, String> = BTreeMap::new();
        map.insert(3, "a".to_string());
        map.insert(10, "b".to_string());
        map.insert(8, "c".to_string());
        if !map.contains_key(&5) {
            let entry_vec = Vec::from_iter(map.iter());
            let vec = Vec::from_iter(map.keys());
            let index = binary_search(vec, 5);
            println!("index:{:?}", index);
            let (_, value) = entry_vec[index as usize];
            println!("result:{:?}", value);
            return;
        };
        let result = map.get(&7).unwrap();
        println!("result:{:?}", result);
    }

    #[test]
    fn binary_search_test() {
        let mut map = BTreeMap::new();
        map.insert(3, "a");
        map.insert(10, "b");
        map.insert(8, "c");
        let vec = Vec::from_iter(map.keys());
        println!("result:{}", (binary_search(vec, 2)));
    }
}
