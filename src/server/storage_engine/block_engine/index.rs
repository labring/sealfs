// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use super::AllocatorEntry;
use dashmap::DashMap;

pub(crate) struct FileIndex {
    index: DashMap<String, Vec<AllocatorEntry>>,
}

impl FileIndex {
    pub(crate) fn new() -> Self {
        let index = DashMap::new();
        Self { index }
    }

    pub(crate) fn search(&self, file_name: &str) -> Vec<AllocatorEntry> {
        let value = self.index.get(file_name);
        match value {
            Some(entry) => entry.value().to_vec(),
            None => Vec::new(),
        }
    }

    pub(crate) fn update_index(&self, path: &str, mut vec: Vec<AllocatorEntry>) {
        let mut index_value_vec = self.search(path);
        index_value_vec.append(vec.as_mut());
        self.index.insert(path.to_string(), index_value_vec);
    }
}

#[cfg(test)]
mod tests {
    use super::AllocatorEntry;
    use super::FileIndex;

    #[test]
    fn search_and_update_index_test() {
        let index = FileIndex::new();
        let vec = index.search("test");
        assert_eq!(vec.is_empty(), true);
        let mut vec = Vec::new();
        let alloc = AllocatorEntry {
            begin: 0,
            length: 1,
        };
        vec.push(alloc);
        index.update_index("test", vec);
        let mut vec = index.search("test");
        let alloc = vec.pop().unwrap();
        assert_eq!(alloc.begin, 0);
        assert_eq!(alloc.length, 1);
    }
}
