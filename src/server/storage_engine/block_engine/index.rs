// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;

pub(crate) struct FileIndex {
    index: DashMap<String, Vec<u64>>,
}

impl FileIndex {
    pub(crate) fn new() -> Self {
        let index = DashMap::new();
        Self { index }
    }

    pub(crate) fn search(&self, file_name: &str) -> Vec<u64> {
        let value = self.index.get(file_name);
        match value {
            Some(entry) => entry.value().to_vec(),
            None => Vec::new(),
        }
    }

    pub(crate) fn update_index(&self, path: &str, mut vec: Vec<u64>) {
        let mut index_value_vec = self.search(path);
        index_value_vec.append(vec.as_mut());
        self.index.insert(path.to_string(), index_value_vec);
    }
}

#[derive(Clone, Copy)]
#[allow(unused)]
pub(crate) struct IndexEntry {
    chunk: u64,
    begin: u64,
    length: u64,
}

#[cfg(test)]
mod tests {
    use super::FileIndex;

    #[test]
    fn search_and_update_index_test() {
        let index = FileIndex::new();
        let vec = index.search("test");
        assert_eq!(vec.is_empty(), true);
        let mut vec = Vec::new();
        vec.push(1);
        index.update_index("test", vec);
        let mut vec = index.search("test");
        assert_eq!(vec.pop(), Some(1));
    }
}
