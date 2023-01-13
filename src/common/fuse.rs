use std::collections::HashMap;

use fuser::FileAttr;
use serde::{Deserialize, Serialize};

pub struct Node {
    pub nodeid: u64,
    pub generation: u64,
    pub attr: FileAttr,
    pub name: String,
    pub path: String,
}

impl Node {
    pub fn new(path: String, name: String, attr: FileAttr) -> Self {
        Self {
            nodeid: attr.ino,
            generation: 0,
            attr,
            name,
            path,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SubDirectory {
    pub sub_dir: HashMap<String, String>,
}

impl Default for SubDirectory {
    fn default() -> Self {
        Self::new()
    }
}

impl SubDirectory {
    pub fn new() -> Self {
        let sub_dir = HashMap::from([
            (".".to_string(), "d".to_string()),
            //("".to_string(), "d".to_string()),
            ("..".to_string(), "d".to_string()),
        ]);
        SubDirectory { sub_dir }
    }

    pub fn add_dir(&mut self, dir: String) {
        self.sub_dir.insert(dir, "d".to_string());
    }

    pub fn add_file(&mut self, file: String) {
        self.sub_dir.insert(file, "f".to_string());
    }

    pub fn delete_dir(&mut self, dir: String) {
        self.sub_dir.remove(&dir);
    }

    pub fn delete_file(&mut self, file: String) {
        self.sub_dir.remove(&file);
    }
}
