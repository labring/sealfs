use fuser::FileAttr;

pub struct Node {
    pub nodeid: u64,
    pub generation: u64,
    pub attr: FileAttr,
    pub parent: u64,
    pub name: String,
    pub path: String,
}

impl Node {
    pub fn new(parent: u64, path: String, name: String, attr: FileAttr) -> Self {
        Self {
            nodeid: attr.ino,
            generation: 0,
            attr,
            parent,
            name,
            path,
        }
    }
}
