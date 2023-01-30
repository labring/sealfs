use std::{collections::HashMap, time::SystemTime};

use fuser::FileType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct FileAttrSimple {
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: u32,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
    pub blksize: u32,
}

impl Default for FileAttrSimple {
    fn default() -> Self {
        Self::new(fuser::FileType::RegularFile)
    }
}

impl FileAttrSimple {
    pub fn new(r#type: FileType) -> Self {
        let kind = match r#type {
            FileType::NamedPipe => 0,
            FileType::CharDevice => 1,
            FileType::BlockDevice => 2,
            FileType::Directory => 3,
            FileType::RegularFile => 4,
            FileType::Symlink => 5,
            FileType::Socket => 6,
        };
        FileAttrSimple {
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind,
            perm: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }
}

impl From<FileAttrSimple> for fuser::FileAttr {
    fn from(attr: FileAttrSimple) -> Self {
        let kind = match attr.kind {
            0 => FileType::NamedPipe,
            1 => FileType::CharDevice,
            2 => FileType::BlockDevice,
            3 => FileType::Directory,
            4 => FileType::RegularFile,
            5 => FileType::Symlink,
            6 => FileType::Socket,
            _ => FileType::RegularFile,
        };
        fuser::FileAttr {
            ino: 0,
            size: attr.size,
            blocks: attr.blocks,
            atime: attr.atime,
            mtime: attr.mtime,
            ctime: attr.ctime,
            crtime: attr.crtime,
            kind,
            perm: attr.perm,
            nlink: attr.nlink,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            flags: attr.flags,
            blksize: attr.blksize,
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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReadFileMetaData {
    pub offset: i64,
    pub size: u32,
}
