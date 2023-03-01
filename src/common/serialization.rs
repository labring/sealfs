use std::{collections::HashMap, time::SystemTime};

use fuser::FileType;
use serde::{Deserialize, Serialize};

pub enum OperationType {
    Unkown = 0,
    Lookup = 1,
    CreateFile = 2,
    CreateDir = 3,
    GetFileAttr = 4,
    ReadDir = 5,
    OpenFile = 6,
    ReadFile = 7,
    WriteFile = 8,
    DeleteFile = 9,
    DeleteDir = 10,
    DirectoryAddEntry = 11,
    DirectoryDeleteEntry = 12,
    SendHeart = 13,
    GetMetadata = 14,
}

impl TryFrom<u32> for OperationType {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OperationType::Unkown),
            1 => Ok(OperationType::Lookup),
            2 => Ok(OperationType::CreateFile),
            3 => Ok(OperationType::CreateDir),
            4 => Ok(OperationType::GetFileAttr),
            5 => Ok(OperationType::ReadDir),
            6 => Ok(OperationType::OpenFile),
            7 => Ok(OperationType::ReadFile),
            8 => Ok(OperationType::WriteFile),
            9 => Ok(OperationType::DeleteFile),
            10 => Ok(OperationType::DeleteDir),
            11 => Ok(OperationType::DirectoryAddEntry),
            12 => Ok(OperationType::DirectoryDeleteEntry),
            13 => Ok(OperationType::SendHeart),
            14 => Ok(OperationType::GetMetadata),
            _ => panic!("Unkown value: {}", value),
        }
    }
}

impl From<OperationType> for u32 {
    fn from(value: OperationType) -> Self {
        match value {
            OperationType::Unkown => 0,
            OperationType::Lookup => 1,
            OperationType::CreateFile => 2,
            OperationType::CreateDir => 3,
            OperationType::GetFileAttr => 4,
            OperationType::ReadDir => 5,
            OperationType::OpenFile => 6,
            OperationType::ReadFile => 7,
            OperationType::WriteFile => 8,
            OperationType::DeleteFile => 9,
            OperationType::DeleteDir => 10,
            OperationType::DirectoryAddEntry => 11,
            OperationType::DirectoryDeleteEntry => 12,
            OperationType::SendHeart => 13,
            OperationType::GetMetadata => 14,
        }
    }
}

impl OperationType {
    pub fn to_le_bytes(&self) -> [u8; 4] {
        match self {
            OperationType::Unkown => 0u32.to_le_bytes(),
            OperationType::Lookup => 1u32.to_le_bytes(),
            OperationType::CreateFile => 2u32.to_le_bytes(),
            OperationType::CreateDir => 3u32.to_le_bytes(),
            OperationType::GetFileAttr => 4u32.to_le_bytes(),
            OperationType::ReadDir => 5u32.to_le_bytes(),
            OperationType::OpenFile => 6u32.to_le_bytes(),
            OperationType::ReadFile => 7u32.to_le_bytes(),
            OperationType::WriteFile => 8u32.to_le_bytes(),
            OperationType::DeleteFile => 9u32.to_le_bytes(),
            OperationType::DeleteDir => 10u32.to_le_bytes(),
            OperationType::DirectoryAddEntry => 11u32.to_le_bytes(),
            OperationType::DirectoryDeleteEntry => 12u32.to_le_bytes(),
            OperationType::SendHeart => 13u32.to_le_bytes(),
            OperationType::GetMetadata => 14u32.to_le_bytes(),
        }
    }
}

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
        let size = match r#type {
            FileType::Directory => 4096,
            _ => 0,
        };
        FileAttrSimple {
            size,
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
pub struct ReadFileSendMetaData {
    pub offset: i64,
    pub size: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct WriteFileSendMetaData {
    pub offset: i64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DirectoryEntrySendMetaData {
    pub file_type: u8,
}
