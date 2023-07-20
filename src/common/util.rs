// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time::SystemTime;

use fuser::{FileAttr, FileType};

pub fn get_full_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        return format!("/{}", name);
    }
    let path = format!("{}/{}", parent, name);
    path
}

pub fn empty_file() -> FileAttr {
    FileAttr {
        ino: 0,
        size: 0,
        blocks: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::now(),
        ctime: SystemTime::now(),
        crtime: SystemTime::now(),
        kind: FileType::RegularFile,
        perm: 0,
        nlink: 0,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

pub fn empty_dir() -> FileAttr {
    FileAttr {
        ino: 0,
        size: 4096,
        blocks: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::now(),
        ctime: SystemTime::now(),
        crtime: SystemTime::now(),
        kind: FileType::Directory,
        perm: 0,
        nlink: 0,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}
