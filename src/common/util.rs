// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time::SystemTime;

use fuser::{FileAttr, FileType};
use log::error;

pub fn get_full_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        return format!("/{}", name);
    }
    let path = format!("{}/{}", parent, name);
    path
}

//  path_split: the path should not be empty, and it does not end with a slash unless it is the root directory.
pub fn path_split(path: &str) -> Result<(String, String), i32> {
    if path.is_empty() {
        error!("path is empty");
        return Err(libc::EINVAL);
    }
    if path == "/" {
        error!("path is root");
        return Err(libc::EINVAL);
    }
    if path.ends_with('/') {
        error!("path ends with /");
        return Err(libc::EINVAL);
    }
    let index = match path.rfind('/') {
        Some(value) => value,
        None => {
            error!("path does not contain /");
            return Err(libc::EINVAL);
        }
    };
    match index {
        0 => Ok(("/".into(), path[1..].into())),
        _ => Ok((path[..index].into(), path[(index + 1)..].into())),
    }
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
