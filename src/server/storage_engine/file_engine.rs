// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::util::empty_file;
use crate::common::{cache::LRUCache, errors::status_to_string};

use super::meta_engine::MetaEngine;
use super::StorageEngine;
use log::{debug, error};
use nix::errno::errno;
use nix::{
    fcntl::OFlag,
    sys::stat::Mode,
    unistd::{self, mkdir},
};
use std::ffi::CString;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::Path,
    sync::Arc,
};

pub struct FileEngine {
    pub meta_engine: Arc<MetaEngine>,
    pub root: String,
    pub cache: LRUCache<FileDescriptor>,
}

#[derive(Debug, Clone)]
pub struct FileDescriptor {
    fd: i32,
}

impl FileDescriptor {
    pub(crate) fn new(fd: i32) -> Self {
        Self { fd }
    }
}

impl Drop for FileDescriptor {
    fn drop(&mut self) {
        unistd::close(self.fd).unwrap();
    }
}

impl StorageEngine for FileEngine {
    fn new(root: &str, meta_engine: Arc<MetaEngine>) -> Self {
        if !Path::new(root).exists() {
            let mode =
                Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH | Mode::S_IWOTH;
            mkdir(root, mode).unwrap();
        }

        Self {
            meta_engine,
            root: root.to_string(),
            cache: LRUCache::new(512),
        }
    }

    fn init(&self) {
        self.fsck().unwrap();
        self.meta_engine.init();
    }

    fn read_file(&self, path: &str, size: u32, offset: i64) -> Result<Vec<u8>, i32> {
        if self.meta_engine.is_dir(path)? {
            return Err(libc::EISDIR);
        }

        let local_file_name = generate_local_file_name(&self.root, path);
        let oflag = OFlag::O_RDWR;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = match self.cache.get(local_file_name.as_bytes()) {
            Some(value) => value.fd,
            None => {
                let fd = unsafe {
                    libc::open(
                        CString::new(local_file_name.clone())
                            .unwrap()
                            .as_c_str()
                            .as_ptr() as *const i8,
                        oflag.bits(),
                        mode.bits(),
                    )
                };
                if fd < 0 {
                    let f_errno = errno();
                    error!("read file error: {:?}", status_to_string(f_errno));
                    return Err(f_errno);
                }
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let mut data = vec![0; size as usize];
        let real_size = unsafe {
            libc::pread(
                fd,
                data.as_mut_slice().as_mut_ptr() as *mut libc::c_void,
                size as usize,
                offset,
            )
        };
        if real_size < 0 {
            let f_errno = errno();
            error!("read file error: {:?}", status_to_string(f_errno));
            return Err(f_errno);
        };
        debug!(
            "read_file path: {}, size: {}, offset: {}, data: {:?}",
            path, real_size, offset, data
        );

        // this is a temporary solution, which results in an extra memory copy.
        // TODO: optimize it by return the hole data vector and the real size both.
        Ok(data[..real_size as usize].to_vec())
    }

    fn write_file(&self, path: &str, data: &[u8], offset: i64) -> Result<usize, i32> {
        if self.meta_engine.is_dir(path)? {
            return Err(libc::EISDIR);
        }

        let local_file_name = generate_local_file_name(&self.root, path);
        let oflag = OFlag::O_RDWR;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = match self.cache.get(local_file_name.as_bytes()) {
            Some(value) => value.fd,
            None => {
                let fd = unsafe {
                    libc::open(
                        CString::new(local_file_name.clone())
                            .unwrap()
                            .as_c_str()
                            .as_ptr() as *const i8,
                        oflag.bits(),
                        mode.bits(),
                    )
                };
                if fd < 0 {
                    let f_errno = errno();
                    error!("read file error: {:?}", status_to_string(f_errno));
                    return Err(f_errno);
                }
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let write_size =
            unsafe { libc::pwrite(fd, data.as_ptr() as *const libc::c_void, data.len(), offset) };
        if write_size < 0 {
            let f_errno = errno();
            error!("write file error: {:?}", status_to_string(f_errno));
            return Err(f_errno);
        }

        debug!(
            "write_file path: {}, write_size: {}, data_len: {}",
            path,
            write_size,
            data.len()
        );

        self.meta_engine.update_size(path, offset as u64 + write_size as u64)?;

        Ok(write_size as usize)
    }

    fn create_file(&self, path: &str, _oflag: i32, _umask: u32, mode: u32) -> Result<Vec<u8>, i32> {
        let local_file_name = generate_local_file_name(&self.root, path);
        let oflag = OFlag::O_CREAT | OFlag::O_RDWR;
        match self.cache.get(local_file_name.as_bytes()) {
            Some(_) => {}
            None => {
                let fd = unsafe {
                    libc::open(
                        CString::new(local_file_name.clone())
                            .unwrap()
                            .as_c_str()
                            .as_ptr() as *const i8,
                        oflag.bits(),
                        mode,
                    )
                };
                if fd < 0 {
                    let f_errno = errno();
                    error!("read file error: {:?}", status_to_string(f_errno));
                    return Err(f_errno);
                }
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
            }
        };
        self.meta_engine.create_file(empty_file(), &local_file_name, path)
    }

    fn delete_file(&self, path: &str) -> Result<(), i32> {
        let local_file_name = generate_local_file_name(&self.root, path);
        self.cache.remove(local_file_name.as_bytes());
        let status = unsafe {
            libc::unlink(
                CString::new(local_file_name.clone())
                    .unwrap()
                    .as_c_str()
                    .as_ptr() as *const i8,
            )
        };
        if status < 0 {
            let f_errno = errno();
            error!("delete file error: {:?}", status_to_string(f_errno));
            return Err(f_errno);
        };
        self.meta_engine.delete_file(&local_file_name, path)?;
        Ok(())
    }

    fn truncate_file(&self, path: &str, length: i64) -> Result<(), i32> {
        let local_file_name = generate_local_file_name(&self.root, path);
        let status = unsafe {
            libc::truncate(
                CString::new(local_file_name).unwrap().as_c_str().as_ptr() as *const i8,
                length,
            )
        };
        if status < 0 {
            let f_errno = errno();
            error!("truncate file error: {:?}", status_to_string(f_errno));
            return Err(f_errno);
        };
        // TODO: update file attr
        Ok(())
    }

    fn open_file(&self, path: &str, _flags: i32, mode: u32) -> Result<(), i32> {
        let local_file_name = generate_local_file_name(&self.root, path);

        let oflag = OFlag::O_RDWR;
        let fd = unsafe {
            libc::open(
                CString::new(local_file_name.clone())
                    .unwrap()
                    .as_c_str()
                    .as_ptr() as *const i8,
                oflag.bits(),
                mode,
            )
        };
        if fd < 0 {
            let f_errno = errno();
            error!("read file error: {:?}", status_to_string(f_errno));
            return Err(f_errno);
        }
        self.cache
            .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
        Ok(())
    }
}

impl FileEngine {
    fn fsck(&self) -> Result<(), i32> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) => {
                error!("read dir error: {:?}", err);
                return Err(libc::EIO); // I'm not sure how to replace read_dir by libc, so I can't translate the error code
            }
        };
        for entry in entries {
            let entry = entry.map_err(|err| {
                error!("read dir error: {:?}", err);
                libc::EIO
            })?;
            let file_name = format!("{}/{}", self.root, entry.file_name().to_str().unwrap());
            if self.meta_engine.check_file(&file_name) {
                continue;
            }
            let _ = std::fs::remove_file(entry.path());
        }

        self.meta_engine.check_dir();

        Ok(())
    }
}

#[inline]
fn generate_local_file_name(root: &str, path: &str) -> String {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{}/{}", root, hasher.finish())
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use crate::server::storage_engine::meta_engine::MetaEngine;
    use fuser::FileType;
    use libc::mode_t;
    use nix::{
        fcntl::{self, OFlag},
        sys::stat::Mode,
    };

    use crate::server::storage_engine::{file_engine::generate_local_file_name, StorageEngine};

    use super::FileEngine;

    #[test]
    fn test_init() {
        let root = "/tmp/test_init";
        let db_path = "/tmp/test_db";
        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine);
            engine.init();

            let oflag = OFlag::O_CREAT | OFlag::O_EXCL;
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            let _fd = fcntl::open(format!("{}/test", root).as_str(), oflag, mode).unwrap();
            assert_eq!(Path::new(format!("{}/test", root).as_str()).is_file(), true);
        }

        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine);
            engine.init();
            assert_eq!(
                Path::new(format!("{}/test", root).as_str()).is_file(),
                false
            );
        }
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_dir", db_path)).unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_file", db_path)).unwrap();
        rocksdb::DB::destroy(
            &rocksdb::Options::default(),
            format!("{}_file_attr", db_path),
        )
        .unwrap();
    }

    #[test]
    fn test_create_delete_file() {
        let root = "/tmp/test_create_delete_file";
        let db_path = "/tmp/test_file_db";
        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine.clone());
            engine.init();
            meta_engine.create_directory("test1", 0o777).unwrap();
            let mode: mode_t = 0o777;
            let oflag: i32 = OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits();
            engine.create_file("test1/a.txt", oflag, 0, mode).unwrap();
            let file_attr = meta_engine.get_file_attr("test1/a.txt").unwrap();
            assert_eq!(file_attr.kind, FileType::RegularFile); // 4 is RegularFile
            let local_file_name = generate_local_file_name(root, "test1/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("test1/a.txt").unwrap();
            assert_eq!(Path::new(&local_file_name).is_file(), false);
            meta_engine.delete_directory("test1").unwrap();
        }

        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine.clone());
            engine.init();
            meta_engine.create_directory("test1", 0o777).unwrap();
            let mode: mode_t = 0o777;
            let oflag: i32 = OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits();
            meta_engine.create_directory("test1/test_a", mode).unwrap();
            meta_engine
                .create_directory("test1/test_a/a", mode)
                .unwrap();
            engine
                .create_file("test1/test_a/a/a.txt", oflag, 0, mode)
                .unwrap();
            let local_file_name = generate_local_file_name(root, "test1/test_a/a/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("test1/test_a/a/a.txt").unwrap();
            assert_eq!(Path::new(&local_file_name).is_file(), false);
            meta_engine.delete_directory("test1/test_a/a").unwrap();
            meta_engine.delete_directory("test1/test_a").unwrap();
            meta_engine.delete_directory("test1").unwrap();
        }
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_dir", db_path)).unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_file", db_path)).unwrap();
        rocksdb::DB::destroy(
            &rocksdb::Options::default(),
            format!("{}_file_attr", db_path),
        )
        .unwrap();
    }

    #[test]
    fn test_read_write_file() {
        let root = "/tmp/test_read_write_file";
        let db_path = "/tmp/test_rw_db";
        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine.clone());
            engine.init();
            let mode: mode_t = 0o777;
            let oflag: i32 = OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits();
            engine.create_file("test1/b.txt", oflag, 0, mode).unwrap();
            engine
                .write_file("test1/b.txt", "hello world".as_bytes(), 0)
                .unwrap();
            let value = engine.read_file("test1/b.txt", 11, 0).unwrap();
            assert_eq!("hello world", String::from_utf8(value).unwrap());
            let file_attr = meta_engine.get_file_attr("test1/b.txt").unwrap();
            assert_eq!(file_attr.size, 11);
        }
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_dir", db_path)).unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), format!("{}_file", db_path)).unwrap();
        rocksdb::DB::destroy(
            &rocksdb::Options::default(),
            format!("{}_file_attr", db_path),
        )
        .unwrap();
    }
}
