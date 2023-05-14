// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::serialization::FileAttrSimple;
use crate::common::{cache::LRUCache, serialization::FileTypeSimple};

use super::EngineError;

use super::meta_engine::MetaEngine;
use super::StorageEngine;
use log::{debug, error};
use nix::{
    fcntl::{self, OFlag},
    sys::{
        stat::Mode,
        uio::{pread, pwrite},
    },
    unistd::{self, mkdir},
};
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
        self.meta_engine.init();
        self.fsck().unwrap();
    }

    fn read_file(&self, path: &str, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        if self.meta_engine.is_dir(path)? {
            return Err(EngineError::IsDir);
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
                let fd = match fcntl::open(local_file_name.as_str(), oflag, mode) {
                    Ok(fd) => fd,
                    Err(err) => {
                        error!("open file error: {:?}", err);
                        return Err(EngineError::IO);
                    }
                };
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let mut data = vec![0; size as usize];
        let real_size = match pread(fd, data.as_mut_slice(), offset) {
            Ok(size) => size,
            Err(err) => {
                error!("read file error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        debug!(
            "read_file path: {}, size: {}, offset: {}, data: {:?}",
            path, real_size, offset, data
        );

        // this is a temporary solution, which results in an extra memory copy.
        // TODO: optimize it by return the hole data vector and the real size both.
        Ok(data[..real_size].to_vec())
    }

    fn write_file(&self, path: &str, data: &[u8], offset: i64) -> Result<usize, EngineError> {
        if self.meta_engine.is_dir(path)? {
            return Err(EngineError::IsDir);
        }

        let local_file_name = generate_local_file_name(&self.root, path);
        let oflags = OFlag::O_RDWR;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = match self.cache.get(local_file_name.as_bytes()) {
            Some(value) => value.fd,
            None => {
                let fd = match fcntl::open(local_file_name.as_str(), oflags, mode) {
                    Ok(fd) => fd,
                    Err(err) => {
                        error!("open file error: {:?}", err);
                        return Err(EngineError::IO);
                    }
                };
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let write_size = match pwrite(fd, data, offset) {
            Ok(size) => size,
            Err(err) => {
                error!("write file error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        debug!(
            "write_file path: {}, write_size: {}, data_len: {}",
            path,
            write_size,
            data.len()
        );

        let mut file_attr = self.meta_engine.get_file_attr(path)?;
        file_attr.size = file_attr.size.max(offset as u64 + write_size as u64);
        self.meta_engine.put_file_attr(path, file_attr)?;

        Ok(write_size)
    }

    fn create_file(
        &self,
        path: &str,
        _oflag: i32,
        _umask: u32,
        mode: u32,
    ) -> Result<Vec<u8>, EngineError> {
        let local_file_name = generate_local_file_name(&self.root, path);
        match self.cache.get(local_file_name.as_bytes()) {
            Some(_) => {}
            None => {
                let fd = match fcntl::open(
                    local_file_name.as_str(),
                    OFlag::from_bits_truncate(OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits()),
                    Mode::from_bits_truncate(mode),
                ) {
                    Ok(fd) => fd,
                    Err(err) => {
                        error!("open file error: {:?}", err);
                        return Err(EngineError::IO);
                    }
                };
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
            }
        };
        self.meta_engine.put_file(&local_file_name, path)?;
        let attr = FileAttrSimple::new(FileTypeSimple::RegularFile);
        self.meta_engine.put_file_attr(path, attr)
    }

    fn delete_file(&self, path: &str) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, path);
        self.cache.remove(local_file_name.as_bytes());
        match unistd::unlink(local_file_name.as_str()) {
            Ok(_) => {}
            Err(err) => {
                error!("delete file error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        self.meta_engine.delete_file_attr(path)?;
        self.meta_engine.delete_file(&local_file_name)?;
        Ok(())
    }

    fn truncate_file(&self, path: &str, length: i64) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, path);
        match unistd::truncate(local_file_name.as_str(), length) {
            Ok(_) => {}
            Err(err) => {
                error!("truncate file error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        Ok(())
    }

    fn open_file(&self, path: &str, _flags: i32, mode: u32) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, path);
        match fcntl::open(
            local_file_name.as_str(),
            OFlag::from_bits_truncate(OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits()),
            Mode::from_bits_truncate(mode),
        ) {
            Ok(fd) => {
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
            }
            Err(err) => {
                error!("open file error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        Ok(())
    }
}

impl FileEngine {
    fn fsck(&self) -> Result<(), EngineError> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) => {
                error!("read dir error: {:?}", err);
                return Err(EngineError::IO);
            }
        };
        for entry in entries {
            let entry = entry?;
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
            let mode: mode_t = 0o777;
            let oflag: i32 = OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits();
            engine.create_file("/a.txt", oflag, 0, mode).unwrap();
            let file_attr = meta_engine.get_file_attr("/a.txt").unwrap();
            assert_eq!(file_attr.kind, 4); // 4 is RegularFile
            let local_file_name = generate_local_file_name(root, "/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("/a.txt").unwrap();
            assert_eq!(Path::new(&local_file_name).is_file(), false);
        }

        {
            let meta_engine = Arc::new(MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024));
            let engine = FileEngine::new(root, meta_engine.clone());
            engine.init();
            let mode: mode_t = 0o777;
            let oflag: i32 = OFlag::O_CREAT.bits() | OFlag::O_RDWR.bits();
            meta_engine.create_directory("/test_a", mode).unwrap();
            meta_engine.create_directory("/test_a/a", mode).unwrap();
            engine
                .create_file("/test_a/a/a.txt", oflag, 0, mode)
                .unwrap();
            let local_file_name = generate_local_file_name(root, "/test_a/a/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("/test_a/a/a.txt").unwrap();
            assert_eq!(Path::new(&local_file_name).is_file(), false);
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
            engine.create_file("/b.txt", oflag, 0, mode).unwrap();
            engine
                .write_file("/b.txt", "hello world".as_bytes(), 0)
                .unwrap();
            let value = engine.read_file("/b.txt", 11, 0).unwrap();
            assert_eq!("hello world", String::from_utf8(value).unwrap());
            let file_attr = meta_engine.get_file_attr("/b.txt").unwrap();
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
