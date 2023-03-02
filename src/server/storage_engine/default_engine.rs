// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::cache::LRUCache;
use crate::common::serialization::{FileAttrSimple, SubDirectory};
use crate::server::distributed_engine::path_split;

use super::EngineError;

use super::StorageEngine;
use log::debug;
use log::error;
use nix::{
    fcntl::{self, OFlag},
    sys::{
        stat::Mode,
        uio::{pread, pwrite},
    },
    unistd::{self, mkdir},
};
use rocksdb::{IteratorMode, Options, DB};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::Path,
};

pub struct DefaultEngine {
    pub file_db: Database,
    pub dir_db: Database,
    pub file_attr_db: Database,
    pub root: String,
    pub cache: LRUCache<FileDescriptor>,
}

pub struct Database {
    pub db: DB,
    pub db_opts: Options,
    pub path: String,
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

impl StorageEngine for DefaultEngine {
    fn new(db_path: &str, root: &str) -> Self {
        let file_db = {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            let path = format!("{}_file", db_path);
            let db = match DB::open(&db_opts, path.as_str()) {
                Ok(db) => db,
                Err(_) => panic!("Failed to create db"),
            };
            Database { db, db_opts, path }
        };

        let dir_db = {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            let path = format!("{}_dir", db_path);
            let db = match DB::open(&db_opts, path.as_str()) {
                Ok(db) => db,
                Err(_) => panic!("Failed to create db"),
            };
            Database { db, db_opts, path }
        };

        let file_attr_db = {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            let path = format!("{}_file_attr", db_path);
            let db = match DB::open(&db_opts, path.as_str()) {
                Ok(db) => db,
                Err(_) => panic!("Failed to create db"),
            };
            Database { db, db_opts, path }
        };

        if !Path::new(root).exists() {
            let mode =
                Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH | Mode::S_IWOTH;
            mkdir(root, mode).unwrap();
        }

        Self {
            file_db,
            dir_db,
            file_attr_db,
            root: root.to_string(),
            cache: LRUCache::new(512),
        }
    }

    fn init(&self) {
        if !self.dir_db.db.key_may_exist(b"/") {
            let root_sub_dir = SubDirectory::new();
            let _ = self
                .dir_db
                .db
                .put(b"/", bincode::serialize(&root_sub_dir).unwrap());
        }
        if !self.file_attr_db.db.key_may_exist(b"/") {
            let file_attr = FileAttrSimple::new(fuser::FileType::Directory);
            let _ = self
                .file_attr_db
                .db
                .put(b"/", bincode::serialize(&file_attr).unwrap());
        }
        if !self.file_db.db.key_may_exist(b"/") {
            let _ = self.file_db.db.put(b"/", b"");
        }
        self.fsck().unwrap();
    }

    // The path parameter for all methods is absolute path end not with '/', except for root path
    // For efficiency, we don't check the path is valid or not.
    // This should be done in the upper layer (client for now).

    fn get_file_attributes(&self, path: String) -> Result<Vec<u8>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes())? {
            Some(value) => Ok(value),
            None => Err(EngineError::NoEntry),
        }
    }

    fn read_directory(&self, path: String) -> Result<Vec<u8>, EngineError> {
        if let Some(value) = self.file_attr_db.db.get(path.as_bytes())? {
            // debug!("read_dir getting attr, path: {}, value: {:?}", path, value);
            match bincode::deserialize::<FileAttrSimple>(&value) {
                Ok(file_attr) => {
                    // fuser::FileType::Directory
                    if file_attr.kind != 3 {
                        return Err(EngineError::NotDir);
                    }
                }
                Err(_) => {
                    return Err(EngineError::IO);
                }
            }
        }
        match self.dir_db.db.get(path.as_bytes())? {
            Some(value) => {
                // debug!("read_dir path: {}, value: {:?}", path, value);
                Ok(value)
            }
            None => Err(EngineError::NoEntry),
        }
    }

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(value)) => {
                match bincode::deserialize::<FileAttrSimple>(&value) {
                    Ok(file_attr) => {
                        // fuser::FileType::RegularFile
                        if file_attr.kind != 4 {
                            return Err(EngineError::IsDir); // not a file
                        }
                    }
                    Err(e) => {
                        error!("deserialize error: {:?}", e);
                        return Err(EngineError::IO);
                    }
                }
            }
            Ok(None) => {
                debug!(
                    "read_file path: {}, size: {}, offset: {}, no entry",
                    path, size, offset
                );
                return Err(EngineError::NoEntry);
            }
            Err(_) => {
                debug!(
                    "read_file path: {}, size: {}, offset: {}, io error",
                    path, size, offset
                );
                return Err(EngineError::IO);
            }
        }

        let local_file_name = generate_local_file_name(&self.root, &path);
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
                let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let mut data = vec![0; size as usize];
        let real_size = pread(fd, data.as_mut_slice(), offset)?;
        debug!(
            "read_file path: {}, size: {}, offset: {}, data: {:?}",
            path, real_size, offset, data
        );

        // this is a temporary solution, which results in an extra memory copy.
        // TODO: optimize it by return the hole data vector and the real size both.
        Ok(data[..real_size].to_vec())
    }

    fn write_file(&self, path: String, data: &[u8], offset: i64) -> Result<usize, EngineError> {
        let mut file_attr = match self.file_attr_db.db.get(path.as_bytes())? {
            Some(value) => {
                match bincode::deserialize::<FileAttrSimple>(&value) {
                    Ok(file_attr) => {
                        if file_attr.kind != 4 {
                            // fuser::FileType::RegularFile
                            return Err(EngineError::IsDir); // not a file
                        }
                        file_attr
                    }
                    Err(e) => {
                        error!("deserialize error: {:?}", e);
                        return Err(EngineError::IO);
                    }
                }
            }
            None => {
                return Err(EngineError::NoEntry);
            }
        };
        let local_file_name = generate_local_file_name(&self.root, &path);
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
                let fd = fcntl::open(local_file_name.as_str(), oflags, mode)?;
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
                fd
            }
        };
        let write_size = pwrite(fd, data, offset)?;
        debug!(
            "write_file path: {}, write_size: {}, data_len: {}",
            path,
            write_size,
            data.len()
        );
        file_attr.size = file_attr.size.max(offset as u64 + write_size as u64);
        let file_attr_bytes = bincode::serialize(&file_attr).unwrap();
        self.file_attr_db.db.put(path.as_bytes(), file_attr_bytes)?;
        Ok(write_size)
    }

    fn delete_directory_recursive(&self, path: String) -> Result<(), EngineError> {
        self.file_attr_db.db.delete(path.as_bytes())?;
        self.delete_dir_recursive(path.clone())?;
        self.delete_from_parent(path)?;
        Ok(())
    }

    fn is_exist(&self, path: String) -> Result<bool, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(_value)) => Ok(true),
            Ok(None) => Ok(false),
            Err(_) => Err(EngineError::IO),
        }
    }

    fn directory_add_entry(
        &self,
        parent_dir: String,
        file_name: String,
        file_type: u8,
    ) -> Result<(), EngineError> {
        match self.dir_db.db.get(parent_dir.as_bytes()) {
            Ok(Some(value)) => {
                let mut sub_dirs: SubDirectory = bincode::deserialize(&value[..]).unwrap();
                match file_type {
                    3 => sub_dirs.add_dir(file_name),
                    _ => sub_dirs.add_file(file_name),
                }
                // sub_dirs.add_dir(file_name);
                self.dir_db.db.put(
                    parent_dir.as_bytes(),
                    bincode::serialize(&sub_dirs).unwrap(),
                )?;
            }
            Ok(None) => {
                return Err(EngineError::NoEntry);
            }
            Err(_) => {
                return Err(EngineError::IO);
            }
        }
        Ok(())
    }

    fn directory_delete_entry(
        &self,
        parent_dir: String,
        file_name: String,
        file_type: u8,
    ) -> Result<(), EngineError> {
        if let Some(value) = self.dir_db.db.get(parent_dir.as_bytes())? {
            let mut sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
            match file_type {
                3 => sub_dirs.delete_dir(file_name),
                _ => sub_dirs.delete_file(file_name),
            }
            self.dir_db.db.put(
                parent_dir.as_bytes(),
                bincode::serialize(&sub_dirs).unwrap(),
            )?;
        }
        Ok(())
    }

    fn create_file(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflag = OFlag::O_CREAT | OFlag::O_RDWR;
        match self.cache.get(local_file_name.as_bytes()) {
            Some(_) => {}
            None => {
                let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
                self.cache
                    .insert(local_file_name.as_bytes(), FileDescriptor::new(fd));
            }
        };
        self.file_db.db.put(&local_file_name, &path)?;
        let attr = FileAttrSimple::new(fuser::FileType::RegularFile);
        self.add_file_attr(&path, attr)
    }

    fn delete_file(&self, path: String) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        self.cache.remove(local_file_name.as_bytes());
        unistd::unlink(local_file_name.as_str())?;
        self.delete_file_attr(&path)?;
        self.file_db.db.delete(local_file_name)?;
        Ok(())
    }

    fn truncate_file(&self, path: String, length: i64) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        unistd::truncate(local_file_name.as_str(), length)?;
        Ok(())
    }

    fn create_directory(&self, path: String, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        let sub_dirs = SubDirectory::new();
        self.dir_db
            .db
            .put(path.as_bytes(), bincode::serialize(&sub_dirs).unwrap())?;
        let attr = FileAttrSimple::new(fuser::FileType::Directory);
        self.add_file_attr(&path, attr)
    }

    fn delete_directory(&self, path: String) -> Result<(), EngineError> {
        if let Some(value) = self.dir_db.db.get(path.as_bytes())? {
            let sub_dir = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
            if sub_dir.sub_dir.len() != 2 {
                return Err(EngineError::NotEmpty);
            }
        }
        self.delete_file_attr(&path)?;
        self.dir_db.db.delete(path.as_bytes())?;
        Ok(())
    }

    fn open_file(&self, path: String, mode: Mode) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflags = OFlag::O_RDWR;
        let _ = fcntl::open(local_file_name.as_str(), oflags, mode)?;
        Ok(())
    }
}

impl DefaultEngine {
    fn fsck(&self) -> Result<(), EngineError> {
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            let file_name = format!("{}/{}", self.root, entry.file_name().to_str().unwrap());
            let mut file_str = String::new();
            if self.file_db.db.key_may_exist(&file_name) {
                let file_vec = self.file_db.db.get(&file_name).unwrap().unwrap();
                file_str = String::from_utf8(file_vec).unwrap();
                if self.file_attr_db.db.key_may_exist(&file_str) {
                    continue;
                }
                let _ = self.file_db.db.delete(&file_name);
            }
            if self.file_attr_db.db.key_may_exist(&file_str) {
                let _ = self.file_attr_db.db.delete(file_str);
            }
            let _ = std::fs::remove_file(entry.path());
        }

        for item in self.dir_db.db.iterator(IteratorMode::End) {
            let (key, _value) = item.unwrap();
            if !self.file_attr_db.db.key_may_exist(&key) {
                let _ = self.dir_db.db.delete(&key);
            }
        }
        Ok(())
    }

    fn delete_from_parent(&self, path: String) -> Result<(), EngineError> {
        let (parent, name) = path_split(path).unwrap();
        if let Some(value) = self.dir_db.db.get(parent.as_bytes())? {
            let mut sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
            sub_dirs.delete_dir(name);
            self.dir_db
                .db
                .put(parent.as_bytes(), bincode::serialize(&sub_dirs).unwrap())?;
        }
        Ok(())
    }

    fn delete_dir_recursive(&self, path: String) -> Result<(), EngineError> {
        match self.dir_db.db.get(path.as_bytes())? {
            Some(value) => {
                let sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
                if sub_dirs == SubDirectory::new() {
                    self.dir_db.db.delete(path.as_bytes())?;
                    return Ok(());
                }
                for sub_dir in sub_dirs.sub_dir {
                    if sub_dir.0 == "." || sub_dir.0 == ".." || sub_dir.0.is_empty() {
                        continue;
                    }
                    let p = format!("{}/{}", path, sub_dir.0);
                    if sub_dir.1.eq("f") {
                        self.file_attr_db.db.delete(p.as_bytes())?;
                        let local_file = match self.file_db.db.get(p.as_bytes())? {
                            Some(value) => String::from_utf8(value).unwrap(),
                            None => {
                                return Err(EngineError::NoEntry);
                            }
                        };
                        unistd::unlink(local_file.as_str())?;
                        self.file_db.db.delete(p.as_bytes())?;
                        continue;
                    }
                    self.delete_dir_recursive(p)?;
                }
                self.dir_db.db.delete(path.as_bytes())?;
            }
            None => {
                self.dir_db.db.delete(path.as_bytes())?;
                return Ok(());
            }
        }
        Ok(())
    }

    fn add_file_attr(&self, path: &str, attr: FileAttrSimple) -> Result<Vec<u8>, EngineError> {
        let value = bincode::serialize(&attr).unwrap();
        self.file_attr_db.db.put(&path, &value)?;
        Ok(value)
    }

    fn delete_file_attr(&self, path: &str) -> Result<(), EngineError> {
        self.file_attr_db.db.delete(path.as_bytes())?;
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
    use std::path::Path;

    use crate::common::serialization::{FileAttrSimple, SubDirectory};
    use nix::{
        fcntl::{self, OFlag},
        sys::stat::Mode,
    };

    use crate::server::storage_engine::{default_engine::generate_local_file_name, StorageEngine};

    use super::DefaultEngine;

    #[test]
    fn test_init() {
        let root = "/tmp/test_init";
        let db_path = "/tmp/test_db";
        {
            let engine = DefaultEngine::new(db_path, root);
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
            let engine = DefaultEngine::new(db_path, root);
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
    fn test_create_delete_dir() {
        let root = "/tmp/test_create_delete_dir";
        let db_path = "/tmp/test_dir_db";
        {
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            engine
                .directory_add_entry("/".to_string(), "a".to_string(), 3)
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a".to_string());
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a".to_string(), 3)
                .unwrap();
            engine.delete_directory("/a".to_string()).unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a").unwrap());
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.delete_dir("a".to_string());
            assert_eq!(sub_dir, dirs);
        }

        {
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            engine
                .directory_add_entry("/".to_string(), "a1".to_string(), 3)
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a1".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a1").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);

            engine
                .directory_add_entry("/a1".to_string(), "a2".to_string(), 3)
                .unwrap();
            engine.create_directory("/a1/a2".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a1").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a2".to_string());
            assert_eq!(dirs, sub_dir);

            engine
                .delete_directory_recursive("/a1".to_string())
                .unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a1").unwrap());

            engine
                .directory_add_entry("/".to_string(), "a3".to_string(), 3)
                .unwrap();
            engine.create_directory("/a3".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a3").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a3".to_string(), 3)
                .unwrap();
            engine.delete_directory("/a3".to_string()).unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a3").unwrap());

            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            assert_eq!(SubDirectory::new(), dirs);
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
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_file("/a.txt".to_string(), mode).unwrap();
            let file_attr = {
                let file_attr_bytes = engine.get_file_attributes("/a.txt".to_string()).unwrap();
                bincode::deserialize::<FileAttrSimple>(&file_attr_bytes[..]).unwrap()
            };
            assert_eq!(file_attr.kind, 4); // 4 is RegularFile
            let local_file_name = generate_local_file_name(root, "/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("/a.txt".to_string()).unwrap();
            assert_eq!(Path::new(&local_file_name).is_file(), false);
        }

        {
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine
                .create_directory("/test_a".to_string(), mode)
                .unwrap();
            engine
                .create_directory("/test_a/a".to_string(), mode)
                .unwrap();
            engine
                .create_file("/test_a/a/a.txt".to_string(), mode)
                .unwrap();
            let local_file_name = generate_local_file_name(root, "/test_a/a/a.txt");
            assert_eq!(Path::new(&local_file_name).is_file(), true);
            engine.delete_file("/test_a/a/a.txt".to_string()).unwrap();
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
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_file("/b.txt".to_string(), mode).unwrap();
            engine
                .write_file("/b.txt".to_string(), "hello world".as_bytes(), 0)
                .unwrap();
            let value = engine.read_file("/b.txt".to_string(), 11, 0).unwrap();
            assert_eq!("hello world", String::from_utf8(value).unwrap());
            let file_attr = {
                let file_attr_bytes = engine.get_file_attributes("/b.txt".to_string()).unwrap();
                bincode::deserialize::<FileAttrSimple>(&file_attr_bytes[..]).unwrap()
            };
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
