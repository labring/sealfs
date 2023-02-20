// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::serialization::{FileAttrSimple, SubDirectory};
use crate::server::distributed_engine::path_split;

use super::EngineError;

use super::StorageEngine;
use dashmap::DashMap;
use log::debug;
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
};

pub struct DefaultEngine {
    pub file_map: DashMap<String, String>,
    pub dir_map: DashMap<String, SubDirectory>,
    pub file_attr_map: DashMap<String, FileAttrSimple>,
    pub root: String,
}

impl StorageEngine for DefaultEngine {
    fn new(_db_path: &str, root: &str) -> Self {
        let file_db = DashMap::new();

        let dir_db = DashMap::new();

        let file_attr_db = DashMap::new();

        if !Path::new(root).exists() {
            let mode =
                Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH | Mode::S_IWOTH;
            mkdir(root, mode).unwrap();
        }

        Self {
            file_map: file_db,
            dir_map: dir_db,
            file_attr_map: file_attr_db,
            root: root.to_string(),
        }
    }

    fn init(&self) {
        let result = { self.dir_map.contains_key("/") };
        if !result {
            let root_sub_dir = SubDirectory::new();
            let _ = self.dir_map.insert("/".to_string(), root_sub_dir);
        }
        let result = { self.file_attr_map.contains_key("/") };
        if !result {
            let file_attr = FileAttrSimple::new(fuser::FileType::Directory);
            let _ = self.file_attr_map.insert("/".to_string(), file_attr);
        }

        let result = self.file_map.contains_key("/");
        if !result {
            let _ = self.file_map.insert("/".to_string(), "".to_string());
        }
        self.fsck().unwrap();
    }

    // The path parameter for all methods is absolute path end not with '/', except for root path
    // For efficiency, we don't check the path is valid or not.
    // This should be done in the upper layer (client for now).

    fn get_file_attributes(&self, path: String) -> Result<Vec<u8>, EngineError> {
        debug!("get_file_attributes path: {}", path);
        match self.file_attr_map.get(&path) {
            Some(value) => {
                debug!("path: {}, value: {:?}", path, value);
                // value.value().;
                Ok(bincode::serialize(value.value()).unwrap())
            }
            None => Err(EngineError::NoEntry),
        }
    }

    fn read_directory(&self, path: String) -> Result<Vec<u8>, EngineError> {
        if let Some(value) = self.file_attr_map.get(&path) {
            debug!("read_dir getting attr, path: {}, value: {:?}", path, value);
            let file_attr = value.value();
            // fuser::FileType::Directory
            if file_attr.kind != 3 {
                return Err(EngineError::NotDir);
            }
        }
        match self.dir_map.get(&path) {
            Some(value) => {
                debug!("read_dir path: {}, value: {:?}", path, value);
                Ok(bincode::serialize(value.value()).unwrap())
            }
            None => Err(EngineError::NoEntry),
        }
    }

    fn read_file(&self, path: String, size: u32, offset: i64) -> Result<Vec<u8>, EngineError> {
        match self.file_attr_map.get(&path) {
            Some(value) => {
                let file_attr = value.value();
                // fuser::FileType::RegularFile
                if file_attr.kind != 4 {
                    return Err(EngineError::IsDir); // not a file
                }
            }
            None => {
                debug!(
                    "read_file path: {}, size: {}, offset: {}, no entry",
                    path, size, offset
                );
                return Err(EngineError::NoEntry);
            }
        }

        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflag = OFlag::O_RDONLY;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
        let mut data = vec![0; size as usize];
        let real_size = pread(fd, data.as_mut_slice(), offset)?;
        debug!(
            "read_file path: {}, size: {}, offset: {}, data: {:?}",
            path, real_size, offset, data
        );
        nix::unistd::close(fd)?;

        // this is a temporary solution, which results in an extra memory copy.
        // TODO: optimize it by return the hole data vector and the real size both.
        Ok(data[..real_size].to_vec())
    }

    fn write_file(&self, path: String, data: &[u8], offset: i64) -> Result<usize, EngineError> {
        let mut file_attr = {
            match self.file_attr_map.get(&path) {
                Some(value) => {
                    let file_attr = value.value().clone();

                    if file_attr.kind != 4 {
                        // fuser::FileType::RegularFile
                        return Err(EngineError::IsDir); // not a file
                    }
                    file_attr
                }
                None => {
                    return Err(EngineError::NoEntry);
                }
            }
        };
        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflags = OFlag::O_WRONLY;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(local_file_name.as_str(), oflags, mode)?;
        let write_size = pwrite(fd, data, offset)?;
        debug!(
            "write_file path: {}, write_size: {}, data_len: {}",
            path,
            write_size,
            data.len()
        );
        nix::unistd::close(fd)?;
        file_attr.size = file_attr.size.max(offset as u64 + write_size as u64);
        self.file_attr_map.insert(path, file_attr);
        Ok(write_size)
    }

    fn delete_directory_recursive(&self, path: String) -> Result<(), EngineError> {
        self.file_attr_map.remove(&path);
        self.delete_dir_recursive(path.clone())?;
        self.delete_from_parent(path)?;
        Ok(())
    }

    fn is_exist(&self, path: String) -> bool {
        self.file_attr_map.contains_key(&path)
    }

    fn directory_add_entry(
        &self,
        parent_dir: String,
        file_name: String,
    ) -> Result<(), EngineError> {
        match self.dir_map.get_mut(&parent_dir) {
            Some(mut value) => {
                (*value).add_dir(file_name);
            }
            None => {
                return Err(EngineError::NoEntry);
            }
        }

        Ok(())
    }

    fn directory_delete_entry(
        &self,
        parent_dir: String,
        file_name: String,
    ) -> Result<(), EngineError> {
        match self.dir_map.get_mut(&parent_dir) {
            Some(mut value) => {
                value.delete_dir(file_name);
            }
            None => return Ok(()),
        }

        Ok(())
    }

    fn create_file(&self, path: String, mode: Mode) -> Result<Vec<u8>, EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflag = OFlag::O_CREAT | OFlag::O_EXCL;
        let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
        nix::unistd::close(fd)?;
        self.file_map.insert(local_file_name, path.clone());
        let attr = FileAttrSimple::new(fuser::FileType::RegularFile);
        self.add_file_attr(&path, attr)
    }

    fn delete_file(&self, path: String) -> Result<(), EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        unistd::unlink(local_file_name.as_str())?;
        self.delete_file_attr(&path)?;
        self.file_map.remove(&local_file_name);
        Ok(())
    }

    fn create_directory(&self, path: String, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        let sub_dirs = SubDirectory::new();
        self.dir_map.insert(path.clone(), sub_dirs);
        let attr = FileAttrSimple::new(fuser::FileType::Directory);
        self.add_file_attr(&path, attr)
    }

    fn delete_directory(&self, path: String) -> Result<(), EngineError> {
        let sub_dirs = {
            match self.dir_map.get(&path) {
                Some(value) => value.value().clone(),
                None => return Ok(()),
            }
        };

        if sub_dirs.sub_dir.len() != 2 {
            return Err(EngineError::NotEmpty);
        }

        self.delete_file_attr(&path)?;
        self.dir_map.remove(&path);
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
    // memory not need
    fn fsck(&self) -> Result<(), EngineError> {
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            // let file_name = format!("{}/{}", self.root, entry.file_name().to_str().unwrap());
            // let mut file_str = String::new();
            // if self.file_db.db.key_may_exist(&file_name) {
            //     let file_vec = self.file_db.db.get(&file_name).unwrap().unwrap();
            //     file_str = String::from_utf8(file_vec).unwrap();
            //     if self.file_attr_db.db.key_may_exist(&file_str) {
            //         continue;
            //     }
            //     let _ = self.file_db.db.delete(&file_name);
            // }
            // if self.file_attr_db.db.key_may_exist(&file_str) {
            //     let _ = self.file_attr_db.db.delete(file_str);
            // }
            let _ = std::fs::remove_file(entry.path());
        }

        // for item in self.dir_db.db.iterator(IteratorMode::End) {
        //     let (key, _value) = item.unwrap();
        //     if !self.file_attr_db.db.key_may_exist(&key) {
        //         let _ = self.dir_db.db.delete(&key);
        //     }
        // }
        Ok(())
    }

    fn delete_from_parent(&self, path: String) -> Result<(), EngineError> {
        let (parent, name) = path_split(path).unwrap();
        let mut sub_dirs = {
            match self.dir_map.get(&parent) {
                Some(value) => value.value().clone(),
                None => return Ok(()),
            }
        };

        sub_dirs.delete_dir(name);
        self.dir_map.insert(parent, sub_dirs);

        Ok(())
    }

    fn delete_dir_recursive(&self, path: String) -> Result<(), EngineError> {
        let sub_dirs = {
            match self.dir_map.get(&path) {
                Some(value) => value.value().clone(),
                None => return Ok(()),
            }
        };

        if sub_dirs == SubDirectory::new() {
            self.dir_map.remove(&path);
            return Ok(());
        }
        for sub_dir in sub_dirs.sub_dir {
            if sub_dir.0 == "." || sub_dir.0 == ".." || sub_dir.0.is_empty() {
                continue;
            }
            let p = format!("{}/{}", path, sub_dir.0);
            if sub_dir.1.eq("f") {
                self.file_attr_map.remove(&p);
                let local_file = {
                    match self.file_map.get(&p) {
                        Some(value) => value,
                        None => {
                            return Err(EngineError::NoEntry);
                        }
                    }
                };
                unistd::unlink(local_file.as_str())?;
                self.file_map.remove(&p);
                continue;
            }
            self.delete_dir_recursive(p)?;
        }
        self.dir_map.remove(&path);

        Ok(())
    }

    fn add_file_attr(&self, path: &str, attr: FileAttrSimple) -> Result<Vec<u8>, EngineError> {
        self.file_attr_map.insert(path.to_string(), attr.clone());
        Ok(bincode::serialize(&attr).unwrap())
    }

    fn delete_file_attr(&self, path: &str) -> Result<(), EngineError> {
        self.file_attr_map.remove(path);
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
                .directory_add_entry("/".to_string(), "a".to_string())
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a".to_string(), mode).unwrap();
            let dirs = engine.dir_map.get("/a").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            let dirs = engine.dir_map.get("/").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a".to_string());
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a".to_string())
                .unwrap();
            engine.delete_directory("/a".to_string()).unwrap();
            assert_eq!(true, engine.dir_map.get("/a").is_none());
            let dirs = engine.dir_map.get("/").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.delete_dir("a".to_string());
            assert_eq!(sub_dir, dirs);
        }

        {
            let engine = DefaultEngine::new(db_path, root);
            engine.init();
            engine
                .directory_add_entry("/".to_string(), "a1".to_string())
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a1".to_string(), mode).unwrap();
            let dirs = engine.dir_map.get("/a1").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);

            engine
                .directory_add_entry("/a1".to_string(), "a2".to_string())
                .unwrap();
            engine.create_directory("/a1/a2".to_string(), mode).unwrap();
            let dirs = engine.dir_map.get("/a1").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a2".to_string());
            assert_eq!(dirs, sub_dir);

            engine
                .delete_directory_recursive("/a1".to_string())
                .unwrap();
            assert_eq!(true, engine.dir_map.get("/a1").is_none());

            engine
                .directory_add_entry("/".to_string(), "a3".to_string())
                .unwrap();
            engine.create_directory("/a3".to_string(), mode).unwrap();
            let dirs = engine.dir_map.get("/a3").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a3".to_string())
                .unwrap();
            engine.delete_directory("/a3".to_string()).unwrap();
            assert_eq!(true, engine.dir_map.get("/a3").is_none());

            let dirs = engine.dir_map.get("/").unwrap().value().clone();
            // let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
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
