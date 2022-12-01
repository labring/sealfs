// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use crate::EngineError;

use super::StorageEngine;
use nix::{
    fcntl::{self, OFlag},
    sys::{
        stat::Mode,
        uio::{pread, pwrite},
    },
    unistd::{self, mkdir},
};
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    path::Path,
};

pub struct DefaultEngine {
    pub file_db: Database,
    pub dir_db: Database,
    pub file_attr_db: Database,
    pub root: String,
}

pub struct Database {
    pub db: DB,
    pub db_opts: Options,
    pub path: String,
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
        }
    }

    fn init(&self) {
        let root_sub_dir = SubDirectory::new();
        let _ = self
            .dir_db
            .db
            .put(b"/", bincode::serialize(&root_sub_dir).unwrap());
        let _ = self.file_attr_db.db.put(b"/", b"d");
        let _ = self.file_db.db.put(b"/", b"");
    }

    fn get_file_attributes(&self, path: String) -> Result<Option<Vec<String>>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes())? {
            Some(value) => Ok(Some(bincode::deserialize(&value[..]).unwrap())),
            None => Ok(None),
        }
    }

    fn read_directory(&self, path: String) -> Result<Vec<String>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes())? {
            Some(value) => {
                if "d" != bincode::deserialize::<String>(&value[..]).unwrap() {
                    return Err(EngineError::NotDir);
                }
            }
            None => {}
        }
        match self.dir_db.db.get(path.as_bytes())? {
            Some(value) => {
                let sub_dirs: SubDirectory = bincode::deserialize(&value[..]).unwrap();
                let mut sub_dirs_str = Vec::new();
                for sub_dir in sub_dirs.sub_dir {
                    sub_dirs_str.push(sub_dir.0);
                }
                Ok(sub_dirs_str)
            }
            None => Err(EngineError::NoEntry),
        }
    }

    fn read_file(&self, path: String, size: i64, offset: i64) -> Result<Vec<u8>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(value)) => {
                if "f" != String::from_utf8(value).unwrap() {
                    return Err(EngineError::IsDir);
                }
            }
            Ok(None) => {
                return Err(EngineError::NoEntry);
            }
            Err(_) => {
                return Err(EngineError::IO);
            }
        }

        let local_file_name: String = match self.file_db.db.get(path.as_bytes())? {
            Some(value) => String::from_utf8(value).unwrap(),
            None => {
                return Err(EngineError::NoEntry);
            }
        };
        let oflag = OFlag::O_RDONLY;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
        let mut data = vec![0; size as usize];
        let _size = pread(fd, data.as_mut_slice(), offset)?;
        Ok(data)
    }

    fn write_file(&self, path: String, data: &[u8], offset: i64) -> Result<usize, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes())? {
            Some(value) => {
                if "f" != String::from_utf8(value).unwrap() {
                    return Err(EngineError::IsDir);
                }
            }
            None => {
                return Err(EngineError::NoEntry);
            }
        }
        let local_file_name = match self.file_db.db.get(path.as_bytes())? {
            Some(value) => String::from_utf8(value).unwrap(),
            None => {
                return Err(EngineError::NoEntry);
            }
        };
        let oflags = OFlag::O_WRONLY | OFlag::O_SYNC;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(local_file_name.as_str(), oflags, mode)?;
        let write_size = pwrite(fd, data, offset)?;
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
    ) -> Result<(), EngineError> {
        match self.dir_db.db.get(parent_dir.as_bytes()) {
            Ok(Some(value)) => {
                let mut sub_dirs: SubDirectory = bincode::deserialize(&value[..]).unwrap();
                sub_dirs.add_dir(file_name);
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
    ) -> Result<(), EngineError> {
        match self.dir_db.db.get(parent_dir.as_bytes())? {
            Some(value) => {
                let mut sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
                sub_dirs.delete_dir(file_name);
                self.dir_db.db.put(
                    parent_dir.as_bytes(),
                    bincode::serialize(&sub_dirs).unwrap(),
                )?;
            }
            None => {}
        }
        Ok(())
    }

    fn create_file(&self, path: String, mode: Mode) -> Result<i32, EngineError> {
        let local_file_name = generate_local_file_name(&self.root, &path);
        let oflag = OFlag::O_CREAT | OFlag::O_EXCL;
        let fd = fcntl::open(local_file_name.as_str(), oflag, mode)?;
        let attr = FileAttr::new().with_nlink(1);
        self.add_file_attr(&path, attr)?;
        self.file_db
            .db
            .put(&path.as_bytes(), local_file_name.as_bytes())?;
        Ok(fd)
    }

    fn delete_file(&self, path: String) -> Result<(), EngineError> {
        match self.file_db.db.get(path.as_bytes())? {
            Some(value) => {
                let local_file_name = String::from_utf8(value).unwrap();
                unistd::unlink(local_file_name.as_str())?;
            }
            None => {}
        }
        let attr = FileAttr::new().with_nlink(1);
        self.delete_file_attr(&path, attr)?;
        self.file_db.db.delete(path.as_bytes())?;
        Ok(())
    }

    fn create_directory(&self, path: String, _mode: Mode) -> Result<(), EngineError> {
        let sub_dirs = SubDirectory::new();
        self.dir_db
            .db
            .put(path.as_bytes(), bincode::serialize(&sub_dirs).unwrap())?;
        let attr = FileAttr::new().with_nlink(2);
        self.add_file_attr(&path, attr)?;
        self.file_attr_db.db.put(path.as_bytes(), b"d")?;
        Ok(())
    }

    fn delete_directory(&self, path: String) -> Result<(), EngineError> {
        match self.dir_db.db.get(path.as_bytes())? {
            Some(value) => {
                let sub_dir = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
                if sub_dir.sub_dir.len() != 3 {
                    return Err(EngineError::NotEmpty);
                }
            }
            None => {}
        }
        let attr = FileAttr::new().with_nlink(2);
        self.delete_file_attr(&path, attr)?;
        self.dir_db.db.delete(path.as_bytes())?;
        Ok(())
    }
}

impl DefaultEngine {
    fn delete_from_parent(&self, path: String) -> Result<(), EngineError> {
        let l = path.len() - 1;
        let (index, _) = path
            .chars()
            .into_iter()
            .rev()
            .enumerate()
            .find(|(i, c)| *i != l - 1 && *c == '/')
            .unwrap();
        let parent_dir = &path[..=index];
        // a temporary implementation
        match self.dir_db.db.get(parent_dir.as_bytes())? {
            Some(value) => {
                let mut sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
                sub_dirs.delete_dir(path[index + 1..].to_string());
                self.dir_db.db.put(
                    parent_dir.as_bytes(),
                    bincode::serialize(&sub_dirs).unwrap(),
                )?;
            }
            None => {}
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

    fn add_file_attr(&self, path: &str, attr: FileAttr) -> Result<(), EngineError> {
        self.file_attr_db.db.put(&path, "f")?;
        if let Some(v) = attr.nlink {
            let path_nlink = format!("{}_nlink", path);
            self.file_attr_db.db.put(&path_nlink, &[v])?;
        }
        Ok(())
    }

    fn delete_file_attr(&self, path: &str, attr: FileAttr) -> Result<(), EngineError> {
        self.file_attr_db.db.delete(path.as_bytes())?;
        if attr.nlink.is_some() {
            let path_nlink = format!("{}_nlink", path);
            self.file_attr_db.db.delete(&path_nlink)?;
        }
        Ok(())
    }
}

struct FileAttr {
    nlink: Option<u8>,
}

impl FileAttr {
    fn new() -> Self {
        Self { nlink: None }
    }

    fn with_nlink(mut self, v: u8) -> Self {
        self.nlink = Some(v);
        self
    }
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct SubDirectory {
    sub_dir: HashMap<String, String>,
}

impl SubDirectory {
    pub fn new() -> Self {
        let sub_dir = HashMap::from([
            (".".to_string(), "d".to_string()),
            ("".to_string(), "d".to_string()),
            ("..".to_string(), "d".to_string()),
        ]);
        SubDirectory { sub_dir }
    }

    pub fn add_dir(&mut self, dir: String) {
        self.sub_dir.insert(dir, "d".to_string());
    }

    pub fn delete_dir(&mut self, dir: String) {
        self.sub_dir.remove(&dir);
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
    use nix::sys::stat::Mode;

    use crate::storage_engine::{default_engine::generate_local_file_name, StorageEngine};

    use super::{DefaultEngine, SubDirectory};
    #[test]
    fn test_create_delete_dir() {
        {
            let engine = DefaultEngine::new("/tmp/test_dir_db", "/tmp/test");
            engine.init();
            engine
                .directory_add_entry("/".to_string(), "a/".to_string())
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a/".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a/".to_string());
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a/".to_string())
                .unwrap();
            engine.delete_directory("/a/".to_string()).unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a/").unwrap());
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.delete_dir("a/".to_string());
            assert_eq!(sub_dir, dirs);
        }

        {
            let engine = DefaultEngine::new("/tmp/test_dir_db", "/tmp/test");
            engine.init();
            engine
                .directory_add_entry("/".to_string(), "a1/".to_string())
                .unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a1/".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a1/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let mut sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);

            engine
                .directory_add_entry("/a1/".to_string(), "a2/".to_string())
                .unwrap();
            engine
                .create_directory("/a1/a2/".to_string(), mode)
                .unwrap();
            let v = engine.dir_db.db.get("/a1/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            sub_dir.add_dir("a2/".to_string());
            assert_eq!(dirs, sub_dir);

            engine
                .delete_directory_recursive("/a1/".to_string())
                .unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a1/").unwrap());

            engine
                .directory_add_entry("/".to_string(), "a3/".to_string())
                .unwrap();
            engine.create_directory("/a3/".to_string(), mode).unwrap();
            let v = engine.dir_db.db.get("/a3/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            let sub_dir = SubDirectory::new();
            assert_eq!(dirs, sub_dir);
            engine
                .directory_delete_entry("/".to_string(), "a3/".to_string())
                .unwrap();
            engine.delete_directory("/a3/".to_string()).unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a3/").unwrap());

            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let dirs = bincode::deserialize::<SubDirectory>(&v[..]).unwrap();
            assert_eq!(SubDirectory::new(), dirs);
        }
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_dir_db_dir").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_dir_db_file").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_dir_db_file_attr").unwrap();
    }

    #[test]
    fn test_create_delete_file() {
        {
            let engine = DefaultEngine::new("/tmp/test_file_db", "/tmp/test");
            engine.init();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_file("/a.txt".to_string(), mode).unwrap();
            let v = engine.file_db.db.get(b"/a.txt").unwrap().unwrap();
            let local_file_name = String::from_utf8(v).unwrap();
            assert_eq!(
                local_file_name,
                generate_local_file_name("/tmp/test", "/a.txt")
            );
            engine.delete_file("/a.txt".to_string()).unwrap();
            assert_eq!(None, engine.file_db.db.get(b"/a.txt").unwrap());
        }

        {
            let engine = DefaultEngine::new("/tmp/test_file_db", "/tmp/test");
            engine.init();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine
                .create_directory("/test_a/".to_string(), mode)
                .unwrap();
            engine
                .create_directory("/test_a/a/".to_string(), mode)
                .unwrap();
            engine
                .create_file("/test_a/a/a.txt".to_string(), mode)
                .unwrap();
            let v = engine.file_db.db.get(b"/test_a/a/a.txt").unwrap().unwrap();
            let local_file_name = String::from_utf8(v).unwrap();
            assert_eq!(
                local_file_name,
                generate_local_file_name("/tmp/test", "/test_a/a/a.txt")
            );
            engine.delete_file("/test_a/a/a.txt".to_string()).unwrap();
            assert_eq!(None, engine.file_db.db.get(b"/test_a/a/a.txt").unwrap());
        }

        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_file_db_dir").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_file_db_file").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_file_db_file_attr").unwrap();
    }

    #[test]
    fn test_read_write_file() {
        {
            let engine = DefaultEngine::new("/tmp/test_rw_db", "/tmp/test");
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
        }
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_rw_db_dir").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_rw_db_file").unwrap();
        rocksdb::DB::destroy(&rocksdb::Options::default(), "/tmp/test_rw_db_file_attr").unwrap();
    }
}
