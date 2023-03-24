use bytes::BufMut;
use libc::{DT_DIR, DT_LNK, DT_REG};
use log::{debug, error};
use nix::sys::stat::Mode;
#[cfg(feature = "mem-db")]
use pegasusdb::DB;
#[cfg(feature = "disk-db")]
use rocksdb::{IteratorMode, Options, DB};

use crate::{
    common::serialization::FileAttrSimple,
    server::{path_split, EngineError},
};

#[cfg(feature = "disk-db")]
pub struct Database {
    pub db: DB,
    pub db_opts: Options,
    pub path: String,
}

#[cfg(feature = "mem-db")]
pub struct Database {
    pub db: DB,
}

pub struct MetaEngine {
    pub file_db: Database,
    pub dir_db: Database,
    pub file_attr_db: Database,
}

impl MetaEngine {
    pub fn new(db_path: &str) -> Self {
        #[cfg(feature = "disk-db")]
        let (file_db, dir_db, file_attr_db) = {
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
            (file_db, dir_db, file_attr_db)
        };

        #[cfg(feature = "mem-db")]
        let (file_db, dir_db, file_attr_db) = {
            let file_db = DB::open(format!("{db_path}_file"));
            let dir_db = DB::open(format!("{db_path}_dir"));
            let file_attr_db = DB::open(format!("{db_path}_file_attr"));
            (
                Database { db: file_db },
                Database { db: dir_db },
                Database { db: file_attr_db },
            )
        };

        Self {
            file_db,
            dir_db,
            file_attr_db,
        }
    }

    pub fn init(&self) {
        if !self.dir_db.db.key_may_exist(b"/") {
            let _ = self.dir_db.db.put(b"/", 2_usize.to_le_bytes());
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
    }

    pub fn put_file(&self, loacl_file_name: &str, path: &str) -> Result<(), EngineError> {
        self.file_db.db.put(loacl_file_name, path)?;
        Ok(())
    }

    pub fn delete_file(&self, local_file_name: &str) -> Result<(), EngineError> {
        self.file_db.db.delete(local_file_name)?;
        Ok(())
    }

    pub fn is_exist(&self, path: &str) -> Result<bool, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(_value)) => Ok(true),
            Ok(None) => Ok(false),
            Err(_) => Err(EngineError::IO),
        }
    }

    pub fn create_directory(&self, path: &str, _mode: Mode) -> Result<Vec<u8>, EngineError> {
        self.dir_db.db.put(path.as_bytes(), 2_usize.to_le_bytes())?;
        let attr = FileAttrSimple::new(fuser::FileType::Directory);
        self.put_file_attr(path, attr)
    }

    pub fn delete_directory(&self, path: &str) -> Result<(), EngineError> {
        if let Some(value) = self.dir_db.db.get(path.as_bytes())? {
            if usize::from_le_bytes(value.as_slice().try_into().unwrap()) > 2 {
                return Err(EngineError::NotEmpty);
            }
        }
        self.delete_file_attr(path)?;
        self.dir_db.db.delete(path)?;
        Ok(())
    }

    pub fn read_directory(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
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

        let mut offset = offset;
        match self.dir_db.db.get(path.as_bytes())? {
            Some(value) => {
                let mut result = Vec::with_capacity(size as usize);
                let mut l = usize::from_le_bytes(value.as_slice().try_into().unwrap());
                let mut total = 0;
                for item in self.dir_db.db.iterator(IteratorMode::From(
                    format!("{}-", path).as_bytes(),
                    rocksdb::Direction::Forward,
                )) {
                    if l == 2 {
                        break;
                    }
                    if offset > 0 {
                        offset -= 1;
                        l -= 1;
                        continue;
                    }
                    let (key, value) = item.unwrap();
                    let ty = match key.last().unwrap() {
                        b'f' => DT_REG,
                        b'd' => DT_DIR,
                        b's' => DT_LNK,
                        _ => DT_REG,
                    };
                    let rec_len = value.len() + 3;
                    total += rec_len;
                    if total > size as usize {
                        break;
                    }
                    result.put_u8(ty);
                    result.put(value.len().to_le_bytes().as_ref());
                    result.put(value.as_ref());
                    l -= 1;
                }
                Ok(result)
            }
            None => Err(EngineError::IO),
        }
    }

    pub fn directory_add_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), EngineError> {
        let ft = match file_type {
            3 => "d",
            _ => "f",
        };
        match self.dir_db.db.get(parent_dir)? {
            Some(value) => {
                let len = usize::from_le_bytes(value.as_slice().try_into().unwrap());
                self.dir_db.db.put(parent_dir, (len + 1).to_le_bytes())?;
                self.dir_db
                    .db
                    .put(format!("{}-{}-{}", parent_dir, file_name, ft), file_name)?;
            }
            None => {
                return Err(EngineError::NoEntry);
            }
        }
        Ok(())
    }

    pub fn directory_delete_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), EngineError> {
        if let Some(value) = self.dir_db.db.get(parent_dir.as_bytes())? {
            let ft = match file_type {
                3 => "d",
                _ => "f",
            };
            let len = usize::from_le_bytes(value.as_slice().try_into().unwrap());
            self.dir_db
                .db
                .put(parent_dir.as_bytes(), (len - 1).to_le_bytes())?;
            self.dir_db
                .db
                .delete(format!("{}-{}-{}", parent_dir, file_name, ft))?;
        }
        Ok(())
    }

    pub fn delete_from_parent(&self, path: &str, file_type: u8) -> Result<(), EngineError> {
        let (parent, name) = path_split(path.to_string()).unwrap();
        let ft = match file_type {
            3 => "d",
            _ => "f",
        };
        if let Some(value) = self.dir_db.db.get(parent.as_bytes())? {
            let l = usize::from_le_bytes(value.as_slice().try_into().unwrap());
            self.dir_db.db.put(&parent, (l - 1).to_le_bytes())?;
            self.dir_db
                .db
                .delete(format!("{}-{}-{}", parent, name, ft))?;
        }
        Ok(())
    }

    // fn delete_dir_recursive(&self, path: String) -> Result<(), EngineError> {
    //     match self.dir_db.db.get(path.as_bytes())? {
    //         Some(value) => {
    //             let sub_dirs = bincode::deserialize::<SubDirectory>(&value[..]).unwrap();
    //             if sub_dirs == SubDirectory::new() {
    //                 self.dir_db.db.delete(path.as_bytes())?;
    //                 return Ok(());
    //             }
    //             for sub_dir in sub_dirs.sub_dir {
    //                 if sub_dir.0 == "." || sub_dir.0 == ".." || sub_dir.0.is_empty() {
    //                     continue;
    //                 }
    //                 let p = format!("{}/{}", path, sub_dir.0);
    //                 if sub_dir.1.eq("f") {
    //                     self.file_attr_db.db.delete(p.as_bytes())?;
    //                     let local_file = match self.file_db.db.get(p.as_bytes())? {
    //                         Some(value) => String::from_utf8(value).unwrap(),
    //                         None => {
    //                             return Err(EngineError::NoEntry);
    //                         }
    //                     };
    //                     unistd::unlink(local_file.as_str())?;
    //                     self.file_db.db.delete(p.as_bytes())?;
    //                     continue;
    //                 }
    //                 self.delete_dir_recursive(p)?;
    //             }
    //             self.dir_db.db.delete(path.as_bytes())?;
    //         }
    //         None => {
    //             self.dir_db.db.delete(path.as_bytes())?;
    //             return Ok(());
    //         }
    //     }
    //     Ok(())
    // }

    pub fn put_file_attr(&self, path: &str, attr: FileAttrSimple) -> Result<Vec<u8>, EngineError> {
        let value = bincode::serialize(&attr).map_err(|_e| EngineError::IO)?;
        self.file_attr_db.db.put(path, &value)?;
        Ok(value)
    }

    pub fn get_file_attr(&self, path: &str) -> Result<FileAttrSimple, EngineError> {
        match self.file_attr_db.db.get(path)? {
            Some(value) => {
                bincode::deserialize::<FileAttrSimple>(&value).map_err(|_e| EngineError::IO)
            }
            None => Err(EngineError::NoEntry),
        }
    }

    pub fn get_file_attr_raw(&self, path: &str) -> Result<Vec<u8>, EngineError> {
        self.file_attr_db
            .db
            .get(path)
            .map_err(|_e| EngineError::IO)
            .map(|v| match v {
                Some(v) => Ok(v),
                None => Err(EngineError::NoEntry),
            })?
    }

    pub fn delete_file_attr(&self, path: &str) -> Result<(), EngineError> {
        self.file_attr_db.db.delete(path.as_bytes())?;
        Ok(())
    }

    pub fn is_dir(&self, path: &str) -> Result<bool, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(value)) => {
                match bincode::deserialize::<FileAttrSimple>(&value) {
                    Ok(file_attr) => {
                        // fuser::FileType::RegularFile
                        if file_attr.kind != 4 {
                            Ok(true) // not a file
                        } else {
                            Ok(false)
                        }
                    }
                    Err(e) => {
                        error!("deserialize error: {:?}", e);
                        Err(EngineError::IO)
                    }
                }
            }
            Ok(None) => {
                debug!("read_file path: {}, no entry", path);
                Err(EngineError::NoEntry)
            }
            Err(_) => {
                debug!("read_file path: {}, io error", path);
                Err(EngineError::IO)
            }
        }
    }

    pub fn check_dir(&self) {
        #[cfg(feature = "disk-db")]
        for item in self.dir_db.db.iterator(IteratorMode::End) {
            let (key, _value) = item.unwrap();
            if !self.file_attr_db.db.key_may_exist(&key) {
                let _ = self.dir_db.db.delete(&key);
            }
        }
    }

    pub fn check_file(&self, file_name: &str) -> bool {
        let mut file_str = String::new();
        if self.file_db.db.key_may_exist(file_name) {
            let file_vec = self.file_db.db.get(file_name).unwrap().unwrap();
            file_str = String::from_utf8(file_vec).unwrap();
            if self.file_attr_db.db.key_may_exist(&file_str) {
                return true;
            }
            let _ = self.file_db.db.delete(file_name);
        }
        if self.file_attr_db.db.key_may_exist(&file_str) {
            let _ = self.file_attr_db.db.delete(file_str);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use nix::sys::stat::Mode;

    use crate::server::storage_engine::meta_engine::MetaEngine;

    #[test]
    fn test_create_delete_dir() {
        let db_path = "/tmp/test_dir_db";
        {
            let engine = MetaEngine::new(db_path);
            engine.init();
            engine.directory_add_entry("/", "a", 3).unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a", mode).unwrap();
            let v = engine.dir_db.db.get("/a").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(2, l);
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(3, l);
            engine.directory_delete_entry("/", "a", 3).unwrap();
            engine.delete_directory("/a").unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a").unwrap());
            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(2, l);
        }

        {
            let engine = MetaEngine::new(db_path);
            engine.init();
            engine.directory_add_entry("/", "a1", 3).unwrap();
            let mode = Mode::S_IRUSR
                | Mode::S_IWUSR
                | Mode::S_IRGRP
                | Mode::S_IWGRP
                | Mode::S_IROTH
                | Mode::S_IWOTH;
            engine.create_directory("/a1", mode).unwrap();
            let v = engine.dir_db.db.get("/a1").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(2, l);

            engine.directory_add_entry("/a1", "a2", 3).unwrap();
            engine.create_directory("/a1/a2", mode).unwrap();
            let v = engine.dir_db.db.get("/a1").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(3, l);
            engine.delete_directory("/a1/a2").unwrap();
            engine.delete_from_parent("/a1/a2", 3).unwrap();
            engine.delete_directory("/a1").unwrap();
            engine.delete_from_parent("/a1", 3).unwrap();

            engine.directory_add_entry("/", "a3", 3).unwrap();
            engine.create_directory("/a3", mode).unwrap();
            let v = engine.dir_db.db.get("/a3").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(2, l);
            engine.directory_delete_entry("/", "a3", 3).unwrap();
            engine.delete_directory("/a3").unwrap();
            assert_eq!(None, engine.dir_db.db.get("/a3").unwrap());

            let v = engine.dir_db.db.get("/").unwrap().unwrap();
            let l = usize::from_le_bytes(v.as_slice().try_into().unwrap());
            assert_eq!(2, l);
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
