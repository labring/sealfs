use bytes::BufMut;
use libc::{DT_DIR, DT_LNK, DT_REG};
use log::{debug, error};
#[cfg(feature = "mem-db")]
use pegasusdb::DB;
use rocksdb::BlockBasedOptions;
#[cfg(feature = "disk-db")]
use rocksdb::{Cache, IteratorMode, Options, DB};

use crate::{
    common::serialization::{FileAttrSimple, FileTypeSimple},
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
    pub fn new(
        db_path: &str,
        #[cfg(feature = "disk-db")] cache_capacity: usize,
        #[cfg(feature = "disk-db")] write_buffer_size: usize,
    ) -> Self {
        #[cfg(feature = "disk-db")]
        let (file_db, dir_db, file_attr_db) = {
            let file_db = {
                let mut db_opts = Options::default();
                let mut block_opts = BlockBasedOptions::default();
                let cache = Cache::new_lru_cache(cache_capacity).unwrap();
                block_opts.set_block_cache(&cache);
                db_opts.set_block_based_table_factory(&block_opts);
                db_opts.set_write_buffer_size(write_buffer_size);
                db_opts.create_if_missing(true);
                let path = format!("{}_file", db_path);
                let db = match DB::open(&db_opts, path.as_str()) {
                    Ok(db) => db,
                    Err(e) => panic!("{}", e),
                };
                Database { db, db_opts, path }
            };

            let dir_db = {
                let mut db_opts = Options::default();
                let mut block_opts = BlockBasedOptions::default();
                let cache = Cache::new_lru_cache(cache_capacity).unwrap();
                block_opts.set_block_cache(&cache);
                db_opts.set_block_based_table_factory(&block_opts);
                db_opts.set_write_buffer_size(write_buffer_size);
                db_opts.create_if_missing(true);
                let path = format!("{}_dir", db_path);
                let db = match DB::open(&db_opts, path.as_str()) {
                    Ok(db) => db,
                    Err(e) => panic!("{}", e),
                };
                Database { db, db_opts, path }
            };

            let file_attr_db = {
                let mut db_opts = Options::default();
                let mut block_opts = BlockBasedOptions::default();
                let cache = Cache::new_lru_cache(cache_capacity).unwrap();
                block_opts.set_block_cache(&cache);
                db_opts.set_block_based_table_factory(&block_opts);
                db_opts.set_write_buffer_size(write_buffer_size);
                db_opts.create_if_missing(true);
                let path = format!("{}_file_attr", db_path);
                let db = match DB::open(&db_opts, path.as_str()) {
                    Ok(db) => db,
                    Err(e) => panic!("{}", e),
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
            let file_attr = FileAttrSimple::new(FileTypeSimple::Directory);
            let _ = self
                .file_attr_db
                .db
                .put(b"/", bincode::serialize(&file_attr).unwrap());
        }
        if !self.file_db.db.key_may_exist(b"/") {
            let _ = self.file_db.db.put(b"/", b"");
        }
    }

    pub fn get_file_map(&self) -> Result<Vec<String>, EngineError> {
        let mut file_map = Vec::new();
        self.file_attr_db
            .db
            .iterator(IteratorMode::Start)
            .for_each(|result| {
                let (k, _) = result.unwrap();
                let k = String::from_utf8(k.to_vec()).unwrap();
                file_map.push(k);
            });
        Ok(file_map)
    }

    pub fn put_file(&self, loacl_file_name: &str, path: &str) -> Result<(), EngineError> {
        match self.file_db.db.put(loacl_file_name, path) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("put file error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn delete_file(&self, local_file_name: &str) -> Result<(), EngineError> {
        match self.file_db.db.delete(local_file_name) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("delete file error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn is_exist(&self, path: &str) -> Result<bool, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            Ok(Some(_value)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => {
                error!("is exist error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn create_directory(&self, path: &str, _mode: u32) -> Result<Vec<u8>, EngineError> {
        match self.dir_db.db.put(path.as_bytes(), 2_usize.to_le_bytes()) {
            Ok(_) => {
                let attr = FileAttrSimple::new(FileTypeSimple::Directory);
                self.put_file_attr(path, attr)
            }
            Err(e) => {
                error!("create directory error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn delete_directory(&self, path: &str) -> Result<(), EngineError> {
        match self.dir_db.db.get(path.as_bytes()) {
            Ok(Some(value)) => {
                if usize::from_le_bytes(value.as_slice().try_into().unwrap()) > 2 {
                    return Err(EngineError::NotEmpty);
                }
            }
            Ok(None) => return Err(EngineError::NoEntry),
            Err(e) => {
                error!("delete directory error: {}", e);
                return Err(EngineError::IO);
            }
        };
        self.delete_file_attr(path)?;
        match self.dir_db.db.delete(path) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("delete directory error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn read_directory(
        &self,
        path: &str,
        size: u32,
        offset: i64,
    ) -> Result<Vec<u8>, EngineError> {
        match self.file_attr_db.db.get(path.as_bytes()) {
            // debug!("read_dir getting attr, path: {}, value: {:?}", path, value);
            Ok(Some(value)) => {
                match bincode::deserialize::<FileAttrSimple>(&value) {
                    Ok(file_attr) => {
                        // fuser::FileType::Directory
                        if file_attr.kind != 3 {
                            return Err(EngineError::NotDir);
                        }
                    }
                    Err(e) => {
                        error!("read directory error: {}", e);
                        return Err(EngineError::IO);
                    }
                }
            }
            Ok(None) => return Err(EngineError::NoEntry),
            Err(e) => {
                error!("read directory error: {}", e);
                return Err(EngineError::IO);
            }
        }

        let mut offset = offset;
        match self.dir_db.db.get(path.as_bytes()) {
            Ok(Some(value)) => {
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
                    let ty = {
                        match (*key.last().unwrap()).try_into() {
                            Ok(FileTypeSimple::RegularFile) => DT_REG,
                            Ok(FileTypeSimple::Directory) => DT_DIR,
                            Ok(FileTypeSimple::Symlink) => DT_LNK,
                            Ok(_) => DT_REG,
                            Err(e) => {
                                error!(
                                    "read directory error: {}, path: {}, key as string: {}",
                                    e,
                                    path,
                                    String::from_utf8(key.to_vec()).unwrap()
                                );
                                return Err(EngineError::IO);
                            }
                        }
                    };
                    let rec_len = value.len() + 3;
                    total += rec_len;
                    if total > size as usize {
                        break;
                    }
                    result.put_u8(ty);
                    result.put((value.len() as u16).to_le_bytes().as_ref());
                    result.put(value.as_ref());
                    l -= 1;
                }
                Ok(result)
            }
            Ok(None) => Err(EngineError::IO),
            Err(e) => {
                error!("read directory error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn directory_add_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), EngineError> {
        match self.dir_db.db.get(parent_dir) {
            Ok(Some(value)) => {
                let len = usize::from_le_bytes(value.as_slice().try_into().unwrap());
                match self.dir_db.db.put(parent_dir, (len + 1).to_le_bytes()) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("directory add entry error: {}", e);
                        return Err(EngineError::IO);
                    }
                }
                match self.dir_db.db.put(
                    format!("{}-{}-{}", parent_dir, file_name, file_type as char),
                    file_name,
                ) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("directory add entry error: {}", e);
                        Err(EngineError::IO)
                    }
                }
            }
            Ok(None) => Err(EngineError::NoEntry),
            Err(e) => {
                error!("directory add entry error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn directory_delete_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), EngineError> {
        match self.dir_db.db.get(parent_dir.as_bytes()) {
            Ok(Some(value)) => {
                let len = usize::from_le_bytes(value.as_slice().try_into().unwrap());
                match self
                    .dir_db
                    .db
                    .put(parent_dir.as_bytes(), (len - 1).to_le_bytes())
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("directory delete entry error: {}", e);
                        return Err(EngineError::IO);
                    }
                }
                match self.dir_db.db.delete(format!(
                    "{}-{}-{}",
                    parent_dir, file_name, file_type as char
                )) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("directory delete entry error: {}", e);
                        Err(EngineError::IO)
                    }
                }
            }
            Ok(None) => Err(EngineError::NoEntry),
            Err(e) => {
                error!("directory delete entry error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn delete_from_parent(&self, path: &str, file_type: u8) -> Result<(), EngineError> {
        let (parent, name) = path_split(path).unwrap();
        match self.dir_db.db.get(parent.as_bytes()) {
            Ok(Some(value)) => {
                let l = usize::from_le_bytes(value.as_slice().try_into().unwrap());
                match self.dir_db.db.put(&parent, (l - 1).to_le_bytes()) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("delete from parent error: {}", e);
                        return Err(EngineError::IO);
                    }
                }
                match self
                    .dir_db
                    .db
                    .delete(format!("{}-{}-{}", parent, name, file_type as char))
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("delete from parent error: {}", e);
                        Err(EngineError::IO)
                    }
                }
            }
            Ok(None) => Err(EngineError::NoEntry),
            Err(e) => {
                error!("delete from parent error: {}", e);
                Err(EngineError::IO)
            }
        }
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
        let value = match bincode::serialize(&attr).map_err(|_e| EngineError::IO) {
            Ok(v) => v,
            Err(e) => {
                error!("put_file_attr error: {}", e);
                return Err(EngineError::IO);
            }
        };
        match self.file_attr_db.db.put(path, &value) {
            Ok(_) => Ok(value),
            Err(e) => {
                error!("put_file_attr error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn get_file_attr(&self, path: &str) -> Result<FileAttrSimple, EngineError> {
        match self.file_attr_db.db.get(path) {
            Ok(Some(value)) => {
                bincode::deserialize::<FileAttrSimple>(&value).map_err(|_e| EngineError::IO)
            }
            Ok(None) => Err(EngineError::NoEntry),
            Err(e) => {
                error!("get_file_attr error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn get_file_attr_raw(&self, path: &str) -> Result<Vec<u8>, EngineError> {
        match self
            .file_attr_db
            .db
            .get(path)
            .map_err(|_e| EngineError::IO)
            .map(|v| match v {
                Some(v) => Ok(v),
                None => Err(EngineError::NoEntry),
            }) {
            Ok(v) => v,
            Err(e) => {
                error!("get_file_attr_raw error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn complete_transfer_file(
        &self,
        path: &str,
        file_attr: FileAttrSimple,
    ) -> Result<(), EngineError> {
        let value = match bincode::serialize(&file_attr).map_err(|_e| EngineError::IO) {
            Ok(v) => v,
            Err(e) => {
                error!("complete_transfer_file error: {}", e);
                return Err(EngineError::IO);
            }
        };
        match self.file_attr_db.db.put(path, value) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("complete_transfer_file error: {}", e);
                Err(EngineError::IO)
            }
        }
    }

    pub fn delete_file_attr(&self, path: &str) -> Result<(), EngineError> {
        match self.file_attr_db.db.delete(path.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("delete_file_attr error: {}", e);
                Err(EngineError::IO)
            }
        }
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
            Err(e) => {
                error!("read_file path: {}, io error: {:?}", path, e);
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

    use libc::mode_t;

    use crate::server::storage_engine::meta_engine::MetaEngine;

    #[test]
    fn test_create_delete_dir() {
        let db_path = "/tmp/test_dir_db";
        {
            let engine = MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024);
            engine.init();
            engine.directory_add_entry("/", "a", 3).unwrap();
            let mode: mode_t = 0o777;
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
            let engine = MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024);
            engine.init();
            engine.directory_add_entry("/", "a1", 3).unwrap();
            let mode: mode_t = 0o777;
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
