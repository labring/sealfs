use std::sync::atomic::{AtomicU32, Ordering};

use bytes::BufMut;
use dashmap::DashMap;
use fuser::{FileAttr, FileType};
use libc::{DT_DIR, DT_LNK, DT_REG};
use log::{debug, error, info};
#[cfg(feature = "mem-db")]
use pegasusdb::DB;
use rocksdb::{BlockBasedOptions, WriteBatch};
#[cfg(feature = "disk-db")]
use rocksdb::{Cache, IteratorMode, Options, DB};

use crate::{
    common::{
        errors::{DATABASE_ERROR, SERIALIZATION_ERROR},
        serialization::{bytes_as_file_attr, file_attr_as_bytes, FileTypeSimple, Volume},
        util::empty_dir,
    },
    server::path_split,
};

const INIT_SUB_FILES_NUM: u32 = 2;

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

pub struct FileIndex {
    pub file_attr: FileAttr,
    pub status: u32,
    pub sub_files_num: AtomicU32,
}

pub struct MetaEngine {
    pub file_db: Database,
    pub dir_db: Database,
    pub file_attr_db: Database,
    pub file_indexs: DashMap<String, FileIndex>,
    pub volumes: DashMap<String, Volume>,
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
            file_indexs: DashMap::new(),
            volumes: DashMap::new(),
        }
    }

    pub fn init(&self) {
        for file_name in self.file_attr_db.db.iterator(IteratorMode::Start) {
            let (k, v) = file_name.unwrap();
            let k = String::from_utf8(k.to_vec()).unwrap();
            let attr = unsafe { bytes_as_file_attr(&v) };
            let file_type = attr.kind;
            match file_type {
                FileType::RegularFile => {
                    // RegularFile
                    self.file_indexs.insert(
                        k,
                        FileIndex {
                            file_attr: attr.clone(),
                            status: 0,
                            sub_files_num: AtomicU32::new(0),
                        },
                    );
                }
                FileType::Directory => {
                    // Directory
                    self.file_indexs.insert(
                        k.clone(),
                        FileIndex {
                            file_attr: attr.clone(),
                            status: 0,
                            sub_files_num: AtomicU32::new(INIT_SUB_FILES_NUM),
                        },
                    );
                    if !k.contains('/') {
                        self.volumes.insert(
                            k.clone(),
                            Volume {
                                name: k,
                                size: 10000000,
                                used_size: 0,
                            },
                        );
                    }
                }
                _ => {}
            }
        }

        for dir_name in self.dir_db.db.iterator(IteratorMode::Start) {
            let sub_dir_info = String::from_utf8(dir_name.unwrap().0.to_vec()).unwrap();
            let list = sub_dir_info.split('$').collect::<Vec<&str>>();
            info!("list: {:?}", list);
            let file_index = self
                .file_indexs
                .get(list.first().unwrap().to_owned())
                .unwrap();
            file_index.sub_files_num.fetch_add(1, Ordering::Relaxed);
            info!(
                "file_index.sub_files_num: {:}",
                file_index.sub_files_num.load(Ordering::Relaxed)
            );
        }
    }

    pub fn get_file_map(&self) -> Result<Vec<String>, i32> {
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

    pub fn create_file(&self, file_attr: FileAttr, loacl_file_name: &str, path: &str) -> Result<Vec<u8>, i32> {
        let value = self.put_file_attr(path, &file_attr)?;
        match self.file_indexs.insert(
            path.to_string(),
            FileIndex {
                file_attr,
                status: 0,
                sub_files_num: AtomicU32::new(INIT_SUB_FILES_NUM),
            },
        ) {
            Some(_) => Err(libc::EEXIST),
            None => match self.file_db.db.put(loacl_file_name, path) {
                Ok(_) => Ok(value),
                Err(e) => {
                    error!("put file error: {}", e);
                    Err(DATABASE_ERROR)
                }
            },
        }
    }

    pub fn delete_file(&self, local_file_name: &str, path: &str) -> Result<(), i32> {
        match self.file_indexs.remove(path) {
            Some(_) => match self.file_db.db.delete(local_file_name) {
                Ok(_) => {
                    self.delete_file_attr(path)?;
                    Ok(())
                }
                Err(e) => {
                    error!("delete file error: {}", e);
                    Err(DATABASE_ERROR)
                }
            },
            None => Err(libc::ENOENT),
        }
    }

    pub fn is_exist(&self, path: &str) -> Result<bool, i32> {
        match self.file_indexs.get(path) {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    // this function does not need to be thread safe
    pub fn create_directory(&self, path: &str, _mode: u32) -> Result<Vec<u8>, i32> {
        match self.file_indexs.insert(
                    path.to_owned(),
                    FileIndex {
                        file_attr: empty_dir(),
                        status: 0,
                        sub_files_num: AtomicU32::new(INIT_SUB_FILES_NUM),
                    },
                ) {
            Some(_) => Err(libc::EEXIST),
            None => {
                let attr = empty_dir();
                self.put_file_attr(path, &attr)
            }
        }
    }

    // this function does not need to be thread safe
    pub fn delete_directory(&self, path: &str) -> Result<(), i32> {
        match self.file_indexs.get(path) {
            Some(value) => {
                if value.sub_files_num.load(Ordering::Relaxed) > INIT_SUB_FILES_NUM {
                    Err(libc::ENOTEMPTY)
                } else {
                    drop(value);
                    self.file_indexs.remove(path).unwrap();
                    self.delete_file_attr(path)
                }
            }
            None => Err(libc::ENOENT),
        }
    }

    pub fn delete_directory_force(&self, path: &str) -> Result<(), i32> {
        if self.file_indexs.remove(path).is_none() {
            return Err(libc::ENOENT);
        }

        // delete sub file index in dir_db with prefix "path_"
        let (start_key, end_key) = (path.to_owned() + "$", path.to_owned() + "$~");
        let mut batch = WriteBatch::default();
        batch.delete_range(start_key, end_key);
        match self.dir_db.db.write(batch) {
            Ok(_) => {}
            Err(e) => {
                error!("delete directory force error: {}", e);
                return Err(DATABASE_ERROR);
            }
        }

        self.delete_file_attr(path)
    }

    pub fn read_directory(&self, path: &str, size: u32, offset: i64) -> Result<Vec<u8>, i32> {
        match self.file_indexs.get(path) {
            Some(value) => {
                if value.file_attr.kind != FileType::Directory {
                    return Err(libc::ENOTDIR);
                }
            }
            None => return Err(libc::ENOENT),
        }

        let mut offset = offset;

        // TODO: optimize the situation while offset is not 0

        let mut index_num = match self.file_indexs.get(path) {
            Some(value) => value.sub_files_num.load(Ordering::Relaxed), //maybe better hold a lock
            None => {
                return Err(libc::ENOENT);
            }
        };

        info!(
            "read directory: {}, size: {}, offset: {}, index_num: {}",
            path, size, offset, index_num
        );

        let mut result = Vec::with_capacity(size as usize);
        let mut total = 0;
        for item in self.dir_db.db.iterator(IteratorMode::From(
            format!("{}$", path).as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            info!("item: {:?}", item);
            if index_num == INIT_SUB_FILES_NUM {
                break;
            }
            if offset > 0 {
                offset -= 1;
                index_num -= 1;
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
                        return Err(SERIALIZATION_ERROR);
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
            index_num -= 1;
        }
        Ok(result)
    }

    pub fn directory_add_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), i32> {
        match self.file_indexs.get(parent_dir) {
            Some(value) => {
                if value.file_attr.kind != FileType::Directory {
                    return Err(libc::ENOTDIR);
                }
                match self.dir_db.db.put(
                    format!("{}${}${}", parent_dir, file_name, file_type as char),
                    file_name,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("directory add entry error: {}", e);
                        return Err(DATABASE_ERROR);
                    }
                }
                value.sub_files_num.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            None => {
                error!("directory add entry error: {}", libc::ENOENT);
                Err(libc::ENOENT)
            }
        }
    }

    pub fn directory_delete_entry(
        &self,
        parent_dir: &str,
        file_name: &str,
        file_type: u8,
    ) -> Result<(), i32> {
        match self.file_indexs.get(parent_dir) {
            Some(value) => {
                if value.file_attr.kind != FileType::Directory {
                    return Err(libc::ENOTDIR);
                }
                match self.dir_db.db.delete(format!(
                    "{}${}${}",
                    parent_dir, file_name, file_type as char
                )) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("directory delete entry error: {}", e);
                        return Err(DATABASE_ERROR);
                    }
                }
                //assert!(value.sub_files_num > INIT_SUB_FILES_NUM);
                value.sub_files_num.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }
            None => {
                error!("directory delete entry error: {}", libc::ENOENT);
                Err(libc::ENOENT)
            }
        }
    }

    pub fn delete_from_parent(&self, path: &str, file_type: u8) -> Result<(), i32> {
        let (parent, name) = path_split(path).unwrap();
        match self.file_indexs.get(&parent) {
            Some(value) => {
                if let Err(e) = self
                    .dir_db
                    .db
                    .delete(format!("{}${}${}", parent, name, file_type as char))
                {
                    error!("delete from parent error: {}", e);
                    return Err(DATABASE_ERROR);
                }
                value.sub_files_num.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }
            None => Err(libc::ENOENT),
        }
    }

    pub fn put_file_attr(&self, path: &str, attr: &FileAttr) -> Result<Vec<u8>, i32> {
        let value = unsafe { file_attr_as_bytes(attr) }.to_vec();
        match self.file_attr_db.db.put(path, &value) {
            Ok(_) => Ok(value),
            Err(e) => {
                error!("put_file_attr error: {}", e);
                Err(DATABASE_ERROR)
            }
        }
    }

    pub fn update_size(&self, path: &str, size: u64) -> Result<(), i32> {
        match self.file_indexs.get_mut(path) {
            Some(mut value) => {
                if value.file_attr.size >= size {
                    return Ok(());
                }
                value.file_attr.size = size;
                match self.put_file_attr(path, &value.file_attr) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            None => Err(libc::ENOENT),
        }
    }

    pub fn get_file_attr(&self, path: &str) -> Result<FileAttr, i32> {
        match self.file_indexs.get(path) {
            Some(value) => Ok(value.file_attr.clone()),
            None => Err(libc::ENOENT),
        }
    }

    pub fn get_file_attr_raw(&self, path: &str) -> Result<Vec<u8>, i32> {
        match self.file_indexs.get(path) {
            Some(value) => Ok(unsafe { file_attr_as_bytes(&value.file_attr) }.to_vec()),
            None => Err(libc::ENOENT),
        }
    }

    pub fn complete_transfer_file(&self, path: &str, file_attr: &FileAttr) -> Result<(), i32> {
        let value = unsafe { file_attr_as_bytes(file_attr) };
        match self.file_attr_db.db.put(path, value) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("complete_transfer_file error: {}", e);
                Err(DATABASE_ERROR)
            }
        }
    }

    pub fn delete_file_attr(&self, path: &str) -> Result<(), i32> {
        match self.file_attr_db.db.delete(path.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("delete_file_attr error: {}", e);
                Err(DATABASE_ERROR)
            }
        }
    }

    pub fn create_volume(&self, name: &str) -> Result<(), i32> {
        if self.volumes.contains_key(name) {
            return Err(libc::EEXIST);
        }
        self.volumes.insert(
            name.to_owned(),
            Volume {
                name: name.to_owned(),
                size: 100000000,
                used_size: 0,
            },
        );
        match self.create_directory(name, 0o755) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn list_volumes(&self) -> Result<Vec<u8>, i32> {
        let mut volumes = Vec::new();
        for kv in self.volumes.iter() {
            volumes.push((*kv).clone());
        }
        Ok(bincode::serialize(&volumes).unwrap())
    }

    pub fn init_volume(&self, name: &str) -> Result<(), i32> {
        if !self.volumes.contains_key(name) {
            return Err(libc::ENOENT);
        }
        Ok(())
    }

    // make sure the volume is empty
    pub fn delete_volume(&self, name: &str) -> Result<(), i32> {
        if !self.volumes.contains_key(name) {
            return Err(libc::ENOENT);
        }
        self.volumes.remove(name);
        match self.delete_directory_force(name) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn is_dir(&self, path: &str) -> Result<bool, i32> {
        match self.file_indexs.get(path) {
            Some(value) => {
                if value.file_attr.kind == FileType::Directory {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            None => {
                debug!("is_dir path: {}, no entry", path);
                Err(libc::ENOENT)
            }
        }
    }

    pub fn check_dir(&self) {
        #[cfg(feature = "disk-db")]
        for item in self.dir_db.db.iterator(IteratorMode::End) {
            let (key, _value) = item.unwrap();
            let key = String::from_utf8(key.to_vec()).unwrap();
            if !self
                .file_attr_db
                .db
                .key_may_exist(key.split('$').next().unwrap())
            {
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

    use std::sync::atomic::Ordering;

    use libc::mode_t;

    use crate::server::storage_engine::meta_engine::{MetaEngine, INIT_SUB_FILES_NUM};

    #[test]
    fn test_create_delete_dir() {
        let db_path = "/tmp/test_dir_db";
        {
            let engine = MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024);
            engine.init();
            engine.create_directory("test1", 0o777).unwrap();
            engine.directory_add_entry("test1", "a", 3).unwrap();
            let mode: mode_t = 0o777;
            engine.create_directory("test1/a", mode).unwrap();
            let l = engine
                .file_indexs
                .get("test1/a")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM, l);
            let l = engine
                .file_indexs
                .get("test1")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM + 1, l);
            engine.directory_delete_entry("test1", "a", 3).unwrap();
            engine.delete_directory("test1/a").unwrap();
            assert_eq!(engine.file_indexs.get("test1/a").is_none(), true);
            let l = engine
                .file_indexs
                .get("test1")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM, l);
            engine.delete_directory("test1").unwrap();
        }

        {
            let engine = MetaEngine::new(db_path, 128 << 20, 128 * 1024 * 1024);
            engine.init();
            engine.create_directory("test1", 0o777).unwrap();
            engine.directory_add_entry("test1", "a1", 3).unwrap();
            let mode: mode_t = 0o777;
            engine.create_directory("test1/a1", mode).unwrap();
            let l = engine
                .file_indexs
                .get("test1/a1")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM, l);

            engine.directory_add_entry("test1/a1", "a2", 3).unwrap();
            engine.create_directory("test1/a1/a2", mode).unwrap();
            let l = engine
                .file_indexs
                .get("test1/a1")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM + 1, l);
            engine.delete_directory("test1/a1/a2").unwrap();
            engine.delete_from_parent("test1/a1/a2", 3).unwrap();
            engine.delete_directory("test1/a1").unwrap();
            engine.delete_from_parent("test1/a1", 3).unwrap();

            engine.directory_add_entry("test1", "a3", 3).unwrap();
            engine.create_directory("test1/a3", mode).unwrap();
            let l = engine
                .file_indexs
                .get("test1/a3")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM, l);
            engine.directory_delete_entry("test1", "a3", 3).unwrap();
            engine.delete_directory("test1/a3").unwrap();
            assert_eq!(engine.file_indexs.get("test1/a3").is_none(), true);

            let l = engine
                .file_indexs
                .get("test1")
                .unwrap()
                .sub_files_num
                .load(Ordering::SeqCst);
            assert_eq!(INIT_SUB_FILES_NUM, l);
            engine.delete_directory("test1").unwrap();
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
