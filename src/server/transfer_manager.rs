use std::collections::HashMap;

use dashmap::DashMap;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct LockPool {
    locks: HashMap<String, RwLock<()>>,
}

pub struct TransferManager {
    transferring_locks: *const LockPool,
    transferring_status: DashMap<String, bool>,
}

unsafe impl std::marker::Sync for TransferManager {}
unsafe impl std::marker::Send for TransferManager {}

impl Default for TransferManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransferManager {
    pub fn new() -> Self {
        TransferManager {
            transferring_locks: Box::into_raw(Box::new(LockPool {
                locks: HashMap::new(),
            })),
            transferring_status: DashMap::new(),
        }
    }

    pub fn get_lock(&self, path: &str) -> &RwLock<()> {
        unsafe { (*self.transferring_locks).locks.get(path).unwrap() }
    }

    pub fn make_up_files(&self, paths: &Vec<String>) {
        self.transferring_status.clear();
        let transferring_locks = unsafe { &mut *(self.transferring_locks as *mut LockPool) };
        transferring_locks.locks.clear();
        for path in paths {
            transferring_locks
                .locks
                .insert(path.clone(), RwLock::new(()));
            self.transferring_status.insert(path.clone(), false);
        }
    }

    pub async fn get_rlock(&self, path: &str) -> RwLockReadGuard<'_, ()> {
        let lock = self.get_lock(path);
        lock.read().await
    }

    pub async fn get_wlock(&self, path: &str) -> RwLockWriteGuard<'_, ()> {
        let lock = self.get_lock(path);
        lock.write().await
    }

    pub fn status(&self, path: &str) -> Option<bool> {
        self.transferring_status.get(path).map(|status| *status)
    }

    pub fn set_status(&self, path: &str, status: bool) {
        self.transferring_status.insert(path.to_string(), status);
    }
}
