use std::{ptr::drop_in_place, sync::{Arc, atomic::{AtomicI32, AtomicU64}}, time::Duration};

use crate::rpc::protocol::REQUEST_POOL_SIZE;
use chrono::Utc;
use kanal;
use log::debug;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct OperationCallback {
    pub data: *const u8,
    pub meta_data: *const u8,
    pub data_length: usize,
    pub meta_data_length: usize,
    pub request_status: libc::c_int,
    pub flags: u32,
    pub receiver: *mut Receiver<u64>,
}

unsafe impl std::marker::Sync for OperationCallback {}
unsafe impl std::marker::Send for OperationCallback {}

impl Default for OperationCallback {
    fn default() -> Self {
        let (_, receiver) = channel(1);
        Self::new(receiver)
    }
}

impl OperationCallback {
    pub fn new(receiver: Receiver<u64>) -> Self {
        Self {
            data: std::ptr::null(),
            meta_data: std::ptr::null(),
            data_length: 0,
            meta_data_length: 0,
            request_status: 0,
            flags: 0,
            receiver: Box::into_raw(Box::new(receiver)),
        }
    }

    pub fn get_receiver(&self) -> &mut Receiver<u64> {
        unsafe { &mut *self.receiver }
    }
}

pub struct CallbackPool {
    callbacks: Vec<*const OperationCallback>,
    status: Vec<Arc<AtomicI32>>,  // 0: success, 1: pending, 2: timeout
    uids: Vec<AtomicU64>,
    senders: Vec<Arc<Sender<u64>>>,  // uid
    id_channel: (kanal::AsyncSender<u32>, kanal::AsyncReceiver<u32>),  // index
    timeout: Arc<Sender<(i64, u32, u64)>>,  // timestamp, index, uid
}

pub async fn timeout_callbacks(
    pool: Vec<Arc<Sender<u64>>>,
    status: Vec<Arc<AtomicI32>>,
    mut timeout: Receiver<(i64, u32, u64)>,
) {
    loop {
        let (time, id, uid) = timeout.recv().await.unwrap();
        let now = Utc::now().timestamp_millis();
        if time > now {
            tokio::time::sleep(Duration::from_millis(time as u64 - now as u64)).await;
        }
        if status[id as usize].compare_exchange(1, 2, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire).is_ok() {
            debug!("timeout: {}", uid);
            pool[id as usize].send(uid).await.unwrap();
        } else {
            debug!("already returned: {}", uid);
        }
    }
}

impl Default for CallbackPool {
    fn default() -> Self {
        CallbackPool::new()
    }
}

impl CallbackPool {
    pub fn new() -> Self {
        let id_channel = kanal::unbounded_async::<u32>();
        let mut callbacks = Vec::with_capacity(REQUEST_POOL_SIZE);
        let mut status = Vec::with_capacity(REQUEST_POOL_SIZE);
        let mut uids = Vec::with_capacity(REQUEST_POOL_SIZE);
        let mut senders = Vec::with_capacity(REQUEST_POOL_SIZE);
        for i in 0..REQUEST_POOL_SIZE {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            id_channel.0.clone_sync().send(i as u32).unwrap();
            callbacks.push(Box::into_raw(Box::new(OperationCallback::new(receiver))) as *const OperationCallback);
            status.push(Arc::new(AtomicI32::new(0)));
            uids.push(AtomicU64::new(0));
            senders.push(Arc::new(sender));
        }
        debug!("callback pool initialized, ids_len: {}", id_channel.1.len());
        let (sender, receiver) = tokio::sync::mpsc::channel(1000000);
        tokio::spawn(timeout_callbacks(senders.clone(), status.clone(), receiver));
        Self {
            callbacks,
            status,
            uids,
            senders,
            id_channel,
            timeout: Arc::new(sender),
        }
    }

    pub fn init(&mut self) {}

    pub fn free(&self) {
        for i in 0..self.callbacks.len() {
            unsafe {
                drop_in_place(self.callbacks[i as usize] as *mut OperationCallback);
            }
        }
    }

    pub async fn register_callback(
        &self,
        uid: u64,
        rsp_meta_data: &mut [u8],
        rsp_data: &mut [u8],
    ) -> Result<u32, Box<dyn std::error::Error>> {
        debug!("register_callback: {}", uid);
        match self.id_channel.1.recv().await {
            Ok(index) => {
                debug!("register_callback recv: {}", uid);
                self.uids[index as usize].store(uid, std::sync::atomic::Ordering::Release);
                let callback =
                    unsafe { &mut *(self.callbacks[index as usize] as *mut OperationCallback) };
                callback.data = rsp_data.as_ptr();
                callback.meta_data = rsp_meta_data.as_ptr();
                self.status[index as usize].store(1, std::sync::atomic::Ordering::Release);
                Ok(index)
            }
            Err(_) => Err("register_callback error")?,
        }
    }

    pub fn lock_if_not_timeout(
        &self,
        uid: u64,
        index: u32,
    ) -> Result<Arc<AtomicI32>, Box<dyn std::error::Error>> {
        let status = self.status[index as usize].clone();
        let last_uid = self.uids[index as usize].load(std::sync::atomic::Ordering::Acquire);
        if uid != last_uid {
            debug!("uid not match: {}, {}", uid, last_uid);
            return Err("already timed out")?;
        }
        if status.compare_exchange(1, 0, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire).is_ok() {
            Ok(status)
        } else {
            Err("already timed out")?
        }
    }

    pub fn get_data_ref(
        &self,
        index: u32,
        data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[index as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).data as *mut u8,
                data_length,
            ))
        }
    }

    pub fn get_meta_data_ref(
        &self,
        index: u32,
        meta_data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[index as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).meta_data as *mut u8,
                meta_data_length,
            ))
        }
    }

    pub async fn response(
        &self,
        uid: u64,
        index: u32,
        status: libc::c_int,
        flags: u32,
        meta_data_length: usize,
        data_lenght: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert!(uid == self.uids[index as usize].load(std::sync::atomic::Ordering::Acquire));
        {
            let callback = unsafe { &mut *(self.callbacks[index as usize] as *mut OperationCallback) };
            callback.request_status = status;
            callback.flags = flags;
            callback.meta_data_length = meta_data_length;
            callback.data_length = data_lenght;
        }
        self.senders[index as usize].send(self.uids[index as usize].load(std::sync::atomic::Ordering::Acquire) as u64).await?;
        Ok(())
    }

    // wait_for_callback
    // return: (status, flags, meta_data_length, data_length)
    pub async fn wait_for_callback(
        &self,
        index: u32,
    ) -> Result<(i32, u32, usize, usize), Box<dyn std::error::Error>> {
        let receiver = unsafe {
            (*(self.callbacks[index as usize] as *mut OperationCallback)).get_receiver()
        };
        let uid = self.uids[index as usize].load(std::sync::atomic::Ordering::Acquire);
        self.timeout
            .send((Utc::now().timestamp_millis() + 3000, index, uid))
            .await?;
        
        let result = { receiver.recv().await };
        match result {
            Some(uid) => {
                if uid != uid {
                    self.id_channel.0.send(index).await?;
                    return Err(format!("wait_for_callbakc {index} error, timeout"))?;
                }
                let status = self.status[index as usize].load(std::sync::atomic::Ordering::Acquire);
                if status == 2 {
                    self.id_channel.0.send(index).await?;
                    return Err(format!("wait_for_callbakc {index} error, timeout"))?;
                } else if status != 0 {
                    Err(format!("wait_for_callbakc {index} error, unexpected"))?
                }
                let (status, flags, meta_data_length, data_length) = {
                    let callback = unsafe {
                        &mut *(self.callbacks[index as usize] as *mut OperationCallback)
                    };
                    (
                        callback.request_status,
                        callback.flags,
                        callback.meta_data_length,
                        callback.data_length,
                    )
                };
                self.id_channel.0.send(index).await?;
                return Ok((status, flags, meta_data_length, data_length));
            }
            None => {
                debug!("wait_for_callbakc error, id: {:?}", index);
                self.id_channel.0.send(index).await?;
                Err("wait_for_callbakc error")?
            }
        }
    }
}

unsafe impl std::marker::Sync for CallbackPool {}
unsafe impl std::marker::Send for CallbackPool {}

#[allow(unused)]
#[cfg(test)]
mod tests {
    use super::{CallbackPool, OperationCallback};
    use core::time;
    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_register_callback() {
    //     let mut pool = CallbackPool::new();
    //     pool.init();
    //     let mut recv_meta_data: Vec<u8> = vec![];
    //     let mut recv_data = vec![0u8; 1024];
    //     let result = pool
    //         .register_callback(&mut recv_meta_data, &mut recv_data)
    //         .await;
    //     match result {
    //         Ok(_) => assert!(true),
    //         Err(_) => assert!(false),
    //     }
    // }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_get_metadata_ref() {
    //     let mut pool = CallbackPool::new();
    //     pool.init();
    //     let mut recv_meta_data = vec![];
    //     let mut recv_data = vec![0u8; 1024];
    //     let result = pool
    //         .register_callback(&mut recv_meta_data, &mut recv_data)
    //         .await;
    //     match result {
    //         Ok(id) => match pool.get_meta_data_ref(id, recv_meta_data.len()) {
    //             Ok(recv_meta_data_ref) => {
    //                 assert_eq!(recv_meta_data_ref, &mut recv_meta_data);
    //             }
    //             Err(_) => assert!(false),
    //         },
    //         Err(_) => assert!(false),
    //     }
    // }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_get_data_ref() {
    //     let mut pool = CallbackPool::new();
    //     pool.init();
    //     let mut recv_meta_data = vec![];
    //     let mut recv_data = vec![0u8; 1024];
    //     let result = pool
    //         .register_callback(&mut recv_meta_data, &mut recv_data)
    //         .await;
    //     match result {
    //         Ok(id) => match pool.get_data_ref(id, recv_data.len()) {
    //             Ok(recv_data_ref) => {
    //                 assert_eq!(recv_data_ref, &mut recv_data);
    //             }
    //             Err(_) => assert!(false),
    //         },
    //         Err(_) => assert!(false),
    //     }
    // }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_wait_for_callback() {
    //     let mut pool = CallbackPool::new();
    //     pool.init();
    //     let mut recv_meta_data = vec![];
    //     let mut recv_data = vec![0u8; 1024];
    //     let result = pool
    //         .register_callback(&mut recv_meta_data, &mut recv_data)
    //         .await;
    //     match result {
    //         Ok(id) => {
    //             let callback = pool.callbacks[id as usize];
    //             unsafe {
    //                 let oc = &*(callback as *mut OperationCallback);
    //                 match pool.channels[id as usize].0.send(()).await {
    //                     Ok(_) => assert!(true),
    //                     Err(_) => assert!(false),
    //                 };
    //                 match pool.wait_for_callback(id).await {
    //                     Ok(_) => assert!(true),
    //                     Err(_) => assert!(false),
    //                 };
    //             }
    //         }
    //         Err(_) => assert!(false),
    //     }
    // }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    // async fn test_reponse() {
    //     let mut pool = CallbackPool::new();
    //     pool.init();
    //     let mut recv_meta_data = vec![];
    //     let mut recv_data = vec![0u8; 1024];
    //     let result = pool
    //         .register_callback(&mut recv_meta_data, &mut recv_data)
    //         .await;
    //     match result {
    //         Ok(id) => {
    //             let callback = pool.callbacks[id as usize];
    //             unsafe {
    //                 match pool
    //                     .response(id, 1 as i32, 2 as u32, 24 as usize, 512 as usize)
    //                     .await
    //                 {
    //                     Ok(_) => assert!(true),
    //                     Err(_) => assert!(false),
    //                 };
    //                 let oc = &*(callback as *mut OperationCallback);
    //                 match pool.channels[id as usize].1.recv().await {
    //                     Ok(_) => {
    //                         assert_eq!(oc.request_status, 1);
    //                         assert_eq!(oc.flags, 2);
    //                         assert_eq!(oc.meta_data_length, 24);
    //                         assert_eq!(oc.data_length, 512);
    //                     }
    //                     Err(_) => assert!(false),
    //                 }
    //             }
    //         }
    //         Err(_) => assert!(false),
    //     }
    // }
}
