use std::{
    ptr::drop_in_place,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::rpc::protocol::REQUEST_POOL_SIZE;
use kanal;
use log::debug;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};

use super::protocol::CLIENT_REQUEST_TIMEOUT;

pub struct OperationCallback {
    pub data: *const u8,
    pub meta_data: *const u8,
    pub data_length: usize,
    pub meta_data_length: usize,
    pub request_status: libc::c_int,
    pub flags: u32,
    pub receiver: *mut Receiver<()>,
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
    pub fn new(receiver: Receiver<()>) -> Self {
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

    #[allow(clippy::mut_from_ref)]
    pub fn get_receiver(&self) -> &mut Receiver<()> {
        unsafe { &mut *self.receiver }
    }
}

pub struct CallbackPool {
    callbacks: Vec<*const OperationCallback>,
    senders: Vec<Arc<Sender<()>>>,
    ids: (kanal::AsyncSender<u32>, kanal::AsyncReceiver<u32>),
    callback_status: Vec<AtomicU32>,
    // batch id for each callback, used to check if the callback is for the current request.
    // Each request has a unique batch id and will increase the batch id by 1 for each callback.
    batch: Vec<AtomicU32>,
}

impl Default for CallbackPool {
    fn default() -> Self {
        CallbackPool::new()
    }
}

impl CallbackPool {
    pub fn new() -> Self {
        let mut callbacks = Vec::with_capacity(REQUEST_POOL_SIZE);
        let mut senders = Vec::with_capacity(REQUEST_POOL_SIZE);
        let ids = kanal::unbounded_async::<u32>();
        let mut callback_status = Vec::with_capacity(REQUEST_POOL_SIZE);
        let mut batch = Vec::with_capacity(REQUEST_POOL_SIZE);
        for i in 0..REQUEST_POOL_SIZE as u32 {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            callbacks.push(Box::into_raw(Box::new(OperationCallback::new(receiver)))
                as *const OperationCallback);
            senders.push(Arc::new(sender));
            ids.0.clone_sync().send(i).unwrap();
            callback_status.push(AtomicU32::new(0));
            batch.push(AtomicU32::new(0));
        }
        Self {
            callbacks,
            senders,
            ids,
            callback_status,
            batch,
        }
    }

    pub fn init(&mut self) {}

    pub fn free(&self) {
        for i in 0..self.callbacks.len() {
            unsafe {
                drop_in_place(self.callbacks[i] as *mut OperationCallback);
            }
        }
    }

    pub async fn register_callback(
        &self,
        rsp_meta_data: &mut [u8],
        rsp_data: &mut [u8],
    ) -> Result<(u32, u32), Box<dyn std::error::Error>> {
        match self.ids.1.clone().recv().await {
            Ok(id) => {
                let callback =
                    unsafe { &mut *(self.callbacks[id as usize] as *mut OperationCallback) };
                callback.data = rsp_data.as_ptr();
                callback.meta_data = rsp_meta_data.as_ptr();

                // codes above can be reordered, so we don't use AcqRel. Maybe directly use fetch and store is better.
                let batch = self.batch[id as usize].fetch_add(1, Ordering::Release);

                assert!(self.callback_status[id as usize].load(Ordering::Acquire) == 0); // only for debug
                self.callback_status[id as usize].store(1, Ordering::Release);
                Ok((batch + 1, id))
            }
            Err(e) => Err(format!("register callback failed: {}", e).into()),
        }
    }

    pub fn lock_if_not_timeout(
        &self,
        batch: u32,
        id: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now_batch = self.batch[id as usize].load(std::sync::atomic::Ordering::Acquire);
        if batch != now_batch {
            debug!("uid not match: {}, {}", batch, now_batch);
            return Err("already timed out")?;
        }
        if self.callback_status[id as usize]
            .compare_exchange(
                1,
                0,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            Ok(())
        } else {
            Err("lock failed, already timed out")?
        }
    }

    pub fn get_data_ref(
        &self,
        id: u32,
        data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).data as *mut u8,
                data_length,
            ))
        }
    }

    pub fn get_meta_data_ref(
        &self,
        id: u32,
        meta_data_length: usize,
    ) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let callback = self.callbacks[id as usize];
        unsafe {
            Ok(std::slice::from_raw_parts_mut(
                (*callback).meta_data as *mut u8,
                meta_data_length,
            ))
        }
    }

    pub async fn response(
        &self,
        id: u32,
        status: libc::c_int,
        flags: u32,
        meta_data_length: usize,
        data_lenght: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        {
            let callback = unsafe { &mut *(self.callbacks[id as usize] as *mut OperationCallback) };
            callback.request_status = status;
            callback.flags = flags;
            callback.meta_data_length = meta_data_length;
            callback.data_length = data_lenght;
        }
        self.senders[id as usize].send(()).await?;
        debug!("Response success");
        Ok(())
    }

    // wait_for_callback
    // return: (status, flags, meta_data_length, data_length)
    pub async fn wait_for_callback(
        &self,
        id: u32,
    ) -> Result<(i32, u32, usize, usize), Box<dyn std::error::Error>> {
        let receiver =
            unsafe { (*(self.callbacks[id as usize] as *mut OperationCallback)).get_receiver() };
        let result = timeout(CLIENT_REQUEST_TIMEOUT, receiver.recv()).await;
        let is_ok = match result {
            Ok(r) => match r {
                Some(_) => true,
                None => panic!("wait_for_callback error"),
            },
            Err(_) => {
                match self.callback_status[id as usize].compare_exchange(
                    1,
                    0,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => false,
                    Err(_) => {
                        // This means that the response has been received, and signal has been sent.
                        match timeout(CLIENT_REQUEST_TIMEOUT, receiver.recv()).await {
                            Ok(r) => match r {
                                Some(_) => true,
                                None => panic!("wait_for_callback error"),
                            },
                            Err(_) => {
                                panic!("unexpected wait_for_callback timeout")
                            }
                        }
                    }
                }
            }
        };
        match is_ok {
            true => {
                let (status, flags, meta_data_length, data_length) = {
                    let callback =
                        unsafe { &mut *(self.callbacks[id as usize] as *mut OperationCallback) };
                    (
                        callback.request_status,
                        callback.flags,
                        callback.meta_data_length,
                        callback.data_length,
                    )
                };
                self.ids.0.clone().send(id).await?;
                Ok((status, flags, meta_data_length, data_length))
            }
            false => {
                self.ids.0.clone().send(id).await?;
                Err(format!("wait_for_callback timeout, id: {}", id))?
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_register_callback() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data: Vec<u8> = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok(_) => assert!(true),
            Err(_) => assert!(false),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_metadata_ref() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok((batch, id)) => match pool.get_meta_data_ref(id, recv_meta_data.len()) {
                Ok(recv_meta_data_ref) => {
                    assert_eq!(recv_meta_data_ref, &mut recv_meta_data);
                }
                Err(_) => assert!(false),
            },
            Err(_) => assert!(false),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_data_ref() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok((batch, id)) => match pool.get_data_ref(id, recv_data.len()) {
                Ok(recv_data_ref) => {
                    assert_eq!(recv_data_ref, &mut recv_data);
                }
                Err(_) => assert!(false),
            },
            Err(_) => assert!(false),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_wait_for_callback() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok((batch, id)) => {
                let callback = pool.callbacks[id as usize];
                unsafe {
                    let oc = &*(callback as *mut OperationCallback);
                    match pool.senders[id as usize].send(()).await {
                        Ok(_) => assert!(true),
                        Err(_) => assert!(false),
                    };
                    match pool.wait_for_callback(id).await {
                        Ok(_) => assert!(true),
                        Err(_) => assert!(false),
                    };
                }
            }
            Err(_) => assert!(false),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_reponse() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok((batch, id)) => {
                let callback = pool.callbacks[id as usize];
                unsafe {
                    match pool
                        .response(id, 1 as i32, 2 as u32, 24 as usize, 512 as usize)
                        .await
                    {
                        Ok(_) => assert!(true),
                        Err(_) => assert!(false),
                    };
                    let oc = &*(callback as *mut OperationCallback);
                    let mut receiver = unsafe {
                        (*(pool.callbacks[id as usize] as *mut OperationCallback)).get_receiver()
                    };
                    match receiver.recv().await {
                        Some(_) => {
                            assert_eq!(oc.request_status, 1);
                            assert_eq!(oc.flags, 2);
                            assert_eq!(oc.meta_data_length, 24);
                            assert_eq!(oc.data_length, 512);
                        }
                        None => assert!(false),
                    }
                }
            }
            Err(_) => assert!(false),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_lock_if_not_timeout() {
        let mut pool = CallbackPool::new();
        pool.init();
        let mut recv_meta_data = vec![];
        let mut recv_data = vec![0u8; 1024];
        let result = pool
            .register_callback(&mut recv_meta_data, &mut recv_data)
            .await;
        match result {
            Ok((batch, id)) => unsafe {
                match pool.lock_if_not_timeout(batch, id) {
                    Ok(_) => assert!(true),
                    Err(_) => assert!(false),
                };
                match pool
                    .response(id, 1 as i32, 2 as u32, 24 as usize, 512 as usize)
                    .await
                {
                    Ok(_) => assert!(true),
                    Err(_) => assert!(false),
                };
                match pool.wait_for_callback(id).await {
                    Ok(_) => assert!(true),
                    Err(_) => assert!(false),
                };
            },
            Err(_) => assert!(false),
        }
    }
}
