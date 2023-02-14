use std::ptr::drop_in_place;

use crate::rpc::protocol::REQUEST_POOL_SIZE;
use kanal;
use log::debug;

pub struct OperationCallback {
    pub data: *const u8,
    pub meta_data: *const u8,
    pub data_length: usize,
    pub meta_data_length: usize,
    pub request_status: libc::c_int,
    pub flags: u32,
}

unsafe impl std::marker::Sync for OperationCallback {}
unsafe impl std::marker::Send for OperationCallback {}

impl Default for OperationCallback {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            meta_data: std::ptr::null(),
            data_length: 0,
            meta_data_length: 0,
            request_status: 0,
            flags: 0,
        }
    }
}

impl OperationCallback {
    pub fn new() -> Self {
        Self {
            data: std::ptr::null(),
            meta_data: std::ptr::null(),
            data_length: 0,
            meta_data_length: 0,
            request_status: 0,
            flags: 0,
        }
    }
}

pub struct CallbackPool {
    callbacks: Vec<*const OperationCallback>,
    channels: Vec<(kanal::AsyncSender<()>, kanal::AsyncReceiver<()>)>,
    ids: (kanal::AsyncSender<u32>, kanal::AsyncReceiver<u32>),
}

impl Default for CallbackPool {
    fn default() -> Self {
        CallbackPool::new()
    }
}

impl CallbackPool {
    pub fn new() -> Self {
        let mut channels = Vec::with_capacity(REQUEST_POOL_SIZE);
        let ids = kanal::unbounded_async::<u32>();
        for i in 0..REQUEST_POOL_SIZE as u32 {
            channels.push(kanal::unbounded_async());
            ids.0.clone_sync().send(i).unwrap();
        }
        Self {
            callbacks: vec![std::ptr::null_mut(); REQUEST_POOL_SIZE],
            channels,
            ids,
        }
    }

    pub fn init(&mut self) {
        for i in 0..self.callbacks.len() {
            self.callbacks[i] = Box::into_raw(Box::new(OperationCallback::new()));
        }
    }

    pub fn free(&self) {
        for i in 0..self.callbacks.len() {
            unsafe {
                drop_in_place(self.callbacks[i as usize] as *mut OperationCallback);
            }
        }
    }

    pub async fn register_callback(
        &self,
        rsp_meta_data: &mut [u8],
        rsp_data: &mut [u8],
    ) -> Result<u32, Box<dyn std::error::Error>> {
        match self.ids.1.clone().recv().await {
            Ok(id) => {
                let callback =
                    unsafe { &mut *(self.callbacks[id as usize] as *mut OperationCallback) };
                callback.data = rsp_data.as_ptr();
                callback.meta_data = rsp_meta_data.as_ptr();
                Ok(id)
            }
            Err(_) => Err("register_callback error")?,
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
        self.channels[id as usize].0.clone().send(()).await?;
        Ok(())
    }

    // wait_for_callback
    // return: (status, flags, meta_data_length, data_length)
    pub async fn wait_for_callback(
        &self,
        id: u32,
    ) -> Result<(i32, u32, usize, usize), Box<dyn std::error::Error>> {
        let result = self.channels[id as usize].1.clone().recv().await;
        match result {
            Ok(_) => {
                let (status, flags, meta_data_length, data_length) = {
                    let callback =
                        unsafe { &mut *(self.callbacks[id as usize] as *mut OperationCallback) };
                    debug!("callback meta data length: {}", callback.meta_data_length);
                    debug!("callback data length: {}", callback.data_length);
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
            Err(_) => {
                debug!("wait_for_callbakc error, id: {:?}", id);
                self.ids.0.clone().send(id).await?;
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
            Ok(id) => match pool.get_meta_data_ref(id, recv_meta_data.len()) {
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
            Ok(id) => match pool.get_data_ref(id, recv_data.len()) {
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
            Ok(id) => {
                let callback = pool.callbacks[id as usize];
                unsafe {
                    let oc = &*(callback as *mut OperationCallback);
                    match pool.channels[id as usize].0.send(()).await {
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
            Ok(id) => {
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
                    match pool.channels[id as usize].1.recv().await {
                        Ok(_) => {
                            assert_eq!(oc.request_status, 1);
                            assert_eq!(oc.flags, 2);
                            assert_eq!(oc.meta_data_length, 24);
                            assert_eq!(oc.data_length, 512);
                        }
                        Err(_) => assert!(false),
                    }
                }
            }
            Err(_) => assert!(false),
        }
    }
}
