// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap::DashMap;
use lazy_static::lazy_static;
use libc::{dirent64, iovec, DT_DIR, DT_LNK, DT_REG, O_CREAT, O_DIRECTORY};
use log::debug;
use sealfs::common::serialization::{
    FileAttrSimple, LinuxDirent, OperationType, ReadFileSendMetaData, SubDirectory,
    TruncateFileSendMetaData,
};
use sealfs::rpc;
pub struct Client {
    // TODO replace with a thread safe data structure
    pub client: rpc::client::Client,
    pub inodes: DashMap<String, u64>,
    pub inodes_reverse: DashMap<u64, String>,
    handle: tokio::runtime::Handle,
}
const CHUNK_SIZE: i64 = 65536;
const OFFSET_DIRENT_NAME: u16 = 19;
impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        let handle = tokio::runtime::Handle::current();
        Self {
            client: rpc::client::Client::default(),
            inodes: DashMap::new(),
            inodes_reverse: DashMap::new(),
            handle,
        }
    }

    pub async fn add_connection(&self, server_address: &str) {
        self.client.add_connection(server_address).await;
    }

    pub fn remove_connection(&self, server_address: &str) {
        self.client.remove_connection(server_address);
    }

    pub fn get_connection_address(
        &self,
        _path: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // mock
        Ok("127.0.0.1:8085".to_string())
    }

    pub fn open_remote(&self, pathname: &str, flag: i32, mode: u32) -> Result<(), i32> {
        debug!("open_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = vec![0u8; 1024];
        let (operation_type, send_meta_data) = match flag & O_CREAT {
            0 => {
                if (flag & O_DIRECTORY) != 0 {
                    return Ok(());
                }
                (OperationType::OpenFile, vec![])
            }
            _ => (OperationType::CreateFile, Vec::from(mode.to_le_bytes())),
        };
        // todo: Err -> errno
        if self
            .handle
            .block_on(self.client.call_remote(
                &server_address,
                operation_type.into(),
                0,
                pathname,
                &send_meta_data,
                &[],
                &mut status,
                &mut rsp_flags,
                &mut recv_meta_data_length,
                &mut recv_data_length,
                &mut recv_meta_data,
                &mut [],
            ))
            .is_err()
        {
            return Err(libc::EIO);
        }
        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn rename_remote(&self, _oldpath: &str, _newpath: &str) -> i32 {
        debug!("rename_remote");
        todo!()
    }

    pub fn truncate_remote(&self, pathname: &str, length: i64) -> Result<(), i32> {
        debug!("truncate_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let send_meta_data = bincode::serialize(&TruncateFileSendMetaData { length }).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::TruncateFile.into(),
            0,
            pathname,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut [],
        )) {
            return Err(libc::EIO);
        }

        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn mkdir_remote(&self, pathname: &str, mode: u32) -> Result<(), i32> {
        debug!("mkdir_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let send_meta_data = mode.to_le_bytes();
        let mut recv_meta_data = vec![0u8; 1024];
        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::CreateDir.into(),
            0,
            pathname,
            &send_meta_data,
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn rmdir_remote(&self, pathname: &str) -> Result<(), i32> {
        debug!("rmdir_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::DeleteDir.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut [],
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn getdents_remote(
        &self,
        pathname: &str,
        dirp: &mut [u8],
        dirp_offset: i64,
    ) -> Result<isize, i32> {
        debug!("getdents_remote {}", pathname);
        if dirp_offset != 0 {
            return Ok(0);
        }
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        // todo: The entries needs to be stored one by one
        let mut recv_data = vec![0u8; 800000];

        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::ReadDir.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut recv_data,
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            return Err(status);
        }
        let entries: SubDirectory = bincode::deserialize(&recv_data[..recv_data_length]).unwrap();
        let dirp_ptr = dirp.as_mut_ptr();
        let mut realoffset = 0;
        for entry in entries.sub_dir {
            let mut name = entry.0;
            name.push('\0');
            let kind = {
                match entry.1.as_bytes()[0] {
                    b'f' => DT_REG,
                    b'd' => DT_DIR,
                    b's' => DT_LNK,
                    _ => DT_REG,
                }
            };
            name.push(kind as char);
            let reclen = 18 + name.len() as u16;
            if realoffset as u16 + reclen > dirp.len() as u16 {
                break;
            }

            unsafe {
                (*(dirp_ptr.add(realoffset) as *mut LinuxDirent)).d_ino = 1;
                (*(dirp_ptr.add(realoffset) as *mut LinuxDirent)).d_off = 2;
                (*(dirp_ptr.add(realoffset) as *mut LinuxDirent)).d_reclen = reclen;
                std::ptr::copy(
                    name.as_bytes().to_vec().as_ptr(),
                    (*(dirp_ptr.add(realoffset) as *mut LinuxDirent))
                        .d_name
                        .as_mut_ptr() as *mut u8,
                    name.len(),
                )
            }
            realoffset += reclen as usize;
        }
        Ok(realoffset as isize)
    }

    pub fn getdents64_remote(
        &self,
        pathname: &str,
        dirp: &mut [u8],
        dirp_offset: i64,
    ) -> Result<isize, i32> {
        debug!("getdents64_remote {}", pathname);
        if dirp_offset != 0 {
            return Ok(0);
        }
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        // todo: The entries needs to be stored one by one
        let mut recv_data = vec![0u8; 200000];

        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::ReadDir.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut recv_data,
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            return Err(status);
        }
        let entries: SubDirectory = bincode::deserialize(&recv_data[..recv_data_length]).unwrap();
        let dirp_ptr = dirp.as_mut_ptr();
        let mut realoffset = 0;
        for entry in entries.sub_dir {
            let mut name = entry.0;
            name.push('\0');
            let reclen = (OFFSET_DIRENT_NAME + name.len() as u16 + 7) / 8 * 8;
            if realoffset as u16 + reclen > dirp.len() as u16 {
                break;
            }
            let kind = {
                match entry.1.as_bytes()[0] {
                    b'f' => DT_REG,
                    b'd' => DT_DIR,
                    b's' => DT_LNK,
                    _ => DT_REG,
                }
            };
            unsafe {
                (*(dirp_ptr.add(realoffset) as *mut dirent64)).d_ino = 1;
                (*(dirp_ptr.add(realoffset) as *mut dirent64)).d_off = 2;
                (*(dirp_ptr.add(realoffset) as *mut dirent64)).d_reclen = reclen;
                (*(dirp_ptr.add(realoffset) as *mut dirent64)).d_type = kind as u8;
                std::ptr::copy(
                    name.as_bytes().to_vec().as_ptr(),
                    (*(dirp_ptr.add(realoffset) as *mut dirent64))
                        .d_name
                        .as_mut_ptr() as *mut u8,
                    name.len(),
                )
            }
            realoffset += reclen as usize;
        }
        Ok(realoffset as isize)
    }

    pub fn unlink_remote(&self, pathname: &str) -> Result<(), i32> {
        debug!("unlink_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::DeleteFile.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut [],
            &mut [],
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            Err(status)
        } else {
            Ok(())
        }
    }

    pub fn stat_remote(&self, pathname: &str, statbuf: &mut [u8]) -> Result<(), i32> {
        debug!("stat_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = [0u8; 1024];
        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::GetFileAttr.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            return Err(status);
        }

        let file_attr_simple: FileAttrSimple =
            bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
        file_attr_simple.tostat(statbuf);
        Ok(())
    }

    pub fn statx_remote(&self, pathname: &str, statxbuf: &mut [u8]) -> Result<(), i32> {
        debug!("statx_remote {}", pathname);
        let server_address = self.get_connection_address(pathname).unwrap();
        let mut status = 0i32;
        let mut rsp_flags = 0u32;

        let mut recv_meta_data_length = 0usize;
        let mut recv_data_length = 0usize;

        let mut recv_meta_data = [0u8; 1024];
        if let Err(_) = self.handle.block_on(self.client.call_remote(
            &server_address,
            OperationType::GetFileAttr.into(),
            0,
            pathname,
            &[],
            &[],
            &mut status,
            &mut rsp_flags,
            &mut recv_meta_data_length,
            &mut recv_data_length,
            &mut recv_meta_data,
            &mut [],
        )) {
            return Err(libc::EIO);
        }
        if status != 0 {
            return Err(status);
        }

        let file_attr_simple: FileAttrSimple =
            bincode::deserialize(&recv_meta_data[..recv_meta_data_length]).unwrap();
        file_attr_simple.tostatx(statxbuf);
        Ok(())
    }

    pub fn pread_remote(&self, pathname: &str, buf: &mut [u8], offset: i64) -> Result<isize, i32> {
        debug!("pread_remote {}", pathname);
        let mut idx = offset / CHUNK_SIZE;
        let end_idx = offset + buf.len() as i64;
        let mut chunk_left = offset;
        let mut chunk_right = std::cmp::min((idx + 1) * CHUNK_SIZE, offset + buf.len() as i64);
        let mut inbuf = buf;
        self.handle.block_on(async {
            let mut result = 0;
            while chunk_left < offset + inbuf.len() as i64 {
                let server_address = self
                    .get_connection_address(&format!("{}_{}", pathname, idx))
                    .unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;

                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;
                let (chunk_buf, next_buf) = inbuf.split_at_mut((chunk_right - chunk_left) as usize);
                inbuf = next_buf;
                let send_meta_data = bincode::serialize(&ReadFileSendMetaData {
                    offset: chunk_left,
                    size: chunk_buf.len() as u32,
                })
                .unwrap();
                if let Err(_) = self
                    .client
                    .call_remote(
                        &server_address,
                        OperationType::ReadFile.into(),
                        0,
                        pathname,
                        &send_meta_data,
                        &[],
                        &mut status,
                        &mut rsp_flags,
                        &mut recv_meta_data_length,
                        &mut recv_data_length,
                        &mut [],
                        chunk_buf,
                    )
                    .await
                {
                    return Err(libc::EIO);
                }
                if status != 0 {
                    return Err(status);
                }
                idx += 1;
                result += recv_data_length as isize;
                if recv_data_length < chunk_right as usize - chunk_left as usize {
                    break;
                }
                chunk_left = chunk_right;
                chunk_right = std::cmp::min(chunk_right + CHUNK_SIZE, end_idx);
            }
            Ok(result)
        })
    }

    pub fn preadv_remote(&self, _pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
        debug!("preadv_remote");
        todo!()
    }

    pub fn pwrite_remote(&self, pathname: &str, buf: &[u8], offset: i64) -> Result<isize, i32> {
        debug!("pwrite_remote {}", pathname);
        let mut idx = offset / CHUNK_SIZE;
        let end_idx = offset + buf.len() as i64;
        let mut chunk_left = offset;
        let mut chunk_right = std::cmp::min((idx + 1) * CHUNK_SIZE, end_idx);
        let mut outbuf = buf;
        self.handle.block_on(async {
            let mut result = 0;
            while chunk_left < offset + buf.len() as i64 {
                let server_address = self
                    .get_connection_address(&format!("{}_{}", pathname, idx))
                    .unwrap();
                let mut status = 0i32;
                let mut rsp_flags = 0u32;
                let (chunk_buf, next_buf) = outbuf.split_at((chunk_right - chunk_left) as usize);
                outbuf = next_buf;
                let mut recv_meta_data_length = 0usize;
                let mut recv_data_length = 0usize;

                let mut recv_meta_data = [0u8; std::mem::size_of::<isize>()];
                if let Err(_) = self
                    .client
                    .call_remote(
                        &server_address,
                        OperationType::WriteFile.into(),
                        0,
                        pathname,
                        &chunk_left.to_le_bytes(),
                        chunk_buf,
                        &mut status,
                        &mut rsp_flags,
                        &mut recv_meta_data_length,
                        &mut recv_data_length,
                        &mut recv_meta_data,
                        &mut [],
                    )
                    .await
                {
                    return Err(libc::EIO);
                }
                if status != 0 {
                    return Err(status);
                }
                let size = isize::from_le_bytes(recv_meta_data);
                idx += 1;
                chunk_left = chunk_right;
                chunk_right = std::cmp::min(chunk_right + CHUNK_SIZE, end_idx);
                result += size as isize;
            }
            Ok(result)
        })
    }

    pub fn pwritev_remote(&self, _pathname: &str, _buf: &[iovec], _offset: i64) -> isize {
        debug!("pwritev_remote");
        todo!()
    }
}

lazy_static! {
    pub static ref CLIENT: Client = Client::new();
}
