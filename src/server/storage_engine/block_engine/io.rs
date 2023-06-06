// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use nix::{
    fcntl::{self, OFlag},
    sys::{
        stat::Mode,
        uio::{pread, pwrite},
    },
};

pub(crate) struct Storage {
    _fd: i32,
}

impl Storage {
    pub(crate) fn new(path: &str) -> Storage {
        let oflags = OFlag::O_RDWR;
        let mode = Mode::S_IRUSR
            | Mode::S_IWUSR
            | Mode::S_IRGRP
            | Mode::S_IWGRP
            | Mode::S_IROTH
            | Mode::S_IWOTH;
        let fd = fcntl::open(path, oflags, mode);
        match fd {
            Ok(fd) => Self { _fd: fd },
            Err(_) => panic!("No Raw blockdevice"),
        }
    }

    pub(crate) fn _write(&self, data: &[u8], offset: i64) -> Result<usize, i32> {
        match pwrite(self._fd, data, offset) {
            Ok(size) => Ok(size),
            Err(_) => Err(libc::EIO),
        }
    }

    pub(crate) fn _read(&self, size: u32, offset: i64) -> Result<Vec<u8>, i32> {
        let mut data = vec![0; size as usize];
        let length = pread(self._fd, data.as_mut_slice(), offset).map_err(|_| libc::EIO)?;
        Ok(data[..length].to_vec())
    }
}

#[cfg(feature = "block_test")]
#[cfg(test)]
mod tests {
    use std::process::Command;

    use crate::server::storage_engine::block_device::io::Storage;
    #[test]
    fn write_and_read_test() {
        Command::new("bash")
            .arg("-c")
            .arg("dd if=/dev/zero of=node1 bs=4M count=1")
            .output()
            .unwrap();
        Command::new("bash")
            .arg("-c")
            .arg("losetup /dev/loop8 node1")
            .output()
            .unwrap();
        let storage = Storage::new("/dev/loop8");
        let writre_result = storage.write(&b"some bytes"[..], 0).unwrap();
        assert_eq!(writre_result, 10);
        let read_result = storage.read(10, 0).unwrap();
        assert_eq!(read_result, &b"some bytes"[..]);
        Command::new("bash")
            .arg("-c")
            .arg("losetup -d /dev/loop8")
            .output()
            .unwrap();
        Command::new("bash")
            .arg("-c")
            .arg("rm node1")
            .output()
            .unwrap();
    }
}
