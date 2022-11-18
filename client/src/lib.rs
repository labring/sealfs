// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
pub mod connection;
pub mod distribute_hash_table;
pub mod manager;

use clap::Parser;
use fuser::{
    Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use manager::MANAGER;
use std::ffi::OsStr;

struct SealFS;

impl Filesystem for SealFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        unsafe {
            MANAGER.lookup_remote(parent, name, reply);
        }
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        unsafe {
            MANAGER.create_remote(parent, name, mode, umask, flags, reply);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        unsafe {
            MANAGER.getattr_remote(ino, reply);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        unsafe {
            MANAGER.readdir_remote(ino, offset, reply);
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        unsafe {
            MANAGER.read_remote(ino, offset, size, reply);
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        unsafe {
            MANAGER.write_remote(ino, offset, data, reply);
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        unsafe {
            MANAGER.mkdir_remote(parent, name, mode, reply);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        unsafe {
            MANAGER.open_remote(ino, flags, reply);
        }
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        unsafe {
            MANAGER.unlink_remote(_parent, _name, reply);
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: fuser::ReplyEmpty) {
        unsafe {
            MANAGER.rmdir_remote(_parent, _name, reply);
        }
    }
}

pub fn init_fs_client() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    env_logger::init();
    let mountpoint = cli.mount_point.unwrap();
    let mut options = vec![MountOption::RO, MountOption::FSName("seal".to_string())];
    if cli.auto_unmount {
        options.push(MountOption::AutoUnmount);
    }
    if cli.allow_root {
        options.push(MountOption::AllowRoot);
    }

    fuser::mount2(SealFS, mountpoint, &options).unwrap();
    /* TODO
    thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 107, kind: NotConnected, message: "Transport endpoint is not connected" }', sealfs-rust/client/src/lib.rs:162:49

    should be fixed by checking if the mountpoint is valid
    */

    Ok(())
}

#[derive(Parser)]
#[command(author = "Christopher Berner", version, about, long_about = None)]
struct Cli {
    /// Act as a client, and mount FUSE at given path
    #[arg(required = true, name = "MOUNT_POINT")]
    mount_point: Option<String>,

    /// Automatically unmount on process exit
    #[arg(long = "auto_unmount", name = "auto_unmount")]
    auto_unmount: bool,

    /// Allow root user to access filesystem
    #[arg(long = "allow-root", name = "allow-root")]
    allow_root: bool,
}
