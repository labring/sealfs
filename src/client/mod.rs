// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
pub mod fuse_client;

use clap::{Parser, Subcommand};
use fuse_client::CLIENT;
use fuser::{
    Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyWrite, Request,
};
use log::info;
use std::{ffi::OsStr, str::FromStr};

#[derive(Parser)]
#[command(author = "Christopher Berner", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Log level
    #[arg(long = "log-level", name = "log-level")]
    log_level: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Create {
        /// Create a volume with a given mount point and size
        #[arg(required = true, name = "MOUNT_POINT")]
        mount_point: Option<String>,

        /// Size of the volume
        #[arg(required = true, name = "volume_SIZE")]
        volume_size: Option<u64>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        manager_address: Option<String>,
    },
    Mount {
        /// Act as a client, and mount FUSE at given path
        #[arg(required = true, name = "MOUNT_POINT")]
        mount_point: Option<String>,

        /// Remote volume name
        #[arg(required = true, name = "volume_NAME")]
        volume_name: Option<String>,

        /// Automatically unmount on process exit
        #[arg(long = "auto_unmount", name = "auto_unmount")]
        auto_unmount: bool,

        /// Allow root user to access filesystem
        #[arg(long = "allow-root", name = "allow-root")]
        allow_root: bool,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        manager_address: Option<String>,
    },
    Add {
        /// Add a server to the cluster
        #[arg(required = true, name = "SERVER_ADDRESS")]
        server_address: Option<String>,

        /// Weight of the server
        #[arg(long = "weight", name = "weight")]
        weight: Option<usize>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        manager_address: Option<String>,
    },
    Delete {
        /// Delete a server from the cluster
        #[arg(required = true, name = "SERVER_ADDRESS")]
        server_address: Option<String>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        manager_address: Option<String>,
    },
    ListServers {
        /// List all servers in the cluster
        #[arg(long = "list", name = "list")]
        _list: bool,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        _manager_address: Option<String>,
    },
    ListVolumes {
        /// List all servers in the cluster
        #[arg(long = "list", name = "list")]
        _list: bool,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        _manager_address: Option<String>,
    },
    Status {
        /// Get the status of the cluster
        #[arg(long = "status", name = "status")]
        _status: bool,

        /// Address of the manager
        #[arg(short = 'm', long = "manager_address", name = "manager_address")]
        _manager_address: Option<String>,
    },
}

struct SealFS;

impl Filesystem for SealFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup, parent = {}, name = {:?}", parent, name);
        CLIENT.lookup_remote(parent, name, reply);
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
        info!(
            "create, parent = {}, name = {:?}, mode = {}, umask = {}, flags = {}",
            parent, name, mode, umask, flags
        );
        CLIENT.create_remote(parent, name, mode, umask, flags, reply);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr, ino = {}", ino);
        CLIENT.getattr_remote(ino, reply);
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        info!("readdir, ino = {}, offset = {}", ino, offset);
        CLIENT.readdir_remote(ino, offset, reply);
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
        info!("read, ino = {}, offset = {}, size = {}", ino, offset, size);
        CLIENT.read_remote(ino, offset, size, reply);
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
        info!(
            "write, ino = {}, offset = {}, data_len = {}",
            ino,
            offset,
            data.len()
        );
        CLIENT.write_remote(ino, offset, data, reply);
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
        info!(
            "mkdir, parent = {}, name = {:?}, mode = {}",
            parent, name, mode
        );
        CLIENT.mkdir_remote(parent, name, mode, reply);
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        CLIENT.open_remote(ino, flags, reply);
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        info!("unlink");
        CLIENT.unlink_remote(_parent, _name, reply);
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: fuser::ReplyEmpty) {
        info!("rmdir");
        CLIENT.rmdir_remote(_parent, _name, reply);
    }
}

pub async fn connect_to_manager(manager_address: String) -> Result<(), Box<dyn std::error::Error>> {
    CLIENT.connect_to_manager(&manager_address).await;
    tokio::spawn(CLIENT.sync_cluster_infos());
    tokio::spawn(CLIENT.watch_status());
    Ok(())
}

pub fn run_command() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let log_level = match cli.log_level {
        Some(level) => level,
        None => "warn".to_owned(),
    };
    let mut builder = env_logger::Builder::from_default_env();
    builder
        .format_timestamp(None)
        .filter(None, log::LevelFilter::from_str(&log_level).unwrap());
    builder.init();

    info!("spawn client");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    match cli.command {
        Commands::Create {
            mount_point,
            volume_size,
            manager_address,
        } => {
            let mountpoint = mount_point.unwrap();

            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            runtime.block_on(connect_to_manager(manager_address))?;

            info!("connect_servers");
            runtime.block_on(CLIENT.connect_servers())?;

            info!("create_volume");
            runtime.block_on(CLIENT.create_volume(&mountpoint, volume_size.unwrap()))?;

            Ok(())
        }
        Commands::Mount {
            mount_point,
            auto_unmount,
            allow_root,
            manager_address,
            volume_name,
        } => {
            let mountpoint = mount_point.unwrap();
            let mut options = vec![MountOption::RW, MountOption::FSName("seal".to_string())];
            if auto_unmount {
                options.push(MountOption::AutoUnmount);
            }
            if allow_root {
                options.push(MountOption::AllowRoot);
            }

            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            runtime.block_on(connect_to_manager(manager_address))?;

            info!("init_volume");
            runtime.block_on(CLIENT.init_volume(&volume_name.unwrap()))?;

            info!("connect_servers");
            runtime.block_on(CLIENT.connect_servers())?;

            info!("start fuse");

            fuser::mount2(SealFS, mountpoint, &options).unwrap();
            /* TODO
            thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 107, kind: NotConnected, message: "Transport endpoint is not connected" }', sealfs-rust/client/src/lib.rs:162:49

            should be fixed by checking if the mountpoint is valid
            */

            Ok(())
        }
        Commands::Add {
            server_address,
            weight,
            manager_address,
        } => {
            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            let result = runtime.block_on(connect_to_manager(manager_address));
            match result {
                Ok(_) => info!("init manager success"),
                Err(e) => panic!("init manager failed, error = {}", e),
            }

            let new_servers_info = vec![(server_address.unwrap(), weight.unwrap_or(100))];
            let result = runtime.block_on(CLIENT.add_new_servers(new_servers_info));

            match result {
                Ok(_) => {
                    info!("add server success");
                }
                Err(e) => {
                    info!("add server failed, error = {}", e);
                }
            }
            Ok(())
        }
        Commands::Delete {
            server_address,
            manager_address,
        } => {
            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            let result = runtime.block_on(connect_to_manager(manager_address));
            match result {
                Ok(_) => info!("init manager success"),
                Err(e) => panic!("init manager failed, error = {}", e),
            }

            let new_servers_info = vec![server_address.unwrap()];
            let result = runtime.block_on(CLIENT.delete_servers(new_servers_info));

            match result {
                Ok(_) => {
                    info!("add server success");
                }
                Err(e) => {
                    info!("add server failed, error = {}", e);
                }
            }
            Ok(())
        }
        Commands::ListServers {
            _list,
            _manager_address,
        } => todo!(),
        Commands::ListVolumes {
            _list,
            _manager_address,
        } => todo!(),
        Commands::Status {
            _status,
            _manager_address,
        } => todo!(),
    }
}
