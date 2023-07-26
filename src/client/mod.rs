// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0
pub mod daemon;
pub mod fuse_client;

use clap::{Parser, Subcommand};
use fuser::{
    Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    ReplyWrite, Request,
};
use log::{error, info};
use std::{ffi::OsStr, str::FromStr, sync::Arc};

use crate::{
    client::daemon::{LocalCli, SealfsFused},
    common::{
        errors::status_to_string,
        info_syncer::{init_network_connections, ClientStatusMonitor, InfoSyncer},
    },
    rpc::server::RpcServer,
};

use self::fuse_client::Client;

const LOCAL_PATH: &str = "/tmp/sealfs.sock";
const LOCAL_INDEX_PATH: &str = "/tmp/sealfs.index";

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
    CreateVolume {
        /// Create a volume with a given mount point and size
        #[arg(required = true, name = "mount-point")]
        mount_point: Option<String>,

        /// Size of the volume
        #[arg(required = true, name = "volume-size")]
        volume_size: Option<u64>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,
    },
    DeleteVolume {
        /// Create a volume with a given mount point and size
        #[arg(required = true, name = "mount-point")]
        mount_point: Option<String>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,
    },
    Daemon {
        /// Start a daemon that hosts volumes

        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,

        /// index file
        #[arg(long = "index-file", name = "index-file")]
        index_file: Option<String>,

        /// socket path
        #[arg(long = "socket-path", name = "socket-path")]
        socket_path: Option<String>,

        /// clean socket file
        #[arg(long = "clean-socket", name = "clean-socket")]
        clean_socket: bool,
    },
    Mount {
        /// Act as a client, and mount FUSE at given path
        #[arg(required = true, name = "mount-point")]
        mount_point: Option<String>,

        /// Remote volume name
        #[arg(required = true, name = "volume-name")]
        volume_name: Option<String>,

        #[arg(long = "socket-path", name = "socket-path")]
        socket_path: Option<String>,

        #[arg(long = "read-only", name = "read-only")]
        read_only: bool,
    },
    Umount {
        /// Unmount FUSE at given path

        #[arg(required = true, name = "mount-point")]
        mount_point: Option<String>,

        #[arg(name = "socket-path")]
        socket_path: Option<String>,
    },
    Add {
        /// Add a server to the cluster
        #[arg(required = true, name = "server-address")]
        server_address: Option<String>,

        /// Weight of the server
        #[arg(long = "weight", name = "weight")]
        weight: Option<usize>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,
    },
    Delete {
        /// Delete a server from the cluster
        #[arg(required = true, name = "server-address")]
        server_address: Option<String>,

        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,
    },
    ListServers {
        /// List all servers in the cluster
        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        _manager_address: Option<String>,
    },
    ListVolumes {
        /// List all servers in the cluster
        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-address")]
        manager_address: Option<String>,
    },
    ListMountpoints {
        #[arg(name = "socket-path")]
        socket_path: Option<String>,
    },
    Status {
        /// Address of the manager
        #[arg(short = 'm', long = "manager-address", name = "manager-ddress")]
        manager_address: Option<String>,
    },
    Probe {
        #[arg(name = "socket-path")]
        socket_path: Option<String>,
        // Probe the local client
    },
}

struct SealFS {
    client: Arc<Client>,
    volume_root_inode: u64,
}

impl SealFS {
    fn new(client: Arc<Client>, volume_root_inode: u64) -> Self {
        Self {
            client,
            volume_root_inode,
        }
    }
}

impl Filesystem for SealFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup, parent = {}, name = {:?}", parent, name);
        let client = self.client.clone();
        let name = name.to_owned();
        let parent = if parent == 1 {
            self.volume_root_inode
        } else {
            parent
        };
        self.client
            .handle
            .spawn(async move { client.lookup_remote(parent, name, reply).await });
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
        let parent = if parent == 1 {
            self.volume_root_inode
        } else {
            parent
        };
        let client = self.client.clone();
        let name = name.to_owned();
        self.client.handle.spawn(async move {
            client
                .create_remote(parent, name, mode, umask, flags, reply)
                .await
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        info!("getattr, ino = {}", ino);
        let client = self.client.clone();
        let ino = if ino == 1 {
            self.volume_root_inode
        } else {
            ino
        };
        self.client
            .handle
            .spawn(async move { client.getattr_remote(ino, reply).await });
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, reply: ReplyDirectory) {
        info!("readdir, ino = {}, offset = {}", ino, offset);
        let client = self.client.clone();
        let ino = if ino == 1 {
            self.volume_root_inode
        } else {
            ino
        };
        self.client
            .handle
            .spawn(async move { client.readdir_remote(ino, offset, reply).await });
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
        let client = self.client.clone();
        let ino = if ino == 1 {
            self.volume_root_inode
        } else {
            ino
        };
        self.client
            .handle
            .spawn(async move { client.read_remote(ino, offset, size, reply).await });
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
        let client = self.client.clone();
        let data = data.to_owned();
        let ino = if ino == 1 {
            self.volume_root_inode
        } else {
            ino
        };
        self.client.handle.spawn(async move {
            client
                .write_remote(ino, offset, data.to_owned(), reply)
                .await
        });
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
        let client = self.client.clone();
        let name = name.to_owned();
        let parent = if parent == 1 {
            self.volume_root_inode
        } else {
            parent
        };
        self.client.handle.spawn(async move {
            client
                .mkdir_remote(parent, name.to_owned(), mode, reply)
                .await
        });
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let client = self.client.clone();
        let ino = if ino == 1 {
            self.volume_root_inode
        } else {
            ino
        };
        self.client
            .handle
            .spawn(async move { client.open_remote(ino, flags, reply).await });
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        info!("unlink");
        let client = self.client.clone();
        let name = name.to_owned();
        let parent = if parent == 1 {
            self.volume_root_inode
        } else {
            parent
        };
        self.client
            .handle
            .spawn(async move { client.unlink_remote(parent, name.to_owned(), reply).await });
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        info!("rmdir");
        let client = self.client.clone();
        let name = name.to_owned();
        let parent = if parent == 1 {
            self.volume_root_inode
        } else {
            parent
        };
        self.client
            .handle
            .spawn(async move { client.rmdir_remote(parent, name.to_owned(), reply).await });
    }
}

pub async fn run_command() -> Result<(), Box<dyn std::error::Error>> {
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

    let client = Arc::new(Client::new());

    match cli.command {
        Commands::CreateVolume {
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
            init_network_connections(manager_address, client.clone()).await;

            info!("connect_servers");
            if let Err(status) = client.connect_servers().await {
                error!(
                    "connect_servers failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            info!("create_volume");
            if let Err(status) = client
                .create_volume(&mountpoint, volume_size.unwrap())
                .await
            {
                error!(
                    "create_volume failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            Ok(())
        }
        Commands::DeleteVolume {
            mount_point,
            manager_address,
        } => {
            let mountpoint = mount_point.unwrap();

            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            init_network_connections(manager_address, client.clone()).await;

            info!("connect_servers");
            if let Err(status) = client.connect_servers().await {
                error!(
                    "connect_servers failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            info!("delete_volume");
            if let Err(status) = client.delete_volume(&mountpoint).await {
                error!(
                    "delete_volume failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            Ok(())
        }
        Commands::Daemon {
            index_file,
            manager_address,
            socket_path,
            clean_socket,
        } => {
            let index_file = match index_file {
                Some(file) => file,
                None => LOCAL_INDEX_PATH.to_owned(),
            };

            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };
            info!("init client");
            init_network_connections(manager_address, client.clone()).await;

            info!("connect_servers");
            if let Err(status) = client.connect_servers().await {
                error!(
                    "connect_servers failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            let sealfsd = SealfsFused::new(index_file, client);
            match sealfsd.init().await {
                Ok(_) => info!("sealfsd init success"),
                Err(e) => panic!("sealfsd init failed, error = {}", e),
            }

            let socket_path = match socket_path {
                Some(path) => path,
                None => LOCAL_PATH.to_owned(),
            };

            if clean_socket {
                if let Err(e) = std::fs::remove_file(&socket_path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        panic!("remove socket file failed, error = {}", e);
                    }
                }
            }

            let server = RpcServer::new(Arc::new(sealfsd), &socket_path);
            let result = server.run_unix_stream().await;
            match result {
                Ok(_) => info!("server run success"),
                Err(e) => {
                    panic!("server run failed, error = {}", e)
                }
            };
            Ok(())
        }
        Commands::Mount {
            mount_point,
            volume_name,
            socket_path,
            read_only,
        } => {
            let socket_path = match socket_path {
                Some(path) => path,
                None => LOCAL_PATH.to_owned(),
            };
            let local_client = LocalCli::new(socket_path.clone());

            if let Err(e) = local_client.add_connection(&socket_path).await {
                panic!("add connection failed, error = {}", status_to_string(e))
            }

            let result = local_client
                .mount(&volume_name.unwrap(), &mount_point.unwrap(), read_only)
                .await;
            match result {
                Ok(_) => info!("mount success"),
                Err(e) => panic!("mount failed, error = {}", status_to_string(e)),
            };

            Ok(())
        }
        Commands::Umount {
            mount_point,
            socket_path,
        } => {
            let socket_path = match socket_path {
                Some(path) => path,
                None => LOCAL_PATH.to_owned(),
            };
            let local_client = LocalCli::new(socket_path.clone());

            if let Err(e) = local_client.add_connection(&socket_path).await {
                panic!("add connection failed, error = {}", status_to_string(e))
            }

            let result = local_client.umount(&mount_point.unwrap()).await;
            match result {
                Ok(_) => info!("umount success"),
                Err(e) => panic!("umount failed, error = {}", status_to_string(e)),
            };

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
            init_network_connections(manager_address, client.clone()).await;

            let new_servers_info = vec![(server_address.unwrap(), weight.unwrap_or(100))];
            let result = client.add_new_servers(new_servers_info).await;

            match result {
                Ok(_) => {
                    info!("add server success");
                }
                Err(e) => {
                    info!("add server failed, error = {}", status_to_string(e))
                }
            };
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
            init_network_connections(manager_address, client.clone()).await;

            let new_servers_info = vec![server_address.unwrap()];
            let result = client.delete_servers(new_servers_info).await;

            match result {
                Ok(_) => {
                    info!("add server success");
                }
                Err(e) => {
                    info!("add server failed, error = {}", status_to_string(e))
                }
            };
            Ok(())
        }
        Commands::ListServers { _manager_address } => todo!(),
        Commands::ListVolumes { manager_address } => {
            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };
            info!("init client");
            init_network_connections(manager_address, client.clone()).await;

            info!("connect_servers");
            if let Err(status) = client.connect_servers().await {
                error!(
                    "connect_servers failed, status = {:?}",
                    status_to_string(status)
                );
                return Ok(());
            }

            let result = client.list_volumes().await;
            match result {
                Ok(volumes) => {
                    info!("list volumes success");
                    for volume in volumes {
                        println!("{}", volume);
                    }
                }
                Err(e) => {
                    info!("list volumes failed, error = {}", status_to_string(e))
                }
            };
            Ok(())
        }
        Commands::ListMountpoints { socket_path } => {
            let socket_path = match socket_path {
                Some(path) => path,
                None => LOCAL_PATH.to_owned(),
            };
            let local_client = LocalCli::new(socket_path.clone());

            if let Err(e) = local_client.add_connection(&socket_path).await {
                panic!("add connection failed, error = {}", status_to_string(e))
            }

            let result = local_client.list_mountpoints().await;
            match result {
                Ok(mountpoints) => {
                    info!("list mountpoints success");
                    for mountpoint in mountpoints {
                        println!("{}, {}", mountpoint.0, mountpoint.1);
                    }
                }
                Err(e) => {
                    info!("list mountpoints failed, error = {}", status_to_string(e))
                }
            };
            Ok(())
        }
        Commands::Status { manager_address } => {
            let manager_address = match manager_address {
                Some(address) => address,
                None => "127.0.0.1:8081".to_owned(),
            };

            info!("init client");
            init_network_connections(manager_address, client.clone()).await;
            let result = client.get_cluster_status().await;
            match result {
                Ok(status) => {
                    info!("get cluster status success");
                    println!("{}", status);
                }
                Err(e) => {
                    info!("get cluster status failed, error = {}", status_to_string(e))
                }
            };
            Ok(())
        }
        Commands::Probe { socket_path } => {
            let socket_path = match socket_path {
                Some(path) => path,
                None => LOCAL_PATH.to_owned(),
            };
            let local_client = LocalCli::new(socket_path.clone());

            if let Err(e) = local_client.add_connection(&socket_path).await {
                panic!("add connection failed, error = {}", status_to_string(e))
            }

            let result = local_client.probe().await;
            match result {
                Ok(_) => info!("probe success"),
                Err(e) => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("probe failed, error = {}", status_to_string(e)),
                    )))
                }
            };

            Ok(())
        }
    }
}
