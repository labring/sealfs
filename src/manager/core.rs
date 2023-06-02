use std::sync::{Arc, Mutex, RwLock};

use ahash::{HashMap, HashMapExt};
use anyhow::Error;
use dashmap::DashMap;
use log::{debug, info};

use crate::common::hash_ring::{HashRing, ServerNode};
use crate::common::serialization::{ClusterStatus, ServerStatus, ServerType};
pub struct Manager {
    hashring: Arc<RwLock<Option<HashRing>>>,
    new_hashring: Arc<RwLock<Option<HashRing>>>,
    servers: Arc<Mutex<HashMap<String, Server>>>,
    cluster_status: Arc<Mutex<ClusterStatus>>,
    _clients: DashMap<String, String>,
}

pub struct Server {
    status: ServerStatus,
    r#_type: ServerType,
    _replicas: usize,
}

impl Manager {
    pub fn new(servers: Vec<(String, usize)>) -> Self {
        let hashring = Arc::new(RwLock::new(Some(HashRing::new(servers.clone()))));
        let manager = Manager {
            hashring,
            new_hashring: Arc::new(RwLock::new(None)),
            servers: Arc::new(Mutex::new(HashMap::new())),
            cluster_status: Arc::new(Mutex::new(ClusterStatus::Initializing)),
            _clients: DashMap::new(),
        };

        for (server, weight) in servers {
            manager.servers.lock().unwrap().insert(
                server,
                Server {
                    status: ServerStatus::Initializing,
                    r#_type: ServerType::Running,
                    _replicas: weight,
                },
            );
        }

        manager
    }

    pub fn get_cluster_status(&self) -> ClusterStatus {
        let status = *self.cluster_status.lock().unwrap();
        debug!("get_cluster_status: {:?}", status);
        status
    }

    pub fn get_hash_ring_info(&self) -> Vec<(String, usize)> {
        self.hashring
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .servers
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn get_new_hash_ring_info(&self) -> Result<Vec<(String, usize)>, Error> {
        if let Some(new_hashring) = self.new_hashring.read().unwrap().as_ref() {
            Ok(new_hashring
                .servers
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect())
        } else {
            Err(anyhow::anyhow!("new hashring is none"))
        }
    }

    pub fn add_nodes(&self, nodes: Vec<(String, usize)>) -> Option<Error> {
        info!("add_nodes: {:?}", nodes);
        let mut cluster_status = self.cluster_status.lock().unwrap();
        if *cluster_status != ClusterStatus::Idle {
            return Some(anyhow::anyhow!("cluster is not idle"));
        }
        let mut new_hashring = self.hashring.read().unwrap().clone().unwrap();
        let mut servers = self.servers.lock().unwrap();
        for (node, weight) in nodes {
            new_hashring.add(
                ServerNode {
                    address: node.clone(),
                },
                weight,
            );
            servers.insert(
                node,
                Server {
                    status: ServerStatus::Initializing,
                    r#_type: ServerType::Running,
                    _replicas: weight,
                },
            );
        }

        self.new_hashring.write().unwrap().replace(new_hashring);
        *cluster_status = ClusterStatus::NodesStarting;

        None
    }

    pub fn delete_nodes(&self, nodes: Vec<String>) -> Option<Error> {
        let mut cluster_status = self.cluster_status.lock().unwrap();
        if *cluster_status != ClusterStatus::Idle {
            return Some(anyhow::anyhow!("cluster is not idle"));
        }
        let mut new_hashring = self.hashring.read().unwrap().clone().unwrap();
        new_hashring.remove(&ServerNode {
            address: nodes[0].clone(),
        });

        self.new_hashring.write().unwrap().replace(new_hashring);

        *cluster_status = ClusterStatus::SyncNewHashRing;
        None
    }

    pub fn set_server_status(&self, server_id: String, status: ServerStatus) -> Option<Error> {
        // debug : logs all server_name in self.servers
        debug!(
            "set_server_status: {:?}",
            self.servers
                .lock()
                .unwrap()
                .iter()
                .map(|kv| kv.0.clone())
                .collect::<Vec<String>>()
        );

        info!("set server status: {} {:?}", server_id, status);

        match status {
            ServerStatus::Initializing => {
                panic!("cannot set server status to init");
            }
            ServerStatus::PreTransfer => {
                let mut cluster_status = self.cluster_status.lock().unwrap();
                if *cluster_status != ClusterStatus::SyncNewHashRing {
                    return Some(anyhow::anyhow!("cannot pretransfer for server: {}, cluster is not SyncNewHashRing: status: {:?}" , server_id, *cluster_status));
                }
                let mut servers = self.servers.lock().unwrap();
                if servers.get(&server_id).unwrap().status != ServerStatus::Finished {
                    return Some(anyhow::anyhow!(
                        "cannot pretransfer for server: {}, server is not finish: status: {:?}",
                        server_id,
                        servers.get(&server_id).unwrap().status
                    ));
                }
                servers.get_mut(&server_id).unwrap().status = ServerStatus::PreTransfer;
                // if every server is pretransfer, then change cluster_status to pretransfer
                if servers
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::PreTransfer)
                {
                    *cluster_status = ClusterStatus::PreTransfer;
                }
                None
            }
            ServerStatus::Transferring => {
                let mut cluster_status = self.cluster_status.lock().unwrap();
                if *cluster_status != ClusterStatus::PreTransfer {
                    return Some(anyhow::anyhow!(
                        "cannot transfer for server: {}, cluster is not PreTransfer: status: {:?}",
                        server_id,
                        *cluster_status
                    ));
                }
                let mut servers = self.servers.lock().unwrap();
                if servers.get(&server_id).unwrap().status != ServerStatus::PreTransfer {
                    return Some(anyhow::anyhow!(
                        "cannot transfer for server: {}, server is not finish: status: {:?}",
                        server_id,
                        servers.get(&server_id).unwrap().status
                    ));
                }
                servers.get_mut(&server_id).unwrap().status = ServerStatus::Transferring;
                // if every server is transferring, then change cluster_status to transferring
                if servers
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Transferring)
                {
                    *cluster_status = ClusterStatus::Transferring;
                }
                None
            }
            ServerStatus::PreFinish => {
                let mut cluster_status = self.cluster_status.lock().unwrap();
                if *cluster_status != ClusterStatus::Transferring {
                    return Some(anyhow::anyhow!("cannot prefinish for server: {}, cluster is not Transferring: status: {:?}" , server_id, *cluster_status));
                }
                let mut servers = self.servers.lock().unwrap();
                if servers.get(&server_id).unwrap().status != ServerStatus::Transferring {
                    return Some(anyhow::anyhow!(
                        "cannot prefinish for server: {}, server is not transferring: status: {:?}",
                        server_id,
                        servers.get(&server_id).unwrap().status
                    ));
                }
                servers.get_mut(&server_id).unwrap().status = ServerStatus::PreFinish;
                // if every server is prefinish, and all old client is updated(TO), then change cluster_status to prefinish
                if servers
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::PreFinish)
                {
                    // manager should update all old hashring before updating cluster_status,
                    // so that the new client can not get the old hashring and PreFinish status at the same time
                    let _ = self
                        .hashring
                        .write()
                        .unwrap()
                        .replace(self.new_hashring.read().unwrap().clone().unwrap());
                    *cluster_status = ClusterStatus::PreFinish;
                }
                None
            }
            ServerStatus::Finishing => {
                let mut cluster_status = self.cluster_status.lock().unwrap();
                let mut servers: std::sync::MutexGuard<
                    std::collections::HashMap<String, Server, ahash::RandomState>,
                > = self.servers.lock().unwrap();
                if servers.get(&server_id).unwrap().status != ServerStatus::PreFinish {
                    return Some(anyhow::anyhow!(
                        "cannot finish for server: {}, server is not prefinish: status: {:?}",
                        server_id,
                        servers.get(&server_id).unwrap().status
                    ));
                }
                servers.get_mut(&server_id).unwrap().status = ServerStatus::Finishing;
                // if every server is finish, then change cluster_status to finish
                if servers
                    .iter()
                    .all(|kv| kv.1.status == ServerStatus::Finishing)
                {
                    *cluster_status = ClusterStatus::Finishing;
                    // // remove servers which is not in new_hashring
                    // let mut new_hashring = self.new_hashring.write().unwrap();
                    // servers.retain(|k, _| new_hashring.as_ref().unwrap().contains(k));
                    // // move new_hashring to hashring
                    // new_hashring.take().unwrap();
                }
                None
            }
            ServerStatus::Finished => {
                let mut cluster_status = self.cluster_status.lock().unwrap();
                match *cluster_status {
                    ClusterStatus::Finishing => {
                        let mut servers: std::sync::MutexGuard<std::collections::HashMap<String, Server, ahash::RandomState>> = self.servers.lock().unwrap();
                        if servers.get(&server_id).unwrap().status != ServerStatus::Finishing {
                            return Some(anyhow::anyhow!("cannot finish for server: {}, server is not Finishing: status: {:?}", server_id, servers.get(&server_id).unwrap().status));
                        }
                        servers.get_mut(&server_id).unwrap().status = ServerStatus::Finished;
                        // if every server is finish, then change cluster_status to finish
                        if servers.iter().all(|kv| kv.1.status == ServerStatus::Finished) {
                            // remove servers which is not in new_hashring
                            let mut new_hashring = self.new_hashring.write().unwrap();
                            servers.retain(|k, _| new_hashring.as_ref().unwrap().contains(k));
                            // move new_hashring to hashring
                            let _ = new_hashring.take().unwrap();
                            *cluster_status = ClusterStatus::Idle;
                        }
                        None
                    }
                    ClusterStatus::Initializing => {
                        let mut servers = self.servers.lock().unwrap();
                        if servers.get(&server_id).unwrap().status != ServerStatus::Initializing {
                            return Some(anyhow::anyhow!(
                                "cannot finish for server: {}, server is not Initializing: status: {:?}",
                                server_id,
                                servers.get(&server_id).unwrap().status
                            ));
                        }
                        servers.get_mut(&server_id).unwrap().status = ServerStatus::Finished;
                        // if every server is finish, then change cluster_status to finish
                        if servers.iter().all(|kv| kv.1.status == ServerStatus::Finished) {
                            *cluster_status = ClusterStatus::Idle;
                        }
                        None
                    }
                    ClusterStatus::NodesStarting => {
                        let mut servers = self.servers.lock().unwrap();
                        if !self
                            .new_hashring
                            .read()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .contains(&server_id)
                        {
                            return Some(anyhow::anyhow!(
                                "cannot finish for server: {}, server is not in new_hashring",
                                server_id
                            ));
                        }
                        if servers.get(&server_id).unwrap().status != ServerStatus::Initializing {
                            return Some(anyhow::anyhow!(
                                "cannot finish for server: {}, server is not Initializing: status: {:?}",
                                server_id,
                                servers.get(&server_id).unwrap().status
                            ));
                        }
                        servers.get_mut(&server_id).unwrap().status = ServerStatus::Finished;
                        // if every server is finish, then change cluster_status to finish
                        if servers.iter().all(|kv| kv.1.status == ServerStatus::Finished) {
                            *cluster_status = ClusterStatus::SyncNewHashRing;
                        }
                        None
                    }
                    _ => {
                        Some(anyhow::anyhow!(
                            "cannot finish for server: {}, cluster is not Finishing, Init or AddNodes: status: {:?}",
                            server_id,
                            *cluster_status
                        ))
                    }
                }
            }
        }
    }
}
