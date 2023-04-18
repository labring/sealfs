use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Error;
use serde::{Serialize, Deserialize};

use crate::common::hash_ring::{HashRing, ServerNode};
pub struct Manager {
    hashring: Arc<RwLock<HashRing>>,
    new_hashring: Option<Arc<RwLock<HashRing>>>,
    servers: HashMap<String, Server>,
    cluster_status: ClusterStatus,
}


pub struct Node {
    id: String,
    hash: u64,
}

pub struct Server {
    status: ServerStatus,
    replicas: usize,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ServerStatus {
    Idle,
    Transferring,
    PreFinish,
    Finish
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum ClusterStatus {
    Idle,
    Updating,
    PreFinish,
    Finish
}

impl Manager {
    pub fn new(servers: Vec<(String, usize)>) -> Self {
        let hashring = Arc::new(RwLock::new(HashRing::new(servers.clone())));
        let mut manager = Manager {
            hashring,
            new_hashring: None,
            servers: HashMap::new(),
            cluster_status: ClusterStatus::Idle,
        };

        for (server, weight) in servers {
            manager.servers.insert(server, Server {
                status: ServerStatus::Idle,
                replicas: weight,
            });
        }

        manager
    }

    pub fn get_cluster_status(&self) -> ClusterStatus {
        self.cluster_status
    }

    pub fn get_hash_ring_info(&self) -> Vec<(String, usize)> {
        self.hashring.read().unwrap().servers.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn get_new_hash_ring_info(&self) -> Option<Vec<(String, usize)>> {
        if let Some(new_hashring) = &self.new_hashring {
            Some(new_hashring.read().unwrap().servers.iter().map(|(k, v)| (k.clone(), *v)).collect())
        } else {
            None
        }
    }

    pub fn add_nodes(&mut self, nodes: Vec<(String, usize)>) -> Option<Error> {
        let hashring = self.hashring.read().unwrap();
        self.cluster_status = ClusterStatus::Updating;
        let mut new_hashring = hashring.clone();
        for (node, weight) in nodes.clone() {
            new_hashring.add(ServerNode { address: node.clone() }, weight);
        }

        self.new_hashring = Some(Arc::new(RwLock::new(new_hashring)));
        self.notify_status();

        None
    }

    pub fn delete_nodes(&mut self, nodes: Vec<String>) -> Option<Error> {
        let mut hashring = self.hashring.read().unwrap();
        self.cluster_status = ClusterStatus::Updating;
        let mut new_hashring = hashring.clone();
        new_hashring.remove(&ServerNode { address: nodes[0].clone() });

        self.new_hashring = Some(Arc::new(RwLock::new(new_hashring)));
        self.notify_status();

        None
    }

    pub fn pre_finish_server(&mut self, server_id: String) {
        let mut server = self.servers.get_mut(&server_id).unwrap();
        server.status = ServerStatus::PreFinish;
        // 如果每个server都prefinish了，那么就可以把cluster_status改成prefinish
        if self.servers.iter().all(|(_, server)| server.status == ServerStatus::PreFinish) {
            self.cluster_status = ClusterStatus::PreFinish;
            self.notify_status();
        }
    }

    pub fn finish_server(&mut self, server_id: String) {
        let mut server = self.servers.get_mut(&server_id).unwrap();
        server.status = ServerStatus::Finish;
        // 如果每个server都finish了，那么就可以把cluster_status改成prefinish
        if self.servers.iter().all(|(_, server)| server.status == ServerStatus::Finish) {
            self.cluster_status = ClusterStatus::Finish;
            self.notify_status();
        }
    }

    pub fn notify_status(&self) {

    }
}
