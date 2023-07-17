// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use conhash::{ConsistentHash, Node};

#[derive(Clone)]
pub struct ServerNode {
    pub address: String,
}

impl Node for ServerNode {
    fn name(&self) -> String {
        self.address.clone()
    }
}

pub struct HashRing {
    pub ring: ConsistentHash<ServerNode>,
    pub servers: HashMap<String, usize>,
}

impl Clone for HashRing {
    fn clone(&self) -> Self {
        let servers = self.servers.clone();
        let mut ring = ConsistentHash::<ServerNode>::new();
        for (server, weight) in servers.iter() {
            ring.add(
                &ServerNode {
                    address: server.clone(),
                },
                *weight,
            );
        }
        HashRing { ring, servers }
    }
}

impl HashRing {
    pub fn new(servers: Vec<(String, usize)>) -> Self {
        let mut ring = ConsistentHash::<ServerNode>::new();
        let mut servers_map = HashMap::new();
        for (server, weight) in servers {
            ring.add(
                &ServerNode {
                    address: server.clone(),
                },
                weight,
            );
            servers_map.insert(server, weight);
        }
        HashRing {
            ring,
            servers: servers_map,
        }
    }

    pub fn get(&self, key: &str) -> Option<&ServerNode> {
        self.ring.get_str(key)
    }

    pub fn add(&mut self, server: ServerNode, weight: usize) {
        self.ring.add(&server, weight);
        self.servers.insert(server.address, weight);
    }

    pub fn remove(&mut self, server: &ServerNode) {
        self.ring.remove(server);
        self.servers.remove(&server.address);
    }

    pub fn contains(&self, server: &str) -> bool {
        self.servers.contains_key(server)
    }

    pub fn get_server_lists(&self) -> Vec<String> {
        self.servers.keys().cloned().collect()
    }
}
