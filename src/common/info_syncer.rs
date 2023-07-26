use std::{
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use log::{debug, error, info};
use spin::RwLock;
use tokio::time::sleep;

use crate::common::errors::{self, status_to_string, CONNECTION_ERROR};

use super::{hash_ring::HashRing, sender::Sender, serialization::ClusterStatus};

#[async_trait]
pub trait InfoSyncer {
    async fn get_cluster_status(&self) -> Result<ClusterStatus, i32>;
    fn cluster_status(&self) -> &AtomicI32;
}

async fn sync_cluster_infos<I: InfoSyncer>(client: Arc<I>) {
    loop {
        {
            let result = client.get_cluster_status().await;
            match result {
                Ok(status) => {
                    let status = status.into();
                    if client.cluster_status().load(Ordering::Relaxed) != status {
                        client.cluster_status().store(status, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    info!("sync server infos failed, error = {}", e);
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[async_trait]
pub trait ClientStatusMonitor: InfoSyncer {
    fn hash_ring(&self) -> &Arc<RwLock<Option<HashRing>>>;
    fn new_hash_ring(&self) -> &Arc<RwLock<Option<HashRing>>>;
    fn sender(&self) -> &Sender;
    fn manager_address(&self) -> &Arc<tokio::sync::Mutex<String>>;

    fn get_address(&self, path: &str) -> String {
        self.hash_ring()
            .read()
            .as_ref()
            .unwrap()
            .get(path)
            .unwrap()
            .address
            .clone()
    }

    fn get_new_address(&self, path: &str) -> String {
        match self.new_hash_ring().read().as_ref() {
            Some(hash_ring) => hash_ring.get(path).unwrap().address.clone(),
            None => self.get_address(path),
        }
    }

    async fn get_hash_ring_info(&self) -> Result<Vec<(String, usize)>, i32> {
        self.sender()
            .get_hash_ring_info(&self.manager_address().lock().await)
            .await
    }
    async fn get_new_hash_ring_info(&self) -> Result<Vec<(String, usize)>, i32> {
        self.sender()
            .get_new_hash_ring_info(&self.manager_address().lock().await)
            .await
    }

    fn get_connection_address(&self, path: &str) -> String {
        let cluster_status = self.cluster_status().load(Ordering::Acquire);

        // check the ClusterStatus is not Idle
        // for efficiency, we use i32 operation to check the ClusterStatus
        if cluster_status == 301 {
            return self.get_address(path);
        }

        match cluster_status.try_into().unwrap() {
            ClusterStatus::Initializing => panic!("cluster status is not ready"),
            ClusterStatus::Idle => todo!(),
            ClusterStatus::NodesStarting => self.get_address(path),
            ClusterStatus::SyncNewHashRing => self.get_address(path),
            ClusterStatus::PreTransfer => self.get_address(path),
            ClusterStatus::Transferring => self.get_address(path),
            ClusterStatus::PreFinish => self.get_new_address(path),
            ClusterStatus::Finishing => self.get_address(path),
            ClusterStatus::StatusError => todo!(),
        }
    }

    async fn add_connection(&self, server_address: &str) -> Result<(), i32>;

    async fn connect_to_manager(&self, manager_address: &str) -> Result<(), i32> {
        self.manager_address()
            .lock()
            .await
            .push_str(manager_address);
        self.add_connection(manager_address).await.map_err(|e| {
            error!("add connection failed: {:?}", e);
            CONNECTION_ERROR
        })
    }

    async fn add_new_servers(&self, new_servers_info: Vec<(String, usize)>) -> Result<(), i32> {
        self.sender()
            .add_new_servers(&self.manager_address().lock().await, new_servers_info)
            .await
    }

    async fn connect_servers(&self) -> Result<(), i32> {
        debug!("init");

        let result = async {
            loop {
                match self
                    .cluster_status()
                    .load(Ordering::Acquire)
                    .try_into()
                    .unwrap()
                {
                    ClusterStatus::Idle => {
                        return self.get_hash_ring_info().await;
                    }
                    ClusterStatus::Initializing => {
                        info!("cluster is initalling, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    ClusterStatus::PreFinish => {
                        info!("cluster is initalling, wait for a while");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    s => {
                        error!("invalid cluster status: {}", s);
                        return Err(errors::INVALID_CLUSTER_STATUS);
                    }
                }
            }
        }
        .await;

        match result {
            Ok(all_servers_address) => {
                for server_address in &all_servers_address {
                    self.add_connection(&server_address.0).await?;
                }
                self.hash_ring()
                    .write()
                    .replace(HashRing::new(all_servers_address.clone()));
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

async fn client_watch_status<I: ClientStatusMonitor + std::marker::Sync + std::marker::Send>(
    client: Arc<I>,
) {
    loop {
        match client
            .cluster_status()
            .load(Ordering::Relaxed)
            .try_into()
            .unwrap()
        {
            ClusterStatus::SyncNewHashRing => {
                // here I write a long code block to deal with the process from SyncNewHashRing to new Idle status.
                // this is because we don't make persistent flags for status, so we could not check a status is finished or not.
                // so we have to check the status in a long code block, and we could not use a loop to check the status.
                // in the future, we will make persistent flags for status, and we separate the code block for each status.
                info!("Transfer: start to sync new hash ring");
                let all_servers_address = match client.get_new_hash_ring_info().await {
                    Ok(value) => value,
                    Err(e) => {
                        panic!("Get Hash Ring Info Failed. Error = {}", e);
                    }
                };
                info!("Transfer: get new hash ring info");

                for value in all_servers_address.iter() {
                    if client
                        .hash_ring()
                        .read()
                        .as_ref()
                        .unwrap()
                        .contains(&value.0)
                    {
                        continue;
                    }
                    if let Err(e) = client.add_connection(&value.0).await {
                        // TODO: we should rollback the transfer process
                        panic!("Add Connection Failed. Error = {}", e);
                    }
                }
                client
                    .new_hash_ring()
                    .write()
                    .replace(HashRing::new(all_servers_address));
                info!("Transfer: sync new hash ring finished");

                // wait for all servers to be PreTransfer

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    client.cluster_status().load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::SyncNewHashRing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        client.cluster_status().load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::PreTransfer
                );

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    client.cluster_status().load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::PreTransfer
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        client.cluster_status().load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Transferring
                );

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    client.cluster_status().load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::Transferring
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        client.cluster_status().load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::PreFinish
                );

                let _old_hash_ring = client
                    .hash_ring()
                    .write()
                    .replace(client.new_hash_ring().read().as_ref().unwrap().clone());

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    client.cluster_status().load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::PreFinish
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        client.cluster_status().load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Finishing
                );

                let _ = client.new_hash_ring().write().take();
                // here we should close connections to old servers, but now we just wait for remote servers to close connections and do nothing

                while <i32 as TryInto<ClusterStatus>>::try_into(
                    client.cluster_status().load(Ordering::Relaxed),
                )
                .unwrap()
                    == ClusterStatus::Finishing
                {
                    sleep(Duration::from_secs(1)).await;
                }
                assert!(
                    <i32 as TryInto<ClusterStatus>>::try_into(
                        client.cluster_status().load(Ordering::Relaxed)
                    )
                    .unwrap()
                        == ClusterStatus::Idle
                );

                info!("transferring data finished");
            }
            ClusterStatus::Idle => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::Initializing => {
                sleep(Duration::from_secs(1)).await;
            }
            ClusterStatus::NodesStarting => {
                sleep(Duration::from_secs(1)).await;
            }
            e => {
                panic!("cluster status error: {:?}", e as u32);
            }
        }
    }
}

pub async fn init_network_connections<
    I: ClientStatusMonitor + std::marker::Sync + std::marker::Send + 'static,
>(
    manager_address: String,
    client: Arc<I>,
) {
    if let Err(e) = client.connect_to_manager(&manager_address).await {
        panic!("connect to manager failed, err = {}", status_to_string(e));
    }
    tokio::spawn(sync_cluster_infos(client.clone()));
    tokio::spawn(client_watch_status(client));
}
