// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap;
use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time;
use tokio::time::MissedTickBehavior;

const UNHEALTHY_TIME: u64 = 15;

#[derive(Default)]
pub struct Heart {
    pub instances: DashMap<String, u64>,
}

impl Heart {
    pub async fn register_server(&self, address: String, _lifetime: String) {
        self.instances.insert(
            address,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    pub async fn healthy_check(&self) {
        let instances = self.instances.clone();
        tokio::spawn(async move{
            let mut interval = time::interval(time::Duration::from_secs(5));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                instances.iter().for_each(|instance| {
                    let time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let time_past = instance.value();
                    if time - time_past >= UNHEALTHY_TIME {
                        instances.remove(instance.key());
                    }
                });
                interval.tick().await;
            }
        });
    }
}
