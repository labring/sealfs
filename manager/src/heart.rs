// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use dashmap;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time;
use tokio::time::MissedTickBehavior;

const UNHEALTHY_TIME: u64 = 15;

lazy_static! {
    static ref HEALTHY_MAP: DashMap<String, u64> = DashMap::new();
}

pub async fn register_server(address: String, _lifetime: String) {
    HEALTHY_MAP.insert(
        address,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
}

pub async fn healthy_check() {
    let mut interval = time::interval(time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        HEALTHY_MAP.iter().for_each(|instance| {
            let time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let time_past = instance.value();
            if time - time_past >= UNHEALTHY_TIME {
                HEALTHY_MAP.remove(instance.key());
            }
        });
        interval.tick().await;
    }
}
