// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
struct Uid {
    _sign_bits: i64,
    _time_bits: i64,
    //use later
    //work_bits:i32,
    _sequence_bits: i64,
    _time_stamp: i64,
    _sequence: i64,
    _last_time_stamp: i64,
    _sequence_mask: i64,
}

impl Uid {
    fn _new(sign_bits: i64, time_bits: i64, sequence_bits: i64) -> Self {
        let time = Self::_get_current_time();
        Uid {
            _sign_bits: sign_bits,
            _time_bits: time_bits,
            _sequence_bits: sequence_bits,
            _time_stamp: 0,
            _sequence: 0,
            _last_time_stamp: time,
            _sequence_mask: -1 ^ (-1 << sequence_bits) as i64,
        }
    }

    fn _get_current_time() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn _get_next_time(&self) -> i64 {
        let mut time = Self::_get_current_time();
        while time < self._last_time_stamp {
            time = Self::_get_current_time();
        }
        time
    }

    fn _get_next_id(&mut self) -> i64 {
        let mut time = Self::_get_current_time();
        if time < self._last_time_stamp {
            panic!("clock callback error!")
        }
        if time == self._last_time_stamp {
            self._sequence = (self._sequence + 1) & self._sequence_mask;
            if self._sequence == 0 {
                time = Self::_get_next_time(self);
            }
        } else {
            self._sequence = 0;
        }
        self._last_time_stamp = time;
        time << self._sequence_bits | self._sequence
    }
}

impl Default for Uid {
    fn default() -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Uid {
            _sign_bits: 1,
            _time_bits: 51,
            _sequence_bits: 12,
            _time_stamp: 0,
            _sequence: 0,
            _last_time_stamp: time,
            _sequence_mask: -1 ^ (-1 << 12) as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::uid::Uid;

    #[test]
    fn uid_test() {
        let mut uid = Uid::_new(1, 51, 12);
        let id = uid._get_next_id();
        let next_id = uid._get_next_id();
        println!("{:?}", uid);
        println!("{},{}", id, next_id);
    }
}
