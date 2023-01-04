// Copyright 2022 labring. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub fn array2u32(array: &[u8]) -> u32 {
    (array[0] as u32)
        + ((array[1] as u32) << 8)
        + ((array[2] as u32) << 16)
        + ((array[3] as u32) << 24)
}
