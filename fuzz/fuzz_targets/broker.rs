// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing requests sent to a running broker over TCP (`openduct_hostile::broker`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if let Err(why) = openduct_hostile::broker(data) {
        panic!("{why}");
    }
});
