// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing the records inside a batch, as log compaction reads and rewrites them (`openduct_hostile::records`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| openduct_hostile::records(data));
