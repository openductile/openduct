// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing replication messages from PostgreSQL (`openduct_hostile::postgres`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| openduct_hostile::postgres(data));
