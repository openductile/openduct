// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing binary log events from MySQL (`openduct_hostile::mysql`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| openduct_hostile::mysql(data));
