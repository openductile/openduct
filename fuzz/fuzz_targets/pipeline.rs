// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing a pipeline file (`openduct_hostile::pipeline`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| openduct_hostile::pipeline(data));
