// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fuzzing a Kafka request frame, as the broker reads one (`openduct_hostile::kafka_request`).

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| openduct_hostile::kafka_request(data));
