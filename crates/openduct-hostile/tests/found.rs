// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Inputs the fuzzer found, kept so they stay fixed: each must be handled, quickly.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

/// Answers within `limit`, or fails the test instead of hanging it.
fn within(limit: Duration, work: impl FnOnce() + Send + 'static) {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        work();
        let _ = done.send(());
    });
    let started = Instant::now();
    assert!(
        finished.recv_timeout(limit).is_ok(),
        "still working after {:?}",
        started.elapsed()
    );
}

/// A binary log the fuzzer found (2026-11-17) that kept the MySQL reader working for good.
#[test]
fn a_mysql_binlog_found_by_the_fuzzer_is_read_quickly() {
    let input: &[u8] = &[
        10, 0, 0, 0, 59, 23, 23, 255, 23, 19, 59, 59, 59, 59, 59, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 231,
        144, 59, 59, 0, 0, 0, 0, 0, 11, 0, 0, 0, 239, 197, 0, 0, 7, 35, 7, 0, 0, 32, 0, 0, 0, 55, 5, 98, 155,
        128, 0, 0, 0, 0, 0, 0, 0, 59, 23, 23, 255, 23, 19, 59, 59, 59, 59, 59, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0,
        0, 0, 231, 144, 59, 59, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 197, 0, 0, 7, 35, 7, 0, 0, 32, 0, 0, 12,
    ];
    within(Duration::from_secs(5), move || openduct_hostile::mysql(input));
}
