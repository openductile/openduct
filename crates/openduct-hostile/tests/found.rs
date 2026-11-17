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

/// A binary log from CI's first MySQL fuzzing timeout (2026-11-17). It is read in milliseconds here; kept as found.
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

/// A binary log whose rows event marks no column present (2026-11-17): each row took no bytes, so its rows were read
/// for ever, and memory grew with them. Such a row is refused now.
#[test]
fn a_second_mysql_binlog_found_by_the_fuzzer_is_read_quickly() {
    let input: &[u8] = &[
        0, 0, 44, 0, 32, 0, 0, 19, 0, 0, 0, 0, 44, 0, 0, 0, 0, 1, 28, 155, 137, 65, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 191, 6, 0, 0, 0, 121, 0, 32, 0, 0, 76, 0, 0, 0, 0, 44, 0, 32, 0, 0, 19, 0, 0, 0, 0, 44, 0,
        0, 0, 0, 1, 28, 155, 137, 210, 240, 7, 16, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 28, 155, 0, 0, 44, 0, 0,
        0, 0, 23, 0, 0, 0, 0, 44, 0, 0, 0, 0, 1, 28, 155, 137, 0, 0, 0, 0, 0, 0, 0, 23, 22, 40, 23, 254, 30,
        179, 156, 23, 23, 23, 100, 0, 0, 0, 0, 0, 0, 4, 10, 0, 0, 0, 0, 0, 1, 0, 0, 44, 0, 32, 0, 0, 19, 0,
        0, 0, 0, 44, 0, 0, 0, 0, 1, 28, 126, 0, 83, 10, 155, 137, 0, 0, 0, 28, 155, 0, 0, 5, 0, 0, 0, 0, 0,
        44, 0, 0, 0, 0, 23, 0, 0, 0, 0, 44, 0, 0, 0, 0, 1, 28, 155, 137, 0, 0, 0, 15, 0, 0, 0, 0, 4, 64, 1,
        0, 0, 0, 0, 0, 1, 28, 155, 137, 0, 0, 0, 28, 155, 0, 0, 44, 0, 0, 0, 0, 23, 0, 0, 0, 0, 44, 0, 0, 0,
        0, 1, 28, 155, 137, 0, 0, 0, 0, 0, 0, 0, 23, 0, 0, 0, 1, 28, 155, 137, 0, 0, 0, 0, 15, 0, 22, 40, 23,
        254, 30, 179, 156, 23, 23, 23, 100, 0, 0, 0, 0, 0, 0, 4, 10, 0, 0, 0, 0, 0, 1, 0, 0, 44, 0, 32, 0, 0,
        19, 0, 0, 0,
    ];
    within(Duration::from_secs(5), move || openduct_hostile::mysql(input));
}
