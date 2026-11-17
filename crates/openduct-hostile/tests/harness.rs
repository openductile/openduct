// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The hostile-input harness checks itself: a panic in a task the broker runs is seen, though it ends only its own
//! connection. (In a file of its own: the deliberate panic would otherwise reach the other tests' broker.)

// A test file: a failed step should stop the test where it failed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[test]
fn a_panic_on_another_thread_is_reported() {
    // ApiVersions v3 with an empty body, in a two-byte-length chunk.
    let api_versions = [0, 18, 0, 3, 0, 0, 0, 1, 0, 1, b'h', 0, 0, 0, 0];
    let mut input = u16::try_from(api_versions.len()).unwrap().to_be_bytes().to_vec();
    input.extend_from_slice(&api_versions);
    openduct_hostile::broker(&input).unwrap();
    std::thread::spawn(|| panic!("a task's panic"))
        .join()
        .unwrap_err();
    let reported = openduct_hostile::broker(&input).unwrap_err();
    assert!(reported.contains("a task's panic"), "{reported}");
    // Taken once: the next input is judged on its own.
    openduct_hostile::broker(&input).unwrap();
}
