// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Storage in `openduct start`: the folders `--files-root` allows offered to the builder, a workflow started by a new
//! file in one of them that reads it and writes a copy beside it, the folder's trigger shown with how often it looks,
//! and a folder given wrongly refused before anything starts.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::process::Command;

use serde_json::json;
use studio::{call, owner, start_with, until};

#[test]
fn a_new_file_in_a_folder_the_admin_allows_runs_a_workflow_that_writes_beside_it() {
    let data = tempfile::tempdir().unwrap();
    let files = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(files.path().join("in")).unwrap();
    std::fs::write(files.path().join("in/old.csv"), "id\n1\n").unwrap();
    let root = format!("inbox={}", files.path().display());
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--files-root",
            &root,
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let roots = call(&studio, &signed, "GET", "/api/storage/roots", None);
    assert_eq!(roots.status, 200, "{}", roots.body);
    assert_eq!(roots.body["roots"], json!(["inbox"]));

    let definition = json!({
        "name": "Copy new orders",
        "trigger": { "storage": { "root": "inbox", "folder": "in", "pattern": "*.csv", "every": "10s" } },
        "steps": [
            { "name": "file", "storage": { "action": "read", "root": "inbox", "path": "{{ value.path }}" } },
            { "name": "rows", "extract_file": { "file": "steps.file" } },
            { "name": "copy", "storage": { "action": "write", "root": "inbox", "path": "out/{{ value.name }}",
                "file": "steps.file" } },
        ],
    });
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let on = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);
    until(30, "the trigger to be shown", || {
        let triggers = call(&studio, &signed, "GET", "/api/triggers", None);
        triggers.body["triggers"].as_array().is_some_and(|t| {
            t.iter()
                .any(|t| t["workflow"] == id && t["kind"] == "storage" && t["every_ms"] == 10_000)
        })
    });
    // The first look - at the next ten seconds of the clock - notes the file already there; one that comes after it
    // runs.
    std::thread::sleep(std::time::Duration::from_secs(15));
    std::fs::write(files.path().join("in/new.csv"), "id\n2\n").unwrap();
    until(60, "the new file's copy", || {
        files.path().join("out/new.csv").exists()
    });
    assert_eq!(
        std::fs::read_to_string(files.path().join("out/new.csv")).unwrap(),
        "id\n2\n"
    );
    assert!(
        !files.path().join("out/old.csv").exists(),
        "a file there before the first look does not run"
    );
    studio::stop(running);

    // A folder given wrongly: refused, saying why, before anything starts.
    let refused = Command::new(env!("CARGO_BIN_EXE_openduct"))
        .args(["start", "--memory", "--files-root", "inbox=relative/path"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    let said =
        String::from_utf8_lossy(&refused.stderr).into_owned() + &String::from_utf8_lossy(&refused.stdout);
    assert!(said.contains("from the top of the disk"), "{said}");
}
