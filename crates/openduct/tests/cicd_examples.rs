// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The ready-made CI of `examples/ci`: its workflow files pass `openduct workflow check` (and those with a sample,
//! `openduct workflow test`), and the GitHub Actions and GitLab CI files hold the jobs they say - check on every
//! change, push to test after a merge, promote to production on a tag or by hand - each with commands the program
//! has and its token from the CI's secrets.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "studio")]

use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

fn examples() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/ci")
}

fn run(args: &[&str]) -> (bool, String) {
    let out = Command::new(OPENDUCT).args(args).output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn the_example_workflows_pass_check_and_test() {
    let dir = examples().join("workflows");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    assert!(files.len() >= 2);
    for file in files {
        let path = file.to_str().unwrap();
        let (ok, out) = run(&["workflow", "check", path]);
        assert!(ok, "{path}: {out}");
        assert!(out.contains("ready"), "{out}");
        if std::fs::read_to_string(&file).unwrap().contains("\nsample:") {
            let (ok, out) = run(&["workflow", "test", path]);
            assert!(ok, "{path}: {out}");
            let first: Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
            assert_ne!(first["status"], "failed", "{path}: {out}");
        }
    }
}

fn yaml(name: &str) -> (String, Value) {
    let text = std::fs::read_to_string(examples().join(name)).unwrap();
    let json = openduct_flow::yaml_to_json(&text).unwrap();
    (text, json)
}

#[test]
fn the_ci_files_check_push_to_test_and_promote_to_production() {
    let (_, help) = run(&["workflow", "--help"]);
    for command in ["check", "test", "pull", "diff", "push", "rollback", "promote"] {
        assert!(help.contains(command), "openduct workflow {command}: {help}");
    }

    let (text, github) = yaml("github-actions.yml");
    let jobs = github["jobs"].as_object().unwrap();
    for job in ["check", "push-to-test", "promote-to-production"] {
        assert!(jobs.contains_key(job), "GitHub Actions: {job}");
    }
    assert!(github["on"]["pull_request"].is_object() && github["on"]["push"]["tags"].is_array());
    assert!(github["on"].get("workflow_dispatch").is_some(), "by hand");
    let runs = |job: &str| -> String {
        jobs[job]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["run"].as_str())
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert!(runs("check").contains("openduct workflow check"));
    assert!(runs("push-to-test").contains("openduct workflow push workflows"));
    assert!(runs("promote-to-production").contains("openduct workflow promote --all"));
    assert_eq!(jobs["promote-to-production"]["environment"], "production");
    assert!(text.contains("sha256sum -c"), "the release's checksum is checked");
    assert!(
        text.contains("${{ secrets.OPENDUCT_TEST_TOKEN }}")
            && text.contains("${{ secrets.OPENDUCT_PRODUCTION_TOKEN }}")
    );
    assert!(!text.contains("odapi_"), "no token in the file");
    for step in jobs.values().flat_map(|j| j["steps"].as_array().unwrap()) {
        if let Some(uses) = step["uses"].as_str() {
            let (_, pin) = uses.split_once('@').unwrap();
            assert_eq!(pin.len(), 40, "actions pinned to a commit: {uses}");
        }
    }

    let (text, gitlab) = yaml("gitlab-ci.yml");
    for job in ["check", "diff", "push-to-test", "promote-to-production"] {
        assert!(gitlab[job]["script"].is_array(), "GitLab CI: {job}");
    }
    assert!(
        gitlab["default"]["image"]["name"]
            .as_str()
            .unwrap()
            .contains("openduct"),
        "the released container"
    );
    let script = |job: &str| {
        gitlab[job]["script"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert!(script("check").contains("openduct workflow check"));
    assert!(script("push-to-test").contains("openduct workflow push workflows"));
    assert!(script("promote-to-production").contains("openduct workflow promote --all"));
    assert_eq!(gitlab["promote-to-production"]["environment"], "production");
    assert_eq!(
        gitlab["push-to-test"]["variables"]["OPENDUCT_TOKEN"],
        "$OPENDUCT_TEST_TOKEN"
    );
    assert!(!text.contains("odapi_"), "no token in the file");
}
