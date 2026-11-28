// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The kit for building an app (`openduct app ...`): a new app checks and tests at once; a mistake is found with where
//! it is; a recording that does not hold says what differs; `try` shows the request with the secrets hidden.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "studio")]

use std::path::Path;
use std::process::Command;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

fn app(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new(OPENDUCT)
        .arg("app")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn a_new_app_checks_and_tests_and_its_mistakes_are_found() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (ok, out) = app(d, &["new", "my_crm"]);
    assert!(ok, "{out}");
    let (ok, out) = app(d, &["check"]);
    assert!(ok && out.contains("✓ my_crm (My Crm): 2 actions"), "{out}");
    let (ok, out) = app(d, &["test"]);
    assert!(ok && out.contains("1 passed, 0 failed"), "{out}");

    // `try` shows the request; the key is hidden.
    let key = d.join("key.env");
    std::fs::write(&key, "api_key=sk_live_very_secret\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let (ok, out) = app(
        d,
        &[
            "try",
            "apps/my_crm",
            "add_contact",
            "--credential-file",
            "key.env",
            "--input",
            "email=ann@example.com",
        ],
    );
    assert!(ok, "{out}");
    assert!(out.contains("POST https://api.example.com/v1/contacts"), "{out}");
    assert!(
        out.contains("Bearer •••") && !out.contains("very_secret"),
        "{out}"
    );
    assert!(out.contains(r#"{"email":"ann@example.com"}"#), "{out}");

    // A recording that no longer holds says what differs.
    let file = d.join("apps/my_crm/app.yaml");
    let text = std::fs::read_to_string(&file).unwrap();
    std::fs::write(
        &file,
        text.replace("path: /contacts\n    inputs", "path: /people\n    inputs"),
    )
    .unwrap();
    let (ok, out) = app(d, &["test"]);
    assert!(!ok, "{out}");
    assert!(
        out.contains("sent.url: it sent https://api.example.com/v1/people"),
        "{out}"
    );

    // A mistake in the definition is found, with where it is.
    std::fs::write(&file, text.replace("items: value.data", "items: value.data +")).unwrap();
    let (ok, out) = app(d, &["check"]);
    assert!(!ok && out.contains("actions.list_contacts.items"), "{out}");
    std::fs::write(
        &file,
        text.replace("hosts: [api.example.com]", "hosts: [elsewhere.example]"),
    )
    .unwrap();
    let (ok, out) = app(d, &["check"]);
    assert!(!ok && out.contains("is not among its hosts"), "{out}");

    // The apps OpenDuct comes with are listed, and their recordings hold.
    let (ok, out) = app(d, &["list"]);
    assert!(
        // messaging: Slack's most used methods
        ok && out.contains("slack (Slack): 10 actions, 2 triggers, signs in as slack or slack_webhook"),
        "{out}"
    );
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let (ok, out) = app(&repo, &["test"]);
    assert!(ok && out.contains(" passed, 0 failed"), "{out}");
}
