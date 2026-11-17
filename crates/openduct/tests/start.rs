// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct start`: one command starts the broker and the studio, the first visit makes the owner's account, it is
//! kept across a restart, and the studio refuses what it must.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "studio")]

mod studio;

use std::process::Command;

use studio::{JSON, OPENDUCT, OWNER, http, start, stop};

#[cfg(unix)]
#[test]
fn one_command_starts_it_all_and_the_owner_is_kept_across_a_restart() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    // The broker answers Kafka clients on its address.
    let topics = Command::new(OPENDUCT)
        .args(["topic", "list", "--bootstrap", &running.broker])
        .output()
        .unwrap();
    assert!(
        topics.status.success(),
        "{}",
        String::from_utf8_lossy(&topics.stderr)
    );

    assert_eq!(http(&studio, "GET", "/api/status", &[], None).body["setup"], true);
    let made = http(&studio, "POST", "/api/setup", &[JSON], Some(OWNER));
    assert_eq!(made.status, 201, "{}", made.body);
    assert_eq!(made.body["account"]["email"], "ada@example.com");
    let headers = made.headers.to_ascii_lowercase();
    assert!(headers.contains("httponly") && headers.contains("samesite=strict"));
    let cookie = format!("cookie: {}", made.cookie());
    let session = http(&studio, "GET", "/api/session", &[&cookie], None);
    assert_eq!(
        session.body["account"]["admin"], true,
        "the first account is the installation's admin"
    );
    assert_eq!(session.body["workspaces"][0]["name"], "My workspace");
    assert_eq!(session.body["workspaces"][0]["role"], "admin");
    let again = http(&studio, "POST", "/api/setup", &[JSON], Some(OWNER));
    assert_eq!(again.status, 409, "only the first visit makes an owner");
    assert_eq!(http(&studio, "GET", "/", &[], None).status, 200, "the page");

    stop(running);
    let running = start(data.path());
    let studio = running.studio.clone();
    assert_eq!(
        http(&studio, "GET", "/api/status", &[], None).body["setup"],
        false,
        "kept"
    );
    let wrong = http(
        &studio,
        "POST",
        "/api/sign-in",
        &[JSON],
        Some(r#"{"email": "ada@example.com", "password": "not it at all"}"#),
    );
    assert_eq!(wrong.status, 401);
    let signed = http(
        &studio,
        "POST",
        "/api/sign-in",
        &[JSON],
        Some(r#"{"email": "ada@example.com", "password": "correct horse battery"}"#),
    );
    assert_eq!(signed.status, 200, "{}", signed.body);
    let old = http(&studio, "GET", "/api/session", &[&cookie], None);
    assert_eq!(
        old.status, 200,
        "a session lasts across a restart: its key is kept with the data"
    );
    stop(running);
}

#[cfg(unix)]
#[test]
fn the_studio_refuses_other_names_other_sites_changes_without_its_token_and_guessing() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let made = http(&studio, "POST", "/api/setup", &[JSON], Some(OWNER));
    let cookie = format!("cookie: {}", made.cookie());
    let csrf = format!("x-openduct-csrf: {}", made.body["csrf"].as_str().unwrap());

    let renamed = http(
        &studio,
        "GET",
        "/api/status",
        &["host: openduct.attacker.example"],
        None,
    );
    assert_eq!(
        renamed.status, 421,
        "a site pointing its own name at this machine reaches nothing"
    );
    let other_site = http(
        &studio,
        "POST",
        "/api/sign-out",
        &[JSON, &cookie, &csrf, "origin: https://attacker.example"],
        Some("{}"),
    );
    assert_eq!(other_site.status, 403);
    let a_form = http(
        &studio,
        "POST",
        "/api/sign-in",
        &["content-type: application/x-www-form-urlencoded"],
        Some("email=a"),
    );
    assert_eq!(
        a_form.status, 415,
        "a change is JSON, which another site cannot send unasked"
    );
    let no_token = http(&studio, "POST", "/api/sign-out", &[JSON, &cookie], Some("{}"));
    assert_eq!(no_token.status, 403);
    let out = http(
        &studio,
        "POST",
        "/api/sign-out",
        &[JSON, &cookie, &csrf],
        Some("{}"),
    );
    assert_eq!(out.status, 200);
    assert_eq!(
        http(&studio, "GET", "/api/session", &[&cookie], None).status,
        401,
        "signed out everywhere"
    );

    for _ in 0..10 {
        let wrong = http(
            &studio,
            "POST",
            "/api/sign-in",
            &[JSON],
            Some(r#"{"email": "ada@example.com", "password": "guess guess guess"}"#),
        );
        assert_eq!(wrong.status, 401);
    }
    let waits = http(
        &studio,
        "POST",
        "/api/sign-in",
        &[JSON],
        Some(r#"{"email": "ada@example.com", "password": "correct horse battery"}"#),
    );
    assert_eq!(
        waits.status, 429,
        "an address that keeps guessing waits, even with the right password"
    );
    stop(running);
}
