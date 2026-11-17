// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app a person signs in to at the vendor (OAuth 2.0), end to end against a stand-in vendor on this machine: the
//! admin gives the install's own registration; Connect sends the person to the vendor with PKCE and a sealed state;
//! the callback trades the code for tokens and keeps them as the workspace's credential; a workflow calls the vendor
//! with them; a token the vendor no longer takes is refreshed and kept.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use serde_json::{Value, json};
use studio::{call, owner, start_with, stop, until};

#[derive(Default)]
struct Vendor {
    /// The PKCE challenge the sign-in was started with.
    challenge: String,
    /// Tokens it takes now.
    valid: Vec<String>,
    refreshed: u32,
}

fn form(body: &str) -> std::collections::BTreeMap<String, String> {
    body.split('&')
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.replace("%3A", ":").replace("%2F", "/")))
        .collect()
}

/// A stand-in vendor: `/token` trades a code (checking the PKCE verifier and the client) or a refresh token, `/v1/me`
/// answers only to a token it takes.
fn vendor(state: Arc<Mutex<Vendor>>) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut seen = Vec::new();
            let mut buf = [0u8; 8192];
            let (head, body) = loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break (String::new(), String::new()),
                    Ok(n) => seen.extend_from_slice(&buf[..n]),
                }
                let text = String::from_utf8_lossy(&seen).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break (head.to_owned(), body.to_owned());
                    }
                }
            };
            let mut v = state.lock().unwrap();
            let (status, answer) = if head.starts_with("POST /token") {
                let f = form(&body);
                let client = f.get("client_id").map(String::as_str) == Some("cid")
                    && f.get("client_secret").map(String::as_str) == Some("csecret");
                match f.get("grant_type").map(String::as_str) {
                    Some("authorization_code")
                        if client && f.get("code").map(String::as_str) == Some("code-1") =>
                    {
                        let verifier = f.get("code_verifier").cloned().unwrap_or_default();
                        let digest = ring::digest::digest(&ring::digest::SHA256, verifier.as_bytes());
                        let challenge =
                            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.as_ref());
                        if challenge == v.challenge {
                            v.valid.push("at-1".to_owned());
                            (
                                200,
                                json!({ "access_token": "at-1", "refresh_token": "rt-1", "expires_in": 3600 }),
                            )
                        } else {
                            (
                                400,
                                json!({ "error": "invalid_grant", "error_description": "PKCE verifier does not match" }),
                            )
                        }
                    }
                    Some("refresh_token")
                        if client && f.get("refresh_token").map(String::as_str) == Some("rt-1") =>
                    {
                        v.refreshed += 1;
                        let token = format!("at-r{}", v.refreshed);
                        v.valid.push(token.clone());
                        (200, json!({ "access_token": token, "expires_in": 3600 }))
                    }
                    _ => (400, json!({ "error": "invalid_grant" })),
                }
            } else if head.starts_with("GET /v1/me") {
                let token = head
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("authorization: bearer ")
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                if v.valid.contains(&token) {
                    (200, json!({ "name": "Ann", "token": token }))
                } else {
                    (401, json!({ "message": "token not valid" }))
                }
            } else {
                (404, json!({}))
            };
            drop(v);
            let text = answer.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    addr
}

fn query_of(url: &str) -> std::collections::BTreeMap<String, String> {
    let q = url.split_once('?').map_or("", |(_, q)| q);
    q.split('&')
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| {
            let mut out = Vec::new();
            let b = v.as_bytes();
            let mut i = 0;
            while i < b.len() {
                if b[i] == b'%' && i + 2 < b.len() + 1 {
                    out.push(u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap(), 16).unwrap());
                    i += 3;
                } else {
                    out.push(if b[i] == b'+' { b' ' } else { b[i] });
                    i += 1;
                }
            }
            (k.to_owned(), String::from_utf8(out).unwrap())
        })
        .collect()
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    got.body["runs"].as_array().cloned().unwrap_or_default()
}

#[test]
fn an_app_signed_in_at_its_vendor_is_connected_used_and_refreshed() {
    let shared = Arc::new(Mutex::new(Vendor::default()));
    let at = vendor(Arc::clone(&shared));
    let apps = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(apps.path().join("fakevendor")).unwrap();
    std::fs::write(
        apps.path().join("fakevendor/app.yaml"),
        format!(
            "app: fakevendor\nname: Fake Vendor\nhosts: [127.0.0.1]\nbase_url: http://{at}/v1\nsign_in:\n  - id: fakevendor\n    label: Sign in at Fake Vendor\n    oauth2:\n      authorize: http://{at}/authorize\n      token: http://{at}/token\n      scopes: [profile, orders]\n    headers: {{ Authorization: \"Bearer {{{{ credential.access_token }}}}\" }}\nactions:\n  me:\n    label: Who I am\n    method: GET\n    path: /me\n    result: value\n"
        ),
    )
    .unwrap();
    let data = tempfile::tempdir().unwrap();
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--allow-private-network",
            "--apps-dir",
            apps.path().to_str().unwrap(),
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);

    // Before the install is registered, Connect says what the admin must do.
    let early = call(
        &studio,
        &signed,
        "POST",
        "/api/oauth/start",
        Some(r#"{"sign_in": "fakevendor", "name": "vendor"}"#),
    );
    assert_eq!(early.status, 409, "{}", early.body);
    assert!(
        early.body["error"].as_str().unwrap().contains("under Settings"),
        "{}",
        early.body
    );

    // The admin registers the install with the vendor; the address to give the vendor is shown.
    let listed = call(&studio, &signed, "GET", "/api/oauth/apps", None);
    assert_eq!(listed.status, 200, "{}", listed.body);
    let callback = format!("http://{studio}/oauth/callback");
    assert_eq!(listed.body["callback"], callback.as_str());
    let reg = call(
        &studio,
        &signed,
        "PUT",
        "/api/oauth/apps/fakevendor",
        Some(r#"{"client_id": "cid", "client_secret": "csecret"}"#),
    );
    assert_eq!(reg.status, 200, "{}", reg.body);
    let listed = call(&studio, &signed, "GET", "/api/oauth/apps", None);
    // One row an app: QuickBooks' live and sandbox sign-ins share its registration.
    let rows = listed.body["apps"].as_array().unwrap();
    let quickbooks: Vec<&serde_json::Value> = rows.iter().filter(|a| a["app"] == "quickbooks").collect();
    assert_eq!(quickbooks.len(), 1, "{rows:?}");
    assert_eq!(
        quickbooks[0]["sign_ins"],
        json!(["quickbooks", "quickbooks_sandbox"])
    );
    // Beside the apps OpenDuct comes with that sign in at their vendors: this one, now registered.
    let shown = listed.body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["app"] == "fakevendor")
        .cloned()
        .unwrap();
    assert_eq!(
        (shown["registered"].clone(), shown["client_id"].clone()),
        (json!(true), json!("cid"))
    );
    assert!(
        !listed.body.to_string().contains("csecret"),
        "the secret was shown: {}",
        listed.body
    );
    let kinds = call(&studio, &signed, "GET", "/api/credential-kinds", None);
    let kind = kinds.body["kinds"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["kind"] == "fakevendor")
        .cloned()
        .unwrap();
    assert_eq!(
        (
            kind["connect"].clone(),
            kind["registered"].clone(),
            kind["fields"].clone()
        ),
        (json!(true), json!(true), json!([]))
    );

    // Connect: to the vendor, with the install's client id, PKCE, and a state only this studio reads.
    let started = call(
        &studio,
        &signed,
        "POST",
        "/api/oauth/start",
        Some(r#"{"sign_in": "fakevendor", "name": "vendor"}"#),
    );
    assert_eq!(started.status, 200, "{}", started.body);
    let url = started.body["url"].as_str().unwrap().to_owned();
    assert!(url.starts_with(&format!("http://{at}/authorize?")), "{url}");
    let q = query_of(&url);
    assert_eq!(q["client_id"], "cid");
    assert_eq!(q["redirect_uri"], callback);
    assert_eq!(q["code_challenge_method"], "S256");
    assert_eq!(q["scope"], "profile orders");
    shared.lock().unwrap().challenge = q["code_challenge"].clone();

    // A state this studio did not seal is refused.
    let forged = call(
        &studio,
        &signed,
        "GET",
        "/oauth/callback?code=code-1&state=v1%3Anot-ours",
        None,
    );
    assert!(
        forged.body["location"].as_str().unwrap().contains("oauth_error"),
        "{}",
        forged.body
    );

    // Back from the vendor: the code traded, the tokens kept as the workspace's credential.
    let state = q["state"]
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
        .replace(':', "%3A");
    let back = call(
        &studio,
        &signed,
        "GET",
        &format!("/oauth/callback?code=code-1&state={state}"),
        None,
    );
    assert_eq!(back.status, 302, "{}", back.body);
    assert_eq!(back.body["location"], "/credentials?connected=vendor");
    let creds = call(&studio, &signed, "GET", "/api/credentials", None);
    let kept = creds.body["credentials"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "vendor")
        .cloned()
        .unwrap();
    assert_eq!(kept["kind"], "fakevendor");
    assert!(
        !creds.body.to_string().contains("at-1") && !creds.body.to_string().contains("rt-1"),
        "{}",
        creds.body
    );

    // A workflow calls the vendor with it.
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": { "name": "Who", "steps": [{ "name": "me", "fakevendor": { "credential": "vendor", "action": "me" } }] } }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#)
        )
        .status,
        200
    );
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/run"),
            Some("{}")
        )
        .status,
        202
    );
    until(30, "the first run", || runs(&studio, &signed, &id).len() == 1);
    let first = &runs(&studio, &signed, &id)[0];
    assert_eq!(first["status"], "done", "{first}");
    assert_eq!(first["steps"][0]["result"]["token"], "at-1");

    // The vendor stops taking the token: it is refreshed, kept, and the call made again.
    shared.lock().unwrap().valid.clear();
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/run"),
            Some("{}")
        )
        .status,
        202
    );
    until(30, "the second run", || runs(&studio, &signed, &id).len() == 2);
    let second = runs(&studio, &signed, &id)
        .into_iter()
        .find(|r| r["steps"][0]["result"]["token"] != "at-1")
        .unwrap();
    assert_eq!(second["status"], "done", "{second}");
    assert_eq!(second["steps"][0]["result"]["token"], "at-r1");
    // The refreshed token was kept: the next run uses it, with no refresh of its own.
    assert_eq!(
        call(
            &studio,
            &signed,
            "POST",
            &format!("/api/workflows/{id}/run"),
            Some("{}")
        )
        .status,
        202
    );
    until(30, "the third run", || runs(&studio, &signed, &id).len() == 3);
    assert_eq!(shared.lock().unwrap().refreshed, 1);
    stop(running);
}
