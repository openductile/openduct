// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The messengers' triggers in `openduct start`: Discord's interactions take only deliveries signed with the
//! application's Ed25519 key - its PING answered with a PONG, a slash command run and answered at once as deferred, a
//! forged one refused - Microsoft Graph's check of a Teams workflow's address is answered back while the workflow is
//! not on yet, and a mailbox's new-email trigger is scheduled. Nothing here calls a real service: the deliveries are
//! made and signed here, as each vendor documents.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use studio::{call, http, owner, start, until};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string()
}

/// A key made for the test, as Discord's application has one.
fn key() -> Ed25519KeyPair {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap()
}

/// An interaction delivered to `hook`, signed as Discord signs one: the time and the body, with Ed25519.
fn interaction(studio: &str, hook: &str, key: &Ed25519KeyPair, body: &str) -> studio::Answer {
    let time = now();
    let signature = hex(key.sign(format!("{time}{body}").as_bytes()).as_ref());
    let signed = format!("x-signature-ed25519: {signature}");
    let timestamp = format!("x-signature-timestamp: {time}");
    http(
        studio,
        "POST",
        hook,
        &["content-type: application/json", &signed, &timestamp],
        Some(body),
    )
}

fn made(studio: &str, signed: &[String], definition: &Value, on: bool) -> (String, String) {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    if on {
        let turned = call(
            studio,
            signed,
            "POST",
            &format!("/api/workflows/{id}/active"),
            Some(r#"{"active": true}"#),
        );
        assert_eq!(turned.status, 200, "{}", turned.body);
    }
    let got = call(studio, signed, "GET", &format!("/api/workflows/{id}"), None);
    let hook = got.body["hook"].as_str().unwrap_or_default().to_owned();
    (id, hook)
}

fn credential(studio: &str, signed: &[String], body: Value) {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/credentials",
        Some(&body.to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
}

#[test]
fn discords_signed_interactions_are_answered_and_run_and_teams_check_is_answered_back() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let key = key();
    credential(
        &studio,
        &signed,
        json!({ "name": "shop-bot", "kind": "discord_bot", "fields": { "bot_token": "test-bot-token", "public_key": hex(key.public_key().as_ref()) } }),
    );
    let (id, hook) = made(
        &studio,
        &signed,
        &json!({
            "name": "Price command",
            "trigger": { "discord": { "when": "interaction", "credential": "shop-bot", "command": "price" } },
            "steps": [{ "name": "asked", "set": { "item": "value.option", "user": "value.user.username" } }],
        }),
        true,
    );
    assert!(hook.starts_with("/hooks/"), "{hook}");

    // Discord checks the address with a signed PING: a PONG, as JSON.
    let ping = interaction(
        &studio,
        &hook,
        &key,
        r#"{"id":"1","application_id":"10","type":1,"token":"t0","version":1}"#,
    );
    assert_eq!(
        (ping.status, &ping.body),
        (200, &json!({ "type": 1 })),
        "{}",
        ping.headers
    );

    // A slash command: answered at once that the bot is thinking, and run.
    let command = r#"{"id":"2","application_id":"10","type":2,"token":"t2","channel_id":"91","member":{"user":{"id":"92","username":"ann"}},"data":{"name":"price","options":[{"name":"item","type":3,"value":"tea"}]},"version":1}"#;
    let answered = interaction(&studio, &hook, &key, command);
    assert_eq!((answered.status, &answered.body), (200, &json!({ "type": 5 })));
    until(30, "the command's run", || {
        let runs = call(
            &studio,
            &signed,
            "GET",
            &format!("/api/workflows/{id}/runs"),
            None,
        );
        runs.body["runs"].as_array().is_some_and(|r| !r.is_empty())
    });
    let runs = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/runs"),
        None,
    );
    assert_eq!(
        runs.body["runs"][0]["steps"][0]["result"],
        json!({ "item": "tea", "user": "ann" })
    );

    // Signed with another key, or not at all: refused with 401, as Discord asks.
    let forged = interaction(&studio, &hook, &self::key(), command);
    assert_eq!(forged.status, 401, "{}", forged.body);
    let unsigned = http(
        &studio,
        "POST",
        &hook,
        &["content-type: application/json"],
        Some(command),
    );
    assert_eq!(unsigned.status, 401, "{}", unsigned.body);

    // Teams: Graph checks the address while the workflow is being turned on - answered back as plain text, though it
    // is not on yet; a notification is not taken until it is.
    credential(
        &studio,
        &signed,
        json!({ "name": "teams", "kind": "teams", "fields": { "access_token": "eyJ0" } }),
    );
    let (_, teams_hook) = made(
        &studio,
        &signed,
        &json!({
            "name": "Channel messages",
            "trigger": { "teams": { "when": "new_message", "credential": "teams", "team_id": "t1", "channel_id": "c1" } },
            "steps": [{ "name": "seen", "set": { "message": "value.message_id" } }],
        }),
        false,
    );
    let check = http(
        &studio,
        "POST",
        &format!("{teams_hook}?validationToken=12345"),
        &["content-type: text/plain; charset=utf-8"],
        Some(""),
    );
    assert_eq!(
        (check.status, &check.body),
        (200, &json!(12345)),
        "{}",
        check.headers
    );
    assert!(
        check.headers.to_ascii_lowercase().contains("text/plain"),
        "{}",
        check.headers
    );
    let markup = http(
        &studio,
        "POST",
        &format!("{teams_hook}?validationToken=%3Cscript%3E"),
        &["content-type: text/plain"],
        Some(""),
    );
    assert_eq!(markup.status, 400, "{}", markup.body);
    let notification = http(
        &studio,
        "POST",
        &teams_hook,
        &["content-type: application/json"],
        Some(
            r#"{"value":[{"subscriptionId":"s","changeType":"created","clientState":"guess","resource":"teams('t1')/channels('c1')/messages('1')"}]}"#,
        ),
    );
    assert_eq!(notification.status, 503, "{}", notification.body);
}

#[test]
fn a_mailboxs_new_email_trigger_is_scheduled() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    credential(
        &studio,
        &signed,
        json!({ "name": "shop-mail", "kind": "imap", "fields": { "host": "imap.example.com", "username": "ann@example.com", "password": "s3cret" } }),
    );
    let (id, _) = made(
        &studio,
        &signed,
        &json!({
            "name": "New mail",
            "trigger": { "mailbox": { "credential": "shop-mail", "from": "billing@example.com", "every": "2m" } },
            "steps": [{ "name": "seen", "set": { "subject": "value.subject" } }],
        }),
        true,
    );
    until(30, "the trigger to be listed", || {
        let listed = call(&studio, &signed, "GET", "/api/triggers", None);
        listed.body["triggers"].as_array().is_some_and(|t| {
            t.iter()
                .any(|t| t["workflow"] == id.as_str() && t["kind"] == "mailbox" && t["every_ms"] == 120_000)
        })
    });
    // A test run of it reaches no server and answers with a sample message.
    let tested = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/test"),
        Some("{}"),
    );
    assert_eq!(tested.status, 200, "{}", tested.body);
}
