// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! App triggers in `openduct start`: a workflow started by an app's own deliveries takes only those the app signed -
//! a challenge answered, a message run once, the same delivery again not run, a forged, unsigned or old one refused -
//! and a workflow started by an app's poll is scheduled; the app list says each app's triggers. (Slack's Events API
//! is used: it is set up in Slack's own pages, so nothing here calls a real service.)

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use serde_json::{Value, json};
use studio::{call, http, owner, start, until};

const SECRET: &str = "8f742231b10e8888abcd99yyyzzz85a5";

fn signature(time: &str, body: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET.as_bytes());
    let tag = ring::hmac::sign(&key, format!("v0:{time}:{body}").as_bytes());
    let hex: String = tag.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    format!("v0={hex}")
}

fn now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string()
}

/// A delivery to `hook`, signed (or not) as Slack signs one, at `time`.
fn deliver(studio: &str, hook: &str, body: &str, time: &str, signed: Option<&str>) -> studio::Answer {
    let time_header = format!("x-slack-request-timestamp: {time}");
    let signature_header = format!(
        "x-slack-signature: {}",
        signed.map_or_else(|| signature(time, body), str::to_owned)
    );
    let mut headers = vec!["content-type: application/json", time_header.as_str()];
    if signed != Some("") {
        headers.push(signature_header.as_str());
    }
    http(studio, "POST", hook, &headers, Some(body))
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

fn made(studio: &str, signed: &[String], definition: &Value) -> String {
    let made = call(
        studio,
        signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let id = made.body["id"].as_str().unwrap().to_owned();
    let on = call(
        studio,
        signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);
    id
}

#[test]
fn an_app_s_signed_deliveries_run_once_and_others_are_refused() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let cred = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&json!({ "name": "team", "kind": "slack", "fields": { "bot_token": "xoxb-1", "signing_secret": SECRET } }).to_string()),
    );
    assert_eq!(cred.status, 201, "{}", cred.body);
    let id = made(
        &studio,
        &signed,
        &json!({
            "name": "Slack messages",
            "trigger": { "slack": { "when": "message", "credential": "team" } },
            "steps": [{ "name": "said", "set": { "text": "value.text", "who": "value.user" } }],
        }),
    );
    let got = call(&studio, &signed, "GET", &format!("/api/workflows/{id}"), None);
    let hook = got.body["hook"]
        .as_str()
        .expect("an app's webhook has an address")
        .to_owned();

    // Slack checks the address with a signed challenge.
    let challenge = r#"{"type":"url_verification","challenge":"c4l1"}"#;
    let answered = deliver(&studio, &hook, challenge, &now(), None);
    assert_eq!(
        (answered.status, &answered.body),
        (200, &json!({ "challenge": "c4l1" }))
    );

    // A message runs once; the same delivery again is answered and not run.
    let message = r#"{"type":"event_callback","event_id":"Ev1","event":{"type":"message","user":"U1","text":"hello","ts":"1.2"}}"#;
    let first = deliver(&studio, &hook, message, &now(), None);
    assert_eq!(first.status, 202, "{}", first.body);
    until(30, "the message's run", || runs(&studio, &signed, &id).len() == 1);
    let again = deliver(&studio, &hook, message, &now(), None);
    assert_eq!((again.status, &again.body["repeated"]), (200, &json!(1)));

    // Forged, unsigned, signed long ago: refused, nothing run.
    let other = r#"{"type":"event_callback","event_id":"Ev2","event":{"type":"message","user":"U2","text":"let me in","ts":"1.3"}}"#;
    let forged = deliver(&studio, &hook, other, &now(), Some("v0=00ff"));
    assert_eq!(forged.status, 401, "{}", forged.body);
    let unsigned = deliver(&studio, &hook, other, &now(), Some(""));
    assert_eq!(unsigned.status, 401, "{}", unsigned.body);
    let old = (now().parse::<u64>().unwrap() - 600).to_string();
    let late = deliver(&studio, &hook, other, &old, None);
    assert_eq!(late.status, 401, "{}", late.body);
    assert!(late.body.to_string().contains("too long ago"), "{}", late.body);

    std::thread::sleep(std::time::Duration::from_secs(1));
    let all = runs(&studio, &signed, &id);
    assert_eq!(all.len(), 1, "{all:?}");
    assert_eq!(
        all[0]["steps"][0]["result"],
        json!({ "text": "hello", "who": "U1" })
    );
    assert_eq!(all[0]["trigger"]["app"], "slack");
    assert_eq!(all[0]["trigger"]["id"], "Ev1");
    assert!(
        !all[0].to_string().contains("x-slack-signature"),
        "the signature is not kept with the run"
    );

    // A workflow turned off takes no deliveries.
    let off = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": false}"#),
    );
    assert_eq!(off.status, 200, "{}", off.body);
    until(10, "the workflow to be off", || {
        deliver(&studio, &hook, message, &now(), None).status == 503
    });
}

#[test]
fn a_poll_is_scheduled_and_the_app_list_says_each_app_s_triggers() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let apps = call(&studio, &signed, "GET", "/api/apps", None);
    let gmail = apps.body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "gmail")
        .unwrap()
        .clone();
    assert_eq!(gmail["triggers"][0]["name"], "new_email");
    assert_eq!(gmail["triggers"][0]["kind"], "poll");
    let stripe = apps.body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == "stripe")
        .unwrap()
        .clone();
    assert_eq!(
        (
            &stripe["triggers"][0]["kind"],
            &stripe["triggers"][0]["registers"]
        ),
        (&json!("webhook"), &json!(true))
    );

    // A setting the trigger does not have is refused when it is saved, with where it is.
    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": { "name": "Bot", "trigger": { "telegram": { "when": "new_message", "credential": "bot", "every": "5s" } } } }).to_string()),
    );
    assert_eq!(wrong.status, 422, "{}", wrong.body);
    assert!(
        wrong.body.to_string().contains("telegram.every"),
        "{}",
        wrong.body
    );

    let id = made(
        &studio,
        &signed,
        &json!({ "name": "Bot", "trigger": { "telegram": { "when": "new_message", "credential": "bot", "every": "2m" } } }),
    );
    until(30, "the poll's trigger to be kept", || {
        call(&studio, &signed, "GET", "/api/triggers", None).body["triggers"]
            .as_array()
            .is_some_and(|t| {
                t.iter()
                    .any(|t| t["workflow"] == id.as_str() && t["kind"] == "app" && t["every_ms"] == 120_000)
            })
    });
    let listed = call(&studio, &signed, "GET", "/api/workflows", None);
    let bot = listed.body["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["id"] == id.as_str())
        .unwrap()
        .clone();
    assert_eq!(bot["trigger"], "app");
}
