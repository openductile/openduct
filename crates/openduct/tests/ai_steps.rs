// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The AI steps in `openduct start`, through the API the studio's page calls: a mistake in one refused when the
//! workflow is checked, a test run that asks no model, and a preview against a stand-in model on this machine - the
//! category it chose, that category's steps, the fields pulled out - with the credential's key in no answer.

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

use serde_json::{Value, json};
use studio::{call, owner, start_with, stop};

/// A stand-in model on this machine, in the chat completions API's shape: it answers by what it is asked (the
/// system message), and keeps each request's head and body.
fn stand_in_model() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut got = Vec::new();
            let mut buf = vec![0u8; 65536];
            let (head, body) = loop {
                let n = stream.read(&mut buf).unwrap_or(0);
                if n == 0 {
                    break (String::new(), String::new());
                }
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).into_owned();
                if let Some((head, rest)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if rest.len() >= length {
                        break (head.to_owned(), rest.to_owned());
                    }
                }
            };
            kept.lock().unwrap().push(format!("{head}\r\n\r\n{body}"));
            let asked: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
            let system = asked["messages"][0]["content"].as_str().unwrap_or_default();
            // Asked again (more than the system message and the text): the extract's right answer; first, the
            // amount as text, which its schema refuses.
            let again = asked["messages"].as_array().map_or(0, Vec::len) > 2;
            let text = if system.starts_with("Sort the text") {
                r#"{"category": "Refund"}"#
            } else if again {
                r#"{"order_id": "A-1042", "amount": 49.9, "reason": "charged twice"}"#
            } else {
                r#"{"order_id": "A-1042", "amount": "49.90"}"#
            };
            let answer = json!({
                "model": "stand-in-1",
                "choices": [{ "message": { "content": text }, "finish_reason": "stop" }],
                "usage": { "prompt_tokens": 20, "completion_tokens": 5 },
            })
            .to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                answer.len()
            );
            let _ = stream.write_all(reply.as_bytes());
        }
    });
    (addr, seen)
}

#[test]
fn ai_steps_are_checked_tested_and_previewed_through_the_studio_s_api() {
    let data = tempfile::tempdir().unwrap();
    // The stand-in model is on this machine: workflows may reach it only when OpenDuct is told they may.
    let running = start_with(
        data.path(),
        &[
            "--listen",
            "127.0.0.1:0",
            "--studio-listen",
            "127.0.0.1:0",
            "--allow-private-network",
        ],
        &[],
    );
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let (model, seen) = stand_in_model();
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(
            &json!({ "name": "model", "kind": "ai", "fields": {
                "base_url": format!("http://{model}/v1"), "api_key": "key-SECRET-7", "model": "stand-in-1",
            } })
            .to_string(),
        ),
    );
    assert_eq!(made.status, 201, "{}", made.body);

    let wrong = call(
        &studio,
        &signed,
        "POST",
        "/api/check",
        Some(
            r#"{"definition": {"name": "x", "steps": [{"classify": {"credential": "model", "text": "hi", "categories": ["a", "A"]}}]}}"#,
        ),
    );
    assert_eq!(wrong.status, 422);
    assert!(
        wrong.body["error"]
            .as_str()
            .unwrap()
            .contains("\"A\" is there twice"),
        "{}",
        wrong.body
    );

    let definition = json!({ "name": "Refunds", "trigger": { "webhook": {} }, "steps": [
        { "name": "sort", "classify": {
            "credential": "model", "text": "{{ value.text }}",
            "categories": [
                { "name": "refund", "description": "Asks for money back", "steps": [
                    { "name": "order", "extract": { "credential": "model", "text": "{{ value.text }}", "fields": [
                        { "name": "order_id", "required": true },
                        { "name": "amount", "type": "number" },
                        { "name": "reason" },
                    ] } },
                ] },
                { "name": "bug", "steps": [{ "name": "to_bugs", "set": { "bug": "true" } }] },
            ],
        } },
        { "name": "done", "set": { "category": "steps.sort.category" } },
    ] });
    let saved = call(
        &studio,
        &signed,
        "POST",
        "/api/workflows",
        Some(&json!({ "definition": definition }).to_string()),
    );
    assert_eq!(saved.status, 201, "{}", saved.body);
    let id = saved.body["id"].as_str().unwrap().to_owned();
    let input = json!({ "text": "Order A-1042 was charged twice: refund one, please." });

    // A test run asks no model: it shows what would be sent, and takes no category's steps.
    let tried = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/test"),
        Some(&json!({ "input": input }).to_string()),
    );
    assert_eq!(tried.status, 200, "{}", tried.body);
    let steps = tried.body["run"]["steps"].as_array().unwrap().clone();
    assert_eq!(steps[0]["result"]["test"], "not sent");
    assert_eq!(
        steps[0]["result"]["request"]["headers"]["authorization"],
        "Bearer •••"
    );
    assert_eq!(steps.len(), 2, "sort, then done: {steps:#?}");
    assert!(seen.lock().unwrap().is_empty(), "a test run asks no model");

    // A preview asks the model (a read): the category it chose, its steps, the fields - asked again for a number.
    let previewed = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/preview"),
        Some(&json!({ "input": input }).to_string()),
    );
    assert_eq!(previewed.status, 200, "{}", previewed.body);
    let run = previewed.body["run"].clone();
    assert_eq!(run["status"], "done", "{run:#}");
    let names: Vec<&str> = run["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["sort", "order", "done"]);
    assert_eq!(run["steps"][0]["kind"], "classify");
    assert_eq!(run["steps"][0]["result"]["category"], "refund");
    assert_eq!(run["steps"][1]["kind"], "extract");
    assert_eq!(
        run["steps"][1]["result"]["json"],
        json!({ "order_id": "A-1042", "amount": 49.9, "reason": "charged twice" })
    );
    assert_eq!(run["steps"][1]["result"]["metrics"]["repairs"], 1);
    assert_eq!(run["steps"][1]["result"]["usage"]["total_tokens"], 50);
    assert_eq!(run["steps"][2]["result"]["category"], "refund");
    let asked = seen.lock().unwrap().clone();
    assert_eq!(asked.len(), 3, "classify once, extract twice");
    assert!(asked[0].starts_with("POST /v1/chat/completions"), "{}", asked[0]);
    assert!(
        asked[0]
            .to_ascii_lowercase()
            .contains("authorization: bearer key-secret-7")
    );
    assert!(
        !previewed.body.to_string().contains("key-SECRET-7")
            && !tried.body.to_string().contains("key-SECRET-7"),
        "the key is in no answer"
    );
    stop(running);
}
