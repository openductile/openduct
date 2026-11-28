// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow started by a chat, in `openduct start`, against a stand-in model on this machine: its page and its
//! script for sites, its password and its sites, a message answered whole and one answered as it is made - the
//! stand-in holding back the rest of its answer until the first piece has reached the chat - the conversation's
//! earlier turn given to the next run, its limits, and the studio's "try the chat".

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use studio::{JSON, call, http, owner, start_with, until};

/// A model's API on this machine: what it was asked, and whether a streamed answer's first piece reached the chat
/// before the rest of it was sent.
struct Model {
    addr: String,
    asked: Arc<Mutex<Vec<Value>>>,
    first_piece_seen: Arc<AtomicBool>,
    held_back_until_seen: Arc<AtomicBool>,
}

fn model() -> Model {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicBool::new(false));
    let (a, s, h) = (Arc::clone(&asked), Arc::clone(&seen), Arc::clone(&held));
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (a, s, h) = (Arc::clone(&a), Arc::clone(&s), Arc::clone(&h));
            std::thread::spawn(move || answer(stream, &a, &s, &h));
        }
    });
    Model {
        addr,
        asked,
        first_piece_seen: seen,
        held_back_until_seen: held,
    }
}

/// One request to the stand-in: a whole answer, or events when streaming is asked for.
fn answer(stream: TcpStream, asked: &Mutex<Vec<Value>>, seen: &AtomicBool, held: &AtomicBool) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut length = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            length = v.trim().parse().unwrap();
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    let request: Value = serde_json::from_slice(&body).unwrap();
    asked.lock().unwrap().push(request.clone());
    let last = request["messages"]
        .as_array()
        .and_then(|m| m.last())
        .and_then(|m| m["content"].as_str())
        .unwrap_or_default()
        .to_owned();
    let mut out = stream;
    if request["stream"] == true {
        out.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n")
            .unwrap();
        out.write_all(
            b"data: {\"model\":\"m-1\",\"choices\":[{\"delta\":{\"content\":\"You asked: \"}}]}\n\n",
        )
        .unwrap();
        out.flush().unwrap();
        // The rest waits until the chat has shown the first piece: proof that it came as it was made.
        let until = Instant::now() + Duration::from_secs(20);
        while !seen.load(Ordering::SeqCst) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        held.store(seen.load(Ordering::SeqCst), Ordering::SeqCst);
        let rest = json!({ "choices": [{ "delta": { "content": last }, "finish_reason": "stop" }] });
        let usage = json!({ "choices": [], "usage": { "prompt_tokens": 9, "completion_tokens": 4 } });
        let _ = out.write_all(format!("data: {rest}\n\ndata: {usage}\n\ndata: [DONE]\n\n").as_bytes());
        return;
    }
    let whole = json!({
        "model": "m-1",
        "choices": [{ "message": { "role": "assistant", "content": format!("You asked: {last}") }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 9, "completion_tokens": 4 },
    })
    .to_string();
    let _ = out.write_all(
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{whole}",
            whole.len()
        )
        .as_bytes(),
    );
}

/// A request whose answer comes as events: each event (name, data) as it arrives, `on` told of each.
fn events(
    addr: &str,
    path: &str,
    headers: &[&str],
    body: &str,
    mut on: impl FnMut(&str, &Value),
) -> (u16, Vec<(String, Value)>) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut request = format!("POST {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n");
    for h in headers {
        request.push_str(&format!("{h}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(request.as_bytes()).unwrap();
    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).unwrap();
    let status: u16 = status_line.split(' ').nth(1).unwrap().parse().unwrap();
    let mut chunked = false;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if line
            .to_ascii_lowercase()
            .starts_with("transfer-encoding: chunked")
        {
            chunked = true;
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut text = String::new();
    let mut out = Vec::new();
    let mut take = |text: &mut String, out: &mut Vec<(String, Value)>| {
        while let Some(end) = text.find("\n\n") {
            let block: String = text.drain(..end + 2).collect();
            let mut name = "message".to_owned();
            let mut data = String::new();
            for line in block.lines() {
                if let Some(n) = line.strip_prefix("event: ") {
                    name = n.to_owned();
                } else if let Some(d) = line.strip_prefix("data: ") {
                    data.push_str(d);
                }
            }
            if let Ok(value) = serde_json::from_str::<Value>(&data) {
                on(&name, &value);
                out.push((name, value));
            }
        }
    };
    if !chunked {
        reader.read_to_string(&mut text).unwrap();
        take(&mut text, &mut out);
        return (status, out);
    }
    loop {
        let mut size = String::new();
        if reader.read_line(&mut size).unwrap() == 0 {
            break;
        }
        let size = usize::from_str_radix(size.trim(), 16).unwrap();
        if size == 0 {
            break;
        }
        let mut chunk = vec![0; size + 2];
        reader.read_exact(&mut chunk).unwrap();
        text.push_str(&String::from_utf8_lossy(&chunk[..size]));
        take(&mut text, &mut out);
    }
    (status, out)
}

#[test]
fn a_chat_answers_on_its_page_and_by_json_streams_as_the_answer_is_made_and_keeps_its_conversation() {
    let model = model();
    let data = tempfile::tempdir().unwrap();
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
    let credential = json!({
        "name": "model", "kind": "ai",
        "fields": { "base_url": format!("http://{}/v1", model.addr), "api_key": "k-secret", "model": "m-1" },
    });
    let made = call(
        &studio,
        &signed,
        "POST",
        "/api/credentials",
        Some(&credential.to_string()),
    );
    assert_eq!(made.status, 201, "{}", made.body);
    let definition = json!({
        "name": "Help desk",
        "trigger": { "chat": {
            "texts": { "title": "Help <b>desk</b>", "welcome": "Ask me." },
            "theme": { "accent": "#0f766e" },
            "origins": ["https://shop.example.com"],
            "password": "open sesame",
            "limits": { "per_session": 3 },
            "history": 5,
        } },
        "steps": [{ "name": "answer", "ai": {
            "credential": "model",
            "system": "You help {{ has(value.person.name) ? value.person.name : 'a customer' }}.",
            "messages": "{{ value.history }}",
            "prompt": "{{ value.message }}",
        } }],
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
    let chat = made.body["chat"].as_str().unwrap().to_owned();
    assert!(
        chat.starts_with("/chat/") && made.body["hook"].is_null(),
        "{}",
        made.body
    );

    // Off: the page says it is closed, and no message is taken.
    assert_eq!(http(&studio, "GET", &chat, &[], None).status, 503);
    let on = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/active"),
        Some(r#"{"active": true}"#),
    );
    assert_eq!(on.status, 200, "{}", on.body);

    // The page: made here, its words escaped, framed only by the studio and the shop, its script from here.
    let page = raw(&studio, "GET", &chat, &[]);
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.contains("Help &lt;b&gt;desk&lt;/b&gt;") && !page.contains("<b>desk"));
    assert!(page.contains("frame-ancestors 'self' https://shop.example.com"));
    assert!(!page.to_ascii_lowercase().contains("x-frame-options"));
    assert!(page.contains(&format!("src=\"{chat}/chat.js\"")) && !page.contains("https://cdn"));
    let embed = raw(&studio, "GET", &format!("{chat}/embed.js"), &[]);
    assert!(embed.contains("\"accent\":\"#0f766e\"") && embed.contains("text/javascript"));
    assert!(raw(&studio, "GET", &format!("{chat}/chat.css"), &[]).contains("--accent: #0f766e"));
    assert_eq!(
        http(
            &studio,
            "GET",
            "/chat/0123456789abcdef0123456789abcdef",
            &[],
            None
        )
        .status,
        404
    );

    let messages = format!("{chat}/messages");
    let send = |body: Value, extra: &[&str]| {
        let mut headers = vec![JSON];
        headers.extend_from_slice(extra);
        http(&studio, "POST", &messages, &headers, Some(&body.to_string()))
    };
    let session = "conversation-0001-abcdef";
    // Without the password, from another site, or too long: refused, in plain words.
    let none = send(json!({ "message": "Hi", "session": session }), &[]);
    assert_eq!(none.status, 401, "{}", none.body);
    let elsewhere = send(
        json!({ "message": "Hi", "session": session, "password": "open sesame" }),
        &["origin: https://evil.example"],
    );
    assert_eq!(elsewhere.status, 403, "{}", elsewhere.body);
    let long = send(
        json!({ "message": "x".repeat(5_000), "session": session, "password": "open sesame" }),
        &[],
    );
    assert_eq!(long.status, 422, "{}", long.body);

    // A message from the shop's own page, as JSON: the answer whole, readable by the shop.
    let first = send(
        json!({ "message": "Do you ship to Canada?", "session": session, "person": { "name": "Ada" } }),
        &[
            "origin: https://shop.example.com",
            "authorization: Bearer open sesame",
        ],
    );
    assert_eq!(first.status, 200, "{}", first.body);
    assert_eq!(first.body["answer"], "You asked: Do you ship to Canada?");
    assert_eq!(first.body["session"], session);
    assert!(
        first
            .headers
            .contains("access-control-allow-origin: https://shop.example.com")
    );

    // The next one, as events: the first piece shown before the rest was made, then the whole answer.
    let seen = Arc::clone(&model.first_piece_seen);
    let body =
        json!({ "message": "And how long does it take?", "session": session, "password": "open sesame" });
    let (status, got) = events(
        &studio,
        &messages,
        &[JSON, "accept: text/event-stream"],
        &body.to_string(),
        |name, _| {
            if name == "text" {
                seen.store(true, Ordering::SeqCst);
            }
        },
    );
    assert_eq!(status, 200, "{got:?}");
    let names: Vec<&str> = got.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names.first(), Some(&"start"), "{names:?}");
    assert_eq!(names.last(), Some(&"answer"), "{names:?}");
    assert!(names.iter().filter(|n| **n == "text").count() >= 2, "{names:?}");
    let shown: String = got
        .iter()
        .filter(|(n, _)| n == "text")
        .map(|(_, d)| d["text"].as_str().unwrap())
        .collect();
    assert_eq!(shown, "You asked: And how long does it take?");
    assert_eq!(got.last().unwrap().1["answer"], shown);
    assert!(
        model.held_back_until_seen.load(Ordering::SeqCst),
        "the first piece reached the chat while the rest was still to come"
    );

    // The second run was given the first turn, as an AI step's messages; the person's name reached the first.
    let asked = model.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 2);
    assert_eq!(asked[0]["messages"][0]["content"], "You help Ada.");
    let second: Vec<(&str, &str)> = asked[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| (m["role"].as_str().unwrap(), m["content"].as_str().unwrap()))
        .collect();
    assert_eq!(
        second[1..],
        [
            ("user", "Do you ship to Canada?"),
            ("assistant", "You asked: Do you ship to Canada?"),
            ("user", "And how long does it take?"),
        ]
    );
    assert_eq!(asked[1]["stream"], true);

    // Three messages a minute in a conversation: the fourth waits.
    let third = send(
        json!({ "message": "Thanks", "session": session, "password": "open sesame" }),
        &[],
    );
    assert_eq!(third.status, 200, "{}", third.body);
    let fourth = send(
        json!({ "message": "One more", "session": session, "password": "open sesame" }),
        &[],
    );
    assert_eq!(fourth.status, 429, "{}", fourth.body);

    // A browser asking first, from the shop and from elsewhere.
    let ask = raw(
        &studio,
        "OPTIONS",
        &messages,
        &["origin: https://shop.example.com"],
    );
    assert!(
        ask.starts_with("HTTP/1.1 204")
            && ask.contains("access-control-allow-origin: https://shop.example.com")
    );
    assert!(
        raw(&studio, "OPTIONS", &messages, &["origin: https://evil.example"]).starts_with("HTTP/1.1 403")
    );

    // Each message is a run in the workflow's history, started by the chat, the session its key.
    until(20, "three chat runs in the history", || {
        let runs = call(
            &studio,
            &signed,
            "GET",
            &format!("/api/workflows/{id}/runs"),
            None,
        );
        runs.body["runs"].as_array().is_some_and(|r| r.len() == 3)
    });
    let runs = call(
        &studio,
        &signed,
        "GET",
        &format!("/api/workflows/{id}/runs"),
        None,
    );
    let run = &runs.body["runs"][0];
    assert_eq!(run["trigger"]["trigger"], "chat");
    assert_eq!(run["trigger"]["session"], session);
    assert!(
        !runs.body.to_string().contains("k-secret"),
        "the key is in no run"
    );

    // The studio's "try the chat": the editor's workflow as a preview, the conversation kept by the panel.
    let tried = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/chat"),
        Some(
            &json!({
                "message": "Is it you?",
                "history": [{ "role": "user", "content": "Hi" }, { "role": "assistant", "content": "Hello" }],
            })
            .to_string(),
        ),
    );
    assert_eq!(tried.status, 200, "{}", tried.body);
    assert_eq!(tried.body["answer"], "You asked: Is it you?");
    assert_eq!(tried.body["run"]["status"], "done");
    let asked = model.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 4);
    assert_eq!(asked[3]["messages"][0]["content"], "You help a customer.");
    assert_eq!(asked[3]["messages"][1]["content"], "Hi");
    let not_chat = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/chat"),
        Some(r#"{"definition": {"name": "x", "steps": []}, "message": "Hi"}"#),
    );
    assert_eq!(not_chat.status, 422);
}

/// A request's answer as it came: its head and its body, as text.
fn raw(addr: &str, method: &str, path: &str, headers: &[&str]) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut request =
        format!("{method} {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\ncontent-length: 0\r\n");
    for h in headers {
        request.push_str(&format!("{h}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    out
}
