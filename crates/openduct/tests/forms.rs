// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Forms and runs that wait, in `openduct start`: a form trigger's page and its answers (checked, protected, each a
//! run); an approval that waits across a crash of the program and takes one answer from its page; a wait for a call
//! that takes the call at its one-time address.

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
use std::net::TcpStream;
use std::time::Duration;

use serde_json::{Value, json};
use studio::{JSON, call, http, owner, start, stop, until};

/// A request whose answer is read as text: a page.
fn page(addr: &str, method: &str, path: &str, extra: &[&str], body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut request = format!("{method} {path} HTTP/1.1\r\nconnection: close\r\nhost: {addr}\r\n");
    for header in extra {
        request.push_str(header);
        request.push_str("\r\n");
    }
    let body = body.unwrap_or_default();
    request.push_str(&format!("content-length: {}\r\n\r\n{body}", body.len()));
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    (head.split(' ').nth(1).unwrap().parse().unwrap(), body.to_owned())
}

/// The page's own token, from its hidden field.
fn token_of(html: &str) -> String {
    let at = html.find("name=\"_form\" value=\"").unwrap() + "name=\"_form\" value=\"".len();
    html[at..].split('"').next().unwrap().to_owned()
}

/// `a=1&b=x y` as a form sends it.
fn encoded(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| {
            let enc = |t: &str| {
                t.bytes()
                    .map(|b| match b {
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                            (b as char).to_string()
                        }
                        b' ' => "+".to_owned(),
                        other => format!("%{other:02X}"),
                    })
                    .collect::<String>()
            };
            format!("{}={}", enc(k), enc(v))
        })
        .collect::<Vec<_>>()
        .join("&")
}

const FORM: &str = "content-type: application/x-www-form-urlencoded";

fn make(studio: &str, signed: &[String], definition: &Value) -> Value {
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
    on.body
}

fn runs(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body["runs"].as_array().cloned().unwrap_or_default()
}

fn waiting(studio: &str, signed: &[String], id: &str) -> Vec<Value> {
    let answer = call(
        studio,
        signed,
        "GET",
        &format!("/api/workflows/{id}/waiting"),
        None,
    );
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body["waiting"].as_array().cloned().unwrap_or_default()
}

fn signed_in(studio: &str) -> Vec<String> {
    let answer = http(
        studio,
        "POST",
        "/api/sign-in",
        &[JSON],
        Some(r#"{"email": "ada@example.com", "password": "correct horse battery"}"#),
    );
    vec![
        format!("cookie: {}", answer.cookie()),
        format!("x-openduct-csrf: {}", answer.body["csrf"].as_str().unwrap()),
        JSON.to_owned(),
    ]
}

#[test]
fn a_form_starts_a_run_with_its_answers_and_refuses_what_it_should() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let workflow = make(
        &studio,
        &signed,
        &json!({
            "name": "Contact",
            "trigger": { "form": {
                "title": "Contact <us>",
                "fields": [
                    { "name": "email", "type": "email", "required": true },
                    { "name": "size", "type": "dropdown", "options": ["S", "M"] },
                    { "name": "note", "type": "long_text" },
                ],
                "thanks": "Thanks: we write back soon.",
                "rate_limit": 3,
            } },
            "steps": [{ "name": "got", "set": { "email": "value.email", "size": "value.size" } }],
        }),
    );
    let id = workflow["id"].as_str().unwrap();
    let address = workflow["form"].as_str().unwrap().to_owned();
    assert!(address.starts_with("/forms/"), "{workflow}");

    let (status, html) = page(&studio, "GET", &address, &[], None);
    assert_eq!(status, 200);
    assert!(html.contains("<h1>Contact &lt;us&gt;</h1>"), "{html}");
    assert!(html.contains("type=\"email\"") && html.contains(" required"));
    assert!(!html.contains("<script"), "no script on a form's page");
    let token = token_of(&html);

    // A wrong answer: the page again, saying what is wrong, with what was typed.
    let wrong = encoded(&[
        ("_form", &token),
        ("email", "not-an-address"),
        ("size", "XL"),
        ("note", "hi <b>"),
    ]);
    let (status, html) = page(&studio, "POST", &address, &[FORM], Some(&wrong));
    assert_eq!(status, 422);
    assert!(html.contains("An e-mail address, as name@example.com."), "{html}");
    assert!(html.contains("Choose from the choices given."));
    assert!(
        html.contains("hi &lt;b&gt;"),
        "what was typed, shown again and escaped"
    );
    // No page token: not taken.
    let (status, _) = page(
        &studio,
        "POST",
        &address,
        &[FORM],
        Some(&encoded(&[("email", "a@b.test")])),
    );
    assert_eq!(status, 400);
    // From another site: not taken.
    let (status, _) = page(
        &studio,
        "POST",
        &address,
        &[FORM, "origin: https://evil.example"],
        Some(&wrong),
    );
    assert_eq!(status, 403);

    let good = encoded(&[("_form", &token), ("email", "ada@example.com"), ("size", "M")]);
    let (status, html) = page(&studio, "POST", &address, &[FORM], Some(&good));
    assert_eq!(status, 200, "{html}");
    assert!(html.contains("Thanks: we write back soon."));
    // Three answers from one address this minute (one from another site is not counted): the fourth waits.
    let (status, _) = page(&studio, "POST", &address, &[FORM], Some(&good));
    assert_eq!(status, 429);

    until(30, "the form's run", || {
        runs(&studio, &signed, id).iter().any(|r| r["status"] == "done")
    });
    let run = &runs(&studio, &signed, id)[0];
    assert_eq!(
        run["steps"][0]["result"],
        json!({ "email": "ada@example.com", "size": "M" })
    );
    assert_eq!(run["trigger"]["trigger"], "form");
    // A form trigger's hook is not a webhook.
    let hook = address.replace("/forms/", "/hooks/");
    assert_eq!(http(&studio, "POST", &hook, &[JSON], Some("{}")).status, 404);
    stop(running);
}

#[test]
fn an_approval_waits_across_a_crash_and_takes_one_answer_from_its_page() {
    let data = tempfile::tempdir().unwrap();
    let mut running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let workflow = make(
        &studio,
        &signed,
        &json!({
            "name": "Refunds",
            "trigger": "manual",
            "steps": [
                { "name": "order", "set": { "id": "value.id", "amount": "value.amount" } },
                { "name": "link", "set": { "url": "links.ok" } },
                { "name": "ok", "approval": { "title": "Refund {{ steps.order.amount }} on {{ steps.order.id }}?", "comment": true, "timeout": "2d" } },
                { "name": "after", "set": { "decision": "steps.ok.decision", "note": "steps.ok.comment" } },
            ],
        }),
    );
    let id = workflow["id"].as_str().unwrap().to_owned();
    let ran = call(
        &studio,
        &signed,
        "POST",
        &format!("/api/workflows/{id}/run"),
        Some(r#"{"input": {"id": "A-7", "amount": 40}}"#),
    );
    assert_eq!(ran.status, 202, "{}", ran.body);
    until(30, "the run to wait", || {
        waiting(&studio, &signed, &id).len() == 1
    });
    let shown = runs(&studio, &signed, &id);
    assert_eq!(shown.len(), 1);
    assert_eq!(shown[0]["status"], "waiting");
    assert_eq!(shown[0]["waiting"]["step"], "ok");
    // The address an earlier step had is the address it waits at.
    let url = waiting(&studio, &signed, &id)[0]["url"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(shown[0]["steps"][1]["result"]["url"], url);
    let path = format!("/waits/{}", url.rsplit('/').next().unwrap());

    // Killed while it waits: nothing of it is in memory any more.
    running.child.kill().unwrap();
    running.child.wait().unwrap();
    drop(running);
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = signed_in(&studio);
    until(40, "the run still waiting after the restart", || {
        waiting(&studio, &signed, &id).len() == 1
    });

    let (status, html) = page(&studio, "GET", &path, &[], None);
    assert_eq!(status, 200, "{html}");
    assert!(html.contains("Refund 40 on A-7?"), "{html}");
    assert!(html.contains("value=\"approve\"") && html.contains("value=\"reject\""));
    let token = token_of(&html);
    let yes = encoded(&[("_form", &token), ("_decision", "approve"), ("_comment", "fine")]);
    let (status, html) = page(&studio, "POST", &path, &[FORM], Some(&yes));
    assert_eq!(status, 200, "{html}");
    let no = encoded(&[("_form", &token), ("_decision", "reject")]);
    let (status, html) = page(&studio, "POST", &path, &[FORM], Some(&no));
    assert_eq!(status, 409, "a second answer is not taken");
    assert!(html.contains("had its answer"));

    until(40, "the run to go on", || {
        runs(&studio, &signed, &id).iter().any(|r| r["status"] == "done")
    });
    let shown = runs(&studio, &signed, &id);
    assert_eq!(shown.len(), 1, "one run, from its start to its end: {shown:?}");
    let steps = shown[0]["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 4);
    assert_eq!(steps[2]["result"]["approved"], true);
    assert_eq!(
        steps[3]["result"],
        json!({ "decision": "approved", "note": "fine" })
    );
    until(20, "nothing waiting any more", || {
        waiting(&studio, &signed, &id).is_empty()
    });
    let (status, _) = page(&studio, "GET", &path, &[], None);
    assert_eq!(status, 404, "a one-time address is gone once used");
    stop(running);
}

#[test]
fn a_wait_for_a_call_takes_the_call_at_its_address_once() {
    let data = tempfile::tempdir().unwrap();
    let running = start(data.path());
    let studio = running.studio.clone();
    let signed = owner(&studio);
    let workflow = make(
        &studio,
        &signed,
        &json!({
            "name": "Callback",
            "trigger": "webhook",
            "steps": [
                { "name": "hand", "set": { "callback": "links.back" } },
                { "name": "back", "wait": { "webhook": true, "timeout": "1h" } },
                { "name": "then", "set": { "job": "steps.back.body.job", "how": "steps.back.method" } },
            ],
        }),
    );
    let id = workflow["id"].as_str().unwrap().to_owned();
    let hook = workflow["hook"].as_str().unwrap().to_owned();
    assert_eq!(http(&studio, "POST", &hook, &[JSON], Some("{}")).status, 202);
    until(30, "the run to wait for its call", || {
        waiting(&studio, &signed, &id).len() == 1
    });
    let url = waiting(&studio, &signed, &id)[0]["url"]
        .as_str()
        .unwrap()
        .to_owned();
    let path = format!("/waits/{}", url.rsplit('/').next().unwrap());
    let called = http(
        &studio,
        "PUT",
        &path,
        &[JSON, "x-openduct-token: hidden"],
        Some(r#"{"job": 9}"#),
    );
    assert_eq!(called.status, 200, "{}", called.body);
    assert_eq!(called.body["resumed"], true);
    let again = http(&studio, "POST", &path, &[JSON], Some(r#"{"job": 10}"#));
    assert_eq!(again.status, 409);
    until(30, "the run to go on with the call", || {
        runs(&studio, &signed, &id).iter().any(|r| r["status"] == "done")
    });
    let run = &runs(&studio, &signed, &id)[0];
    assert_eq!(run["steps"][2]["result"], json!({ "job": 9, "how": "PUT" }));
    assert!(
        run["steps"][1]["result"]["headers"]
            .get("x-openduct-token")
            .is_none(),
        "secrets are not kept"
    );
    // An address nothing waits at, after a moment's hold.
    let unknown = http(
        &studio,
        "POST",
        "/waits/AAAAAAAAAAAAAAAAAAAAAA",
        &[JSON],
        Some("{}"),
    );
    assert_eq!(unknown.status, 404);
    assert_eq!(
        http(&studio, "POST", "/waits/..%2F..%2Fetc", &[JSON], Some("{}")).status,
        404
    );
    stop(running);
}
