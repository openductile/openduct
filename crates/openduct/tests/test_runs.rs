// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Test runs in `openduct start` that send nothing and still go through the steps after a request, on the two
//! workflows a model built in a first test with a real model, whose test runs stopped. The ticket digest's test run
//! now goes through every step on Zendesk's recorded example, marked as a sample beside the request it would have
//! sent, and asks no model; the lead intake's test run uses an empty stand-in for the table it needs and says so, a
//! test makes no table, a real run against the table not made yet fails plainly, and once the tables API has made it
//! a test reads the table itself. Every business detail is made up.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(all(feature = "studio", unix))]

mod studio;

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use studio::{call, owner, start_with, until};

/// A model on this machine: the bodies it was sent, and its answers, in order.
struct Model {
    addr: String,
    seen: Arc<Mutex<Vec<String>>>,
    answers: Arc<Mutex<VecDeque<Value>>>,
}

fn model() -> Model {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let answers = Arc::new(Mutex::new(VecDeque::new()));
    let (s, a) = (Arc::clone(&seen), Arc::clone(&answers));
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (s, a) = (Arc::clone(&s), Arc::clone(&a));
            std::thread::spawn(move || serve(stream, &s, &a));
        }
    });
    Model { addr, seen, answers }
}

fn serve(stream: TcpStream, seen: &Mutex<Vec<String>>, answers: &Mutex<VecDeque<Value>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut length = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
            break;
        }
        if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = v.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    seen.lock()
        .unwrap()
        .push(String::from_utf8_lossy(&body).into_owned());
    let answer = answers
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or(json!({ "error": "no answer scripted" }));
    let text = answer.to_string();
    let mut out = stream;
    let _ = out.write_all(
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
            text.len()
        )
        .as_bytes(),
    );
}

const TICKETS: &str = r#"
name: Weekday urgent ticket digest
description: "Every weekday at 9:00 Berlin time, read the Zendesk tickets, have an AI agent pick the urgent open ones and summarise each, and e-mail the digest to the support lead."
trigger:
  schedule:
    every: weekday
    at: "09:00"
    timezone: Europe/Berlin
sample:
  scheduled_at: "2026-11-17T08:00:00Z"
steps:
  - name: tickets
    zendesk:
      credential: zendesk
      action: list_tickets
    retry:
      attempts: 3
      wait: 30s
  - name: digest
    agent:
      credential: anthropic
      instructions: "You help a support lead. Pick out the urgent tickets and summarise each in two lines."
      input: |-
        The tickets:
        {{ type(steps.tickets) == list ? steps.tickets.map(t, '#' + string(t.id) + ' | ' + string(t.subject) + ' | status: ' + string(t.status) + ' | priority: ' + string(t.priority) + ' | ' + string(t.description)).join('\n') : string(steps.tickets) }}
      max_turns: 3
  - name: send
    email:
      credential: mail
      to: support-lead@example.com
      subject: Urgent support tickets
      text: "{{ steps.digest.text }}"
"#;

const LEADS: &str = r#"
name: Contact form lead intake
description: "Saves each contact form answer in the leads table, classifies the message, and creates a HubSpot contact for sales leads."
trigger:
  form:
    title: Contact us
    description: Tell us how we can help.
    fields:
      - { name: name, label: Name, type: text, required: true }
      - { name: company, label: Company, type: text }
      - { name: email, label: E-mail, type: email, required: true }
      - { name: message, label: Message, type: long_text, required: true, max_length: 3000 }
    thanks: "Thank you, we will be in touch."
sample:
  name: Sam Lee
  company: Lee Bakery
  email: sam@leebakery.example
  message: Could you send a quote for 200 bags?
steps:
  - name: save_lead
    table:
      name: leads
      action: insert
      row:
        name: "{{ value.name }}"
        company: "{{ value.company }}"
        email: "{{ value.email }}"
        message: "{{ value.message }}"
  - name: sort
    classify:
      credential: anthropic
      text: "{{ value.message }}"
      instructions: "Decide whether the message is a sales enquiry, a support request, or something else."
      categories:
        - name: sales
          description: "Asks about prices, quotes, plans, demos or buying"
          steps:
            - name: crm_contact
              hubspot:
                credential: hubspot
                action: create_contact
                email: "{{ value.email }}"
                firstname: "{{ value.name }}"
                company: "{{ value.company }}"
        - name: support
          description: Needs help with something that does not work or an existing order
      default:
        - name: other_kept
          set:
            kind: "'other'"
"#;

/// An AI credential for the scripted model.
fn ai_credential(model: &Model) -> Value {
    json!({ "name": "anthropic", "kind": "ai",
        "fields": { "base_url": format!("http://{}/v1", model.addr), "api_key": "a-made-up-key", "model": "m-1" } })
}

/// The newest run of the workflow, once it has failed.
fn failed_run(studio: &str, signed: &[String], id: &str) -> Value {
    let mut failed = Value::Null;
    until(30, "the real run failed", || {
        let runs = call(studio, signed, "GET", &format!("/api/workflows/{id}/runs"), None);
        failed = runs.body["runs"]
            .as_array()
            .and_then(|r| r.first().cloned())
            .unwrap_or(Value::Null);
        failed["status"] == "failed"
    });
    failed
}

#[test]
fn test_runs_answer_with_samples_and_use_an_empty_stand_in_for_a_table_not_made_yet() {
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
    let post = |path: &str, body: &Value| call(&studio, &signed, "POST", path, Some(&body.to_string()));
    assert_eq!(post("/api/credentials", &ai_credential(&model)).status, 201);
    // An answer for a model asked, which no test run should take.
    model
        .answers
        .lock()
        .unwrap()
        .push_back(json!({ "error": "a test run asked the model" }));
    let save = |yaml: &str| -> (String, Value) {
        let definition = openduct_flow::yaml_to_json(yaml).unwrap();
        let saved = post("/api/workflows", &json!({ "definition": definition }));
        assert_eq!(saved.status, 201, "{}", saved.body);
        (
            saved.body["id"].as_str().unwrap().to_owned(),
            definition["sample"].clone(),
        )
    };
    let test = |id: &str, sample: &Value| -> Value {
        let tested = post(&format!("/api/workflows/{id}/test"), &json!({ "input": sample }));
        assert_eq!(tested.status, 200, "{}", tested.body);
        tested.body["run"].clone()
    };

    // The lead intake: its test run uses an empty table leads in place of the one not made yet, and says so.
    let (id, sample) = save(LEADS);
    let run = test(&id, &sample);
    assert_eq!(run["status"], "done", "{run}");
    assert_eq!(run["steps"][0]["error"], Value::Null);
    assert_eq!(
        run["steps"][0]["note"],
        "the table leads does not exist yet: this test used an empty one"
    );
    assert_eq!(run["steps"][0]["result"]["rows"][0]["company"], "Lee Bakery");
    let tables = || call(&studio, &signed, "GET", "/api/tables", None).body["tables"].clone();
    assert_eq!(tables(), json!([]), "a test makes no table");

    // A real run against the table not made yet fails, plainly.
    assert_eq!(
        post(&format!("/api/workflows/{id}/active"), &json!({ "active": true })).status,
        200
    );
    let queued = post(&format!("/api/workflows/{id}/run"), &json!({ "input": sample }));
    assert_eq!(queued.status, 202, "{}", queued.body);
    let failed = failed_run(&studio, &signed, &id);
    assert_eq!(
        failed["error"], "there is no table leads: make it under Tables",
        "{failed}"
    );

    // Once the table is made, a test reads it itself: no stand-in, no note.
    let columns = json!([
        { "name": "name", "type": "text" }, { "name": "company", "type": "text" },
        { "name": "email", "type": "text" }, { "name": "message", "type": "text" },
    ]);
    let made = post("/api/tables", &json!({ "name": "leads", "columns": columns }));
    assert_eq!(made.status, 201, "{}", made.body);
    assert_eq!(tables()[0]["columns"], columns);
    let run = test(&id, &sample);
    assert_eq!(run["status"], "done", "{run}");
    assert!(run["steps"][0].get("note").is_none());

    // The ticket digest: its test run goes through every step on Zendesk's recorded example.
    let (id, sample) = save(TICKETS);
    let run = test(&id, &sample);
    assert_eq!(run["status"], "done", "{run}");
    let steps: Vec<&str> = run["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(steps, ["tickets", "digest", "send"]);
    let tickets = &run["steps"][0];
    assert_eq!(tickets["sample"], "sample answer, nothing sent");
    assert!(
        tickets["result"].as_array().is_some_and(|t| t.len() == 2),
        "{tickets}"
    );
    assert!(
        tickets["request"]["url"]
            .as_str()
            .unwrap()
            .ends_with("zendesk.com/api/v2/tickets.json"),
        "{tickets}"
    );
    assert!(model.seen.lock().unwrap().is_empty(), "a test run asks no model");
    assert_eq!(model.answers.lock().unwrap().len(), 1, "its answer still there");
}
