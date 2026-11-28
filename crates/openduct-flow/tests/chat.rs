// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A chat's run, against a stand-in model on this machine: the step a chat streams asks for events, in either API
//! shape, and tells each piece to the chat listening while the whole answer becomes the step's result as it always
//! does; a step tried again says so; a service that will not stream is asked again plainly; a test run sends nothing.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::{Credential, DryRun, Event, Live, Outcome, Secrets, Workflow, chat, run};
use serde_json::{Value as Json, json};

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

/// A stand-in model: each request answered by the next of `answers` (a status, a content type, a body), and what
/// each request asked kept.
fn model(answers: Vec<(u16, &'static str, String)>) -> (String, Arc<Mutex<Vec<Json>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&asked);
    std::thread::spawn(move || {
        for (stream, (status, kind, body)) in listener.incoming().flatten().zip(answers) {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
                if line == "\r\n" {
                    break;
                }
            }
            let mut request = vec![0; length];
            reader.read_exact(&mut request).unwrap();
            kept.lock()
                .unwrap()
                .push(serde_json::from_slice(&request).unwrap());
            let mut out = stream;
            let _ = out.write_all(
                format!("HTTP/1.1 {status} X\r\ncontent-type: {kind}\r\nconnection: close\r\n\r\n{body}")
                    .as_bytes(),
            );
        }
    });
    (addr, asked)
}

fn live(addr: &str, api: &str) -> Live {
    let credential = Credential {
        kind: "ai".to_owned(),
        fields: [
            ("base_url".to_owned(), format!("http://{addr}/v1")),
            ("api_key".to_owned(), "k".to_owned()),
            ("model".to_owned(), "m-1".to_owned()),
            ("api".to_owned(), api.to_owned()),
        ]
        .into(),
    };
    Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(Arc::new(Kept([("model".to_owned(), credential)].into())))
}

const WORKFLOW: &str = r#"
name: Help desk
trigger:
  chat: { stream: answer }
steps:
  - name: answer
    retry: { attempts: 2, wait: 10ms }
    ai:
      credential: model
      messages: "{{ value.history }}"
      prompt: "{{ value.message }}"
"#;

fn chat_event(reply: &str) -> Event {
    let history = [chat::Turn {
        message: "Hi".to_owned(),
        answer: "Hello".to_owned(),
    }];
    Event {
        value: Value::from_json(&chat::event_value(
            "Open on Sunday?",
            "s-0123456789abcdef",
            &history,
            &json!({}),
        )),
        details: Value::from_json(&chat::event_details(
            "s-0123456789abcdef",
            reply,
            Some("answer"),
            "t",
        )),
    }
}

fn result(record: &openduct_flow::RunRecord) -> Json {
    serde_json::from_str(&record.steps[0].result.to_json()).unwrap()
}

#[tokio::test]
async fn a_streamed_step_tells_the_chat_each_piece_and_keeps_the_whole_answer() {
    let events = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m-1\",\"usage\":{\"input_tokens\":11}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Yes, \"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"10 to 4.\"}}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n",
    );
    let (addr, asked) = model(vec![(200, "text/event-stream", events.to_owned())]);
    let effects = live(&addr, "messages");
    let workflow = Workflow::parse(WORKFLOW).unwrap();
    let listening = chat::listen("reply-1");
    let record = run(&workflow, &chat_event("reply-1"), &effects).await;
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.steps);
    let told = listening.receiver.borrow().clone();
    assert_eq!((told.attempt, told.text.as_str()), (1, "Yes, 10 to 4."));
    let answer = result(&record);
    assert_eq!(answer["text"], "Yes, 10 to 4.");
    assert_eq!(answer["usage"]["prompt_tokens"], 11);
    assert_eq!(answer["usage"]["completion_tokens"], 5);
    assert_eq!(
        answer["transcript"][1]["content"], "Hello",
        "the earlier turn was in the conversation"
    );
    let sent = asked.lock().unwrap()[0].clone();
    assert_eq!(sent["stream"], true);
    assert_eq!(sent["messages"][0], json!({ "role": "user", "content": "Hi" }));
    assert_eq!(
        sent["messages"][2],
        json!({ "role": "user", "content": "Open on Sunday?" })
    );
}

#[tokio::test]
async fn a_step_that_fails_in_its_stream_is_tried_again_and_the_chat_starts_its_answer_again() {
    let broken = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Half an ans\"}}]}\n\n",
        "data: {\"error\":{\"message\":\"overloaded\"}}\n\n",
    );
    let whole = concat!(
        "data: {\"model\":\"m-1\",\"choices\":[{\"delta\":{\"content\":\"Yes.\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n",
    );
    let (addr, _) = model(vec![
        (200, "text/event-stream", broken.to_owned()),
        (200, "text/event-stream", whole.to_owned()),
    ]);
    let effects = live(&addr, "chat_completions");
    let workflow = Workflow::parse(WORKFLOW).unwrap();
    let listening = chat::listen("reply-2");
    let record = run(&workflow, &chat_event("reply-2"), &effects).await;
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.steps);
    assert_eq!(record.steps[0].attempts, 2);
    let told = listening.receiver.borrow().clone();
    assert_eq!(
        (told.attempt, told.text.as_str()),
        (2, "Yes."),
        "the half answer was cleared"
    );
    assert_eq!(result(&record)["usage"]["total_tokens"], 4);
}

#[tokio::test]
async fn a_service_that_will_not_stream_is_asked_again_plainly() {
    let whole = json!({
        "model": "m-1",
        "choices": [{ "message": { "role": "assistant", "content": "Plainly." }, "finish_reason": "stop" }],
    });
    let (addr, asked) = model(vec![
        (
            400,
            "application/json",
            r#"{"error":{"message":"stream_options is not supported"}}"#.to_owned(),
        ),
        (200, "application/json", whole.to_string()),
    ]);
    let effects = live(&addr, "chat_completions");
    let workflow = Workflow::parse(WORKFLOW).unwrap();
    let record = run(&workflow, &chat_event("reply-3"), &effects).await;
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.steps);
    assert_eq!(result(&record)["text"], "Plainly.");
    let asked = asked.lock().unwrap();
    assert_eq!(
        (asked[0]["stream"].clone(), asked[1].get("stream")),
        (json!(true), None)
    );
}

#[tokio::test]
async fn a_test_run_of_a_chat_sends_nothing_and_a_step_not_streamed_is_asked_as_always() {
    let workflow = Workflow::parse(WORKFLOW).unwrap();
    let record = run(&workflow, &chat_event("reply-4"), &DryRun::default()).await;
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.steps);
    let shown = result(&record);
    assert_eq!(shown["test"], "not sent");
    assert!(shown["request"]["body"]["content"].get("stream").is_none());

    // A run of a webhook naming the same step streams nothing.
    let mut event = chat_event("reply-5");
    event.details =
        Value::from_json(&json!({ "trigger": "webhook", "reply": "reply-5", "stream": "answer" }));
    let record = run(&workflow, &event, &DryRun::default()).await;
    assert_eq!(record.outcome, Outcome::Done);
}

#[test]
fn a_chat_trigger_is_checked_with_its_workflow() {
    let named = |stream: &str| {
        Workflow::parse(&format!(
            "name: x\ntrigger:\n  chat: {{ stream: {stream} }}\nsteps:\n  - name: answer\n    ai: {{ credential: m, prompt: hi }}\n  - name: tidy\n    set: {{ a: '1' }}\n"
        ))
    };
    assert!(named("answer").is_ok());
    assert!(named("false").is_ok());
    let e = named("tidy").unwrap_err();
    assert!(e.contains("chat.stream: no ai step is called \"tidy\""), "{e}");
    let e = Workflow::parse("name: x\ntrigger:\n  chat: { colour: red }\n").unwrap_err();
    assert!(
        e.starts_with("trigger: chat.colour: not a setting of chat"),
        "{e}"
    );
    let w = Workflow::parse("name: x\ntrigger:\n  chat:\n").unwrap();
    let openduct_flow::Trigger::Chat(chat) = &w.trigger else {
        panic!("{:?}", w.trigger)
    };
    assert_eq!(chat.history, 10);
    assert_eq!(chat.streamed_step(&w.steps), None, "no steps: nothing to stream");
}
