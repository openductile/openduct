// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows: read and checked, then run - each step's result for the next, branches, loops, retries, failures.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use openduct_expr::Value;
use openduct_flow::{DryRun, Effects, Event, HttpRequest, Live, Outcome, Trigger, Workflow, run};
use serde_json::Value as Json;

fn event(json: &str) -> Event {
    Event {
        value: Value::parse_json(json).unwrap(),
        details: Value::parse_json(r#"{"trigger": "webhook"}"#).unwrap(),
    }
}

fn go(workflow: &Workflow, event: &Event, effects: &dyn Effects) -> openduct_flow::RunRecord {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(workflow, event, effects))
}

fn result(record: &openduct_flow::RunRecord, step: &str) -> Json {
    let found = record
        .steps
        .iter()
        .find(|s| s.name == step)
        .unwrap_or_else(|| panic!("no step {step}"));
    serde_json::from_str(&found.result.to_json()).unwrap()
}

const ORDERS: &str = r#"
name: Big orders to the team
trigger:
  webhook: { token: s3cret }
steps:
  - name: order
    set:
      total: "value.lines.map(l, l.qty * l.price).sum()"
      who: value.customer.trim()
  - where: steps.order.total > 100
  - name: size
    if:
      when: steps.order.total > 500
      then:
        - name: big
          set: { label: "'big'" }
      else:
        - name: medium
          set: { label: "'medium'" }
  - name: tell
    http:
      method: POST
      url: "https://chat.example.com/hooks/{{ trigger.trigger }}"
      headers: { x-order: "{{ value.id }}" }
      json:
        text: "Order {{ value.id }} from {{ steps.order.who }}: {{ steps.order.total }}"
        lines: "{{ value.lines }}"
  - name: keep
    topic: { name: big-orders, key: "{{ value.id }}", value: { id: "{{ value.id }}", total: "{{ steps.order.total }}" } }
"#;

#[test]
fn a_workflow_is_read_checked_and_named() {
    let w = Workflow::parse(ORDERS).unwrap();
    assert_eq!(w.id, "big-orders-to-the-team");
    assert_eq!(
        w.trigger,
        Trigger::Webhook {
            token: Some("s3cret".into())
        }
    );
    assert_eq!(w.steps.len(), 5);
    assert_eq!(
        w.steps[1].name, "where_2",
        "a step without a name is named by its kind and place"
    );
    let wrong = |text: &str| Workflow::parse(text).unwrap_err();
    assert!(
        wrong("name: x\nsteps:\n  - http: { url: 'x' }\n    set: { a: '1' }\n").contains("one of set"),
        "two kinds"
    );
    assert!(
        wrong("name: x\nsteps:\n  - set: { a: 'nosuch(1)' }\n")
            .contains("steps[0] (set_1).set.a: there is no function \"nosuch\"")
    );
    assert!(
        wrong("name: x\nsteps:\n  - name: a\n    set: { b: '1' }\n  - name: a\n    set: { b: '1' }\n")
            .contains("another step has this name")
    );
    assert!(wrong("name: x\ntrigger: { schedule: { every: soon } }\n").contains("schedule.every"));
    assert!(wrong("name: x\nsteps:\n  - http: { url: 'https://x/{{ value.id' }\n").contains("no }}"));
    assert!(wrong("name: x\nsteps:\n  - wait: 2h\n").contains("at most 1h"));
    assert!(wrong("name: x\nfrom: y\n").contains("\"from\" is not a workflow setting"));
}

#[test]
fn each_step_reads_what_the_steps_before_it_made() {
    let w = Workflow::parse(ORDERS).unwrap();
    let order = event(
        r#"{"id": "A-7", "customer": " Ada ", "lines": [{"qty": 3, "price": 70}, {"qty": 1, "price": 9.5}]}"#,
    );
    let record = go(&w, &order, &DryRun::default());
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(result(&record, "order")["total"], serde_json::json!(219.5));
    assert_eq!(result(&record, "size")["branch"], "else");
    assert_eq!(result(&record, "medium")["label"], "medium");
    assert!(
        record.steps.iter().all(|s| s.name != "big"),
        "the branch not taken does not run"
    );
    let sent = result(&record, "tell")["request"].clone();
    assert_eq!(sent["url"], "https://chat.example.com/hooks/webhook");
    assert_eq!(sent["headers"]["x-order"], "A-7");
    assert_eq!(sent["body"]["content"]["text"], "Order A-7 from Ada: 219.5");
    assert_eq!(
        sent["body"]["content"]["lines"][0]["qty"], 3,
        "a template that is one value keeps it"
    );
    assert_eq!(
        result(&record, "keep")["value"],
        serde_json::json!({ "id": "A-7", "total": 219.5 })
    );

    let small = go(
        &w,
        &event(r#"{"id": "A-8", "customer": "Bo", "lines": [{"qty": 1, "price": 5}]}"#),
        &DryRun::default(),
    );
    assert_eq!(small.outcome, Outcome::Skipped { at: "where_2".into() });
    assert_eq!(small.steps.len(), 2, "nothing after the where");
}

#[test]
fn lists_are_gone_through_one_item_at_a_time() {
    let w = Workflow::parse(
        "name: lines\nsteps:\n  - name: each\n    for_each:\n      items: value.lines\n      steps:\n        - name: line\n          set: { sku: item.sku, place: index }\n        - where: item.qty > 0\n        - name: priced\n          set: { amount: item.qty * item.price }\n",
    )
    .unwrap();
    let record = go(
        &w,
        &event(r#"{"lines": [{"sku": "a", "qty": 2, "price": 5}, {"sku": "b", "qty": 0, "price": 9}]}"#),
        &DryRun::default(),
    );
    assert_eq!(record.outcome, Outcome::Done);
    let each = result(&record, "each");
    assert_eq!(each[0]["line"], serde_json::json!({ "sku": "a", "place": 0 }));
    assert_eq!(each[0]["priced"]["amount"], 10);
    assert_eq!(
        each[1]["line"]["sku"], "b",
        "a where inside a loop skips only its item"
    );
}

/// Effects whose HTTP calls fail a set number of times, then answer.
struct Flaky {
    failures: AtomicU32,
    calls: AtomicU32,
    status: u16,
}

impl Effects for Flaky {
    fn http<'a>(
        &'a self,
        _: HttpRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failures.load(Ordering::SeqCst) > 0 {
                self.failures.fetch_sub(1, Ordering::SeqCst);
                return Err("the connection was dropped".to_owned());
            }
            Ok(Value::from_json(
                &serde_json::json!({ "status": self.status, "body": "busy right now" }),
            ))
        })
    }
    fn write<'a>(
        &'a self,
        _: &'a str,
        _: Option<String>,
        _: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(Value::Null) })
    }
    fn file<'a>(
        &'a self,
        _: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(Value::list(Vec::new())) })
    }

    fn credential(&self, _: &str) -> Result<openduct_flow::Credential, String> {
        Ok(openduct_flow::Credential::default())
    }
}

#[test]
fn a_step_is_tried_again_and_a_failure_ends_the_run_with_the_reason() {
    let retried = Workflow::parse("name: r\nsteps:\n  - name: call\n    http: { url: 'https://api.example.com' }\n    retry: { attempts: 3, wait: 10ms }\n").unwrap();
    let flaky = Flaky {
        failures: AtomicU32::new(2),
        calls: AtomicU32::new(0),
        status: 200,
    };
    let record = go(&retried, &event("{}"), &flaky);
    assert_eq!(record.outcome, Outcome::Done);
    assert_eq!(
        (flaky.calls.load(Ordering::SeqCst), record.steps[0].attempts),
        (3, 3)
    );

    let refused = Flaky {
        failures: AtomicU32::new(0),
        calls: AtomicU32::new(0),
        status: 503,
    };
    let record = go(&retried, &event("{}"), &refused);
    assert_eq!(
        record.outcome,
        Outcome::Failed {
            at: "call".into(),
            error: "answered 503: busy right now".into()
        }
    );

    let carry_on = Workflow::parse("name: c\nsteps:\n  - name: call\n    http: { url: 'https://api.example.com' }\n    on_error: continue\n  - name: after\n    set: { saw: steps.call.error }\n").unwrap();
    let record = go(&carry_on, &event("{}"), &refused);
    assert_eq!(record.outcome, Outcome::Done);
    assert_eq!(result(&record, "after")["saw"], "answered 503: busy right now");

    let stopped = Workflow::parse(
        "name: s\nsteps:\n  - stop: { error: 'no customer for {{ value.id }}' }\n  - set: { never: '1' }\n",
    )
    .unwrap();
    let record = go(&stopped, &event(r#"{"id": 4}"#), &DryRun::default());
    assert_eq!(
        record.outcome,
        Outcome::Failed {
            at: "stop_1".into(),
            error: "no customer for 4".into()
        }
    );
    assert_eq!(record.steps.len(), 1);
}

/// A stand-in API on this machine: answers each request with what it received.
fn echo_server() -> (
    String,
    std::thread::JoinHandle<()>,
    std::sync::Arc<Mutex<Vec<String>>>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let kept = std::sync::Arc::clone(&seen);
    let handle = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).into_owned();
        kept.lock().unwrap().push(request);
        let body = r#"{"ok": true, "id": 42}"#;
        let answer = format!(
            "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(answer.as_bytes()).unwrap();
    });
    (addr, handle, seen)
}

#[test]
fn an_http_step_calls_out_and_reads_the_answer_but_not_this_machines_network_unless_allowed() {
    let (addr, server, seen) = echo_server();
    let w = Workflow::parse(&format!(
        "name: call\nsteps:\n  - name: post\n    http:\n      method: POST\n      url: 'http://{addr}/orders'\n      query: {{ source: openduct }}\n      json: {{ id: '{{{{ value.id }}}}' }}\n"
    ))
    .unwrap();
    let allowed = Live::new("127.0.0.1:1", true).unwrap();
    let record = go(&w, &event(r#"{"id": "A-9"}"#), &allowed);
    server.join().unwrap();
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(result(&record, "post")["status"], 201);
    assert_eq!(
        result(&record, "post")["body"]["id"],
        42,
        "a JSON answer is read as JSON"
    );
    let request = seen.lock().unwrap()[0].clone();
    assert!(
        request.starts_with("POST /orders?source=openduct HTTP/1.1"),
        "{request}"
    );
    assert!(request.ends_with(r#"{"id":"A-9"}"#), "{request}");

    let guarded = Live::new("127.0.0.1:1", false).unwrap();
    let record = go(&w, &event(r#"{"id": "A-9"}"#), &guarded);
    match &record.outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("--allow-private-network"), "{error}"),
        other => panic!("{other:?}"),
    }
    let by_name = Workflow::parse("name: n\nsteps:\n  - http: { url: 'http://localhost:9/' }\n").unwrap();
    match go(&by_name, &event("{}"), &guarded).outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("own network"), "{error}"),
        other => panic!("a name that resolves to this machine: {other:?}"),
    }
}

/// Credentials kept in memory, for the tests.
#[derive(Debug)]
struct Kept(std::collections::BTreeMap<String, openduct_flow::Credential>);

impl openduct_flow::Secrets for Kept {
    fn get(&self, name: &str) -> Option<openduct_flow::Credential> {
        self.0.get(name).cloned()
    }
}

fn credential(kind: &str, fields: &[(&str, &str)]) -> openduct_flow::Credential {
    openduct_flow::Credential {
        kind: kind.to_owned(),
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
    }
}

/// A stand-in server answering one request with `body`, keeping what it was sent.
fn answering(body: &'static str) -> (String, std::thread::JoinHandle<String>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = vec![0u8; 65536];
        let mut got = Vec::new();
        loop {
            let n = stream.read(&mut buf).unwrap();
            got.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&got);
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
                    break;
                }
            }
        }
        let answer = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(answer.as_bytes()).unwrap();
        String::from_utf8_lossy(&got).into_owned()
    });
    (addr, handle)
}

#[test]
fn a_test_run_shows_what_each_app_would_be_sent_with_its_secrets_hidden() {
    let w = Workflow::parse(
        "name: tell\nsteps:\n  - name: tg\n    telegram: { credential: bot, chat_id: '42', text: 'Order {{ value.id }}' }\n  - name: sl\n    slack: { credential: team, channel: '#sales', text: 'Order {{ value.id }}' }\n  - name: think\n    ai: { credential: brain, model: some-model, prompt: 'Sort {{ value.id }}', json: true }\n  - name: api\n    http: { url: 'https://api.example.com', credential: key }\n  - name: gc\n    google_chat: { credential: space, text: 'Order {{ value.id }}' }\n",
    )
    .unwrap();
    let mut test = DryRun::default();
    test.credentials.insert(
        "space".into(),
        credential(
            "google_chat_webhook",
            &[(
                "url",
                "https://chat.googleapis.com/v1/spaces/AAA/messages?key=k&token=t",
            )],
        ),
    );
    test.credentials.insert(
        "brain".into(),
        credential(
            "ai",
            &[
                ("base_url", "https://models.example.com/v1"),
                ("api", "messages"),
                ("api_key", "secret-1"),
                ("key_header", "x-api-key"),
            ],
        ),
    );
    let record = go(&w, &event(r#"{"id": "A-1"}"#), &test);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let tg = result(&record, "tg")["request"].clone();
    assert_eq!(tg["url"], "https://api.telegram.org/bot•••/sendMessage");
    assert_eq!(
        tg["body"]["content"],
        serde_json::json!({ "chat_id": "42", "text": "Order A-1" })
    );
    assert_eq!(
        result(&record, "sl")["request"]["headers"]["authorization"],
        "Bearer •••"
    );
    let ai = result(&record, "think")["request"].clone();
    assert_eq!(
        ai["url"], "https://models.example.com/v1/messages",
        "the credential's address and its API's shape"
    );
    assert_eq!(ai["headers"]["x-api-key"], "•••");
    assert_eq!(
        result(&record, "api")["request"]["headers"]["authorization"],
        "•••"
    );
    // A Google Chat space's webhook: its address is the secret; the message is its text.
    let gc = result(&record, "gc")["request"].clone();
    assert_eq!(gc["url"], "•••");
    assert_eq!(gc["body"]["content"], serde_json::json!({ "text": "Order A-1" }));
    let unknown =
        Workflow::parse("name: x\nsteps:\n  - telegram: { credential: bot, text: hi }\n").unwrap_err();
    assert!(unknown.contains("chat_id: Telegram's send needs it"), "{unknown}");
}

#[test]
fn an_ai_step_asks_the_model_and_reads_its_json_and_a_messenger_that_refuses_fails_the_step() {
    let (addr, server) = answering(
        r#"{"choices": [{"message": {"content": "```json\n{\"mood\": \"happy\"}\n```"}}], "usage": {"prompt_tokens": 12, "completion_tokens": 5}}"#,
    );
    let w = Workflow::parse(
        "name: mood\nsteps:\n  - name: read\n    ai: { credential: local, model: tiny, system: 'You sort moods.', prompt: 'Mood of: {{ value.text }}', json: true, temperature: 0 }\n  - name: after\n    set: { mood: steps.read.json.mood }\n",
    )
    .unwrap();
    let mut kept = std::collections::BTreeMap::new();
    kept.insert(
        "local".to_owned(),
        credential(
            "ai",
            &[
                ("api_key", "key-test"),
                ("base_url", &format!("http://{addr}/v1")),
            ],
        ),
    );
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(std::sync::Arc::new(Kept(kept)));
    let record = go(&w, &event(r#"{"text": "what a day!"}"#), &live);
    let sent = server.join().unwrap();
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        result(&record, "after")["mood"],
        "happy",
        "JSON in a fenced block is read"
    );
    assert_eq!(result(&record, "read")["usage"]["prompt_tokens"], 12);
    assert_eq!(
        result(&record, "read")["transcript"][2]["role"],
        "assistant",
        "the transcript ends with the answer"
    );
    assert!(result(&record, "read")["metrics"]["latency_ms"].is_number());
    assert!(sent.starts_with("POST /v1/chat/completions"), "{sent}");
    assert!(
        sent.to_ascii_lowercase()
            .contains("authorization: bearer key-test"),
        "the key is sent to the model"
    );
    assert!(
        sent.contains(r#""response_format":{"type":"json_object"}"#),
        "{sent}"
    );
    assert!(
        !format!("{:?}", record.steps).contains("key-test"),
        "and kept nowhere in the run"
    );

    let (addr, server) = answering(r#"{"ok": false, "error": "channel_not_found"}"#);
    let w = Workflow::parse("name: s\nsteps:\n  - slack: { credential: hook, text: hi }\n").unwrap();
    let mut kept = std::collections::BTreeMap::new();
    kept.insert(
        "hook".to_owned(),
        credential("slack_webhook", &[("url", &format!("http://{addr}/x"))]),
    );
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(std::sync::Arc::new(Kept(kept)));
    let record = go(&w, &event("{}"), &live);
    server.join().unwrap();
    assert_eq!(
        record.outcome,
        Outcome::Failed {
            at: "slack_1".into(),
            error: "Slack refused it: channel_not_found".into()
        }
    );

    let mut kept = std::collections::BTreeMap::new();
    kept.insert("hook".to_owned(), credential("telegram", &[("bot_token", "x")]));
    let live = Live::new("127.0.0.1:1", false)
        .unwrap()
        .with_secrets(std::sync::Arc::new(Kept(kept)));
    match go(&w, &event("{}"), &live).outcome {
        Outcome::Failed { error, .. } => assert_eq!(
            error,
            "hook is a telegram credential: Slack takes slack or slack_webhook"
        ),
        other => panic!("{other:?}"),
    }
    match go(&w, &event("{}"), &Live::new("127.0.0.1:1", false).unwrap()).outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("no credential called \"hook\""), "{error}"),
        other => panic!("{other:?}"),
    }
}

/// Live effects that answer every request, keep what was sent, and hand out a credential with a secret.
#[derive(Debug, Default)]
struct Recorded {
    sent: Mutex<Vec<String>>,
}

impl Effects for Recorded {
    fn http<'a>(
        &'a self,
        r: HttpRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            self.sent.lock().unwrap().push(format!("{} {}", r.method, r.url));
            let body = if r.url.contains("/chat/completions") {
                serde_json::json!({ "choices": [{ "message": { "content": "a summary" }, "finish_reason": "stop" }] })
            } else {
                serde_json::json!({ "price": 12 })
            };
            Ok(Value::from_json(
                &serde_json::json!({ "status": 200, "body": body }),
            ))
        })
    }
    fn write<'a>(
        &'a self,
        _: &'a str,
        _: Option<String>,
        _: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { panic!("a preview writes nothing") })
    }
    fn credential(&self, name: &str) -> Result<openduct_flow::Credential, String> {
        Ok(match name {
            "bot" => credential("telegram", &[("bot_token", "123456:SECRET-TOKEN")]),
            _ => credential(
                "ai",
                &[
                    ("base_url", "https://models.example.com/v1"),
                    ("api_key", "key-SECRET"),
                ],
            ),
        })
    }
    fn file<'a>(
        &'a self,
        _: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async { Ok(Value::parse_json(r#"[{"sku": "a", "name": "Apple"}]"#).unwrap()) })
    }
}

#[test]
fn a_preview_reads_for_real_and_only_shows_its_writes_with_their_secrets_hidden() {
    let w = Workflow::parse(
        "name: p\nsteps:\n  - name: price\n    http: { url: 'https://api.example.com/price' }\n  - name: item\n    lookup: { file: /items.csv, where: { sku: '{{ value.sku }}' } }\n  - name: sum\n    ai: { credential: model, model: m, prompt: 'Summarise {{ steps.item.name }}' }\n  - name: tell\n    telegram: { credential: bot, chat_id: '42', text: '{{ steps.sum.text }} at {{ steps.price.body.price }}' }\n  - name: keep\n    topic: { name: out }\n",
    )
    .unwrap();
    let live = std::sync::Arc::new(Recorded::default());
    let preview = openduct_flow::Preview::new(live.clone());
    let record = go(&w, &event(r#"{"sku": "a"}"#), &preview);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let sent = live.sent.lock().unwrap().clone();
    assert_eq!(
        sent,
        [
            "GET https://api.example.com/price",
            "POST https://models.example.com/v1/chat/completions"
        ],
        "only the reads are sent"
    );
    assert_eq!(result(&record, "item")["name"], "Apple", "a lookup is a read");
    let shown = result(&record, "tell")["request"].clone();
    assert_eq!(
        shown["url"], "https://api.telegram.org/bot•••/sendMessage",
        "the token is hidden in what is shown"
    );
    assert_eq!(shown["body"]["content"]["text"], "a summary at 12");
    assert_eq!(result(&record, "keep")["test"], "not written");
    assert!(
        !format!("{:?}", record.steps).contains("SECRET"),
        "no secret anywhere in the preview"
    );
}
