// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The agent step: a model choosing tools over several turns, with stand-ins for the model (scripted answers, in the
//! standard shape and the `messages` one, and a model server on this machine reached over HTTP), for its tools'
//! services, and for its memory - then its memory kept on a broker in memory. No real model is asked.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_expr::Value;
use openduct_flow::{
    Credential, DryRun, Effects, Event, HttpRequest, Live, LogMemory, Memory, Outcome, RunRecord,
    ScriptRequest, Secrets, Workflow, run,
};
use serde_json::{Value as Json, json};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Effects with a scripted model: each request to it answered by the next answer given, in order.
#[derive(Default)]
struct Scripted {
    messages_api: bool,
    answers: Mutex<VecDeque<Json>>,
    /// Every request the model was sent, its body.
    asked: Mutex<Vec<Json>>,
    /// Every other request: its method, address and body.
    called: Mutex<Vec<(String, String, Json)>>,
    memory: Mutex<HashMap<String, Value>>,
    /// How long a tool's HTTP call takes.
    slow: Option<Duration>,
}

impl Scripted {
    fn new(answers: Vec<Json>) -> Self {
        Self {
            answers: Mutex::new(answers.into()),
            ..Self::default()
        }
    }

    fn asked(&self) -> Vec<Json> {
        self.asked.lock().unwrap().clone()
    }
}

impl Effects for Scripted {
    fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let body: Json = r
                .body
                .as_ref()
                .and_then(|(_, b)| serde_json::from_slice(b).ok())
                .unwrap_or(Json::Null);
            if r.url.ends_with("/chat/completions") || r.url.ends_with("/messages") {
                self.asked.lock().unwrap().push(body);
                let next = self.answers.lock().unwrap().pop_front();
                let (status, body) = match next {
                    Some(a) => (200, a),
                    None => (
                        500,
                        json!({ "error": { "message": "the script has no more answers" } }),
                    ),
                };
                return Ok(Value::from_json(
                    &json!({ "status": status, "headers": {}, "body": body }),
                ));
            }
            self.called
                .lock()
                .unwrap()
                .push((r.method.clone(), r.url.clone(), body));
            if let Some(slow) = self.slow {
                tokio::time::sleep(slow).await;
            }
            Ok(Value::from_json(&json!({
                "status": 200, "headers": {}, "body": { "status": "shipped", "asked": r.url },
            })))
        })
    }

    fn write<'a>(&'a self, topic: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async move { Ok(Value::from_json(&json!({ "topic": topic }))) })
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        if name != "model" {
            return Ok(Credential {
                kind: "test".to_owned(),
                fields: BTreeMap::new(),
            });
        }
        let mut fields: BTreeMap<String, String> = [
            ("base_url".to_owned(), "https://models.example.com/v1".to_owned()),
            ("api_key".to_owned(), "sk-secret-key".to_owned()),
            ("model".to_owned(), "small-1".to_owned()),
        ]
        .into();
        if self.messages_api {
            fields.insert("api".to_owned(), "messages".to_owned());
        }
        Ok(Credential {
            kind: "ai".to_owned(),
            fields,
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path}")) })
    }

    /// A stand-in for the sandbox: the sum of the arguments `a` and `b` the script is given.
    fn script<'a>(&'a self, r: ScriptRequest) -> Pending<'a> {
        Box::pin(async move {
            let args = r.names.get("args").cloned().unwrap_or(Json::Null);
            let sum = args["a"].as_f64().unwrap_or(0.0) + args["b"].as_f64().unwrap_or(0.0);
            Ok(Value::from_json(
                &json!({ "result": { "sum": sum, "language": r.language }, "logs": ["summed"] }),
            ))
        })
    }

    fn workflow<'a>(&'a self, id: &'a str) -> Pending<'a> {
        Box::pin(async move {
            if id != "refunds" {
                return Err(format!("no workflow {id}"));
            }
            Ok(Value::from_json(&json!({
                "name": "Refunds",
                "steps": [{ "name": "refund", "set": { "refunded": "value.order", "amount": "value.amount * 1" } }],
            })))
        })
    }

    fn recall<'a>(&'a self, key: &'a str) -> Pending<'a> {
        Box::pin(async move {
            Ok(self
                .memory
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .unwrap_or(Value::Null))
        })
    }

    fn remember<'a>(&'a self, key: &'a str, turns: Option<Value>, _: Duration) -> Pending<'a> {
        Box::pin(async move {
            let mut memory = self.memory.lock().unwrap();
            match turns {
                Some(t) => memory.insert(key.to_owned(), t),
                None => memory.remove(key),
            };
            Ok(Value::from_json(&json!({ "kept": true })))
        })
    }
}

const USAGE: (u64, u64) = (40, 10);

/// The model calls tools, in the standard shape.
fn calls(list: &[(&str, &str, Json)]) -> Json {
    let calls: Vec<Json> = list
        .iter()
        .map(|(id, name, args)| json!({ "id": id, "type": "function", "function": { "name": name, "arguments": args.to_string() } }))
        .collect();
    json!({
        "model": "small-1",
        "choices": [{ "finish_reason": "tool_calls", "message": { "role": "assistant", "content": null, "tool_calls": calls } }],
        "usage": { "prompt_tokens": USAGE.0, "completion_tokens": USAGE.1 },
    })
}

/// The model answers, in the standard shape.
fn says(text: &str) -> Json {
    json!({
        "model": "small-1",
        "choices": [{ "finish_reason": "stop", "message": { "role": "assistant", "content": text } }],
        "usage": { "prompt_tokens": USAGE.0, "completion_tokens": USAGE.1 },
    })
}

fn go(yaml: &str, input: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(yaml).unwrap();
    let event = Event {
        value: Value::from_json(&input),
        details: Value::from_json(&json!({ "trigger": "test" })),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&workflow, &event, effects))
}

fn result(record: &RunRecord, step: &str) -> Json {
    let found = record.steps.iter().find(|s| s.name == step).unwrap();
    serde_json::from_str(&found.result.to_json()).unwrap()
}

const SHOP: &str = r#"
name: Shop helper
steps:
  - name: helper
    agent:
      credential: model
      instructions: You help the shop's customers. Use the tools to look things up.
      input: "{{ value.text }}"
      tools:
        - name: order_status
          description: The status of an order, by its id.
          parameters:
            type: object
            properties: { id: { type: string, description: "The order's id, as A-1" } }
            required: [id]
          http:
            url: "https://shop.example.com/orders/{{ args.id }}"
        - name: add
          description: Adds two numbers.
          parameters:
            type: object
            properties: { a: { type: number }, b: { type: number } }
            required: [a, b]
          javascript: "return { sum: args.a + args.b }"
        - name: refund
          description: Refunds an order.
          parameters:
            type: object
            properties: { order: { type: string }, amount: { type: number } }
            required: [order, amount]
          run_workflow: { workflow: refunds }
  - name: after
    set: { said: steps.helper.text }
"#;

#[test]
fn an_agent_calls_its_tools_over_turns_and_its_record_holds_each_turn_and_call() {
    let effects = Scripted::new(vec![
        calls(&[("c1", "order_status", json!({ "id": "A-1" }))]),
        calls(&[
            ("c2", "add", json!({ "a": 2, "b": 3 })),
            ("c3", "refund", json!({ "order": "A-1", "amount": 5 })),
        ]),
        says("Order A-1 has shipped; I refunded 5."),
    ]);
    let record = go(SHOP, json!({ "text": "Where is A-1? Refund 2 + 3." }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "helper");
    assert_eq!(r["text"], "Order A-1 has shipped; I refunded 5.");
    assert_eq!(
        result(&record, "after")["said"],
        r["text"],
        "the steps after it read its answer"
    );
    assert_eq!(r["turns"].as_array().unwrap().len(), 3);
    assert_eq!(r["turns"][1]["tool_calls"], json!(["add", "refund"]));
    assert_eq!(r["usage"]["total_tokens"], json!(3 * (USAGE.0 + USAGE.1)));
    assert_eq!(r["metrics"]["tool_calls"], 3);
    let calls = r["calls"].as_array().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        (calls[0]["tool"].as_str(), calls[0]["arguments"]["id"].as_str()),
        (Some("order_status"), Some("A-1"))
    );
    assert_eq!(calls[0]["result"]["body"]["status"], "shipped");
    assert_eq!(
        calls[1]["result"]["sum"], 5.0,
        "the script ran with the model's arguments as args"
    );
    assert_eq!(
        calls[2]["result"]["result"]["refunded"], "A-1",
        "the other workflow ran on the arguments"
    );
    assert!(calls.iter().all(|c| c["ms"].is_u64() && c.get("error").is_none()));
    // The tool's HTTP request went out as the HTTP step's does.
    let called = effects.called.lock().unwrap().clone();
    assert_eq!(
        (called[0].0.as_str(), called[0].1.as_str()),
        ("GET", "https://shop.example.com/orders/A-1")
    );
    // What the model was sent: the tools each time, and each call's result after it.
    let asked = effects.asked();
    assert_eq!(asked.len(), 3);
    assert_eq!(asked[0]["model"], "small-1", "the credential's model");
    let offered: Vec<&str> = asked[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["function"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(offered, ["order_status", "add", "refund"]);
    assert_eq!(asked[0]["messages"][0]["role"], "system");
    let second = asked[1]["messages"].as_array().unwrap();
    assert_eq!(second[2]["tool_calls"][0]["id"], "c1");
    assert_eq!(second[3]["role"], "tool");
    assert_eq!(second[3]["tool_call_id"], "c1");
    assert!(second[3]["content"].as_str().unwrap().contains("shipped"));
    assert_eq!(
        asked[2]["messages"].as_array().unwrap().len(),
        7,
        "system, user, call, result, calls, 2 results"
    );
    assert!(
        !format!("{:?}", record.steps).contains("sk-secret-key"),
        "the key is in no record"
    );
}

#[test]
fn the_messages_shape_carries_the_same_conversation() {
    let effects = Scripted {
        messages_api: true,
        ..Scripted::new(vec![
            json!({
                "content": [{ "type": "text", "text": "Let me look." }, { "type": "tool_use", "id": "tu1", "name": "order_status", "input": { "id": "A-9" } }],
                "stop_reason": "tool_use", "usage": { "input_tokens": 30, "output_tokens": 8 },
            }),
            json!({
                "content": [{ "type": "text", "text": "A-9 has shipped." }],
                "stop_reason": "end_turn", "usage": { "input_tokens": 50, "output_tokens": 6 },
            }),
        ])
    };
    let record = go(SHOP, json!({ "text": "Where is A-9?" }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "helper");
    assert_eq!(r["text"], "A-9 has shipped.");
    assert_eq!(r["usage"]["total_tokens"], 94);
    assert_eq!(r["metrics"]["api"], "messages");
    let asked = effects.asked();
    assert!(asked[0]["system"].as_str().unwrap().starts_with("You help"));
    assert_eq!(asked[0]["tools"][0]["input_schema"]["required"][0], "id");
    let turns = asked[1]["messages"].as_array().unwrap();
    assert_eq!(turns[1]["content"][1]["type"], "tool_use");
    assert_eq!(turns[2]["content"][0]["type"], "tool_result");
    assert_eq!(turns[2]["content"][0]["tool_use_id"], "tu1");
}

#[test]
fn only_allowed_tools_are_offered_and_a_call_to_another_is_refused() {
    let workflow = SHOP.replace("      tools:\n", "      allow: [order_status]\n      tools:\n");
    let effects = Scripted::new(vec![
        calls(&[("c1", "refund", json!({ "order": "A-1", "amount": 5 }))]),
        says("I may not refund."),
    ]);
    let record = go(&workflow, json!({ "text": "Refund A-1" }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "helper");
    let asked = effects.asked();
    assert_eq!(asked[0]["tools"].as_array().unwrap().len(), 1);
    assert!(
        r["calls"][0]["error"]
            .as_str()
            .unwrap()
            .contains("no tool \"refund\" you may use"),
        "{}",
        r["calls"][0]
    );
    assert_eq!(r["metrics"]["tool_calls"], 0, "nothing ran");
    let back = &asked[1]["messages"].as_array().unwrap()[3];
    assert!(
        back["content"]
            .as_str()
            .unwrap()
            .contains("the tools are order_status")
    );
}

#[test]
fn arguments_that_do_not_match_a_tool_s_schema_go_back_to_the_model_unrun() {
    let effects = Scripted::new(vec![
        calls(&[
            ("c1", "order_status", json!({ "order": "A-1" })),
            ("c2", "add", json!("not a record")),
        ]),
        says("Sorry."),
    ]);
    let record = go(SHOP, json!({ "text": "Where?" }), &effects);
    let r = result(&record, "helper");
    assert_eq!(
        r["calls"][0]["error"],
        "the arguments do not match the tool's schema: id: missing"
    );
    assert!(r["calls"][1]["error"].as_str().unwrap().contains("a JSON record"));
    assert!(effects.called.lock().unwrap().is_empty(), "no request was made");
}

#[test]
fn the_limits_end_an_agent_that_goes_on() {
    let looping = |n: usize| {
        (0..n)
            .map(|i| calls(&[(&format!("c{i}"), "add", json!({ "a": 1, "b": 1 }))]))
            .collect::<Vec<_>>()
    };
    // Turns.
    let workflow = SHOP.replace("      tools:\n", "      max_turns: 2\n      tools:\n");
    let effects = Scripted::new(looping(5));
    let record = go(&workflow, json!({ "text": "go" }), &effects);
    match &record.outcome {
        Outcome::Failed { at, error } => {
            assert_eq!(at, "helper");
            assert_eq!(error, "the agent did not finish in 2 turns (max_turns)");
        }
        other => panic!("{other:?}"),
    }
    // Tool calls: once they are used up, the model is told to answer, and a call past them is refused.
    let workflow = SHOP.replace("      tools:\n", "      max_tool_calls: 1\n      tools:\n");
    let mut answers = looping(2);
    answers.push(says("done"));
    let effects = Scripted::new(answers);
    let record = go(&workflow, json!({ "text": "go" }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let asked = effects.asked();
    assert!(asked[0].get("tool_choice").is_none());
    assert_eq!(asked[1]["tool_choice"], "none");
    let r = result(&record, "helper");
    assert!(
        r["calls"][1]["error"]
            .as_str()
            .unwrap()
            .starts_with("no more tool calls: the most is 1")
    );
    // Calls in one turn: past 32, they are let go, and the turn says how many.
    let many: Vec<(String, &str, Json)> = (0..40)
        .map(|i| (format!("m{i}"), "add", json!({ "a": i, "b": 1 })))
        .collect();
    let many: Vec<(&str, &str, Json)> = many.iter().map(|(i, n, a)| (i.as_str(), *n, a.clone())).collect();
    let workflow = SHOP.replace("      tools:\n", "      max_tool_calls: 100\n      tools:\n");
    let effects = Scripted::new(vec![calls(&many), says("done")]);
    let record = go(&workflow, json!({ "text": "go" }), &effects);
    let r = result(&record, "helper");
    assert_eq!(
        (
            r["calls"].as_array().unwrap().len(),
            r["turns"][0]["calls_let_go"].as_u64()
        ),
        (32, Some(8))
    );
    assert_eq!(
        effects.asked()[1]["messages"].as_array().unwrap().len(),
        3 + 32,
        "only what ran is in the conversation"
    );
    // Tokens.
    let workflow = SHOP.replace("      tools:\n", "      max_total_tokens: 60\n      tools:\n");
    let record = go(&workflow, json!({ "text": "go" }), &Scripted::new(looping(3)));
    match &record.outcome {
        Outcome::Failed { error, .. } => assert_eq!(
            error,
            "the agent used 100 tokens, its most is 60 (max_total_tokens)"
        ),
        other => panic!("{other:?}"),
    }
    // Time: a call cut off when it runs out, and the step ended.
    let workflow = SHOP.replace("      tools:\n", "      timeout: 1s\n      tools:\n");
    let effects = Scripted {
        slow: Some(Duration::from_secs(3)),
        ..Scripted::new(vec![
            calls(&[("c1", "order_status", json!({ "id": "A-1" }))]),
            says("late"),
        ])
    };
    let record = go(&workflow, json!({ "text": "go" }), &effects);
    match &record.outcome {
        Outcome::Failed { error, .. } => assert_eq!(error, "the agent took longer than its 1s (timeout)"),
        other => panic!("{other:?}"),
    }
    let failed = record.steps.iter().find(|s| s.name == "helper").unwrap();
    assert!(
        failed.ms < 2_500,
        "it was cut off at its time, not the call's: {} ms",
        failed.ms
    );
}

const SORTER: &str = r#"
name: Sorter
steps:
  - name: sort
    agent:
      credential: model
      input: "Sort this message: {{ value.text }}"
      output:
        schema:
          type: object
          properties:
            team: { enum: [billing, shipping, other] }
            urgent: { type: boolean }
          required: [team, urgent]
"#;

#[test]
fn an_answer_asked_as_json_is_checked_and_put_right_once() {
    let effects = Scripted::new(vec![
        says("{\"team\": \"sales\"}"),
        says("```json\n{\"team\": \"billing\", \"urgent\": true}\n```"),
    ]);
    let record = go(SORTER, json!({ "text": "I was charged twice!" }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "sort");
    assert_eq!(r["json"], json!({ "team": "billing", "urgent": true }));
    let asked = effects.asked();
    assert!(
        asked[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("matching this JSON schema")
    );
    assert_eq!(asked[0]["response_format"]["type"], "json_object");
    let again = asked[1]["messages"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(again["role"], "user");
    assert!(
        again["content"].as_str().unwrap().contains("team: one of"),
        "{again}"
    );
    // A second answer that is wrong too fails the step, saying why.
    let effects = Scripted::new(vec![says("billing"), says("still not JSON")]);
    let record = go(SORTER, json!({ "text": "x" }), &effects);
    match &record.outcome {
        Outcome::Failed { error, .. } => {
            assert!(
                error.starts_with("the agent's answer is not the JSON asked for: it is not JSON"),
                "{error}"
            );
        }
        other => panic!("{other:?}"),
    }
}

const CHAT: &str = r#"
name: Chat
steps:
  - name: chat
    agent:
      credential: model
      input: "{{ value.text }}"
      memory:
        key: "{{ value.chat }}"
        window: 2
        clear: "value.text == '/reset'"
"#;

#[test]
fn memory_keeps_the_last_turns_of_each_conversation_and_forgets_when_told() {
    let effects = Scripted::new((1..=6).map(|n| says(&format!("answer {n}"))).collect());
    let say = |chat: u32, text: &str| {
        let record = go(CHAT, json!({ "chat": chat, "text": text }), &effects);
        assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
        result(&record, "chat")
    };
    say(7, "one");
    say(7, "two");
    let third = say(7, "three");
    assert_eq!(third["memory"]["recalled"], 4, "two turns, the window");
    let asked = effects.asked();
    let sent: Vec<&str> = asked[2]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["content"].as_str().unwrap())
        .collect();
    assert_eq!(sent, ["one", "answer 1", "two", "answer 2", "three"]);
    let other = say(8, "hello");
    assert_eq!(
        other["memory"]["recalled"], 0,
        "another chat's conversation is its own"
    );
    let reset = say(7, "/reset");
    assert_eq!(
        (
            reset["memory"]["cleared"].as_bool(),
            reset["memory"]["recalled"].as_u64()
        ),
        (Some(true), Some(0))
    );
    assert_eq!(effects.asked()[4]["messages"].as_array().unwrap().len(), 1);
    let keys: Vec<String> = effects.memory.lock().unwrap().keys().cloned().collect();
    assert!(
        keys.contains(&"chat/chat/7".to_owned()),
        "kept under the workflow and step: {keys:?}"
    );
    // The transcript says which messages came from memory.
    let after = say(7, "again");
    assert_eq!(after["transcript"][0]["remembered"], true);
}

#[test]
fn a_test_run_shows_the_request_it_would_send_and_answers_as_a_real_run_would() {
    let record = go(SHOP, json!({ "text": "Where is A-1?" }), &DryRun::default());
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "helper");
    assert_eq!(r["test"], "not sent");
    assert_eq!(r["text"], "(the agent's answer, in a real run)");
    assert_eq!(r["tools"], json!(["order_status", "add", "refund"]));
    assert_eq!(
        r["request"]["body"]["content"]["tools"].as_array().unwrap().len(),
        3
    );
    let record = go(SORTER, json!({ "text": "x" }), &DryRun::default());
    assert_eq!(result(&record, "sort")["json"], json!({}));
}

#[test]
fn an_agent_uses_another_agent_and_an_app_s_action_as_tools() {
    let workflow = r#"
name: Team
steps:
  - name: lead
    agent:
      credential: model
      input: "{{ value.text }}"
      tools:
        - name: researcher
          description: Finds things out.
          agent:
            credential: model
            instructions: You research.
        - name: log_row
          description: Adds a row to the log sheet.
          google_sheets: { action: add_row, credential: sheets, spreadsheet_id: sheet-1, sheet: Log }
"#;
    let effects = Scripted::new(vec![
        calls(&[("c1", "researcher", json!({ "input": "What is 6 x 7?" }))]),
        says("42"),
        calls(&[("c2", "log_row", json!({ "values": ["6x7", 42] }))]),
        says("It is 42, and it is logged."),
    ]);
    let record = go(workflow, json!({ "text": "Work out 6 x 7 and log it" }), &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let r = result(&record, "lead");
    assert_eq!(r["calls"][0]["result"]["text"], "42", "the other agent's answer");
    let asked = effects.asked();
    assert_eq!(asked[1]["messages"][0]["content"], "You research.");
    assert_eq!(asked[1]["messages"][1]["content"], "What is 6 x 7?");
    let called = effects.called.lock().unwrap().clone();
    let (method, url, body) = &called[0];
    assert_eq!(method, "POST");
    assert!(url.contains("sheet-1") && url.contains("Log"), "{url}");
    assert_eq!(
        body["values"][0],
        json!(["6x7", 42]),
        "the model's values, as they are"
    );
}

#[derive(Debug)]
struct Kept(Credential);

impl Secrets for Kept {
    fn get(&self, _: &str) -> Option<Credential> {
        Some(self.0.clone())
    }
}

/// A model on this machine, over HTTP: each request answered with the next answer, the requests' bodies kept.
async fn model_server(answers: Vec<Json>) -> (String, Arc<Mutex<Vec<Json>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}/v1", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&seen);
    tokio::spawn(async move {
        let mut answers: VecDeque<Json> = answers.into();
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut got = Vec::new();
            let mut buf = [0u8; 4096];
            let body = loop {
                let n = socket.read(&mut buf).await.unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got).into_owned();
                if let Some(end) = text.find("\r\n\r\n") {
                    let length: usize = text[..end]
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if got.len() >= end + 4 + length {
                        break got[end + 4..end + 4 + length].to_vec();
                    }
                }
                if n == 0 {
                    break Vec::new();
                }
            };
            kept.lock()
                .unwrap()
                .push(serde_json::from_slice::<Json>(&body).unwrap_or(Json::Null));
            let answer = answers
                .pop_front()
                .unwrap_or(json!({ "error": { "message": "no more" } }))
                .to_string();
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                answer.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
        }
    });
    (address, seen)
}

#[test]
fn a_model_reached_over_http_drives_the_agent_and_a_tool_may_not_reach_this_machine() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (address, seen) = model_server(vec![
            calls(&[("c1", "order_status", json!({ "id": "A-1" }))]),
            says("I could not look it up."),
        ])
        .await;
        let credential = Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), address),
                ("model".to_owned(), "small-1".to_owned()),
            ]
            .into(),
        };
        // The model is on this machine, so this install lets workflows reach it; the tool asks for this machine too.
        let workflow = SHOP.replace(
            "https://shop.example.com/orders/",
            "http://169.254.169.254/latest/",
        );
        let live = Live::new("127.0.0.1:1", true)
            .unwrap()
            .with_secrets(Arc::new(Kept(credential.clone())));
        let w = Workflow::parse(&workflow).unwrap();
        let event = Event {
            value: Value::from_json(&json!({ "text": "Where is A-1?" })),
            details: Value::Null,
        };
        let record = run(&w, &event, &live).await;
        assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
        let r = result(&record, "helper");
        assert_eq!(r["text"], "I could not look it up.");
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(seen.lock().unwrap()[1]["messages"][3]["role"], "tool");
        // Without that allowance, nothing on this machine's network is reached: not the model, not a tool.
        let guarded = Live::new("127.0.0.1:1", false)
            .unwrap()
            .with_secrets(Arc::new(Kept(credential)));
        let record = run(&w, &event, &guarded).await;
        match &record.outcome {
            Outcome::Failed { error, .. } => assert!(error.contains("this machine's own network"), "{error}"),
            other => panic!("{other:?}"),
        }
    });
}

#[test]
fn memory_on_the_broker_is_the_same_on_every_node_and_forgets_on_time() {
    use openduct_broker::{Broker, BrokerConfig};
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let broker = Broker::start(
            BrokerConfig::new("127.0.0.1:0".parse().unwrap()),
            Arc::new(openduct_storage::MemoryStore::new()),
        )
        .await
        .unwrap();
        let address = broker.local_addr().to_string();
        let (one, two) = (LogMemory::new(&address), LogMemory::new(&address));
        let day = Duration::from_secs(86_400);
        let turns = |n: u32| Value::from_json(&json!([{ "role": "user", "content": format!("hi {n}") }, { "role": "assistant", "content": "hello" }]));
        assert!(one.recall("w.a.chat/chat/7").await.unwrap().is_null(), "nothing yet");
        one.remember("w.a.chat/chat/7", Some(turns(1)), day).await.unwrap();
        one.remember("w.a.chat/chat/8", Some(turns(8)), day).await.unwrap();
        one.remember("w.a.chat/chat/7", Some(turns(2)), day).await.unwrap();
        let read = |v: Value| serde_json::from_str::<Json>(&v.to_json()).unwrap();
        assert_eq!(read(two.recall("w.a.chat/chat/7").await.unwrap())[0]["content"], "hi 2", "the newest, on another node");
        assert_eq!(read(two.recall("w.a.chat/chat/8").await.unwrap())[0]["content"], "hi 8");
        two.remember("w.a.chat/chat/8", None, day).await.unwrap();
        assert!(one.recall("w.a.chat/chat/8").await.unwrap().is_null(), "forgotten everywhere");
        one.remember("w.a.chat/chat/9", Some(turns(9)), Duration::from_millis(1)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(two.recall("w.a.chat/chat/9").await.unwrap().is_null(), "past its time");
        // A node started later reads the same.
        let three = LogMemory::new(&address);
        assert_eq!(read(three.recall("w.a.chat/chat/7").await.unwrap())[1]["content"], "hello");
        // Through a workspace's effects, each workspace's keys are its own.
        let memory: Arc<dyn Memory> = Arc::new(LogMemory::new(&address));
        let a = Live::new(&address, false).unwrap().in_workspace("a").with_memory(Arc::clone(&memory));
        let b = Live::new(&address, false).unwrap().in_workspace("b").with_memory(memory);
        assert_eq!(read(a.recall("chat/chat/7").await.unwrap())[0]["content"], "hi 2");
        assert!(b.recall("chat/chat/7").await.unwrap().is_null(), "another workspace sees nothing of it");
        // At its most conversations, a new one lets go of the one nearest to being forgotten.
        let small = LogMemory::in_topic(&address, "few-memories").keeping_at_most(2);
        small.remember("x", Some(turns(1)), day).await.unwrap();
        small.remember("y", Some(turns(2)), day * 2).await.unwrap();
        small.remember("z", Some(turns(3)), day * 3).await.unwrap();
        assert!(small.recall("x").await.unwrap().is_null(), "the nearest to being forgotten went");
        assert!(!small.recall("y").await.unwrap().is_null());
        assert!(!small.recall("z").await.unwrap().is_null());
        broker.shutdown().await;
    });
}

#[test]
fn the_template_with_an_agent_reads_and_runs_on_its_sample_with_a_scripted_model() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../templates/ai-agent-order-help.yaml"
    );
    let text = std::fs::read_to_string(path).unwrap();
    let json = openduct_flow::yaml_to_json(&text).unwrap();
    let sample = json["sample"].clone();
    let record = go(&text, sample.clone(), &DryRun::default());
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        result(&record, "assistant")["tools"],
        json!(["order_status", "arrival_day"])
    );
    // With a model: it looks the order up, works out the day, and the answer goes back to the chat.
    let effects = Scripted::new(vec![
        calls(&[("c1", "order_status", json!({ "id": "A-1042" }))]),
        calls(&[("c2", "arrival_day", json!({ "sent": "2026-11-20", "days": 3 }))]),
        says("A-1042 has shipped and arrives on 2026-11-23."),
    ]);
    let record = go(&text, sample, &effects);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        result(&record, "reply")["body"]["answer"],
        "A-1042 has shipped and arrives on 2026-11-23."
    );
    let r = result(&record, "assistant");
    assert_eq!(r["calls"][0]["result"]["body"]["status"], "shipped");
    assert_eq!(r["memory"]["kept"], 2);
}
