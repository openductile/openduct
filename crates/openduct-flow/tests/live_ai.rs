// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! AI steps against a real model, when one is given. Without these settings each test says it was skipped and passes:
//! - OPENDUCT_TEST_AI_BASE_URL: the model API's address;
//! - OPENDUCT_TEST_AI_MODEL: the model to ask;
//! - OPENDUCT_TEST_AI_KEY_FILE: a file holding the API key, bare or as `API_KEY=value`;
//! - and, as the service needs, OPENDUCT_TEST_AI_API (`chat_completions`, the default, or `messages`),
//!   OPENDUCT_TEST_AI_KEY_HEADER and OPENDUCT_TEST_AI_HEADERS (a JSON record).
//!
//! The key is read here and kept nowhere: not printed, not in a run's results.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use openduct_expr::Value;
use openduct_flow::{Credential, Event, Live, Outcome, RunRecord, Secrets, Workflow, run};
use serde_json::Value as Json;

#[derive(Debug)]
struct Kept(BTreeMap<String, Credential>);

impl Secrets for Kept {
    fn get(&self, name: &str) -> Option<Credential> {
        self.0.get(name).cloned()
    }
}

/// The live effects with an `ai` credential called `model`, the model to ask and the key; `None` when not given.
fn live() -> Option<(Live, String, String)> {
    let setting = |name: &str| {
        std::env::var(format!("OPENDUCT_TEST_AI_{name}"))
            .ok()
            .filter(|v| !v.trim().is_empty())
    };
    let (Some(base_url), Some(model), Some(file)) =
        (setting("BASE_URL"), setting("MODEL"), setting("KEY_FILE"))
    else {
        eprintln!(
            "skipped: set OPENDUCT_TEST_AI_BASE_URL, OPENDUCT_TEST_AI_MODEL and OPENDUCT_TEST_AI_KEY_FILE"
        );
        return None;
    };
    let text = std::fs::read_to_string(file).unwrap();
    let line = text
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_default()
        .trim();
    let key = line
        .split_once('=')
        .map_or(line, |(_, v)| v)
        .trim()
        .trim_matches(['"', '\''])
        .to_owned();
    let mut fields: BTreeMap<String, String> = [
        ("base_url".to_owned(), base_url),
        ("api_key".to_owned(), key.clone()),
    ]
    .into();
    for (name, field) in [
        ("API", "api"),
        ("KEY_HEADER", "key_header"),
        ("HEADERS", "headers"),
    ] {
        if let Some(value) = setting(name) {
            fields.insert(field.to_owned(), value);
        }
    }
    let credential = Credential {
        kind: "ai".to_owned(),
        fields,
    };
    let kept = Kept([("model".to_owned(), credential)].into());
    Some((
        Live::new("127.0.0.1:1", false)
            .unwrap()
            .with_secrets(Arc::new(kept)),
        model,
        key,
    ))
}

fn go(workflow: &str, input: &str, effects: &Live) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap();
    let event = Event {
        value: Value::parse_json(input).unwrap(),
        details: Value::Null,
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

#[test]
fn a_summary_comes_back_in_the_standard_form_with_its_transcript_and_metrics() {
    let Some((effects, model, key)) = live() else {
        return;
    };
    let record = go(
        &format!(
            "name: summary\nsteps:\n  - name: sum\n    ai:\n      credential: model\n      model: {model}\n      system: You summarise in one short sentence.\n      prompt: 'Summarise: {{{{ value.text }}}}'\n      max_tokens: 200\n      temperature: 0\n"
        ),
        r#"{"text": "OpenDuct keeps every event in a durable log before any step runs, so a crash in the middle of a run repeats the run instead of losing it, and an event whose run failed is set aside with the reason until it is run again."}"#,
        &effects,
    );
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let sum = result(&record, "sum");
    assert!(sum["text"].as_str().unwrap().len() > 10, "{sum}");
    assert_eq!(sum["finish_reason"], "stop");
    assert!(sum["usage"]["prompt_tokens"].as_u64().unwrap() > 0);
    assert!(sum["usage"]["completion_tokens"].as_u64().unwrap() > 0);
    assert_eq!(
        sum["usage"]["total_tokens"].as_u64().unwrap(),
        sum["usage"]["prompt_tokens"].as_u64().unwrap() + sum["usage"]["completion_tokens"].as_u64().unwrap()
    );
    let roles: Vec<&str> = sum["transcript"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["system", "user", "assistant"]);
    assert!(sum["metrics"]["latency_ms"].as_u64().unwrap() > 0);
    assert!(
        !format!("{:?}", record.steps).contains(&key),
        "the key is kept nowhere in the run"
    );
    eprintln!(
        "summary: {}  ({} ms, {} tokens)",
        sum["text"], sum["metrics"]["latency_ms"], sum["usage"]["total_tokens"]
    );
}

#[test]
fn one_step_writes_the_system_prompt_and_the_next_does_the_work_with_it() {
    let Some((effects, model, _)) = live() else { return };
    let workflow = format!(
        r#"name: chain
steps:
  - name: writer
    ai:
      credential: model
      model: {model}
      system: You write system prompts for other AI assistants. Answer with the system prompt alone.
      prompt: "Write a system prompt for an assistant that does this task: {{{{ value.task }}}}"
      max_tokens: 300
  - name: worker
    ai:
      credential: model
      model: {model}
      system: "{{{{ steps.writer.text }}}}"
      prompt: "{{{{ value.input }}}}"
      max_tokens: 300
  - name: sorted
    ai:
      credential: model
      model: {model}
      messages:
        - role: user
          content: "Is this reply polite? Reply: {{{{ steps.worker.text }}}}"
      response_format: json
      system: 'Answer as JSON: {{"polite": true or false, "reason": "a few words"}}'
      max_tokens: 100
      temperature: 0
"#
    );
    let record = go(
        &workflow,
        r#"{"task": "Answer customers who ask where their order is, kindly, in two sentences.", "input": "Hi, where is my order A-1042? It has been a week."}"#,
        &effects,
    );
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let written = result(&record, "writer")["text"].as_str().unwrap().to_owned();
    let worker = result(&record, "worker");
    assert_eq!(worker["transcript"][0]["role"], "system");
    assert_eq!(
        worker["transcript"][0]["content"], written,
        "the second step ran on the system prompt the first wrote"
    );
    let sorted = result(&record, "sorted");
    assert!(sorted["json"]["polite"].is_boolean(), "{sorted}");
    eprintln!(
        "written prompt: {}\nreply: {}\nsorted: {}",
        written.chars().take(200).collect::<String>(),
        worker["text"],
        sorted["json"]
    );
}
