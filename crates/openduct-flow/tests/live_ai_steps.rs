// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The AI steps built on the `ai` step against a real model, when one is given - with the settings live_ai.rs reads
//! (OPENDUCT_TEST_AI_BASE_URL, OPENDUCT_TEST_AI_MODEL, OPENDUCT_TEST_AI_KEY_FILE, and as the service needs
//! OPENDUCT_TEST_AI_API, OPENDUCT_TEST_AI_KEY_HEADER, OPENDUCT_TEST_AI_HEADERS). Without them it says it was skipped
//! and passes. The key is read here and kept nowhere.

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
    let kept = Kept(
        [(
            "model".to_owned(),
            Credential {
                kind: "ai".to_owned(),
                fields,
            },
        )]
        .into(),
    );
    Some((
        Live::new("127.0.0.1:1", false)
            .unwrap()
            .with_secrets(Arc::new(kept)),
        model,
        key,
    ))
}

#[test]
fn a_real_model_classifies_reads_sentiment_extracts_and_summarises() {
    let Some((effects, model, key)) = live() else {
        return;
    };
    let workflow = format!(
        r#"
name: live
steps:
  - name: sort
    classify:
      credential: model
      model: {model}
      text: "{{{{ value.text }}}}"
      categories:
        - {{ name: refund, description: Asks for money back }}
        - {{ name: bug, description: Something does not work }}
  - name: mood
    sentiment: {{ credential: model, model: {model}, text: "{{{{ value.text }}}}" }}
  - name: order
    extract:
      credential: model
      model: {model}
      text: "{{{{ value.text }}}}"
      fields:
        - {{ name: order_id, required: true }}
        - {{ name: amount, type: number }}
  - name: sum
    summarise: {{ credential: model, model: {model}, text: "{{{{ value.text }}}}", length: short }}
"#
    );
    let workflow = Workflow::parse(&workflow).unwrap();
    let event = Event {
        value: Value::parse_json(
            r#"{"text": "I was charged twice for order A-1042 (49.90 each time). This is the second time - please refund one, I am really fed up."}"#,
        )
        .unwrap(),
        details: Value::Null,
    };
    let record: RunRecord = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&workflow, &event, &effects));
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let result = |step: &str| -> Json {
        let found = record.steps.iter().find(|s| s.name == step).unwrap();
        serde_json::from_str(&found.result.to_json()).unwrap()
    };
    assert_eq!(result("sort")["category"], "refund");
    assert_eq!(result("mood")["label"], "negative");
    assert_eq!(result("order")["json"]["order_id"], "A-1042");
    assert!(result("sum")["summary"].as_str().unwrap().len() > 10);
    assert!(
        !format!("{:?}", record.steps).contains(&key),
        "the key is in no step's record"
    );
}
