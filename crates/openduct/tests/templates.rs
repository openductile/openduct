// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Every template the studio offers works as it comes: it carries a sample event for its trigger, every field it names
//! of that event is in it, and a test run on it - `openduct workflow test`, with lookup files at hand - goes through
//! without a step failing. Credentials the workspace does not have yet stand in, as they do in the studio.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
#![cfg(feature = "studio")]

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const OPENDUCT: &str = env!("CARGO_BIN_EXE_openduct");

fn templates() -> Vec<PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../templates");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
        .collect();
    found.sort();
    found
}

/// Every text a template's steps hold.
fn texts(value: &Value, into: &mut Vec<String>) {
    match value {
        Value::String(s) => into.push(s.clone()),
        Value::Array(list) => list.iter().for_each(|v| texts(v, into)),
        Value::Object(map) => map.values().for_each(|v| texts(v, into)),
        _ => {}
    }
}

/// The paths of the event a text names - `value.data.object.amount`, `value.lines[0]` - leaving out a method called on
/// one (`value.email.trim()`) and one only asked about (`has(value.x)`).
fn named(text: &str) -> Vec<Vec<String>> {
    let bytes = text.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(found) = text[at..].find("value.") {
        let start = at + found;
        at = start + 6;
        if start > 0 && (ident(bytes[start - 1]) || bytes[start - 1] == b'.') {
            continue;
        }
        if text[..start].trim_end().ends_with("has(") {
            continue;
        }
        let mut path = Vec::new();
        let mut i = start + 5;
        loop {
            if i < bytes.len() && bytes[i] == b'.' {
                let begin = i + 1;
                let mut end = begin;
                while end < bytes.len() && ident(bytes[end]) {
                    end += 1;
                }
                if end == begin {
                    break;
                }
                if end < bytes.len() && bytes[end] == b'(' {
                    break;
                }
                path.push(text[begin..end].to_owned());
                i = end;
            } else if i < bytes.len() && bytes[i] == b'[' {
                let Some(close) = text[i..].find(']') else { break };
                let inner = &text[i + 1..i + close];
                if inner.chars().all(|c| c.is_ascii_digit()) && !inner.is_empty() {
                    path.push(format!("[{inner}]"));
                    i += close + 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if !path.is_empty() {
            out.push(path);
        }
    }
    out
}

fn found(sample: &Value, path: &[String]) -> bool {
    let mut here = sample;
    for key in path {
        let next = match key.strip_prefix('[').and_then(|k| k.strip_suffix(']')) {
            Some(n) => here.as_array().and_then(|a| a.get(n.parse::<usize>().unwrap())),
            None => here.as_object().and_then(|o| o.get(key)),
        };
        match next {
            Some(v) => here = v,
            None => return false,
        }
    }
    true
}

#[test]
fn every_template_runs_on_its_own_sample_and_names_only_fields_it_has() {
    let files = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/template_files");
    let mut problems = Vec::new();
    let mut ran = 0;
    for template in templates() {
        let name = template.file_name().unwrap().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&template).unwrap();
        let json = openduct_flow::yaml_to_json(&text).unwrap();
        let Some(sample) = json.get("sample") else {
            problems.push(format!(
                "{name}: no sample: a test of it would run on an event it does not expect"
            ));
            continue;
        };
        let mut said = Vec::new();
        texts(json.get("steps").unwrap_or(&Value::Null), &mut said);
        for path in said.iter().flat_map(|t| named(t)) {
            if !found(sample, &path) {
                problems.push(format!(
                    "{name}: names value.{} - not in its sample",
                    path.join(".").replace(".[", "[")
                ));
            }
        }
        let out = Command::new(OPENDUCT)
            .args(["workflow", "test"])
            .arg(&template)
            .arg("--files-dir")
            .arg(&files)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let Some(run) = stdout
            .lines()
            .next()
            .and_then(|l| serde_json::from_str::<Value>(l).ok())
        else {
            problems.push(format!(
                "{name}: did not run: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
            continue;
        };
        if run["status"] == "failed" {
            problems.push(format!("{name}: failed at {}: {}", run["at"], run["error"]));
        }
        ran += 1;
    }
    assert!(
        problems.is_empty(),
        "{} problem(s):\n{}",
        problems.len(),
        problems.join("\n")
    );
    assert!(ran >= 28, "only {ran} templates ran");
}

#[test]
fn the_paths_a_text_names_are_read_as_written() {
    assert_eq!(
        named("{{ value.data.object.amount / 100.0 }}"),
        vec![vec!["data", "object", "amount"]]
    );
    assert_eq!(named("value.email.trim().lowerAscii()"), vec![vec!["email"]]);
    assert_eq!(
        named("has(value.x) && value.lines[0].sku"),
        vec![vec!["lines", "[0]", "sku"]]
    );
    assert!(named("steps.value.x and myvalue.y").is_empty());
}

/// One event a template is given, and the way its run must go: filtered out (`skipped`), refused (`failed`, saying
/// why), or through - writing where it says.
struct Case {
    template: &'static str,
    event: &'static str,
    status: &'static str,
    says: &'static str,
}

const CASES: &[Case] = &[
    Case {
        template: "big-orders-alert",
        event: r#"{"id":"A-1","customer":"Ada","lines":[{"sku":"CAP","qty":1,"price":24}]}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "slack-failed-payment",
        event: r#"{"type":"payment_intent.succeeded","data":{"object":{"amount":7600}}}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "telegram-ai-assistant",
        event: r#"{"update_id":2,"edited_message":{"chat":{"id":1},"text":"hi"}}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "whatsapp-order-confirmation",
        event: r#"{"id":"A-1","name":"Ada","total":5}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "email-order-shipped",
        event: r#"{"id":"A-1","name":"Ada","tracking":"1Z"}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "teams-service-down",
        event: r#"{"service":"checkout","status":"up","message":"back"}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "code-score-a-lead",
        event: r#"{"email":"sam@home.example","company":"Home","employees":1,"message":"hello"}"#,
        status: "skipped",
        says: "",
    },
    Case {
        template: "lookup-price-an-item",
        event: r#"{"sku":"NOPE","qty":1}"#,
        status: "failed",
        says: "no price for sku NOPE",
    },
    Case {
        template: "lookup-convert-currency",
        event: r#"{"amount":5,"currency":"XYZ"}"#,
        status: "failed",
        says: "no rate for XYZ",
    },
    Case {
        template: "topic-route-events",
        event: r#"{"type":"refund","id":"R-1"}"#,
        status: "done",
        says: "other-events",
    },
    Case {
        template: "topic-route-events",
        event: r#"{"type":"order","id":"A-1"}"#,
        status: "done",
        says: "\"orders\"",
    },
    // MCP: a tool's call for an item not in the price list answers with why.
    Case {
        template: "mcp-price-list-tool",
        event: r#"{"sku":"NOPE","qty":1}"#,
        status: "failed",
        says: "no price for sku NOPE",
    },
];

#[test]
fn each_template_filters_refuses_and_routes_as_its_events_say() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = dir.join("tests/template_files");
    let scratch = std::env::temp_dir().join(format!("openduct-template-cases-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).unwrap();
    let mut wrong = Vec::new();
    for (n, case) in CASES.iter().enumerate() {
        let input = scratch.join(format!("{n}.json"));
        std::fs::write(&input, case.event).unwrap();
        let out = Command::new(OPENDUCT)
            .args(["workflow", "test"])
            .arg(dir.join(format!("../../templates/{}.yaml", case.template)))
            .arg("--input")
            .arg(&input)
            .arg("--files-dir")
            .arg(&files)
            .output()
            .unwrap();
        let line = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned();
        let run: Value = serde_json::from_str(&line).unwrap_or(Value::Null);
        if run["status"] != case.status || !line.contains(case.says) {
            wrong.push(format!(
                "{} on {}: {} (wanted {} saying {:?})",
                case.template, case.event, line, case.status, case.says
            ));
        }
    }
    std::fs::remove_dir_all(&scratch).ok();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
