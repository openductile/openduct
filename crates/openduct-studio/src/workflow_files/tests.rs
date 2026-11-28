// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use std::collections::HashSet;

use serde_json::{Value, json};

use super::*;

#[test]
fn every_template_written_as_yaml_reads_back_the_same() {
    let templates = crate::templates::all();
    assert!(templates.len() > 10);
    for t in templates {
        let definition = &t["definition"];
        let text = write(definition, false);
        let back = read("t.yaml", &text).unwrap();
        assert_eq!(&back.definition, definition, "{}:\n{text}", t["id"]);
        assert_eq!(back.on, None);
        assert!(text.starts_with("name: "), "the name first: {text}");
    }
}

#[test]
fn awkward_texts_are_quoted_or_kept_in_blocks_and_read_back() {
    let definition = json!({
        "name": "Awkward: ones",
        "steps": [
            { "name": "true", "set": { "a": "yes", "b": "1e5", "c": "0x1F", "d": "2026-11-27", "e": "  padded ",
              "f": "#hash", "g": "- dash", "h": "{{ value.x }}", "i": "key: value", "j": "", "k": "null", "l": "~",
              "m": "inf", "n": "tab\there", "o": "line\r\nbreak", "p": "\u{7f}\u{85}\u{feff}", "q": "Ünïcode 注文",
              "r": "/rates.csv", "s": "it's fine", "t": "ends with newline\n", "u": "two\n\n", "v": "no\nnewline",
              "w": " leading space\nsecond", "x": [[1, 2], []], "y": {}, "z": [{}], "num": 1.5, "big": 12_345_678_901_i64,
              "neg": -3, "nothing": null, "on": false } },
            { "javascript": { "code": "const a = 1;\n\nif (a) {\n  return { a };\n}\n" } },
            ["nested", ["deeper"]],
        ],
        "on-call": "x",
        "with space": 1,
    });
    let text = write(&definition, true);
    let back = read("a.yaml", &text).unwrap();
    assert_eq!(back.definition, definition, "{text}");
    assert_eq!(back.on, Some(true));
    assert!(text.contains("code: |\n"), "a script as a literal block: {text}");
    assert!(text.contains("\non: true\n"));
}

#[test]
fn a_file_says_on_or_off_and_json_files_are_read_too() {
    let on = read("a.yaml", "name: A\non: false\nsteps: []\n").unwrap();
    assert_eq!(on.on, Some(false));
    assert_eq!(on.definition, json!({ "name": "A", "steps": [] }));
    let j = read("b.json", r#"{"name": "B", "on": true}"#).unwrap();
    assert_eq!((j.on, j.definition), (Some(true), json!({ "name": "B" })));
    assert!(
        read("c.yaml", "name: C\non: maybe\n")
            .unwrap_err()
            .contains("on: true or false")
    );
    assert!(read("d.json", "{").unwrap_err().starts_with("d.json"));
    let big = format!("name: x\ndescription: {}\n", "a".repeat(MAX_FILE));
    assert!(read("e.yaml", &big).unwrap_err().contains("larger"));
}

#[test]
fn files_are_named_after_their_workflows_and_never_twice() {
    let mut taken = HashSet::new();
    assert_eq!(file_name("Order alerts", &mut taken), "order-alerts.yaml");
    assert_eq!(file_name("Order  Alerts!", &mut taken), "order-alerts-2.yaml");
    assert_eq!(file_name("注文", &mut taken), "workflow.yaml");
    assert_eq!(file_name("", &mut taken), "workflow-2.yaml");
    assert!(is_workflow_file("a.yaml") && is_workflow_file("B.YML") && is_workflow_file("c.json"));
    assert!(!is_workflow_file(".hidden.yaml") && !is_workflow_file("notes.md") && !is_workflow_file(".yaml"));
}

#[test]
fn what_a_workflow_needs_is_found_wherever_it_is_named() {
    let definition = json!({
        "name": "N",
        "trigger": { "failed": { "workflow": "Billing" } },
        "steps": [
            { "http": { "credential": "shop", "url": "https://x.example" } },
            { "lookup": { "file": "/rates.csv", "where": "true" } },
            { "table": { "name": "leads", "action": "get" } },
            { "vector_store": { "credential": "db", "table": "pieces" } },
            { "if": { "when": "true", "then": [ { "run_workflow": { "workflow": "0190-id" } } ] } },
            { "agent": { "credential": "model", "tools": [ { "name": "t", "run_workflow": { "workflow": "Sub" } } ] } },
            { "email": { "credential": "{{ value.which }}" } },
        ],
    });
    let n = needs(&definition);
    let set = |v: &[&str]| {
        v.iter()
            .map(|s| (*s).to_owned())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(n.credentials, set(&["db", "model", "shop"]));
    assert_eq!(n.files, set(&["/rates.csv"]));
    assert_eq!(
        n.tables,
        set(&["leads"]),
        "a vector store's SQL table is not a data table"
    );
    assert_eq!(n.workflows, set(&["0190-id", "Billing", "Sub"]));
    let mut renamed = definition.clone();
    rename_workflows(&mut renamed, &|w| (w == "Sub").then(|| "id-of-sub".to_owned()));
    assert_eq!(
        renamed["steps"][5]["agent"]["tools"][0]["run_workflow"]["workflow"],
        "id-of-sub"
    );
    assert_eq!(
        renamed["trigger"]["failed"]["workflow"], "Billing",
        "left as it was"
    );
    assert!(needs(&json!({ "name": "x" })).is_empty());
}

#[test]
fn scopes_are_names_or_beginnings() {
    let scopes = vec!["Billing".to_owned(), "orders-*".to_owned()];
    assert!(in_scope(Some(&scopes), "Billing") && in_scope(Some(&scopes), "orders-eu"));
    assert!(!in_scope(Some(&scopes), "billing") && !in_scope(Some(&scopes), "Order"));
    assert!(in_scope(None, "anything"));
}

#[test]
fn a_change_is_told_setting_by_setting_and_step_by_step() {
    let old = json!({
        "name": "W", "trigger": { "webhook": {} },
        "steps": [
            { "name": "price", "http": { "url": "https://a.example", "method": "GET" } },
            { "name": "old", "set": { "a": 1 } },
            { "where": "value.ok" },
            { "name": "same", "set": { "b": 2 } },
        ],
    });
    let new = json!({
        "name": "W", "trigger": { "schedule": { "every": "1h" } }, "description": "now on a schedule",
        "steps": [
            { "name": "same", "set": { "b": 2 } },
            { "name": "price", "http": { "url": "https://b.example", "method": "GET" }, "retry": 3 },
            { "where": "value.ok" },
            { "name": "notify", "email": { "to": "x@example.com" } },
        ],
    });
    let c = changes(&old, &new);
    assert_eq!(c.settings, vec!["trigger", "description"]);
    assert_eq!(
        c.steps,
        vec![
            StepChange {
                step: "price".into(),
                change: "changed",
                fields: vec!["http.url".into(), "retry".into()]
            },
            StepChange {
                step: "notify".into(),
                change: "added",
                fields: vec![]
            },
            StepChange {
                step: "old".into(),
                change: "removed",
                fields: vec![]
            },
        ]
    );
    assert!(c.reordered);
    assert!(changes(&old, &old).is_empty());
    let unnamed = json!({ "name": "W", "steps": [ { "where": "a" }, { "where": "b" } ] });
    let unnamed_too = json!({ "name": "W", "steps": [ { "where": "a" }, { "where": "c" } ] });
    assert_eq!(changes(&unnamed, &unnamed_too).steps[0].step, "where #2");
    let reordered_keys: Value = serde_json::from_str(r#"{"steps": [], "name": "W"}"#).unwrap();
    assert!(
        changes(&json!({ "name": "W", "steps": [] }), &reordered_keys).is_empty(),
        "the order of settings is no change"
    );
}
