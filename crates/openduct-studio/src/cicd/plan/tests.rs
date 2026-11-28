// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]

use serde_json::json;

use super::*;

fn current(id: &str, name: &str, active: bool, definition: Value) -> Current {
    Current {
        id: id.into(),
        name: name.into(),
        version: 3,
        active,
        definition,
    }
}

fn incoming(file: &str, definition: Value, on: Option<bool>) -> Incoming {
    Incoming {
        file: file.into(),
        definition,
        on,
    }
}

fn inventory() -> Inventory {
    Inventory {
        credentials: ["shop".to_owned()].into(),
        files: ["/rates.csv".to_owned()].into(),
        tables: ["leads".to_owned()].into(),
    }
}

fn hook(name: &str, url: &str) -> Value {
    json!({ "name": name, "trigger": { "webhook": {} }, "steps": [ { "name": "call", "http": { "url": url } } ] })
}

#[test]
fn new_changed_and_same_workflows_are_told_apart_and_stay_as_they_were_unless_asked() {
    let here = [
        current("a", "Alpha", true, hook("Alpha", "https://a.example")),
        current("b", "Beta", false, hook("Beta", "https://b.example")),
        current("c", "Gamma", true, hook("Gamma", "https://c.example")),
    ];
    let pushed = [
        incoming("alpha.yaml", hook("Alpha", "https://a2.example"), None),
        incoming("beta.yaml", hook("Beta", "https://b.example"), None),
        incoming("delta.yaml", hook("Delta", "https://d.example"), None),
    ];
    let planned = plan(&here, &pushed, &inventory(), &Options::default()).unwrap();
    let by = |n: &str| planned.iter().find(|p| p.name == n).unwrap();
    assert_eq!(by("Alpha").action, Action::Update);
    assert_eq!(
        (by("Alpha").from_version, by("Alpha").to_version),
        (Some(3), Some(4))
    );
    assert!(by("Alpha").on, "on stays on, running its new version");
    assert_eq!(by("Alpha").changes.steps[0].fields, vec!["http.url"]);
    assert_eq!(by("Beta").action, Action::Unchanged);
    assert!(!by("Beta").on && by("Beta").definition.is_none());
    assert_eq!(by("Delta").action, Action::Create);
    assert!(!by("Delta").on, "a new workflow stays off");
    assert!(
        planned.iter().all(|p| p.name != "Gamma"),
        "not pruned unless asked"
    );
    let s = summary(&planned);
    assert_eq!(
        (s["create"].as_u64(), s["update"].as_u64(), s["changes"].as_bool()),
        (Some(1), Some(1), Some(true))
    );
}

#[test]
fn activate_and_on_turn_workflows_on_and_prune_turns_off_those_not_pushed() {
    let here = [
        current("a", "Alpha", false, hook("Alpha", "https://a.example")),
        current("c", "Gamma", true, hook("Gamma", "https://c.example")),
        current(
            "d",
            "Off already",
            false,
            hook("Off already", "https://d.example"),
        ),
    ];
    let pushed = [
        incoming("alpha.yaml", hook("Alpha", "https://a.example"), None),
        incoming("new.yaml", hook("New", "https://n.example"), Some(false)),
    ];
    let options = Options {
        activate: true,
        prune: true,
        scopes: None,
    };
    let planned = plan(&here, &pushed, &inventory(), &options).unwrap();
    let by = |n: &str| planned.iter().find(|p| p.name == n).unwrap();
    assert!(by("Alpha").on && !by("Alpha").was_on && changes_anything(by("Alpha")));
    assert!(!by("New").on, "the file's on: false wins over --activate");
    assert_eq!(by("Gamma").action, Action::TurnOff);
    assert!(
        planned.iter().all(|p| p.name != "Off already"),
        "one off already is left alone"
    );
}

#[test]
fn every_wrong_file_is_said_and_nothing_is_planned() {
    let here = [
        current("x1", "Twice", false, hook("Twice", "https://x.example")),
        current("x2", "Twice", false, hook("Twice", "https://y.example")),
    ];
    let pushed = [
        incoming("a.yaml", json!({ "steps": [] }), None),
        incoming(
            "b.yaml",
            json!({ "name": "B", "trigger": { "nonsense": {} } }),
            None,
        ),
        incoming("c.yaml", hook("C", "https://c.example"), None),
        incoming("c-again.yaml", hook("C", "https://c.example"), None),
        incoming("twice.yaml", hook("Twice", "https://x.example"), None),
    ];
    let Err(Refusal::Problems(problems)) = plan(&here, &pushed, &inventory(), &Options::default()) else {
        panic!("refused");
    };
    let files: Vec<&str> = problems.iter().map(|p| p["file"].as_str().unwrap()).collect();
    assert_eq!(files, vec!["a.yaml", "c-again.yaml", "twice.yaml", "b.yaml"]);
    assert!(problems[3]["problem"].as_str().unwrap().contains("nonsense"));
}

#[test]
fn what_the_workspace_lacks_is_said_by_name_for_each_workflow() {
    let needs_all = json!({
        "name": "Needs", "steps": [
            { "http": { "credential": "shop", "url": "https://x.example" } },
            { "email": { "credential": "mail", "to": "a@example.com", "subject": "s", "text": "t" } },
            { "lookup": { "file": "/missing.csv", "where": { "code": "{{ value.code }}" } } },
            { "table": { "name": "nowhere", "action": "count" } },
            { "run_workflow": { "workflow": "Not here" } },
        ],
    });
    let Err(Refusal::Missing(missing)) = plan(
        &[],
        &[incoming("n.yaml", needs_all, None)],
        &inventory(),
        &Options::default(),
    ) else {
        panic!("refused");
    };
    assert_eq!(missing[0]["credentials"], json!(["mail"]));
    assert_eq!(missing[0]["files"], json!(["/missing.csv"]));
    assert_eq!(missing[0]["tables"], json!(["nowhere"]));
    assert_eq!(missing[0]["workflows"], json!(["Not here"]));
    let body = Refusal::Missing(missing).body();
    assert!(body["error"].as_str().unwrap().contains("nothing was changed"));
}

#[test]
fn other_workflows_named_by_name_land_as_their_ids_here() {
    let here = [current("sub-id", "Sub", false, hook("Sub", "https://s.example"))];
    let caller = json!({ "name": "Caller", "steps": [ { "run_workflow": { "workflow": "Sub" } }, { "run_workflow": { "workflow": "Also new" } } ] });
    let pushed = [
        incoming("caller.yaml", caller, None),
        incoming("also.yaml", hook("Also new", "https://n.example"), None),
    ];
    let planned = plan(&here, &pushed, &inventory(), &Options::default()).unwrap();
    let kept = planned[0].definition.as_ref().unwrap();
    assert_eq!(kept["steps"][0]["run_workflow"]["workflow"], "sub-id");
    assert_eq!(
        kept["steps"][1]["run_workflow"]["workflow"],
        json!(planned[1].id),
        "a new one's id as it will be"
    );
    // The same file pushed again, once Caller holds the ids: the same.
    let again = [
        current("sub-id", "Sub", false, hook("Sub", "https://s.example")),
        current(&planned[0].id, "Caller", false, kept.clone()),
        current(
            &planned[1].id,
            "Also new",
            false,
            hook("Also new", "https://n.example"),
        ),
    ];
    let caller = json!({ "name": "Caller", "steps": [ { "run_workflow": { "workflow": "Sub" } }, { "run_workflow": { "workflow": "Also new" } } ] });
    let second = plan(
        &again,
        &[incoming("caller.yaml", caller, None)],
        &inventory(),
        &Options::default(),
    )
    .unwrap();
    assert_eq!(second[0].action, Action::Unchanged);
}

#[test]
fn a_token_limited_to_some_workflows_pushes_and_prunes_only_those() {
    let here = [
        current("a", "billing-eu", true, hook("billing-eu", "https://a.example")),
        current("b", "Other", true, hook("Other", "https://b.example")),
    ];
    let options = Options {
        activate: false,
        prune: true,
        scopes: Some(vec!["billing-*".to_owned()]),
    };
    let Err(Refusal::Problems(p)) = plan(
        &here,
        &[incoming("o.yaml", hook("Other", "https://b2.example"), None)],
        &inventory(),
        &options,
    ) else {
        panic!("refused");
    };
    assert!(p[0]["problem"].as_str().unwrap().contains("outside"));
    let planned = plan(&here, &[], &inventory(), &options).unwrap();
    assert_eq!(planned.len(), 1);
    assert_eq!(
        (planned[0].name.as_str(), planned[0].action),
        ("billing-eu", Action::TurnOff)
    );
}
