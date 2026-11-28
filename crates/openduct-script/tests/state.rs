// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A script given its workflow's state (`state`) hands back what it left there - changed in place or set anew - and
//! a script not given one hands back none.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::time::Duration;

use openduct_script::{Language, Script, run_here};
use serde_json::{Map, Value as Json, json};

fn script(language: Language, code: &str, names: Json) -> Script {
    let names: Map<String, Json> = names.as_object().cloned().unwrap_or_default();
    Script {
        language,
        code: code.to_owned(),
        names,
        time: Duration::from_secs(10),
    }
}

#[test]
fn javascript_leaves_the_state_it_was_given_changed() {
    let given = json!({ "value": { "id": 7 }, "state": { "count": 2, "seen": [1] } });
    let answer = run_here(&script(
        Language::JavaScript,
        "state.count = (state.count || 0) + 1; state.seen.push(value.id); state.last = value.id; return state.count;",
        given,
    ))
    .unwrap();
    assert_eq!(answer.result, json!(3));
    assert_eq!(
        answer.state,
        Some(json!({ "count": 3, "seen": [1, 7], "last": 7 }))
    );
    // Set anew, as a whole.
    let answer = run_here(&script(
        Language::JavaScript,
        "state = { fresh: true };",
        json!({ "state": {} }),
    ))
    .unwrap();
    assert_eq!(answer.state, Some(json!({ "fresh": true })));
    // Not given a state: none handed back, whatever the script names.
    let answer = run_here(&script(
        Language::JavaScript,
        "var state = 1; return 2;",
        json!({}),
    ))
    .unwrap();
    assert_eq!((answer.result, answer.state), (json!(2), None));
}

#[test]
fn python_leaves_the_state_it_was_given_changed() {
    let given = json!({ "value": { "id": 7 }, "state": { "count": 2 } });
    let answer = run_here(&script(
        Language::Python,
        "state[\"count\"] = state.get(\"count\", 0) + 1\nstate[\"last\"] = value[\"id\"]\nresult = state[\"count\"]",
        given,
    ))
    .unwrap();
    assert_eq!(answer.result, json!(3));
    assert_eq!(answer.state, Some(json!({ "count": 3, "last": 7 })));
    let answer = run_here(&script(Language::Python, "result = 1", json!({}))).unwrap();
    assert_eq!(answer.state, None);
}
