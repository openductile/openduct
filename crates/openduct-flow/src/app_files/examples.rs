// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What an app's actions answer, from its recorded exchanges (`apps/<id>/tests/*.yaml`, see [`super::recorded`]):
//! each recording's `result`, the vendor's answer as the action reads it. A test run sends nothing; it answers an
//! action with one of these, so the steps after it go through a list or read a field as they will. // test runs

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use serde_json::Value as Json;

use super::AppDef;

/// The largest example kept, in bytes as JSON; a larger one is left out.
const MAX_EXAMPLE: usize = 64 * 1024;
/// The most recordings read for one app, and the largest file read.
const MAX_RECORDED: usize = 500;
const MAX_FILE: u64 = 256 * 1024;

/// One recording's answer: the file it is in, the action, the sign-in it was made with (none: the app's first), and
/// the result.
#[derive(Debug, Clone)]
struct Example {
    file: String,
    action: String,
    sign_in: Option<String>,
    result: Json,
}

/// The examples, by app id: the built-in apps' read once, an added app's when it is added.
fn held() -> &'static RwLock<HashMap<String, Vec<Example>>> {
    static HELD: OnceLock<RwLock<HashMap<String, Vec<Example>>>> = OnceLock::new();
    HELD.get_or_init(|| {
        let mut by_app: HashMap<String, Vec<Example>> = HashMap::new();
        for app in super::all() {
            let Some(dir) = app.source.strip_suffix("/app.yaml") else {
                continue;
            };
            let found: Vec<Example> = super::built_in::RECORDED
                .iter()
                .filter(|(of, _, _)| *of == dir)
                .filter_map(|(_, file, text)| read(file, text))
                .take(MAX_RECORDED)
                .collect();
            if !found.is_empty() {
                by_app.insert(app.id.clone(), found);
            }
        }
        RwLock::new(by_app)
    })
}

/// A recording's answer, when it has one: an action's, with a result (not a refusal, nor a trigger's), small enough.
fn read(file: &str, text: &str) -> Option<Example> {
    let recorded = crate::yaml_to_json(text).ok()?;
    let action = recorded.get("action")?.as_str()?.to_owned();
    let result = recorded.get("result").filter(|r| !r.is_null())?.clone();
    if recorded.get("error").is_some() || result.to_string().len() > MAX_EXAMPLE {
        return None;
    }
    Some(Example {
        file: file.to_owned(),
        action,
        sign_in: recorded.get("sign_in").and_then(Json::as_str).map(str::to_owned),
        result,
    })
}

/// Keeps the recordings in `tests` (`*.yaml`) as app `app`'s examples, in place of any it had: an app of one's own
/// added from a folder. A folder that cannot be read gives none.
pub(super) fn add_dir(app: &str, tests: &std::path::Path) {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(tests)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "yaml"))
                .filter(|p| std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.len() <= MAX_FILE))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    let found: Vec<Example> = files
        .iter()
        .take(MAX_RECORDED)
        .filter_map(|p| {
            let name = p.file_name()?.to_string_lossy().into_owned();
            read(&name, &std::fs::read_to_string(p).ok()?)
        })
        .collect();
    if let Ok(mut all) = held().write() {
        all.insert(app.to_owned(), found);
    }
}

/// The example answer of `app`'s action `action`, for a credential of kind `kind`: a recording made with that
/// sign-in first, and one named after the action first; none when the app recorded none for it.
pub fn example(app: &AppDef, action: &str, kind: &str) -> Option<Json> {
    let first = app.sign_ins.first().map_or("", |s| s.id.as_str());
    let all = held().read().ok()?;
    let rank = |e: &Example| {
        let sign_in = e.sign_in.as_deref().unwrap_or(first);
        (
            sign_in != kind,
            e.file.strip_suffix(".yaml") != Some(action),
            e.file.clone(),
        )
    };
    all.get(&app.id)?
        .iter()
        .filter(|e| e.action == action)
        .min_by_key(|e| rank(e))
        .map(|e| e.result.clone())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_action_s_example_is_its_recorded_result() {
        let zendesk = super::super::find("zendesk").unwrap();
        let tickets = example(zendesk, "list_tickets", "zendesk").unwrap();
        assert!(tickets.as_array().is_some_and(|t| t.len() >= 2), "{tickets}");
        assert_eq!(tickets[0]["id"], json!(1));
        // A credential of the other sign-in: the recording made with it.
        let client = example(zendesk, "list_tickets", "zendesk_client").unwrap();
        assert_eq!(client, json!([{ "id": 1 }]));
        // A refusal is no example; an action with no recording has none.
        let hubspot = super::super::find("hubspot").unwrap();
        assert_eq!(
            example(hubspot, "create_contact", "hubspot").unwrap()["id"],
            "512"
        );
        assert!(example(zendesk, "no_such_action", "zendesk").is_none());
        assert!(read("t.yaml", "trigger: new\nexchanges: []\nids: []\n").is_none());
        assert!(read("t.yaml", "action: a\nexchanges: []\nerror: no\n").is_none());
        let big = format!(
            "action: a\nexchanges: []\nresult: \"{}\"\n",
            "x".repeat(MAX_EXAMPLE)
        );
        assert!(read("t.yaml", &big).is_none(), "a larger one is left out");
    }
}
