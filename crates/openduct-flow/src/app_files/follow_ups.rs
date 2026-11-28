// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An action made of more than one request (`then:` in its definition): each request after the first reads the
//! step's inputs and `input.previous`, what the request before gave - Slack's upload asks for an address, sends the
//! file's bytes there, and finishes the upload. And a body sent as it is (`body: raw`): text, or a run's file, whose
//! bytes are loaded where the run keeps them.

use openduct_expr::{Record, Value};
use serde_json::{Map, Value as Json};

use super::definition::Action;
use crate::run::{Effects, HttpRequest};

/// The most requests that follow an action's own.
const MOST_FOLLOW_UPS: usize = 3;
/// What a raw body naming a run's file is marked as, until the file's bytes take its place.
pub(super) const FILE_BODY: &str = "application/x-openduct-run-file";

/// Reads an action's follow-up requests: each written as an action is, its label and whether it writes the action's.
pub(super) fn read(
    list: &[Json],
    at: &str,
    label: &str,
    write: bool,
    sign_ins: &[String],
) -> Result<Vec<Action>, String> {
    if list.len() > MOST_FOLLOW_UPS {
        return Err(format!("{at}.then: at most {MOST_FOLLOW_UPS} requests"));
    }
    let mut out = Vec::with_capacity(list.len());
    for (i, json) in list.iter().enumerate() {
        let here = format!("{at}.then[{i}]");
        let Json::Object(fields) = json else {
            return Err(format!("{here}: a request, written as an action is"));
        };
        if let Some(k) = ["label", "inputs", "then", "items", "next", "by", "mcp"]
            .iter()
            .find(|k| fields.contains_key(**k))
        {
            return Err(format!(
                "{here}.{k}: a request that follows takes the action's inputs and gives one answer"
            ));
        }
        let mut own: Map<String, Json> = fields.clone();
        own.insert("label".to_owned(), Json::String(label.to_owned()));
        own.insert("write".to_owned(), Json::Bool(write));
        // A whole address (one the request before gave, as Slack's upload address): for every sign-in, and checked
        // against the app's hosts as each request is made.
        if let Some(url) = own.remove("url") {
            let by: Map<String, Json> = sign_ins
                .iter()
                .map(|id| (id.clone(), serde_json::json!({ "url": url })))
                .collect();
            own.insert("by".to_owned(), Json::Object(by));
        }
        out.push(super::definition::action_of(&Json::Object(own), &here, sign_ins)?);
    }
    Ok(out)
}

/// The inputs a request that follows reads: the step's own, `previous` (what the request before gave) and `answers`
/// (what each request before gave, the action's own first).
pub(crate) fn with_previous(inputs: &Value, answers: &[Value]) -> Value {
    let mut record = match inputs {
        Value::Record(r) => (**r).clone(),
        _ => Record::new(),
    };
    record.insert(
        "previous".to_owned(),
        answers.last().cloned().unwrap_or(Value::Null),
    );
    record.insert("answers".to_owned(), Value::list(answers.to_vec()));
    Value::record(record)
}

/// A raw body that names a run's file, its bytes put in its place (a test run, which keeps no files, sends none).
pub(crate) async fn load_file_body(effects: &dyn Effects, request: &mut HttpRequest) -> Result<(), String> {
    let Some((kind, named)) = &request.body else {
        return Ok(());
    };
    if kind != FILE_BODY {
        return Ok(());
    }
    let reference = Value::from_json(&serde_json::from_slice::<Json>(named).unwrap_or(Json::Null));
    if effects.samples() {
        request.body = Some(("application/octet-stream".to_owned(), Vec::new()));
        return Ok(());
    }
    let file = crate::files::RunFiles::default()
        .read(effects, &reference)
        .await?;
    request.body = Some((file.media_type.clone(), file.bytes.to_vec()));
    Ok(())
}
