// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A test run's answers shaped like real ones ([`Effects::samples`]), so the steps after a request go through a list
//! or read a field as they will in a real run: an app's action answers with the app's own recorded example, an HTTP
//! step with the sample answer it was given - each marked "sample answer, nothing sent", beside the request it would
//! have sent. A table not made yet is an empty stand-in, with the columns the step gives it, and the step's record
//! says so. A preview and a real run are not changed: a preview reads for real, and a real run still fails, plainly,
//! at a table that is not there. // test runs

use std::sync::Arc;

use openduct_expr::{Env, Record, Value};
use serde_json::{Value as Json, json};

use super::{Done, Effects, Run};
use crate::model::tables::TableStep;
use crate::model::{Defined, Http};
use crate::tables::{Access, Plan, StatePlan, TableData, Tables};

/// What a step's record says of an answer given in place of sending.
pub(crate) const SAMPLE: &str = "sample answer, nothing sent";
/// The largest sample answer an HTTP step is given, in bytes as JSON.
pub(crate) const MAX_SAMPLE: usize = 64 * 1024;

type Pending<'a, T> = std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// An HTTP step's `sample`: the body of the answer a test run gives in place of sending, any JSON, at most
/// [`MAX_SAMPLE`].
pub(crate) fn http_sample(v: Option<&Json>, at: &str) -> Result<Option<Json>, String> {
    match v {
        None | Some(Json::Null) => Ok(None),
        Some(v) if v.to_string().len() > MAX_SAMPLE => Err(format!(
            "{at}.sample: a sample answer of at most {} KiB",
            MAX_SAMPLE / 1024
        )),
        Some(v) => Ok(Some(v.clone())),
    }
}

/// The request a test run did not send, when `answer` is a test run's answer for one.
fn not_sent(answer: &Value) -> Option<Value> {
    let Value::Record(r) = answer else { return None };
    let test = r.get("test").map(crate::template::as_text)?;
    (test == "not sent").then(|| r.get("request").cloned().unwrap_or(Value::Null))
}

/// `sample` as the step's result, its record saying so, with the request it would have sent.
fn noted(sample: Value, request: Value) -> Done {
    let mut note = Record::new();
    note.insert("sample".to_owned(), Value::text(SAMPLE));
    note.insert("request".to_owned(), request);
    Done::Noted(sample, Value::record(note))
}

/// An app's action: in a test run, its recorded example in place of what it would have sent.
pub(super) async fn app(run: &Run<'_>, d: &Defined, env: &Env<'_>) -> Result<Done, String> {
    let answer = super::defined::run(run.effects, d, env).await?;
    if run.effects.samples()
        && let Some(request) = not_sent(&answer)
    {
        let kind = run
            .effects
            .credential(&d.credential)
            .map(|c| c.kind)
            .unwrap_or_default();
        if let Some(example) = crate::app_files::example(d.app, &d.action, &kind) {
            return Ok(noted(Value::from_json(&example), request));
        }
    }
    Ok(Done::Value(answer))
}

/// An HTTP step's answer: in a test run, the sample answer the person gave it (`{status: 200, headers, body}`), when
/// it has one.
pub(super) fn http(effects: &dyn Effects, http: &Http, answer: Value) -> Done {
    match (&http.sample, effects.samples(), not_sent(&answer)) {
        (Some(body), true, Some(request)) => noted(
            Value::from_json(&json!({ "status": 200, "headers": {}, "body": body })),
            request,
        ),
        _ => Done::Value(answer),
    }
}

/// A `table` step. In a test run, a table not made yet is an empty one with the step's columns: reads find nothing,
/// a write is worked out and shown, and the record says the table does not exist yet.
pub(super) async fn table(run: &Run<'_>, step: &TableStep, env: &Env<'_>) -> Result<Done, String> {
    let access = run.effects.tables();
    let failed = match super::tables::table_in(access, step, env).await {
        Ok(result) => return Ok(Done::Value(result)),
        Err(e) => e,
    };
    let tables = match access {
        Access::Read(t) | Access::Write(t) if run.effects.samples() => t,
        _ => return Err(failed),
    };
    // Only a table that is not there: any other failure is the step's.
    if tables.has(&step.table).await.unwrap_or(true) {
        return Err(failed);
    }
    let stand_in = StandIn(Arc::new(TableData {
        name: step.table.clone(),
        columns: crate::tables::wanted::columns(step, env),
        next_id: 1,
        stand_in: true,
        ..TableData::default()
    }));
    let result = super::tables::table_in(Access::Read(&stand_in), step, env).await?;
    let mut note = Record::new();
    note.insert(
        "note".to_owned(),
        Value::text(
            format!(
                "the table {} does not exist yet: this test used an empty one",
                step.table
            )
            .as_str(),
        ),
    );
    Ok(Done::Noted(result, Value::record(note)))
}

/// One empty table, read and never written: a test run's stand-in for a table not made yet.
#[derive(Debug)]
struct StandIn(Arc<TableData>);

impl Tables for StandIn {
    fn read<'a>(&'a self, _: &'a str) -> Pending<'a, Arc<TableData>> {
        Box::pin(async move { Ok(Arc::clone(&self.0)) })
    }

    fn change<'a>(&'a self, _: &'a str, _: Plan<'a>) -> Pending<'a, Value> {
        Box::pin(async { Err("a test run writes no table".to_owned()) })
    }

    fn state<'a>(&'a self, _: &'a str) -> Pending<'a, Arc<Record>> {
        Box::pin(async { Ok(Arc::default()) })
    }

    fn change_state<'a>(&'a self, _: &'a str, _: StatePlan<'a>) -> Pending<'a, Value> {
        Box::pin(async { Err("a test run writes no state".to_owned()) })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_answer_is_bounded_and_only_a_request_not_sent_is_answered_with_one() {
        assert_eq!(http_sample(None, "s").unwrap(), None);
        assert_eq!(
            http_sample(Some(&json!({ "a": 1 })), "s").unwrap(),
            Some(json!({ "a": 1 }))
        );
        let big = json!("x".repeat(MAX_SAMPLE));
        assert!(
            http_sample(Some(&big), "steps.get")
                .unwrap_err()
                .contains("steps.get.sample")
        );
        let shown =
            Value::from_json(&json!({ "test": "not sent", "request": { "url": "https://x.example" } }));
        assert_eq!(
            not_sent(&shown).unwrap().to_json(),
            r#"{"url":"https://x.example"}"#
        );
        for other in [
            json!({ "test": "not written" }),
            json!({ "status": 200, "body": { "test": "not sent" } }),
            json!([1]),
        ] {
            assert!(not_sent(&Value::from_json(&other)).is_none(), "{other}");
        }
    }
}
