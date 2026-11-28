// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app's recorded exchanges (`apps/<id>/tests/*.yaml`): an action run with a credential and inputs, what each of
//! its requests must send, and what the vendor answered - so a definition is checked with no account and no network.
//! `openduct app test` runs them, and so does CI for every app.
//!
//! ```yaml
//! action: add_customer
//! credential: { key: sk_test_1 }          # its fields; `sign_in:` names which, the first by default
//! input: { email: a@b.example }
//! exchanges:
//!   - sent:                               # each part optional: what is given must match
//!       method: POST
//!       url: https://api.shop.example/v1/customers
//!       headers: { authorization: Bearer sk_test_1 }
//!       body: { email: a@b.example }      # JSON, or a form read as names and values
//!     answer: { status: 200, body: { id: cus_1 } }
//! result: { id: cus_1 }                    # or `error:` - words the refusal must hold
//! ```

use std::collections::BTreeMap;

use openduct_expr::Value;
use serde::Deserialize;
use serde_json::{Value as Json, json};

use super::definition::AppDef;
use crate::apps::Credential;
use crate::run::HttpRequest;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Recorded {
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    action: String,
    #[serde(default)]
    sign_in: Option<String>,
    #[serde(default)]
    credential: BTreeMap<String, String>,
    #[serde(default)]
    input: Option<Json>,
    exchanges: Vec<Exchange>,
    #[serde(default)]
    result: Option<Json>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Exchange {
    #[serde(default)]
    pub(super) sent: Sent,
    pub(super) answer: Answered,
}

#[derive(Deserialize, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Sent {
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    query: Option<BTreeMap<String, String>>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: Option<Json>,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Answered {
    #[serde(default = "ok")]
    pub(super) status: u64,
    #[serde(default)]
    pub(super) headers: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) body: Json,
}

fn ok() -> u64 {
    200
}

/// A request's body as a test writes it: JSON as it is, a form as its names and values.
fn body_of(r: &HttpRequest) -> Json {
    match &r.body {
        None => Json::Null,
        Some((kind, bytes)) if kind.starts_with("application/x-www-form-urlencoded") => {
            // A name given more than once (`events[]=a&events[]=b`) is a list of its values.
            let mut pairs = serde_json::Map::new();
            for (k, v) in url_pairs(bytes) {
                match pairs.get_mut(&k) {
                    Some(Json::Array(list)) => list.push(Json::String(v)),
                    Some(first) => *first = Json::Array(vec![first.take(), Json::String(v)]),
                    None => {
                        pairs.insert(k, Json::String(v));
                    }
                }
            }
            Json::Object(pairs)
        }
        Some((_, bytes)) => serde_json::from_slice(bytes)
            .unwrap_or_else(|_| Json::String(String::from_utf8_lossy(bytes).into_owned())),
    }
}

fn url_pairs(bytes: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(bytes);
    reqwest::Url::parse(&format!("http://form/?{text}"))
        .map(|u| {
            u.query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default()
}

/// Whether two values are the same as a service reads them: numbers by their value (`100` is `100.0`), records
/// whatever the order of their fields.
pub(super) fn same(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Number(x), Json::Number(y)) => x.as_f64() == y.as_f64(),
        (Json::Array(x), Json::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q)),
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

pub(super) fn compare(r: &HttpRequest, sent: &Sent, at: &str) -> Result<(), String> {
    let differ = |what: &str, got: &dyn std::fmt::Display, want: &dyn std::fmt::Display| {
        Err(format!(
            "{at}.sent.{what}: it sent {got}, the recording has {want}"
        ))
    };
    if let Some(m) = &sent.method
        && !m.eq_ignore_ascii_case(&r.method)
    {
        return differ("method", &r.method, m);
    }
    if let Some(u) = &sent.url
        && u != &r.url
    {
        return differ("url", &r.url, u);
    }
    if let Some(q) = &sent.query {
        let got: BTreeMap<String, String> = r.query.iter().cloned().collect();
        // A name sent twice (page=1&page=2) is not the query recorded, whatever the last one says.
        if got.len() != r.query.len() || &got != q {
            return differ("query", &format!("{got:?}"), &format!("{q:?}"));
        }
    }
    for (name, want) in &sent.headers {
        let got = r
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str());
        if got != Some(want.as_str()) {
            return differ(&format!("headers.{name}"), &got.unwrap_or("nothing"), want);
        }
    }
    if let Some(want) = &sent.body {
        let got = body_of(r);
        if !same(&got, want) {
            return differ("body", &got, want);
        }
    }
    Ok(())
}

/// Runs one recorded test of `app` (`text`, read from `source`): every request as recorded, the result (or the
/// refusal) as recorded. What differs is said, with where.
pub fn check(app: &AppDef, text: &str, source: &str) -> Result<(), String> {
    let json = crate::yaml::to_json(text).map_err(|e| format!("{source}: {e}"))?;
    // G3 app triggers: a recording of a trigger's poll, delivery or registration.
    if json.get("trigger").is_some() {
        return super::recorded_triggers::check(app, json, source);
    }
    // app generator: a recording of an action that calls a tool of the app's MCP server.
    if json.get("call").is_some() {
        return super::mcp_action::check_recorded(app, json, source);
    }
    let t: Recorded = serde_json::from_value(json).map_err(|e| format!("{source}: {e}"))?;
    let (_, action) = app
        .action(Some(&t.action))
        .ok_or_else(|| format!("{source}: action: {} has no action {}", app.name, t.action))?;
    let kind = match &t.sign_in {
        Some(s) if app.sign_in(s).is_none() => {
            return Err(format!("{source}: sign_in: {} has no sign-in {s}", app.name));
        }
        Some(s) => s.clone(),
        None => app.sign_ins.first().map(|s| s.id.clone()).unwrap_or_default(),
    };
    let credential = Credential {
        kind,
        fields: t.credential.clone(),
    };
    let input = t.input.clone().unwrap_or_else(|| json!({}));
    if let Json::Object(given) = &input {
        for k in given.keys() {
            if !action.inputs.iter().any(|i| &i.name == k) {
                return Err(format!("{source}: input.{k}: not an input of {}", t.action));
            }
        }
        for i in action.inputs.iter().filter(|i| i.required) {
            if !given.contains_key(&i.name) {
                return Err(format!("{source}: input.{}: {} needs it", i.name, t.action));
            }
        }
    }
    let inputs = Value::from_json(&input);
    let refused = |e: String| -> Result<(), String> {
        match &t.error {
            Some(want) if e.contains(want.as_str()) => Ok(()),
            Some(want) => Err(format!(
                "{source}: it was refused \"{e}\", the recording says \"{want}\""
            )),
            None => Err(format!("{source}: {e}")),
        }
    };
    let mut page = Value::Null;
    let mut items: Vec<Value> = Vec::new();
    let mut result: Option<Value> = None;
    // messaging: the request at hand - the action's own, then each that follows it - and what it reads.
    let (first, mut step, mut inputs) = (action, 0usize, inputs);
    let (original, mut answers) = (inputs.clone(), Vec::new());
    for (i, ex) in t.exchanges.iter().enumerate() {
        let at = format!("{source}: exchanges[{i}]");
        if let Some(done) = result.take_if(|_| step < first.then.len()) {
            answers.push(done);
            inputs = super::follow_ups::with_previous(&original, &answers);
            step += 1;
        }
        let action = step
            .checked_sub(1)
            .and_then(|s| first.then.get(s))
            .unwrap_or(first);
        if result.is_some() {
            return Err(format!("{at}: the action had finished: there was no request {i}"));
        }
        let request = match super::request(app, action, &credential, &inputs, &page) {
            Ok(r) => r,
            Err(e) => return refused(e),
        };
        compare(&request, &ex.sent, &at)?;
        let answered = Value::from_json(&json!({
            "status": ex.answer.status, "headers": ex.answer.headers, "body": ex.answer.body,
        }));
        let got = match super::answer(app, action, &credential, &answered, &page) {
            Ok(g) => g,
            Err(e) => return refused(e),
        };
        if action.items.is_none() {
            result = Some(got.result);
            continue;
        }
        items.extend(got.items);
        if matches!(got.next, Value::Null) {
            result = Some(Value::list(std::mem::take(&mut items)));
        } else {
            page = got.next;
        }
    }
    let Some(result) = result else {
        return Err(format!(
            "{source}: the action asked for another page: the recording has no answer for it"
        ));
    };
    // messaging: every request that follows the action's own is recorded too.
    if step < first.then.len() && t.error.is_none() {
        return Err(format!(
            "{source}: the action makes {} more requests: the recording has no answer for them",
            first.then.len() - step
        ));
    }
    if let Some(want) = &t.error {
        return Err(format!(
            "{source}: it succeeded; the recording says it is refused (\"{want}\")"
        ));
    }
    if let Some(want) = &t.result {
        let got: Json = serde_json::from_str(&result.to_json()).unwrap_or(Json::Null);
        if !same(&got, want) {
            return Err(format!(
                "{source}: result: it gave {got}, the recording has {want}"
            ));
        }
    }
    Ok(())
}
