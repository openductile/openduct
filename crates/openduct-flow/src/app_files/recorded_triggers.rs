// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Recorded tests of an app's triggers (`apps/<id>/tests/*.yaml` with `trigger:`), run with no account and no
//! network, as the actions' are:
//!
//! ```yaml
//! trigger: new_email                       # a poll: what it asks, what the app answered, the runs it starts
//! credential: { access_token: t1 }
//! input: { q: "in:inbox" }
//! seen: { items: [[m1, ""]] }              # what the polls before saw; none: the first poll
//! exchanges: [ { sent: {...}, answer: {...} } ]
//! runs: [ { id: m2 } ]                     # each run's value, oldest first
//! cursor: ...                              # where the next poll starts, if the trigger keeps one
//! ```
//!
//! ```yaml
//! trigger: event                           # a delivery: as the app sent it, signature and all
//! credential: { webhook_secret: whsec_1 }
//! delivery: { headers: { stripe-signature: "t=...,v1=..." }, body: '{"id": "evt_1"}', at: 1700000000 }
//! runs: [ { id: evt_1 } ]                  # or answer: (a challenge), passed: true, refused: words
//! ```
//!
//! ```yaml
//! trigger: event                           # registering the address with the app when a workflow is turned on
//! register: true                           # (or unregister: true)
//! workflow: wf1
//! address: https://studio.example/hooks/h1
//! exchanges: [ ... ]
//! kept: { webhook_secret: whsec_1 }        # what is kept for the workflow
//! ```
//!
//! The requests go through the same driver a live poll and a live registration use, its token renewal included.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use openduct_expr::Value;
use serde::Deserialize;
use serde_json::{Value as Json, json};

use super::definition::AppDef;
use super::hook::{Delivery, Verdict};
use super::poll::{Polling, Seen};
use super::recorded::{Exchange, compare, same};
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// The time a recording's registering and renewing happen at, in milliseconds since 1970. // messaging
const RECORDED_AT: i64 = 1_700_000_000_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Recorded {
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    trigger: String,
    #[serde(default)]
    sign_in: Option<String>,
    #[serde(default)]
    credential: BTreeMap<String, String>,
    #[serde(default)]
    input: Option<Json>,
    #[serde(default)]
    seen: Option<Seen>,
    #[serde(default)]
    max_items: Option<u32>,
    #[serde(default)]
    exchanges: Vec<Exchange>,
    #[serde(default)]
    runs: Option<Vec<Json>>,
    #[serde(default)]
    ids: Option<Vec<String>>,
    #[serde(default)]
    cursor: Option<Json>,
    #[serde(default)]
    delivery: Option<DeliveryFile>,
    #[serde(default)]
    answer: Option<Json>,
    #[serde(default)]
    passed: bool,
    #[serde(default)]
    refused: Option<String>,
    #[serde(default)]
    register: bool,
    #[serde(default)]
    unregister: bool,
    /// messaging: a renewal of what registering made (its requests at the recording's time, 2023-11-14T22:13:20Z).
    #[serde(default)]
    renew: bool,
    #[serde(default)]
    workflow: Option<String>,
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    kept: Option<BTreeMap<String, String>>,
    /// A GET that checks the address, as its query: answered with `answer` (text), or refused.
    #[serde(default)]
    confirm: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryFile {
    #[serde(default = "post")]
    method: String,
    #[serde(default)]
    address: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    query: String,
    #[serde(default)]
    content_type: Option<String>,
    body: String,
    /// When it is checked, in seconds since 1970.
    at: i64,
}

fn post() -> String {
    "POST".to_owned()
}

/// Effects answering from the recording: each request compared with the next exchange's, its answer given back.
struct Played {
    exchanges: Mutex<VecDeque<(usize, Exchange)>>,
    credential: Credential,
    kept: Mutex<BTreeMap<String, String>>,
    source: String,
    differs: Mutex<Option<String>>,
}

impl Effects for Played {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let next = self.exchanges.lock().map_err(|_| "poisoned")?.pop_front();
            let Some((i, ex)) = next else {
                let e = format!(
                    "{}: it sent {} {}: the recording has no more exchanges",
                    self.source, request.method, request.url
                );
                return Err(self.differ(e));
            };
            if let Err(e) = compare(&request, &ex.sent, &format!("{}: exchanges[{i}]", self.source)) {
                return Err(self.differ(e));
            }
            Ok(Value::from_json(&json!({
                "status": ex.answer.status, "headers": ex.answer.headers, "body": ex.answer.body,
            })))
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("a trigger writes to no topic".to_owned()) })
    }

    fn credential(&self, _: &str) -> Result<Credential, String> {
        Ok(self.credential.clone())
    }

    fn keep_credential<'a>(&'a self, _: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        Box::pin(async move {
            self.kept.lock().map_err(|_| "poisoned")?.extend(fields);
            Ok(Value::Null)
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("a trigger reads no file {path}")) })
    }
}

impl Played {
    /// A request that differs from the recording: said as it is, whatever the driver makes of the error.
    fn differ(&self, e: String) -> String {
        if let Ok(mut d) = self.differs.lock() {
            d.get_or_insert_with(|| e.clone());
        }
        e
    }
}

/// Runs a future whose every wait is already over (the recording answers at once): no runtime is needed.
fn at_once<F: Future>(f: F) -> Result<F::Output, String> {
    let mut f = std::pin::pin!(f);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..10_000 {
        if let std::task::Poll::Ready(out) = f.as_mut().poll(&mut cx) {
            return Ok(out);
        }
    }
    Err("the recorded test waited on something that never came".to_owned())
}

fn as_json(v: &Value) -> Json {
    serde_json::from_str(&v.to_json()).unwrap_or(Json::Null)
}

/// Runs one recorded test of a trigger of `app`. What differs is said, with where.
pub(super) fn check(app: &AppDef, json: Json, source: &str) -> Result<(), String> {
    let t: Recorded = serde_json::from_value(json).map_err(|e| format!("{source}: {e}"))?;
    let (name, trigger) = app
        .trigger(Some(&t.trigger))
        .ok_or_else(|| format!("{source}: trigger: {} has no trigger {}", app.name, t.trigger))?;
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
            if !trigger.inputs.iter().any(|i| &i.name == k) {
                return Err(format!("{source}: input.{k}: not an input of {name}"));
            }
        }
    }
    let played = Played {
        exchanges: Mutex::new(t.exchanges.iter().cloned().enumerate().collect()),
        credential: credential.clone(),
        kept: Mutex::new(BTreeMap::new()),
        source: source.to_owned(),
        differs: Mutex::new(None),
    };
    let workflow = t.workflow.clone().unwrap_or_else(|| "recorded".to_owned());
    let outcome = if let Some(query) = &t.confirm {
        super::hook::confirm(trigger, &credential, query)
            .map_err(|r| format!("{} {}", r.status, r.reason))
            .and_then(|got| match &t.answer {
                Some(want) if same(&Json::String(got.clone()), want) => Ok(()),
                _ => Err(format!(
                    "{source}: it answered {got:?}, and the recording has another outcome"
                )),
            })
    } else if let Some(d) = &t.delivery {
        delivery(trigger, &credential, &input, d, &t, &workflow, source)
    } else if t.register || t.unregister || t.renew {
        let address = t
            .address
            .clone()
            .unwrap_or_else(|| "https://studio.example/hooks/recorded".to_owned());
        let target = super::hook::Target {
            name: "recorded",
            workflow: &workflow,
            address: &address,
            secret: Some("recorded-secret"),
        };
        let done = if t.register {
            // messaging: at the recording's time, so what the requests send of `input.now` is the same each run.
            at_once(super::hook::register_at(
                &played,
                app,
                trigger,
                &input,
                &target,
                RECORDED_AT,
            ))?
            .map(|_| ())
        } else if t.renew {
            at_once(super::hook::renew_at(
                &played,
                app,
                trigger,
                &input,
                "recorded",
                &workflow,
                RECORDED_AT,
            ))?
        } else {
            at_once(super::hook::unregister(
                &played, app, trigger, &input, "recorded", &workflow, &address,
            ))?
        };
        done.and_then(|()| {
            let Some(want) = &t.kept else { return Ok(()) };
            let got: BTreeMap<String, String> = played
                .kept
                .lock()
                .map_err(|_| "poisoned")?
                .iter()
                .map(|(k, v)| (k.trim_end_matches(&format!("@{workflow}")).to_owned(), v.clone()))
                .collect();
            if &got != want {
                return Err(format!(
                    "{source}: kept: it kept {got:?}, the recording has {want:?}"
                ));
            }
            Ok(())
        })
    } else {
        polled(app, trigger, &credential, &input, &t, &played, &workflow, source)
    };
    if let Some(e) = played.differs.lock().map_err(|_| "poisoned")?.take() {
        return Err(e);
    }
    let left = played.exchanges.lock().map_err(|_| "poisoned")?.len();
    match (outcome, &t.refused) {
        (Err(e), Some(want)) if e.contains(want.as_str()) => Ok(()),
        (Err(e), Some(want)) => Err(format!(
            "{source}: it was refused \"{e}\", the recording says \"{want}\""
        )),
        (Err(e), None) => Err(if e.starts_with(source) {
            e
        } else {
            format!("{source}: {e}")
        }),
        (Ok(()), Some(want)) => Err(format!(
            "{source}: it went through; the recording says it is refused (\"{want}\")"
        )),
        (Ok(()), None) if left > 0 => Err(format!("{source}: {left} recorded exchanges were not asked for")),
        (Ok(()), None) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn polled(
    app: &AppDef,
    trigger: &super::triggers::TriggerDef,
    credential: &Credential,
    input: &Json,
    t: &Recorded,
    played: &Played,
    workflow: &str,
    source: &str,
) -> Result<(), String> {
    let polling = Polling::new(
        app,
        trigger,
        credential.clone(),
        input,
        workflow,
        t.seen.as_ref(),
        t.max_items.unwrap_or(super::poll::POLL_ITEMS),
        1_700_000_000_000,
    )?;
    let found = at_once(super::poll::poll(played, polling, "recorded", 1))??;
    if let Some(want) = &t.runs {
        let got: Vec<Json> = found.runs.iter().map(|(_, v)| as_json(v)).collect();
        if got.len() != want.len() || !got.iter().zip(want).all(|(g, w)| same(g, w)) {
            return Err(format!(
                "{source}: runs: it started {}, the recording has {}",
                Json::Array(got),
                Json::Array(want.clone())
            ));
        }
    }
    if let Some(want) = &t.ids {
        let got: Vec<&str> = found.runs.iter().map(|(id, _)| id.as_str()).collect();
        if got != *want {
            return Err(format!(
                "{source}: ids: it ran {got:?}, the recording has {want:?}"
            ));
        }
    }
    if let Some(want) = &t.cursor
        && !same(&found.seen.cursor, want)
    {
        return Err(format!(
            "{source}: cursor: it keeps {}, the recording has {want}",
            found.seen.cursor
        ));
    }
    Ok(())
}

fn delivery(
    trigger: &super::triggers::TriggerDef,
    credential: &Credential,
    input: &Json,
    d: &DeliveryFile,
    t: &Recorded,
    workflow: &str,
    source: &str,
) -> Result<(), String> {
    let hook = trigger
        .hook()
        .ok_or_else(|| format!("{source}: delivery: this trigger polls"))?;
    let headers: Vec<(String, String)> = d
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect();
    let content_type = d
        .content_type
        .clone()
        .unwrap_or_else(|| "application/json".to_owned());
    let delivery = Delivery {
        method: &d.method,
        address: &d.address,
        headers: &headers,
        query: &d.query,
        content_type: &content_type,
        body: d.body.as_bytes(),
    };
    let secret = super::hook::secret_of(hook, credential, workflow);
    let verdict = super::hook::check(
        trigger,
        secret.as_deref(),
        input,
        &delivery,
        d.at.saturating_mul(1000),
    )
    .map_err(|r| format!("{} {}", r.status, r.reason))?;
    match verdict {
        Verdict::Run(runs) => {
            // messaging: what the app is answered with, when the trigger says.
            let replied = super::hook::answer_for(trigger, input, &delivery)
                .map_err(|r| format!("{} {}", r.status, r.reason))?;
            if let Some(want) = &t.answer
                && !replied.as_ref().is_some_and(|got| same(got, want))
            {
                return Err(format!(
                    "{source}: answer: it answered {}, the recording has {want}",
                    replied.unwrap_or(Json::Null)
                ));
            }
            let got: Vec<Json> = runs.iter().map(|(_, v)| as_json(v)).collect();
            match &t.runs {
                Some(want) if got.len() == want.len() && got.iter().zip(want).all(|(g, w)| same(g, w)) => {}
                Some(want) => {
                    return Err(format!(
                        "{source}: runs: it started {}, the recording has {}",
                        Json::Array(got),
                        Json::Array(want.clone())
                    ));
                }
                None if t.passed || (t.answer.is_some() && replied.is_none()) => {
                    return Err(format!(
                        "{source}: it started {} runs; the recording says none",
                        got.len()
                    ));
                }
                None => {}
            }
            if let Some(want) = &t.ids {
                let got: Vec<String> = runs
                    .iter()
                    .map(|(id, _)| id.clone().unwrap_or_default())
                    .collect();
                if got != *want {
                    return Err(format!(
                        "{source}: ids: it gave {got:?}, the recording has {want:?}"
                    ));
                }
            }
            Ok(())
        }
        Verdict::Answer(a) => match &t.answer {
            Some(want) if same(&a, want) => Ok(()),
            _ => Err(format!(
                "{source}: it answered {a}, and the recording has another outcome"
            )),
        },
        Verdict::Pass(why) if t.passed => {
            let _ = why;
            Ok(())
        }
        Verdict::Pass(why) => Err(format!(
            "{source}: it was passed ({why}), and the recording has another outcome"
        )),
    }
}
