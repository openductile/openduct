// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! App triggers in the studio. A workflow started by an app's webhook gets its deliveries at `/hooks/<hook>`: each is
//! checked against the app's signature with the secret the credential holds - unsigned, wrongly signed, too old and
//! seen-before deliveries are refused, and run nothing - then each event in it is kept for a run before it is
//! answered. Some apps check the address first with a GET (answered with their challenge), or a signed challenge
//! (answered as they ask). Where the app's API allows, turning the workflow on registers its address with the app
//! (what that gives, the signing secret, kept sealed in the credential for that workflow alone), and turning it off
//! takes it away. A workflow started by an app's poll is run by its runner (see `openduct_flow::runner`).

use std::sync::OnceLock;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::HeaderValue;
use hyper::{Response, StatusCode};
use openduct_flow::app_files::hook::{self, Delivery, Replays, Verdict};
use openduct_flow::runner;
use openduct_flow::{AppTrigger, Trigger};
use serde_json::{Value, json};

use crate::State;
use crate::http::{Answer, Call, json as answer_json, problem};
use crate::model::{PRODUCTION, WorkflowRecord};
use crate::workflows::{definition_of, now_ms, queued};

/// The deliveries this node took lately: one sent again is not run again.
fn replays() -> &'static Replays {
    static SEEN: OnceLock<Replays> = OnceLock::new();
    SEEN.get_or_init(Replays::default)
}

/// Whether `trigger` is one an app sends deliveries to (so the workflow has an address to show).
pub(crate) fn takes_deliveries(trigger: &Trigger) -> bool {
    matches!(trigger, Trigger::App(t) if t.definition().is_some_and(|(_, d)| d.hook().is_some()))
}

/// The whole address of a workflow's hook, as the app is given it and signs it.
fn address(state: &State, call: &Call, record: &WorkflowRecord) -> String {
    let scheme = if state.secure_cookies { "https" } else { "http" };
    format!("{scheme}://{}/hooks/{}", call.host, record.hook)
}

/// Headers a run is shown: not the signature, nor anything that signs in.
fn shown_headers(call: &Call, signature: &str) -> Value {
    let shown: serde_json::Map<String, Value> = call
        .headers
        .iter()
        .filter(|(k, _)| !matches!(k.as_str(), "authorization" | "cookie") && k != signature)
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    Value::Object(shown)
}

/// A delivery from an app to a workflow it starts.
pub(crate) async fn deliver(state: &State, call: &Call, record: &WorkflowRecord, t: &AppTrigger) -> Answer {
    let Some((app, def)) = t.definition() else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    let Some(hook_def) = def.hook() else {
        return problem(
            StatusCode::NOT_FOUND,
            "this workflow asks the app for what is new: it takes no deliveries",
        );
    };
    // messaging: an app checking the address while the workflow is being turned on (Microsoft Graph's
    // validationToken) is answered with what it asks back, before anything else.
    let checked_at = address(state, call, record);
    let check = Delivery {
        method: call.method.as_str(),
        address: &checked_at,
        headers: &call.headers,
        query: &call.query,
        content_type: &call.content_type,
        body: &call.raw,
    };
    if let Some(echoed) = hook::echo(def, &t.settings, &check) {
        return match echoed {
            Ok(text) => challenge(&Value::String(text)),
            Err(refused) => problem(StatusCode::BAD_REQUEST, &refused.reason),
        };
    }
    if !record.active {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "the workflow is off");
    }
    let Some(credential) = state.vault.get(&record.workspace, PRODUCTION, &t.credential) else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "the workflow's credential is missing",
        );
    };
    let secret = hook::secret_of(hook_def, &credential, &record.id);
    let address = address(state, call, record);
    let delivery = Delivery {
        method: call.method.as_str(),
        address: &address,
        headers: &call.headers,
        query: &call.query,
        content_type: &call.content_type,
        body: &call.raw,
    };
    let now = now_ms();
    let runs = match hook::check(def, secret.as_deref(), &t.settings, &delivery, now) {
        Err(refused) => {
            tracing::debug!(
                workflow = record.id,
                reason = refused.reason,
                "a delivery was refused"
            );
            let status = StatusCode::from_u16(refused.status).unwrap_or(StatusCode::UNAUTHORIZED);
            return problem(status, &refused.reason);
        }
        Ok(Verdict::Answer(answer)) => return challenge(&answer),
        Ok(Verdict::Pass(why)) => return answer_json(StatusCode::OK, &json!({ "passed": why })),
        Ok(Verdict::Run(runs)) => runs,
    };
    let headers = shown_headers(call, &hook_def.signature.header);
    let (mut started, mut repeated) = (Vec::new(), 0);
    for (id, event) in runs {
        if let Some(id) = &id
            && !replays().first_time(&format!("{}/{id}", record.id), now)
        {
            repeated += 1;
            continue;
        }
        let value: Value = serde_json::from_str(&event.to_json()).unwrap_or(Value::Null);
        let details = json!({
            "trigger": "app", "app": app.id, "when": t.when, "id": id, "at": runner::iso(now), "headers": headers,
        });
        match queued(state, &record.id, &value, &details).await {
            Ok(run) => started.push(run),
            Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
        }
    }
    // messaging: the answer the app asks for a delivery it sent (Discord's deferred reply), when the trigger says.
    if let Ok(Some(answer)) = hook::answer_for(def, &t.settings, &delivery) {
        return challenge(&answer);
    }
    if started.is_empty() && repeated > 0 {
        // Taken before: answered as then, so the app stops sending it, and not run again.
        return answer_json(StatusCode::OK, &json!({ "accepted": true, "repeated": repeated }));
    }
    answer_json(
        StatusCode::ACCEPTED,
        &json!({ "accepted": true, "runs": started, "repeated": repeated }),
    )
}

/// The answer to a challenge: text as text, anything else as JSON.
fn challenge(answer: &Value) -> Answer {
    let (body, kind) = match answer {
        Value::String(text) => (text.clone(), "text/plain; charset=utf-8"),
        other => (other.to_string(), "application/json"),
    };
    let mut response = Response::new(Full::new(Bytes::from(body)));
    response
        .headers_mut()
        .insert(hyper::header::CONTENT_TYPE, HeaderValue::from_static(kind));
    response
}

/// The workflow whose hook this is, and its app trigger.
async fn hooked(state: &State, hook: &str) -> Option<(WorkflowRecord, AppTrigger)> {
    let record = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .find(|w| w.hook == hook && w.deleted.is_none())?;
    let definition = definition_of(state, &record).await;
    match crate::custom_apps::workflow(&record.workspace, &definition, Some(&record.id))
        .ok()?
        .trigger
    {
        Trigger::App(t) => Some((record, t)),
        _ => None,
    }
}

/// A GET to a hook: an app checking the address before it sends deliveries (Meta's), answered with its challenge.
pub(crate) async fn confirm(state: &State, call: &Call, hook: &str) -> Answer {
    let Some((record, t)) = hooked(state, hook).await else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    let Some((_, def)) = t.definition() else {
        return problem(StatusCode::NOT_FOUND, "no such webhook");
    };
    let Some(credential) = state.vault.get(&record.workspace, PRODUCTION, &t.credential) else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "the workflow's credential is missing",
        );
    };
    match hook::confirm(def, &credential, &call.query) {
        Ok(text) => challenge(&Value::String(text)),
        Err(refused) => problem(
            StatusCode::from_u16(refused.status).unwrap_or(StatusCode::FORBIDDEN),
            &refused.reason,
        ),
    }
}

/// The workflow is being turned on (or off): its address registered with the app (or taken away), where the app's
/// API allows. A registration the app refuses keeps the workflow off, with the app's words.
pub(crate) async fn turned(
    state: &State,
    call: &Call,
    record: &WorkflowRecord,
    on: bool,
) -> Result<(), Answer> {
    let definition = definition_of(state, record).await;
    let Ok(workflow) = crate::custom_apps::workflow(&record.workspace, &definition, Some(&record.id)) else {
        return Ok(());
    };
    let Trigger::App(t) = &workflow.trigger else {
        return Ok(());
    };
    let Some((app, def)) = t.definition() else {
        return Ok(());
    };
    let Some(h) = def.hook() else {
        return Ok(());
    };
    if h.register.is_empty() || (on && record.active) {
        return Ok(());
    }
    let effects = state
        .effects_for(&record.workspace)
        .map_err(|e| problem(StatusCode::SERVICE_UNAVAILABLE, &e))?;
    let address = address(state, call, record);
    let done = if on {
        // One registered before (turned on, and off without taking it away) goes first: the app keeps one.
        let _ = hook::unregister(
            effects.as_ref(),
            app,
            def,
            &t.settings,
            &t.credential,
            &record.id,
            &address,
        )
        .await;
        hook::register(
            effects.as_ref(),
            app,
            def,
            &t.settings,
            &t.credential,
            &record.id,
            &address,
        )
        .await
        .map(|_| ())
    } else {
        hook::unregister(
            effects.as_ref(),
            app,
            def,
            &t.settings,
            &t.credential,
            &record.id,
            &address,
        )
        .await
    };
    match done {
        Ok(()) => Ok(()),
        Err(e) if on => Err(problem(
            StatusCode::BAD_GATEWAY,
            &format!(
                "{} did not take the workflow's address ({address}): {e}",
                app.name
            ),
        )),
        Err(e) => {
            // Off it goes whatever the app says: the address stays with it, and deliveries to it are refused.
            tracing::warn!(
                workflow = record.id,
                error = e,
                "the address could not be taken away from the app"
            );
            Ok(())
        }
    }
}
