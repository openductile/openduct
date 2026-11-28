// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The work of a step that runs an app's action (see [`crate::app_files`]): its request sent through the run's
//! effects - so a preview shows a write and does not send it, and the private-network rule holds - and its answer
//! read; a list action page after page, up to its most.

use openduct_expr::{Env, Record, Value};

use super::outside::Effects;
use crate::apps::Credential;
use crate::model::Defined;

/// A fresh token for an OAuth sign-in's credential, kept in it: refreshed (a person's sign-in) or fetched (client
/// credentials).
async fn fresh(effects: &dyn Effects, d: &Defined, credential: &Credential) -> Result<Credential, String> {
    let sign_in = d
        .app
        .sign_in(&credential.kind)
        .ok_or_else(|| format!("{} has no sign-in {}", d.app.name, credential.kind))?;
    let registration = effects.registration(&d.app.id);
    let request = crate::app_files::oauth::refresh(sign_in, credential, registration.as_ref())
        .map_err(|e| format!("{}: {e}", d.credential))?;
    let answer = effects.http(request).await?;
    let tokens =
        crate::app_files::oauth::tokens(&answer, credential).map_err(|e| format!("{}: {e}", d.credential))?;
    effects.keep_credential(&d.credential, tokens.clone()).await?;
    let mut updated = credential.clone();
    updated.fields.extend(tokens);
    Ok(updated)
}

fn status_of(answer: &Value) -> u64 {
    match answer {
        Value::Record(r) => r
            .get("status")
            .and_then(|s| crate::template::as_text(s).parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// The step's result: what the action gave, the records of every page of a list, or (a preview) what would be sent.
pub(super) async fn run(effects: &dyn Effects, d: &Defined, env: &Env<'_>) -> Result<Value, String> {
    let mut credential = effects.credential(&d.credential)?;
    if credential.kind != "test" && d.app.sign_in(&credential.kind).is_none() {
        let kinds: Vec<&str> = d.app.sign_ins.iter().map(|s| s.id.as_str()).collect();
        return Err(format!(
            "{} is a {} credential: {} takes {}",
            d.credential,
            credential.kind,
            d.app.name,
            kinds.join(" or ")
        ));
    }
    let (_, action) = d
        .app
        .action(Some(&d.action))
        .ok_or_else(|| format!("{} has no action {}", d.app.name, d.action))?;
    let mut inputs = Record::new();
    for (name, tree) in &d.inputs {
        inputs.insert(
            name.clone(),
            super::tree(tree, env).map_err(|e| format!("{name}: {e}"))?,
        );
    }
    let inputs = Value::record(inputs);
    let oauth = d.app.sign_in(&credential.kind).filter(|s| s.oauth2.is_some());
    if let Some(sign_in) = oauth
        && crate::app_files::oauth::wanted(sign_in, &credential, false)
    {
        credential = fresh(effects, d, &credential).await?;
    }
    // app generator: an action that calls a tool of the app's MCP server, not a request of its own.
    if action.mcp.is_some() {
        return crate::app_files::mcp_action::run(effects, d.app, action, &credential, &inputs).await;
    }
    let mut page = Value::Null;
    let mut items: Vec<Value> = Vec::new();
    for _ in 0..action.max_pages {
        let mut request = crate::app_files::request(d.app, action, &credential, &inputs, &page)?;
        crate::app_files::follow_ups::load_file_body(effects, &mut request).await?; // messaging
        let mut answer = effects.http(request).await?;
        // A token the vendor no longer takes: a fresh one, and the request once more.
        if status_of(&answer) == 401
            && let Some(sign_in) = oauth
            && crate::app_files::oauth::wanted(sign_in, &credential, true)
        {
            credential = fresh(effects, d, &credential).await?;
            let again = crate::app_files::request(d.app, action, &credential, &inputs, &page)?;
            answer = effects.http(again).await?;
        }
        // A test run answers with what it would have sent.
        if let Value::Record(r) = &answer
            && r.get("test").is_some()
        {
            return Ok(answer);
        }
        let got = crate::app_files::answer(d.app, action, &credential, &answer, &page)?;
        if action.items.is_none() && !action.then.is_empty() {
            return follow_up(effects, d, action, &credential, &inputs, got.result).await; // messaging
        }
        if action.items.is_none() {
            return Ok(got.result);
        }
        items.extend(got.items);
        if matches!(got.next, Value::Null) {
            break;
        }
        page = got.next;
    }
    Ok(Value::list(items))
}

// messaging
/// The requests that follow an action's own, in order, each reading `input.previous` (what the one before gave):
/// the last one's result. A test run stops at the first, answered with what it would send.
async fn follow_up(
    effects: &dyn Effects,
    d: &Defined,
    action: &crate::app_files::Action,
    credential: &Credential,
    inputs: &Value,
    first: Value,
) -> Result<Value, String> {
    let mut answers = vec![first];
    for next in &action.then {
        let given = crate::app_files::follow_ups::with_previous(inputs, &answers);
        let mut request = crate::app_files::request(d.app, next, credential, &given, &Value::Null)?;
        crate::app_files::follow_ups::load_file_body(effects, &mut request).await?;
        let answer = effects.http(request).await?;
        if let Value::Record(r) = &answer
            && r.get("test").is_some()
        {
            return Ok(answer);
        }
        answers.push(crate::app_files::answer(d.app, next, credential, &answer, &Value::Null)?.result);
    }
    Ok(answers.pop().unwrap_or(Value::Null))
}
