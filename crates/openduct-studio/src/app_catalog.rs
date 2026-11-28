// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The apps workflows may use (`GET /api/apps`): each with what it is, how a credential of it signs in, and its
//! actions and their inputs - what the editor makes each app's step and form from, so a new app needs no code here.

use hyper::StatusCode;
use serde_json::{Value, json};

use crate::State;
use crate::api::signed_in;
use crate::credentials::registration;
use crate::http::{Answer, Call, json};

/// An app's triggers as the editor shows them: each one's inputs, whether it polls (and how often by default) or is
/// sent deliveries (and whether turning the workflow on registers its address, or what to do in the app instead).
fn triggers_of(a: &openduct_flow::app_files::AppDef) -> Vec<Value> {
    a.triggers
        .iter()
        .map(|(name, t)| {
            let inputs: Vec<Value> = t
                .inputs
                .iter()
                .map(|i| json!({ "name": i.name, "label": i.label, "required": i.required, "description": i.description, "kind": i.kind }))
                .collect();
            let (kind, every, registers, setup) = match (t.polls(), t.hook()) {
                (Some(p), _) => ("poll", u64::try_from(p.every.as_millis()).ok(), false, String::new()),
                (None, Some(h)) => ("webhook", None, !h.register.is_empty(), h.setup.clone()),
                (None, None) => ("webhook", None, false, String::new()),
            };
            json!({
                "name": name, "label": t.label, "description": t.description, "inputs": inputs, "kind": kind,
                "every_ms": every, "registers": registers, "setup": setup,
            })
        })
        .collect()
}

/// An app as the editor's catalog shows it.
pub(crate) fn shown(state: &State, a: &openduct_flow::app_files::AppDef) -> Value {
    let sign_ins: Vec<Value> = a
        .sign_ins
        .iter()
        .map(|s| {
            let connect = s
                .oauth2
                .as_ref()
                .is_some_and(|o| o.grant == openduct_flow::app_files::Grant::AuthorizationCode);
            json!({ "id": s.id, "label": s.label, "connect": connect })
        })
        .collect();
    let actions: Vec<Value> = a
        .actions
        .iter()
        .map(|(name, x)| {
            let inputs: Vec<Value> = x
                .inputs
                .iter()
                .map(|i| {
                    json!({ "name": i.name, "label": i.label, "required": i.required, "description": i.description, "kind": i.kind })
                })
                .collect();
            json!({
                "name": name, "label": x.label, "description": x.description, "write": x.write,
                "list": x.items.is_some(), "inputs": inputs,
            })
        })
        .collect();
    json!({
        "id": a.id, "name": a.name, "category": a.category, "description": a.description, "docs": a.docs,
        "default_action": a.default_action, "sign_ins": sign_ins, "actions": actions,
        "registered": registration(&state.vault, &a.id).is_some(),
        "triggers": triggers_of(a), // G3 app triggers
    })
}

pub(crate) async fn list(state: &State, call: &Call) -> Answer {
    if let Err(answer) = signed_in(state, call).await {
        return answer;
    }
    let mut apps: Vec<Value> = openduct_flow::app_files::all()
        .into_iter()
        .map(|a| shown(state, a))
        .collect();
    apps.extend(crate::custom_apps::catalog(state, call).await); // app generator: the workspace's own apps
    json(StatusCode::OK, &json!({ "apps": apps }))
}
