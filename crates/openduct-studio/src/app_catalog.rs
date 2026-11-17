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

pub(crate) async fn list(state: &State, call: &Call) -> Answer {
    if let Err(answer) = signed_in(state, call).await {
        return answer;
    }
    let apps: Vec<Value> = openduct_flow::app_files::all()
        .into_iter()
        .map(|a| {
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
            })
        })
        .collect();
    json(StatusCode::OK, &json!({ "apps": apps }))
}
