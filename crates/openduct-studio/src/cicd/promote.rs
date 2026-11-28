// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Promotion: workflows copied from one environment to the next (development to test, test to production) - each
//! the newest version of its definition, saved as a new version in the target, after the target is checked to have
//! every credential, lookup file, data table and other workflow it names. Nothing secret is copied: a credential is
//! named, never carried.
//!
//! `POST /api/cicd/promote` `{from, to, workflows, activate, dry_run}`: `from` and `to` are workspaces (by id or name),
//! one of them the request's own; `workflows` names those to promote, or is left out for all. A person needs a
//! viewer's role in the source and an editor's in the target; a token must be the target's, and its maker must see
//! the source. Both must be marked as environments, the source before the target.

use hyper::StatusCode;
use serde_json::{Value, json};

use super::apply::{Source, current, plan_and_apply, portable, scopes_of};
use super::plan::{Incoming, Options};
use crate::State;
use crate::api::{Access, access};
use crate::api_tokens::{by_token, person_of};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, Workspace};
use crate::workflow_files::in_scope;

/// The most workflows one promotion names.
const MAX_NAMED: usize = 500;

/// The workspace `named` (an id or a name) among those `person` sees, with the role there.
async fn find(state: &State, person: &crate::model::User, named: &str) -> Result<(Workspace, Role), Answer> {
    let mine = state.model.workspaces_of(person).await;
    if let Some(found) = mine.iter().find(|(w, _)| w.id == named) {
        return Ok(found.clone());
    }
    let by_name: Vec<&(Workspace, Role)> = mine.iter().filter(|(w, _)| w.name == named).collect();
    match by_name.as_slice() {
        [one] => Ok((*one).clone()),
        [] => Err(problem(
            StatusCode::NOT_FOUND,
            &format!("no workspace {named:?} you can see"),
        )),
        _ => Err(problem(
            StatusCode::CONFLICT,
            &format!("several workspaces are called {named:?}: give its id"),
        )),
    }
}

fn text<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

pub(crate) async fn promote(state: &State, call: &Call) -> Answer {
    let dry_run = call.body.get("dry_run").and_then(Value::as_bool).unwrap_or(false);
    let asked = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(person) = person_of(state, &asked).await else {
        return problem(StatusCode::UNAUTHORIZED, "the person this request is for is gone");
    };
    let here = asked.workspace.id.clone();
    let from = match text(&call.body, "from") {
        Some(f) => match find(state, &person, f).await {
            Ok(found) => found,
            Err(answer) => return answer,
        },
        None => (asked.workspace.clone(), asked.role),
    };
    let to = match text(&call.body, "to") {
        Some(t) => match find(state, &person, t).await {
            Ok(found) => found,
            Err(answer) => return answer,
        },
        None => (asked.workspace.clone(), asked.role),
    };
    if from.0.id == to.0.id {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "from and to: two different workspaces",
        );
    }
    if from.0.id != here && to.0.id != here {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "from or to: one of them is the workspace the request is for",
        );
    }
    // A token writes only in its own workspace.
    if by_token(&asked) && to.0.id != here {
        return problem(
            StatusCode::FORBIDDEN,
            &format!(
                "this token is for {}: promote with a token of {}",
                asked.workspace.name, to.0.name
            ),
        );
    }
    // The roles: a viewer's in the source, an editor's in the target (the token's own, where it is the target).
    let target_role = if to.0.id == here { asked.role } else { to.1 };
    let source_role = if from.0.id == here { asked.role } else { from.1 };
    if source_role < Role::Viewer || (!dry_run && target_role < Role::Editor) {
        return problem(
            StatusCode::FORBIDDEN,
            &format!("this needs an editor's role in {}", to.0.name),
        );
    }
    let (source_env, target_env) = (
        state.cicd.delivery(&from.0.id).await,
        state.cicd.delivery(&to.0.id).await,
    );
    let (Some(source_kind), Some(target_kind)) = (&source_env.environment, &target_env.environment) else {
        return problem(
            StatusCode::CONFLICT,
            "promotion is between workspaces marked as environments: mark both in Settings",
        );
    };
    if source_env.order >= target_env.order {
        return problem(
            StatusCode::CONFLICT,
            &format!(
                "promotion goes forward: {} ({source_kind}, {}) is not before {} ({target_kind}, {})",
                from.0.name, source_env.order, to.0.name, target_env.order
            ),
        );
    }
    let named: Option<Vec<String>> = match call.body.get("workflows") {
        None | Some(Value::Null) => None,
        Some(Value::Array(names)) if names.len() <= MAX_NAMED => Some(
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
        ),
        Some(_) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!("workflows: up to {MAX_NAMED} names, or none given for all"),
            );
        }
    };
    let target = Access {
        user: asked.user.clone(),
        workspace: to.0.clone(),
        role: target_role,
    };
    let scopes = scopes_of(state, &asked).await;
    let source_all = current(state, &from.0.id).await;
    let mut incoming = Vec::new();
    for (record, c) in &source_all {
        let wanted = named.as_ref().is_none_or(|n| n.contains(&record.name));
        if !wanted || (named.is_none() && !in_scope(scopes.as_deref(), &record.name)) {
            continue;
        }
        incoming.push(Incoming {
            file: format!("{} v{}", from.0.name, record.version),
            definition: portable(&c.definition, &source_all),
            on: None,
        });
    }
    if let Some(names) = &named
        && let Some(lost) = names
            .iter()
            .find(|n| !source_all.iter().any(|(r, _)| r.name == **n))
    {
        return problem(
            StatusCode::NOT_FOUND,
            &format!("{} has no workflow called {lost:?}", from.0.name),
        );
    }
    let options = Options {
        activate: call
            .body
            .get("activate")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        prune: false,
        scopes,
    };
    let source = Source {
        kind: "promotion",
        detail: None,
        from_file: true,
    };
    let (status, mut body) =
        plan_and_apply(state, call, &target, &incoming, &options, &source, dry_run).await;
    if let Some(fields) = body.as_object_mut() {
        fields.insert(
            "from".to_owned(),
            json!({ "id": from.0.id, "name": from.0.name, "environment": source_kind }),
        );
        fields.insert(
            "to".to_owned(),
            json!({ "id": to.0.id, "name": to.0.name, "environment": target_kind }),
        );
    }
    if status == StatusCode::OK && !dry_run {
        tracing::info!(from = %from.0.id, to = %to.0.id, by = %asked.user.email, promoted = incoming.len(),
            "workflows promoted");
    }
    json(status, &body)
}
