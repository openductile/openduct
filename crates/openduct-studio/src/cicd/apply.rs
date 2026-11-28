// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Pull, push and rollback: a workspace's workflows given as files, a folder's files planned against them
//! ([`super::plan`]) and, unless only a dry run is asked for, saved - each new or changed one as a new version, each
//! turned on or off as the plan says. Every check is made before anything is written; should a write fail part way
//! (the broker gone), the answer says which workflows were saved and which were not.

use std::collections::HashSet;

use hyper::StatusCode;
use serde_json::{Value, json};

use super::plan::{Action, Current, Incoming, Inventory, Options, Planned, plan, summary};
use super::{VersionSource, made_by};
use crate::State;
use crate::api::{Access, access};
use crate::api_tokens::token_of;
use crate::http::{Answer, Call, json, problem};
use crate::model::{PRODUCTION, Role, SCHEMA, Version, WorkflowRecord, now, version_key};
use crate::workflow_files::{self, in_scope, rename_workflows};

/// How a version came to be: `push`, `promotion`, `rollback`, `git`, with more words for the history.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    pub kind: &'static str,
    pub detail: Option<String>,
    /// Each version's words are where its workflow came from (a promotion's `Test v4`): `from <file>`.
    pub from_file: bool,
}

/// The workflow scopes of the token a request carries; `None` for a person, or a token of every workflow.
pub(crate) async fn scopes_of(state: &State, access: &Access) -> Option<Vec<String>> {
    token_of(state, access).await.and_then(|t| t.workflows)
}

/// A workspace's workflows now, oldest first, each with its newest definition.
pub(crate) async fn current(state: &State, workspace: &str) -> Vec<(WorkflowRecord, Current)> {
    let _ = state.model.workflows.refresh().await;
    let _ = state.model.definitions.refresh().await;
    let mut records: Vec<WorkflowRecord> = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .filter(|w| w.workspace == workspace && w.deleted.is_none())
        .collect();
    records.sort_by(|a, b| a.created.cmp(&b.created).then_with(|| a.id.cmp(&b.id)));
    let mut out = Vec::new();
    for w in records {
        let definition = crate::workflows::definition_of(state, &w).await;
        let c = Current {
            id: w.id.clone(),
            name: w.name.clone(),
            version: w.version,
            active: w.active,
            definition,
        };
        out.push((w, c));
    }
    out
}

/// The credentials, lookup files and data tables a workspace holds, by name.
pub(crate) async fn inventory(state: &State, workspace: &str) -> Inventory {
    let _ = state.model.credentials.refresh().await;
    let _ = state.model.files.refresh().await;
    let credentials = state
        .model
        .credentials
        .all()
        .await
        .into_iter()
        .map(|(_, c)| c)
        .filter(|c| c.workspace == workspace && c.environment == PRODUCTION && c.deleted.is_none())
        .map(|c| c.name)
        .collect();
    let files = state
        .model
        .files
        .all()
        .await
        .into_iter()
        .map(|(_, f)| f)
        .filter(|f| f.workspace == workspace && f.deleted.is_none())
        .map(|f| f.path)
        .collect();
    let tables = state
        .tables
        .list(workspace)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(def, _, _)| def.name)
        .collect();
    Inventory {
        credentials,
        files,
        tables,
    }
}

/// A definition as a file gives it: the other workflows it names by their names here, not their ids.
pub(crate) fn portable(definition: &Value, all: &[(WorkflowRecord, Current)]) -> Value {
    let mut out = definition.clone();
    rename_workflows(&mut out, &|w| {
        all.iter().find(|(r, _)| r.id == w).map(|(r, _)| r.name.clone())
    });
    out
}

/// Every workflow of the workspace (those the token reaches) as files: `GET /api/cicd/workflows`.
pub(crate) async fn pull(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let scopes = scopes_of(state, &access).await;
    let all = current(state, &access.workspace.id).await;
    let mut taken = HashSet::new();
    let mut out = Vec::new();
    for (record, c) in &all {
        if !in_scope(scopes.as_deref(), &record.name) {
            continue;
        }
        let definition = portable(&c.definition, &all);
        out.push(json!({
            "id": record.id, "name": record.name, "version": record.version, "active": record.active,
            "file": workflow_files::file_name(&record.name, &mut taken),
            "definition": definition,
        }));
    }
    let delivery = state.cicd.delivery(&access.workspace.id).await;
    json(
        StatusCode::OK,
        &json!({
            "workspace": { "id": access.workspace.id, "name": access.workspace.name, "environment": delivery.environment },
            "workflows": out,
        }),
    )
}

/// What a push request sends: its files, read.
fn incoming_of(body: &Value) -> Result<Vec<Incoming>, Answer> {
    let Some(files) = body.get("workflows").and_then(Value::as_array) else {
        return Err(problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "workflows: the files pushed, each {file, definition, on}",
        ));
    };
    if files.len() > workflow_files::MAX_FILES {
        return Err(problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("at most {} workflows a push", workflow_files::MAX_FILES),
        ));
    }
    let mut out = Vec::new();
    for (n, f) in files.iter().enumerate() {
        let file = f
            .get("file")
            .and_then(Value::as_str)
            .map(|s| s.chars().take(200).collect::<String>())
            .unwrap_or_else(|| format!("workflow {}", n + 1));
        let Some(definition) = f.get("definition").filter(|d| d.is_object()) else {
            return Err(problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!("{file}: definition: the workflow"),
            ));
        };
        let on = match f.get("on") {
            None | Some(Value::Null) => None,
            Some(Value::Bool(b)) => Some(*b),
            Some(_) => {
                return Err(problem(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("{file}: on: true or false"),
                ));
            }
        };
        out.push(Incoming {
            file,
            definition: definition.clone(),
            on,
        });
    }
    Ok(out)
}

/// `POST /api/cicd/push`: `{workflows: [{file, definition, on}], activate, prune, dry_run}`.
pub(crate) async fn push(state: &State, call: &Call) -> Answer {
    let flag = |name: &str| call.body.get(name).and_then(Value::as_bool).unwrap_or(false);
    let dry_run = flag("dry_run");
    let access = match access(state, call, if dry_run { Role::Viewer } else { Role::Editor }).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let incoming = match incoming_of(&call.body) {
        Ok(i) => i,
        Err(answer) => return answer,
    };
    let options = Options {
        activate: flag("activate"),
        prune: flag("prune"),
        scopes: scopes_of(state, &access).await,
    };
    let source = Source {
        kind: "push",
        detail: None,
        from_file: false,
    };
    planned_and_applied(state, call, &access, &incoming, &options, &source, dry_run).await
}

/// A plan of `incoming` in the asker's workspace, applied unless `dry_run`: the answer of a push, a promotion and a
/// deploy from Git.
pub(crate) async fn planned_and_applied(
    state: &State,
    call: &Call,
    access: &Access,
    incoming: &[Incoming],
    options: &Options,
    source: &Source,
    dry_run: bool,
) -> Answer {
    let (status, body) = plan_and_apply(state, call, access, incoming, options, source, dry_run).await;
    json(status, &body)
}

pub(crate) async fn plan_and_apply(
    state: &State,
    call: &Call,
    access: &Access,
    incoming: &[Incoming],
    options: &Options,
    source: &Source,
    dry_run: bool,
) -> (StatusCode, Value) {
    let all = current(state, &access.workspace.id).await;
    let here: Vec<Current> = all.iter().map(|(_, c)| c.clone()).collect();
    let inventory = inventory(state, &access.workspace.id).await;
    let planned = match plan(&here, incoming, &inventory, options) {
        Ok(p) => p,
        Err(refusal) => return (StatusCode::UNPROCESSABLE_ENTITY, refusal.body()),
    };
    let shown = json!({ "workflows": planned, "summary": summary(&planned), "dry_run": dry_run });
    if dry_run {
        return (StatusCode::OK, shown);
    }
    let _one = state.cicd.deploying.lock().await;
    let (applied, failed) = apply(state, call, access, &all, &planned, source).await;
    state.manager.sync(state).await;
    let mut body = shown;
    if let Some(fields) = body.as_object_mut() {
        fields.insert("applied".to_owned(), json!(applied));
    }
    match failed {
        None => {
            tracing::info!(workspace = %access.workspace.id, by = %access.user.email, source = source.kind,
                saved = applied.len(), "workflows pushed");
            (StatusCode::OK, body)
        }
        Some((name, why)) => {
            let not: Vec<&String> = planned
                .iter()
                .map(|p| &p.name)
                .filter(|n| !applied.contains(n))
                .collect();
            if let Some(fields) = body.as_object_mut() {
                fields.insert("not_applied".to_owned(), json!(not));
                fields.insert(
                    "error".to_owned(),
                    json!(format!(
                        "{name} could not be saved ({why}): the workflows before it were, those after it were not"
                    )),
                );
            }
            (StatusCode::SERVICE_UNAVAILABLE, body)
        }
    }
}

/// The plan written, workflow by workflow, in its order: those saved, and the first that failed with why.
async fn apply(
    state: &State,
    call: &Call,
    access: &Access,
    all: &[(WorkflowRecord, Current)],
    planned: &[Planned],
    source: &Source,
) -> (Vec<String>, Option<(String, String)>) {
    let (by, token) = made_by(state, access).await;
    let mut applied = Vec::new();
    for p in planned {
        if !super::plan::changes_anything(p) {
            continue;
        }
        let existing = all.iter().find(|(r, _)| r.id == p.id).map(|(r, _)| r.clone());
        match one(state, call, access, existing, p, source, &by, token.as_ref()).await {
            Ok(()) => applied.push(p.name.clone()),
            Err(why) => return (applied, Some((p.name.clone(), why))),
        }
    }
    (applied, None)
}

#[allow(clippy::too_many_arguments)]
async fn one(
    state: &State,
    call: &Call,
    access: &Access,
    existing: Option<WorkflowRecord>,
    p: &Planned,
    source: &Source,
    by: &str,
    token: Option<&(String, String)>,
) -> Result<(), String> {
    let mut record = match existing {
        Some(r) => r,
        None => WorkflowRecord {
            schema: SCHEMA,
            id: p.id.clone(),
            workspace: access.workspace.id.clone(),
            environment: PRODUCTION.to_owned(),
            name: p.name.clone(),
            version: 0,
            hash: String::new(),
            active: false,
            hook: crate::model::new_id().replace('-', ""),
            failed_from: 0,
            created: now(),
            updated: now(),
            by: access.user.id.clone(),
            deleted: None,
        },
    };
    if let Some(definition) = &p.definition
        && matches!(p.action, Action::Create | Action::Update)
    {
        let hash = state
            .model
            .keep_definition(&access.workspace.id, definition)
            .await?;
        record.version += 1;
        record.hash = hash.clone();
        let version = Version {
            schema: SCHEMA,
            workflow: record.id.clone(),
            number: record.version,
            hash,
            created: now(),
            by: access.user.id.clone(),
        };
        let key = version_key(&record.id, record.version);
        state.model.versions.put(&key, &version).await?;
        let kept = VersionSource {
            schema: SCHEMA,
            workflow: record.id.clone(),
            number: record.version,
            source: source.kind.to_owned(),
            token: token.map(|(name, _)| name.clone()),
            token_id: token.map(|(_, id)| id.clone()),
            by: by.to_owned(),
            detail: if source.from_file {
                p.file.as_ref().map(|f| format!("from {f}"))
            } else {
                source.detail.clone()
            },
            at: now(),
        };
        state.cicd.sources.put(&key, &kept).await?;
    }
    if record.active != p.on {
        // An app's trigger: its address registered with the app as it is turned on, taken away as it is off.
        crate::app_hooks::turned(state, call, &record, p.on)
            .await
            .map_err(|_| "the app did not take the workflow's address".to_owned())?;
    }
    record.active = p.on;
    record.name = p.name.clone();
    record.updated = now();
    state.model.workflows.put(&record.id.clone(), &record).await
}

/// `POST /api/cicd/rollback` `{name, to}`: the version `to` (or the one before the newest) saved again as the newest.
pub(crate) async fn rollback(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let name = call
        .body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if !in_scope(scopes_of(state, &access).await.as_deref(), &name) {
        return problem(StatusCode::FORBIDDEN, "outside the workflows this token reaches");
    }
    let all = current(state, &access.workspace.id).await;
    let found: Vec<&(WorkflowRecord, Current)> = all.iter().filter(|(r, _)| r.name == name).collect();
    let [(record, now_at)] = found.as_slice() else {
        return if found.is_empty() {
            problem(
                StatusCode::NOT_FOUND,
                &format!("there is no workflow called {name:?} here"),
            )
        } else {
            problem(
                StatusCode::CONFLICT,
                &format!("several workflows are called {name:?}: rename all but one"),
            )
        };
    };
    let to = match call.body.get("to") {
        None | Some(Value::Null) => record.version.saturating_sub(1),
        Some(v) => match v.as_u64() {
            Some(v) => v,
            None => return problem(StatusCode::UNPROCESSABLE_ENTITY, "to: a version's number"),
        },
    };
    if to == 0 || to >= record.version {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "to: a version before the newest, v1 to v{}",
                record.version.saturating_sub(1)
            ),
        );
    }
    let _ = state.model.versions.refresh().await;
    let Some(version) = state.model.versions.get(&version_key(&record.id, to)).await else {
        return problem(StatusCode::NOT_FOUND, &format!("{name} has no version {to}"));
    };
    let Some(definition) = state.model.definition(&access.workspace.id, &version.hash).await else {
        return problem(
            StatusCode::NOT_FOUND,
            &format!("version {to} of {name} has no definition"),
        );
    };
    let planned = Planned {
        name: record.name.clone(),
        file: None,
        action: if definition == now_at.definition {
            Action::Unchanged
        } else {
            Action::Update
        },
        id: record.id.clone(),
        from_version: Some(record.version),
        to_version: Some(record.version + 1),
        was_on: record.active,
        on: record.active,
        changes: workflow_files::changes(&now_at.definition, &definition),
        definition: Some(definition),
    };
    if planned.action == Action::Unchanged {
        return json(
            StatusCode::OK,
            &json!({ "name": name, "version": record.version, "unchanged": true, "rolled_back_to": to }),
        );
    }
    let source = Source {
        kind: "rollback",
        detail: Some(format!("to v{to}")),
        from_file: false,
    };
    let (by, token) = made_by(state, &access).await;
    let outcome = one(
        state,
        call,
        &access,
        Some(record.clone()),
        &planned,
        &source,
        &by,
        token.as_ref(),
    )
    .await;
    state.manager.sync(state).await;
    match outcome {
        Ok(()) => {
            tracing::info!(workspace = %access.workspace.id, workflow = %name, to, "a workflow rolled back");
            json(
                StatusCode::OK,
                &json!({ "name": name, "version": record.version + 1, "rolled_back_to": to, "changes": planned.changes }),
            )
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}
