// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Workflows in a workspace: made, changed (each change saved a version, its definition kept once by its hash),
//! turned on and off, tried out on sample input, started by hand. Their history and what was set aside are in
//! [`crate::history`]; their webhooks in [`crate::hooks`]; the runners that run those that are on in
//! [`crate::manager`].

use hyper::{Method, StatusCode};
use openduct_expr::Value as Data;
use openduct_flow::runner;
use openduct_flow::{DryRun, Event, Trigger, Workflow};
use serde_json::{Value, json};

use crate::State;
use crate::api::{Access, access};
use crate::http::{Answer, Call, json, problem};
use crate::model::{PRODUCTION, Role, SCHEMA, Version, WorkflowRecord, new_id, now, version_key};

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::POST, ["hooks", hook, ..]) => crate::hooks::deliver(state, call, hook).await,
        (&Method::GET, ["hooks", hook, ..]) => crate::app_hooks::confirm(state, call, hook).await, // G3 app triggers
        (&Method::POST, ["api", "check"]) => check(state, call).await,
        // cron-schedules
        (&Method::POST, ["api", "schedule"]) => crate::schedules::next(state, call).await,
        (&Method::GET, ["api", "workflows"]) => list(state, call).await,
        (&Method::POST, ["api", "workflows"]) => create(state, call).await,
        (&Method::GET, ["api", "workflows", id]) => get(state, call, id).await,
        (&Method::PUT, ["api", "workflows", id]) => update(state, call, id).await,
        (&Method::DELETE, ["api", "workflows", id]) => delete(state, call, id).await,
        (&Method::GET, ["api", "workflows", id, "versions"]) => versions(state, call, id).await,
        (&Method::POST, ["api", "workflows", id, "active"]) => activate(state, call, id).await,
        (&Method::POST, ["api", "workflows", id, "test"]) => test(state, call, id).await,
        (&Method::POST, ["api", "workflows", id, "preview"]) => preview(state, call, id).await,
        (&Method::POST, ["api", "workflows", id, "run"]) => run_now(state, call, id).await,
        (&Method::GET, ["api", "workflows", id, "runs"]) => crate::history::runs(state, call, id).await,
        (&Method::GET, ["api", "workflows", id, "metrics"]) => crate::history::metrics(state, call, id).await,
        (&Method::GET, ["api", "workflows", id, "failed"]) => crate::history::failed(state, call, id).await,
        (&Method::POST, ["api", "workflows", id, "failed", "run"]) => {
            crate::history::run_failed(state, call, id).await
        }
        (&Method::GET, ["api", "triggers"]) => crate::manager::triggers(state, call).await,
        (&Method::POST, ["api", "workflows", id, "chat"]) => crate::chat::try_it(state, call, id).await, // chat trigger (G8)
        _ => return None,
    })
}

pub(crate) fn trigger_name(trigger: &Trigger) -> &'static str {
    match trigger {
        Trigger::Webhook { .. } => "webhook",
        Trigger::Every(_) => "schedule",
        Trigger::Calendar(_) => "schedule", // cron-schedules
        Trigger::Topic { .. } => "topic",
        Trigger::Manual => "manual",
        Trigger::Failed { .. } => "failed",
        Trigger::Chat(_) => "chat", // chat trigger (G8)
        // forms-and-resume
        Trigger::Form(_) => "form",
        Trigger::App(_) => "app", // G3 app triggers
        // sql databases
        Trigger::Rows(_) => "database",
        // storage
        Trigger::Storage(w) => w.kind(),
        Trigger::Feed(_) => "feed",       // web
        Trigger::Mailbox(_) => "mailbox", // messaging
    }
}

/// The definition a request sends, checked (with the workspace's custom apps in reach); the answer saying what is
/// wrong if it is not a workflow.
fn checked(workspace: &str, definition: Option<&Value>, id: &str) -> Result<Workflow, Answer> {
    let definition =
        definition.ok_or_else(|| problem(StatusCode::UNPROCESSABLE_ENTITY, "definition: the workflow"))?;
    crate::custom_apps::workflow(workspace, definition, Some(id)) // app generator
        .map_err(|e| problem(StatusCode::UNPROCESSABLE_ENTITY, &e))
}

/// The workflow `id` of the asker's workspace; a 404 for any other.
pub(crate) async fn workflow_in(state: &State, access: &Access, id: &str) -> Result<WorkflowRecord, Answer> {
    state
        .model
        .workflows
        .get(id)
        .await
        .filter(|w| w.workspace == access.workspace.id && w.deleted.is_none())
        .ok_or_else(|| problem(StatusCode::NOT_FOUND, "no such workflow"))
}

/// The definition of a workflow's newest version.
/// The workflows of one workspace, for its runs' run_workflow steps: found by id among that workspace's own.
#[derive(Debug)]
pub(crate) struct Definitions {
    pub(crate) model: std::sync::Arc<crate::model::Model>,
    pub(crate) workspace: String,
}

impl openduct_flow::Workflows for Definitions {
    fn definition<'a>(
        &'a self,
        id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<openduct_expr::Value, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let _ = self.model.workflows.refresh().await;
            let found = self
                .model
                .workflows
                .get(id)
                .await
                .filter(|w| w.workspace == self.workspace && w.deleted.is_none())
                .ok_or_else(|| format!("there is no workflow {id} in this workspace"))?;
            let definition = self
                .model
                .definition(&found.workspace, &found.hash)
                .await
                .ok_or_else(|| format!("workflow {id} has no definition"))?;
            Ok(openduct_expr::Value::from_json(&definition))
        })
    }
}

pub(crate) async fn definition_of(state: &State, workflow: &WorkflowRecord) -> Value {
    state
        .model
        .definition(&workflow.workspace, &workflow.hash)
        .await
        .unwrap_or(Value::Null)
}

async fn check(state: &State, call: &Call) -> Answer {
    let workspace = match access(state, call, Role::Editor).await {
        Ok(a) => a.workspace.id,
        Err(answer) => return answer,
    };
    match checked(&workspace, call.body.get("definition"), "check") {
        Ok(w) => json(
            StatusCode::OK,
            &json!({ "ok": true, "trigger": trigger_name(&w.trigger), "steps": w.steps.len() }),
        ),
        Err(answer) => answer,
    }
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let _ = state.model.workflows.refresh().await;
    let mut out = Vec::new();
    for (_, w) in state.model.workflows.all().await {
        if w.workspace != access.workspace.id || w.deleted.is_some() {
            continue;
        }
        let definition = definition_of(state, &w).await;
        let trigger = crate::custom_apps::workflow(&w.workspace, &definition, Some(&w.id))
            .map_or("unknown", |f| trigger_name(&f.trigger));
        out.push(json!({
            "id": w.id, "name": w.name, "active": w.active, "trigger": trigger, "version": w.version, "updated": w.updated,
        }));
    }
    json(StatusCode::OK, &json!({ "workflows": out, "role": access.role }))
}

/// A workflow as the API shows it: its record, the newest definition, and its webhook's address when it has one.
async fn shown(state: &State, w: &WorkflowRecord) -> Value {
    let definition = definition_of(state, w).await;
    let webhook = crate::custom_apps::workflow(&w.workspace, &definition, Some(&w.id)).is_ok_and(|f| {
        matches!(f.trigger, Trigger::Webhook { .. }) || crate::app_hooks::takes_deliveries(&f.trigger)
    }); // G3 app triggers
    // forms-and-resume: a form trigger's page.
    let form = crate::custom_apps::workflow(&w.workspace, &definition, Some(&w.id))
        .is_ok_and(|f| matches!(f.trigger, Trigger::Form(_)));
    json!({
        "id": w.id, "name": w.name, "active": w.active, "version": w.version, "definition": definition,
        "hook": webhook.then(|| format!("/hooks/{}", w.hook)),
        "chat": crate::chat::address(&definition, w), // chat trigger (G8)
        "form": form.then(|| format!("/forms/{}", w.hook)),
        "created": w.created, "updated": w.updated,
    })
}

async fn create(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = crate::cicd::studio_edit(state, &access).await {
        return answer; // developer CI/CD: a workspace that takes changes only by push or promotion
    }
    let id = new_id();
    let workflow = match checked(&access.workspace.id, call.body.get("definition"), &id) {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let definition = call.body.get("definition").cloned().unwrap_or(Value::Null);
    let hash = match state
        .model
        .keep_definition(&access.workspace.id, &definition)
        .await
    {
        Ok(h) => h,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let record = WorkflowRecord {
        schema: SCHEMA,
        id: id.clone(),
        workspace: access.workspace.id.clone(),
        environment: PRODUCTION.to_owned(),
        name: workflow.name.clone(),
        version: 1,
        hash: hash.clone(),
        active: false,
        hook: new_id().replace('-', ""),
        failed_from: 0,
        created: now(),
        updated: now(),
        by: access.user.id.clone(),
        deleted: None,
    };
    let version = Version {
        schema: SCHEMA,
        workflow: id.clone(),
        number: 1,
        hash,
        created: now(),
        by: access.user.id,
    };
    let kept = async {
        state.model.versions.put(&version_key(&id, 1), &version).await?;
        state.model.workflows.put(&id, &record).await
    };
    if let Err(e) = kept.await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(StatusCode::CREATED, &shown(state, &record).await)
}

async fn get(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    match workflow_in(state, &access, id).await {
        Ok(w) => json(StatusCode::OK, &shown(state, &w).await),
        Err(answer) => answer,
    }
}

/// A change: saved as a new version when the definition is not the newest one's.
async fn update(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = crate::cicd::studio_edit(state, &access).await {
        return answer; // developer CI/CD
    }
    let mut record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let workflow = match checked(&access.workspace.id, call.body.get("definition"), id) {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let definition = call.body.get("definition").cloned().unwrap_or(Value::Null);
    let hash = match state
        .model
        .keep_definition(&access.workspace.id, &definition)
        .await
    {
        Ok(h) => h,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    if hash != record.hash {
        record.version += 1;
        record.hash = hash.clone();
        let version = Version {
            schema: SCHEMA,
            workflow: id.to_owned(),
            number: record.version,
            hash,
            created: now(),
            by: access.user.id.clone(),
        };
        if let Err(e) = state
            .model
            .versions
            .put(&version_key(id, record.version), &version)
            .await
        {
            return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
        }
    }
    record.name = workflow.name;
    record.updated = now();
    save(state, &record).await
}

async fn save(state: &State, record: &WorkflowRecord) -> Answer {
    if let Err(e) = state.model.workflows.put(&record.id, record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    state.manager.sync(state).await;
    json(StatusCode::OK, &shown(state, record).await)
}

async fn delete(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = crate::cicd::studio_edit(state, &access).await {
        return answer; // developer CI/CD
    }
    let mut record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    // G3 app triggers: a workflow deleted takes its address away from the app, as one turned off does.
    let _ = crate::app_hooks::turned(state, call, &record, false).await;
    record.active = false;
    record.deleted = Some(now());
    if let Err(e) = state.model.workflows.put(id, &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    state.manager.sync(state).await;
    json(StatusCode::OK, &json!({ "deleted": id }))
}

async fn versions(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = workflow_in(state, &access, id).await {
        return answer;
    }
    let _ = state.model.versions.refresh().await;
    let mut out = Vec::new();
    for (_, v) in state.model.versions.all().await {
        if v.workflow != id {
            continue;
        }
        let definition = state.model.definition(&access.workspace.id, &v.hash).await;
        let source = crate::cicd::source_of(state, id, v.number).await; // developer CI/CD: push, promotion, Git
        out.push(json!({ "number": v.number, "hash": v.hash, "created": v.created, "by": v.by, "definition": definition, "source": source }));
    }
    out.reverse();
    json(StatusCode::OK, &json!({ "versions": out }))
}

async fn activate(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let active = call.body.get("active").and_then(Value::as_bool).unwrap_or(true);
    if active && let Err(answer) = checked(&record.workspace, Some(&definition_of(state, &record).await), id)
    {
        return answer;
    }
    // G3 app triggers: the address registered with the app as the workflow is turned on, taken away as it is off.
    if let Err(answer) = crate::app_hooks::turned(state, call, &record, active).await {
        return answer;
    }
    record.active = active;
    record.updated = now();
    save(state, &record).await
}

/// A run on sample input that sends nothing: the definition sent (the editor's, unsaved) or the newest kept.
async fn test(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let definition = match call.body.get("definition") {
        Some(d) => d.clone(),
        None => definition_of(state, &record).await,
    };
    let workflow = match checked(&record.workspace, Some(&definition), id) {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let input = call.body.get("input").cloned().unwrap_or(Value::Null);
    let event = Event {
        value: Data::from_json(&input),
        details: Data::from_json(&json!({ "trigger": "test", "at": runner::iso(now_ms()) })),
    };
    let test = DryRun {
        credentials: state.vault.masked(&access.workspace.id, PRODUCTION),
        files: Some(state.files_for(&access.workspace.id)),
        scripts: Some(state.scripts_for()),
        workflows: Some(state.workflows_for(&access.workspace.id)),
        tables: Some(state.tables_for(&access.workspace.id)), // data tables
    };
    let ran = openduct_flow::run(&workflow, &event, &test).await;
    json(StatusCode::OK, &json!({ "run": ran.to_json() }))
}

/// A preview: the workflow (the editor's definition, or the newest kept) on sample input or on the most recent real
/// event (`"from": "last"`), every step's output shown; reads done for real, writes only shown.
async fn preview(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let definition = match call.body.get("definition") {
        Some(d) => d.clone(),
        None => definition_of(state, &record).await,
    };
    let workflow = match checked(&record.workspace, Some(&definition), id) {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let (value, details) = if call.body.get("from").and_then(Value::as_str) == Some("last") {
        match crate::history::last_event(state, id).await {
            Some(found) => found,
            None => {
                return problem(
                    StatusCode::NOT_FOUND,
                    "this workflow has no real record yet: preview the sample input instead",
                );
            }
        }
    } else {
        let input = call.body.get("input").cloned().unwrap_or(Value::Null);
        (
            input,
            json!({ "trigger": "preview", "at": runner::iso(now_ms()) }),
        )
    };
    let live = match state.effects_for(&access.workspace.id) {
        Ok(e) => e,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let effects = openduct_flow::Preview::new(live);
    let event = Event {
        value: Data::from_json(&value),
        details: Data::from_json(&details),
    };
    let ran = openduct_flow::run(&workflow, &event, &effects).await;
    json(StatusCode::OK, &json!({ "run": ran.to_json(), "input": value }))
}

pub(crate) fn now_ms() -> i64 {
    i64::try_from(now()).unwrap_or(0) * 1000
}

async fn run_now(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    if !record.active {
        return problem(StatusCode::CONFLICT, "the workflow is off: turn it on to run it");
    }
    let input = call.body.get("input").cloned().unwrap_or(json!({}));
    let details = json!({ "trigger": "manual", "by": access.user.email, "at": runner::iso(now_ms()) });
    queue(state, id, &input, &details).await
}

/// An event written to the workflow's topic, for a run: kept before it is answered.
pub(crate) async fn queue(state: &State, id: &str, value: &Value, details: &Value) -> Answer {
    match queued(state, id, value, details).await {
        Ok(run) => json(StatusCode::ACCEPTED, &json!({ "accepted": true, "run": run })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// A run queued for workflow `id`: the id the run will have.
pub(crate) async fn queued(
    state: &State,
    id: &str,
    value: &Value,
    details: &Value,
) -> Result<String, String> {
    state
        .with_client(|client| {
            let (id, value, details) = (id.to_owned(), value.clone(), details.clone());
            Box::pin(async move {
                runner::prepare(client, &id).await?;
                runner::queue(client, &id, &value, &details).await
            })
        })
        .await
}
