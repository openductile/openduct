// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The runners: one kept going for every workflow that is on (its newest version), stopped when it is turned off,
//! changed or deleted, started again after a failure. Each runs with effects that open only its own workspace's
//! credentials and files, so a workflow of one client cannot use another's. The manager also keeps each workflow's
//! trigger record - its webhook's address, its schedule, its topic - so every trigger of a workspace can be listed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use openduct_flow::runner;
use openduct_flow::{Trigger, Workflow};
use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{PRODUCTION, Role, SCHEMA, TriggerRecord, WorkflowRecord, now};
use crate::workflows::trigger_name;

type Task = tokio::task::JoinHandle<()>;

struct Running {
    /// The version it runs: a new one starts it again.
    hash: String,
    stop: watch::Sender<bool>,
    tasks: Vec<Task>,
}

/// Keeps a runner going for every workflow that is on, and stops those turned off, changed or deleted.
#[derive(Default)]
pub(crate) struct Manager {
    running: Mutex<HashMap<String, Running>>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager").finish_non_exhaustive()
    }
}

/// The trigger record a workflow that is on has.
fn trigger_of(record: &WorkflowRecord, workflow: &Workflow, last_tick: Option<i64>) -> TriggerRecord {
    let (hook, every_ms, topic) = match &workflow.trigger {
        Trigger::Webhook { .. } => (Some(format!("/hooks/{}", record.hook)), None, None),
        Trigger::Every(every) => (None, u64::try_from(every.as_millis()).ok(), None),
        Trigger::Calendar(_) => (None, None, None), // cron-schedules
        // Shown as its workspace writes it: `orders`, not the broker's `w.<workspace>.orders`.
        Trigger::Topic { name, .. } => (
            None,
            None,
            Some(
                name.strip_prefix(&openduct_flow::workspace_prefix(&record.workspace))
                    .unwrap_or(name)
                    .to_owned(),
            ),
        ),
        Trigger::Manual => (None, None, None),
        Trigger::Chat(_) => (Some(format!("/chat/{}", record.hook)), None, None), // chat trigger (G8)
        // forms-and-resume
        Trigger::Form(_) => (Some(format!("/forms/{}", record.hook)), None, None),
        Trigger::Failed { workflow } => (
            None,
            None,
            Some(openduct_flow::runner::Topics::of(workflow).failed),
        ),
        // G3 app triggers: how often a poll asks, or the address an app sends its deliveries to.
        Trigger::App(t) => match t.every {
            Some(every) => (None, u64::try_from(every.as_millis()).ok(), None),
            None => (Some(format!("/hooks/{}", record.hook)), None, None),
        },
        Trigger::Rows(t) => (None, u64::try_from(t.every.as_millis()).ok(), None), // sql databases
        // storage: how often a folder is polled; a channel's messages come as they are published.
        Trigger::Storage(w) => (
            None,
            w.every().and_then(|e| u64::try_from(e.as_millis()).ok()),
            None,
        ),
        Trigger::Feed(t) => (None, u64::try_from(t.every().as_millis()).ok(), None), // web: how often a feed is asked
        Trigger::Mailbox(w) => (None, u64::try_from(w.every.as_millis()).ok(), None), // messaging
    };
    TriggerRecord {
        schema: SCHEMA,
        workflow: record.id.clone(),
        workspace: record.workspace.clone(),
        kind: trigger_name(&workflow.trigger).to_owned(),
        version: record.version,
        hook,
        every_ms,
        last_tick,
        topic,
        active: true,
        updated: now(),
    }
}

impl Manager {
    pub(crate) async fn sync(&self, state: &State) {
        if let Err(e) = state.model.refresh().await {
            tracing::warn!(error = e, "cannot read the studio's records");
            return;
        }
        crate::custom_apps::sync(state).await; // app generator: the custom apps, before the workflows that use them
        let mut wanted: HashMap<String, (WorkflowRecord, Workflow)> = HashMap::new();
        for (id, record) in state.model.workflows.all().await {
            if !record.active || record.deleted.is_some() {
                continue;
            }
            let definition = state
                .model
                .definition(&record.workspace, &record.hash)
                .await
                .unwrap_or(Value::Null);
            // app generator: read with the workspace's custom apps in reach.
            match crate::custom_apps::workflow(&record.workspace, &definition, Some(&id)) {
                Ok(mut w) => {
                    // A workflow reads its own workspace's topics, and watches only its own workspace's workflows.
                    match &mut w.trigger {
                        Trigger::Topic { name, .. } => {
                            *name = openduct_flow::workspace_topic(&record.workspace, name)
                        }
                        Trigger::Failed { workflow } => {
                            let same = state
                                .model
                                .workflows
                                .get(workflow)
                                .await
                                .is_some_and(|other| other.workspace == record.workspace);
                            if !same {
                                tracing::warn!(
                                    workflow = id,
                                    "it watches a workflow of another workspace, or none: it does not run"
                                );
                                continue;
                            }
                        }
                        _ => {}
                    }
                    wanted.insert(id, (record, w));
                }
                Err(e) => tracing::warn!(workflow = id, error = e, "a workflow that is on cannot run"),
            }
        }
        self.keep_triggers(state, &wanted).await;
        let mut running = self.running.lock().await;
        let gone: Vec<String> = running
            .iter()
            .filter(|(id, r)| wanted.get(*id).is_none_or(|(w, _)| w.hash != r.hash))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(r) = running.remove(&id) {
                let _ = r.stop.send(true);
                // Its runs finish, it commits and leaves its group, while its new version (if any) starts: the new
                // one is given its partitions as soon as the old one has left, not when its session runs out.
                tokio::spawn(finish(r.tasks));
                tracing::info!(workflow = id, "workflow stopped");
            }
        }
        for (id, (record, workflow)) in wanted {
            if running.contains_key(&id) {
                continue;
            }
            let effects = match state.effects_for(&record.workspace) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(workflow = id, error = e, "cannot set up the workflow's effects");
                    continue;
                }
            };
            let (stop, stopping) = watch::channel(false);
            let workflow = Arc::new(workflow);
            // storage: a folder's polls are queued as a schedule's times are; a channel is listened to on one node.
            let storage = crate::storage::tasks(&id, &workflow, state, &effects, &stopping);
            let messaging = crate::messaging::tasks(&id, &workflow, state, &effects, &stopping); // messaging
            let mut tasks = vec![tokio::spawn(keep_running(
                Arc::clone(&workflow),
                Arc::clone(&state.shared),
                effects,
                stopping.clone(),
            ))];
            tasks.extend(storage); // storage
            tasks.extend(messaging); // messaging
            // cron-schedules: set times, queued by every node and claimed once.
            if let Trigger::Calendar(calendar) = &workflow.trigger {
                tasks.push(tokio::spawn(openduct_flow::calendar::schedule(
                    id.clone(),
                    calendar.clone(),
                    Arc::clone(&state.shared),
                    stopping.clone(),
                )));
            }
            // G3 app triggers: a poll's times are queued as a schedule's are, and its runner polls at each.
            if let Trigger::App(t) = &workflow.trigger
                && let Some(every) = t.every
            {
                tasks.push(tokio::spawn(keep_scheduling(
                    id.clone(),
                    every,
                    Arc::clone(&state.shared),
                    stopping.clone(),
                )));
            }
            // web: a feed's polls are queued as a schedule's times are, and its runner polls at each.
            if let Trigger::Feed(t) = &workflow.trigger {
                let (id, shared) = (id.clone(), Arc::clone(&state.shared));
                tasks.push(tokio::spawn(keep_scheduling(
                    id,
                    t.every(),
                    shared,
                    stopping.clone(),
                )));
            }
            // sql databases: a table's polls are queued as a schedule's times are, and its runner polls at each.
            if let Trigger::Rows(t) = &workflow.trigger {
                tasks.push(tokio::spawn(keep_scheduling(
                    id.clone(),
                    t.every,
                    Arc::clone(&state.shared),
                    stopping.clone(),
                )));
            }
            if let Trigger::Every(every) = workflow.trigger {
                tasks.push(tokio::spawn(keep_scheduling(
                    id.clone(),
                    every,
                    Arc::clone(&state.shared),
                    stopping,
                )));
            }
            tracing::info!(workflow = id, "workflow running");
            running.insert(
                id,
                Running {
                    hash: record.hash,
                    stop,
                    tasks,
                },
            );
        }
    }

    /// Writes the trigger records that changed: one for each workflow that is on, off for the rest.
    async fn keep_triggers(&self, state: &State, wanted: &HashMap<String, (WorkflowRecord, Workflow)>) {
        let kept: HashMap<String, TriggerRecord> = state.model.triggers.all().await.into_iter().collect();
        for (id, (record, workflow)) in wanted {
            let last_tick = kept.get(id).and_then(|t| t.last_tick);
            let fresh = trigger_of(record, workflow, last_tick);
            let same = kept.get(id).is_some_and(|t| {
                t.active
                    && t.kind == fresh.kind
                    && t.version == fresh.version
                    && t.hook == fresh.hook
                    && t.every_ms == fresh.every_ms
                    && t.topic == fresh.topic
            });
            if !same && let Err(e) = state.model.triggers.put(id, &fresh).await {
                tracing::warn!(workflow = id, error = e, "cannot keep the workflow's trigger");
            }
        }
        for (id, mut trigger) in kept {
            if trigger.active && !wanted.contains_key(&id) {
                trigger.active = false;
                trigger.updated = now();
                if let Err(e) = state.model.triggers.put(&id, &trigger).await {
                    tracing::warn!(workflow = id, error = e, "cannot keep the workflow's trigger");
                }
            }
        }
    }

    /// Every workflow told to stop at once, each given until one deadline to finish its runs and commit.
    pub(crate) async fn stop_all(&self) {
        let mut running = self.running.lock().await;
        let all: Vec<_> = running.drain().map(|(_, r)| r).collect();
        for r in &all {
            let _ = r.stop.send(true);
        }
        finish(all.into_iter().flat_map(|r| r.tasks).collect()).await;
    }
}

/// How long a stopped workflow's tasks have to finish their runs, commit and leave, before they are cut off.
const FINISHING: Duration = Duration::from_secs(15);

/// Waits for a stopped workflow's tasks - told to stop already - to end, cutting off those still going at the end.
async fn finish(tasks: Vec<tokio::task::JoinHandle<()>>) {
    let until = tokio::time::Instant::now() + FINISHING;
    for task in tasks {
        let stopping = task.abort_handle();
        if tokio::time::timeout_at(until, task).await.is_err() {
            stopping.abort();
        }
    }
}

/// Every trigger of the workspace that is on: webhooks with their addresses, schedules, topics.
pub(crate) async fn triggers(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(e) = state.model.triggers.refresh().await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    let workflows: HashMap<String, WorkflowRecord> = state.model.workflows.all().await.into_iter().collect();
    let out: Vec<Value> = state
        .model
        .triggers
        .all()
        .await
        .into_iter()
        .filter(|(_, t)| t.workspace == access.workspace.id && t.active)
        .map(|(id, t)| {
            json!({
                "workflow": id, "name": workflows.get(&id).map(|w| w.name.clone()), "kind": t.kind,
                "version": t.version, "hook": t.hook, "every_ms": t.every_ms, "topic": t.topic,
                "environment": PRODUCTION,
            })
        })
        .collect();
    json(StatusCode::OK, &json!({ "triggers": out }))
}

/// The runner, started again after a failure (the broker restarting, say), with a growing wait.
async fn keep_running(
    workflow: Arc<Workflow>,
    shared: Arc<openduct_broker::client::Shared>,
    effects: Arc<dyn openduct_flow::Effects>,
    stop: watch::Receiver<bool>,
) {
    let mut wait = Duration::from_secs(1);
    while !*stop.borrow() {
        match runner::serve(
            Arc::clone(&workflow),
            Arc::clone(&shared),
            Arc::clone(&effects),
            stop.clone(),
        )
        .await
        {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(
                    workflow = workflow.id,
                    error = e,
                    "the workflow's runner stopped: starting it again"
                );
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(60));
            }
        }
    }
}

pub(crate) async fn keep_scheduling(
    id: String,
    every: Duration,
    shared: Arc<openduct_broker::client::Shared>,
    stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        if let Err(e) = runner::schedule(id.clone(), every, Arc::clone(&shared), stop.clone()).await {
            tracing::warn!(
                workflow = id,
                error = e,
                "the workflow's schedule stopped: starting it again"
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}
