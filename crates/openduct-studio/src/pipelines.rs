// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Pipelines in the studio: a workspace's pipeline files, written and checked here, tried over sample messages, and -
//! once turned on - run inside OpenDuct by the program's own run loop (a [`PipelineRunner`]), each with a group of
//! its own so no two workspaces' pipelines share their progress. In a cluster every node runs each pipeline that reads
//! a topic, and the copies share its partitions as one group; one that reads a connector (an HTTP endpoint, a
//! webhook) runs on one node only, so its messages are not read twice. A run that fails is tried again, waiting
//! longer each time, and its reason is shown.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hyper::{Method, StatusCode};
use openduct_pipeline::{Pipeline, test_lines};
use serde_json::{Value, json};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{PipelineRecord, Role, SCHEMA, new_id, now};

/// What a pipeline is run with: its file, what its workspace's topics are named on the broker beginning with
/// (`orders` is `w.<workspace>.orders`), the group that keeps its progress, the secrets it names (from its
/// workspace's credentials), and whether it may reach this machine's own network (`--allow-private-network`).
#[derive(Clone)]
pub struct PipelineRun {
    pub source: String,
    pub topic_prefix: String,
    pub group: String,
    pub secrets: BTreeMap<String, String>,
    pub private_network: bool,
}

impl std::fmt::Debug for PipelineRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The secrets' names at most: never a value.
        f.debug_struct("PipelineRun")
            .field("group", &self.group)
            .field("secrets", &self.secrets.keys().collect::<Vec<_>>())
            .field("private_network", &self.private_network)
            .finish_non_exhaustive()
    }
}

/// Runs a pipeline inside OpenDuct: the program's own run loop, given what it is run with, a stop signal, and where to
/// show how far it has got (read, written, set aside).
pub trait PipelineRunner: Send + Sync + std::fmt::Debug {
    fn run(
        &self,
        run: PipelineRun,
        stop: watch::Receiver<bool>,
        progress: Arc<dyn Fn(u64, u64, u64) + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
}

/// The most sample input a test reads.
const MAX_SAMPLE: usize = 256 * 1024;
/// The longest a failed run waits before it is tried again.
const MOST_WAIT: Duration = Duration::from_secs(60);

/// How one pipeline's run on this node is going.
#[derive(Debug, Default)]
pub(crate) struct Status {
    read: AtomicU64,
    written: AtomicU64,
    set_aside: AtomicU64,
    since: AtomicU64,
    error: std::sync::Mutex<Option<String>>,
}

#[derive(Debug)]
struct Running {
    source: String,
    workspace: String,
    /// What its secrets were when it started (a mark, not the secrets): a credential replaced starts it again.
    secrets: u64,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
    status: Arc<Status>,
}

/// The pipelines this node runs.
#[derive(Debug, Default)]
pub(crate) struct Pipes {
    running: Mutex<BTreeMap<String, Running>>,
}

/// The group a pipeline's progress is kept by: its own, whatever its file calls it.
fn group_of(id: &str) -> String {
    format!("openduct-pipeline-{id}")
}

/// What a pipeline reads and writes, in words: a topic's name, or the connector's kind.
fn ends(p: &Pipeline) -> (String, String) {
    let from = p
        .source
        .as_ref()
        .map_or_else(|| p.from_topic.clone(), |c| format!("{} (connector)", c.kind));
    let to = p
        .sink
        .as_ref()
        .map_or_else(|| p.to_topic.clone(), |c| format!("{} (connector)", c.kind));
    (from, to)
}

impl Pipes {
    /// Runs every pipeline that is on, stops those turned off or removed, and starts again any whose file changed.
    pub(crate) async fn sync(&self, state: &State) {
        let Some(runner) = state.pipeline_runner.clone() else {
            return;
        };
        let _ = state.model.pipelines.refresh().await;
        let wanted: BTreeMap<String, (String, String, u64)> = state
            .model
            .pipelines
            .all()
            .await
            .into_iter()
            .map(|(_, p)| p)
            .filter(|p| p.active && p.deleted.is_none())
            .filter(|p| {
                // One that reads a connector runs on one node only.
                state.runs_connector_sources
                    || Pipeline::parse(&p.source).is_ok_and(|parsed| parsed.source.is_none())
            })
            .map(|p| {
                let secrets = crate::pipeline_secrets::mark(&crate::pipeline_secrets::of(
                    &state.vault,
                    &p.workspace,
                    &p.source,
                ));
                (p.id, (p.source, p.workspace, secrets))
            })
            .collect();
        let mut running = self.running.lock().await;
        let gone: Vec<String> = running
            .iter()
            .filter(|(id, r)| wanted.get(*id) != Some(&(r.source.clone(), r.workspace.clone(), r.secrets)))
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(r) = running.remove(&id) {
                let _ = r.stop.send(true);
                // Given a moment to stop well (its last commit); then ended.
                tokio::spawn(async move {
                    let task = r.task;
                    if tokio::time::timeout(Duration::from_secs(15), task).await.is_err() {
                        tracing::warn!(pipeline = id, "a pipeline did not stop in time");
                    }
                });
                tracing::info!("pipeline stopped");
            }
        }
        for (id, (source, workspace, secrets)) in wanted {
            if running.contains_key(&id) {
                continue;
            }
            let prefix = openduct_flow::workspace_prefix(&workspace);
            let (stop, stopping) = watch::channel(false);
            let status = Arc::new(Status::default());
            status.since.store(now(), Ordering::Relaxed);
            let vault = Arc::clone(&state.vault);
            let (lookup_workspace, lookup_source) = (workspace.clone(), source.clone());
            let lookup: Lookup =
                Arc::new(move || crate::pipeline_secrets::of(&vault, &lookup_workspace, &lookup_source));
            let run = PipelineRun {
                source: source.clone(),
                topic_prefix: prefix,
                group: group_of(&id),
                secrets: BTreeMap::new(),
                private_network: state.allow_private_network,
            };
            let task = tokio::spawn(keep_running(
                Arc::clone(&runner),
                run,
                lookup,
                stopping,
                Arc::clone(&status),
            ));
            tracing::info!(pipeline = id, "pipeline running");
            running.insert(
                id,
                Running {
                    source,
                    workspace,
                    secrets,
                    stop,
                    task,
                    status,
                },
            );
        }
    }

    pub(crate) async fn status(&self, id: &str) -> Option<Arc<Status>> {
        self.running.lock().await.get(id).map(|r| Arc::clone(&r.status))
    }

    /// Every pipeline told to stop, each given a moment to.
    pub(crate) async fn stop_all(&self) {
        let running = std::mem::take(&mut *self.running.lock().await);
        for (_, r) in running {
            let _ = r.stop.send(true);
            let _ = tokio::time::timeout(Duration::from_secs(10), r.task).await;
        }
    }
}

/// Runs a pipeline until told to stop; a run that fails is tried again, waiting longer each time (to a minute).
/// The secrets a pipeline names, looked up now.
type Lookup = Arc<dyn Fn() -> Result<BTreeMap<String, String>, String> + Send + Sync>;

async fn keep_running(
    runner: Arc<dyn PipelineRunner>,
    run: PipelineRun,
    lookup: Lookup,
    stopping: watch::Receiver<bool>,
    status: Arc<Status>,
) {
    let mut wait = Duration::from_secs(2);
    loop {
        let shown = Arc::clone(&status);
        let progress: Arc<dyn Fn(u64, u64, u64) + Send + Sync> = Arc::new(move |read, written, set_aside| {
            shown.read.store(read, Ordering::Relaxed);
            shown.written.store(written, Ordering::Relaxed);
            shown.set_aside.store(set_aside, Ordering::Relaxed);
            // Counting again means it runs: what went wrong before is over.
            if let Ok(mut e) = shown.error.lock() {
                *e = None;
            }
        });
        let outcome = match lookup() {
            Ok(secrets) => {
                runner
                    .run(
                        PipelineRun {
                            secrets,
                            ..run.clone()
                        },
                        stopping.clone(),
                        progress,
                    )
                    .await
            }
            Err(why) => Err(why),
        };
        if *stopping.borrow() {
            return;
        }
        match outcome {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(
                    group = run.group,
                    error = e,
                    "a pipeline stopped; it is tried again"
                );
                if let Ok(mut held) = status.error.lock() {
                    *held = Some(e);
                }
            }
        }
        let mut stop = stopping.clone();
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            _ = stop.changed() => return,
        }
        wait = (wait * 2).min(MOST_WAIT);
    }
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "pipelines"]) => list(state, call).await,
        (&Method::POST, ["api", "pipelines"]) => create(state, call).await,
        (&Method::POST, ["api", "pipelines", "check"]) => check(state, call).await,
        (&Method::POST, ["api", "pipelines", "test"]) => test(state, call).await,
        (&Method::GET, ["api", "pipelines", id]) => get(state, call, id).await,
        (&Method::PUT, ["api", "pipelines", id]) => update(state, call, id).await,
        (&Method::DELETE, ["api", "pipelines", id]) => remove(state, call, id).await,
        (&Method::POST, ["api", "pipelines", id, "active"]) => activate(state, call, id).await,
        _ => return None,
    })
}

/// A pipeline file, read and checked: its expressions compiled, its connectors' settings known, and no secret it may
/// not use here (`env:`, `file:`): a workspace's secrets are its credentials.
fn parsed(source: &str) -> Result<Pipeline, Answer> {
    let p = Pipeline::parse(source).map_err(|e| problem(StatusCode::UNPROCESSABLE_ENTITY, &e))?;
    if let Some(why) = openduct_pipeline::secrets::first_refused(&p, false) {
        return Err(problem(StatusCode::UNPROCESSABLE_ENTITY, &why));
    }
    Ok(p)
}

fn source_of(call: &Call) -> Result<String, Answer> {
    call.body
        .get("source")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| problem(StatusCode::UNPROCESSABLE_ENTITY, "source: the pipeline file"))
}

async fn shown(state: &State, p: &PipelineRecord) -> Value {
    let parsed = Pipeline::parse(&p.source).ok();
    let (from, to) = parsed.as_ref().map(ends).unwrap_or_default();
    let status = state.pipes.status(&p.id).await;
    let (read, written, set_aside, since, error) = status.map_or((0, 0, 0, None, None), |s| {
        (
            s.read.load(Ordering::Relaxed),
            s.written.load(Ordering::Relaxed),
            s.set_aside.load(Ordering::Relaxed),
            Some(s.since.load(Ordering::Relaxed)),
            s.error.lock().ok().and_then(|e| e.clone()),
        )
    });
    json!({
        "id": p.id, "name": p.name, "active": p.active, "updated": p.updated, "source": p.source,
        "from": from, "to": to, "error_topic": parsed.and_then(|x| x.error_topic),
        "running": since.is_some(), "since": since, "read": read, "written": written, "set_aside": set_aside, "error": error,
    })
}

async fn mine(state: &State, workspace: &str) -> Vec<PipelineRecord> {
    let _ = state.model.pipelines.refresh().await;
    let mut out: Vec<PipelineRecord> = state
        .model
        .pipelines
        .all()
        .await
        .into_iter()
        .map(|(_, p)| p)
        .filter(|p| p.workspace == workspace && p.deleted.is_none())
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut out = Vec::new();
    for p in mine(state, &access.workspace.id).await {
        out.push(shown(state, &p).await);
    }
    json(StatusCode::OK, &json!({ "pipelines": out }))
}

async fn one(state: &State, workspace: &str, id: &str) -> Result<PipelineRecord, Answer> {
    mine(state, workspace)
        .await
        .into_iter()
        .find(|p| p.id == id)
        .ok_or_else(|| problem(StatusCode::NOT_FOUND, "no such pipeline"))
}

async fn get(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    match one(state, &access.workspace.id, id).await {
        Ok(p) => json(StatusCode::OK, &shown(state, &p).await),
        Err(answer) => answer,
    }
}

/// Whether another pipeline of the workspace already has the name.
async fn taken(state: &State, workspace: &str, name: &str, other_than: Option<&str>) -> bool {
    mine(state, workspace)
        .await
        .iter()
        .any(|p| p.name == name && Some(p.id.as_str()) != other_than)
}

async fn create(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let source = match source_of(call) {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    let p = match parsed(&source) {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    if taken(state, &access.workspace.id, &p.name, None).await {
        return problem(
            StatusCode::CONFLICT,
            &format!("name: a pipeline named {} is there already", p.name),
        );
    }
    let record = PipelineRecord {
        schema: SCHEMA,
        id: new_id(),
        workspace: access.workspace.id.clone(),
        name: p.name.clone(),
        source,
        active: false,
        created: now(),
        updated: now(),
        by: access.user.id.clone(),
        deleted: None,
    };
    match state.model.pipelines.put(&record.id, &record).await {
        Ok(()) => json(StatusCode::CREATED, &shown(state, &record).await),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn update(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut record = match one(state, &access.workspace.id, id).await {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    let source = match source_of(call) {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    let p = match parsed(&source) {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    if taken(state, &access.workspace.id, &p.name, Some(id)).await {
        return problem(
            StatusCode::CONFLICT,
            &format!("name: a pipeline named {} is there already", p.name),
        );
    }
    if record.active
        && let Err(e) = make_topics(state, &record.workspace, &p).await
    {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    record.name = p.name;
    record.source = source;
    record.updated = now();
    match state.model.pipelines.put(&record.id, &record).await {
        Ok(()) => {
            state.pipes.sync(state).await;
            json(StatusCode::OK, &shown(state, &record).await)
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn remove(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut record = match one(state, &access.workspace.id, id).await {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    record.active = false;
    record.deleted = Some(now());
    match state.model.pipelines.put(&record.id, &record).await {
        Ok(()) => {
            state.pipes.sync(state).await;
            json(StatusCode::OK, &json!({ "removed": true }))
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The topics a pipeline names, made if they are not there yet (one partition each): it can run from the moment it is
/// turned on.
async fn make_topics(state: &State, workspace: &str, p: &Pipeline) -> Result<(), String> {
    let mut topics: Vec<String> = Vec::new();
    if p.source.is_none() {
        topics.push(p.from_topic.clone());
    }
    if p.sink.is_none() {
        topics.push(p.to_topic.clone());
    }
    topics.extend(p.error_topic.clone());
    // The workspace's own: `orders` is `w.<workspace>.orders` on the broker.
    let topics: Vec<String> = topics
        .iter()
        .map(|t| openduct_flow::workspace_topic(workspace, t))
        .collect();
    state
        .with_client(|client| {
            Box::pin(async move {
                let there: Vec<String> = client
                    .topics()
                    .await
                    .map_err(|e| format!("cannot list the topics: {e}"))?
                    .into_iter()
                    .map(|t| t.name)
                    .collect();
                for topic in topics.iter().filter(|t| !there.contains(t)) {
                    match client.create_topic(topic, 1, None).await {
                        Ok(()) | Err(openduct_broker::client::ClientError::Refused { code: 36, .. }) => {}
                        Err(e) => return Err(format!("cannot make the topic {topic}: {e}")),
                    }
                }
                Ok(())
            })
        })
        .await
}

async fn activate(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut record = match one(state, &access.workspace.id, id).await {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    let on = call.body.get("active").and_then(Value::as_bool).unwrap_or(false);
    if on {
        if state.pipeline_runner.is_none() {
            return problem(
                StatusCode::CONFLICT,
                "this program runs no pipelines: it was built without them",
            );
        }
        let p = match parsed(&record.source) {
            Ok(p) => p,
            Err(answer) => return answer,
        };
        if let Err(e) = make_topics(state, &record.workspace, &p).await {
            return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
        }
    }
    record.active = on;
    record.updated = now();
    match state.model.pipelines.put(&record.id, &record).await {
        Ok(()) => {
            state.pipes.sync(state).await;
            json(StatusCode::OK, &shown(state, &record).await)
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

async fn check(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let source = match source_of(call) {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    match parsed(&source) {
        Ok(p) => {
            let (from, to) = ends(&p);
            // Credentials it names that the workspace does not have yet: it may be kept, but it runs once they are.
            let missing = crate::pipeline_secrets::missing(&state.vault, &access.workspace.id, &source);
            json(
                StatusCode::OK,
                &json!({ "ok": true, "name": p.name, "from": from, "to": to, "error_topic": p.error_topic, "missing": missing }),
            )
        }
        Err(answer) => answer,
    }
}

async fn test(state: &State, call: &Call) -> Answer {
    if let Err(answer) = access(state, call, Role::Editor).await {
        return answer;
    }
    let source = match source_of(call) {
        Ok(s) => s,
        Err(answer) => return answer,
    };
    let p = match parsed(&source) {
        Ok(p) => p,
        Err(answer) => return answer,
    };
    let input = call.body.get("input").and_then(Value::as_str).unwrap_or_default();
    if input.len() > MAX_SAMPLE {
        return problem(
            StatusCode::PAYLOAD_TOO_LARGE,
            "input: at most 256 KiB of sample messages",
        );
    }
    let lines: Vec<Value> = test_lines(&p, input)
        .into_iter()
        .map(|l| serde_json::from_str(&l).unwrap_or(Value::String(l)))
        .collect();
    json(StatusCode::OK, &json!({ "lines": lines }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn a_pipelines_group_is_its_own_and_its_ends_are_said_in_words() {
        assert_eq!(group_of("abc"), "openduct-pipeline-abc");
        let p = Pipeline::parse("name: x\nfrom:\n  topic: orders\nto:\n  topic: orders-clean\n").unwrap();
        assert_eq!(ends(&p), ("orders".to_owned(), "orders-clean".to_owned()));
    }
}
