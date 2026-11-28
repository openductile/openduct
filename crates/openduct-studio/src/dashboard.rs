// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The dashboard: how a workspace is doing over a period - its workflows, its runs by day (or by hour, for a day),
//! its success, the records waiting in CatchErr - and what needs its people, as alerts with where each is fixed.
//! The overview does the same for every workspace a person belongs to, for those who look after several (an
//! agency and its clients): only theirs, never another's.

use std::collections::BTreeSet;

use hyper::StatusCode;
use openduct_flow::runner::Topics;
use serde_json::{Value, json};

use crate::State;
use crate::api::{access, signed_in};
use crate::history::{last, runs_of};
use crate::hooks::form;
use crate::http::{Answer, Call, json, problem};
use crate::logs::at;
use crate::model::{PRODUCTION, Role, User, WorkflowRecord};
use crate::workflows::now_ms;

/// The runs read back from each workflow's history for a period.
const MEASURED: i64 = 500;
/// The most CatchErr records counted for a workflow.
const COUNTED_WAITING: i64 = 500;
/// Failures in a period from which a workflow is said to keep failing.
const FAILING: u64 = 3;
/// The runs shown as recent activity.
const RECENT: usize = 12;

/// A period: its length and the buckets its runs are counted in.
#[derive(Debug, Clone, Copy)]
struct Range {
    name: &'static str,
    ms: i64,
    bucket_ms: i64,
}

fn range_of(call: &Call) -> Range {
    const HOUR: i64 = 3_600_000;
    const DAY: i64 = 24 * HOUR;
    let asked = form(&call.query);
    match asked.get("range").and_then(Value::as_str).unwrap_or("7d") {
        "1d" | "today" => Range {
            name: "1d",
            ms: DAY,
            bucket_ms: HOUR,
        },
        "30d" => Range {
            name: "30d",
            ms: 30 * DAY,
            bucket_ms: DAY,
        },
        _ => Range {
            name: "7d",
            ms: 7 * DAY,
            bucket_ms: DAY,
        },
    }
}

/// The credentials every step of a definition names, wherever the step is (branches, cases and loops included).
fn credentials_named(definition: &Value, into: &mut BTreeSet<String>) {
    match definition {
        Value::Object(fields) => {
            for (k, v) in fields {
                // retrieval: its steps name AI credentials beside their database.
                if k == "credential" || k == "embed_with" || k == "answer_with" {
                    if let Some(name) = v.as_str().filter(|n| !n.trim().is_empty()) {
                        into.insert(name.to_owned());
                    }
                } else {
                    credentials_named(v, into);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                credentials_named(item, into);
            }
        }
        _ => {}
    }
}

fn alert(key: String, level: &str, title: String, detail: String, label: &str, path: String) -> Value {
    json!({ "key": key, "level": level, "title": title, "detail": detail, "action": { "label": label, "path": path } })
}

/// How a workspace is doing over `range`, and what needs its people.
async fn summary(state: &State, workspace: &str, range: Range, admin: bool) -> Value {
    let now = now_ms();
    let from = now - range.ms;
    let _ = state.model.workflows.refresh().await;
    let _ = state.model.credentials.refresh().await;
    let workflows: Vec<WorkflowRecord> = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .filter(|w| w.workspace == workspace && w.deleted.is_none())
        .collect();
    let credentials: BTreeSet<String> = state
        .model
        .credentials
        .all()
        .await
        .into_iter()
        .map(|(_, c)| c)
        .filter(|c| c.workspace == workspace && c.environment == PRODUCTION && c.deleted.is_none())
        .map(|c| c.name)
        .collect();
    let buckets = usize::try_from(range.ms / range.bucket_ms).unwrap_or(1).max(1);
    let mut by_bucket = vec![[0u64; 3]; buckets];
    let (mut done, mut failed, mut skipped, mut waiting_all, mut never) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut per_workflow = Vec::with_capacity(workflows.len());
    let mut recent: Vec<Value> = Vec::new();
    let mut alerts: Vec<Value> = Vec::new();
    for w in &workflows {
        let runs = last(state, &w.id, Topics::of(&w.id).runs, MEASURED, 0)
            .await
            .map(|found| runs_of(&found))
            .unwrap_or_default();
        if runs.is_empty() {
            never += 1;
        }
        let waiting = last(
            state,
            &w.id,
            Topics::of(&w.id).failed,
            COUNTED_WAITING,
            w.failed_from,
        )
        .await
        .map_or(0, |found| u64::try_from(found.len()).unwrap_or(0));
        waiting_all += waiting;
        let (mut w_runs, mut w_failed) = (0u64, 0u64);
        // Newest first, as the history gives them.
        for run in &runs {
            let when = at(run, "/at").as_i64().unwrap_or(0);
            if when < from {
                continue;
            }
            w_runs += 1;
            let column = match at(run, "/status").as_str() {
                Some("done") => {
                    done += 1;
                    0
                }
                Some("failed") => {
                    failed += 1;
                    w_failed += 1;
                    1
                }
                _ => {
                    skipped += 1;
                    2
                }
            };
            let bucket = usize::try_from((when - from) / range.bucket_ms)
                .unwrap_or(0)
                .min(buckets - 1);
            if let Some(b) = by_bucket.get_mut(bucket)
                && let Some(c) = b.get_mut(column)
            {
                *c += 1;
            }
            let mut shown = run.clone();
            if let Some(fields) = shown.as_object_mut() {
                fields.remove("steps");
                fields.remove("event");
                fields.insert("workflow".to_owned(), json!({ "id": w.id, "name": w.name }));
            }
            recent.push(shown);
        }
        let latest = runs.first();
        per_workflow.push(json!({
            "id": w.id, "name": w.name, "active": w.active,
            "runs": w_runs, "failed": w_failed, "waiting": waiting,
            "last_at": latest.and_then(|r| at(r, "/at").as_i64()),
            "last_status": latest.and_then(|r| at(r, "/status").as_str()),
        }));
        let path = format!("/workflows/{}", w.id);
        if waiting > 0 {
            alerts.push(alert(
                format!("waiting/{}", w.id),
                "critical",
                format!(
                    "{} record{} waiting in CatchErr",
                    waiting,
                    if waiting == 1 { "" } else { "s" }
                ),
                format!(
                    "{}: runs failed and their records wait to be run again once the cause is fixed.",
                    w.name
                ),
                "Review",
                path.clone(),
            ));
        }
        if w_failed >= FAILING {
            alerts.push(alert(
                format!("failing/{}", w.id),
                "warning",
                format!("{} failed {} times", w.name, w_failed),
                "In this period. Its runs show what went wrong.".to_owned(),
                "See its runs",
                path.clone(),
            ));
        }
        let mut named = BTreeSet::new();
        if let Some(definition) = state.model.definition(&w.workspace, &w.hash).await {
            credentials_named(&definition, &mut named);
        }
        for missing in named.difference(&credentials) {
            alerts.push(alert(
                format!("credential/{}/{missing}", w.id),
                "warning",
                format!("{} needs the credential {missing}", w.name),
                "A step uses it, and this workspace has none by that name.".to_owned(),
                "Add it",
                format!("/credentials#add={missing}"),
            ));
        }
    }
    if admin && let Some(dir) = state.log_dir.clone() {
        let since = u128::try_from(from).unwrap_or(0) * 1_000_000;
        let errors = tokio::task::spawn_blocking(move || crate::logs::count_errors(&dir, since))
            .await
            .unwrap_or(0);
        if errors > 0 {
            alerts.push(alert(
                "log/errors".to_owned(),
                "warning",
                format!(
                    "{errors} error{} in OpenDuct's log",
                    if errors == 1 { "" } else { "s" }
                ),
                "On this node, in this period.".to_owned(),
                "Open the log",
                "/logs".to_owned(),
            ));
        }
    }
    recent.sort_by_key(|r| std::cmp::Reverse(at(r, "/at").as_i64().unwrap_or(0)));
    // Failures first among the newest, as they are what needs looking at.
    let (mut bad, good): (Vec<Value>, Vec<Value>) = recent
        .into_iter()
        .take(RECENT * 4)
        .partition(|r| at(r, "/status") == "failed");
    bad.extend(good);
    bad.truncate(RECENT);
    let total = done + failed + skipped;
    let finished = done + failed;
    let success = (finished > 0).then(|| (done * 1000 / finished) as f64 / 10.0);
    let on = workflows.iter().filter(|w| w.active).count();
    let health = if alerts.iter().any(|a| at(a, "/level") == "critical") {
        "red"
    } else if alerts.is_empty() {
        "green"
    } else {
        "amber"
    };
    json!({
        "range": range.name,
        "from": from,
        "bucket_ms": range.bucket_ms,
        "health": health,
        "workflows": { "total": workflows.len(), "on": on, "never_ran": never },
        "credentials": credentials.len(),
        "runs": {
            "total": total, "done": done, "failed": failed, "skipped": skipped,
            "buckets": by_bucket.iter().map(|b| json!({ "done": b[0], "failed": b[1], "skipped": b[2] })).collect::<Vec<_>>(),
        },
        "success_percent": success,
        "waiting": waiting_all,
        "per_workflow": per_workflow,
        "recent": bad,
        "alerts": alerts,
    })
}

/// A workspace's last day, and what needs its people: what its alerts are sent from.
pub(crate) async fn summary_of(state: &State, workspace: &str) -> Value {
    const DAY: i64 = 24 * 3_600_000;
    summary(
        state,
        workspace,
        Range {
            name: "1d",
            ms: DAY,
            bucket_ms: 3_600_000,
        },
        false,
    )
    .await
}

/// `GET /api/dashboard?range=1d|7d|30d`: the current workspace.
pub(crate) async fn dashboard(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let range = range_of(call);
    let mut shown = summary(state, &access.workspace.id, range, access.user.admin).await;
    if let Some(fields) = shown.as_object_mut() {
        fields.insert(
            "workspace".to_owned(),
            json!({ "id": access.workspace.id, "name": access.workspace.name }),
        );
    }
    json(StatusCode::OK, &shown)
}

/// `GET /api/overview?range=…`: every workspace the person belongs to, each summed up with its alerts - for an
/// agency, its clients. Only the person's own: a client sees its workspace and nothing of another's.
pub(crate) async fn overview(state: &State, call: &Call) -> Answer {
    let user: User = match signed_in(state, call).await {
        Ok((user, _)) => user,
        Err(answer) => return answer,
    };
    let range = range_of(call);
    let _ = state.model.workspaces.refresh().await;
    let _ = state.model.members.refresh().await;
    let mine = state.model.workspaces_of(&user).await;
    if mine.is_empty() {
        return problem(StatusCode::NOT_FOUND, "no workspace yet");
    }
    let mut workspaces = Vec::with_capacity(mine.len());
    for (w, role) in mine {
        let s = summary(state, &w.id, range, user.admin).await;
        workspaces.push(json!({
            "id": w.id, "name": w.name, "role": role,
            "health": at(&s, "/health"),
            "workflows": at(&s, "/workflows"),
            "runs": { "total": at(&s, "/runs/total"), "failed": at(&s, "/runs/failed") },
            "success_percent": at(&s, "/success_percent"),
            "waiting": at(&s, "/waiting"),
            "alerts": at(&s, "/alerts"),
            "last_failure": at(&s, "/recent").as_array().and_then(|r| r.iter().find(|x| at(x, "/status") == "failed")).and_then(|x| at(x, "/at").as_i64()),
        }));
    }
    // Those needing attention first: red, then amber, then by failures.
    let rank = |w: &Value| match at(w, "/health").as_str() {
        Some("red") => 0,
        Some("amber") => 1,
        _ => 2,
    };
    workspaces.sort_by_key(|w| {
        (
            rank(w),
            std::cmp::Reverse(at(w, "/runs/failed").as_u64().unwrap_or(0)),
        )
    });
    json(
        StatusCode::OK,
        &json!({ "range": range.name, "workspaces": workspaces }),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn every_credential_a_definition_names_is_found_wherever_its_step_is() {
        let definition = json!({ "steps": [
            { "ai": { "credential": "model" } },
            { "if": { "then": [{ "slack": { "credential": "team-chat" } }], "else": [] } },
            { "switch": { "cases": [{ "steps": [{ "email": { "credential": "mail" } }] }], "default": [] } },
            { "http": { "credential": "" } },
        ] });
        let mut found = BTreeSet::new();
        credentials_named(&definition, &mut found);
        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            ["mail", "model", "team-chat"]
        );
    }

    // retrieval
    #[test]
    fn the_credentials_a_retrieval_step_names_beside_its_database_are_found_too() {
        let definition = json!({ "steps": [
            { "ask_documents": { "credential": "docs-db", "embed_with": "vectors", "answer_with": "writer" } },
            { "vector_store": { "credential": "docs-db", "embed_with": "vectors", "rerank": { "credential": "ranker" } } },
        ] });
        let mut found = BTreeSet::new();
        credentials_named(&definition, &mut found);
        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            ["docs-db", "ranker", "vectors", "writer"]
        );
    }
}
