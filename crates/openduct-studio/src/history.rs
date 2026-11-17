// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow's history: its runs (each step's result, an AI step's transcript among them), its metrics worked out
//! from them, and the events set aside when their run failed - which can be run again once the cause is fixed.

use std::collections::BTreeMap;

use hyper::StatusCode;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use openduct_broker::client::{LATEST, Outgoing, Routed, records};
use openduct_flow::runner::{self, Topics};
use serde_json::{Value, json};

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, now};
use crate::workflows::{now_ms, workflow_in};

/// The most runs shown from a history at once.
const SHOWN_RUNS: i64 = 50;
/// The most runs the Runs page shows, across a workspace's workflows.
const SHOWN_ALL: usize = 300;
/// The runs metrics are worked out from.
const MEASURED_RUNS: i64 = 500;
/// The most set-aside events shown and run again at once.
const SHOWN_FAILED: i64 = 500;

/// The last `n` messages of `topic` (one partition) from `from` on, oldest first.
/// Where a topic's partition 0 ends: the offset its next record will have.
pub(crate) async fn end_of(client: &mut Routed, topic: &str) -> Result<i64, String> {
    client
        .list_offset(topic, 0, LATEST)
        .await
        .map_err(|e| e.to_string())
}

pub(crate) async fn tail(client: &mut Routed, topic: &str, n: i64, from: i64) -> Result<Vec<Record>, String> {
    let end = client
        .list_offset(topic, 0, LATEST)
        .await
        .map_err(|e| e.to_string())?;
    let mut next = (end - n).max(from).max(0);
    let mut out = Vec::new();
    while next < end {
        let fetched = client
            .fetch(topic, &[(0, next)], 0, 4 * 1024 * 1024)
            .await
            .map_err(|e| e.to_string())?;
        let Some(answer) = fetched.into_iter().next() else {
            break;
        };
        if answer.error_code == 1 && next < answer.log_start {
            next = answer.log_start;
            continue;
        }
        let mut batches = answer.batches;
        let before = next;
        while !batches.is_empty() {
            let Ok(set) =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            else {
                break;
            };
            for record in set.records {
                if record.offset >= next && !record.control {
                    next = record.offset + 1;
                    out.push(record);
                }
            }
        }
        if next == before {
            break;
        }
    }
    Ok(out)
}

/// The last `n` messages of one of a workflow's topics, the topics made first if they are missing.
pub(crate) async fn last(
    state: &State,
    id: &str,
    topic: String,
    n: i64,
    from: i64,
) -> Result<Vec<Record>, String> {
    let id = id.to_owned();
    state
        .with_client(|client| {
            Box::pin(async move {
                runner::prepare(client, &id).await?;
                tail(client, &topic, n, from).await
            })
        })
        .await
}

pub(crate) fn runs_of(found: &[Record]) -> Vec<Value> {
    let mut runs: Vec<Value> = found
        .iter()
        .filter_map(|r| r.value.as_ref().and_then(|v| serde_json::from_slice(v).ok()))
        .collect();
    runs.reverse();
    runs
}

/// The newest runs of every workflow of the workspace, newest first, each with its workflow's id and name.
pub(crate) async fn all_runs(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let _ = state.model.workflows.refresh().await;
    let mine: Vec<_> = state
        .model
        .workflows
        .all()
        .await
        .into_iter()
        .map(|(_, w)| w)
        .filter(|w| w.workspace == access.workspace.id && w.deleted.is_none())
        .collect();
    let mut all: Vec<Value> = Vec::new();
    for w in &mine {
        let topic = Topics::of(&w.id).runs;
        // A workflow that never ran has no history yet: nothing to show, not a failure.
        let found = state
            .with_client(|client| Box::pin(async move { tail(client, &topic, SHOWN_RUNS, 0).await }))
            .await
            .unwrap_or_default();
        for mut run in runs_of(&found) {
            if let Some(fields) = run.as_object_mut() {
                fields.insert("workflow".to_owned(), json!({ "id": w.id, "name": w.name }));
            }
            all.push(run);
        }
    }
    all.sort_by_key(|r| std::cmp::Reverse(r["at"].as_i64().unwrap_or(0)));
    all.truncate(SHOWN_ALL);
    json(StatusCode::OK, &json!({ "runs": all }))
}

pub(crate) async fn runs(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = workflow_in(state, &access, id).await {
        return answer;
    }
    match last(state, id, Topics::of(id).runs, SHOWN_RUNS, 0).await {
        Ok(found) => json(StatusCode::OK, &json!({ "runs": runs_of(&found) })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The value at `share` (0 to 1) of sorted `values`.
fn at(values: &[u64], share: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )] // an index
    let i = ((values.len() - 1) as f64 * share).round() as usize;
    values.get(i).copied().unwrap_or(0)
}

/// What the last runs say: how many, how they ended, how long they took, the tokens AI steps used, and each step's
/// time and failures.
pub(crate) fn measure(runs: &[Value]) -> Value {
    let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
    let mut durations: Vec<u64> = Vec::new();
    let (mut prompt, mut completion) = (0u64, 0u64);
    let mut steps: BTreeMap<String, (u64, u64, u64, u64)> = BTreeMap::new();
    for run in runs {
        let status = run
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        *by_status.entry(status).or_default() += 1;
        durations.push(run.get("ms").and_then(Value::as_u64).unwrap_or(0));
        for step in run.get("steps").and_then(Value::as_array).into_iter().flatten() {
            let name = step
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let entry = steps.entry(name).or_default();
            entry.0 += 1;
            entry.1 += step.get("ms").and_then(Value::as_u64).unwrap_or(0);
            if step.get("error").is_some_and(|e| !e.is_null()) {
                entry.2 += 1;
            }
            if let Some(usage) = step.pointer("/result/usage") {
                let p = usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0);
                let c = usage
                    .get("completion_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                prompt += p;
                completion += c;
                entry.3 += p + c;
            }
        }
    }
    durations.sort_unstable();
    let total = runs.len() as u64;
    let done = by_status.get("done").copied().unwrap_or(0) + by_status.get("skipped").copied().unwrap_or(0);
    #[allow(clippy::cast_precision_loss)] // run counts
    let success = if total == 0 {
        Value::Null
    } else {
        json!((done as f64 / total as f64 * 1000.0).round() / 10.0)
    };
    let per_step: Vec<Value> = steps
        .into_iter()
        .map(|(name, (count, ms, failures, tokens))| {
            json!({ "name": name, "runs": count, "average_ms": ms.checked_div(count).unwrap_or(0), "failures": failures, "tokens": tokens })
        })
        .collect();
    json!({
        "runs": total,
        "by_status": by_status,
        "success_percent": success,
        "duration_ms": { "median": at(&durations, 0.5), "p95": at(&durations, 0.95), "max": durations.last().copied().unwrap_or(0) },
        "tokens": { "prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion },
        "steps": per_step,
    })
}

/// The event and details of a workflow's most recent run, for a preview on real data.
pub(crate) async fn last_event(state: &State, id: &str) -> Option<(Value, Value)> {
    let found = last(state, id, Topics::of(id).runs, 1, 0).await.ok()?;
    let run: Value = found
        .last()
        .and_then(|r| r.value.as_ref())
        .and_then(|v| serde_json::from_slice(v).ok())?;
    Some((
        run.get("event").cloned().unwrap_or(Value::Null),
        run.get("trigger").cloned().unwrap_or(Value::Null),
    ))
}

pub(crate) async fn metrics(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    if let Err(answer) = workflow_in(state, &access, id).await {
        return answer;
    }
    match last(state, id, Topics::of(id).runs, MEASURED_RUNS, 0).await {
        Ok(found) => json(StatusCode::OK, &json!({ "metrics": measure(&runs_of(&found)) })),
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

pub(crate) async fn failed(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    match last(state, id, Topics::of(id).failed, SHOWN_FAILED, record.failed_from).await {
        Ok(found) => {
            let events: Vec<Value> = found
                .iter()
                .map(|r| {
                    let header = |name: &str| {
                        r.headers
                            .iter()
                            .find(|(k, _)| k.as_str() == name)
                            .and_then(|(_, v)| v.as_ref())
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                    };
                    let envelope: Value = r.value.as_ref().and_then(|v| serde_json::from_slice(v).ok()).unwrap_or(Value::Null);
                    json!({ "run": header("openduct-run"), "error": header("openduct-error"), "event": envelope.get("value") })
                })
                .collect();
            json(StatusCode::OK, &json!({ "failed": events }))
        }
        Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// Runs every set-aside event again, once its cause is fixed.
pub(crate) async fn run_failed(state: &State, call: &Call, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let mut record = match workflow_in(state, &access, id).await {
        Ok(w) => w,
        Err(answer) => return answer,
    };
    let topics = Topics::of(id);
    let found = match last(state, id, topics.failed.clone(), SHOWN_FAILED, record.failed_from).await {
        Ok(found) => found,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    let count = found.len();
    let through = found.last().map_or(record.failed_from, |r| r.offset + 1);
    let input = topics.input;
    let moved = state
        .with_client(|client| {
            Box::pin(async move {
                let partitions = client.partition_count(&input).await.map_err(|e| e.to_string())?;
                for (i, r) in found.iter().enumerate() {
                    let message = Outgoing {
                        key: None,
                        value: r.value.clone(),
                        headers: Vec::new(),
                    };
                    let p = i32::try_from(i).unwrap_or(0).rem_euclid(partitions.max(1));
                    client
                        .produce(&input, p, &[message], now_ms())
                        .await
                        .map_err(|e| e.to_string())?;
                }
                Ok(())
            })
        })
        .await;
    if let Err(e) = moved {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    record.failed_from = through;
    record.updated = now();
    if let Err(e) = state.model.workflows.put(id, &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    json(StatusCode::ACCEPTED, &json!({ "queued": count }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_count_runs_time_them_and_add_up_tokens() {
        let runs = vec![
            json!({ "status": "done", "ms": 100, "steps": [
                { "name": "ask", "ms": 90, "error": null, "result": { "usage": { "prompt_tokens": 10, "completion_tokens": 5 } } },
            ] }),
            json!({ "status": "failed", "ms": 300, "steps": [ { "name": "ask", "ms": 280, "error": "answered 500" } ] }),
            json!({ "status": "skipped", "ms": 10, "steps": [] }),
        ];
        let m = measure(&runs);
        assert_eq!(m["runs"], 3);
        assert_eq!(m["by_status"]["failed"], 1);
        assert_eq!(m["success_percent"], 66.7);
        assert_eq!(m["duration_ms"]["median"], 100);
        assert_eq!(m["duration_ms"]["max"], 300);
        assert_eq!(m["tokens"]["total_tokens"], 15);
        assert_eq!(
            m["steps"][0],
            json!({ "name": "ask", "runs": 2, "average_ms": 185, "failures": 1, "tokens": 15 })
        );
        assert_eq!(measure(&[])["success_percent"], Value::Null);
    }
}
