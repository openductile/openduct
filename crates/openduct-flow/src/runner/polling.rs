// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow an app's poll starts: each scheduled time - queued by every node and claimed by the one member that
//! reads the schedule's partition, as a schedule's times are - is a poll rather than a run. The poll reads what the
//! polls before saw (the last record of that kind in the schedule's partition), asks the app, and writes each new item
//! (a run) and what is seen now to that partition in one batch: both are kept, or neither. So a restart, or a node's
//! loss, neither runs an item twice nor passes one by: a poll cut short wrote nothing, and the next one asks again
//! from the same place. A poll that fails keeps what was seen as it was, says why in the workflow's history (once,
//! not at every poll), and the next one tries again.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use openduct_broker::client::{EARLIEST, GroupState, LATEST, Membership, Outgoing, Routed, Shared, records};
use serde_json::{Value as Json, json};

use super::{SCHEDULE_PARTITION, Topics, envelope, iso, now_ms};
use crate::app_files::poll::{Polling, Seen};
use crate::app_files::{AppDef, triggers::TriggerDef};
use crate::model::{AppTrigger, Trigger, Workflow};
use crate::run::Effects;

/// The key of the record that holds what a workflow's polls have seen.
const SEEN_KEY: &[u8] = b"poll/seen";
/// How long one poll may take, its requests together.
const POLL_TIME: Duration = Duration::from_secs(300);
/// How far back, at most, the last poll's record is looked for.
const LOOK_BACK: i64 = 65_536;
/// The most bytes one read of the schedule's partition asks for.
const READ_BYTES: i32 = 1024 * 1024;

/// Whether `record` is what a workflow's polls have seen (not an event to run).
pub(super) fn is_seen(record: &Record) -> bool {
    record.key.as_deref() == Some(SEEN_KEY)
}

/// The app, its trigger and the workflow's settings for it, when an app's poll starts `workflow`.
pub(super) fn polls(workflow: &Workflow) -> Option<(&'static AppDef, &'static TriggerDef, &AppTrigger)> {
    let Trigger::App(t) = &workflow.trigger else {
        return None;
    };
    let (app, def) = t.definition()?;
    def.polls()?;
    Some((app, def, t))
}

/// One poll at a time for each workflow on this node: two times claimed together poll one after the other.
pub(super) fn turn(id: &str) -> Arc<tokio::sync::Mutex<()>> {
    static TURNS: OnceLock<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let turns = TURNS.get_or_init(Default::default);
    let mut held = turns.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(held.entry(id.to_owned()).or_default())
}

/// What the polls before saw: the last such record in the schedule's partition (none: no poll has kept one).
pub(super) async fn last_seen(client: &mut Routed, topic: &str) -> Result<Option<Seen>, String> {
    let end = client
        .list_offset(topic, SCHEDULE_PARTITION, LATEST)
        .await
        .map_err(|e| e.to_string())?;
    let start = client
        .list_offset(topic, SCHEDULE_PARTITION, EARLIEST)
        .await
        .map_err(|e| e.to_string())?;
    let mut back = 64;
    loop {
        let from = (end - back).max(start).max(0);
        if let Some(seen) = seen_between(client, topic, from, end).await? {
            return Ok(Some(seen));
        }
        if from <= start || back >= LOOK_BACK {
            if from > start {
                tracing::warn!(
                    topic,
                    "what the polls before saw is not in the last {LOOK_BACK} records: from now"
                );
            }
            return Ok(None);
        }
        back *= 8;
    }
}

/// The last record of what was seen from `from` up to `end`.
async fn seen_between(client: &mut Routed, topic: &str, from: i64, end: i64) -> Result<Option<Seen>, String> {
    let mut at = from;
    let mut found = None;
    while at < end {
        let fetched = client
            .fetch(topic, &[(SCHEDULE_PARTITION, at)], 0, READ_BYTES)
            .await
            .map_err(|e| e.to_string())?;
        let Some(answer) = fetched.into_iter().find(|f| f.partition == SCHEDULE_PARTITION) else {
            break;
        };
        if answer.error_code != 0 {
            return Err(format!("{topic} answered with error code {}", answer.error_code));
        }
        let mut batches = answer.batches;
        let before = at;
        while !batches.is_empty() {
            let Ok(set) =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            else {
                break;
            };
            let from_here = at;
            for record in set.records.iter().filter(|r| r.offset >= from_here) {
                at = record.offset + 1;
                if is_seen(record)
                    && let Some(seen) = record
                        .value
                        .as_ref()
                        .and_then(|v| serde_json::from_slice::<Json>(v).ok())
                        .and_then(|j| serde_json::from_value::<Seen>(j.get("seen")?.clone()).ok())
                {
                    found = Some(seen);
                }
            }
        }
        if at == before {
            break;
        }
    }
    Ok(found)
}

pub(super) fn seen_record(seen: &Seen) -> Outgoing {
    Outgoing {
        key: Some(Bytes::from_static(SEEN_KEY)),
        value: Some(Bytes::from(json!({ "seen": seen }).to_string().into_bytes())),
        headers: Vec::new(),
    }
}

/// The poll of the time `slot` (milliseconds since 1970), claimed by `member`.
pub(super) async fn tick(
    workflow: Arc<Workflow>,
    shared: Arc<Shared>,
    effects: Arc<dyn Effects>,
    member: Membership,
    topics: Arc<Topics>,
    slot: i64,
) {
    if let Err(e) = poll_once(&workflow, &shared, effects.as_ref(), &member, &topics, slot).await {
        tracing::warn!(
            workflow = workflow.id,
            error = e,
            "a poll could not be kept: the next one asks again"
        );
    }
}

async fn poll_once(
    workflow: &Workflow,
    shared: &Shared,
    effects: &dyn Effects,
    member: &Membership,
    topics: &Topics,
    slot: i64,
) -> Result<(), String> {
    let Some((app, def, settings)) = polls(workflow) else {
        return Ok(());
    };
    let turn = turn(&workflow.id);
    let _turn = turn.lock().await;
    let before = {
        let mut client = shared.lend().await.map_err(|e| e.to_string())?;
        last_seen(&mut client, &topics.input).await?
    };
    // A time already polled for, or passed by a later poll (a node that queued it late, a takeover that reads it
    // again): nothing to do.
    if before.as_ref().is_some_and(|b| b.slot >= slot) {
        return Ok(());
    }
    let started = Instant::now();
    let asked = async {
        let credential = effects.credential(&settings.credential)?;
        if credential.kind != "test" && app.sign_in(&credential.kind).is_none() {
            let kinds: Vec<&str> = app.sign_ins.iter().map(|s| s.id.as_str()).collect();
            return Err(format!(
                "{} is a {} credential: {} takes {}",
                settings.credential,
                credential.kind,
                app.name,
                kinds.join(" or ")
            ));
        }
        let polling = Polling::new(
            app,
            def,
            credential,
            &settings.settings,
            &workflow.id,
            before.as_ref().filter(|b| !b.pending),
            settings.max_items,
            now_ms(),
        )?;
        crate::app_files::poll::poll(effects, polling, &settings.credential, slot).await
    };
    let polled = match tokio::time::timeout(POLL_TIME, asked).await {
        Ok(p) => p,
        Err(_) => Err(format!("the poll took more than {POLL_TIME:?}")),
    };
    let mut client = shared.lend().await.map_err(|e| e.to_string())?;
    // Only the member that holds the schedule's partition writes: one that lost it meanwhile keeps nothing.
    if client.heartbeat(member).await.map_err(|e| e.to_string())? != GroupState::Stable {
        return Ok(());
    }
    let now = now_ms();
    let batch = match polled {
        Ok(found) => {
            let mut batch: Vec<Outgoing> = found
                .runs
                .iter()
                .map(|(id, value)| {
                    let value: Json = serde_json::from_str(&value.to_json()).unwrap_or(Json::Null);
                    let details = json!({
                        "trigger": "app", "app": app.id, "when": settings.when, "id": id, "at": iso(now),
                        "polled": iso(slot),
                    });
                    Outgoing {
                        key: None,
                        value: Some(Bytes::from(envelope(&value, &details))),
                        headers: Vec::new(),
                    }
                })
                .collect();
            batch.push(seen_record(&found.seen));
            batch
        }
        Err(error) => {
            // What was seen stays as it was; a first poll that failed leaves the next one the first.
            let mut seen = before.clone().unwrap_or(Seen {
                pending: true,
                ..Seen::default()
            });
            if seen.failing.as_deref() != Some(error.chars().take(500).collect::<String>().as_str()) {
                say_failed(shared, topics, workflow, slot, &error, started).await;
            }
            seen.failing = Some(error.chars().take(500).collect());
            seen.slot = slot;
            vec![seen_record(&seen)]
        }
    };
    client
        .produce(&topics.input, SCHEDULE_PARTITION, &batch, now)
        .await
        .map(|_| ())
        .map_err(|e| format!("cannot keep the poll: {e}"))
}

/// A failed poll in the workflow's history, as a failed run is: why, and when.
async fn say_failed(
    shared: &Shared,
    topics: &Topics,
    workflow: &Workflow,
    slot: i64,
    error: &str,
    started: Instant,
) {
    let Some((app, _, settings)) = polls(workflow) else {
        return;
    };
    let id = format!("poll-{slot}");
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let history = json!({
        "status": "failed", "error": error, "ms": ms,
        "steps": [{ "name": "poll", "kind": "poll", "attempts": 1, "ms": ms, "result": null, "error": error }],
        "run": id, "at": now_ms(), "event": null,
        "trigger": { "trigger": "app", "app": app.id, "when": settings.when, "polled": iso(slot) },
    });
    let message = Outgoing {
        key: Some(Bytes::from(id.into_bytes())),
        value: Some(Bytes::from(history.to_string().into_bytes())),
        headers: Vec::new(),
    };
    if let Err(e) = shared.produce(&topics.runs, 0, vec![message]).await {
        tracing::warn!(workflow = workflow.id, error = %e, "a failed poll could not be kept in the history");
    }
}
