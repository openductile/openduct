// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow a database's new or changed rows start (see [`crate::databases::trigger`]): each scheduled time -
//! queued by every node and claimed by the one member that reads the schedule's partition - is a poll of the table.
//! As an app's poll (see [`super::polling`]), it reads what the polls before saw from that partition, asks the
//! database, and writes a run for each row taken and what is seen now in one batch: both kept, or neither. So a
//! restart or a node's loss neither runs a row twice nor passes one by. A poll that fails keeps what was seen, says
//! why in the workflow's history once, and the next one asks again.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use openduct_broker::client::{GroupState, Membership, Outgoing, Shared};
use serde_json::{Value as Json, json};

use super::polling::{last_seen, seen_record, turn};
use super::{SCHEDULE_PARTITION, Topics, envelope, iso, now_ms};
use crate::app_files::poll::Seen;
use crate::databases::trigger::{RowsTrigger, poll};
use crate::model::{Trigger, Workflow};
use crate::run::{Effects, Event};

/// How long one poll may take, its queries together.
const POLL_TIME: Duration = Duration::from_secs(300);

/// The trigger, when a database's rows start `workflow`.
pub(super) fn polls(workflow: &Workflow) -> Option<&RowsTrigger> {
    match &workflow.trigger {
        Trigger::Rows(t) => Some(t),
        _ => None,
    }
}

/// The poll of a claimed time, when a database's rows start `workflow`; none for any other workflow.
pub(super) fn tick(
    workflow: &Arc<Workflow>,
    shared: &Arc<Shared>,
    effects: &Arc<dyn Effects>,
    member: &Membership,
    topics: &Arc<Topics>,
    event: &Event,
) -> Option<impl Future<Output = ()> + Send + 'static> {
    polls(workflow)?;
    let slot = super::slot_of(event)?;
    let (workflow, shared, effects, member, topics) = (
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(effects),
        member.clone(),
        Arc::clone(topics),
    );
    Some(async move {
        if let Err(e) = poll_once(&workflow, &shared, effects.as_ref(), &member, &topics, slot).await {
            tracing::warn!(
                workflow = workflow.id,
                error = e,
                "a poll could not be kept: the next one asks again"
            );
        }
    })
}

async fn poll_once(
    workflow: &Workflow,
    shared: &Shared,
    effects: &dyn Effects,
    member: &Membership,
    topics: &Topics,
    slot: i64,
) -> Result<(), String> {
    let Some(t) = polls(workflow) else {
        return Ok(());
    };
    let turn = turn(&workflow.id);
    let _turn = turn.lock().await;
    let before = {
        let mut client = shared.lend().await.map_err(|e| e.to_string())?;
        last_seen(&mut client, &topics.input).await?
    };
    if before.as_ref().is_some_and(|b| b.slot >= slot) {
        return Ok(());
    }
    let started = Instant::now();
    let asked = poll(effects, t, before.as_ref().filter(|b| !b.pending));
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
        Ok(mut found) => {
            found.seen.slot = slot;
            let mut batch: Vec<Outgoing> = found
                .runs
                .iter()
                .map(|(id, value)| {
                    let value: Json = serde_json::from_str(&value.to_json()).unwrap_or(Json::Null);
                    let details = json!({
                        "trigger": t.db.name(), "table": t.table, "id": id, "at": iso(now), "polled": iso(slot),
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
            let said: String = error.chars().take(500).collect();
            let mut seen = before.clone().unwrap_or(Seen {
                pending: true,
                ..Seen::default()
            });
            if seen.failing.as_deref() != Some(said.as_str()) {
                say_failed(shared, topics, workflow, t, slot, &error, started).await;
            }
            seen.failing = Some(said);
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
    t: &RowsTrigger,
    slot: i64,
    error: &str,
    started: Instant,
) {
    let id = format!("poll-{slot}");
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let history = json!({
        "status": "failed", "error": error, "ms": ms,
        "steps": [{ "name": "poll", "kind": "poll", "attempts": 1, "ms": ms, "result": null, "error": error }],
        "run": id, "at": now_ms(), "event": null,
        "trigger": { "trigger": t.db.name(), "table": t.table, "polled": iso(slot) },
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
