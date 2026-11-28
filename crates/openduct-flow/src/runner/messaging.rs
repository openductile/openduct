// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The scheduled times of the messaging triggers, each claimed by the one member that reads the schedule's partition
//! and taken as work rather than as a run:
//!
//! - a mailbox's poll (the `mailbox` trigger): what the polls before saw is read from that partition, the server asked
//!   for the messages after it, and each new message's run written with what is seen now, in one batch - as an app's
//!   poll is ([`super::polling`]). A poll cut short writes nothing; the next asks again.
//! - a renewal of an app's webhook registration that runs out (Microsoft Graph's subscriptions): the app's `renew`
//!   requests, made once in the cluster at each time, and the address registered again when the app no longer has it
//!   ([`crate::app_files::hook::renew`]).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use openduct_broker::client::{GroupState, Membership, Outgoing, Shared};
use serde_json::{Value as Json, json};

use super::polling::{last_seen, seen_record, turn};
use super::{SCHEDULE_PARTITION, Topics, envelope, iso, now_ms};
use crate::app_files::poll::Seen;
use crate::mailbox::MailWatch;
use crate::model::{Trigger, Workflow};
use crate::run::{Effects, Event};

/// How long one poll, or one renewal, may take.
const WORK_TIME: Duration = Duration::from_secs(300);

/// The mailbox a workflow's polls ask, when a mailbox's new messages start it.
fn polls(workflow: &Workflow) -> Option<&MailWatch> {
    match &workflow.trigger {
        Trigger::Mailbox(w) => Some(w),
        _ => None,
    }
}

/// The work of a claimed time, when it is a mailbox's poll or an app's renewal; none for any other workflow.
pub(super) fn tick(
    workflow: &Arc<Workflow>,
    shared: &Arc<Shared>,
    effects: &Arc<dyn Effects>,
    member: &Membership,
    topics: &Arc<Topics>,
    event: &Event,
) -> Option<impl Future<Output = ()> + Send + 'static> {
    let renews = crate::app_files::hook::renews(&workflow.trigger);
    if polls(workflow).is_none() && !renews {
        return None;
    }
    let slot = super::slot_of(event)?;
    let (workflow, shared, effects, member, topics) = (
        Arc::clone(workflow),
        Arc::clone(shared),
        Arc::clone(effects),
        member.clone(),
        Arc::clone(topics),
    );
    Some(async move {
        let done = if renews {
            renew_once(&workflow, &shared, effects.as_ref(), &topics, slot).await
        } else {
            poll_once(&workflow, &shared, effects.as_ref(), &member, &topics, slot).await
        };
        if let Err(e) = done {
            tracing::warn!(
                workflow = workflow.id,
                error = e,
                "a scheduled poll or renewal could not be kept: the next one tries again"
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
    let Some(watch) = polls(workflow) else {
        return Ok(());
    };
    let turn = turn(&workflow.id);
    let _turn = turn.lock().await;
    let before = {
        let mut client = shared.lend().await.map_err(|e| e.to_string())?;
        last_seen(&mut client, &topics.input).await?
    };
    // A time already polled for, or passed by a later poll (a time queued late, or one a mailbox's news asked for).
    if before.as_ref().is_some_and(|b| b.slot >= slot) {
        return Ok(());
    }
    let started = Instant::now();
    let polled = match tokio::time::timeout(
        WORK_TIME,
        crate::mailbox::watch::poll(effects, watch, before.as_ref(), slot),
    )
    .await
    {
        Ok(p) => p,
        Err(_) => Err(format!("the poll took more than {WORK_TIME:?}")),
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
                        "trigger": "mailbox", "credential": watch.credential, "folder": watch.folder, "id": id,
                        "at": iso(now), "polled": iso(slot),
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
            let mut seen = before.clone().unwrap_or(Seen {
                pending: true,
                ..Seen::default()
            });
            let said: String = error.chars().take(500).collect();
            if seen.failing.as_deref() != Some(said.as_str()) {
                say_failed(shared, topics, workflow, slot, "poll", &error, started).await;
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

async fn renew_once(
    workflow: &Workflow,
    shared: &Shared,
    effects: &dyn Effects,
    topics: &Topics,
    slot: i64,
) -> Result<(), String> {
    let Trigger::App(t) = &workflow.trigger else {
        return Ok(());
    };
    let Some((app, def)) = t.definition() else {
        return Ok(());
    };
    let turn = turn(&workflow.id);
    let _turn = turn.lock().await;
    let started = Instant::now();
    let renewed = tokio::time::timeout(
        WORK_TIME,
        crate::app_files::hook::renew(effects, app, def, &t.settings, &t.credential, &workflow.id),
    )
    .await
    .unwrap_or_else(|_| Err(format!("the renewal took more than {WORK_TIME:?}")));
    if let Err(error) = renewed {
        say_failed(shared, topics, workflow, slot, "renew", &error, started).await;
        return Err(error);
    }
    Ok(())
}

/// A failed poll or renewal in the workflow's history, as a failed run is: why, and when.
async fn say_failed(
    shared: &Shared,
    topics: &Topics,
    workflow: &Workflow,
    slot: i64,
    what: &str,
    error: &str,
    started: Instant,
) {
    let id = format!("{what}-{slot}");
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let trigger = match &workflow.trigger {
        Trigger::App(t) => json!({ "trigger": "app", "app": t.app, "when": t.when, "polled": iso(slot) }),
        _ => json!({ "trigger": "mailbox", "polled": iso(slot) }),
    };
    let history = json!({
        "status": "failed", "error": error, "ms": ms,
        "steps": [{ "name": what, "kind": what, "attempts": 1, "ms": ms, "result": null, "error": error }],
        "run": id, "at": now_ms(), "event": null, "trigger": trigger,
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
