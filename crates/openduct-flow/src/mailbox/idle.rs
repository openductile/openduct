// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A mailbox trigger with `idle: true`: between its polls, the folder is listened to with IMAP's IDLE (RFC 2177), and
//! a message arriving asks for a poll at once - a scheduled time queued now, which the node that reads the schedule
//! claims and polls as it polls every other ([`super::watch`]). The waiting itself runs nothing and keeps nothing, so
//! it adds speed and takes nothing from what the polls promise: a message the waiting misses is run by the next poll.
//!
//! One node listens, the one that holds the first partition of a group of the workflow's own (a lease the broker
//! keeps), as a Redis channel is listened to. IDLE is given up and taken again every 25 minutes (servers end one after
//! 30); a server that does not offer it is said once, and the polls go on alone.

use std::sync::Arc;
use std::time::Duration;

use openduct_broker::client::{GroupState, Membership, Shared};
use tokio::sync::watch as signal;

use super::watch::MailWatch;
use crate::run::Effects;
use crate::runner::{PREFIX, Topics, prepare};

const SESSION_MS: i32 = 30_000;
const HEARTBEAT: Duration = Duration::from_secs(3);
/// How long one IDLE lasts before it is taken again.
const IDLE_FOR: Duration = Duration::from_secs(25 * 60);
/// The least time between two polls asked for.
const NUDGE_GAP: Duration = Duration::from_secs(5);
const MOST_BACKOFF: Duration = Duration::from_secs(300);

/// Listens to the folder `watch` names for workflow `id` until `stop` says so, on whichever node holds the lease.
pub async fn idle(
    id: String,
    watch: MailWatch,
    shared: Arc<Shared>,
    effects: Arc<dyn Effects>,
    mut stop: signal::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(2);
    let mut said: Option<String> = None;
    let mut member: Option<Membership> = None;
    loop {
        if *stop.borrow() {
            break;
        }
        match hold(&id, &watch, &shared, effects.as_ref(), &mut member, &mut stop).await {
            Ok(()) => backoff = Duration::from_secs(2),
            Err(e) => {
                if said.as_deref() != Some(e.as_str()) {
                    tracing::warn!(
                        workflow = id,
                        error = e,
                        "a mailbox could not be listened to between polls: the polls go on, and it is tried again"
                    );
                    said = Some(e);
                }
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    _ = stop.changed() => break,
                }
                backoff = (backoff * 2).min(MOST_BACKOFF);
            }
        }
    }
    if let (Some(m), Ok(mut client)) = (member, shared.lend().await) {
        let _ = tokio::time::timeout(Duration::from_secs(3), client.leave_group(&m)).await;
    }
}

/// Heartbeats the group until it is shared out again (or a heartbeat fails): then says so.
async fn keep_beating(shared: Arc<Shared>, joined: Membership, lost: tokio::sync::oneshot::Sender<()>) {
    let mut beat = tokio::time::interval(HEARTBEAT);
    beat.tick().await;
    loop {
        beat.tick().await;
        let state = match shared.lend().await {
            Ok(mut client) => client.heartbeat(&joined).await.ok(),
            Err(_) => None,
        };
        if state != Some(GroupState::Stable) {
            let _ = lost.send(());
            return;
        }
    }
}

async fn hold(
    id: &str,
    watch: &MailWatch,
    shared: &Arc<Shared>,
    effects: &dyn Effects,
    member: &mut Option<Membership>,
    stop: &mut signal::Receiver<bool>,
) -> Result<(), String> {
    let topics = Topics::of(id);
    let partitions = {
        let mut client = shared.lend().await.map_err(|e| e.to_string())?;
        prepare(&mut client, id).await?;
        client
            .partition_count(&topics.input)
            .await
            .map_err(|e| e.to_string())?
    };
    let joined = shared
        .lend_for_join()
        .await
        .map_err(|e| e.to_string())?
        .join_group(
            &format!("{PREFIX}{id}.idle"),
            &topics.input,
            partitions,
            SESSION_MS,
            member.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())?;
    *member = Some(joined.clone());
    let (lost_tx, mut lost) = tokio::sync::oneshot::channel();
    let beating = tokio::spawn(keep_beating(Arc::clone(shared), joined.clone(), lost_tx));
    let held = async {
        if !joined.partitions.contains(&0) {
            // Another node listens: this one keeps its place in the group.
            tokio::select! {
                _ = &mut lost => {}
                _ = stop.changed() => {}
            }
            return Ok(());
        }
        let places = effects
            .storage()
            .allows(false)
            .map_err(|_| "this node reaches no mailbox".to_owned())?
            .clone();
        let credential = effects.credential(&watch.credential)?;
        if credential.kind != "imap" {
            return Err(format!("{} is not an imap credential", watch.credential));
        }
        let mut session = super::open(effects, &watch.credential, &credential, &places).await?;
        if !session.has("IDLE") {
            tracing::info!(
                workflow = id,
                "the mail server does not offer IDLE: new messages come with each poll"
            );
            tokio::select! {
                _ = &mut lost => {}
                _ = stop.changed() => {}
            }
            return Ok(());
        }
        session.select(&watch.folder, false).await?;
        tracing::info!(
            workflow = id,
            folder = watch.folder,
            "listening to a mailbox between polls"
        );
        let mut last_nudge: Option<tokio::time::Instant> = None;
        loop {
            tokio::select! {
                news = session.idle(IDLE_FOR) => {
                    if !news? {
                        continue;
                    }
                    if let Some(at) = last_nudge {
                        let since = at.elapsed();
                        if since < NUDGE_GAP {
                            tokio::time::sleep(NUDGE_GAP - since).await;
                        }
                    }
                    last_nudge = Some(tokio::time::Instant::now());
                    let mut client = shared.lend().await.map_err(|e| e.to_string())?;
                    crate::runner::queue_scheduled(&mut client, id, crate::runner::now_ms()).await?;
                }
                _ = &mut lost => return Ok(()),
                _ = stop.changed() => return Ok(()),
            }
        }
    };
    let done = held.await;
    beating.abort();
    done
}
