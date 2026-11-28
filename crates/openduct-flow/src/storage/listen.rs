// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `redis` trigger: a run for each message of a channel. Redis gives a channel's messages to every subscriber, so
//! in a cluster only one node subscribes: the one that holds the first partition in a group of the workflow's own,
//! joined by every node (a lease the broker keeps). When nodes come or go the group is shared out again, the node
//! that held it lets the subscription go, and the one that holds it now subscribes. Each message is written to the
//! workflow's own topic, where it runs as any other event, recorded in its history.
//!
//! A channel keeps nothing: what is published while no node is subscribed - while one takes over from another, or the
//! connection is made again after a failure - is not seen, as Redis itself says of its channels. For messages that must
//! not be lost, push them to a list and pop it in a scheduled workflow instead.

use std::sync::Arc;
use std::time::Duration;

use openduct_broker::client::{GroupState, Membership, Shared};
use serde_json::json;
use tokio::sync::watch as signal;

use super::watch::ChannelWatch;
use crate::run::Effects;
use crate::runner::{PREFIX, Topics, iso, prepare, queue};

const SESSION_MS: i32 = 30_000;
const HEARTBEAT: Duration = Duration::from_secs(3);
/// The longest wait before connecting again after a failure.
const MOST_BACKOFF: Duration = Duration::from_secs(60);
/// The largest message that starts a run, in bytes as JSON: a larger one is passed by, and said.
const LARGEST_MESSAGE: usize = 1024 * 1024;

/// Listens to the channel `watch` names for workflow `id` until `stop` says so, on whichever node holds the lease.
pub async fn listen(
    id: String,
    watch: ChannelWatch,
    shared: Arc<Shared>,
    effects: Arc<dyn Effects>,
    mut stop: signal::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(1);
    let mut said: Option<String> = None;
    let mut member: Option<Membership> = None;
    loop {
        if *stop.borrow() {
            break;
        }
        match hold(&id, &watch, &shared, effects.as_ref(), &mut member, &mut stop).await {
            Ok(()) => backoff = Duration::from_secs(1),
            Err(e) => {
                if said.as_deref() != Some(e.as_str()) {
                    tracing::warn!(
                        workflow = id,
                        error = e,
                        "a Redis channel could not be listened to: trying again"
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

/// Holds the group's first partition, if it is this node's, and listens while it does. Returns when the group is
/// shared out again (to join again), when `stop` says so, or on a failure.
async fn hold(
    id: &str,
    watch: &ChannelWatch,
    shared: &Shared,
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
            &format!("{PREFIX}{id}.listen"),
            &topics.input,
            partitions,
            SESSION_MS,
            member.as_ref(),
        )
        .await
        .map_err(|e| e.to_string())?;
    *member = Some(joined.clone());
    let mut beat = tokio::time::interval(HEARTBEAT);
    beat.tick().await;
    if !joined.partitions.contains(&0) {
        // Another node listens: this one waits, keeping its place in the group.
        loop {
            tokio::select! {
                _ = beat.tick() => {
                    let mut client = shared.lend().await.map_err(|e| e.to_string())?;
                    if client.heartbeat(&joined).await.map_err(|e| e.to_string())? == GroupState::Rebalancing {
                        return Ok(());
                    }
                }
                _ = stop.changed() => return Ok(()),
            }
        }
    }
    let credential = effects.credential(&watch.credential)?;
    if credential.kind != "redis" {
        return Err(format!(
            "{} is a {} credential: a redis trigger takes a redis one",
            watch.credential, credential.kind
        ));
    }
    let places = effects
        .storage()
        .allows(false)
        .map_err(|_| "this node reaches no Redis".to_owned())?;
    let mut subscription = super::redis::Subscription::open(&credential, places, &watch.channel).await?;
    tracing::info!(
        workflow = id,
        channel = watch.channel,
        "listening to a Redis channel"
    );
    loop {
        tokio::select! {
            message = subscription.next() => {
                let (channel, value) = message?;
                let value: serde_json::Value = serde_json::from_str(&value.to_json()).unwrap_or_default();
                if value.to_string().len() > LARGEST_MESSAGE {
                    tracing::warn!(workflow = id, channel, "a message of more than 1 MB was passed by");
                    continue;
                }
                let details = json!({
                    "trigger": "redis", "channel": channel, "subscribed": watch.channel,
                    "at": iso(crate::runner::now_ms()),
                });
                let mut client = shared.lend().await.map_err(|e| e.to_string())?;
                queue(&mut client, id, &value, &details).await?;
            }
            _ = beat.tick() => {
                let mut client = shared.lend().await.map_err(|e| e.to_string())?;
                if client.heartbeat(&joined).await.map_err(|e| e.to_string())? == GroupState::Rebalancing {
                    return Ok(());
                }
            }
            _ = stop.changed() => return Ok(()),
        }
    }
}
