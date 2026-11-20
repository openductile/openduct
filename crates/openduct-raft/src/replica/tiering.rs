// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The leader's part of tiered storage (see `tier`): store closed segments, drop local copies past the local
//! retention, delete what retention removed.

use std::sync::Arc;

use chrono::Utc;
use openduct_storage::TieredSegment;

use super::Inner;
use crate::members::Member;
use crate::tier::TierConfig;
use crate::types::{Command, GroupId, PartitionRaft, Reply};

/// Looks after every partition's tiers, as often as the settings say, until the broker stops.
pub(super) async fn tend(inner: Arc<Inner>, tier: TierConfig) {
    loop {
        tokio::time::sleep(tier.every).await;
        for (group, member) in inner.members.all() {
            let (GroupId::Partition { topic, partition }, Member::Partition(raft, shared)) = (group, member)
            else {
                continue;
            };
            let log = shared.log();
            // Retention runs on every copy: each deletes what its retention removed (a delete of a deleted
            // object is no error).
            for key in log.take_garbage() {
                if let Err(e) = tier.s3.delete(&key).await {
                    tracing::warn!(key, error = %e, "cannot delete a segment retention removed");
                }
            }
            if inner.members.leader_of(&group).map(|(l, _)| l) != Some(inner.id) {
                continue;
            }
            // A compacted topic stays on the local disk, as in Kafka: its segments are written again as records
            // are replaced, and object storage holds a segment as it was when stored.
            let compacts = inner
                .control
                .state()
                .topics
                .get(&topic.to_string())
                .is_some_and(|t| t.info.settings.compacts());
            if compacts {
                continue;
            }
            if let Err(e) = store(&raft, &tier, &topic.to_string(), partition, &log).await {
                tracing::warn!(%group, error = %e, "storing segments goes on later");
            }
            if let Err(e) = offload(&raft, &tier, &log).await {
                tracing::warn!(%group, error = %e, "dropping local copies goes on later");
            }
        }
    }
}

async fn write(raft: &PartitionRaft, command: Command) -> Result<Reply, String> {
    match raft.client_write(command).await {
        Ok(written) => match written.data {
            Reply::Refused(r) => Err(format!("{r:?}")),
            reply => Ok(reply),
        },
        Err(e) => Err(e.to_string()),
    }
}

/// Copies each closed segment not yet stored to object storage, oldest first, and records it.
async fn store(
    raft: &PartitionRaft,
    tier: &TierConfig,
    topic: &str,
    partition: i32,
    log: &openduct_storage::PartitionLog,
) -> Result<(), String> {
    for seg in log.local_segments().into_iter().filter(|s| !s.tiered) {
        let bytes = tokio::fs::read(&seg.path).await.map_err(|e| e.to_string())?;
        if bytes.len() as u64 != seg.size {
            // Still being trimmed or rebuilt: next time.
            return Ok(());
        }
        let key = format!(
            "{}/{topic}/{partition}/{:020}-{:020}.log",
            tier.prefix, seg.base, seg.last
        );
        tier.s3.put(&key, bytes).await?;
        let segment = TieredSegment {
            base: seg.base,
            last: seg.last,
            max_timestamp: seg.max_timestamp,
            size: seg.size,
            key,
        };
        write(raft, Command::Tiered { segment }).await?;
    }
    Ok(())
}

/// Drops the oldest stored segments' local copies while the partition keeps more than its local retention.
async fn offload(
    raft: &PartitionRaft,
    tier: &TierConfig,
    log: &openduct_storage::PartitionLog,
) -> Result<(), String> {
    let local = log.local_segments();
    let mut kept: u64 = local.iter().map(|s| s.size).sum();
    let cutoff = tier
        .local_retention_ms
        .map(|ms| Utc::now().timestamp_millis().saturating_sub(ms));
    let mut before = None;
    for seg in local.iter().take_while(|s| s.tiered) {
        let too_old = cutoff.is_some_and(|c| seg.max_timestamp < c);
        if kept <= tier.local_retention_bytes && !too_old {
            break;
        }
        kept = kept.saturating_sub(seg.size);
        before = Some(seg.last + 1);
    }
    if let Some(before) = before {
        write(raft, Command::Offload { before }).await?;
    }
    Ok(())
}
