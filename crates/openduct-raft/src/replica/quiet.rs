// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A partition with one replica - this broker, leading it - has nothing to time: no follower to show itself alive to,
//! no election to hold. Its Raft clock (heartbeats and the election timer) is stopped while it is so, and started
//! again the moment it is not: a replica added, the partition moved, its leadership lost. A node holding thousands of
//! single-copy partitions then costs nothing while they are idle, where each ticked five times a second.

use openraft::async_runtime::WatchReceiver;

use crate::types::PartitionRaft;

/// Whether a group led by `me` holds `me` alone, as its metrics say.
fn alone(raft: &PartitionRaft, me: u64) -> bool {
    let metrics = raft.metrics();
    let m = metrics.borrow_watched();
    let membership = m.membership_config.membership();
    m.current_leader == Some(me)
        && membership.voter_ids().eq([me])
        && membership.non_write_voter_ids().next().is_none()
}

/// Keeps `raft`'s clock stopped while this broker is its only replica and leads it; ends when the group stops.
pub(crate) async fn quiet_when_alone(raft: PartitionRaft, me: u64) {
    let mut metrics = raft.metrics();
    loop {
        raft.runtime_config().tick(!alone(&raft, me));
        if metrics.changed().await.is_err() {
            return;
        }
    }
}
