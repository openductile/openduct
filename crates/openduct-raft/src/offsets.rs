// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Consumer groups' committed offsets, spread over the partitions of an internal topic - each its own Raft group,
//! led by whichever broker leads it - instead of all going through the controller. A group belongs to one partition
//! (by a hash of its name), and that partition's leader is the group's coordinator: it takes the group's joins,
//! heartbeats and commits. Commits then scale with the partitions and the brokers, and a busy group slows only its
//! own partition.
//!
//! The topic is made by the controller's leader once the cluster has enough brokers for its copies; until then
//! offsets go through the controller as before, and offsets committed that way are still read.

pub use openduct_storage::{OFFSETS_TOPIC, is_internal_topic};

/// The partition of the offsets topic a group belongs to. Part of the stored format: a different function would
/// move every group, so it never changes (FNV-1a, 32 bits).
pub fn partition_of(group: &str, partitions: i32) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in group.as_bytes() {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    let n = u32::try_from(partitions.max(1)).unwrap_or(1);
    i32::try_from(hash % n).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_always_lands_on_the_same_partition() {
        // Fixed: these never change (see partition_of).
        assert_eq!(partition_of("", 16), 5);
        assert_eq!(partition_of("billing", 16), 12);
        assert_eq!(partition_of("gateway-doors", 16), 5);
        assert_eq!(partition_of("anything", 1), 0);
        let spread: std::collections::BTreeSet<i32> =
            (0..200).map(|i| partition_of(&format!("g{i}"), 16)).collect();
        assert_eq!(spread.len(), 16, "groups spread over every partition");
    }
}
