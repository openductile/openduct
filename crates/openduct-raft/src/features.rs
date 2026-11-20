// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Upgrading a cluster one broker at a time: which protocol two brokers speak, and which features the cluster uses.
//!
//! - **Protocol.** Each broker speaks a range of protocol versions between brokers - the current one and the one
//!   before it - and two brokers agree on the newest both speak. A broker of the current version and one of the
//!   previous run side by side while a cluster is upgraded.
//! - **Feature level.** A broker's build supports a feature level; every broker registers its own with the
//!   controller, and the controller's leader raises the cluster's level only once every broker of the cluster
//!   supports it. A new behaviour - a new command, a new message, a newer file format - starts only at its level,
//!   so a broker of the previous version never receives what it cannot read. The level never goes down: a broker
//!   whose build is older than the level in use is refused at every connection, and so gets nothing it would
//!   misread, until it is upgraded.

use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU32, Ordering};

/// The protocol versions between brokers this build speaks: the current one and the one before it, once there is
/// one. Version 3 is the first with OpenDuct's own shapes on the wire (nothing earlier was released).
pub(crate) const PROTOCOLS: RangeInclusive<u16> = 3..=3;

/// The feature level this build supports. Level 1: everything up to the first release. Level 2: a topic's own
/// settings (`TOPIC_SETTINGS_LEVEL`). Level 3: log compaction (`COMPACTION_LEVEL`). Level 4: transactions
/// (`TRANSACTIONS_LEVEL`).
pub const SUPPORTED_LEVEL: u32 = 4;

/// From this level a partition's log may hold a transaction's records and its markers: a broker of an earlier
/// level refuses both when it applies them, so none is proposed before every broker reads them.
pub(crate) const TRANSACTIONS_LEVEL: u32 = 4;

/// From this level a partition can be told to compact (the Compact command) and a topic's `cleanup.policy` can
/// have `compact`; and to start its next segment (the Roll command, `segment.ms`).
pub(crate) const COMPACTION_LEVEL: u32 = 3;

/// From this level a topic carries settings beside retention.ms (the SetTopicSettings and RetainBytes commands,
/// and `settings` in a topic's record): a broker of level 1 could read none of them.
pub(crate) const TOPIC_SETTINGS_LEVEL: u32 = 2;

/// The newest protocol both ends speak, if any.
pub(crate) fn agree(ours: &RangeInclusive<u16>, theirs: (u16, u16)) -> Option<u16> {
    let (low, high) = ((*ours.start()).max(theirs.0), (*ours.end()).min(theirs.1));
    (low <= high).then_some(high)
}

/// The levels on this broker: what its build supports, and what the cluster uses now (as its controller last
/// applied).
#[derive(Debug)]
pub(crate) struct Levels {
    supported: u32,
    active: AtomicU32,
}

impl Levels {
    pub(crate) fn new(supported: u32) -> Self {
        Self {
            supported,
            active: AtomicU32::new(0),
        }
    }

    pub(crate) fn supported(&self) -> u32 {
        self.supported
    }

    pub(crate) fn active(&self) -> u32 {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn set_active(&self, level: u32) {
        self.active.fetch_max(level, Ordering::AcqRel);
    }

    /// Why a broker whose build supports `level` gets nothing from this cluster, if it does not.
    pub(crate) fn refuses(&self, level: u32) -> Option<String> {
        let active = self.active();
        (level < active).then(|| {
            format!("the cluster uses feature level {active}; that broker's build supports only {level}: upgrade it")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brokers_speak_the_newest_protocol_both_know() {
        assert_eq!(
            agree(&(3..=4), (3, 3)),
            Some(3),
            "a new broker speaks down to the previous one"
        );
        assert_eq!(agree(&(3..=3), (3, 4)), Some(3));
        assert_eq!(agree(&(4..=5), (4, 5)), Some(5));
        assert_eq!(
            agree(&(5..=6), (3, 4)),
            None,
            "two versions apart: no common protocol"
        );
    }

    #[test]
    fn a_build_older_than_the_level_in_use_is_refused_and_the_level_never_goes_down() {
        let levels = Levels::new(2);
        levels.set_active(2);
        levels.set_active(1);
        assert_eq!(levels.active(), 2);
        assert!(levels.refuses(1).is_some());
        assert!(levels.refuses(2).is_none());
    }
}
