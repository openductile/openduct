// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A topic's own settings, by the names Kafka's clients and tools use for them.
//!
//! Only settings OpenDuct acts on are taken: a setting it would store and ignore is refused, with the reason, so
//! nobody finds out later that `kafka-configs.sh` said yes and nothing changed. Each one is checked here, in one
//! place, whoever asks for it - a topic being created, IncrementalAlterConfigs, or AlterConfigs.
//!
//! `retention.ms` is not in this list: it has been a topic's own field since the first store, and stays one.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What kind of value a setting takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingKind {
    /// A whole number of at least `min` (and `-1` where that means "no limit").
    Long { min: i64, unlimited: bool },
    /// A whole number from `min` to `max`.
    Int { min: i64, max: i64 },
    /// `cleanup.policy`: `delete`, `compact`, or both - kept as [`POLICY_DELETE`], [`POLICY_COMPACT`] or
    /// [`POLICY_BOTH`].
    Policy,
    /// One of these words, each kept as its number.
    Words(&'static [(&'static str, i64)]),
}

pub const POLICY_DELETE: i64 = 0;
pub const POLICY_COMPACT: i64 = 1;
pub const POLICY_BOTH: i64 = 2;

/// One setting a topic can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingSpec {
    pub name: &'static str,
    pub kind: SettingKind,
    /// What it is when the topic does not say.
    pub default: i64,
    pub about: &'static str,
}

pub const RETENTION_BYTES: &str = "retention.bytes";
pub const MAX_MESSAGE_BYTES: &str = "max.message.bytes";
pub const CLEANUP_POLICY: &str = "cleanup.policy";
pub const DELETE_RETENTION_MS: &str = "delete.retention.ms";
pub const MIN_COMPACTION_LAG_MS: &str = "min.compaction.lag.ms";
pub const MESSAGE_TIMESTAMP_TYPE: &str = "message.timestamp.type";
pub const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
pub const SEGMENT_MS: &str = "segment.ms";

/// `message.timestamp.type`: the producer's timestamps are kept.
pub const CREATE_TIME: i64 = 0;
/// `message.timestamp.type`: the broker stamps each batch with its own time.
pub const LOG_APPEND_TIME: i64 = 1;

/// Every setting a topic can carry beside `retention.ms`. More join it as each is honoured.
pub const TOPIC_SETTINGS: &[SettingSpec] = &[
    SettingSpec {
        name: CLEANUP_POLICY,
        kind: SettingKind::Policy,
        default: POLICY_DELETE,
        about: "delete: old data goes by retention. compact: only the newest record of each key stays. Both: both.",
    },
    SettingSpec {
        name: DELETE_RETENTION_MS,
        kind: SettingKind::Long {
            min: 0,
            unlimited: false,
        },
        default: 86_400_000,
        about: "How long a compacted topic keeps a tombstone after cleaning past it, for consumers to see it.",
    },
    SettingSpec {
        name: MIN_COMPACTION_LAG_MS,
        kind: SettingKind::Long {
            min: 0,
            unlimited: false,
        },
        default: 0,
        about: "How long a record stays uncompacted at least.",
    },
    SettingSpec {
        name: MESSAGE_TIMESTAMP_TYPE,
        kind: SettingKind::Words(&[("CreateTime", CREATE_TIME), ("LogAppendTime", LOG_APPEND_TIME)]),
        default: CREATE_TIME,
        about: "CreateTime: records keep their producer's timestamps. LogAppendTime: the broker's time when stored.",
    },
    SettingSpec {
        name: MIN_INSYNC_REPLICAS,
        kind: SettingKind::Int { min: 1, max: 32_767 },
        default: 1,
        about: "The copies that must hold a write before it is acknowledged: at most a majority of the topic's.",
    },
    SettingSpec {
        name: SEGMENT_MS,
        kind: SettingKind::Long {
            min: 1,
            unlimited: false,
        },
        default: 604_800_000,
        about: "A segment is closed once its records span this long, full or not - so compaction reaches it.",
    },
    SettingSpec {
        name: RETENTION_BYTES,
        kind: SettingKind::Long {
            min: 0,
            unlimited: true,
        },
        default: -1,
        about: "The most bytes a partition keeps: its oldest segments go when it is larger. -1: no limit.",
    },
    SettingSpec {
        name: MAX_MESSAGE_BYTES,
        kind: SettingKind::Int {
            min: 1,
            max: crate::MAX_BATCH_BYTES as i64,
        },
        default: crate::MAX_BATCH_BYTES as i64,
        about: "The largest record batch the topic takes.",
    },
];

pub fn setting_spec(name: &str) -> Option<&'static SettingSpec> {
    TOPIC_SETTINGS.iter().find(|s| s.name == name)
}

/// A value for a setting, checked: the number it means, or why it cannot be taken.
pub fn check_setting(name: &str, value: &str) -> Result<i64, String> {
    let spec =
        setting_spec(name).ok_or_else(|| format!("{name} is not a setting OpenDuct honours on a topic"))?;
    if spec.kind == SettingKind::Policy {
        let (mut delete, mut compact) = (false, false);
        for word in value.split(',').map(str::trim) {
            match word {
                "delete" => delete = true,
                "compact" => compact = true,
                _ => return Err(format!("{name} is delete, compact or both, not {value:?}")),
            }
        }
        return Ok(match (delete, compact) {
            (true, true) => POLICY_BOTH,
            (false, true) => POLICY_COMPACT,
            _ => POLICY_DELETE,
        });
    }
    if let SettingKind::Words(words) = spec.kind {
        return words
            .iter()
            .find(|(word, _)| *word == value.trim())
            .map(|(_, n)| *n)
            .ok_or_else(|| {
                let known: Vec<&str> = words.iter().map(|(word, _)| *word).collect();
                format!("{name} is one of {}, not {value:?}", known.join(", "))
            });
    }
    let n: i64 = value
        .trim()
        .parse()
        .map_err(|_| format!("{name} must be a whole number, not {value:?}"))?;
    match spec.kind {
        SettingKind::Policy | SettingKind::Words(_) => Ok(n),
        SettingKind::Long { unlimited, .. } if unlimited && n == -1 => Ok(n),
        SettingKind::Long { min, unlimited } if n < min => Err(format!(
            "{name} must be at least {min}{}, not {n}",
            if unlimited { " (or -1 for no limit)" } else { "" }
        )),
        SettingKind::Long { .. } => Ok(n),
        SettingKind::Int { min, max } if n < min || n > max => {
            Err(format!("{name} must be from {min} to {max}, not {n}"))
        }
        SettingKind::Int { .. } => Ok(n),
    }
}

/// A setting's value as Kafka's tools show it.
pub fn setting_text(name: &str, value: i64) -> String {
    match setting_spec(name).map(|s| s.kind) {
        Some(SettingKind::Policy) => match value {
            POLICY_COMPACT => "compact".to_owned(),
            POLICY_BOTH => "compact,delete".to_owned(),
            _ => "delete".to_owned(),
        },
        Some(SettingKind::Words(words)) => words
            .iter()
            .find(|(_, n)| *n == value)
            .map_or_else(|| value.to_string(), |(word, _)| (*word).to_owned()),
        _ => value.to_string(),
    }
}

/// The settings a topic was given, by name; one it was not given is at its default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopicSettings(BTreeMap<String, i64>);

impl TopicSettings {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The value the topic was given, if it was given one.
    pub fn own(&self, name: &str) -> Option<i64> {
        self.0.get(name).copied()
    }

    /// The value in force: the topic's own, or the default.
    pub fn value(&self, name: &str) -> Option<i64> {
        self.own(name).or_else(|| setting_spec(name).map(|s| s.default))
    }

    /// Set a setting from a client's text. Refused, with the reason, when it is not one OpenDuct honours or the
    /// value is not one it can take.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), String> {
        let n = check_setting(name, value)?;
        self.0.insert(name.to_owned(), n);
        Ok(())
    }

    /// Back to the default. Refused for a name that is not a setting.
    pub fn reset(&mut self, name: &str) -> Result<(), String> {
        setting_spec(name).ok_or_else(|| format!("{name} is not a setting OpenDuct honours on a topic"))?;
        self.0.remove(name);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.0.iter().map(|(k, v)| (k.as_str(), *v))
    }

    /// The most bytes a partition keeps, when the topic limits it.
    pub fn retention_bytes(&self) -> Option<u64> {
        self.own(RETENTION_BYTES).and_then(|n| u64::try_from(n).ok())
    }

    /// Only the newest record of each key stays (`cleanup.policy` has `compact`).
    pub fn compacts(&self) -> bool {
        matches!(self.value(CLEANUP_POLICY), Some(POLICY_COMPACT | POLICY_BOTH))
    }

    /// Old data goes by retention (`cleanup.policy` has `delete`): a topic that only compacts keeps its newest
    /// records for ever.
    pub fn deletes(&self) -> bool {
        !matches!(self.value(CLEANUP_POLICY), Some(POLICY_COMPACT))
    }

    pub fn delete_retention_ms(&self) -> i64 {
        self.value(DELETE_RETENTION_MS).unwrap_or(86_400_000)
    }

    pub fn min_compaction_lag_ms(&self) -> i64 {
        self.value(MIN_COMPACTION_LAG_MS).unwrap_or(0)
    }

    /// The broker stamps each batch with its own time (`message.timestamp.type=LogAppendTime`).
    pub fn log_append_time(&self) -> bool {
        self.value(MESSAGE_TIMESTAMP_TYPE) == Some(LOG_APPEND_TIME)
    }

    /// The copies that must hold a write before it is acknowledged.
    pub fn min_insync_replicas(&self) -> i64 {
        self.value(MIN_INSYNC_REPLICAS).unwrap_or(1)
    }

    /// How long a segment's records may span before it is closed.
    pub fn segment_ms(&self) -> i64 {
        self.value(SEGMENT_MS).unwrap_or(604_800_000)
    }

    /// The largest record batch the topic takes.
    pub fn max_message_bytes(&self) -> usize {
        self.value(MAX_MESSAGE_BYTES)
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(crate::MAX_BATCH_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_setting_is_taken_only_as_a_value_it_can_act_on() {
        let mut s = TopicSettings::default();
        assert_eq!(s.retention_bytes(), None, "no limit until one is set");
        s.set(RETENTION_BYTES, "1048576").unwrap();
        assert_eq!(s.retention_bytes(), Some(1_048_576));
        s.set(RETENTION_BYTES, "-1").unwrap();
        assert_eq!(s.retention_bytes(), None, "-1 is Kafka's word for no limit");
        assert!(s.set(RETENTION_BYTES, "-2").unwrap_err().contains("at least 0"));
        assert!(
            s.set(RETENTION_BYTES, "lots")
                .unwrap_err()
                .contains("whole number")
        );
        assert!(s.set(MAX_MESSAGE_BYTES, "0").unwrap_err().contains("from 1 to"));
        let too_big = (crate::MAX_BATCH_BYTES as i64 + 1).to_string();
        assert!(
            s.set(MAX_MESSAGE_BYTES, &too_big).is_err(),
            "never more than the store takes"
        );
    }

    #[test]
    fn a_setting_openduct_would_ignore_is_refused_not_kept() {
        let mut s = TopicSettings::default();
        let why = s.set("segment.jitter.ms", "5").unwrap_err();
        assert!(why.contains("not a setting OpenDuct honours"), "{why}");
        assert!(s.is_empty());
        assert!(s.reset("segment.jitter.ms").is_err());
    }

    #[test]
    fn the_cleanup_policy_is_delete_compact_or_both() {
        let mut s = TopicSettings::default();
        assert!(s.deletes() && !s.compacts(), "delete, by default");
        s.set(CLEANUP_POLICY, "compact").unwrap();
        assert!(s.compacts() && !s.deletes());
        assert_eq!(
            setting_text(CLEANUP_POLICY, s.own(CLEANUP_POLICY).unwrap()),
            "compact"
        );
        s.set(CLEANUP_POLICY, " delete , compact ").unwrap();
        assert!(s.compacts() && s.deletes());
        assert_eq!(
            setting_text(CLEANUP_POLICY, s.own(CLEANUP_POLICY).unwrap()),
            "compact,delete"
        );
        assert!(
            s.set(CLEANUP_POLICY, "squash")
                .unwrap_err()
                .contains("delete, compact or both")
        );
        assert_eq!(s.delete_retention_ms(), 86_400_000);
        s.set(DELETE_RETENTION_MS, "1000").unwrap();
        s.set(MIN_COMPACTION_LAG_MS, "250").unwrap();
        assert_eq!((s.delete_retention_ms(), s.min_compaction_lag_ms()), (1_000, 250));
        assert!(s.set(DELETE_RETENTION_MS, "-1").is_err());
    }

    #[test]
    fn a_setting_of_words_takes_only_its_words() {
        let mut s = TopicSettings::default();
        assert!(!s.log_append_time());
        s.set(MESSAGE_TIMESTAMP_TYPE, "LogAppendTime").unwrap();
        assert!(s.log_append_time());
        assert_eq!(
            setting_text(MESSAGE_TIMESTAMP_TYPE, LOG_APPEND_TIME),
            "LogAppendTime"
        );
        let why = s.set(MESSAGE_TIMESTAMP_TYPE, "whenever").unwrap_err();
        assert!(why.contains("CreateTime, LogAppendTime"), "{why}");
        assert_eq!((s.min_insync_replicas(), s.segment_ms()), (1, 604_800_000));
        s.set(MIN_INSYNC_REPLICAS, "2").unwrap();
        s.set(SEGMENT_MS, "60000").unwrap();
        assert_eq!((s.min_insync_replicas(), s.segment_ms()), (2, 60_000));
        assert!(s.set(MIN_INSYNC_REPLICAS, "0").is_err() && s.set(SEGMENT_MS, "0").is_err());
    }

    #[test]
    fn reset_puts_a_setting_back_to_its_default() {
        let mut s = TopicSettings::default();
        s.set(MAX_MESSAGE_BYTES, "4096").unwrap();
        assert_eq!(s.max_message_bytes(), 4096);
        s.reset(MAX_MESSAGE_BYTES).unwrap();
        assert_eq!(s.max_message_bytes(), crate::MAX_BATCH_BYTES);
        assert!(s.is_empty());
    }

    #[test]
    fn it_is_kept_as_a_plain_map_and_an_old_file_without_it_reads_as_none() {
        let mut s = TopicSettings::default();
        s.set(RETENTION_BYTES, "10").unwrap();
        assert_eq!(serde_json::to_string(&s).unwrap(), r#"{"retention.bytes":10}"#);
        let old: TopicSettings = serde_json::from_str("{}").unwrap();
        assert!(old.is_empty());
    }
}
