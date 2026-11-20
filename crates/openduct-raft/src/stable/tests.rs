// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Golden files: a sample of every format Raft keeps or sends, checked byte for byte against the files of each
//! format kept in `tests/golden/`. A change that would make a format unreadable by the version before fails here.
//! Run with `OPENDUCT_WRITE_GOLDEN=1` only when a new format is added on purpose - then the new files sit beside
//! the old ones, which every later build must still read.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use bytes::Bytes;
use openduct_storage::batch::BatchError;
use openduct_storage::{CommittedOffset, Manifest, ManifestFile, TieredSegment, TopicConfig, TopicInfo};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::*;
use crate::controller::{BrokerRecord, ControllerState, TopicRecord};
use crate::members::{Forwarded, LeaderOf};
use crate::net::{
    Answer, Envelope, EnvelopeRef, Hello, PeerInfo, Purpose, Request, Response, SnapshotHeader,
};
use crate::replica::Identity;
use crate::snapshot::StoredRecord;
use crate::types::{GroupId, Refusal, Reply};

fn golden(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/v1")
        .join(name)
}

/// Checks `bytes` against the golden file `name` (or writes it, when asked to).
fn check(name: &str, bytes: &[u8]) {
    let path = golden(name);
    if std::env::var("OPENDUCT_WRITE_GOLDEN").is_ok() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        return;
    }
    let kept = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("{}: {e} (a new format needs its golden file)", path.display()));
    assert_eq!(kept, bytes, "{name} is not written as format 1 was");
}

/// A wire message: written as its golden file, and that file read back and written again unchanged.
fn wire<T: Serialize + DeserializeOwned>(name: &str, value: &T) {
    let bytes = postcard::to_stdvec(value).unwrap();
    check(&format!("wire-{name}.bin"), &bytes);
    let read: T = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(
        postcard::to_stdvec(&read).unwrap(),
        bytes,
        "{name} does not read back as written"
    );
}

/// A JSON file, the same way.
fn file<T: Serialize + DeserializeOwned>(name: &str, value: &T) {
    let bytes = serde_json::to_vec(value).unwrap();
    check(&format!("file-{name}.json"), &bytes);
    let read: T = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        serde_json::to_vec(&read).unwrap(),
        bytes,
        "{name} does not read back as written"
    );
}

fn log_id(term: u64, node: u64, index: u64) -> LogId {
    LogId { term, node, index }
}

fn vote(term: u64, node: u64, committed: bool) -> Vote {
    Vote {
        term,
        node,
        committed,
    }
}

fn broker(i: u64) -> Broker {
    Broker {
        raft: format!("10.0.0.{i}:9093"),
        kafka: format!("broker-{i}.internal:9092"),
    }
}

fn membership(configs: &[&[u64]]) -> Membership {
    let configs: Vec<BTreeSet<u64>> = configs.iter().map(|c| c.iter().copied().collect()).collect();
    let nodes = configs.iter().flatten().map(|id| (*id, broker(*id))).collect();
    Membership { configs, nodes }
}

fn topic_id() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)
}

fn offset() -> CommittedOffset {
    CommittedOffset {
        topic: "orders".to_owned(),
        partition: 2,
        offset: 41,
        metadata: Some("m".to_owned()),
        commit_timestamp_ms: 1_700_000_000_000,
    }
}

fn segment() -> TieredSegment {
    TieredSegment {
        base: 100,
        last: 199,
        max_timestamp: 1_700_000_000_500,
        size: 4096,
        key: "openduct/prod/t/0/00000000000000000100-00000000000000000199.log".to_owned(),
    }
}

/// Every command, by name. The match has no catch-all: a command added without a golden sample does not compile.
fn commands() -> Vec<(&'static str, Command)> {
    let all = vec![
        Command::CreateTopic {
            id: topic_id(),
            name: "orders".to_owned(),
            config: TopicConfig {
                partitions: 3,
                retention_ms: Some(86_400_000),
            },
            replicas: vec![vec![1, 2, 3], vec![2, 3, 1], vec![3, 1, 2]],
        },
        Command::DeleteTopic {
            name: "orders".to_owned(),
        },
        Command::NextProducerId,
        Command::CommitOffsets {
            group: "billing".to_owned(),
            offsets: vec![offset()],
        },
        Command::Reassign {
            topic: "orders".to_owned(),
            partition: 1,
            replicas: vec![4, 2, 3],
        },
        Command::Reassigned {
            topic: "orders".to_owned(),
            partition: 1,
            replicas: vec![4, 2, 3],
        },
        Command::Append {
            batch: Bytes::from_static(b"\\x00\\x01a record batch, as the producer sent it"),
        },
        Command::Retain {
            before_ms: 1_699_000_000_000,
        },
        Command::Tiered { segment: segment() },
        Command::Offload { before: 200 },
        Command::Register {
            id: 2,
            version: "0.1.0".to_owned(),
            supported_level: 1,
        },
        Command::RaiseLevel { level: 1 },
        Command::DeleteOffsets {
            group: "billing".to_owned(),
            partitions: Some(vec![("orders".to_owned(), 2)]),
        },
        Command::SetRetention {
            name: "orders".to_owned(),
            retention_ms: Some(3_600_000),
        },
        Command::AddPartitions {
            name: "orders".to_owned(),
            total: 5,
            replicas: vec![vec![1, 2, 3], vec![2, 3, 1]],
        },
        Command::DeleteRecords { before: 120 },
        Command::SetTopicSettings {
            name: "orders".to_owned(),
            settings: {
                let mut s = openduct_storage::TopicSettings::default();
                s.set(openduct_storage::RETENTION_BYTES, "1073741824").unwrap();
                s.set(openduct_storage::MAX_MESSAGE_BYTES, "65536").unwrap();
                s
            },
        },
        Command::RetainBytes { keep: 1_073_741_824 },
        Command::Compact {
            now_ms: 1_699_000_000_000,
            lag_ms: 60_000,
            delete_retention_ms: 86_400_000,
        },
        Command::Roll,
    ];
    all.into_iter()
        .map(|c| {
            let name = match &c {
                Command::CreateTopic { .. } => "create-topic",
                Command::DeleteTopic { .. } => "delete-topic",
                Command::NextProducerId => "next-producer-id",
                Command::CommitOffsets { .. } => "commit-offsets",
                Command::Reassign { .. } => "reassign",
                Command::Reassigned { .. } => "reassigned",
                Command::Append { .. } => "append",
                Command::Retain { .. } => "retain",
                Command::Tiered { .. } => "tiered",
                Command::Offload { .. } => "offload",
                Command::Register { .. } => "register",
                Command::RaiseLevel { .. } => "raise-level",
                Command::DeleteOffsets { .. } => "delete-offsets",
                Command::SetRetention { .. } => "set-retention",
                Command::AddPartitions { .. } => "add-partitions",
                Command::DeleteRecords { .. } => "delete-records",
                Command::SetTopicSettings { .. } => "set-topic-settings",
                Command::RetainBytes { .. } => "retain-bytes",
                Command::Compact { .. } => "compact",
                Command::Roll => "roll",
            };
            (name, c)
        })
        .collect()
}

fn entries() -> Vec<(String, Entry)> {
    let mut out: Vec<(String, Entry)> = commands()
        .into_iter()
        .enumerate()
        .map(|(i, (name, c))| {
            let index = u64::try_from(i).unwrap() + 10;
            (
                format!("entry-{name}"),
                Entry {
                    log_id: log_id(3, 1, index),
                    payload: Payload::Command(c),
                },
            )
        })
        .collect();
    out.push((
        "entry-blank".to_owned(),
        Entry {
            log_id: log_id(3, 1, 1),
            payload: Payload::Blank,
        },
    ));
    out.push((
        "entry-membership".to_owned(),
        Entry {
            log_id: log_id(1, 1, 0),
            payload: Payload::Membership(membership(&[&[1, 2, 3], &[2, 3, 4]])),
        },
    ));
    out
}

#[test]
fn every_kind_of_log_entry_is_written_and_read_as_format_1() {
    for (name, entry) in entries() {
        let raft = entry.clone().into_raft().unwrap();
        let bytes = crate::log::encode_entry(&raft).unwrap();
        assert_eq!(bytes.first(), Some(&FORMAT));
        check(&format!("{name}.bin"), &bytes);
        let read = crate::log::decode(&bytes).unwrap();
        assert_eq!(Entry::from(&read), entry, "{name} does not read back as written");
    }
}

#[test]
fn a_frame_of_a_newer_format_is_refused_not_misread() {
    let (_, entry) = entries().remove(0);
    let mut bytes = crate::log::encode_entry(&entry.into_raft().unwrap()).unwrap();
    bytes[0] = FORMAT + 1;
    let e = crate::log::decode(&bytes).unwrap_err().to_string();
    assert!(e.contains("newer OpenDuct"), "{e}");
}

#[test]
fn every_message_between_brokers_is_written_and_read_as_format_1() {
    let entry = entries().remove(0).1;
    let requests = vec![
        (
            "append",
            Request::Append(Append {
                vote: vote(3, 1, true),
                prev_log_id: Some(log_id(3, 1, 9)),
                entries: vec![entry],
                leader_commit: Some(log_id(3, 1, 8)),
            }),
        ),
        (
            "vote",
            Request::Vote(VoteAsk {
                vote: vote(4, 2, false),
                last_log_id: Some(log_id(3, 1, 9)),
                handed_over_by: None,
            }),
        ),
        (
            "pre-vote",
            Request::PreVote(VoteAsk {
                vote: vote(4, 2, false),
                last_log_id: None,
                handed_over_by: Some(vote(3, 1, true)),
            }),
        ),
        (
            "transfer-leader",
            Request::TransferLeader(Handover {
                from_leader: vote(3, 1, true),
                to: 2,
                last_log_id: Some(log_id(3, 1, 9)),
            }),
        ),
        ("forward", Request::Forward(Command::NextProducerId)),
        (
            "join",
            Request::Join {
                id: 4,
                broker: broker(4),
            },
        ),
        ("leave", Request::Leave { id: 4 }),
        ("leaders", Request::Leaders),
    ];
    for (name, r) in &requests {
        wire(&format!("request-{name}"), r);
    }
    let responses = vec![
        ("append-ok", Response::Append(Ok(Some(log_id(3, 1, 10))))),
        (
            "append-conflict",
            Response::Append(Err(AppendRefusal::Conflict(log_id(2, 1, 5)))),
        ),
        (
            "append-higher-vote",
            Response::Append(Err(AppendRefusal::HigherVote(vote(5, 3, true)))),
        ),
        (
            "vote",
            Response::Vote(VoteAnswer {
                vote: vote(4, 2, false),
                granted: true,
                last_log_id: Some(log_id(3, 1, 9)),
            }),
        ),
        ("snapshot", Response::Snapshot(vote(3, 1, true))),
        ("transfer-ok", Response::TransferLeader(Ok(()))),
        (
            "transfer-vote-changed",
            Response::TransferLeader(Err(HandoverRefusal::VoteChanged {
                expected: vote(3, 1, true),
                actual: vote(4, 2, false),
            })),
        ),
        (
            "transfer-not-flushed",
            Response::TransferLeader(Err(HandoverRefusal::LogNotFlushed {
                expected: Some(log_id(3, 1, 9)),
                actual: None,
            })),
        ),
        ("membership-ok", Response::Membership(Ok(()))),
        (
            "membership-not-leader",
            Response::Membership(Err(Forwarded::NotLeader { leader: Some(2) })),
        ),
        ("forward-ok", Response::Forward(Ok((Reply::ProducerId(7), 42)))),
        (
            "forward-appended",
            Response::Forward(Ok((
                Reply::Appended {
                    base_offset: 5,
                    log_start_offset: 0,
                    duplicate: false,
                },
                43,
            ))),
        ),
        ("forward-done", Response::Forward(Ok((Reply::Done, 44)))),
        ("forward-removed", Response::Forward(Ok((Reply::Removed(3), 45)))),
        (
            "forward-refused",
            Response::Forward(Ok((
                Reply::Refused(Refusal::TopicExists("orders".to_owned())),
                46,
            ))),
        ),
        (
            "forward-refused-batch",
            Response::Forward(Ok((Reply::Refused(Refusal::Batch(BatchError::CrcMismatch)), 47))),
        ),
        (
            "forward-unavailable",
            Response::Forward(Err(Forwarded::Unavailable("no leader".to_owned()))),
        ),
        (
            "leaders",
            Response::Leaders(vec![LeaderOf {
                group: GroupId::Partition {
                    topic: topic_id(),
                    partition: 1,
                },
                leader: 2,
                term: 3,
            }]),
        ),
        ("failed", Response::Failed("this broker holds no copy".to_owned())),
    ];
    for (name, r) in &responses {
        wire(&format!("response-{name}"), r);
    }
    wire(
        "hello",
        &Hello {
            protocols: (3, 3),
            supported_level: 1,
            cluster: "prod".to_owned(),
            secret: "a-cluster-secret".to_owned(),
            from: 1,
            purpose: Purpose::Messages,
        },
    );
    wire(
        "peer-info",
        &PeerInfo {
            incarnation: topic_id(),
            controller_committed: Some(12),
            protocol: 3,
            supported_level: 1,
            active_level: 1,
        },
    );
    wire(
        "snapshot-header",
        &SnapshotHeader {
            group: GroupId::Partition {
                topic: topic_id(),
                partition: 0,
            },
            vote: vote(3, 1, true),
            meta: snapshot_meta(),
            manifest: manifest(),
        },
    );
    let envelope = postcard::to_stdvec(&EnvelopeRef {
        id: 9,
        group: GroupId::Controller,
        request: &Request::Leaders,
    })
    .unwrap();
    check("wire-envelope.bin", &envelope);
    let read: Envelope = postcard::from_bytes(&envelope).unwrap();
    assert_eq!((read.id, read.group), (9, GroupId::Controller));
    wire(
        "answer",
        &Answer {
            id: 9,
            response: Response::Leaders(Vec::new()),
        },
    );
}

fn snapshot_meta() -> SnapshotMeta {
    SnapshotMeta {
        last_log_id: Some(log_id(3, 1, 120)),
        last_membership: AppliedMembership {
            log_id: Some(log_id(1, 1, 0)),
            membership: membership(&[&[1, 2, 3]]),
        },
    }
}

fn manifest() -> Manifest {
    Manifest {
        files: vec![
            ManifestFile {
                path: "00000000000000000000.log".to_owned(),
                len: 4096,
            },
            ManifestFile {
                path: "producers.json".to_owned(),
                len: 52,
            },
        ],
    }
}

#[test]
fn every_small_file_raft_keeps_is_written_and_read_as_format_1() {
    file("vote", &Versioned::current(vote(3, 1, true)));
    file("committed", &Versioned::current(Some(log_id(3, 1, 120))));
    file("purged", &Versioned::current(log_id(3, 1, 100)));
    file(
        "snapshot",
        &Versioned::current(StoredRecord {
            meta: snapshot_meta(),
            manifest: manifest(),
        }),
    );
    let mut state = ControllerState::default();
    state.topics.insert(
        "orders".to_owned(),
        TopicRecord {
            info: TopicInfo {
                id: topic_id(),
                name: "orders".to_owned(),
                partitions: 1,
                retention_ms: None,
                settings: Default::default(),
            },
            replicas: vec![vec![1, 2, 3]],
            created_at: 4,
            moving: BTreeMap::from([(0, vec![4, 2, 3])]),
            added_at: Vec::new(),
        },
    );
    state.next_producer_id = 8;
    state.offsets.insert("billing".to_owned(), vec![offset()]);
    state.brokers.insert(
        1,
        BrokerRecord {
            version: "0.1.0".to_owned(),
            supported_level: 1,
        },
    );
    state.level = 1;
    file("controller-state", &Versioned::current(state.clone()));
    // A topic grown by CreatePartitions: the partitions added, with the entry that added each.
    if let Some(t) = state.topics.get_mut("orders") {
        t.replicas.push(vec![2, 3, 1]);
        t.info.partitions = 2;
        t.added_at = vec![9];
    }
    file("controller-state-grown", &Versioned::current(state));
    file(
        "broker",
        &Versioned::current(Identity {
            id: 1,
            cluster: "prod".to_owned(),
            incarnation: topic_id(),
        }),
    );
}

#[test]
fn a_file_of_a_newer_format_is_refused_not_misread() {
    let newer = Versioned {
        format: 2,
        value: vote(3, 1, true),
    };
    assert!(newer.open("vote").unwrap_err().contains("newer OpenDuct"));
}
