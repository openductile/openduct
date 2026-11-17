// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Which broker answers what when brokers share a replicated store: a partition's data belongs to its leader,
//! consumer groups to the coordinator, and every broker's metadata names them. A broker on its own is all of them.

use kafka_protocol::messages::find_coordinator_response::Coordinator as CoordinatorEntry;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatResponse, JoinGroupResponse,
    LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;

use super::{State, s};
use crate::codes;

/// The brokers as metadata lists them, and the one coordinating groups (Kafka's controller).
pub(crate) struct Brokers {
    pub(crate) coordinator: Option<BrokerId>,
    pub(crate) brokers: Vec<MetadataResponseBroker>,
}

/// A partition as metadata describes it.
pub(crate) struct PartitionPlace {
    pub(crate) leader: Option<BrokerId>,
    pub(crate) leader_epoch: i32,
    pub(crate) replicas: Vec<BrokerId>,
}

pub(crate) fn brokers(state: &State) -> Brokers {
    let Some(view) = state.store.cluster() else {
        let node = BrokerId(state.config.node_id);
        return Brokers {
            coordinator: Some(node),
            brokers: vec![
                MetadataResponseBroker::default()
                    .with_node_id(node)
                    .with_host(s(state.config.advertised_host.clone()))
                    .with_port(state.config.advertised_port),
            ],
        };
    };
    Brokers {
        coordinator: view.coordinator.map(BrokerId),
        brokers: view
            .brokers
            .into_iter()
            .map(|b| {
                MetadataResponseBroker::default()
                    .with_node_id(BrokerId(b.id))
                    .with_host(s(b.host))
                    .with_port(b.port)
            })
            .collect(),
    }
}

/// Where a partition lives: this broker alone, or as the shared store places it.
pub(crate) fn place(state: &State, topic: &str, partition: i32) -> PartitionPlace {
    match state.store.placement(topic, partition) {
        Some(p) => PartitionPlace {
            leader: p.leader.map(BrokerId),
            leader_epoch: p.leader_epoch,
            replicas: p.replicas.into_iter().map(BrokerId).collect(),
        },
        None => {
            let node = BrokerId(state.config.node_id);
            PartitionPlace {
                leader: Some(node),
                leader_epoch: 0,
                replicas: vec![node],
            }
        }
    }
}

/// The broker that coordinates `group`: its id, host and port, or `None` while there is none.
fn coordinator(state: &State, group: &str) -> Option<(BrokerId, StrBytes, i32)> {
    let Some(view) = state.store.cluster() else {
        return Some((
            BrokerId(state.config.node_id),
            s(state.config.advertised_host.clone()),
            state.config.advertised_port,
        ));
    };
    let coordinator = state.store.group_coordinator(group)?;
    view.brokers
        .into_iter()
        .find(|b| b.id == coordinator)
        .map(|b| (BrokerId(b.id), s(b.host), b.port))
}

/// Why this broker cannot answer for a group, if it cannot: a group lives on its coordinator alone.
pub(crate) fn not_coordinator(state: &State, group: &str) -> Option<i16> {
    let view = state.store.cluster()?;
    match state.store.group_coordinator(group) {
        Some(c) if c == view.me => None,
        Some(_) => Some(codes::NOT_COORDINATOR),
        None => Some(codes::COORDINATOR_NOT_AVAILABLE),
    }
}

pub(crate) fn find_coordinator(
    state: &State,
    req: &FindCoordinatorRequest,
    version: i16,
) -> FindCoordinatorResponse {
    const GROUP: i8 = 0;
    let found = |key: &str| {
        if req.key_type == GROUP {
            coordinator(state, key)
        } else {
            None
        }
    };
    let mut res = FindCoordinatorResponse::default();
    if version >= 4 {
        res.coordinators = req
            .coordinator_keys
            .iter()
            .map(|k| {
                let mut c = CoordinatorEntry::default().with_key(k.clone());
                match found(k.as_str()) {
                    Some((node, host, port)) => {
                        c.node_id = node;
                        c.host = host;
                        c.port = port;
                    }
                    None => {
                        c.error_code = codes::COORDINATOR_NOT_AVAILABLE;
                        c.node_id = BrokerId(-1);
                        c.port = -1;
                    }
                }
                c
            })
            .collect();
    } else if let Some((node, host, port)) = found(req.key.as_str()) {
        res.node_id = node;
        res.host = host;
        res.port = port;
    } else {
        res.error_code = codes::COORDINATOR_NOT_AVAILABLE;
        res.node_id = BrokerId(-1);
        res.port = -1;
    }
    res
}

// Answers to group requests sent to a broker that does not coordinate: the client looks for the coordinator again.

pub(crate) fn join_refused(code: i16, version: i16) -> JoinGroupResponse {
    let mut res = JoinGroupResponse::default()
        .with_error_code(code)
        .with_generation_id(-1);
    if version < 7 {
        // Before v7 the protocol name is not nullable.
        res.protocol_name = Some(StrBytes::default());
    }
    res
}

pub(crate) fn sync_refused(code: i16) -> SyncGroupResponse {
    SyncGroupResponse::default().with_error_code(code)
}

pub(crate) fn heartbeat_refused(code: i16) -> HeartbeatResponse {
    HeartbeatResponse::default().with_error_code(code)
}

pub(crate) fn leave_refused(code: i16) -> LeaveGroupResponse {
    LeaveGroupResponse::default().with_error_code(code)
}

pub(crate) fn offset_commit_refused(code: i16, req: &OffsetCommitRequest) -> OffsetCommitResponse {
    OffsetCommitResponse::default().with_topics(
        req.topics
            .iter()
            .map(|t| {
                OffsetCommitResponseTopic::default()
                    .with_name(t.name.clone())
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(code)
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

pub(crate) fn offset_fetch_refused(code: i16, req: &OffsetFetchRequest, version: i16) -> OffsetFetchResponse {
    if version >= 8 {
        return OffsetFetchResponse::default().with_groups(
            req.groups
                .iter()
                .map(|g| {
                    OffsetFetchResponseGroup::default()
                        .with_group_id(g.group_id.clone())
                        .with_error_code(code)
                })
                .collect(),
        );
    }
    let mut res = OffsetFetchResponse::default();
    if version >= 2 {
        res.error_code = code;
    } else {
        // v0 and v1 carry errors per partition only.
        res.topics = req
            .topics
            .iter()
            .flatten()
            .map(|t| {
                OffsetFetchResponseTopic::default()
                    .with_name(t.name.clone())
                    .with_partitions(
                        t.partition_indexes
                            .iter()
                            .map(|p| {
                                OffsetFetchResponsePartition::default()
                                    .with_partition_index(*p)
                                    .with_committed_offset(-1)
                                    .with_error_code(code)
                            })
                            .collect(),
                    )
            })
            .collect();
    }
    res
}
