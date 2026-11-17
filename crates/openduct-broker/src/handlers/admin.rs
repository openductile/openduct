// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Moving partitions and removing brokers (AlterPartitionReassignments, ListPartitionReassignments,
//! UnregisterBroker), as Kafka's own admin tools ask for them. A broker on its own refuses them: it holds every
//! partition, and is the whole cluster.

use kafka_protocol::messages::alter_partition_reassignments_response::{
    ReassignablePartitionResponse, ReassignableTopicResponse,
};
use kafka_protocol::messages::list_partition_reassignments_response::{
    OngoingPartitionReassignment, OngoingTopicReassignment,
};
use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, BrokerId,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, TopicName,
    UnregisterBrokerRequest, UnregisterBrokerResponse,
};
use openduct_storage::Reassignment;

use super::{State, blocking, s};
use crate::codes;

pub(crate) async fn alter_partition_reassignments(
    state: &State,
    req: &AlterPartitionReassignmentsRequest,
) -> AlterPartitionReassignmentsResponse {
    let mut responses = Vec::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let mut out = ReassignablePartitionResponse::default().with_partition_index(p.partition_index);
            match &p.replicas {
                // A move under way is not called back: it finishes, and can be moved again.
                None => {
                    out.error_code = codes::INVALID_REPLICA_ASSIGNMENT;
                    out.error_message = Some(s(
                        "a move under way cannot be cancelled; move the partition again",
                    ));
                }
                Some(replicas) => {
                    let (topic, partition) = (name.clone(), p.partition_index);
                    let replicas: Vec<i32> = replicas.iter().map(|b| b.0).collect();
                    if let Err(e) =
                        blocking(&state.store, move |st| st.reassign(&topic, partition, replicas)).await
                    {
                        out.error_code = codes::of_store_error(&e);
                        out.error_message = Some(s(e.to_string()));
                    }
                }
            }
            partitions.push(out);
        }
        responses.push(
            ReassignableTopicResponse::default()
                .with_name(t.name.clone())
                .with_partitions(partitions),
        );
    }
    AlterPartitionReassignmentsResponse::default().with_responses(responses)
}

pub(crate) fn list_partition_reassignments(
    state: &State,
    req: &ListPartitionReassignmentsRequest,
) -> ListPartitionReassignmentsResponse {
    let wanted = |r: &Reassignment| match &req.topics {
        None => true,
        Some(topics) => topics
            .iter()
            .any(|t| t.name.0.as_str() == r.topic && t.partition_indexes.contains(&r.partition)),
    };
    let mut by_topic: Vec<(String, Vec<OngoingPartitionReassignment>)> = Vec::new();
    for r in state.store.reassignments().into_iter().filter(|r| wanted(r)) {
        let ids = |v: &[i32]| v.iter().map(|b| BrokerId(*b)).collect::<Vec<_>>();
        let ongoing = OngoingPartitionReassignment::default()
            .with_partition_index(r.partition)
            .with_replicas(ids(&r.replicas))
            .with_adding_replicas(ids(&r.adding))
            .with_removing_replicas(ids(&r.removing));
        match by_topic.iter_mut().find(|(t, _)| *t == r.topic) {
            Some((_, partitions)) => partitions.push(ongoing),
            None => by_topic.push((r.topic.clone(), vec![ongoing])),
        }
    }
    ListPartitionReassignmentsResponse::default().with_topics(
        by_topic
            .into_iter()
            .map(|(t, partitions)| {
                OngoingTopicReassignment::default()
                    .with_name(TopicName(s(t)))
                    .with_partitions(partitions)
            })
            .collect(),
    )
}

pub(crate) async fn unregister_broker(
    state: &State,
    req: &UnregisterBrokerRequest,
) -> UnregisterBrokerResponse {
    let id = req.broker_id.0;
    match blocking(&state.store, move |st| st.remove_broker(id)).await {
        Ok(()) => UnregisterBrokerResponse::default(),
        Err(e) => UnregisterBrokerResponse::default()
            .with_error_code(codes::of_store_error(&e))
            .with_error_message(Some(s(e.to_string()))),
    }
}

/// The answers to a principal the access rules do not let manage the cluster.
pub(crate) fn reassignments_refused(code: i16) -> AlterPartitionReassignmentsResponse {
    AlterPartitionReassignmentsResponse::default()
        .with_error_code(code)
        .with_error_message(Some(s("not allowed to alter the cluster")))
}

pub(crate) fn reassignment_list_refused(code: i16) -> ListPartitionReassignmentsResponse {
    ListPartitionReassignmentsResponse::default()
        .with_error_code(code)
        .with_error_message(Some(s("not allowed to describe the cluster")))
}

pub(crate) fn unregister_refused(code: i16) -> UnregisterBrokerResponse {
    UnregisterBrokerResponse::default()
        .with_error_code(code)
        .with_error_message(Some(s("not allowed to alter the cluster")))
}

/// The answer to a producer the access rules do not let write.
pub(crate) fn init_producer_refused(code: i16) -> kafka_protocol::messages::InitProducerIdResponse {
    kafka_protocol::messages::InitProducerIdResponse::default()
        .with_error_code(code)
        .with_producer_id(kafka_protocol::messages::ProducerId(-1))
        .with_producer_epoch(-1)
}
