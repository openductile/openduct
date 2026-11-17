// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Each request checked against the access rules before its handler sees it: what the principal may not touch is
//! taken out of the request and answered with Kafka's own authorization errors, so the handler only ever works on
//! what is allowed, and a client learns nothing of topics it may not see (a listing simply leaves them out).

use std::net::IpAddr;

use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderTopicResult,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest,
    FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest,
    ProduceResponse, TopicName,
};

use super::Principal;
use super::acl::{Authorizer, Operation, Resource};

pub(crate) const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub(crate) const GROUP_AUTHORIZATION_FAILED: i16 = 30;
pub(crate) const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
pub(crate) const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

const CLUSTER: &str = "kafka-cluster";

/// One request's principal and the rules it is held to.
pub(crate) struct Authorize<'a> {
    pub(crate) rules: Option<&'a Authorizer>,
    pub(crate) principal: &'a Principal,
    pub(crate) host: IpAddr,
}

impl Authorize<'_> {
    pub(crate) fn may(&self, resource: Resource, name: &str, op: Operation) -> bool {
        // OpenDuct's own topics are written by OpenDuct alone - not by any client, super users included.
        if resource == Resource::Topic
            && openduct_storage::is_internal_topic(name)
            && matches!(
                op,
                Operation::Write | Operation::Create | Operation::Delete | Operation::Alter | Operation::All
            )
        {
            return false;
        }
        self.rules
            .is_none_or(|r| r.allows(self.principal, self.host, resource, name, op))
    }

    fn topic(&self, name: &TopicName, op: Operation) -> bool {
        self.may(Resource::Topic, name.0.as_str(), op)
    }

    pub(crate) fn group(&self, group: &str, op: Operation) -> bool {
        self.may(Resource::Group, group, op)
    }

    pub(crate) fn cluster(&self, op: Operation) -> bool {
        self.may(Resource::Cluster, CLUSTER, op)
    }

    /// An idempotent producer: IdempotentWrite on the cluster, or Write on some topic (as Kafka 3 allows).
    pub(crate) fn init_producer(&self, transactional_id: Option<&str>) -> Result<(), i16> {
        if let Some(id) = transactional_id {
            return if self.may(Resource::TransactionalId, id, Operation::Write) {
                Ok(())
            } else {
                Err(TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            };
        }
        let any_write = self
            .rules
            .is_none_or(|r| r.allows_any(self.principal, self.host, Resource::Topic, Operation::Write));
        if self.cluster(Operation::IdempotentWrite) || any_write {
            Ok(())
        } else {
            Err(CLUSTER_AUTHORIZATION_FAILED)
        }
    }

    // --- Produce: Write on each topic.

    pub(crate) fn produce(&self, req: &mut ProduceRequest) -> Vec<TopicProduceResponse> {
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut req.topic_data)
            .into_iter()
            .partition(|t| self.topic(&t.name, Operation::Write));
        req.topic_data = allowed;
        denied
            .into_iter()
            .map(|t| {
                TopicProduceResponse::default()
                    .with_name(t.name)
                    .with_partition_responses(
                        t.partition_data
                            .iter()
                            .map(|p| {
                                PartitionProduceResponse::default()
                                    .with_index(p.index)
                                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                                    .with_base_offset(-1)
                                    .with_log_append_time_ms(-1)
                                    .with_log_start_offset(-1)
                            })
                            .collect(),
                    )
            })
            .collect()
    }

    pub(crate) fn produced(res: &mut ProduceResponse, denied: Vec<TopicProduceResponse>) {
        res.responses.extend(denied);
    }

    // --- Fetch: Read on each topic.

    pub(crate) fn fetch(&self, req: &mut FetchRequest) -> Vec<FetchableTopicResponse> {
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut req.topics)
            .into_iter()
            .partition(|t| self.topic(&t.topic, Operation::Read));
        req.topics = allowed;
        denied
            .into_iter()
            .map(|t| {
                FetchableTopicResponse::default()
                    .with_topic(t.topic)
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                PartitionData::default()
                                    .with_partition_index(p.partition)
                                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                                    .with_high_watermark(-1)
                                    .with_last_stable_offset(-1)
                                    .with_log_start_offset(-1)
                            })
                            .collect(),
                    )
            })
            .collect()
    }

    pub(crate) fn fetched(res: &mut FetchResponse, denied: Vec<FetchableTopicResponse>) {
        res.responses.extend(denied);
    }

    // --- Metadata: Describe; a listing of every topic leaves out what the principal may not see.

    pub(crate) fn metadata(&self, req: &mut MetadataRequest) -> Vec<MetadataResponseTopic> {
        let Some(topics) = req.topics.as_mut() else {
            return Vec::new();
        };
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(topics).into_iter().partition(|t| {
            t.name
                .as_ref()
                .is_some_and(|n| self.topic(n, Operation::Describe))
        });
        *topics = allowed;
        denied
            .into_iter()
            .map(|t| {
                MetadataResponseTopic::default()
                    .with_name(t.name)
                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
            })
            .collect()
    }

    pub(crate) fn described(
        &self,
        res: &mut MetadataResponse,
        listed_all: bool,
        denied: Vec<MetadataResponseTopic>,
    ) {
        if listed_all {
            res.topics.retain(|t| {
                t.name
                    .as_ref()
                    .is_some_and(|n| self.topic(n, Operation::Describe))
            });
        }
        res.topics.extend(denied);
    }

    // --- ListOffsets: Describe on each topic.

    pub(crate) fn list_offsets(&self, req: &mut ListOffsetsRequest) -> Vec<ListOffsetsTopicResponse> {
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut req.topics)
            .into_iter()
            .partition(|t| self.topic(&t.name, Operation::Describe));
        req.topics = allowed;
        denied
            .into_iter()
            .map(|t| {
                ListOffsetsTopicResponse::default()
                    .with_name(t.name)
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                ListOffsetsPartitionResponse::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                                    .with_timestamp(-1)
                                    .with_offset(-1)
                            })
                            .collect(),
                    )
            })
            .collect()
    }

    pub(crate) fn listed(res: &mut ListOffsetsResponse, denied: Vec<ListOffsetsTopicResponse>) {
        res.topics.extend(denied);
    }

    // --- OffsetForLeaderEpoch: Describe on each topic, as ListOffsets.

    pub(crate) fn epochs(&self, req: &mut OffsetForLeaderEpochRequest) -> Vec<OffsetForLeaderTopicResult> {
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut req.topics)
            .into_iter()
            .partition(|t| self.topic(&t.topic, Operation::Describe));
        req.topics = allowed;
        denied
            .into_iter()
            .map(|t| {
                OffsetForLeaderTopicResult::default()
                    .with_topic(t.topic)
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                EpochEndOffset::default()
                                    .with_partition(p.partition)
                                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                                    .with_leader_epoch(-1)
                                    .with_end_offset(-1)
                            })
                            .collect(),
                    )
            })
            .collect()
    }

    pub(crate) fn epochs_found(
        res: &mut OffsetForLeaderEpochResponse,
        denied: Vec<OffsetForLeaderTopicResult>,
    ) {
        res.topics.extend(denied);
    }

    // --- Topic admin: Create (on the cluster or the topic), Delete on the topic.

    pub(crate) fn create_topics(&self, req: &mut CreateTopicsRequest) -> Vec<CreatableTopicResult> {
        let on_cluster = self.cluster(Operation::Create);
        let (allowed, denied): (Vec<_>, Vec<_>) = std::mem::take(&mut req.topics)
            .into_iter()
            .partition(|t| on_cluster || self.topic(&t.name, Operation::Create));
        req.topics = allowed;
        denied
            .into_iter()
            .map(|t| {
                CreatableTopicResult::default()
                    .with_name(t.name)
                    .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                    .with_num_partitions(-1)
                    .with_replication_factor(-1)
            })
            .collect()
    }

    pub(crate) fn created(res: &mut CreateTopicsResponse, denied: Vec<CreatableTopicResult>) {
        res.topics.extend(denied);
    }

    /// `name_of` finds a topic's name from its id (a request may name a topic by id only).
    pub(crate) fn delete_topics(
        &self,
        req: &mut DeleteTopicsRequest,
        name_of: impl Fn(&uuid::Uuid) -> Option<String>,
    ) -> Vec<DeletableTopicResult> {
        let mut denied = Vec::new();
        let names = std::mem::take(&mut req.topic_names);
        for n in names {
            if self.topic(&n, Operation::Delete) {
                req.topic_names.push(n);
            } else {
                denied.push(
                    DeletableTopicResult::default()
                        .with_name(Some(n))
                        .with_error_code(TOPIC_AUTHORIZATION_FAILED),
                );
            }
        }
        let states = std::mem::take(&mut req.topics);
        for t in states {
            let name = t
                .name
                .as_ref()
                .map(|n| n.0.to_string())
                .or_else(|| name_of(&t.topic_id));
            if name.is_some_and(|n| self.may(Resource::Topic, &n, Operation::Delete)) {
                req.topics.push(t);
            } else {
                denied.push(
                    DeletableTopicResult::default()
                        .with_name(t.name.clone())
                        .with_topic_id(t.topic_id)
                        .with_error_code(TOPIC_AUTHORIZATION_FAILED),
                );
            }
        }
        denied
    }

    pub(crate) fn deleted(res: &mut DeleteTopicsResponse, denied: Vec<DeletableTopicResult>) {
        res.responses.extend(denied);
    }

    // --- Consumer groups: Read on the group (and Read on each topic committed), Describe to find or fetch.

    pub(crate) fn offset_commit(&self, req: &mut OffsetCommitRequest) -> Vec<OffsetCommitResponseTopic> {
        let group_ok = self.group(req.group_id.0.as_str(), Operation::Read);
        let mut denied = Vec::new();
        let topics = std::mem::take(&mut req.topics);
        for t in topics {
            let code = if !group_ok {
                GROUP_AUTHORIZATION_FAILED
            } else if !self.topic(&t.name, Operation::Read) {
                TOPIC_AUTHORIZATION_FAILED
            } else {
                req.topics.push(t);
                continue;
            };
            denied.push(
                OffsetCommitResponseTopic::default()
                    .with_name(t.name)
                    .with_partitions(
                        t.partitions
                            .iter()
                            .map(|p| {
                                OffsetCommitResponsePartition::default()
                                    .with_partition_index(p.partition_index)
                                    .with_error_code(code)
                            })
                            .collect(),
                    ),
            );
        }
        denied
    }

    pub(crate) fn committed(res: &mut OffsetCommitResponse, denied: Vec<OffsetCommitResponseTopic>) {
        res.topics.extend(denied);
    }

    /// Answers an offset fetch after the handler: groups the principal may not see are refused, topics it may not
    /// see are left out (or refused, when asked for by name).
    pub(crate) fn offsets_fetched(
        &self,
        req: &OffsetFetchRequest,
        res: &mut OffsetFetchResponse,
        version: i16,
    ) {
        if version >= 8 {
            for (g, asked) in res.groups.iter_mut().zip(&req.groups) {
                if !self.group(g.group_id.0.as_str(), Operation::Describe) {
                    g.error_code = GROUP_AUTHORIZATION_FAILED;
                    g.topics.clear();
                    continue;
                }
                if asked.topics.is_none() {
                    g.topics.retain(|t| self.topic(&t.name, Operation::Describe));
                } else {
                    for t in &mut g.topics {
                        if !self.topic(&t.name, Operation::Describe) {
                            for p in &mut t.partitions {
                                p.committed_offset = -1;
                                p.metadata = None;
                                p.error_code = TOPIC_AUTHORIZATION_FAILED;
                            }
                        }
                    }
                }
            }
            return;
        }
        if !self.group(req.group_id.0.as_str(), Operation::Describe) {
            res.error_code = GROUP_AUTHORIZATION_FAILED;
            for t in &mut res.topics {
                for p in &mut t.partitions {
                    p.committed_offset = -1;
                    p.metadata = None;
                    p.error_code = GROUP_AUTHORIZATION_FAILED;
                }
            }
            return;
        }
        if req.topics.is_none() {
            res.topics.retain(|t| self.topic(&t.name, Operation::Describe));
        } else {
            for t in &mut res.topics {
                if !self.topic(&t.name, Operation::Describe) {
                    for p in &mut t.partitions {
                        p.committed_offset = -1;
                        p.metadata = None;
                        p.error_code = TOPIC_AUTHORIZATION_FAILED;
                    }
                }
            }
        }
    }

    /// Answers a coordinator lookup after the handler: keys the principal may not describe are refused.
    pub(crate) fn coordinators_found(
        &self,
        req: &FindCoordinatorRequest,
        res: &mut FindCoordinatorResponse,
        version: i16,
    ) {
        let (kind, op) = if req.key_type == 1 {
            (Resource::TransactionalId, Operation::Describe)
        } else {
            (Resource::Group, Operation::Describe)
        };
        let code = if req.key_type == 1 {
            TRANSACTIONAL_ID_AUTHORIZATION_FAILED
        } else {
            GROUP_AUTHORIZATION_FAILED
        };
        if version >= 4 {
            for c in &mut res.coordinators {
                if !self.may(kind, c.key.as_str(), op) {
                    c.error_code = code;
                    c.node_id = kafka_protocol::messages::BrokerId(-1);
                    c.host = kafka_protocol::protocol::StrBytes::default();
                    c.port = -1;
                }
            }
        } else if !self.may(kind, req.key.as_str(), op) {
            res.error_code = code;
            res.node_id = kafka_protocol::messages::BrokerId(-1);
            res.host = kafka_protocol::protocol::StrBytes::default();
            res.port = -1;
        }
    }
}
