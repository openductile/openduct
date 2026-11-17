// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Sending and reading messages, and a consumer group's progress: what `openduct send` and `openduct read` need.

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    BrokerId, FetchRequest, GroupId, ListOffsetsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

use super::{Client, ClientError};

/// A message to send.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Outgoing {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
}

/// What one partition answered to a fetch: its record batches, still encoded, and where the partition ends.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub partition: i32,
    /// 0 when the partition answered; a Kafka error code otherwise.
    pub error_code: i16,
    pub high_watermark: i64,
    pub log_start: i64,
    /// Record batches, exactly as stored. They begin at the batch holding the requested offset, so records before
    /// it may be among them.
    pub batches: Bytes,
}

/// The earliest offset still kept, for [`Client::list_offset`].
pub const EARLIEST: i64 = -2;
/// The offset the next message will get, for [`Client::list_offset`].
pub const LATEST: i64 = -1;

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

impl Client {
    /// How many partitions `topic` has.
    pub async fn partition_count(&mut self, topic: &str) -> Result<i32, ClientError> {
        let request = MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(TopicName(s(topic)))),
            ]))
            .with_allow_auto_topic_creation(false);
        let response = self.send(&request, 12).await?;
        let Some(found) = response.topics.first() else {
            return Err(ClientError::Protocol("no answer for the topic".into()));
        };
        if found.error_code != 0 {
            return Err(ClientError::Refused {
                code: found.error_code,
                message: Some(format!("topic {topic:?} does not exist")),
            });
        }
        i32::try_from(found.partitions.len()).map_err(|_| ClientError::Protocol("too many partitions".into()))
    }

    /// Writes `messages` to `partition` of `topic` as one batch, once every replica has it: the first one's offset.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        messages: &[Outgoing],
        timestamp_ms: i64,
    ) -> Result<i64, ClientError> {
        let wanted = [(topic.to_owned(), partition, messages.to_vec())];
        self.produce_topics(&wanted, timestamp_ms)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| ClientError::Protocol("no answer for the partition".into()))?
    }

    /// Writes to several partitions of any topics in one request - each partition's messages as one batch - once
    /// every replica has them: for each, in the order given, the first one's offset or why it was refused.
    pub async fn produce_topics(
        &mut self,
        wanted: &[(String, i32, Vec<Outgoing>)],
        timestamp_ms: i64,
    ) -> Result<Vec<Result<i64, ClientError>>, ClientError> {
        let mut topics: Vec<TopicProduceData> = Vec::new();
        for (topic, partition, messages) in wanted {
            let data = PartitionProduceData::default()
                .with_index(*partition)
                .with_records(Some(batch_of(messages, timestamp_ms)?));
            match topics.iter_mut().find(|t| t.name.0.as_str() == topic.as_str()) {
                Some(t) => t.partition_data.push(data),
                None => topics.push(
                    TopicProduceData::default()
                        .with_name(TopicName(s(topic)))
                        .with_partition_data(vec![data]),
                ),
            }
        }
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(30_000)
            .with_topic_data(topics);
        let response = self.send(&request, 9).await?;
        Ok(wanted
            .iter()
            .map(|(topic, partition, _)| {
                let answer = response
                    .responses
                    .iter()
                    .filter(|t| t.name.0.as_str() == topic.as_str())
                    .flat_map(|t| t.partition_responses.iter())
                    .find(|p| p.index == *partition)
                    .ok_or_else(|| ClientError::Protocol("no answer for the partition".into()))?;
                if answer.error_code != 0 {
                    return Err(ClientError::Refused {
                        code: answer.error_code,
                        message: answer.error_message.as_ref().map(|m| m.to_string()),
                    });
                }
                Ok(answer.base_offset)
            })
            .collect())
    }

    /// An offset of one partition: [`EARLIEST`], [`LATEST`], or the first at or after a timestamp in milliseconds.
    pub async fn list_offset(&mut self, topic: &str, partition: i32, at: i64) -> Result<i64, ClientError> {
        let request = ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(TopicName(s(topic)))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(partition)
                            .with_timestamp(at),
                    ]),
            ]);
        let response = self.send(&request, 7).await?;
        let answer = response
            .topics
            .first()
            .and_then(|t| t.partitions.first())
            .ok_or_else(|| ClientError::Protocol("no answer for the partition".into()))?;
        if answer.error_code != 0 {
            return Err(ClientError::Refused {
                code: answer.error_code,
                message: None,
            });
        }
        Ok(answer.offset)
    }

    /// Read from several partitions of a topic at once, waiting up to `max_wait_ms` for something to arrive.
    pub async fn fetch(
        &mut self,
        topic: &str,
        from: &[(i32, i64)],
        max_wait_ms: i32,
        max_bytes: i32,
    ) -> Result<Vec<Fetched>, ClientError> {
        let wanted = [(topic.to_owned(), from.to_vec())];
        Ok(self
            .fetch_topics(&wanted, max_wait_ms, max_bytes, max_bytes)
            .await?
            .into_iter()
            .map(|(_, f)| f)
            .collect())
    }

    /// Reads partitions of several topics in one request, waiting up to `max_wait_ms` for any of them to have
    /// records: each partition at most `partition_max_bytes`, all of them at most `max_bytes`.
    pub async fn fetch_topics(
        &mut self,
        wanted: &[(String, Vec<(i32, i64)>)],
        max_wait_ms: i32,
        partition_max_bytes: i32,
        max_bytes: i32,
    ) -> Result<Vec<(String, Fetched)>, ClientError> {
        let topics = wanted
            .iter()
            .map(|(topic, from)| {
                let partitions = from
                    .iter()
                    .map(|(partition, offset)| {
                        FetchPartition::default()
                            .with_partition(*partition)
                            .with_fetch_offset(*offset)
                            .with_partition_max_bytes(partition_max_bytes)
                    })
                    .collect();
                FetchTopic::default()
                    .with_topic(TopicName(s(topic)))
                    .with_partitions(partitions)
            })
            .collect();
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(max_wait_ms)
            .with_min_bytes(1)
            .with_max_bytes(max_bytes)
            .with_topics(topics);
        let response = self.send(&request, 12).await?;
        let mut out = Vec::new();
        for topic in response.responses {
            let name = topic.topic.0.to_string();
            for p in topic.partitions {
                out.push((
                    name.clone(),
                    Fetched {
                        partition: p.partition_index,
                        error_code: p.error_code,
                        high_watermark: p.high_watermark,
                        log_start: p.log_start_offset,
                        batches: p.records.unwrap_or_default(),
                    },
                ));
            }
        }
        Ok(out)
    }

    /// The offsets `group` has committed on these partitions; -1 where it has none.
    pub async fn committed(
        &mut self,
        group: &str,
        topic: &str,
        partitions: &[i32],
    ) -> Result<Vec<(i32, i64)>, ClientError> {
        let request = OffsetFetchRequest::default().with_groups(vec![
            OffsetFetchRequestGroup::default()
                .with_group_id(GroupId(s(group)))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .with_name(TopicName(s(topic)))
                        .with_partition_indexes(partitions.to_vec()),
                ])),
        ]);
        let response = self.send(&request, 8).await?;
        let group_answer = response
            .groups
            .first()
            .ok_or_else(|| ClientError::Protocol("no answer for the group".into()))?;
        if group_answer.error_code != 0 {
            return Err(ClientError::Refused {
                code: group_answer.error_code,
                message: None,
            });
        }
        Ok(group_answer
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(|p| (p.partition_index, p.committed_offset))
            })
            .collect())
    }

    /// Record `group`'s progress: for each partition, the offset of the next message it will read.
    pub async fn commit(
        &mut self,
        group: &str,
        topic: &str,
        offsets: &[(i32, i64)],
    ) -> Result<(), ClientError> {
        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(s(group)))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::default())
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(s(topic)))
                    .with_partitions(
                        offsets
                            .iter()
                            .map(|(partition, offset)| {
                                OffsetCommitRequestPartition::default()
                                    .with_partition_index(*partition)
                                    .with_committed_offset(*offset)
                            })
                            .collect(),
                    ),
            ]);
        let response = self.send(&request, 8).await?;
        for t in &response.topics {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(ClientError::Refused {
                        code: p.error_code,
                        message: Some(format!(
                            "the group {group:?} is in use by consumers that are running; stop them, or read under \
                             another group name"
                        )),
                    });
                }
            }
        }
        Ok(())
    }
}

impl Client {
    /// What `group` saved with its commit on one partition: a source connector keeps its position there.
    pub async fn saved_position(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<String>, ClientError> {
        let request = OffsetFetchRequest::default().with_groups(vec![
            OffsetFetchRequestGroup::default()
                .with_group_id(GroupId(s(group)))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .with_name(TopicName(s(topic)))
                        .with_partition_indexes(vec![partition]),
                ])),
        ]);
        let response = self.send(&request, 8).await?;
        Ok(response
            .groups
            .first()
            .and_then(|g| g.topics.first())
            .and_then(|t| t.partitions.first())
            .filter(|p| p.committed_offset >= 0)
            .and_then(|p| p.metadata.as_ref())
            .map(|m| m.to_string())
            .filter(|m| !m.is_empty()))
    }

    /// Save a position under `group`, with the commit on one partition.
    pub async fn save_position(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        position: &str,
    ) -> Result<(), ClientError> {
        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(s(group)))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::default())
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(s(topic)))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(partition)
                            .with_committed_offset(offset)
                            .with_committed_metadata(Some(s(position))),
                    ]),
            ]);
        let response = self.send(&request, 8).await?;
        match response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| p.error_code)
            .find(|c| *c != 0)
        {
            None => Ok(()),
            Some(code) => Err(ClientError::Refused {
                code,
                message: Some(format!("cannot save the position of {group:?}")),
            }),
        }
    }
}

/// `messages` as one record batch, as a produce request carries it.
fn batch_of(messages: &[Outgoing], timestamp_ms: i64) -> Result<Bytes, ClientError> {
    let records: Vec<Record> = messages
        .iter()
        .enumerate()
        .map(|(at, message)| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::try_from(at).unwrap_or(i64::MAX),
            // No producer id, so the batch's sequence is -1; stepping with the offset keeps the records in one
            // batch.
            sequence: i32::try_from(at).unwrap_or(i32::MAX).wrapping_sub(1),
            timestamp: timestamp_ms,
            key: message.key.clone(),
            value: message.value.clone(),
            headers: message
                .headers
                .iter()
                .map(|(name, value)| (s(name), Some(value.clone())))
                .collect::<IndexMap<_, _>>(),
            delete_horizon: false,
        })
        .collect();
    let mut batch = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut batch,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .map_err(|e| ClientError::Protocol(e.to_string()))?;
    Ok(batch.freeze())
}
