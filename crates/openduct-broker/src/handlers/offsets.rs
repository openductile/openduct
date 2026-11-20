// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! ListOffsets, OffsetForLeaderEpoch and DeleteRecords: where a partition's offsets are, in which leader epoch each
//! was written, and moving where it starts.
//!
//! A batch is stored in the epoch of the leader that wrote it (a replicated partition's Raft term; 0 for a broker on
//! its own), and the epoch travels in the batch, as with Kafka. A consumer that sees a new leader asks
//! OffsetForLeaderEpoch where the epoch of its position ended, to check that nothing it read was taken back. Nothing
//! committed is ever taken back here, so the answers only have to be honest in one direction: never before the
//! position. ListOffsets answers an epoch beside each offset that is never below the one the offset was written in.

use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsTopicResult,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderTopicResult,
};
use kafka_protocol::messages::{
    DeleteRecordsRequest, DeleteRecordsResponse, ListOffsetsRequest, ListOffsetsResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
};

use super::{State, blocking};
use crate::codes;

pub(crate) async fn list_offsets(
    state: &State,
    req: &ListOffsetsRequest,
    version: i16,
) -> ListOffsetsResponse {
    const LATEST: i64 = -1;
    const EARLIEST: i64 = -2;
    const MAX_TIMESTAMP: i64 = -3;
    let with_epoch = version >= 4;
    let mut topics = Vec::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let mut out = ListOffsetsPartitionResponse::default().with_partition_index(p.partition_index);
            let (topic, partition, ts) = (name.clone(), p.partition_index, p.timestamp);
            // To a consumer that reads only committed data, the end is the last stable offset.
            let committed_only = req.isolation_level == 1;
            let found = blocking(&state.store, move |s| {
                let found = match ts {
                    LATEST => s.offsets(&topic, partition).map(|o| {
                        Some((
                            if committed_only {
                                o.last_stable
                            } else {
                                o.high_watermark
                            },
                            -1,
                        ))
                    }),
                    EARLIEST => s.offsets(&topic, partition).map(|o| Some((o.log_start, -1))),
                    MAX_TIMESTAMP => s.max_timestamp(&topic, partition),
                    t => s.offset_for_timestamp(&topic, partition, t),
                }?;
                // The epoch is read with the offset, from the same leader.
                let epoch = match found {
                    Some((offset, _)) if with_epoch => Some(s.epoch_of(&topic, partition, offset)?),
                    _ => None,
                };
                Ok((found, epoch))
            })
            .await;
            match found {
                Ok((Some((offset, timestamp)), epoch)) => {
                    out.offset = offset;
                    out.timestamp = timestamp;
                    if let Some(epoch) = epoch {
                        out.leader_epoch = epoch;
                    }
                }
                Ok((None, _)) => {}
                Err(e) => out.error_code = codes::of_store_error(&e),
            }
            partitions.push(out);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(t.name.clone())
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}

/// Kafka's answer for an epoch it cannot place: no epoch, no offset.
const UNDEFINED: i32 = -1;
const UNDEFINED_OFFSET: i64 = -1;

pub(crate) async fn offset_for_leader_epoch(
    state: &State,
    req: &OffsetForLeaderEpochRequest,
) -> OffsetForLeaderEpochResponse {
    let mut topics = Vec::new();
    for t in &req.topics {
        let name = t.topic.0.to_string();
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let (topic, partition, epoch) = (name.clone(), p.partition, p.leader_epoch);
            let found = blocking(&state.store, move |s| s.epoch_end(&topic, partition, epoch)).await;
            partitions.push(epoch_end_offset(
                p.partition,
                p.current_leader_epoch,
                epoch,
                found,
            ));
        }
        topics.push(
            OffsetForLeaderTopicResult::default()
                .with_topic(t.topic.clone())
                .with_partitions(partitions),
        );
    }
    OffsetForLeaderEpochResponse::default().with_topics(topics)
}

fn epoch_end_offset(
    partition: i32,
    current_leader_epoch: i32,
    epoch: i32,
    found: Result<openduct_storage::EpochEnd, openduct_storage::StoreError>,
) -> EpochEndOffset {
    let out = EpochEndOffset::default()
        .with_partition(partition)
        .with_leader_epoch(UNDEFINED)
        .with_end_offset(UNDEFINED_OFFSET);
    let end = match found {
        Ok(end) => end,
        Err(e) => return out.with_error_code(codes::of_store_error(&e)),
    };
    // The client names the epoch it believes the leader is in (-1: it does not say). An older one means its
    // metadata is out of date; a newer one, that this broker has not heard of the new leader yet. Either way it
    // asks again, as with Kafka.
    if current_leader_epoch >= 0 && current_leader_epoch < end.current {
        return out.with_error_code(codes::FENCED_LEADER_EPOCH);
    }
    if current_leader_epoch > end.current {
        return out.with_error_code(codes::UNKNOWN_LEADER_EPOCH);
    }
    match end.end_offset {
        Some(offset) => out.with_leader_epoch(epoch).with_end_offset(offset),
        None => out,
    }
}

/// DeleteRecords: each partition starts at the offset asked from now on (`-1`: its high watermark); answered with
/// its new low watermark. `may_delete`: Delete on the topic.
pub(crate) async fn delete_records(
    state: &State,
    req: &DeleteRecordsRequest,
    may_delete: impl Fn(&str) -> bool,
) -> DeleteRecordsResponse {
    let mut topics = Vec::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let allowed = may_delete(&name);
        let mut partitions = Vec::new();
        for p in &t.partitions {
            let out = DeleteRecordsPartitionResult::default()
                .with_partition_index(p.partition_index)
                .with_low_watermark(-1);
            if !allowed {
                partitions.push(out.with_error_code(crate::security::TOPIC_AUTHORIZATION_FAILED));
                continue;
            }
            let (topic, partition, offset) = (name.clone(), p.partition_index, p.offset);
            let found = blocking(&state.store, move |s| s.delete_records(&topic, partition, offset)).await;
            partitions.push(match found {
                Ok(start) => out.with_low_watermark(start),
                Err(e) => out.with_error_code(codes::of_store_error(&e)),
            });
        }
        topics.push(
            DeleteRecordsTopicResult::default()
                .with_name(t.name.clone())
                .with_partitions(partitions),
        );
    }
    DeleteRecordsResponse::default().with_topics(topics)
}

#[cfg(test)]
mod tests {
    use openduct_storage::{EpochEnd, StoreError};

    use super::*;

    fn end(current: i32, end_offset: Option<i64>) -> Result<EpochEnd, StoreError> {
        Ok(EpochEnd { current, end_offset })
    }

    #[test]
    fn an_epoch_is_answered_with_where_it_ended() {
        let out = epoch_end_offset(2, 5, 3, end(5, Some(120)));
        assert_eq!(
            (out.error_code, out.partition, out.leader_epoch, out.end_offset),
            (0, 2, 3, 120)
        );
        // A client that does not name the current epoch is answered too.
        let out = epoch_end_offset(0, -1, 3, end(5, Some(120)));
        assert_eq!((out.error_code, out.end_offset), (0, 120));
    }

    #[test]
    fn a_client_behind_or_ahead_of_the_leader_is_told_to_ask_again() {
        let behind = epoch_end_offset(0, 4, 3, end(5, Some(120)));
        assert_eq!(behind.error_code, codes::FENCED_LEADER_EPOCH);
        let ahead = epoch_end_offset(0, 6, 3, end(5, Some(120)));
        assert_eq!(ahead.error_code, codes::UNKNOWN_LEADER_EPOCH);
        assert_eq!(
            (ahead.leader_epoch, ahead.end_offset),
            (UNDEFINED, UNDEFINED_OFFSET)
        );
    }

    #[test]
    fn an_epoch_the_partition_cannot_have_is_undefined() {
        let out = epoch_end_offset(0, 5, 9, end(5, None));
        assert_eq!(
            (out.error_code, out.leader_epoch, out.end_offset),
            (0, UNDEFINED, UNDEFINED_OFFSET)
        );
    }

    #[test]
    fn a_broker_that_does_not_lead_says_so() {
        let out = epoch_end_offset(0, 5, 3, Err(StoreError::NotLeader { leader: None }));
        assert_eq!(out.error_code, codes::NOT_LEADER_OR_FOLLOWER);
    }
}
