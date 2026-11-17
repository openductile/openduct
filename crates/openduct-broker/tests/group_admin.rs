// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! ListGroups and DescribeGroups over the wire: a group with a member, one that only committed offsets, and one that
//! does not exist, as tools such as `kafka-consumer-groups` ask for them.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    DescribeGroupsRequest, GroupId, JoinGroupRequest, ListGroupsRequest, OffsetCommitRequest,
    SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use openduct_broker::client::Client;
use openduct_broker::{Broker, BrokerConfig};
use openduct_storage::{MemoryStore, Store, TopicConfig};

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_owned())
}

async fn start() -> (Broker, Client) {
    let store = Arc::new(MemoryStore::new());
    store
        .create_topic(
            "orders",
            TopicConfig {
                partitions: 2,
                retention_ms: None,
            },
        )
        .unwrap();
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.group_initial_rebalance_delay = Duration::ZERO;
    let broker = Broker::start(config, store).await.unwrap();
    let client = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    (broker, client)
}

/// A consumer's member metadata, as Kafka's clients write it: the version, then the subscription.
fn subscription(topics: &[&str]) -> Bytes {
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Encodable;
    let mut out = bytes::BytesMut::from(&0i16.to_be_bytes()[..]);
    ConsumerProtocolSubscription::default()
        .with_topics(topics.iter().map(|t| s(t)).collect())
        .encode(&mut out, 0)
        .unwrap();
    out.freeze()
}

/// A member of `group` joined and synced, holding both partitions of `orders`.
async fn member_of(c: &mut Client, group: &str) -> String {
    let join = |member: &str| {
        JoinGroupRequest::default()
            .with_group_id(GroupId(s(group)))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(5_000)
            .with_member_id(s(member))
            .with_protocol_type(s("consumer"))
            .with_protocols(vec![
                JoinGroupRequestProtocol::default()
                    .with_name(s("range"))
                    .with_metadata(subscription(&["orders"])),
            ])
    };
    let first = c.send(&join(""), 9).await.unwrap();
    let member = first.member_id.to_string();
    let joined = c.send(&join(&member), 9).await.unwrap();
    assert_eq!(joined.error_code, 0);
    let synced = c
        .send(
            &SyncGroupRequest::default()
                .with_group_id(GroupId(s(group)))
                .with_generation_id(joined.generation_id)
                .with_member_id(s(&member))
                .with_protocol_type(Some(s("consumer")))
                .with_protocol_name(Some(s("range")))
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(s(&member))
                        .with_assignment(Bytes::from_static(b"orders:0,1")),
                ]),
            5,
        )
        .await
        .unwrap();
    assert_eq!(synced.error_code, 0);
    member
}

/// Offsets committed for `group` by a client outside any group membership (generation -1), as tools do.
async fn commit_only(c: &mut Client, group: &str) {
    let committed = c
        .send(
            &OffsetCommitRequest::default()
                .with_group_id(GroupId(s(group)))
                .with_generation_id_or_member_epoch(-1)
                .with_member_id(s(""))
                .with_topics(vec![
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(s("orders")))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(0)
                                .with_committed_offset(3),
                        ]),
                ]),
            8,
        )
        .await
        .unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);
}

fn listed(
    groups: &[kafka_protocol::messages::list_groups_response::ListedGroup],
) -> Vec<(String, String, String)> {
    groups
        .iter()
        .map(|g| {
            (
                g.group_id.0.to_string(),
                g.group_state.to_string(),
                g.protocol_type.to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn groups_are_listed_and_described_as_kafka_tools_expect() {
    let (broker, mut c) = start().await;
    let member = member_of(&mut c, "billing").await;
    commit_only(&mut c, "audit").await;

    // Every group this broker coordinates: one with a member, one with only offsets.
    for version in 0..=5 {
        let all = c.send(&ListGroupsRequest::default(), version).await.unwrap();
        assert_eq!(all.error_code, 0);
        let groups = listed(&all.groups);
        assert_eq!(groups.len(), 2, "v{version}: {groups:?}");
        assert_eq!((groups[0].0.as_str(), groups[0].2.as_str()), ("audit", ""));
        assert_eq!(
            (groups[1].0.as_str(), groups[1].2.as_str()),
            ("billing", "consumer")
        );
        if version >= 4 {
            assert_eq!(groups[0].1, "Empty");
            assert_eq!(groups[1].1, "Stable");
        }
        if version >= 5 {
            assert!(all.groups.iter().all(|g| g.group_type.as_str() == "classic"));
        }
    }
    // By state (any case), and by type.
    let stable = c
        .send(
            &ListGroupsRequest::default().with_states_filter(vec![s("stable")]),
            5,
        )
        .await
        .unwrap();
    assert_eq!(
        listed(&stable.groups),
        vec![("billing".into(), "Stable".into(), "consumer".into())]
    );
    let other_type = c
        .send(
            &ListGroupsRequest::default().with_types_filter(vec![s("consumer")]),
            5,
        )
        .await
        .unwrap();
    assert!(other_type.groups.is_empty(), "no KIP-848 groups here");

    let described = c
        .send(
            &DescribeGroupsRequest::default()
                .with_groups(vec![
                    GroupId(s("billing")),
                    GroupId(s("audit")),
                    GroupId(s("nobody")),
                ])
                .with_include_authorized_operations(true),
            5,
        )
        .await
        .unwrap();
    let billing = &described.groups[0];
    assert_eq!(billing.error_code, 0);
    assert_eq!(
        (billing.group_state.as_str(), billing.protocol_type.as_str()),
        ("Stable", "consumer")
    );
    assert_eq!(billing.protocol_data.as_str(), "range");
    assert_eq!(billing.members.len(), 1);
    let m = &billing.members[0];
    assert_eq!(m.member_id.as_str(), member);
    assert_eq!(m.client_id.as_str(), "openduct-cli");
    assert_eq!(m.client_host.as_str(), "/127.0.0.1");
    assert_eq!(m.member_metadata, subscription(&["orders"]));
    assert_eq!(m.member_assignment, Bytes::from_static(b"orders:0,1"));
    assert_eq!(
        billing.authorized_operations,
        (1 << 3) | (1 << 6) | (1 << 8),
        "no rules: everything"
    );
    let audit = &described.groups[1];
    assert_eq!((audit.error_code, audit.group_state.as_str()), (0, "Empty"));
    assert!(audit.members.is_empty());
    let nobody = &described.groups[2];
    assert_eq!(
        (nobody.error_code, nobody.group_state.as_str()),
        (0, "Dead"),
        "v5: Dead"
    );

    // From v6, a group that does not exist is an error; and operations not asked for are not answered.
    let v6 = c
        .send(
            &DescribeGroupsRequest::default().with_groups(vec![GroupId(s("nobody")), GroupId(s("billing"))]),
            6,
        )
        .await
        .unwrap();
    assert_eq!(v6.groups[0].error_code, 69, "GROUP_ID_NOT_FOUND");
    assert!(v6.groups[0].error_message.is_some());
    assert_eq!(v6.groups[1].authorized_operations, i32::MIN);
    broker.shutdown().await;
}

fn offset_delete(group: &str, partitions: &[i32]) -> kafka_protocol::messages::OffsetDeleteRequest {
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };
    kafka_protocol::messages::OffsetDeleteRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_topics(vec![
            OffsetDeleteRequestTopic::default()
                .with_name(TopicName(s("orders")))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|p| OffsetDeleteRequestPartition::default().with_partition_index(*p))
                        .collect(),
                ),
        ])
}

fn delete_groups(groups: &[&str]) -> kafka_protocol::messages::DeleteGroupsRequest {
    kafka_protocol::messages::DeleteGroupsRequest::default()
        .with_groups_names(groups.iter().map(|g| GroupId(s(g))).collect())
}

/// The groups the broker lists, by name.
async fn names(c: &mut Client) -> Vec<String> {
    c.send(&ListGroupsRequest::default(), 5)
        .await
        .unwrap()
        .groups
        .iter()
        .map(|g| g.group_id.0.to_string())
        .collect()
}

#[tokio::test]
async fn groups_and_their_offsets_are_deleted_only_when_nothing_uses_them() {
    let (broker, mut c) = start().await;
    // Committed before the member joins: a commit from outside a group with members is refused (ILLEGAL_GENERATION).
    commit_only(&mut c, "billing").await;
    let member = member_of(&mut c, "billing").await;
    commit_only(&mut c, "audit").await;
    commit_only(&mut c, "spare").await;

    // A group with a member keeps its offsets of the topics it subscribes to; a partition that does not exist is
    // named as such.
    let protected = c.send(&offset_delete("billing", &[0, 9]), 0).await.unwrap();
    assert_eq!(protected.error_code, 0);
    let codes: Vec<i16> = protected.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert_eq!(
        codes,
        vec![86, 3],
        "GROUP_SUBSCRIBED_TO_TOPIC, UNKNOWN_TOPIC_OR_PARTITION"
    );
    // A group with members is not deleted; one with only offsets is; one that does not exist is not found.
    let deleted = c
        .send(&delete_groups(&["billing", "audit", "nobody"]), 2)
        .await
        .unwrap();
    let codes: Vec<(String, i16)> = deleted
        .results
        .iter()
        .map(|r| (r.group_id.0.to_string(), r.error_code))
        .collect();
    assert_eq!(
        codes,
        vec![("billing".into(), 68), ("audit".into(), 0), ("nobody".into(), 69)],
        "NON_EMPTY_GROUP, deleted, GROUP_ID_NOT_FOUND"
    );
    assert_eq!(names(&mut c).await, vec!["billing", "spare"]);

    // Offsets of a group with no members go partition by partition; the group goes with its last one.
    let gone = c.send(&offset_delete("spare", &[0]), 0).await.unwrap();
    assert_eq!((gone.error_code, gone.topics[0].partitions[0].error_code), (0, 0));
    assert_eq!(names(&mut c).await, vec!["billing"]);
    assert_eq!(
        c.send(&offset_delete("spare", &[0]), 0).await.unwrap().error_code,
        69
    );

    // Once its member has left, the group and its offsets can go.
    let left = c
        .send(
            &kafka_protocol::messages::LeaveGroupRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_members(vec![
                    kafka_protocol::messages::leave_group_request::MemberIdentity::default()
                        .with_member_id(s(&member)),
                ]),
            5,
        )
        .await
        .unwrap();
    assert_eq!(left.error_code, 0);
    assert_eq!(
        c.send(&offset_delete("billing", &[0]), 0).await.unwrap().topics[0].partitions[0].error_code,
        0
    );
    commit_only(&mut c, "billing").await;
    assert_eq!(
        c.send(&delete_groups(&["billing"]), 0).await.unwrap().results[0].error_code,
        0
    );
    assert!(names(&mut c).await.is_empty());
    let fetched = c
        .send(
            &kafka_protocol::messages::OffsetFetchRequest::default()
                .with_group_id(GroupId(s("billing")))
                .with_topics(None),
            7,
        )
        .await
        .unwrap();
    assert!(fetched.topics.is_empty(), "no offsets left: {fetched:?}");
    broker.shutdown().await;
}
