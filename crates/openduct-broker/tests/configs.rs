// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! DescribeCluster and DescribeConfigs over the wire, at every version: the brokers and the controller, and the
//! settings of a topic and of a broker, as admin tools ask for them.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig};
use kafka_protocol::messages::{
    DescribeClusterRequest, DescribeConfigsRequest, IncrementalAlterConfigsRequest,
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
    for (name, retention_ms) in [("kept", None), ("hourly", Some(3_600_000))] {
        store
            .create_topic(
                name,
                TopicConfig {
                    partitions: 1,
                    retention_ms,
                },
            )
            .unwrap();
    }
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.node_id = 3;
    config.cluster_id = "test-cluster".to_owned();
    let broker = Broker::start(config, store).await.unwrap();
    let client = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    (broker, client)
}

fn resource(kind: i8, name: &str, keys: Option<&[&str]>) -> DescribeConfigsResource {
    DescribeConfigsResource::default()
        .with_resource_type(kind)
        .with_resource_name(s(name))
        .with_configuration_keys(keys.map(|k| k.iter().map(|k| s(k)).collect()))
}

/// The settings of one result, by name: (value, read-only, source).
fn settings(r: &DescribeConfigsResult) -> Vec<(String, String, bool, i8)> {
    r.configs
        .iter()
        .map(|c| {
            (
                c.name.to_string(),
                c.value.as_ref().map(ToString::to_string).unwrap_or_default(),
                c.read_only,
                c.config_source,
            )
        })
        .collect()
}

#[tokio::test]
async fn the_cluster_is_described_with_its_brokers_and_controller() {
    let (broker, mut c) = start().await;
    for version in 0..=2 {
        let res = c
            .send(&DescribeClusterRequest::default().with_endpoint_type(1), version)
            .await
            .unwrap();
        assert_eq!(res.error_code, 0, "v{version}");
        assert_eq!(res.cluster_id.as_str(), "test-cluster");
        assert_eq!(
            res.controller_id.0, 3,
            "a broker on its own is its own controller"
        );
        assert_eq!(res.brokers.len(), 1);
        assert_eq!(res.brokers[0].broker_id.0, 3);
        assert_eq!(res.brokers[0].port, i32::from(broker.local_addr().port()));
        assert_eq!(res.cluster_authorized_operations, i32::MIN, "not asked for");
    }
    let asked = c
        .send(
            &DescribeClusterRequest::default()
                .with_endpoint_type(1)
                .with_include_cluster_authorized_operations(true),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        asked.cluster_authorized_operations, 8096,
        "no rules: every cluster operation"
    );
    // The KRaft controllers' endpoint is not this broker's to answer for; an unknown type is refused too.
    let controllers = c
        .send(&DescribeClusterRequest::default().with_endpoint_type(2), 1)
        .await
        .unwrap();
    assert_eq!(controllers.error_code, 114, "MISMATCHED_ENDPOINT_TYPE");
    let unknown = c
        .send(&DescribeClusterRequest::default().with_endpoint_type(9), 1)
        .await
        .unwrap();
    assert_eq!(unknown.error_code, 115, "UNSUPPORTED_ENDPOINT_TYPE");
    broker.shutdown().await;
}

#[tokio::test]
async fn a_topics_and_a_brokers_settings_are_what_the_broker_acts_on() {
    let (broker, mut c) = start().await;
    for version in 1..=4 {
        let res = c
            .send(
                &DescribeConfigsRequest::default().with_resources(vec![
                    resource(2, "kept", None),
                    resource(2, "hourly", Some(&["retention.ms", "no.such.setting"])),
                    resource(2, "nowhere", None),
                ]),
                version,
            )
            .await
            .unwrap();
        let kept = settings(&res.results[0]);
        assert!(
            kept.contains(&("cleanup.policy".into(), "delete".into(), false, 6)),
            "v{version}: {kept:?}"
        );
        assert!(
            kept.contains(&("retention.ms".into(), "-1".into(), false, 6)),
            "kept for ever, by default"
        );
        assert!(kept.contains(&("compression.type".into(), "producer".into(), true, 6)));
        let changeable = [
            "retention.ms",
            "retention.bytes",
            "max.message.bytes",
            "cleanup.policy",
            "delete.retention.ms",
            "min.compaction.lag.ms",
            "message.timestamp.type",
            "min.insync.replicas",
            "segment.ms",
        ];
        assert!(
            kept.iter()
                .all(|(name, _, read_only, _)| *read_only != changeable.contains(&name.as_str())),
            "retention and the topic's own settings can be changed, nothing else: {kept:?}"
        );
        assert!(
            kept.contains(&("retention.bytes".into(), "-1".into(), false, 6)),
            "no size limit, by default"
        );
        assert_eq!(
            settings(&res.results[1]),
            vec![("retention.ms".into(), "3600000".into(), false, 1)],
            "only the keys asked for, the topic's own value"
        );
        assert_eq!(res.results[2].error_code, 3, "UNKNOWN_TOPIC_OR_PARTITION");
    }

    // A broker's settings: this broker's by its id; the empty name (cluster defaults) has none; another id is not
    // this broker's to answer.
    let res = c
        .send(
            &DescribeConfigsRequest::default()
                .with_resources(vec![
                    resource(4, "3", None),
                    resource(4, "", None),
                    resource(4, "7", None),
                ])
                .with_include_synonyms(true)
                .with_include_documentation(true),
            4,
        )
        .await
        .unwrap();
    let own = &res.results[0];
    assert_eq!(own.error_code, 0);
    let named = settings(own);
    assert!(
        named.contains(&("broker.id".into(), "3".into(), true, 5)),
        "{named:?}"
    );
    assert!(named.contains(&("auto.create.topics.enable".into(), "false".into(), true, 5)));
    assert!(
        own.configs
            .iter()
            .all(|c| c.documentation.is_some() && c.synonyms.len() == 1 && c.config_type > 0)
    );
    assert_eq!((res.results[1].error_code, res.results[1].configs.len()), (0, 0));
    assert_eq!(res.results[2].error_code, 42, "INVALID_REQUEST");
    assert!(
        res.results[2]
            .error_message
            .as_ref()
            .unwrap()
            .contains("broker 3")
    );
    // Broker loggers (type 8) are not served.
    let loggers = c
        .send(
            &DescribeConfigsRequest::default().with_resources(vec![resource(8, "3", None)]),
            4,
        )
        .await
        .unwrap();
    assert_eq!(loggers.results[0].error_code, 42);
    broker.shutdown().await;
}

fn alter(
    name: &str,
    configs: &[(&str, i8, Option<&str>)],
    validate_only: bool,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest::default()
        .with_validate_only(validate_only)
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(s(name))
                .with_configs(
                    configs
                        .iter()
                        .map(|(n, op, v)| {
                            AlterableConfig::default()
                                .with_name(s(n))
                                .with_config_operation(*op)
                                .with_value(v.map(s))
                        })
                        .collect(),
                ),
        ])
}

async fn retention_of(c: &mut Client, topic: &str) -> (String, i8) {
    let res = c
        .send(
            &DescribeConfigsRequest::default().with_resources(vec![resource(
                2,
                topic,
                Some(&["retention.ms"]),
            )]),
            4,
        )
        .await
        .unwrap();
    let r = &res.results[0].configs[0];
    (r.value.as_ref().unwrap().to_string(), r.config_source)
}

#[tokio::test]
async fn a_topics_retention_is_changed_and_nothing_else_is() {
    let (broker, mut c) = start().await;
    for version in 0..=1 {
        let set = c
            .send(
                &alter("kept", &[("retention.ms", 0, Some("86400000"))], false),
                version,
            )
            .await
            .unwrap();
        assert_eq!(set.responses[0].error_code, 0, "v{version}");
        assert_eq!(
            retention_of(&mut c, "kept").await,
            ("86400000".into(), 1),
            "the topic's own now"
        );
        let back = c
            .send(&alter("kept", &[("retention.ms", 1, None)], false), version)
            .await
            .unwrap();
        assert_eq!(back.responses[0].error_code, 0);
        assert_eq!(
            retention_of(&mut c, "kept").await,
            ("-1".into(), 6),
            "deleted: back to for ever"
        );
    }
    // -1 is for ever too; validate_only checks and changes nothing.
    c.send(&alter("hourly", &[("retention.ms", 0, Some("-1"))], false), 1)
        .await
        .unwrap();
    assert_eq!(retention_of(&mut c, "hourly").await.0, "-1");
    let checked = c
        .send(&alter("hourly", &[("retention.ms", 0, Some("5000"))], true), 1)
        .await
        .unwrap();
    assert_eq!(checked.responses[0].error_code, 0);
    assert_eq!(retention_of(&mut c, "hourly").await.0, "-1", "only checked");

    // Refused, each with its reason: a bad value, adding to a number, another setting, an unknown topic.
    for (topic, config, code) in [
        ("kept", ("retention.ms", 0, Some("soon")), 40),
        ("kept", ("retention.ms", 0, Some("-7")), 40),
        ("kept", ("retention.ms", 2, Some("5")), 40),
        ("kept", ("cleanup.policy", 0, Some("squash")), 40),
        ("kept", ("segment.jitter.ms", 0, Some("5")), 40),
        ("nowhere", ("retention.ms", 0, Some("5")), 3),
    ] {
        let res = c.send(&alter(topic, &[config], false), 1).await.unwrap();
        assert_eq!(res.responses[0].error_code, code, "{topic} {config:?}");
        assert!(res.responses[0].error_message.is_some());
    }
    assert_eq!(
        retention_of(&mut c, "kept").await.0,
        "-1",
        "nothing refused was changed"
    );
    // A broker's settings are its start-up's.
    let broker_alter = c
        .send(
            &IncrementalAlterConfigsRequest::default().with_resources(vec![
                AlterConfigsResource::default()
                    .with_resource_type(4)
                    .with_resource_name(s("3")),
            ]),
            1,
        )
        .await
        .unwrap();
    assert_eq!(broker_alter.responses[0].error_code, 42);
    broker.shutdown().await;
}

/// One setting of a topic as DescribeConfigs answers it: (value, source).
async fn setting_of(c: &mut Client, topic: &str, name: &str) -> (String, i8) {
    let res = c
        .send(
            &DescribeConfigsRequest::default().with_resources(vec![resource(2, topic, Some(&[name]))]),
            4,
        )
        .await
        .unwrap();
    let r = &res.results[0].configs[0];
    (r.value.as_ref().unwrap().to_string(), r.config_source)
}

fn replace(
    name: &str,
    configs: &[(&str, &str)],
    validate_only: bool,
) -> kafka_protocol::messages::AlterConfigsRequest {
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
    kafka_protocol::messages::AlterConfigsRequest::default()
        .with_validate_only(validate_only)
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(s(name))
                .with_configs(
                    configs
                        .iter()
                        .map(|(n, v)| AlterableConfig::default().with_name(s(n)).with_value(Some(s(v))))
                        .collect(),
                ),
        ])
}

#[tokio::test]
async fn a_topics_own_settings_are_set_one_at_a_time_and_a_bad_one_is_refused_whole() {
    let (broker, mut c) = start().await;
    let set = c
        .send(
            &alter(
                "kept",
                &[
                    ("retention.bytes", 0, Some("1048576")),
                    ("max.message.bytes", 0, Some("4096")),
                ],
                false,
            ),
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        set.responses[0].error_code, 0,
        "{:?}",
        set.responses[0].error_message
    );
    assert_eq!(
        setting_of(&mut c, "kept", "retention.bytes").await,
        ("1048576".into(), 1)
    );
    assert_eq!(
        setting_of(&mut c, "kept", "max.message.bytes").await,
        ("4096".into(), 1)
    );
    assert_eq!(
        setting_of(&mut c, "hourly", "retention.bytes").await,
        ("-1".into(), 6),
        "another topic's is its own"
    );

    // Deleted: back to the default. The other one stays.
    c.send(&alter("kept", &[("retention.bytes", 1, None)], false), 1)
        .await
        .unwrap();
    assert_eq!(
        setting_of(&mut c, "kept", "retention.bytes").await,
        ("-1".into(), 6)
    );
    assert_eq!(setting_of(&mut c, "kept", "max.message.bytes").await.0, "4096");

    // A request with one bad setting changes none of them; a setting OpenDuct would ignore is refused by name.
    for bad in [
        ("retention.bytes", Some("-9")),
        ("max.message.bytes", Some("0")),
        ("segment.jitter.ms", Some("1000")),
    ] {
        let res = c
            .send(
                &alter(
                    "kept",
                    &[("retention.bytes", 0, Some("77")), (bad.0, 0, bad.1)],
                    false,
                ),
                1,
            )
            .await
            .unwrap();
        assert_eq!(res.responses[0].error_code, 40, "{bad:?}: INVALID_CONFIG");
        assert!(res.responses[0].error_message.as_ref().unwrap().contains(bad.0));
    }
    assert_eq!(
        setting_of(&mut c, "kept", "retention.bytes").await.0,
        "-1",
        "nothing of a refused request stays"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn alter_configs_replaces_every_setting_with_the_ones_it_names() {
    let (broker, mut c) = start().await;
    c.send(
        &alter("hourly", &[("max.message.bytes", 0, Some("2048"))], false),
        1,
    )
    .await
    .unwrap();
    for version in 0..=2 {
        // Checked only: nothing changes.
        let checked = c
            .send(&replace("hourly", &[("retention.bytes", "5000")], true), version)
            .await
            .unwrap();
        assert_eq!(checked.responses[0].error_code, 0, "v{version}");
        assert_eq!(setting_of(&mut c, "hourly", "retention.bytes").await.0, "-1");

        let done = c
            .send(&replace("hourly", &[("retention.bytes", "5000")], false), version)
            .await
            .unwrap();
        assert_eq!(
            done.responses[0].error_code, 0,
            "v{version}: {:?}",
            done.responses[0].error_message
        );
        assert_eq!(
            setting_of(&mut c, "hourly", "retention.bytes").await,
            ("5000".into(), 1)
        );
        // What it did not name went back to its default - that is what this older API means.
        assert_eq!(setting_of(&mut c, "hourly", "max.message.bytes").await.1, 6);
        assert_eq!(
            retention_of(&mut c, "hourly").await,
            ("-1".into(), 6),
            "retention.ms too"
        );

        // Put back for the next version.
        c.send(
            &replace(
                "hourly",
                &[("retention.ms", "3600000"), ("max.message.bytes", "2048")],
                false,
            ),
            version,
        )
        .await
        .unwrap();
        assert_eq!(retention_of(&mut c, "hourly").await.0, "3600000");
        assert_eq!(setting_of(&mut c, "hourly", "retention.bytes").await.1, 6);
    }
    let refused = c
        .send(&replace("hourly", &[("compression.type", "gzip")], false), 2)
        .await
        .unwrap();
    assert_eq!(refused.responses[0].error_code, 40);
    let unknown = c
        .send(&replace("nowhere", &[("retention.bytes", "1")], false), 2)
        .await
        .unwrap();
    assert_eq!(unknown.responses[0].error_code, 3);
    assert_eq!(
        retention_of(&mut c, "hourly").await.0,
        "3600000",
        "a refused replacement leaves the topic alone"
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn a_topic_is_created_with_its_settings_and_refuses_a_batch_larger_than_it_takes() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
    use kafka_protocol::messages::{CreateTopicsRequest, TopicName};
    use openduct_broker::client::{ClientError, Outgoing};

    let (broker, mut c) = start().await;
    let create = |name: &str, configs: &[(&str, &str)]| {
        CreateTopicsRequest::default()
            .with_timeout_ms(5_000)
            .with_topics(vec![
                CreatableTopic::default()
                    .with_name(TopicName(s(name)))
                    .with_num_partitions(1)
                    .with_replication_factor(1)
                    .with_configs(
                        configs
                            .iter()
                            .map(|(n, v)| {
                                CreatableTopicConfig::default()
                                    .with_name(s(n))
                                    .with_value(Some(s(v)))
                            })
                            .collect(),
                    ),
            ])
    };
    let made = c
        .send(
            &create(
                "small",
                &[("max.message.bytes", "200"), ("retention.bytes", "100000")],
            ),
            7,
        )
        .await
        .unwrap();
    assert_eq!(made.topics[0].error_code, 0, "{:?}", made.topics[0].error_message);
    assert_eq!(
        setting_of(&mut c, "small", "max.message.bytes").await,
        ("200".into(), 1)
    );
    assert_eq!(
        setting_of(&mut c, "small", "retention.bytes").await,
        ("100000".into(), 1)
    );

    let bad = c
        .send(&create("never", &[("max.message.bytes", "none")]), 7)
        .await
        .unwrap();
    assert_eq!(
        bad.topics[0].error_code, 40,
        "INVALID_CONFIG, and no topic is made"
    );
    assert!(c.send(&replace("never", &[], true), 2).await.unwrap().responses[0].error_code == 3);

    let message = |bytes: usize| Outgoing {
        key: None,
        value: Some(bytes::Bytes::from(vec![b'x'; bytes])),
        headers: Vec::new(),
    };
    assert_eq!(
        c.produce("small", 0, &[message(20)], 1).await.unwrap(),
        0,
        "one that fits is taken"
    );
    match c.produce("small", 0, &[message(500)], 2).await {
        Err(ClientError::Refused { code: 10, message }) => {
            assert!(message.unwrap().contains("larger than the topic takes"));
        }
        other => panic!("MESSAGE_TOO_LARGE expected, got {other:?}"),
    }
    // The same batch is taken by a topic with no limit of its own, and by this one once its limit is raised.
    assert_eq!(c.produce("kept", 0, &[message(500)], 3).await.unwrap(), 0);
    c.send(&alter("small", &[("max.message.bytes", 1, None)], false), 1)
        .await
        .unwrap();
    assert_eq!(c.produce("small", 0, &[message(500)], 4).await.unwrap(), 1);
    broker.shutdown().await;
}

#[tokio::test]
async fn a_compacted_topic_gives_a_consumer_the_newest_record_of_each_key() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
    use kafka_protocol::messages::{CreateTopicsRequest, TopicName};
    use kafka_protocol::records::RecordBatchDecoder;
    use openduct_broker::client::Outgoing;

    // Its own broker: compaction runs in the background round, here every 100 ms.
    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.retention_check_interval = Duration::from_millis(100);
    let broker = Broker::start(config, Arc::new(MemoryStore::new())).await.unwrap();
    let mut c = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    let made = c
        .send(
            &CreateTopicsRequest::default()
                .with_timeout_ms(5_000)
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(TopicName(s("prices")))
                        .with_num_partitions(1)
                        .with_replication_factor(1)
                        .with_configs(vec![
                            CreatableTopicConfig::default()
                                .with_name(s("cleanup.policy"))
                                .with_value(Some(s("compact"))),
                            CreatableTopicConfig::default()
                                .with_name(s("delete.retention.ms"))
                                .with_value(Some(s("0"))),
                        ]),
                ]),
            7,
        )
        .await
        .unwrap();
    assert_eq!(made.topics[0].error_code, 0, "{:?}", made.topics[0].error_message);
    assert_eq!(
        setting_of(&mut c, "prices", "cleanup.policy").await,
        ("compact".into(), 1)
    );

    let record = |key: &str, value: Option<&str>| Outgoing {
        key: Some(bytes::Bytes::from(key.as_bytes().to_vec())),
        value: value.map(|v| bytes::Bytes::from(v.as_bytes().to_vec())),
        headers: Vec::new(),
    };
    // Each produce is a batch: tea twice, coffee, coffee deleted, milk - then sugar, the newest batch, which
    // the memory store never cleans.
    let written = [
        vec![record("tea", Some("1.00")), record("coffee", Some("2.00"))],
        vec![record("tea", Some("1.10"))],
        vec![record("coffee", None)],
        vec![record("milk", Some("0.80"))],
        vec![record("sugar", Some("0.50"))],
    ];
    for (i, batch) in written.iter().enumerate() {
        c.produce("prices", 0, batch, 1_000 + i as i64).await.unwrap();
    }

    // What a consumer reading from the start sees, decoded by the Kafka protocol library's own reader.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let seen = loop {
        let answers = c.fetch("prices", &[(0, 0)], 0, 1 << 20).await.unwrap();
        assert_eq!(
            (answers[0].log_start, answers[0].high_watermark),
            (0, 6),
            "offsets never move"
        );
        let mut batches = answers[0].batches.clone();
        let mut seen = Vec::new();
        while !batches.is_empty() {
            let set = RecordBatchDecoder::decode(&mut batches).unwrap();
            for r in set.records {
                let key = String::from_utf8(r.key.unwrap().to_vec()).unwrap();
                let value = r.value.map(|v| String::from_utf8(v.to_vec()).unwrap());
                seen.push((r.offset, key, value));
            }
        }
        if seen.len() == 3 {
            break seen;
        }
        assert!(std::time::Instant::now() < deadline, "never compacted: {seen:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        seen,
        vec![
            (2, "tea".to_owned(), Some("1.10".to_owned())),
            (4, "milk".into(), Some("0.80".into())),
            (5, "sugar".into(), Some("0.50".into())),
        ],
        "the newest of each key at the offset it was written at; coffee and its tombstone gone"
    );
    // A fetch from an offset whose record went starts at the next one still there.
    let from_three = c.fetch("prices", &[(0, 3)], 0, 1 << 20).await.unwrap();
    assert_eq!(from_three[0].error_code, 0);
    assert!(!from_three[0].batches.is_empty());
    // And the policy can be turned off again.
    c.send(&alter("prices", &[("cleanup.policy", 1, None)], false), 1)
        .await
        .unwrap();
    assert_eq!(
        setting_of(&mut c, "prices", "cleanup.policy").await,
        ("delete".into(), 6)
    );
    broker.shutdown().await;
}

#[tokio::test]
async fn compressed_batches_of_a_compacted_topic_are_cleaned_and_read_back_by_the_library() {
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{ProduceRequest, TopicName};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use openduct_broker::client::records::{compress, decompress};

    let mut config = BrokerConfig::new("127.0.0.1:0".parse().unwrap());
    config.advertised_port = 0;
    config.retention_check_interval = Duration::from_millis(100);
    let store = Arc::new(MemoryStore::new());
    let broker = Broker::start(config, store.clone()).await.unwrap();
    let mut c = Client::connect(&broker.local_addr().to_string(), Duration::from_secs(10))
        .await
        .unwrap();
    // The Kafka protocol library encodes and compresses each batch, as a client would.
    let encoded = |pairs: &[(&str, &str)], compression: Compression| {
        let records: Vec<Record> = pairs
            .iter()
            .enumerate()
            .map(|(i, (key, value))| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: i64::try_from(i).unwrap(),
                sequence: -1,
                timestamp: 1_000,
                key: Some(bytes::Bytes::copy_from_slice(key.as_bytes())),
                value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                headers: IndexMap::new(),
            })
            .collect();
        let mut buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
            Some(compress),
        )
        .unwrap();
        buf.freeze()
    };
    for (topic, compression) in [
        ("gz", Compression::Gzip),
        ("sn", Compression::Snappy),
        ("lz", Compression::Lz4),
        ("zs", Compression::Zstd),
    ] {
        store
            .create_topic(
                topic,
                TopicConfig {
                    partitions: 1,
                    retention_ms: None,
                },
            )
            .unwrap();
        c.send(&alter(topic, &[("cleanup.policy", 0, Some("compact"))], false), 1)
            .await
            .unwrap();
        // k1 and k2; k1 again with k3 (this batch loses nothing); k2 again; then the newest batch.
        for pairs in [
            &[("k1", "old"), ("k2", "old")][..],
            &[("k1", "new"), ("k3", "only")],
            &[("k2", "new")],
            &[("k4", "last")],
        ] {
            let request = ProduceRequest::default()
                .with_acks(-1)
                .with_timeout_ms(5_000)
                .with_topic_data(vec![
                    TopicProduceData::default()
                        .with_name(TopicName(s(topic)))
                        .with_partition_data(vec![
                            PartitionProduceData::default()
                                .with_index(0)
                                .with_records(Some(encoded(pairs, compression))),
                        ]),
                ]);
            let answer = c.send(&request, 9).await.unwrap();
            assert_eq!(
                answer.responses[0].partition_responses[0].error_code, 0,
                "{topic}"
            );
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let seen = loop {
            let answers = c.fetch(topic, &[(0, 0)], 0, 1 << 20).await.unwrap();
            let mut batches = answers[0].batches.clone();
            let mut seen = Vec::new();
            while !batches.is_empty() {
                let set = RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(decompress))
                    .unwrap();
                for r in set.records {
                    seen.push((
                        r.offset,
                        String::from_utf8(r.key.unwrap().to_vec()).unwrap(),
                        String::from_utf8(r.value.unwrap().to_vec()).unwrap(),
                    ));
                }
            }
            if seen.len() == 4 {
                break seen;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{topic} never compacted: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(
            seen,
            vec![
                (2, "k1".to_owned(), "new".to_owned()),
                (3, "k3".into(), "only".into()),
                (4, "k2".into(), "new".into()),
                (5, "k4".into(), "last".into()),
            ],
            "{topic}: the newest of each key, at its offset, through the library's own decompression"
        );
    }
    broker.shutdown().await;
}

#[tokio::test]
async fn a_keyless_record_is_refused_by_a_compacted_topic_and_the_brokers_time_is_stamped_when_asked() {
    use kafka_protocol::records::RecordBatchDecoder;
    use openduct_broker::client::{ClientError, Outgoing};

    let (broker, mut c) = start().await;
    let message = |key: Option<&str>| Outgoing {
        key: key.map(|k| bytes::Bytes::from(k.as_bytes().to_vec())),
        value: Some(bytes::Bytes::from_static(b"v")),
        headers: Vec::new(),
    };
    let set = c
        .send(
            &alter(
                "kept",
                &[
                    ("cleanup.policy", 0, Some("compact")),
                    ("message.timestamp.type", 0, Some("LogAppendTime")),
                    ("segment.ms", 0, Some("60000")),
                ],
                false,
            ),
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        set.responses[0].error_code, 0,
        "{:?}",
        set.responses[0].error_message
    );
    assert_eq!(
        setting_of(&mut c, "kept", "message.timestamp.type").await,
        ("LogAppendTime".into(), 1)
    );
    assert_eq!(
        setting_of(&mut c, "kept", "segment.ms").await,
        ("60000".into(), 1)
    );
    assert_eq!(
        setting_of(&mut c, "hourly", "segment.ms").await,
        ("604800000".into(), 6)
    );

    // INVALID_RECORD, as Kafka answers it, and nothing is stored.
    match c
        .produce("kept", 0, &[message(Some("k")), message(None)], 5)
        .await
    {
        Err(ClientError::Refused { code: 87, message }) => {
            assert!(message.unwrap().contains("only records with a key"));
        }
        other => panic!("INVALID_RECORD expected, got {other:?}"),
    }
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert_eq!(c.produce("kept", 0, &[message(Some("k"))], 5).await.unwrap(), 0);
    let answers = c.fetch("kept", &[(0, 0)], 0, 1 << 20).await.unwrap();
    // The batch says its timestamps are the broker's (the timestamp-type bit) and carries the broker's time as
    // its largest timestamp - which is what a Kafka client then gives every record of it.
    let stored = answers[0].batches.clone();
    let header = openduct_storage::batch::parse_stored_header(&stored).unwrap();
    assert!(
        header.max_timestamp >= before,
        "the broker's time, not the producer's 5"
    );
    let mut batches = stored;
    let set = RecordBatchDecoder::decode(&mut batches).unwrap();
    assert_eq!(set.records.len(), 1);
    assert_eq!(
        set.records[0].timestamp_type,
        kafka_protocol::records::TimestampType::LogAppend,
        "read as such by the protocol library"
    );
    // A topic that does not compact still takes records without a key; one broker promises one copy.
    assert_eq!(c.produce("hourly", 0, &[message(None)], 5).await.unwrap(), 0);
    let two = c
        .send(
            &alter("hourly", &[("min.insync.replicas", 0, Some("2"))], false),
            1,
        )
        .await
        .unwrap();
    assert_eq!(two.responses[0].error_code, 40);
    assert!(
        two.responses[0]
            .error_message
            .as_ref()
            .unwrap()
            .contains("one copy")
    );
    assert_eq!(
        setting_of(&mut c, "hourly", "min.insync.replicas").await,
        ("1".into(), 6)
    );
    broker.shutdown().await;
}
