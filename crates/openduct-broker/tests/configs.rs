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
            kept.contains(&("cleanup.policy".into(), "delete".into(), true, 6)),
            "v{version}: {kept:?}"
        );
        assert!(
            kept.contains(&("retention.ms".into(), "-1".into(), false, 6)),
            "kept for ever, by default"
        );
        assert!(kept.contains(&("compression.type".into(), "producer".into(), true, 6)));
        assert!(
            kept.iter()
                .all(|(name, _, read_only, _)| *read_only == (name != "retention.ms")),
            "only retention can be changed"
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
        ("kept", ("cleanup.policy", 0, Some("compact")), 40),
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
