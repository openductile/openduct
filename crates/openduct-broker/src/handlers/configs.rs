// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! DescribeCluster, DescribeConfigs and IncrementalAlterConfigs: the brokers and the controller, and the settings of
//! a topic or a broker. Every setting answered is one OpenDuct honours, with the value it acts on. A topic's
//! `retention.ms` and the settings in `openduct_storage::TOPIC_SETTINGS` (`cleanup.policy`, `retention.bytes`,
//! `max.message.bytes`, `delete.retention.ms`, `min.compaction.lag.ms`, `message.timestamp.type`,
//! `min.insync.replicas`, `segment.ms`)
//! can be changed - by IncrementalAlterConfigs, one at a time, or by AlterConfigs, which replaces them all; every
//! other setting is read-only, and one OpenDuct would store and ignore is refused.

use kafka_protocol::messages::alter_configs_request::AlterConfigsResource as WholeResource;
use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse as WholeResourceResponse;
use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
};
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::{AlterConfigsRequest, AlterConfigsResponse};
use kafka_protocol::messages::{
    BrokerId, DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest,
    DescribeConfigsResponse,
};
use kafka_protocol::messages::{IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse};
use openduct_storage::{MAX_BATCH_BYTES, SettingKind, TOPIC_SETTINGS, TopicInfo, TopicSettings};

use super::{State, blocking, s, topic_info};
use crate::codes;
use crate::security::Operation;

/// Kafka's answer for authorized operations not asked for.
const NOT_ASKED: i32 = i32::MIN;
/// DescribeCluster's endpoint types (KIP-919): brokers, or the KRaft controllers (which a broker does not answer for).
const BROKERS: i8 = 1;
const CONTROLLERS: i8 = 2;

/// `may`: whether the principal may do an operation on the cluster.
pub(crate) fn describe_cluster(
    state: &State,
    req: &DescribeClusterRequest,
    version: i16,
    may: impl Fn(Operation) -> bool,
) -> DescribeClusterResponse {
    let res = DescribeClusterResponse::default()
        .with_endpoint_type(BROKERS)
        .with_cluster_id(s(state.config.cluster_id.clone()))
        .with_controller_id(BrokerId(-1))
        .with_cluster_authorized_operations(NOT_ASKED);
    if version >= 1 && req.endpoint_type != BROKERS {
        let (code, why) = if req.endpoint_type == CONTROLLERS {
            (
                codes::MISMATCHED_ENDPOINT_TYPE,
                "this is a broker: it answers for brokers only",
            )
        } else {
            (codes::UNSUPPORTED_ENDPOINT_TYPE, "an endpoint type not known")
        };
        return res.with_error_code(code).with_error_message(Some(s(why)));
    }
    let brokers = super::cluster::brokers(state);
    let operations = if req.include_cluster_authorized_operations {
        cluster_operations(may)
    } else {
        NOT_ASKED
    };
    res.with_controller_id(brokers.coordinator.unwrap_or(BrokerId(-1)))
        .with_cluster_authorized_operations(operations)
        .with_brokers(
            brokers
                .brokers
                .into_iter()
                .map(|b| {
                    DescribeClusterBroker::default()
                        .with_broker_id(b.node_id)
                        .with_host(b.host)
                        .with_port(b.port)
                        .with_rack(b.rack)
                })
                .collect(),
        )
}

/// The operations allowed on the cluster, as Kafka's bit field (bit n for operation code n).
fn cluster_operations(may: impl Fn(Operation) -> bool) -> i32 {
    [
        (Operation::Create, 5),
        (Operation::Alter, 7),
        (Operation::Describe, 8),
        (Operation::ClusterAction, 9),
        (Operation::DescribeConfigs, 10),
        (Operation::AlterConfigs, 11),
        (Operation::IdempotentWrite, 12),
    ]
    .into_iter()
    .filter(|(op, _)| may(*op))
    .fold(0, |bits, (_, code)| bits | (1 << code))
}

// --- DescribeConfigs -------------------------------------------------------------------------------------------

const TOPIC: i8 = 2;
const BROKER: i8 = 4;

/// Where a value comes from (Kafka's config sources).
const FROM_TOPIC: i8 = 1;
const STATIC_BROKER: i8 = 5;
const DEFAULT: i8 = 6;

/// Kafka's config types.
const STRING: i8 = 2;
const INT: i8 = 3;
const LONG: i8 = 5;
const BOOLEAN: i8 = 1;
const LIST: i8 = 7;

/// One setting: its name, value, where the value comes from, its type, and what it does.
struct Setting {
    name: &'static str,
    value: String,
    source: i8,
    kind: i8,
    about: &'static str,
    /// Whether IncrementalAlterConfigs changes it (a topic's `retention.ms` only).
    changeable: bool,
}

fn setting(name: &'static str, value: impl ToString, source: i8, kind: i8, about: &'static str) -> Setting {
    Setting {
        name,
        value: value.to_string(),
        source,
        kind,
        about,
        changeable: false,
    }
}

fn millis(d: std::time::Duration) -> u128 {
    d.as_millis()
}

/// The largest record batch taken: the store's limit, or a request's when that is smaller.
fn largest_batch(state: &State) -> usize {
    MAX_BATCH_BYTES.min(state.config.max_request_bytes)
}

fn topic_settings(topic: &TopicInfo) -> Vec<Setting> {
    let mut out = vec![Setting {
        changeable: true,
        ..match topic.retention_ms {
            Some(ms) => setting("retention.ms", ms, FROM_TOPIC, LONG, "How long a batch is kept."),
            None => setting(
                "retention.ms",
                -1,
                DEFAULT,
                LONG,
                "How long a batch is kept: -1, for ever.",
            ),
        }
    }];
    // The topic's own settings (openduct_storage settings.rs): its value where it was given one, else the default.
    for spec in TOPIC_SETTINGS {
        let own = topic.settings.own(spec.name);
        out.push(Setting {
            changeable: true,
            ..setting(
                spec.name,
                openduct_storage::setting_text(spec.name, own.unwrap_or(spec.default)),
                if own.is_some() { FROM_TOPIC } else { DEFAULT },
                match spec.kind {
                    SettingKind::Long { .. } => LONG,
                    SettingKind::Int { .. } => INT,
                    SettingKind::Policy => LIST,
                    SettingKind::Words(_) => STRING,
                },
                spec.about,
            )
        });
    }
    out.push(setting(
        "compression.type",
        "producer",
        DEFAULT,
        STRING,
        "Batches are stored as the producer compressed them.",
    ));
    out
}

fn broker_settings(state: &State) -> Vec<Setting> {
    let c = &state.config;
    vec![
        setting("broker.id", c.node_id, STATIC_BROKER, INT, "This broker's id."),
        setting(
            "num.partitions",
            c.default_partitions,
            STATIC_BROKER,
            INT,
            "Partitions of a topic created without a count.",
        ),
        setting(
            "auto.create.topics.enable",
            c.auto_create_topics,
            STATIC_BROKER,
            BOOLEAN,
            "Whether a topic is created when a client first uses it.",
        ),
        setting(
            "log.retention.ms",
            -1,
            DEFAULT,
            LONG,
            "How long a topic without its own retention keeps data: for ever.",
        ),
        setting(
            "message.max.bytes",
            largest_batch(state),
            STATIC_BROKER,
            INT,
            "The largest record batch taken.",
        ),
        setting(
            "socket.request.max.bytes",
            c.max_request_bytes,
            STATIC_BROKER,
            INT,
            "The largest request taken.",
        ),
        setting(
            "queued.max.request.bytes",
            c.max_request_memory,
            STATIC_BROKER,
            LONG,
            "The most request bytes held at once.",
        ),
        setting(
            "fetch.max.bytes",
            c.max_fetch_bytes,
            STATIC_BROKER,
            INT,
            "The most bytes one fetch answer carries.",
        ),
        setting(
            "max.connections",
            c.max_connections,
            STATIC_BROKER,
            INT,
            "The most client connections at once.",
        ),
        setting(
            "connections.max.idle.ms",
            millis(c.idle_timeout),
            STATIC_BROKER,
            LONG,
            "A connection idle this long is closed.",
        ),
        setting(
            "group.initial.rebalance.delay.ms",
            millis(c.group_initial_rebalance_delay),
            STATIC_BROKER,
            INT,
            "How long a new group waits for more members.",
        ),
        setting(
            "group.min.session.timeout.ms",
            millis(c.group_min_session_timeout),
            STATIC_BROKER,
            INT,
            "The shortest session a group member may ask for.",
        ),
        setting(
            "group.max.session.timeout.ms",
            millis(c.group_max_session_timeout),
            STATIC_BROKER,
            INT,
            "The longest session a group member may ask for.",
        ),
    ]
}

/// `may`: whether the principal may describe the configs of a topic (`Some(name)`) or of the cluster (`None`).
pub(crate) async fn describe_configs(
    state: &State,
    req: &DescribeConfigsRequest,
    version: i16,
    may: impl Fn(Option<&str>) -> bool,
) -> DescribeConfigsResponse {
    let mut results = Vec::new();
    for r in &req.resources {
        let name = r.resource_name.to_string();
        let out = DescribeConfigsResult::default()
            .with_resource_type(r.resource_type)
            .with_resource_name(r.resource_name.clone());
        let settings = match r.resource_type {
            // Refused before the topic is looked for, so a principal learns nothing of topics it may not see.
            TOPIC if !may(Some(&name)) => Err((crate::security::TOPIC_AUTHORIZATION_FAILED, None)),
            TOPIC => match topic_info(state, &name).await {
                Some(t) => Ok(topic_settings(&t)),
                None => Err((codes::UNKNOWN_TOPIC_OR_PARTITION, None)),
            },
            BROKER if !may(None) => Err((crate::security::CLUSTER_AUTHORIZATION_FAILED, None)),
            // The empty name is the cluster's default dynamic settings: there are none.
            BROKER if name.is_empty() => Ok(Vec::new()),
            BROKER if name == state.config.node_id.to_string() => Ok(broker_settings(state)),
            BROKER => Err((
                codes::INVALID_REQUEST,
                Some(format!(
                    "this is broker {}: ask broker {name} itself",
                    state.config.node_id
                )),
            )),
            _ => Err((
                codes::INVALID_REQUEST,
                Some("a resource type whose settings are not served".to_owned()),
            )),
        };
        let out = match settings {
            Ok(settings) => {
                out.with_configs(answered(settings, r.configuration_keys.as_deref(), req, version))
            }
            Err((code, why)) => out.with_error_code(code).with_error_message(why.map(s)),
        };
        results.push(out);
    }
    DescribeConfigsResponse::default().with_results(results)
}

fn answered(
    settings: Vec<Setting>,
    keys: Option<&[kafka_protocol::protocol::StrBytes]>,
    req: &DescribeConfigsRequest,
    version: i16,
) -> Vec<DescribeConfigsResourceResult> {
    settings
        .into_iter()
        .filter(|st| keys.is_none_or(|k| k.iter().any(|k| k.as_str() == st.name)))
        .map(|st| {
            let mut c = DescribeConfigsResourceResult::default()
                .with_name(s(st.name))
                .with_value(Some(s(st.value.clone())))
                .with_read_only(!st.changeable)
                .with_config_source(st.source);
            if req.include_synonyms {
                c = c.with_synonyms(vec![
                    DescribeConfigsSynonym::default()
                        .with_name(s(st.name))
                        .with_value(Some(s(st.value)))
                        .with_source(st.source),
                ]);
            }
            if version >= 3 {
                c = c.with_config_type(st.kind);
                if req.include_documentation {
                    c = c.with_documentation(Some(s(st.about)));
                }
            }
            c
        })
        .collect()
}

// --- IncrementalAlterConfigs ---------------------------------------------------------------------------------

/// The operations on a setting (Kafka's): set it, put it back to its default, or add to or take from a list.
const SET: i8 = 0;
const DELETE: i8 = 1;

/// A topic's `retention.ms` as a request changes it: `Ok(None)` for ever (Kafka's -1, or back to the default).
fn retention(op: i8, value: Option<&str>) -> Result<Option<i64>, String> {
    match (op, value) {
        (DELETE, _) => Ok(None),
        (SET, Some(v)) => match v.trim().parse::<i64>() {
            Ok(-1) => Ok(None),
            Ok(ms) if ms >= 0 => Ok(Some(ms)),
            _ => Err(format!(
                "retention.ms must be -1 (for ever) or a number of milliseconds, not {v:?}"
            )),
        },
        (SET, None) => Err("retention.ms needs a value".to_owned()),
        _ => Err("retention.ms is a number: it can be set or deleted, not added to or taken from".to_owned()),
    }
}

/// IncrementalAlterConfigs: a topic's changeable settings, each set or put back to its default. `may`: whether
/// the principal may alter the configs of a topic (`Some(name)`) or of the cluster (`None`).
pub(crate) async fn incremental_alter_configs(
    state: &State,
    req: &IncrementalAlterConfigsRequest,
    may: impl Fn(Option<&str>) -> bool,
) -> IncrementalAlterConfigsResponse {
    let mut responses = Vec::new();
    for r in &req.resources {
        let name = r.resource_name.to_string();
        let out = AlterConfigsResourceResponse::default()
            .with_resource_type(r.resource_type)
            .with_resource_name(r.resource_name.clone());
        let outcome = alter_one(state, r, &name, req.validate_only, &may).await;
        responses.push(match outcome {
            Ok(()) => out,
            Err((code, why)) => out.with_error_code(code).with_error_message(Some(s(why))),
        });
    }
    IncrementalAlterConfigsResponse::default().with_responses(responses)
}

async fn alter_one(
    state: &State,
    r: &AlterConfigsResource,
    name: &str,
    validate_only: bool,
    may: &impl Fn(Option<&str>) -> bool,
) -> Result<(), (i16, String)> {
    alterable(r.resource_type, name, may)?;
    let Some(topic) = topic_info(state, name).await else {
        return Err((codes::UNKNOWN_TOPIC_OR_PARTITION, format!("no topic {name:?}")));
    };
    let (mut retention_ms, mut settings) = (topic.retention_ms, topic.settings.clone());
    for c in &r.configs {
        let value = c.value.as_ref().map(|v| v.as_str());
        change(
            &mut retention_ms,
            &mut settings,
            c.name.as_str(),
            c.config_operation,
            value,
        )
        .map_err(|why| (codes::INVALID_CONFIG, why))?;
    }
    if validate_only {
        return Ok(());
    }
    apply(state, name, &topic, retention_ms, settings).await
}

/// One setting changed on the copies a request is working on. `retention.ms` and the topic's own settings can be
/// set or put back to their defaults; anything else is refused by name, so nothing is taken and ignored.
fn change(
    retention_ms: &mut Option<i64>,
    settings: &mut TopicSettings,
    name: &str,
    op: i8,
    value: Option<&str>,
) -> Result<(), String> {
    if name == "retention.ms" {
        *retention_ms = retention(op, value)?;
        return Ok(());
    }
    if openduct_storage::setting_spec(name).is_none() {
        return Err(format!(
            "{name} cannot be changed: of a topic's settings, those DescribeConfigs does not mark read-only can"
        ));
    }
    match (op, value) {
        (DELETE, _) => settings.reset(name),
        (SET, Some(v)) => settings.set(name, v),
        (SET, None) => Err(format!("{name} needs a value")),
        _ => Err(format!(
            "{name} can be set or deleted, not added to or taken from: set the whole value"
        )),
    }
}

/// What changed is written: retention where it differs, the other settings where they differ. Nothing is written
/// for a request that leaves the topic as it is.
async fn apply(
    state: &State,
    name: &str,
    topic: &TopicInfo,
    retention_ms: Option<i64>,
    settings: TopicSettings,
) -> Result<(), (i16, String)> {
    let failed = |e: openduct_storage::StoreError| (codes::of_store_error(&e), e.to_string());
    if settings != topic.settings {
        let n = name.to_owned();
        blocking(&state.store, move |s| s.set_topic_settings(&n, settings))
            .await
            .map_err(failed)?;
    }
    if retention_ms != topic.retention_ms {
        let n = name.to_owned();
        blocking(&state.store, move |s| s.set_retention(&n, retention_ms))
            .await
            .map_err(failed)?;
    }
    Ok(())
}

/// Whether this resource's settings can be changed here, by this principal: a topic's, by one allowed to.
fn alterable(
    resource_type: i8,
    name: &str,
    may: &impl Fn(Option<&str>) -> bool,
) -> Result<(), (i16, String)> {
    match resource_type {
        TOPIC if !may(Some(name)) => Err((
            crate::security::TOPIC_AUTHORIZATION_FAILED,
            "not allowed".to_owned(),
        )),
        TOPIC => Ok(()),
        BROKER if !may(None) => Err((
            crate::security::CLUSTER_AUTHORIZATION_FAILED,
            "not allowed".to_owned(),
        )),
        BROKER => Err((
            codes::INVALID_REQUEST,
            "a broker's settings are changed where it is started, not through the protocol".to_owned(),
        )),
        _ => Err((
            codes::INVALID_REQUEST,
            "a resource type whose settings are not served".to_owned(),
        )),
    }
}

// --- AlterConfigs ----------------------------------------------------------------------------------------------

/// AlterConfigs (the older API, still what `kafka-configs.sh --alter` and many clients send): a topic's settings
/// REPLACED by the ones named - any it does not name goes back to its default. `may`: as for
/// IncrementalAlterConfigs.
pub(crate) async fn alter_configs(
    state: &State,
    req: &AlterConfigsRequest,
    may: impl Fn(Option<&str>) -> bool,
) -> AlterConfigsResponse {
    let mut responses = Vec::new();
    for r in &req.resources {
        let name = r.resource_name.to_string();
        let out = WholeResourceResponse::default()
            .with_resource_type(r.resource_type)
            .with_resource_name(r.resource_name.clone());
        let outcome = replace_one(state, r, &name, req.validate_only, &may).await;
        responses.push(match outcome {
            Ok(()) => out,
            Err((code, why)) => out.with_error_code(code).with_error_message(Some(s(why))),
        });
    }
    AlterConfigsResponse::default().with_responses(responses)
}

async fn replace_one(
    state: &State,
    r: &WholeResource,
    name: &str,
    validate_only: bool,
    may: &impl Fn(Option<&str>) -> bool,
) -> Result<(), (i16, String)> {
    alterable(r.resource_type, name, may)?;
    let Some(topic) = topic_info(state, name).await else {
        return Err((codes::UNKNOWN_TOPIC_OR_PARTITION, format!("no topic {name:?}")));
    };
    let (mut retention_ms, mut settings) = (None, TopicSettings::default());
    for c in &r.configs {
        let value = c.value.as_ref().map(|v| v.as_str());
        change(&mut retention_ms, &mut settings, c.name.as_str(), SET, value)
            .map_err(|why| (codes::INVALID_CONFIG, why))?;
    }
    if validate_only {
        return Ok(());
    }
    apply(state, name, &topic, retention_ms, settings).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cluster_operations_bit_field_is_kafkas() {
        assert_eq!(cluster_operations(|_| false), 0);
        assert_eq!(cluster_operations(|op| op == Operation::Describe), 1 << 8);
        assert_eq!(
            cluster_operations(|_| true),
            8096,
            "bits 5, 7, 8, 9, 10, 11 and 12"
        );
    }
}
