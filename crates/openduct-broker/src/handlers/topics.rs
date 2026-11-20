// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CreateTopics, CreatePartitions and DeleteTopics.

use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, TopicName,
};
use openduct_storage::TopicConfig;

use super::{State, blocking, s, topic_info};
use crate::codes;

pub(crate) async fn create_topics(
    state: &State,
    req: &CreateTopicsRequest,
    version: i16,
) -> CreateTopicsResponse {
    let mut results = Vec::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let mut result = CreatableTopicResult::default().with_name(t.name.clone());
        let partitions = if t.num_partitions <= 0 {
            state.config.default_partitions
        } else {
            t.num_partitions
        };
        let mut retention_ms = None;
        let mut settings = openduct_storage::TopicSettings::default();
        let mut config_error = None;
        for c in &t.configs {
            let value = c.value.as_ref().map(|v| v.to_string()).unwrap_or_default();
            match &*c.name {
                "retention.ms" => match value.parse::<i64>() {
                    Ok(v) if v < 0 => retention_ms = None,
                    Ok(v) => retention_ms = Some(v),
                    Err(_) => config_error = Some(format!("retention.ms must be a number, not {value:?}")),
                },
                // The topic's own settings (openduct_storage settings.rs), each checked as it is taken.
                name if openduct_storage::setting_spec(name).is_some() => {
                    if let Err(why) = settings.set(name, &value) {
                        config_error = Some(why);
                    }
                }
                // Other settings (segment sizes, replication tuning) are the broker's to decide; accepted.
                _ => {}
            }
        }
        let outcome = if let Some(why) = config_error {
            Err((codes::INVALID_CONFIG, why))
        } else if req.validate_only {
            let exists = topic_info(state, &name).await.is_some();
            openduct_storage::validate_topic_name(&name)
                .map_err(|e| (codes::of_store_error(&e), e.to_string()))
                .and_then(|()| {
                    if exists {
                        Err((
                            codes::TOPIC_ALREADY_EXISTS,
                            format!("topic {name:?} already exists"),
                        ))
                    } else {
                        Ok(())
                    }
                })
        } else {
            let n = name.clone();
            blocking(&state.store, move |s| {
                s.create_topic(
                    &n,
                    TopicConfig {
                        partitions,
                        retention_ms,
                    },
                )?;
                // Its other settings, where it was given any. A store that cannot keep them refuses, and the
                // topic is not left behind without the limits it was asked for.
                if settings.is_empty() {
                    return Ok(());
                }
                s.set_topic_settings(&n, settings).inspect_err(|_| {
                    let _ = s.delete_topic(&n);
                })
            })
            .await
            .map_err(|e| (codes::of_store_error(&e), e.to_string()))
        };
        match outcome {
            Ok(()) => {
                if version >= 7 {
                    result.topic_id = topic_info(state, &name).await.map(|t| t.id).unwrap_or_default();
                }
                if version >= 5 {
                    result.num_partitions = partitions;
                    // Replication is the store's business: whatever factor is asked, durability is the store's.
                    result.replication_factor = if t.replication_factor > 0 {
                        t.replication_factor
                    } else {
                        1
                    };
                }
            }
            Err((code, message)) => {
                result.error_code = code;
                if version >= 1 {
                    result.error_message = Some(s(message));
                }
            }
        }
        results.push(result);
    }
    CreateTopicsResponse::default().with_topics(results)
}

pub(crate) async fn delete_topics(
    state: &State,
    req: &DeleteTopicsRequest,
    version: i16,
) -> DeleteTopicsResponse {
    // Up to v5 topics are named in `topic_names`; from v6 in `topics`, by name or by topic id.
    let asked: Vec<(Option<TopicName>, uuid::Uuid)> = if version >= 6 {
        req.topics.iter().map(|t| (t.name.clone(), t.topic_id)).collect()
    } else {
        req.topic_names
            .iter()
            .map(|n| (Some(n.clone()), uuid::Uuid::nil()))
            .collect()
    };
    let mut responses = Vec::with_capacity(asked.len());
    for (name, topic_id) in asked {
        // A topic named by its id (v6+) is found by it; one no topic has is unknown.
        let named = match &name {
            Some(n) => Some(n.0.to_string()),
            None => blocking(&state.store, move |store| {
                Ok(store
                    .topics()
                    .into_iter()
                    .find(|t| t.id == topic_id)
                    .map(|t| t.name))
            })
            .await
            .ok()
            .flatten(),
        };
        let outcome = match named {
            Some(n) => blocking(&state.store, move |store| store.delete_topic(&n))
                .await
                .map_err(|e| (codes::of_store_error(&e), e.to_string())),
            None => Err((codes::UNKNOWN_TOPIC_ID, format!("no topic has the id {topic_id}"))),
        };
        let mut result = DeletableTopicResult::default();
        // Always named up to v5 (the request names every topic); from v6 as asked, by name or by id.
        result.name = name;
        if version >= 6 {
            result.topic_id = topic_id;
        }
        if let Err((code, message)) = outcome {
            result.error_code = code;
            if version >= 5 {
                result.error_message = Some(s(message));
            }
        }
        responses.push(result);
    }
    DeleteTopicsResponse::default().with_responses(responses)
}

/// CreatePartitions: partitions added to topics, each to the count asked (`may_alter`: Alter on the topic). A broker on
/// its own holds every partition itself, so an assignment may name only it.
pub(crate) async fn create_partitions(
    state: &State,
    req: &CreatePartitionsRequest,
    may_alter: impl Fn(&str) -> bool,
) -> CreatePartitionsResponse {
    let mut results = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let result = CreatePartitionsTopicResult::default().with_name(t.name.clone());
        let outcome = if !seen.insert(name.clone()) {
            Err((codes::INVALID_REQUEST, format!("{name} is named twice")))
        } else if !may_alter(&name) {
            Err((
                crate::security::TOPIC_AUTHORIZATION_FAILED,
                "not allowed".to_owned(),
            ))
        } else {
            grow(state, &name, t, req.validate_only).await
        };
        results.push(match outcome {
            Ok(()) => result,
            Err((code, why)) => result.with_error_code(code).with_error_message(Some(s(why))),
        });
    }
    CreatePartitionsResponse::default().with_results(results)
}

async fn grow(
    state: &State,
    name: &str,
    t: &CreatePartitionsTopic,
    validate_only: bool,
) -> Result<(), (i16, String)> {
    let refused = |e: openduct_storage::StoreError| (codes::of_store_error(&e), e.to_string());
    let has = topic_info(state, name)
        .await
        .ok_or_else(|| (codes::UNKNOWN_TOPIC_OR_PARTITION, format!("no topic {name:?}")))?
        .partitions;
    openduct_storage::validate_growth(name, has, t.count).map_err(refused)?;
    // No placements, or an empty list of them (what a client that names none may send): placed as usual.
    let assignment: Option<Vec<Vec<i32>>> = t.assignments.as_ref().filter(|a| !a.is_empty()).map(|a| {
        a.iter()
            .map(|p| p.broker_ids.iter().map(|b| b.0).collect())
            .collect()
    });
    if let Some(given) = &assignment {
        let adding = usize::try_from(t.count - has).unwrap_or(0);
        if given.len() != adding {
            return Err((
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "{adding} partitions are added but {} placements were given",
                    given.len()
                ),
            ));
        }
        if state.store.cluster().is_none() && given.iter().any(|b| b.as_slice() != [state.config.node_id]) {
            return Err((
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "this broker is on its own: every partition is on broker {}",
                    state.config.node_id
                ),
            ));
        }
    }
    if validate_only {
        return Ok(());
    }
    let (n, total) = (name.to_owned(), t.count);
    blocking(&state.store, move |s| s.add_partitions(&n, total, assignment))
        .await
        .map_err(refused)
}
