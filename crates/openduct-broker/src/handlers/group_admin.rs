// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! ListGroups, DescribeGroups, DeleteGroups and OffsetDelete: the groups a broker coordinates, as tools such as
//! `kafka-consumer-groups` show and remove them.
//! A group is the coordinator's while it has members, and the store's while it has committed offsets; a group with
//! offsets and no members is `Empty`. In a cluster each broker answers for the groups it coordinates, and a client
//! asks every broker.

use std::collections::BTreeMap;

use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::offset_delete_response::{
    OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafka_protocol::messages::{
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse, GroupId,
    ListGroupsRequest, ListGroupsResponse, OffsetDeleteRequest, OffsetDeleteResponse,
};

use super::{State, blocking, not_coordinator, s};
use crate::codes;
use crate::groups::{GroupView, Protected};
use crate::security::Operation;

/// The only group type there is here (KIP-848's `consumer` groups are not served).
const CLASSIC: &str = "classic";
/// Kafka's answer for authorized operations not asked for.
const NOT_ASKED: i32 = i32::MIN;

/// Every group this broker answers for: the coordinator's, and the store's with only committed offsets.
async fn known(state: &State) -> Result<BTreeMap<String, GroupView>, i16> {
    let stored = blocking(&state.store, |s| s.groups())
        .await
        .map_err(|e| codes::of_store_error(&e))?;
    let mut views: BTreeMap<String, GroupView> = state
        .groups
        .views()
        .into_iter()
        .map(|v| (v.group_id.clone(), v))
        .collect();
    for g in stored {
        views.entry(g.clone()).or_insert_with(|| GroupView::empty(&g));
    }
    views.retain(|g, _| not_coordinator(state, g).is_none());
    Ok(views)
}

/// `may_see`: whether the principal may see a group (Describe on it, or on the cluster).
pub(crate) async fn list_groups(
    state: &State,
    req: &ListGroupsRequest,
    version: i16,
    may_see: impl Fn(&str) -> bool,
) -> ListGroupsResponse {
    let views = match known(state).await {
        Ok(v) => v,
        Err(code) => return ListGroupsResponse::default().with_error_code(code),
    };
    let states: Vec<String> = if version >= 4 {
        req.states_filter.iter().map(|f| f.to_ascii_lowercase()).collect()
    } else {
        Vec::new()
    };
    let typed = version < 5
        || req.types_filter.is_empty()
        || req.types_filter.iter().any(|t| t.eq_ignore_ascii_case(CLASSIC));
    let groups = views
        .values()
        .filter(|_| typed)
        .filter(|v| states.is_empty() || states.contains(&v.state.to_ascii_lowercase()))
        .filter(|v| may_see(&v.group_id))
        .map(|v| {
            ListedGroup::default()
                .with_group_id(GroupId(s(v.group_id.clone())))
                .with_protocol_type(s(v.protocol_type.clone()))
                .with_group_state(s(v.state))
                .with_group_type(s(CLASSIC))
        })
        .collect();
    ListGroupsResponse::default().with_groups(groups)
}

/// `may`: whether the principal may do an operation on a group.
pub(crate) async fn describe_groups(
    state: &State,
    req: &DescribeGroupsRequest,
    version: i16,
    may: impl Fn(&str, Operation) -> bool,
) -> DescribeGroupsResponse {
    let stored = blocking(&state.store, |s| s.groups()).await;
    let mut groups = Vec::new();
    for id in &req.groups {
        let group = id.0.as_str();
        let out = DescribedGroup::default()
            .with_group_id(id.clone())
            .with_authorized_operations(NOT_ASKED);
        if !may(group, Operation::Describe) {
            groups.push(out.with_error_code(crate::security::GROUP_AUTHORIZATION_FAILED));
            continue;
        }
        if let Some(code) = not_coordinator(state, group) {
            groups.push(out.with_error_code(code));
            continue;
        }
        let view = match (&stored, state.groups.view(group)) {
            (_, Some(view)) => view,
            (Ok(stored), None) if stored.iter().any(|g| g == group) => GroupView::empty(group),
            (Err(e), None) => {
                groups.push(out.with_error_code(codes::of_store_error(e)));
                continue;
            }
            // Unknown: from v6 an error (KIP-1043), before it the state "Dead".
            (Ok(_), None) if version >= 6 => {
                groups.push(
                    out.with_error_code(codes::GROUP_ID_NOT_FOUND)
                        .with_error_message(Some(s(format!("group {group} does not exist")))),
                );
                continue;
            }
            (Ok(_), None) => {
                groups.push(out.with_group_state(s("Dead")));
                continue;
            }
        };
        let operations = if version >= 3 && req.include_authorized_operations {
            authorized_operations(|op| may(group, op))
        } else {
            NOT_ASKED
        };
        groups.push(
            out.with_group_state(s(view.state))
                .with_protocol_type(s(view.protocol_type))
                .with_protocol_data(s(view.protocol))
                .with_authorized_operations(operations)
                .with_members(
                    view.members
                        .into_iter()
                        .map(|m| {
                            DescribedGroupMember::default()
                                .with_member_id(s(m.member_id))
                                .with_group_instance_id(m.group_instance_id.map(s))
                                .with_client_id(s(m.client_id))
                                .with_client_host(s(m.client_host))
                                .with_member_metadata(m.metadata)
                                .with_member_assignment(m.assignment)
                        })
                        .collect(),
                ),
        );
    }
    DescribeGroupsResponse::default().with_groups(groups)
}

/// Whether the store holds offsets for `group`.
async fn has_offsets(state: &State, group: &str) -> Result<bool, i16> {
    let g = group.to_owned();
    blocking(&state.store, move |s| s.committed_offsets(&g))
        .await
        .map(|o| !o.is_empty())
        .map_err(|e| codes::of_store_error(&e))
}

/// DeleteGroups: each group with no members forgotten, its committed offsets with it. `may_delete`: Delete on the
/// group.
pub(crate) async fn delete_groups(
    state: &State,
    req: &DeleteGroupsRequest,
    may_delete: impl Fn(&str) -> bool,
) -> DeleteGroupsResponse {
    let mut results = Vec::new();
    for id in &req.groups_names {
        let group = id.0.to_string();
        let code = delete_group(state, &group, &may_delete).await;
        results.push(
            DeletableGroupResult::default()
                .with_group_id(id.clone())
                .with_error_code(code),
        );
    }
    DeleteGroupsResponse::default().with_results(results)
}

async fn delete_group(state: &State, group: &str, may_delete: &impl Fn(&str) -> bool) -> i16 {
    if !may_delete(group) {
        return crate::security::GROUP_AUTHORIZATION_FAILED;
    }
    if let Some(code) = not_coordinator(state, group) {
        return code;
    }
    let held = match state.groups.remove_empty(group) {
        Ok(held) => held,
        Err(code) => return code,
    };
    match has_offsets(state, group).await {
        Ok(false) if !held => return codes::GROUP_ID_NOT_FOUND,
        Ok(_) => {}
        Err(code) => return code,
    }
    let g = group.to_owned();
    match blocking(&state.store, move |s| s.delete_offsets(&g, None)).await {
        Ok(()) => codes::NONE,
        Err(e) => codes::of_store_error(&e),
    }
}

/// OffsetDelete: a group's offsets of some partitions removed - except those of topics its consumers subscribe to.
/// `may_delete`: Delete on the group; `may_read`: Read on a topic.
pub(crate) async fn delete_offsets(
    state: &State,
    req: &OffsetDeleteRequest,
    may_delete: impl Fn(&str) -> bool,
    may_read: impl Fn(&str) -> bool,
) -> OffsetDeleteResponse {
    let group = req.group_id.0.to_string();
    let refused = |code| OffsetDeleteResponse::default().with_error_code(code);
    if !may_delete(&group) {
        return refused(crate::security::GROUP_AUTHORIZATION_FAILED);
    }
    if let Some(code) = not_coordinator(state, &group) {
        return refused(code);
    }
    let protected = state.groups.protected(&group);
    match (&protected, has_offsets(state, &group).await) {
        (_, Err(code)) => return refused(code),
        (Protected::Unknown, Ok(false)) => return refused(codes::GROUP_ID_NOT_FOUND),
        (Protected::Everything, _) => return refused(codes::NON_EMPTY_GROUP),
        _ => {}
    }
    let mut gone = Vec::new();
    let mut topics = Vec::new();
    for t in &req.topics {
        let name = t.name.0.to_string();
        let partitions = super::topic_info(state, &name).await.map_or(0, |i| i.partitions);
        let code_for = |p: i32| {
            if !may_read(&name) {
                crate::security::TOPIC_AUTHORIZATION_FAILED
            } else if p < 0 || p >= partitions {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            } else if matches!(&protected, Protected::Topics(subscribed) if subscribed.contains(&name)) {
                codes::GROUP_SUBSCRIBED_TO_TOPIC
            } else {
                codes::NONE
            }
        };
        let answered: Vec<OffsetDeleteResponsePartition> = t
            .partitions
            .iter()
            .map(|p| {
                let code = code_for(p.partition_index);
                if code == codes::NONE {
                    gone.push((name.clone(), p.partition_index));
                }
                OffsetDeleteResponsePartition::default()
                    .with_partition_index(p.partition_index)
                    .with_error_code(code)
            })
            .collect();
        topics.push(
            OffsetDeleteResponseTopic::default()
                .with_name(t.name.clone())
                .with_partitions(answered),
        );
    }
    if !gone.is_empty() {
        let g = group.clone();
        if let Err(e) = blocking(&state.store, move |s| s.delete_offsets(&g, Some(&gone))).await {
            return refused(codes::of_store_error(&e));
        }
    }
    OffsetDeleteResponse::default().with_topics(topics)
}

/// The operations allowed on a group, as Kafka's bit field (bit n for its operation code n: READ 3, DELETE 6,
/// DESCRIBE 8).
fn authorized_operations(may: impl Fn(Operation) -> bool) -> i32 {
    [
        (Operation::Read, 3),
        (Operation::Delete, 6),
        (Operation::Describe, 8),
    ]
    .into_iter()
    .filter(|(op, _)| may(*op))
    .fold(0, |bits, (_, code)| bits | (1 << code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_operations_bit_field_is_kafkas() {
        assert_eq!(authorized_operations(|_| true), (1 << 3) | (1 << 6) | (1 << 8));
        assert_eq!(authorized_operations(|op| op == Operation::Describe), 256);
        assert_eq!(authorized_operations(|_| false), 0);
    }
}
