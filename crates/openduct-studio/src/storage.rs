// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Storage in the studio: the folders of this machine the installation allows (`GET /api/storage/roots`, for the
//! builder's choice of folder), and the tasks a storage trigger needs beside its runner - a folder's polls queued as a
//! schedule's times are, or a Redis channel listened to on the one node that holds its lease.

use std::sync::Arc;

use hyper::StatusCode;
use openduct_flow::storage::Watch;
use openduct_flow::{Trigger, Workflow};
use serde_json::json;
use tokio::sync::watch;

use crate::State;
use crate::api::signed_in;
use crate::http::{Answer, Call, json};

/// The folders' names, and whether servers on this machine's own network may be reached.
pub(crate) async fn roots(state: &State, call: &Call) -> Answer {
    if let Err(answer) = signed_in(state, call).await {
        return answer;
    }
    json(
        StatusCode::OK,
        &json!({
            "roots": state.files_roots.names(),
            "allow_private_network": state.allow_private_network,
        }),
    )
}

/// The tasks a workflow's storage trigger needs beside its runner; none for another trigger.
pub(crate) fn tasks(
    id: &str,
    workflow: &Workflow,
    state: &State,
    effects: &Arc<dyn openduct_flow::Effects>,
    stopping: &watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Trigger::Storage(w) = &workflow.trigger else {
        return Vec::new();
    };
    match w.as_ref() {
        Watch::Files(f) => vec![tokio::spawn(crate::manager::keep_scheduling(
            id.to_owned(),
            f.every,
            Arc::clone(&state.shared),
            stopping.clone(),
        ))],
        Watch::Channel(c) => vec![tokio::spawn(openduct_flow::storage::listen(
            id.to_owned(),
            c.clone(),
            Arc::clone(&state.shared),
            Arc::clone(effects),
            stopping.clone(),
        ))],
    }
}
