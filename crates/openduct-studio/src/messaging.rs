// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The messaging triggers in the studio: the tasks they need beside their runner. A mailbox's polls are queued as a
//! schedule's times are (and, with `idle: true`, the folder is listened to between them on the one node that holds a
//! lease); an app's webhook whose registration runs out (Microsoft Graph's subscriptions) has its renewals queued
//! the same way, each made once in the cluster by the runner.

use std::sync::Arc;

use openduct_flow::{Trigger, Workflow};
use tokio::sync::watch;

use crate::State;

/// The tasks a workflow's messaging trigger needs beside its runner; none for another trigger.
pub(crate) fn tasks(
    id: &str,
    workflow: &Workflow,
    state: &State,
    effects: &Arc<dyn openduct_flow::Effects>,
    stopping: &watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let schedule = |every| {
        tokio::spawn(crate::manager::keep_scheduling(
            id.to_owned(),
            every,
            Arc::clone(&state.shared),
            stopping.clone(),
        ))
    };
    match &workflow.trigger {
        Trigger::Mailbox(w) => {
            let mut tasks = vec![schedule(w.every)];
            if w.idle {
                tasks.push(tokio::spawn(openduct_flow::mailbox::idle(
                    id.to_owned(),
                    w.as_ref().clone(),
                    Arc::clone(&state.shared),
                    Arc::clone(effects),
                    stopping.clone(),
                )));
            }
            tasks
        }
        trigger => openduct_flow::app_files::hook::every_renewal(trigger)
            .map(schedule)
            .into_iter()
            .collect(),
    }
}
