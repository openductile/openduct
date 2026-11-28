// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Data tables: a workspace's own tables, with typed columns, that its runs read and write (the `table` step) - and
//! each workflow's state (the `state` step, and scripts given `state`). Kept in the broker's log like everything the
//! studio keeps ([`store`]), so every node of a cluster serves the same tables and two runs on two nodes change a
//! row one after the other. The Tables page makes them, changes their columns, shows, edits, imports and exports
//! their rows ([`api`]).

use std::sync::Arc;

use openduct_expr::{Record, Value};
use openduct_flow::tables::{Plan, StatePlan, TableData, Tables};

pub(crate) mod api;
mod csv;
pub(crate) mod store;

pub(crate) use store::Store;

type Pending<'a, T> = std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// A workspace's tables and its workflows' state, as its runs reach them: its own, and no other workspace's.
#[derive(Debug)]
pub(crate) struct WorkspaceTables {
    pub(crate) store: Arc<Store>,
    pub(crate) workspace: String,
}

impl Tables for WorkspaceTables {
    fn read<'a>(&'a self, name: &'a str) -> Pending<'a, Arc<TableData>> {
        Box::pin(self.store.table(&self.workspace, name))
    }

    fn change<'a>(&'a self, name: &'a str, plan: Plan<'a>) -> Pending<'a, Value> {
        Box::pin(self.store.change_table(&self.workspace, name, plan))
    }

    fn state<'a>(&'a self, workflow: &'a str) -> Pending<'a, Arc<Record>> {
        Box::pin(self.store.state(&self.workspace, workflow))
    }

    fn change_state<'a>(&'a self, workflow: &'a str, plan: StatePlan<'a>) -> Pending<'a, Value> {
        Box::pin(self.store.change_state(&self.workspace, workflow, plan))
    }

    // test runs: only a table not there, not one the store cannot reach, is a test run's stand-in.
    fn has<'a>(&'a self, name: &'a str) -> Pending<'a, bool> {
        Box::pin(async move {
            let found = self.store.list(&self.workspace).await?;
            Ok(found.iter().any(|(d, _, _)| d.name == name))
        })
    }
}
