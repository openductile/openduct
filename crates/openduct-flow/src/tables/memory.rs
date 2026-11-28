// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Tables kept in this process's memory, gone when it ends: for tests, and for trying a workflow out on a machine.
//! Each change is planned and made under one lock, so it is a turn of its own as the studio's are.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openduct_expr::{Record, Value};

use super::{
    Column, Pending, Plan, StatePlan, TableData, Tables, check_changes, check_state, columns_problem,
    name_problem,
};

#[derive(Debug, Default)]
pub struct MemoryTables {
    held: Mutex<Held>,
}

#[derive(Debug, Default)]
struct Held {
    tables: HashMap<String, Arc<TableData>>,
    states: HashMap<String, Arc<Record>>,
}

impl MemoryTables {
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a table: `name`, with `columns`.
    pub fn create(&self, name: &str, columns: Vec<Column>) -> Result<(), String> {
        if let Some(why) = name_problem(name).or_else(|| columns_problem(&columns)) {
            return Err(why);
        }
        let mut held = self.held.lock().map_err(|_| "the tables are unavailable")?;
        if held.tables.contains_key(name) {
            return Err(format!("there is a table called {name} already"));
        }
        held.tables.insert(
            name.to_owned(),
            Arc::new(TableData {
                name: name.to_owned(),
                columns,
                next_id: 1,
                ..TableData::default()
            }),
        );
        Ok(())
    }

    fn table(&self, name: &str) -> Result<Arc<TableData>, String> {
        let held = self.held.lock().map_err(|_| "the tables are unavailable")?;
        held.tables
            .get(name)
            .cloned()
            .ok_or_else(|| format!("there is no table {name}: make it under Tables"))
    }
}

impl Tables for MemoryTables {
    fn read<'a>(&'a self, name: &'a str) -> Pending<'a, Arc<TableData>> {
        Box::pin(async move { self.table(name) })
    }

    // test runs
    fn has<'a>(&'a self, name: &'a str) -> Pending<'a, bool> {
        Box::pin(async move {
            let held = self.held.lock().map_err(|_| "the tables are unavailable")?;
            Ok(held.tables.contains_key(name))
        })
    }

    fn change<'a>(&'a self, name: &'a str, plan: Plan<'a>) -> Pending<'a, Value> {
        Box::pin(async move {
            let mut held = self.held.lock().map_err(|_| "the tables are unavailable")?;
            let table = held
                .tables
                .get_mut(name)
                .ok_or_else(|| format!("there is no table {name}: make it under Tables"))?;
            let (changes, result) = plan(table)?;
            check_changes(table, &changes)?;
            let data = Arc::make_mut(table);
            for id in &changes.delete {
                data.rows.remove(id);
            }
            for row in changes.put {
                data.rows.insert(row.id, row);
            }
            data.next_id = data.next_id.max(changes.next_id);
            Ok(result)
        })
    }

    fn state<'a>(&'a self, workflow: &'a str) -> Pending<'a, Arc<Record>> {
        Box::pin(async move {
            let held = self.held.lock().map_err(|_| "the state is unavailable")?;
            Ok(held.states.get(workflow).cloned().unwrap_or_default())
        })
    }

    fn change_state<'a>(&'a self, workflow: &'a str, plan: StatePlan<'a>) -> Pending<'a, Value> {
        Box::pin(async move {
            let mut held = self.held.lock().map_err(|_| "the state is unavailable")?;
            let current = held.states.get(workflow).cloned().unwrap_or_default();
            let (next, result) = plan(&current)?;
            if let Some(next) = next {
                check_state(&next)?;
                held.states.insert(workflow.to_owned(), Arc::new(next));
            }
            Ok(result)
        })
    }
}
