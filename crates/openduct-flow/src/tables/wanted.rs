// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The data tables and lookup files a workflow uses, worked out from its steps: which a workspace must have before
//! the workflow runs, the columns its steps give each table - so a table not made yet can be offered, made with them
//! - and the empty stand-in a test run uses for a table not made yet. // test runs and needs
//!
//! A table's columns are those an insert, an upsert or an update writes (its `row` and `set`), then those a condition
//! or a sort reads; each is text unless its value says otherwise: a number, true or false, a time, or a record or a
//! list (JSON). A value is what its template gives where it is worked out: the sample a test runs on, or the run.

use openduct_expr::{Env, Value};
use serde_json::Value as Json;

use super::{Column, ColumnType, MAX_COLUMNS, MAX_TABLES, SYSTEM_COLUMNS, name_problem};
use crate::model::tables::{Filter, NewRows, TableAction, TableStep};
use crate::model::{Kind, Step, Tree};

/// The most lookup files listed.
const MAX_FILES: usize = 50;
/// How deep steps are followed, one inside another.
const MAX_DEPTH: usize = 16;

/// A table a workflow uses: its name, and the columns its steps give it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wanted {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Every table the steps use - in their branches, loops, categories and agents' tools too - each once, in the order
/// first used, with the columns all its steps give it.
pub fn tables(steps: &[Step], env: &Env<'_>) -> Vec<Wanted> {
    let mut out: Vec<Wanted> = Vec::new();
    walk(steps, 0, &mut |step| {
        // vector stores: an agent's memory kept in a data table wants that table, with its three columns.
        let (table, given) = match (&step.kind, crate::agent::stores::memory_table(step)) {
            (_, Some(memory)) => memory,
            (Kind::Table(t), None) => (t.table.clone(), columns(t, env)),
            _ => return,
        };
        let at = match out.iter().position(|w| w.name == table) {
            Some(at) => at,
            None if out.len() < MAX_TABLES => {
                out.push(Wanted {
                    name: table.clone(),
                    columns: Vec::new(),
                });
                out.len() - 1
            }
            None => return,
        };
        if let Some(wanted) = out.get_mut(at) {
            for column in given {
                add(&mut wanted.columns, column);
            }
        }
    });
    out
}

/// Every lookup file the steps read, each once, in the order first read.
pub fn files(steps: &[Step]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    walk(steps, 0, &mut |step| {
        if let Kind::Lookup { file, .. } = &step.kind
            && !out.contains(file)
            && out.len() < MAX_FILES
        {
            out.push(file.clone());
        }
    });
    out
}

/// The columns one table step gives its table: those it writes, then those it reads.
pub fn columns(step: &TableStep, env: &Env<'_>) -> Vec<Column> {
    let mut out = Vec::new();
    let written: &[(String, Tree)] = match &step.action {
        TableAction::Insert(NewRows::One(row)) | TableAction::Upsert { row, .. } => row,
        TableAction::Update { set, .. } => set,
        _ => &[],
    };
    for (name, tree) in written {
        add(
            &mut out,
            Column {
                name: name.clone(),
                kind: kind_of_tree(tree, env),
            },
        );
    }
    let filter = match &step.action {
        TableAction::Get { filter, .. }
        | TableAction::Count { filter }
        | TableAction::Update { filter, .. }
        | TableAction::Delete { filter } => Some(filter),
        _ => None,
    };
    if let Some(Filter::Tests { tests, .. }) = filter {
        for test in tests {
            let kind = test
                .value
                .as_ref()
                .and_then(|t| t.value(env).ok())
                .map_or(ColumnType::Text, |v| kind_of_value(&v));
            add(
                &mut out,
                Column {
                    name: test.column.clone(),
                    kind,
                },
            );
        }
    }
    if let TableAction::Get { sort, .. } = &step.action {
        for (name, _) in sort {
            add(
                &mut out,
                Column {
                    name: name.clone(),
                    kind: ColumnType::Text,
                },
            );
        }
    }
    out
}

/// A table's columns in a few words: `name, company, total (number)`.
pub fn said(columns: &[Column]) -> String {
    columns
        .iter()
        .map(|c| match c.kind {
            ColumnType::Text => c.name.clone(),
            other => format!("{} ({})", c.name, other.name()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A column added to `columns`: a new one at the end; one there already keeps its type, unless that is text and this
/// one's values say more. The table's own columns, names a table cannot have, and columns past the most are left out.
fn add(columns: &mut Vec<Column>, column: Column) {
    if SYSTEM_COLUMNS.contains(&column.name.as_str()) || name_problem(&column.name).is_some() {
        return;
    }
    if let Some(there) = columns.iter_mut().find(|c| c.name == column.name) {
        if there.kind == ColumnType::Text {
            there.kind = column.kind;
        }
    } else if columns.len() < MAX_COLUMNS {
        columns.push(column);
    }
}

fn kind_of_tree(tree: &Tree, env: &Env<'_>) -> ColumnType {
    match tree {
        Tree::Literal(j) => kind_of_json(j),
        Tree::Template(t) => t.value(env).map_or(ColumnType::Text, |v| kind_of_value(&v)),
        Tree::List(_) | Tree::Record(_) => ColumnType::Json,
    }
}

fn kind_of_json(j: &Json) -> ColumnType {
    match j {
        Json::Number(_) => ColumnType::Number,
        Json::Bool(_) => ColumnType::Boolean,
        Json::Array(_) | Json::Object(_) => ColumnType::Json,
        Json::Null | Json::String(_) => ColumnType::Text,
    }
}

fn kind_of_value(v: &Value) -> ColumnType {
    match v {
        Value::Number(_) | Value::BigNumber(_) => ColumnType::Number,
        Value::Bool(_) => ColumnType::Boolean,
        Value::Time(_) => ColumnType::DateTime,
        Value::Record(_) | Value::List(_) => ColumnType::Json,
        _ => ColumnType::Text,
    }
}

/// Each step of `steps`, and of their branches, loops, categories and agents' tools, handed to `each`.
fn walk(steps: &[Step], depth: usize, each: &mut dyn FnMut(&Step)) {
    if depth > MAX_DEPTH {
        return;
    }
    for step in steps {
        each(step);
        match &step.kind {
            Kind::If { then, otherwise, .. } => {
                walk(then, depth + 1, each);
                walk(otherwise, depth + 1, each);
            }
            Kind::ForEach { steps, .. } => walk(steps, depth + 1, each),
            Kind::Switch { cases, otherwise } => {
                for case in cases {
                    walk(&case.steps, depth + 1, each);
                }
                walk(otherwise, depth + 1, each);
            }
            Kind::Ai(ai) => {
                for route in ai.routes() {
                    walk(route, depth + 1, each);
                }
            }
            Kind::Agent(agent) => {
                for tool in &agent.tools {
                    let crate::agent::ToolKind::Step { step, .. } = &tool.does;
                    walk(std::slice::from_ref(step.as_ref()), depth + 1, each);
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::model::Workflow;

    fn columns_of(w: &[Wanted], name: &str) -> Vec<(String, &'static str)> {
        w.iter()
            .find(|t| t.name == name)
            .unwrap()
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.kind.name()))
            .collect()
    }

    #[test]
    fn the_tables_and_files_a_workflow_uses_come_with_the_columns_its_steps_give() {
        let w = Workflow::parse(
            r#"
name: Leads
trigger: manual
steps:
  - name: save
    table: { name: leads, action: insert, row: { name: "{{ value.name }}", score: "{{ value.score }}", hot: false, id: 1 } }
  - name: sort
    if:
      when: value.score > 5
      then:
        - name: mark
          table: { name: leads, action: update, where: { email: "{{ value.email }}" }, set: { name: "x", seen: "{{ timestamp('2026-11-27T09:00:00Z') }}", tags: [a] } }
      else:
        - name: rate
          lookup: { file: /rates.csv, where: { code: "{{ value.code }}" } }
  - name: each
    for_each:
      items: value.items
      steps:
        - name: count
          table: { name: counts, action: get, sort: -total, limit: 5 }
        - name: again
          lookup: { file: /rates.csv, where: { code: x } }
"#,
        )
        .unwrap();
        let sample =
            Value::from_json(&serde_json::json!({ "name": "Ann", "score": 7, "email": "a@example.com" }));
        let env = Env::of(&sample);
        let found = tables(&w.steps, &env);
        assert_eq!(
            found.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            ["leads", "counts"]
        );
        assert_eq!(
            columns_of(&found, "leads"),
            [
                ("name".to_owned(), "text"),
                ("score".to_owned(), "number"),
                ("hot".to_owned(), "boolean"),
                ("seen".to_owned(), "datetime"),
                ("tags".to_owned(), "json"),
                ("email".to_owned(), "text"),
            ],
            "id is the table's own; a column given twice keeps one place"
        );
        assert_eq!(columns_of(&found, "counts"), [("total".to_owned(), "text")]);
        assert_eq!(files(&w.steps), ["/rates.csv"]);
        assert_eq!(
            said(&found[0].columns[..3]),
            "name, score (number), hot (boolean)"
        );
    }

    #[test]
    fn a_table_a_category_or_an_agent_s_tool_uses_is_found_too() {
        let w = Workflow::parse(
            r#"
name: Sorted
trigger: manual
steps:
  - name: sort
    classify:
      credential: model
      text: "{{ value.text }}"
      categories:
        - name: sales
          steps:
            - name: keep
              table: { name: leads, action: insert, row: { text: "{{ value.text }}" } }
        - name: support
"#,
        )
        .unwrap();
        let found = tables(&w.steps, &Env::of(&Value::Null));
        assert_eq!(found.len(), 1);
        assert_eq!(columns_of(&found, "leads"), [("text".to_owned(), "text")]);
        // Text stays text when its value is missing: a column the sample does not give.
        let mut cols = vec![Column {
            name: "a".to_owned(),
            kind: ColumnType::Number,
        }];
        add(
            &mut cols,
            Column {
                name: "a".to_owned(),
                kind: ColumnType::Text,
            },
        );
        assert_eq!(cols[0].kind, ColumnType::Number);
    }
}
