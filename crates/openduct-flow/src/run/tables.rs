// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The work of the steps that keep data between runs: a data table's rows (see [`crate::tables`]) and the workflow's
//! own state - for a `table` step, a `state` step, and a script step given the state.
//!
//! A change is worked out while the table's (or the state's) turn is held, from the rows as they are then: an
//! update's values are worked out for each row with the row as it is (`row`), so `count: "{{ row.count + 1 }}"` from
//! two runs at once counts two. A test run or a preview reads the tables and works a change out, and writes nothing:
//! its result says what it would have written.

use std::cmp::Ordering;
use std::sync::Arc;

use openduct_expr::{Compiled, Env, Record, Value};
use rust_decimal::Decimal;
use serde_json::Value as Json;

use super::{Run, ScriptRequest, tree};
use crate::model::Tree;
use crate::model::tables::{Filter, NewRows, Op, StateAction, StateKeys, TableAction, TableStep, Test};
use crate::tables::{
    Access, Changes, ColumnType, MAX_CHANGED, Row, StatePlan, TableData, check_changes, check_state, coerce,
    now_ms, row_values,
};

/// How often a script given the state runs, at most, when other runs keep changing the same values meanwhile.
const SCRIPT_TRIES: u32 = 10;

async fn read(access: Access<'_>, name: &str) -> Result<Arc<TableData>, String> {
    match access {
        Access::None => Ok(Arc::new(TableData::stand_in(name))),
        Access::Read(t) | Access::Write(t) => t.read(name).await,
    }
}

async fn read_state(access: Access<'_>, workflow: &str) -> Result<Arc<Record>, String> {
    match access {
        Access::None => Ok(Arc::default()),
        Access::Read(t) | Access::Write(t) => t.state(workflow).await,
    }
}

/// A result marked as a test run's or a preview's: worked out, not written.
fn not_written(result: Value) -> Value {
    match result {
        Value::Record(mut r) => {
            Arc::make_mut(&mut r).insert("test".to_owned(), Value::text("not written"));
            Value::Record(r)
        }
        other => other,
    }
}

fn number(n: usize) -> Value {
    Value::number(Decimal::from(n))
}

/// A `table` step's result, the run's tables reached through `access`. // test runs: or a stand-in's
pub(super) async fn table_in(access: Access<'_>, step: &TableStep, env: &Env<'_>) -> Result<Value, String> {
    match &step.action {
        TableAction::Get {
            filter,
            sort,
            limit,
            offset,
        } => {
            let data = read(access, &step.table).await?;
            let mut found = matching(filter, &data, env)?;
            sort_rows(&mut found, sort, &data)?;
            Ok(Value::list(
                found
                    .into_iter()
                    .skip(*offset)
                    .take(*limit)
                    .map(|(_, shown)| shown)
                    .collect(),
            ))
        }
        TableAction::Count { filter } => {
            let data = read(access, &step.table).await?;
            let n = matching(filter, &data, env)?.len();
            Ok(Value::record(
                [("count".to_owned(), number(n))].into_iter().collect(),
            ))
        }
        action => {
            // What a run gives is worked out once, before the turn; what a row gives, for each row, in it.
            let given = given_rows(action, env)?;
            let planned: crate::tables::Plan<'_> =
                Box::new(move |data: &TableData| plan(action, given, data, env));
            match access {
                Access::Write(t) => t.change(&step.table, planned).await,
                Access::Read(_) | Access::None => {
                    let data = read(access, &step.table).await?;
                    let (changes, result) = planned(&data)?;
                    check_changes(&data, &changes)?;
                    Ok(not_written(result))
                }
            }
        }
    }
}

/// The rows an insert adds, or the row an upsert writes, as the run gives them.
fn given_rows(action: &TableAction, env: &Env<'_>) -> Result<Vec<Record>, String> {
    let record = |fields: &[(String, Tree)]| -> Result<Record, String> {
        fields
            .iter()
            .map(|(k, t)| Ok((k.clone(), tree(t, env).map_err(|e| format!("{k}: {e}"))?)))
            .collect()
    };
    Ok(match action {
        TableAction::Insert(NewRows::One(fields)) | TableAction::Upsert { row: fields, .. } => {
            vec![record(fields)?]
        }
        TableAction::Insert(NewRows::Many(rows)) => {
            match rows.evaluate(env).map_err(|e| format!("rows: {e}"))? {
                Value::List(items) => {
                    if items.len() > MAX_CHANGED {
                        return Err(format!(
                            "rows: a list of {} records: at most {MAX_CHANGED}",
                            items.len()
                        ));
                    }
                    items
                        .iter()
                        .enumerate()
                        .map(|(i, item)| match item {
                            Value::Record(r) => Ok(r.as_ref().clone()),
                            other => Err(format!("rows[{i}] is {}, not a record", other.describe())),
                        })
                        .collect::<Result<_, _>>()?
                }
                Value::Null => Vec::new(),
                other => return Err(format!("rows is {}, not a list of records", other.describe())),
            }
        }
        _ => Vec::new(),
    })
}

/// A write's changes and result, from the table as it is.
fn plan(
    action: &TableAction,
    given: Vec<Record>,
    data: &TableData,
    env: &Env<'_>,
) -> Result<(Changes, Value), String> {
    let now = now_ms();
    let mut changes = Changes {
        next_id: data.next_id.max(1),
        ..Changes::default()
    };
    let add = |changes: &mut Changes, values: Record| {
        let id = changes.next_id;
        changes.next_id += 1;
        changes.put.push(Row {
            id,
            values: Arc::new(values),
            created: now,
            updated: now,
        });
    };
    let shown = |changes: &Changes| Value::list(changes.put.iter().map(|r| r.shown(&data.columns)).collect());
    let mut out = Record::new();
    match action {
        TableAction::Insert(_) => {
            for (i, record) in given.iter().enumerate() {
                let values = row_values(data, record).map_err(|e| format!("row {}: {e}", i + 1))?;
                add(&mut changes, values);
            }
        }
        TableAction::Update { filter, set } => {
            for (row, row_shown) in matching(filter, data, env)? {
                let names = with(env, "row", row_shown);
                let here = Env {
                    names: &names,
                    ..*env
                };
                let mut values = row.values.as_ref().clone();
                for (column, t) in set {
                    let kind = data.column(column)?;
                    let value = tree(t, &here).map_err(|e| format!("{column}: {e}"))?;
                    values.insert(
                        column.clone(),
                        coerce(kind, &value).map_err(|e| format!("{column}: {e}"))?,
                    );
                }
                changes.put.push(Row {
                    values: Arc::new(values),
                    updated: now,
                    ..row.clone()
                });
                if changes.put.len() > MAX_CHANGED {
                    return Err(format!(
                        "the condition matches more than {MAX_CHANGED} rows: a step changes at most that many"
                    ));
                }
            }
        }
        TableAction::Upsert { key, .. } => {
            let values = row_values(data, given.first().unwrap_or(&Record::new()))?;
            let mut updated = 0;
            for row in data.rows.values() {
                let same = key.iter().all(|k| {
                    let wanted = values.get(k).unwrap_or(&Value::Null);
                    equal(row.values.get(k).unwrap_or(&Value::Null), wanted)
                });
                if same {
                    let mut merged = row.values.as_ref().clone();
                    merged.extend(values.iter().map(|(k, v)| (k.clone(), v.clone())));
                    changes.put.push(Row {
                        values: Arc::new(merged),
                        updated: now,
                        ..row.clone()
                    });
                    updated += 1;
                }
            }
            if updated == 0 {
                add(&mut changes, values);
            }
            out.insert("inserted".to_owned(), number(usize::from(updated == 0)));
            out.insert("updated".to_owned(), number(updated));
        }
        TableAction::Delete { filter } => {
            let gone = matching(filter, data, env)?;
            if gone.len() > MAX_CHANGED {
                return Err(format!(
                    "the condition matches {} rows: a step deletes at most {MAX_CHANGED}",
                    gone.len()
                ));
            }
            out.insert("count".to_owned(), number(gone.len()));
            changes.delete = gone.iter().map(|(r, _)| r.id).collect();
            out.insert(
                "rows".to_owned(),
                Value::list(gone.into_iter().map(|(_, s)| s).collect()),
            );
            return Ok((changes, Value::record(out)));
        }
        TableAction::Get { .. } | TableAction::Count { .. } => {}
    }
    let mut result = Record::new();
    result.insert("count".to_owned(), number(changes.put.len()));
    result.extend(out);
    result.insert("rows".to_owned(), shown(&changes));
    Ok((changes, Value::record(result)))
}

/// The env's names with one more.
fn with(env: &Env<'_>, name: &str, value: Value) -> Vec<(String, Value)> {
    let mut names = env.names.to_vec();
    names.push((name.to_owned(), value));
    names
}

/// A comparison ready to be made on each row: its column, what it does, its column's type, its value.
type Ready = (String, Op, ColumnType, Value);

/// The rows a filter means, each beside its shown form (`row` to an expression), in id order.
fn matching<'d>(
    filter: &Filter,
    data: &'d TableData,
    env: &Env<'_>,
) -> Result<Vec<(&'d Row, Value)>, String> {
    let tests: Vec<Ready> = match filter {
        Filter::Tests { tests, .. } => tests
            .iter()
            .map(|t| ready(t, data, env))
            .collect::<Result<_, _>>()?,
        _ => Vec::new(),
    };
    let mut out = Vec::new();
    for row in data.rows.values() {
        let shown = row.shown(&data.columns);
        let keep = match filter {
            Filter::All => true,
            Filter::Expression(e) => holds(e, env, &shown)?,
            Filter::Tests { any, .. } => {
                let fields = match &shown {
                    Value::Record(r) => Some(r),
                    _ => None,
                };
                let cell = |c: &str| fields.and_then(|r| r.get(c)).cloned().unwrap_or(Value::Null);
                let mut results = tests.iter().map(|(c, op, _, v)| compare(*op, &cell(c), v));
                if *any {
                    results.any(|r| r)
                } else {
                    results.all(|r| r)
                }
            }
        };
        if keep {
            out.push((row, shown));
        }
    }
    Ok(out)
}

fn holds(e: &Compiled, env: &Env<'_>, row: &Value) -> Result<bool, String> {
    let names = with(env, "row", row.clone());
    let here = Env {
        names: &names,
        ..*env
    };
    Ok(matches!(
        e.evaluate(&here).map_err(|e| format!("where: {e}"))?,
        Value::Bool(true)
    ))
}

/// A comparison's value worked out, and read as its column's type (text, for the comparisons of text).
fn ready(t: &Test, data: &TableData, env: &Env<'_>) -> Result<Ready, String> {
    let kind = data.readable(&t.column).map_err(|e| format!("where: {e}"))?;
    let value = match &t.value {
        None => Value::Null,
        Some(template) => {
            let given = template
                .value(env)
                .map_err(|e| format!("where {}: {e}", t.column))?;
            if matches!(t.op, Op::Contains | Op::StartsWith | Op::EndsWith) {
                Value::text(crate::template::as_text(&given).to_lowercase().as_str())
            } else {
                coerce(kind, &given).map_err(|e| format!("where {}: {e}", t.column))?
            }
        }
    };
    Ok((t.column.clone(), t.op, kind, value))
}

fn empty(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Text(t) => t.trim().is_empty(),
        _ => false,
    }
}

fn compare(op: Op, cell: &Value, wanted: &Value) -> bool {
    let lower = || crate::template::as_text(cell).to_lowercase();
    let wanted_text = || crate::template::as_text(wanted);
    match op {
        Op::Empty => empty(cell),
        Op::NotEmpty => !empty(cell),
        Op::Equal => equal(cell, wanted),
        Op::NotEqual => !equal(cell, wanted),
        Op::Contains => lower().contains(&wanted_text()),
        Op::StartsWith => lower().starts_with(&wanted_text()),
        Op::EndsWith => lower().ends_with(&wanted_text()),
        Op::Less | Op::LessOrEqual | Op::Greater | Op::GreaterOrEqual => {
            let Some(o) = ordered(cell, wanted) else {
                return false;
            };
            match op {
                Op::Less => o == Ordering::Less,
                Op::LessOrEqual => o != Ordering::Greater,
                Op::Greater => o == Ordering::Greater,
                _ => o != Ordering::Less,
            }
        }
    }
}

/// Whether two values are the same: numbers by size, times by when, anything else by its JSON.
fn equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y,
        (Value::Time(x), Value::Time(y)) => x == y,
        (Value::Text(x), Value::Text(y)) => x == y,
        _ => a.to_json() == b.to_json(),
    }
}

/// How two values of one kind order; none for two of different kinds, or for nothing.
fn ordered(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => Some(x.cmp(y)),
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Time(x), Value::Time(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Values in the order people expect: nothing first, then by kind's own order; mixed kinds by their JSON.
fn order(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => ordered(a, b).unwrap_or_else(|| a.to_json().cmp(&b.to_json())),
    }
}

fn sort_rows(rows: &mut [(&Row, Value)], sort: &[(String, bool)], data: &TableData) -> Result<(), String> {
    for (column, _) in sort {
        data.readable(column).map_err(|e| format!("sort: {e}"))?;
    }
    let cell = |v: &Value, c: &str| match v {
        Value::Record(r) => r.get(c).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    };
    rows.sort_by(|(_, a), (_, b)| {
        for (column, descending) in sort {
            let o = order(&cell(a, column), &cell(b, column));
            let o = if *descending { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

/// A `state` step's result: the values asked for, or the state after the change.
pub(super) async fn state(run: &Run<'_>, action: &StateAction, env: &Env<'_>) -> Result<Value, String> {
    let access = run.effects.tables();
    let workflow = run.workflow;
    if let StateAction::Get(keys) = action {
        let state = read_state(access, workflow).await?;
        return Ok(match keys {
            StateKeys::All => Value::record(state.as_ref().clone()),
            StateKeys::One(k) => state.get(k).cloned().unwrap_or(Value::Null),
            StateKeys::Some(ks) => Value::record(
                ks.iter()
                    .map(|k| (k.clone(), state.get(k).cloned().unwrap_or(Value::Null)))
                    .collect(),
            ),
        });
    }
    let plan: StatePlan<'_> = Box::new(move |current: &Record| {
        let mut next = current.clone();
        match action {
            StateAction::Set(fields) => {
                let names = with(env, "state", Value::record(current.clone()));
                let here = Env {
                    names: &names,
                    ..*env
                };
                for (k, t) in fields {
                    // A value set to null is forgotten.
                    match tree(t, &here).map_err(|e| format!("{k}: {e}"))? {
                        Value::Null => next.shift_remove(k),
                        value => next.insert(k.clone(), value),
                    };
                }
            }
            StateAction::Delete(keys) => keys.iter().for_each(|k| {
                next.shift_remove(k);
            }),
            StateAction::Clear => next.clear(),
            StateAction::Get(_) => {}
        }
        check_state(&next)?;
        let shown = Value::record(next.clone());
        Ok((Some(next), shown))
    });
    match access {
        Access::Write(t) => t.change_state(workflow, plan).await,
        Access::Read(_) | Access::None => {
            let current = read_state(access, workflow).await?;
            Ok(plan(&current)?.1)
        }
    }
}

/// A script step given the workflow's state: run with it as `state`, and what it changed there kept - unless another
/// run changed one of the same values meanwhile; then the script (which only computes) runs again on the new state.
pub(super) async fn script(
    run: &Run<'_>,
    language: &'static str,
    code: &str,
    time: std::time::Duration,
    env: &Env<'_>,
) -> Result<(Value, Vec<String>), String> {
    let as_json = |v: &Value| serde_json::from_str::<Json>(&v.to_json()).unwrap_or(Json::Null);
    let access = run.effects.tables();
    for _ in 0..SCRIPT_TRIES {
        let before = read_state(access, run.workflow).await?;
        let before_json = as_json(&Value::record(before.as_ref().clone()));
        let mut given = serde_json::Map::new();
        given.insert("value".to_owned(), as_json(env.value));
        for (name, value) in env.names {
            given.insert(name.clone(), as_json(value));
        }
        given.insert("state".to_owned(), before_json.clone());
        let answer = run
            .effects
            .script(ScriptRequest {
                language,
                code: code.to_owned(),
                names: given,
                time,
            })
            .await?;
        let Value::Record(fields) = answer else {
            return Err("the script gave no answer".to_owned());
        };
        let mut logs: Vec<String> = match fields.get("logs") {
            Some(Value::List(lines)) => lines.iter().map(crate::template::as_text).collect(),
            _ => Vec::new(),
        };
        let result = fields.get("result").cloned().unwrap_or(Value::Null);
        let after = match fields.get("state") {
            None | Some(Value::Null) => return Ok((result, logs)),
            Some(Value::Record(r)) => r.as_ref().clone(),
            Some(other) => {
                return Err(format!(
                    "state is the workflow's values by name: the script left {} in it",
                    other.describe()
                ));
            }
        };
        if as_json(&Value::record(after.clone())) == before_json {
            return Ok((result, logs));
        }
        check_state(&after)?;
        let Access::Write(tables) = access else {
            logs.push(format!(
                "state not kept (a test run): {}",
                Value::record(after).to_json()
            ));
            return Ok((result, logs));
        };
        // What the script changed: each value it set, or took away (`None`).
        let text = |v: Option<&Value>| v.map(Value::to_json);
        let changed: Vec<(String, Option<Value>)> = before
            .keys()
            .chain(after.keys())
            .filter(|k| text(before.get(*k)) != text(after.get(*k)))
            .map(|k| (k.clone(), after.get(k).cloned()))
            .collect();
        let was = Arc::clone(&before);
        let plan: StatePlan<'_> = Box::new(move |current: &Record| {
            // Another run changed one of the same values meanwhile: the script runs again on what it left.
            if changed
                .iter()
                .any(|(k, _)| text(current.get(k)) != text(was.get(k)))
            {
                return Ok((None, Value::Bool(false)));
            }
            let mut next = current.clone();
            for (k, v) in changed {
                match v {
                    Some(v) => next.insert(k, v),
                    None => next.shift_remove(&k),
                };
            }
            check_state(&next)?;
            Ok((Some(next), Value::Bool(true)))
        });
        if matches!(tables.change_state(run.workflow, plan).await?, Value::Bool(true)) {
            return Ok((result, logs));
        }
        tracing::debug!(
            workflow = run.workflow,
            "the state changed while a script ran: running it again"
        );
        // A short wait, a little different for each run, so runs that keep meeting stop meeting.
        let jitter = u64::try_from(now_ms().rem_euclid(40)).unwrap_or(0);
        tokio::time::sleep(std::time::Duration::from_millis(5 + jitter)).await;
    }
    Err(format!(
        "the workflow's state kept changing while the script ran (tried {SCRIPT_TRIES} times): other runs change it \
         at the same time"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparisons_read_values_as_people_do() {
        let n = |x: i64| Value::number(Decimal::from(x));
        assert!(compare(Op::Greater, &n(5), &n(3)) && !compare(Op::Less, &n(5), &n(3)));
        assert!(compare(Op::LessOrEqual, &n(3), &n(3)) && compare(Op::GreaterOrEqual, &n(3), &n(3)));
        assert!(
            !compare(Op::Greater, &Value::Null, &n(3)),
            "nothing is not greater"
        );
        assert!(
            !compare(Op::Greater, &Value::text("5"), &n(3)),
            "kinds that differ do not order"
        );
        assert!(compare(
            Op::Contains,
            &Value::text("Ada Lovelace"),
            &Value::text("love")
        ));
        assert!(compare(Op::StartsWith, &Value::text("Ada"), &Value::text("ad")));
        assert!(compare(
            Op::EndsWith,
            &Value::text("x@Example.com"),
            &Value::text("example.com")
        ));
        assert!(
            compare(Op::Empty, &Value::text("  "), &Value::Null)
                && compare(Op::Empty, &Value::Null, &Value::Null)
        );
        assert!(compare(Op::NotEmpty, &n(0), &Value::Null));
        assert!(compare(
            Op::Equal,
            &Value::number(Decimal::new(250, 2)),
            &Value::number(Decimal::new(25, 1))
        ));
        assert!(compare(Op::NotEqual, &Value::text("a"), &Value::text("A")));
        assert_eq!(order(&Value::Null, &n(1)), Ordering::Less);
        assert_eq!(order(&n(2), &n(10)), Ordering::Less);
    }
}
