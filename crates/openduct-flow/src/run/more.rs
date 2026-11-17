// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The work of the steps that shape a run's data: list tools, group, merge, date and time, and a webhook's answer.
//! Each is worked out from the run's names alone, reaching nothing outside.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use openduct_expr::{Compiled, Env, Record, Value};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::json;

use crate::model::{Aggregate, DateTimeOps, GroupOps, ListOps, MergeMode};

/// The most items a list step keeps, or a merge makes.
const MAX: usize = crate::run::MAX_ITEMS;

fn list_of(value: Value, what: &str) -> Result<Vec<Value>, String> {
    match value {
        Value::List(items) => Ok(items.as_ref().clone()),
        Value::Null => Ok(Vec::new()),
        other => Err(format!("{what}: a list, not {}", kind_of(&other))),
    }
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "nothing",
        Value::Bool(_) => "true or false",
        Value::Number(_) | Value::BigNumber(_) => "a number",
        Value::Text(_) => "text",
        Value::List(_) => "a list",
        Value::Record(_) => "a record",
        Value::Time(_) => "a time",
    }
}

/// `expr` worked out for one item of a list, as `item` (and its place as `index`).
fn for_item(expr: &Compiled, env: &Env<'_>, item: &Value, index: usize) -> Result<Value, String> {
    let mut names = env.names.to_vec();
    names.push(("item".to_owned(), item.clone()));
    names.push(("index".to_owned(), Value::number(Decimal::from(index))));
    expr.evaluate(&Env {
        value: env.value,
        message: env.message,
        names: &names,
    })
}

/// Values in the order people expect: nothing first, then false before true, numbers by size, times by when,
/// text by its letters; anything else by its JSON.
fn order(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => a.to_json().cmp(&b.to_json()),
    }
}

pub(super) fn list(ops: &ListOps, env: &Env<'_>) -> Result<Value, String> {
    let mut items = list_of(ops.items.evaluate(env)?, "items")?;
    if let Some(filter) = &ops.filter {
        let mut kept = Vec::with_capacity(items.len());
        for (i, item) in items.into_iter().enumerate() {
            if matches!(
                for_item(filter, env, &item, i).map_err(|e| format!("filter: {e}"))?,
                Value::Bool(true)
            ) {
                kept.push(item);
            }
        }
        items = kept;
    }
    if let Some(sort) = &ops.sort {
        let mut keyed = Vec::with_capacity(items.len());
        for (i, item) in items.into_iter().enumerate() {
            keyed.push((
                for_item(sort, env, &item, i).map_err(|e| format!("sort: {e}"))?,
                item,
            ));
        }
        keyed.sort_by(|a, b| order(&a.0, &b.0));
        if ops.descending {
            keyed.reverse();
        }
        items = keyed.into_iter().map(|(_, item)| item).collect();
    }
    if let Some(unique) = &ops.unique {
        let mut seen = std::collections::HashSet::new();
        let mut kept = Vec::with_capacity(items.len());
        for (i, item) in items.into_iter().enumerate() {
            let key = for_item(unique, env, &item, i)
                .map_err(|e| format!("unique: {e}"))?
                .to_json();
            if seen.insert(key) {
                kept.push(item);
            }
        }
        items = kept;
    }
    if let Some(limit) = ops.limit {
        items.truncate(limit);
    }
    items.truncate(MAX);
    Ok(Value::list(items))
}

fn decimal(v: &Value) -> Option<Decimal> {
    match v {
        Value::Number(n) => Some(*n),
        Value::Text(t) => t.trim().parse().ok(),
        _ => None,
    }
}

pub(super) fn group(ops: &GroupOps, env: &Env<'_>) -> Result<Value, String> {
    let items = list_of(ops.items.evaluate(env)?, "items")?;
    // Groups in the order their first item came.
    let mut groups: Vec<(Value, Vec<(usize, Value)>)> = Vec::new();
    let mut index: BTreeMap<String, usize> = BTreeMap::new();
    for (i, item) in items.into_iter().enumerate() {
        let key = for_item(&ops.by, env, &item, i).map_err(|e| format!("by: {e}"))?;
        let at = *index.entry(key.to_json()).or_insert_with(|| {
            groups.push((key.clone(), Vec::new()));
            groups.len() - 1
        });
        if let Some(group) = groups.get_mut(at) {
            group.1.push((i, item));
        }
    }
    let mut out = Vec::with_capacity(groups.len());
    for (key, members) in groups {
        let mut record = Record::new();
        record.insert("key".to_owned(), key);
        record.insert("count".to_owned(), Value::number(Decimal::from(members.len())));
        for (field, aggregate, expr) in &ops.fields {
            let mut values = Vec::with_capacity(members.len());
            for (i, item) in &members {
                let v = for_item(expr, env, item, *i).map_err(|e| format!("{field}: {e}"))?;
                if !matches!(v, Value::Null) {
                    values.push(v);
                }
            }
            let result = match aggregate {
                Aggregate::Sum | Aggregate::Average => {
                    let mut total = Decimal::ZERO;
                    for v in &values {
                        let n =
                            decimal(v).ok_or_else(|| format!("{field}: {} is not a number", v.to_json()))?;
                        total = total
                            .checked_add(n)
                            .ok_or_else(|| format!("{field}: too large a total"))?;
                    }
                    if *aggregate == Aggregate::Sum {
                        Value::number(total)
                    } else if values.is_empty() {
                        Value::Null
                    } else {
                        Value::number(total / Decimal::from(values.len()))
                    }
                }
                Aggregate::Min => values.into_iter().min_by(order).unwrap_or(Value::Null),
                Aggregate::Max => values.into_iter().max_by(order).unwrap_or(Value::Null),
            };
            record.insert(field.clone(), result);
        }
        out.push(Value::record(record));
    }
    Ok(Value::list(out))
}

pub(super) fn merge(
    of: &[Compiled],
    mode: MergeMode,
    key: Option<&str>,
    env: &Env<'_>,
) -> Result<Value, String> {
    let mut parts = Vec::with_capacity(of.len());
    for (i, expr) in of.iter().enumerate() {
        parts.push(expr.evaluate(env).map_err(|e| format!("of[{i}]: {e}"))?);
    }
    match mode {
        MergeMode::Append => {
            let mut out = Vec::new();
            for part in parts {
                match part {
                    Value::List(items) => out.extend(items.iter().cloned()),
                    Value::Null => {}
                    // A record (a branch's result, say) is one more item.
                    other => out.push(other),
                }
            }
            out.truncate(MAX);
            Ok(Value::list(out))
        }
        MergeMode::Fields => {
            let mut out = Record::new();
            for (i, part) in parts.into_iter().enumerate() {
                match part {
                    Value::Record(fields) => {
                        for (k, v) in fields.iter() {
                            out.insert(k.clone(), v.clone());
                        }
                    }
                    Value::Null => {}
                    other => return Err(format!("of[{i}]: a record to merge, not {}", kind_of(&other))),
                }
            }
            Ok(Value::record(out))
        }
        MergeMode::Key => {
            let key = key.unwrap_or("id");
            let mut order: Vec<String> = Vec::new();
            let mut joined: BTreeMap<String, Record> = BTreeMap::new();
            for (i, part) in parts.into_iter().enumerate() {
                for item in list_of(part, &format!("of[{i}]"))? {
                    let Value::Record(fields) = item else {
                        return Err(format!("of[{i}]: records to join, each with {key}"));
                    };
                    let id = fields.get(key).map_or_else(|| "null".to_owned(), Value::to_json);
                    let into = joined.entry(id.clone()).or_insert_with(|| {
                        order.push(id);
                        Record::new()
                    });
                    for (k, v) in fields.iter() {
                        into.insert(k.clone(), v.clone());
                    }
                }
            }
            let out: Vec<Value> = order
                .into_iter()
                .filter_map(|id| joined.remove(&id))
                .take(MAX)
                .map(Value::record)
                .collect();
            Ok(Value::list(out))
        }
    }
}

pub(super) fn datetime(ops: &DateTimeOps, env: &Env<'_>) -> Result<Value, String> {
    let mut at: DateTime<Utc> = match &ops.value {
        None => Utc::now(),
        Some(expr) => match expr.evaluate(env).map_err(|e| format!("value: {e}"))? {
            Value::Time(t) => t,
            Value::Number(n) => {
                let seconds = n.trunc().to_i64().ok_or("value: too large a number of seconds")?;
                DateTime::from_timestamp(seconds, 0).ok_or("value: not a time")?
            }
            Value::Text(t) => DateTime::parse_from_rfc3339(t.trim())
                .map(|t| t.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDate::parse_from_str(t.trim(), "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc())
                })
                .map_err(|_| format!("value: {t:?} is not a time, as 2026-11-16T09:30:00Z"))?,
            Value::Null => Utc::now(),
            other => return Err(format!("value: a time, not {}", kind_of(&other))),
        },
    };
    let span = |d: std::time::Duration| {
        chrono::Duration::from_std(d).map_err(|_| "too long a time to move by".to_owned())
    };
    if let Some(add) = ops.add {
        at = at
            .checked_add_signed(span(add)?)
            .ok_or("the time moved too far")?;
    }
    if let Some(subtract) = ops.subtract {
        at = at
            .checked_sub_signed(span(subtract)?)
            .ok_or("the time moved too far")?;
    }
    let zone: chrono_tz::Tz = ops
        .timezone
        .as_deref()
        .unwrap_or("UTC")
        .parse()
        .map_err(|_| "not a time zone".to_owned())?;
    let local = at.with_timezone(&zone);
    let mut shown = json!({
        "iso": local.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "utc": at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "unix": at.timestamp(),
        "date": local.format("%Y-%m-%d").to_string(),
        "time": local.format("%H:%M:%S").to_string(),
        "weekday": local.format("%A").to_string(),
        "timezone": zone.name(),
    });
    if let Some(format) = &ops.format {
        // chrono's own check: a format it cannot write is an error, not a panic.
        let mut written = String::new();
        std::fmt::Write::write_fmt(&mut written, format_args!("{}", local.format(format)))
            .map_err(|_| format!("format: {format:?} is not a time format, as %Y-%m-%d %H:%M"))?;
        if let Some(fields) = shown.as_object_mut() {
            fields.insert("formatted".to_owned(), json!(written));
        }
    }
    Ok(Value::from_json(&shown))
}
