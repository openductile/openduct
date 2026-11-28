// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Data tables: a workspace's own named tables, each with typed columns (text, number, boolean, date-time, JSON),
//! that runs read and write - and each workflow's own state, a record of names and values its runs keep between
//! them. Steps reach them through [`Tables`]; the studio keeps them in the broker's log, the same on every node.
//!
//! A change is planned on the table as it is, while the table's **write turn** is held: no other run, on this node
//! or another, changes the table between the plan reading it and the change being written. So two runs updating the
//! same row one after the other each see the other's change: `count = row.count + 1` from two runs at once counts
//! two. Reads take no turn: they see every change written before them.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use openduct_expr::{Record, Value};
use rust_decimal::Decimal;

mod memory;
pub mod wanted; // test runs and needs

pub use memory::MemoryTables;

/// The most tables a workspace has.
pub const MAX_TABLES: usize = 100;
/// The most columns a table has.
pub const MAX_COLUMNS: usize = 50;
/// The most rows a table holds.
pub const MAX_ROWS: usize = 50_000;
/// The largest row, in bytes as JSON.
pub const MAX_ROW_BYTES: usize = 64 * 1024;
/// The most a workspace's tables hold in all, in bytes as JSON.
pub const MAX_WORKSPACE_BYTES: usize = 50 * 1024 * 1024;
/// The largest state a workflow keeps, in bytes as JSON.
pub const MAX_STATE_BYTES: usize = 256 * 1024;
/// The most names a workflow's state holds.
pub const MAX_STATE_KEYS: usize = 1_000;
/// The most rows one step changes (or deletes).
pub const MAX_CHANGED: usize = 10_000;
/// The most rows a `get` gives.
pub const MAX_GET: usize = 10_000;
/// The rows a `get` gives when it does not say.
pub const DEFAULT_GET: usize = 100;
/// The columns every table has, kept by the table itself.
pub const SYSTEM_COLUMNS: &[&str] = &["id", "created_at", "updated_at"];

type Pending<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// What a column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Number,
    Boolean,
    DateTime,
    Json,
}

impl ColumnType {
    pub const ALL: [Self; 5] = [
        Self::Text,
        Self::Number,
        Self::Boolean,
        Self::DateTime,
        Self::Json,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.name() == name)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::DateTime => "datetime",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub kind: ColumnType,
}

/// One row: its id (given by the table, in the order rows were added), its values by column, and when it was added
/// and last changed (milliseconds since 1970).
#[derive(Debug, Clone)]
pub struct Row {
    pub id: u64,
    pub values: Arc<Record>,
    pub created: i64,
    pub updated: i64,
}

impl Row {
    /// The row as a step gives it: `id`, every column, `created_at` and `updated_at`.
    pub fn shown(&self, columns: &[Column]) -> Value {
        let mut out = Record::new();
        out.insert("id".to_owned(), Value::number(Decimal::from(self.id)));
        for column in columns {
            let value = self.values.get(&column.name).cloned().unwrap_or(Value::Null);
            out.insert(column.name.clone(), value);
        }
        out.insert("created_at".to_owned(), time_of(self.created));
        out.insert("updated_at".to_owned(), time_of(self.updated));
        Value::record(out)
    }

    /// Roughly its size, in bytes as JSON.
    pub fn size(&self) -> usize {
        self.values
            .iter()
            .map(|(k, v)| k.len() + 4 + v.size())
            .sum::<usize>()
            + 64
    }
}

fn time_of(ms: i64) -> Value {
    DateTime::<Utc>::from_timestamp_millis(ms).map_or(Value::Null, Value::Time)
}

/// A table as it is: its columns, its rows by id, the id its next row gets.
#[derive(Debug, Clone, Default)]
pub struct TableData {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: BTreeMap<u64, Row>,
    pub next_id: u64,
    /// A stand-in, for a test run that has no tables: empty, any column taken (as JSON).
    pub stand_in: bool,
}

impl TableData {
    /// A test run's stand-in for the table called `name`.
    pub fn stand_in(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            next_id: 1,
            stand_in: true,
            ..Self::default()
        }
    }

    /// The column called `name`, or why there is none.
    pub fn column(&self, name: &str) -> Result<ColumnType, String> {
        if let Some(c) = self.columns.iter().find(|c| c.name == name) {
            return Ok(c.kind);
        }
        if self.stand_in {
            return Ok(ColumnType::Json);
        }
        let have: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        Err(format!(
            "{} has no column {name:?} (its columns: {})",
            self.name,
            if have.is_empty() {
                "none yet".to_owned()
            } else {
                have.join(", ")
            }
        ))
    }

    /// What a step reads of a column, system columns included.
    pub fn readable(&self, name: &str) -> Result<ColumnType, String> {
        match name {
            "id" => Ok(ColumnType::Number),
            "created_at" | "updated_at" => Ok(ColumnType::DateTime),
            _ => self.column(name),
        }
    }

    /// The rows as a step gives them, in id order.
    pub fn shown(&self) -> Vec<Value> {
        self.rows.values().map(|r| r.shown(&self.columns)).collect()
    }
}

/// What a step changes: rows written whole (new ones with the ids from `next_id` on), rows taken away, and the id
/// the next new row gets.
#[derive(Debug, Clone, Default)]
pub struct Changes {
    pub put: Vec<Row>,
    pub delete: Vec<u64>,
    pub next_id: u64,
}

/// A change worked out from the table as it is, while its write turn is held: what to write, and the step's result.
pub type Plan<'a> = Box<dyn FnOnce(&TableData) -> Result<(Changes, Value), String> + Send + 'a>;

/// A change of a workflow's state worked out from the state as it is, while its turn is held: the new state (`None`:
/// none written), and the step's result.
pub type StatePlan<'a> = Box<dyn FnOnce(&Record) -> Result<(Option<Record>, Value), String> + Send + 'a>;

/// Where a run's tables and its workflow's state are: its workspace's.
pub trait Tables: Send + Sync + std::fmt::Debug {
    /// The table called `name`, as it is now.
    fn read<'a>(&'a self, name: &'a str) -> Pending<'a, Arc<TableData>>;
    /// Changes the table called `name` as `plan` says, with the table's write turn held; answers the plan's result.
    fn change<'a>(&'a self, name: &'a str, plan: Plan<'a>) -> Pending<'a, Value>;
    /// The state of workflow `workflow` (empty when it keeps none).
    fn state<'a>(&'a self, workflow: &'a str) -> Pending<'a, Arc<Record>>;
    /// Changes workflow `workflow`'s state as `plan` says, with its turn held; answers the plan's result.
    fn change_state<'a>(&'a self, workflow: &'a str, plan: StatePlan<'a>) -> Pending<'a, Value>;
    /// Whether there is a table called `name`: a test run uses an empty stand-in for one not made yet. // test runs
    fn has<'a>(&'a self, name: &'a str) -> Pending<'a, bool> {
        Box::pin(async move { Ok(self.read(name).await.is_ok()) })
    }
}

/// How a run reaches tables: not at all (a test run without them: each table a stand-in), to read only (a test run
/// or a preview: a write is worked out and shown), or to read and write.
#[derive(Clone, Copy)]
pub enum Access<'a> {
    None,
    Read(&'a dyn Tables),
    Write(&'a dyn Tables),
}

impl std::fmt::Debug for Access<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "None",
            Self::Read(_) => "Read",
            Self::Write(_) => "Write",
        })
    }
}

impl<'a> Access<'a> {
    /// The same tables, to read only: what a preview of a live run reaches.
    pub fn read_only(self) -> Self {
        match self {
            Self::Write(t) => Self::Read(t),
            other => other,
        }
    }
}

/// What is wrong with a table's or a column's name: letters, digits and `_`, starting with a letter, at most 64 - so
/// an expression reads it as `row.<name>`.
pub fn name_problem(name: &str) -> Option<String> {
    let fine = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && name.len() <= 64;
    (!fine).then(|| format!("{name:?}: letters, digits and '_', starting with a letter, at most 64"))
}

/// What is wrong with the name of a value a workflow keeps: 1 to 100 letters, digits, `_`, `-` or `.`.
pub fn key_problem(key: &str) -> Option<String> {
    let fine = !key.is_empty()
        && key.len() <= 100
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    (!fine).then(|| format!("{key:?}: a name of 1 to 100 letters, digits, '_', '-' or '.'"))
}

/// What is wrong with a table's columns, if anything.
pub fn columns_problem(columns: &[Column]) -> Option<String> {
    if columns.len() > MAX_COLUMNS {
        return Some(format!("at most {MAX_COLUMNS} columns"));
    }
    for (i, column) in columns.iter().enumerate() {
        if let Some(why) = name_problem(&column.name) {
            return Some(format!("column {}: {why}", i + 1));
        }
        if SYSTEM_COLUMNS.contains(&column.name.as_str()) {
            return Some(format!(
                "column {}: {} is kept by the table itself",
                i + 1,
                column.name
            ));
        }
        if columns.iter().take(i).any(|c| c.name == column.name) {
            return Some(format!("column {}: two columns called {}", i + 1, column.name));
        }
    }
    None
}

/// `value` as a column of `kind` holds it, or why it cannot be: text from anything; a number from a number or text
/// that reads as one; true or false from a boolean, `true`/`false`/`yes`/`no`/`1`/`0`; a date-time from a time or
/// ISO 8601 text (`2026-11-27`, `2026-11-27T09:30:00Z`); JSON as it is. Null, and empty text in a column that is not
/// text, is empty.
pub fn coerce(kind: ColumnType, value: &Value) -> Result<Value, String> {
    let blank = matches!(value, Value::Text(t) if t.trim().is_empty());
    if value.is_null() || (blank && !matches!(kind, ColumnType::Text | ColumnType::Json)) {
        return Ok(Value::Null);
    }
    let refused = || format!("{} is not {}", value.describe(), article(kind));
    Ok(match (kind, value) {
        (ColumnType::Json, v) => v.clone(),
        (ColumnType::Text, Value::Text(_)) => value.clone(),
        (ColumnType::Text, v) => Value::text(v.to_text().as_str()),
        (ColumnType::Number, Value::Number(_) | Value::BigNumber(_)) => value.clone(),
        (ColumnType::Number, Value::Text(t)) => Value::number(number(t.trim()).ok_or_else(refused)?),
        (ColumnType::Boolean, Value::Bool(_)) => value.clone(),
        (ColumnType::Boolean, Value::Text(t)) => match t.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Value::Bool(true),
            "false" | "no" | "0" => Value::Bool(false),
            _ => return Err(refused()),
        },
        (ColumnType::Boolean, Value::Number(n)) if n.is_zero() || *n == Decimal::ONE => {
            Value::Bool(!n.is_zero())
        }
        (ColumnType::DateTime, Value::Time(_)) => value.clone(),
        (ColumnType::DateTime, Value::Text(t)) => Value::Time(parse_time(t.trim()).ok_or_else(refused)?),
        _ => return Err(refused()),
    })
}

/// As [`coerce`], keeping a value that does not fit as it is: how rows kept before a column changed are read.
pub fn coerce_kept(kind: ColumnType, value: &Value) -> Value {
    coerce(kind, value).unwrap_or_else(|_| value.clone())
}

/// Text as an exact decimal: `42`, `-3.5`, `1.5e3`.
fn number(text: &str) -> Option<Decimal> {
    if text.is_empty() || text.len() > 40 {
        return None;
    }
    text.parse::<Decimal>()
        .ok()
        .or_else(|| Decimal::from_scientific(text).ok())
}

fn article(kind: ColumnType) -> &'static str {
    match kind {
        ColumnType::Text => "text",
        ColumnType::Number => "a number",
        ColumnType::Boolean => "true or false",
        ColumnType::DateTime => "a date and time",
        ColumnType::Json => "JSON",
    }
}

/// ISO 8601: a date (midnight UTC), a date and time without a zone (UTC), or with one.
pub fn parse_time(text: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_rfc3339(text) {
        return Some(t.with_timezone(&Utc));
    }
    for form in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(text, form) {
            return Some(t.and_utc());
        }
    }
    NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| t.and_utc())
}

/// A row's values, given by column name, as the table holds them: every name a column of it, every value of its
/// column's type; the columns not given are empty.
pub fn row_values(table: &TableData, given: &Record) -> Result<Record, String> {
    let mut out = Record::new();
    for (name, value) in given {
        if SYSTEM_COLUMNS.contains(&name.as_str()) {
            continue;
        }
        let kind = table.column(name)?;
        out.insert(
            name.clone(),
            coerce(kind, value).map_err(|e| format!("{name}: {e}"))?,
        );
    }
    Ok(out)
}

/// Whether `changes` keep `table` within its limits: rows of its own columns, each at most [`MAX_ROW_BYTES`], at
/// most [`MAX_ROWS`] in all, at most [`MAX_CHANGED`] changed at once.
pub fn check_changes(table: &TableData, changes: &Changes) -> Result<(), String> {
    if changes.put.len() + changes.delete.len() > MAX_CHANGED {
        return Err(format!(
            "a step changes at most {MAX_CHANGED} rows at once; this one would change {}",
            changes.put.len() + changes.delete.len()
        ));
    }
    let added = changes
        .put
        .iter()
        .filter(|r| !table.rows.contains_key(&r.id))
        .count();
    let removed = changes
        .delete
        .iter()
        .filter(|id| table.rows.contains_key(id))
        .count();
    if table.rows.len() + added > MAX_ROWS + removed {
        return Err(format!(
            "{} would hold more than {MAX_ROWS} rows: delete some first",
            table.name
        ));
    }
    for row in &changes.put {
        if row.size() > MAX_ROW_BYTES {
            return Err(format!(
                "row {} of {} is larger than {} KiB",
                row.id,
                table.name,
                MAX_ROW_BYTES / 1024
            ));
        }
        for (name, value) in row.values.iter() {
            let kind = table.column(name)?;
            if coerce(kind, value).is_err() {
                return Err(format!(
                    "row {}: {name}: {} is not {}",
                    row.id,
                    value.describe(),
                    article(kind)
                ));
            }
        }
    }
    Ok(())
}

/// Whether a workflow's new state is within its limits.
pub fn check_state(state: &Record) -> Result<(), String> {
    if state.len() > MAX_STATE_KEYS {
        return Err(format!("a workflow keeps at most {MAX_STATE_KEYS} values"));
    }
    if let Some(bad) = state.keys().find_map(|k| key_problem(k)) {
        return Err(format!("the state's name {bad}"));
    }
    let size = Value::record(state.clone()).size();
    if size > MAX_STATE_BYTES {
        return Err(format!(
            "a workflow keeps at most {} KiB of state; this would be {} KiB",
            MAX_STATE_BYTES / 1024,
            size / 1024
        ));
    }
    Ok(())
}

/// The time now, in milliseconds since 1970.
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(t: &str) -> Value {
        Value::text(t)
    }

    #[test]
    fn values_take_their_column_s_type_or_say_why_not() {
        assert_eq!(
            coerce(ColumnType::Number, &text(" 42.50 ")).unwrap().to_json(),
            "42.5"
        );
        assert!(
            coerce(ColumnType::Number, &text("forty"))
                .unwrap_err()
                .contains("not a number")
        );
        assert_eq!(
            coerce(ColumnType::Boolean, &text("Yes")).unwrap().to_json(),
            "true"
        );
        assert_eq!(
            coerce(ColumnType::Boolean, &Value::number(Decimal::ZERO))
                .unwrap()
                .to_json(),
            "false"
        );
        assert!(coerce(ColumnType::Boolean, &text("maybe")).is_err());
        assert_eq!(
            coerce(ColumnType::DateTime, &text("2026-11-27"))
                .unwrap()
                .to_json(),
            r#""2026-11-27T00:00:00Z""#
        );
        assert_eq!(
            coerce(ColumnType::DateTime, &text("2026-11-27T10:00:00+01:00"))
                .unwrap()
                .to_json(),
            r#""2026-11-27T09:00:00Z""#
        );
        assert!(coerce(ColumnType::DateTime, &text("next week")).is_err());
        assert_eq!(
            coerce(ColumnType::Text, &Value::Bool(true)).unwrap().to_json(),
            r#""true""#
        );
        let json = Value::parse_json(r#"{"a": [1, 2]}"#).unwrap();
        assert_eq!(
            coerce(ColumnType::Json, &json).unwrap().to_json(),
            r#"{"a":[1,2]}"#
        );
        // Empty is empty, except text, which keeps its empty text.
        assert!(coerce(ColumnType::Number, &text("")).unwrap().is_null());
        assert_eq!(coerce(ColumnType::Text, &text("")).unwrap().to_json(), r#""""#);
        assert!(coerce(ColumnType::Boolean, &Value::Null).unwrap().is_null());
        assert_eq!(
            coerce_kept(ColumnType::Number, &text("n/a")).to_json(),
            r#""n/a""#
        );
    }

    #[test]
    fn names_and_columns_are_checked() {
        assert!(name_problem("leads").is_none() && name_problem("Lead_2").is_none());
        for bad in ["", "2leads", "a-b", "a b", &"x".repeat(65)] {
            assert!(name_problem(bad).is_some(), "{bad}");
        }
        assert!(key_problem("last.seen-id_2").is_none());
        assert!(key_problem("").is_some() && key_problem("a b").is_some());
        let col = |n: &str| Column {
            name: n.to_owned(),
            kind: ColumnType::Text,
        };
        assert!(columns_problem(&[col("email"), col("name")]).is_none());
        assert!(
            columns_problem(&[col("email"), col("email")])
                .unwrap()
                .contains("two columns")
        );
        assert!(
            columns_problem(&[col("id")])
                .unwrap()
                .contains("kept by the table")
        );
        let many: Vec<Column> = (0..=MAX_COLUMNS).map(|i| col(&format!("c{i}"))).collect();
        assert!(columns_problem(&many).is_some());
        assert_eq!(ColumnType::parse("datetime"), Some(ColumnType::DateTime));
        assert_eq!(ColumnType::parse("date"), None);
    }

    #[test]
    fn changes_stay_within_the_limits() {
        let table = TableData {
            name: "leads".to_owned(),
            columns: vec![Column {
                name: "score".to_owned(),
                kind: ColumnType::Number,
            }],
            next_id: 1,
            ..TableData::default()
        };
        let row = |id: u64, v: Value| Row {
            id,
            values: Arc::new([("score".to_owned(), v)].into_iter().collect()),
            created: 0,
            updated: 0,
        };
        let fine = Changes {
            put: vec![row(1, Value::number(Decimal::ONE))],
            ..Changes::default()
        };
        assert!(check_changes(&table, &fine).is_ok());
        let wrong = Changes {
            put: vec![row(1, text("high"))],
            ..Changes::default()
        };
        assert!(
            check_changes(&table, &wrong)
                .unwrap_err()
                .contains("not a number")
        );
        let big = Changes {
            put: vec![row(1, text(&"x".repeat(MAX_ROW_BYTES)))],
            ..Changes::default()
        };
        let mut texts = table.clone();
        texts.columns[0].kind = ColumnType::Text;
        assert!(check_changes(&texts, &big).unwrap_err().contains("larger than"));
        let too_many = Changes {
            delete: (0..=MAX_CHANGED as u64).collect(),
            ..Changes::default()
        };
        assert!(check_changes(&table, &too_many).unwrap_err().contains("at most"));
        assert!(table.column("nope").unwrap_err().contains("has no column"));
        assert_eq!(TableData::stand_in("x").column("any").unwrap(), ColumnType::Json);
    }

    #[test]
    fn a_state_stays_within_its_limits() {
        let mut state = Record::new();
        state.insert("count".to_owned(), Value::number(Decimal::ONE));
        assert!(check_state(&state).is_ok());
        state.insert("big".to_owned(), text(&"x".repeat(MAX_STATE_BYTES)));
        assert!(check_state(&state).unwrap_err().contains("KiB"));
        let mut bad = Record::new();
        bad.insert("a b".to_owned(), Value::Null);
        assert!(check_state(&bad).is_err());
    }
}
