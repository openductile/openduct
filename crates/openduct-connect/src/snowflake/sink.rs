// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: snowflake:` - each message written as a row of one table, many rows a statement, each field bound as text
//! and cast to its column's type. `insert` adds rows; `merge` changes the row whose `match` columns have the
//! message's values, or adds one, so writing a message again changes the same row. A field the message leaves out
//! is left as it is (or the column's default); a field that is `null` is written as NULL.
//!
//! When Snowflake refuses a statement for its data (a value not of the column's type, too long, missing where it
//! is required), its rows are written one at a time and the ones refused are set aside with Snowflake's reason.

use std::collections::BTreeMap;

use super::session::{Refused, Session};
use super::source::find;
use super::values::{Column, bound, cast, quoted};
use crate::{SinkOutcome, SinkRecord};

/// The most rows, bound values and bytes bound in one statement.
const MOST_ROWS: usize = 1_000;
const MOST_BINDS: usize = 16_000;
const MOST_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Insert,
    Merge,
}

#[derive(Debug)]
pub struct SnowflakeSink {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) table: String,
    pub(super) mode: Mode,
    /// The columns a merge finds the row by, as written in the pipeline.
    pub(super) matching: Vec<String>,
    /// The table's columns, learnt with the first messages.
    pub(super) columns: Option<Vec<Column>>,
}

/// Messages as a row: the records (more than one when a merge folds messages of one key together), and the text
/// bound for each column they have (`None` is NULL).
struct Row {
    records: Vec<SinkRecord>,
    values: BTreeMap<usize, Option<String>>,
}

impl SnowflakeSink {
    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    async fn table_columns(&mut self) -> Result<Vec<Column>, String> {
        if let Some(c) = &self.columns {
            return Ok(c.clone());
        }
        let probe = format!("SELECT * FROM {} LIMIT 0", self.table);
        let columns = match self.session.run(&probe, &[]).await? {
            Ok(answer) => answer.columns,
            Err(refused) => {
                return Err(format!(
                    "Snowflake refused writing to {}: {}",
                    self.table, refused.words
                ));
            }
        };
        if let Some(missing) = self.matching.iter().find(|m| find(&columns, m).is_none()) {
            return Err(format!("Snowflake's {} has no column {missing}", self.table));
        }
        self.columns = Some(columns.clone());
        Ok(columns)
    }

    /// The message as a row, or why it cannot be one.
    fn row(
        &self,
        columns: &[Column],
        record: &SinkRecord,
    ) -> Result<BTreeMap<usize, Option<String>>, String> {
        let Ok(serde_json::Value::Object(fields)) = serde_json::from_slice(&record.value) else {
            return Err("the message is not a JSON object of fields".to_owned());
        };
        let mut values = BTreeMap::new();
        for (name, value) in &fields {
            let Some(at) = find(columns, name).and_then(|c| columns.iter().position(|x| x.name == c.name))
            else {
                return Err(format!("Snowflake's {} has no column {name}", self.table));
            };
            let column = columns.get(at).ok_or("the table's columns changed")?;
            values.insert(at, bound(column, value));
        }
        for m in &self.matching {
            let at = find(columns, m).and_then(|c| columns.iter().position(|x| x.name == c.name));
            if !at.is_some_and(|at| values.get(&at).is_some_and(Option::is_some)) {
                return Err(format!("the message has no {m}, which it is matched by"));
            }
        }
        Ok(values)
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        let columns = self.table_columns().await?;
        let mut rejected = Vec::new();
        let mut rows: Vec<Row> = Vec::new();
        for record in records {
            match self.row(&columns, &record) {
                Ok(values) => rows.push(Row {
                    records: vec![record],
                    values,
                }),
                Err(why) => rejected.push((record, why)),
            }
        }
        if self.mode == Mode::Merge {
            rows = one_row_per_key(rows, &self.key_columns(&columns));
        }
        // Rows with the same columns go together, so a column a message leaves out is never written as NULL.
        let mut groups: Vec<(Vec<usize>, Vec<Row>)> = Vec::new();
        for row in rows {
            let shape: Vec<usize> = row.values.keys().copied().collect();
            match groups.iter_mut().find(|(s, _)| *s == shape) {
                Some((_, group)) => group.push(row),
                None => groups.push((shape, vec![row])),
            }
        }
        for (shape, group) in groups {
            for chunk in chunks(group) {
                rejected.extend(self.write(&columns, &shape, chunk).await?);
            }
        }
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    fn key_columns(&self, columns: &[Column]) -> Vec<usize> {
        self.matching
            .iter()
            .filter_map(|m| find(columns, m).and_then(|c| columns.iter().position(|x| x.name == c.name)))
            .collect()
    }

    /// Write rows of one shape; the ones Snowflake refuses, with why.
    async fn write(
        &mut self,
        columns: &[Column],
        shape: &[usize],
        rows: Vec<Row>,
    ) -> Result<Vec<(SinkRecord, String)>, String> {
        let (statement, binds) = self.statement(columns, shape, &rows)?;
        let refused = match self.session.run(&statement, &binds).await? {
            Ok(_) => return Ok(Vec::new()),
            Err(refused) => refused,
        };
        if !refused.of_data() {
            return Err(self.stopped(&refused));
        }
        if rows.len() == 1 {
            let why = format!("Snowflake refused the row: {}", refused.words);
            return Ok(rows
                .into_iter()
                .flat_map(|r| r.records)
                .map(|record| (record, why.clone()))
                .collect());
        }
        // One of them is refused: find which, one at a time.
        let mut out = Vec::new();
        for row in rows {
            let (statement, binds) = self.statement(columns, shape, std::slice::from_ref(&row))?;
            match self.session.run(&statement, &binds).await? {
                Ok(_) => {}
                Err(r) if r.of_data() => {
                    let why = format!("Snowflake refused the row: {}", r.words);
                    out.extend(row.records.into_iter().map(|record| (record, why.clone())));
                }
                Err(r) => return Err(self.stopped(&r)),
            }
        }
        Ok(out)
    }

    fn stopped(&self, refused: &Refused) -> String {
        format!("Snowflake refused writing to {}: {}", self.table, refused.words)
    }

    /// `INSERT INTO t (a, b) SELECT $1::.., $2::.. FROM VALUES (?, NULL), ..`, or the `MERGE` of the same rows.
    fn statement(
        &self,
        columns: &[Column],
        shape: &[usize],
        rows: &[Row],
    ) -> Result<(String, Vec<String>), String> {
        let mut used = Vec::with_capacity(shape.len());
        for at in shape {
            used.push(columns.get(*at).ok_or("the table's columns changed")?);
        }
        let mut binds = Vec::new();
        let tuples: Vec<String> = rows
            .iter()
            .map(|row| {
                let each: Vec<&str> = shape
                    .iter()
                    .map(|at| match row.values.get(at).cloned().flatten() {
                        Some(text) => {
                            binds.push(text);
                            "?"
                        }
                        None => "NULL",
                    })
                    .collect();
                format!("({})", each.join(", "))
            })
            .collect();
        let names: Vec<String> = used.iter().map(|c| quoted(&c.name)).collect();
        let select: Vec<String> = used
            .iter()
            .zip(&names)
            .enumerate()
            .map(|(n, (c, name))| format!("{} AS {name}", cast(c, &format!("${}", n + 1))))
            .collect();
        let values = format!("SELECT {} FROM VALUES {}", select.join(", "), tuples.join(", "));
        let statement = match self.mode {
            Mode::Insert => format!("INSERT INTO {} ({}) {values}", self.table, names.join(", ")),
            Mode::Merge => {
                let keys: Vec<String> = self
                    .key_columns(columns)
                    .iter()
                    .filter_map(|at| columns.get(*at).map(|c| quoted(&c.name)))
                    .collect();
                let on: Vec<String> = keys.iter().map(|k| format!("t.{k} = s.{k}")).collect();
                let set: Vec<String> = names
                    .iter()
                    .filter(|n| !keys.contains(n))
                    .map(|n| format!("t.{n} = s.{n}"))
                    .collect();
                let matched = if set.is_empty() {
                    String::new()
                } else {
                    format!(" WHEN MATCHED THEN UPDATE SET {}", set.join(", "))
                };
                let from: Vec<String> = names.iter().map(|n| format!("s.{n}")).collect();
                format!(
                    "MERGE INTO {} AS t USING ({values}) AS s ON {}{matched} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    self.table,
                    on.join(" AND "),
                    names.join(", "),
                    from.join(", ")
                )
            }
        };
        Ok((statement, binds))
    }
}

/// For a merge, one row for each key: the messages of a key folded together in order, a later field over an
/// earlier one. Merging them one after another would leave the same row; Snowflake refuses a merge that changes one
/// row twice.
fn one_row_per_key(rows: Vec<Row>, keys: &[usize]) -> Vec<Row> {
    let mut out: Vec<Row> = Vec::new();
    let mut at: BTreeMap<Vec<Option<String>>, usize> = BTreeMap::new();
    for row in rows {
        let key: Vec<Option<String>> = keys
            .iter()
            .map(|k| row.values.get(k).cloned().flatten())
            .collect();
        match at.get(&key).and_then(|n| out.get_mut(*n)) {
            Some(first) => {
                first.records.extend(row.records);
                first.values.extend(row.values);
            }
            None => {
                at.insert(key, out.len());
                out.push(row);
            }
        }
    }
    out
}

/// Rows of one shape in statements within the limits.
fn chunks(rows: Vec<Row>) -> Vec<Vec<Row>> {
    let mut out: Vec<Vec<Row>> = Vec::new();
    let (mut binds, mut bytes) = (0, 0);
    for row in rows {
        let (b, n) = row
            .values
            .values()
            .flatten()
            .fold((0, 0), |(b, n), v| (b + 1, n + v.len()));
        let full = out
            .last()
            .is_none_or(|c| c.len() >= MOST_ROWS || binds + b > MOST_BINDS || bytes + n > MOST_BYTES);
        if full {
            out.push(Vec::new());
            (binds, bytes) = (0, 0);
        }
        binds += b;
        bytes += n;
        if let Some(c) = out.last_mut() {
            c.push(row);
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn row(n: i64, values: &[(usize, Option<&str>)]) -> Row {
        Row {
            records: vec![SinkRecord {
                key: None,
                value: Vec::new(),
                headers: Vec::new(),
                partition: 0,
                offset: n,
            }],
            values: values.iter().map(|(k, v)| (*k, v.map(str::to_owned))).collect(),
        }
    }

    #[test]
    fn a_merge_folds_the_messages_of_a_key_in_order() {
        let rows = vec![
            row(0, &[(0, Some("a")), (1, Some("Ada")), (2, Some("paid"))]),
            row(1, &[(0, Some("b")), (2, Some("paid"))]),
            row(2, &[(0, Some("a")), (2, Some("shipped"))]),
            row(3, &[(0, Some("a")), (3, None)]),
        ];
        let rows = one_row_per_key(rows, &[0]);
        assert_eq!(rows.len(), 2);
        let first = rows.first().unwrap();
        let offsets: Vec<i64> = first.records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, [0, 2, 3]);
        let want: BTreeMap<usize, Option<String>> = [
            (0, Some("a".to_owned())),
            (1, Some("Ada".to_owned())),
            (2, Some("shipped".to_owned())),
            (3, None),
        ]
        .into_iter()
        .collect();
        assert_eq!(first.values, want);
    }

    #[test]
    fn statements_stay_within_the_limits() {
        let rows: Vec<Row> = (0..2_500).map(|n| row(n, &[(0, Some("x"))])).collect();
        let sizes: Vec<usize> = chunks(rows).iter().map(Vec::len).collect();
        assert_eq!(sizes, [1_000, 1_000, 500]);
        let big = "x".repeat(600 * 1024);
        let rows: Vec<Row> = (0..3).map(|n| row(n, &[(0, Some(big.as_str()))])).collect();
        let sizes: Vec<usize> = chunks(rows).iter().map(Vec::len).collect();
        assert_eq!(sizes, [1, 1, 1]);
    }
}
