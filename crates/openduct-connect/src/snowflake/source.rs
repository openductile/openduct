// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: snowflake:` - the rows of one table or view as they are added or changed, read on a schedule in the
//! order of a column that grows with each change (`UPDATED_AT`, a sequence), then of a unique id. The position is
//! the last row's values of those two columns, bound to the next query (never written into it), so a restart
//! neither skips nor repeats a row.

use std::time::{Duration, Instant};

use super::session::Session;
use super::values::{Column, bound, cast, quoted, to_json};
use crate::json::json_string;
use crate::{SourceBatch, SourceMessage};

/// The most rows read in one statement.
const PAGE: usize = 1_000;

/// The column types a position can be kept in.
const ORDERED: &[&str] = &[
    "fixed",
    "real",
    "decfloat",
    "text",
    "date",
    "time",
    "timestamp_ntz",
    "timestamp_ltz",
    "timestamp_tz",
];

#[derive(Debug)]
pub struct SnowflakeSource {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) table: String,
    /// The columns to read, as written in the pipeline; empty for every column.
    pub(super) wanted: Vec<String>,
    pub(super) track: String,
    pub(super) id: Option<String>,
    /// More of the `WHERE`, from the pipeline.
    pub(super) filter: Option<String>,
    pub(super) every: Duration,
    /// Found when the source starts: the `SELECT` list and the two columns of the position.
    select: String,
    track_column: Option<Column>,
    id_column: Option<Column>,
    last_poll: Option<Instant>,
    more: bool,
}

/// A column of the table by the name the pipeline gave: exactly, or as Snowflake keeps an unquoted name.
pub(super) fn find<'a>(columns: &'a [Column], name: &str) -> Option<&'a Column> {
    columns
        .iter()
        .find(|c| c.name == name)
        .or_else(|| columns.iter().find(|c| c.name.eq_ignore_ascii_case(name)))
}

impl SnowflakeSource {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        shown: String,
        session: Session,
        table: String,
        wanted: Vec<String>,
        track: String,
        id: Option<String>,
        filter: Option<String>,
        every: Duration,
    ) -> Self {
        Self {
            shown,
            session,
            table,
            wanted,
            track,
            id,
            filter,
            every,
            select: "*".to_owned(),
            track_column: None,
            id_column: None,
            last_poll: None,
            more: false,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Sign in and learn the table's columns, so a wrong table, column or key is found before the first poll.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let probe = format!("SELECT * FROM {} LIMIT 0", self.table);
        let columns = match self.session.run(&probe, &[]).await? {
            Ok(answer) => answer.columns,
            Err(refused) => {
                return Err(format!(
                    "Snowflake refused reading {}: {}",
                    self.table, refused.words
                ));
            }
        };
        let column = |name: &str| {
            find(&columns, name)
                .cloned()
                .ok_or_else(|| format!("Snowflake's {} has no column {name}", self.table))
        };
        let track = column(&self.track)?;
        if !ORDERED.contains(&track.kind.as_str()) {
            return Err(format!(
                "{} is {}, which cannot order the rows: give a number, text, date or time",
                track.name,
                track.kind.to_ascii_uppercase()
            ));
        }
        let id = self.id.clone().map(|i| column(&i)).transpose()?;
        if let Some(i) = &id
            && !ORDERED.contains(&i.kind.as_str())
        {
            return Err(format!(
                "{} is {}, which cannot be an id",
                i.name,
                i.kind.to_ascii_uppercase()
            ));
        }
        if !self.wanted.is_empty() {
            let mut names = Vec::new();
            for w in &self.wanted {
                names.push(column(w)?.name);
            }
            for needed in [Some(&track), id.as_ref()].into_iter().flatten() {
                if !names.contains(&needed.name) {
                    names.push(needed.name.clone());
                }
            }
            self.select = names.iter().map(|n| quoted(n)).collect::<Vec<_>>().join(", ");
        }
        self.track_column = Some(track);
        self.id_column = id;
        Ok(())
    }

    /// The statement after the position, and what it binds.
    fn statement(&self, after: Option<Vec<serde_json::Value>>) -> Result<(String, Vec<String>), String> {
        let track = self.track_column.as_ref().ok_or("the source has not started")?;
        let t = quoted(&track.name);
        // A row with no value to order by cannot be read in order: it is left out.
        let mut conditions = vec![format!("{t} IS NOT NULL")];
        let mut binds = Vec::new();
        if let Some(i) = &self.id_column {
            conditions.push(format!("{} IS NOT NULL", quoted(&i.name)));
        }
        if let Some(after) = after {
            let text = |n: usize, c: &Column| {
                after
                    .get(n)
                    .and_then(|v| bound(c, v))
                    .ok_or_else(|| "the saved position does not fit the table".to_owned())
            };
            let since = text(0, track)?;
            match &self.id_column {
                Some(i) => {
                    let (tc, ic) = (cast(track, "?"), cast(i, "?"));
                    conditions.push(format!(
                        "({t} > {tc} OR ({t} = {tc} AND {} > {ic}))",
                        quoted(&i.name)
                    ));
                    binds.extend([since.clone(), since, text(1, i)?]);
                }
                None => {
                    conditions.push(format!("{t} > {}", cast(track, "?")));
                    binds.push(since);
                }
            }
        }
        if let Some(f) = &self.filter {
            conditions.push(format!("({f})"));
        }
        let order = match &self.id_column {
            Some(i) => format!("{t}, {}", quoted(&i.name)),
            None => t.clone(),
        };
        Ok((
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {order} LIMIT {PAGE}",
                self.select,
                self.table,
                conditions.join(" AND ")
            ),
            binds,
        ))
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        if let (Some(last), false) = (self.last_poll, self.more) {
            tokio::time::sleep_until((last + self.every).into()).await;
        }
        self.last_poll = Some(Instant::now());
        let after = match position {
            None => None,
            Some(p) => match serde_json::from_str::<serde_json::Value>(p) {
                Ok(serde_json::Value::Array(values)) if (1..=2).contains(&values.len()) => Some(values),
                _ => return Err(format!("the saved position {p:?} is not one this source wrote")),
            },
        };
        let (statement, binds) = self.statement(after)?;
        let answer = match self.session.run(&statement, &binds).await? {
            Ok(answer) => answer,
            Err(refused) => {
                return Err(format!(
                    "Snowflake refused reading {}: {}",
                    self.table, refused.words
                ));
            }
        };
        self.more = answer.rows.len() >= PAGE;
        let at = |c: &Option<Column>| {
            c.as_ref()
                .and_then(|c| answer.columns.iter().position(|a| a.name == c.name))
        };
        let (track_at, id_at) = (at(&self.track_column), at(&self.id_column));
        let mut messages = Vec::with_capacity(answer.rows.len());
        let mut last = None;
        for row in &answer.rows {
            let values: Vec<String> = answer
                .columns
                .iter()
                .enumerate()
                .map(|(n, c)| to_json(c, row.get(n).and_then(Option::as_deref)))
                .collect();
            let fields: Vec<String> = answer
                .columns
                .iter()
                .zip(&values)
                .map(|(c, v)| format!("{}:{v}", json_string(&c.name)))
                .collect();
            let value = format!(
                "{{\"op\":\"upsert\",\"table\":{},\"before\":null,\"after\":{{{}}}}}",
                json_string(&self.table),
                fields.join(",")
            );
            let pick = |n: Option<usize>| n.and_then(|n| values.get(n)).cloned();
            let (t, i) = (pick(track_at), pick(id_at));
            let key_part = i.clone().or_else(|| t.clone()).unwrap_or_default();
            let key_text = serde_json::from_str::<serde_json::Value>(&key_part)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or(key_part);
            messages.push(SourceMessage {
                key: Some(format!("{}/{key_text}", self.table).into_bytes()),
                value: Some(value.into_bytes()),
                headers: vec![("snowflake-table".to_owned(), self.table.clone().into_bytes())],
                ack: None,
            });
            last = match (t, i) {
                (Some(t), Some(i)) => Some(format!("[{t},{i}]")),
                (Some(t), None) => Some(format!("[{t}]")),
                _ => last,
            };
        }
        Ok(SourceBatch {
            messages,
            position: last,
        })
    }
}
