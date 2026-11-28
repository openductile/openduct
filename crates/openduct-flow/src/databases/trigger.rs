// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow started by a table's new or changed rows: polled every so often, each row after the last one seen
//! by a cursor column the person names - an id that grows, or a time a row is changed at - taken oldest first, a
//! run for each.
//!
//! ```yaml
//! trigger:
//!   mysql: { credential: shop-db, table: orders, cursor: id, every: 1m }
//! trigger:
//!   sqlserver: { credential: books, table: dbo.invoices, cursor: updated_at, key: id, where: "status = 'paid'" }
//! ```
//!
//! An id cursor is taken as unique: each poll asks for rows past the last. A time cursor can repeat - two rows
//! changed in the same instant - so with `key` (the rows' own key) a poll asks from the last time on, and leaves out
//! the rows at that time it has run already. Without `key`, rows that share the last time a poll saw are not run.
//! The first poll takes nothing: it notes where the table is, so a trigger starts from when it is turned on.
//!
//! The polls run as the app triggers' do (see `runner::polling`): queued by every node, claimed by one, what they
//! saw kept on the log beside the runs they start, in one write - once across a cluster, and through a restart.

use std::collections::HashSet;
use std::time::Duration;

use openduct_connect::sql::{Limits, Mode, Param, Statement};
use openduct_expr::Value;
use serde_json::{Map, Value as Json};

use super::{DatabaseRequest, Db, check};
use crate::app_files::poll::{MOST_SEEN, Polled, Seen};
use crate::run::Effects;

/// How often a trigger polls: by default, and its bounds.
const EVERY: Duration = Duration::from_secs(60);
const LEAST: Duration = Duration::from_secs(5);
const MOST: Duration = Duration::from_secs(86_400);
/// The most rows one poll takes: by default, and at most.
const ROWS: u32 = 50;
const MOST_ROWS: u32 = 500;
/// How long a poll's query may take.
const QUERY_TIME: Duration = Duration::from_secs(60);
/// The longest condition.
const MOST_WHERE: usize = 4096;
const SETTINGS: &[&str] = &[
    "credential",
    "table",
    "cursor",
    "key",
    "where",
    "every",
    "max_rows",
];

/// A trigger of a table's new or changed rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowsTrigger {
    pub db: Db,
    pub credential: String,
    /// `table`, or `schema.table` (`database.schema.table` on SQL Server).
    pub table: String,
    /// The column rows are taken after: an id, or the time a row changed.
    pub cursor: String,
    /// The rows' own key, for a cursor that can repeat.
    pub key: Option<String>,
    /// Only the rows this SQL condition holds for.
    pub condition: Option<String>,
    pub every: Duration,
    pub max_rows: u32,
}

/// A name a database keeps as it is: 1 to 128 letters, digits, `_` and `$`.
fn name(text: &str) -> bool {
    (1..=128).contains(&text.len())
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
        && !text.bytes().all(|b| b.is_ascii_digit())
}

/// Reads a database trigger, as a workflow's trigger.
pub(crate) fn read(kind: &str, s: &Map<String, Json>) -> Result<crate::model::Trigger, String> {
    Ok(crate::model::Trigger::Rows(Box::new(parse(kind, s)?)))
}

/// Reads a database trigger: `mysql: {credential, table, cursor, ...}`.
pub(crate) fn parse(kind: &str, s: &Map<String, Json>) -> Result<RowsTrigger, String> {
    let db = Db::of(kind).ok_or_else(|| format!("{kind:?} is not a database"))?;
    if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
        return Err(format!(
            "{kind}.{k}: not a setting of {kind} ({})",
            SETTINGS.join(", ")
        ));
    }
    let text = |k: &str| {
        s.get(k)
            .and_then(Json::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
    };
    let credential = text("credential")
        .filter(|c| c.len() <= 100)
        .ok_or_else(|| format!("{kind}.credential: the database's credential (a {kind} one)"))?
        .to_owned();
    let most_parts = if db == Db::SqlServer { 3 } else { 2 };
    let table = text("table")
        .filter(|t| {
            let parts: Vec<&str> = t.split('.').collect();
            parts.len() <= most_parts && parts.iter().all(|p| name(p))
        })
        .ok_or_else(|| {
            format!("{kind}.table: the table, as orders or schema.orders (letters, digits, _ and $)")
        })?
        .to_owned();
    let column = |k: &str, what: &str| -> Result<Option<String>, String> {
        match text(k) {
            None => Ok(None),
            Some(c) if name(c) => Ok(Some(c.to_owned())),
            Some(_) => Err(format!(
                "{kind}.{k}: {what}, a column's name (letters, digits, _ and $)"
            )),
        }
    };
    let cursor = column(
        "cursor",
        "the column rows are taken after - an id that grows, or the time a row changed",
    )?
    .ok_or_else(|| {
        format!(
            "{kind}.cursor: the column rows are taken after - an id that grows, or the time a row changed"
        )
    })?;
    let key = column("key", "the rows' own key")?;
    let condition = match text("where") {
        None => None,
        Some(w) => {
            if w.len() > MOST_WHERE || w.contains("{{") {
                return Err(format!(
                    "{kind}.where: an SQL condition of at most {MOST_WHERE} bytes, with no {{{{ }}}}"
                ));
            }
            let whole = format!("SELECT 1 FROM t WHERE {w}");
            check::read_only(&whole, db).map_err(|e| format!("{kind}.where: {e}"))?;
            if check::places(&whole, db)? != 0 {
                return Err(format!(
                    "{kind}.where: a condition with no values to give ({})",
                    check::placeholder(db)
                ));
            }
            Some(w.to_owned())
        }
    };
    let every = match s.get("every") {
        None | Some(Json::Null) => EVERY,
        Some(e) => e
            .as_str()
            .and_then(crate::model::duration)
            .filter(|d| *d >= LEAST && *d <= MOST)
            .ok_or_else(|| format!("{kind}.every: how often it asks, as 1m or 1h: from 5s to 1d"))?,
    };
    let max_rows = match s.get("max_rows") {
        None | Some(Json::Null) => ROWS,
        Some(n) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=MOST_ROWS).contains(n))
            .ok_or_else(|| format!("{kind}.max_rows: how many rows a poll runs at most, 1 to {MOST_ROWS}"))?,
    };
    Ok(RowsTrigger {
        db,
        credential,
        table,
        cursor,
        key,
        condition,
        every,
        max_rows,
    })
}

impl RowsTrigger {
    fn quote(&self, name: &str) -> String {
        name.split('.')
            .map(|part| match self.db {
                Db::Mysql => format!("`{part}`"),
                Db::Postgres => format!("\"{part}\""),
                Db::SqlServer => format!("[{part}]"),
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    fn place(&self) -> &'static str {
        match self.db {
            Db::Mysql => "?",
            Db::Postgres => "$1",
            Db::SqlServer => "@p1",
        }
    }

    /// `SELECT [TOP (n)] what FROM table WHERE ... ORDER BY ... [LIMIT n]`.
    fn select(&self, what: &str, condition: &str, order: Option<&str>, limit: Option<usize>) -> String {
        let top = match (self.db, limit) {
            (Db::SqlServer, Some(n)) => format!("TOP ({n}) "),
            _ => String::new(),
        };
        let and = self
            .condition
            .as_ref()
            .map(|w| format!(" AND ({w})"))
            .unwrap_or_default();
        let mut sql = format!(
            "SELECT {top}{what} FROM {} WHERE {condition}{and}",
            self.quote(&self.table)
        );
        if let Some(order) = order {
            sql.push_str(&format!(" ORDER BY {order}"));
        }
        if let (false, Some(n)) = (self.db == Db::SqlServer, limit) {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }

    /// Where the table is now: its last cursor.
    pub(crate) fn first_sql(&self) -> String {
        let c = self.quote(&self.cursor);
        self.select(&format!("MAX({c}) AS openduct_last"), "1 = 1", None, None)
    }

    /// The keys of the rows at the last cursor.
    pub(crate) fn keys_sql(&self, key: &str) -> String {
        let (c, k) = (self.quote(&self.cursor), self.quote(key));
        self.select(
            &format!("{k} AS openduct_key"),
            &format!("{c} = {}", self.place()),
            None,
            Some(MOST_SEEN),
        )
    }

    /// The rows after what was seen, oldest first, and how many it asks for.
    pub(crate) fn next_sql(&self, seen: &Seen) -> (String, Vec<Param>, usize) {
        let c = self.quote(&self.cursor);
        let ties = if self.key.is_some() { seen.items.len() } else { 0 };
        let limit = self.max_rows as usize + ties;
        let (condition, params) = if seen.cursor.is_null() {
            (format!("{c} IS NOT NULL"), Vec::new())
        } else if ties > 0 {
            (
                format!("{c} >= {}", self.place()),
                vec![cursor_param(&seen.cursor)],
            )
        } else {
            (
                format!("{c} > {}", self.place()),
                vec![cursor_param(&seen.cursor)],
            )
        };
        let order = match &self.key {
            Some(k) => format!("{c}, {}", self.quote(k)),
            None => c.clone(),
        };
        (
            self.select("*", &condition, Some(&order), Some(limit)),
            params,
            limit,
        )
    }
}

/// A cursor's value given back to the database, in its type.
pub(crate) fn cursor_param(v: &Json) -> Param {
    match v {
        Json::Number(n) => n
            .as_i64()
            .map_or_else(|| Param::Decimal(n.to_string()), Param::Int),
        Json::String(s) => Param::Text(s.clone()),
        Json::Bool(b) => Param::Bool(*b),
        Json::Null => Param::Null,
        other => Param::Text(other.to_string()),
    }
}

/// A row's column, by its name in any case (PostgreSQL keeps unquoted names in small letters).
fn column<'a>(row: &'a Map<String, Json>, name: &str) -> Option<&'a Json> {
    row.get(name).or_else(|| {
        row.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

fn text_of(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A row taken, by its id.
type Taken = (String, Map<String, Json>);

/// The rows a poll takes of those it was given (oldest first), and what is seen after them.
pub(crate) fn take(
    t: &RowsTrigger,
    before: &Seen,
    rows: Vec<Map<String, Json>>,
) -> Result<(Vec<Taken>, Seen), String> {
    let seen_keys: HashSet<&str> = before.items.iter().map(|(k, _)| k.as_str()).collect();
    let mut taken: Vec<(String, Json, Map<String, Json>)> = Vec::new();
    for row in rows {
        let cursor = column(&row, &t.cursor).cloned().unwrap_or(Json::Null);
        if cursor.is_null() {
            return Err(format!(
                "the rows have no column {} (the cursor), or it is empty",
                t.cursor
            ));
        }
        let id = match &t.key {
            Some(k) => {
                text_of(column(&row, k).ok_or_else(|| format!("the rows have no column {k} (the key)"))?)
            }
            None => text_of(&cursor),
        };
        if t.key.is_some() && cursor == before.cursor && seen_keys.contains(id.as_str()) {
            continue;
        }
        taken.push((id, cursor, row));
        if taken.len() >= t.max_rows as usize {
            break;
        }
    }
    let mut seen = Seen {
        cursor: before.cursor.clone(),
        items: before.items.clone(),
        ..Seen::default()
    };
    if let Some((_, last, _)) = taken.last() {
        let mut items = if *last == before.cursor {
            before.items.clone()
        } else {
            Vec::new()
        };
        if t.key.is_some() {
            items.extend(
                taken
                    .iter()
                    .filter(|(_, c, _)| c == last)
                    .map(|(id, _, _)| (id.clone(), String::new())),
            );
        }
        let over = items.len().saturating_sub(MOST_SEEN);
        items.drain(..over);
        seen.cursor = last.clone();
        seen.items = items;
    }
    Ok((taken.into_iter().map(|(id, _, row)| (id, row)).collect(), seen))
}

/// One query of a poll: its rows.
async fn ask(
    effects: &dyn Effects,
    t: &RowsTrigger,
    sql: String,
    params: Vec<Param>,
    most: usize,
) -> Result<Vec<Map<String, Json>>, String> {
    let credential = effects.credential(&t.credential)?;
    if credential.kind != "test" && credential.kind != t.db.name() {
        return Err(format!(
            "{} is a {} credential: a {} trigger takes a {} one",
            t.credential,
            credential.kind,
            t.db.name(),
            t.db.name()
        ));
    }
    let answer = effects
        .database(DatabaseRequest {
            db: t.db,
            credential,
            statements: vec![Statement { sql, params }],
            mode: Mode {
                transaction: false,
                read_only: true,
            },
            limits: Limits {
                max_rows: most.max(1),
                max_bytes: crate::run::MAX_RESULT,
                timeout: QUERY_TIME,
            },
        })
        .await?;
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    Ok(json
        .get("rows")
        .and_then(Json::as_array)
        .map(|rows| rows.iter().filter_map(|r| r.as_object().cloned()).collect())
        .unwrap_or_default())
}

/// One poll of `t`, after what the polls before saw (none: the first, which notes where the table is and takes
/// nothing).
pub async fn poll(effects: &dyn Effects, t: &RowsTrigger, before: Option<&Seen>) -> Result<Polled, String> {
    let Some(before) = before else {
        let rows = ask(effects, t, t.first_sql(), Vec::new(), 1).await?;
        let last = rows
            .first()
            .and_then(|r| column(r, "openduct_last"))
            .cloned()
            .unwrap_or(Json::Null);
        let mut items = Vec::new();
        if let (Some(key), false) = (&t.key, last.is_null()) {
            let keys = ask(effects, t, t.keys_sql(key), vec![cursor_param(&last)], MOST_SEEN).await?;
            items = keys
                .iter()
                .filter_map(|r| column(r, "openduct_key"))
                .map(|k| (text_of(k), String::new()))
                .collect();
        }
        return Ok(Polled {
            runs: Vec::new(),
            seen: Seen {
                cursor: last,
                items,
                ..Seen::default()
            },
        });
    };
    let (sql, params, most) = t.next_sql(before);
    let rows = ask(effects, t, sql, params, most).await?;
    let (taken, seen) = take(t, before, rows)?;
    Ok(Polled {
        runs: taken
            .into_iter()
            .map(|(id, row)| (id, Value::from_json(&Json::Object(row))))
            .collect(),
        seen,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use serde_json::json;

    use super::*;

    fn trigger(kind: &str, s: Json) -> Result<RowsTrigger, String> {
        parse(kind, s.as_object().unwrap())
    }

    fn rows(list: Json) -> Vec<Map<String, Json>> {
        list.as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_object().unwrap().clone())
            .collect()
    }

    #[test]
    fn a_trigger_is_read_and_its_names_checked() {
        let t = trigger(
            "sqlserver",
            json!({ "credential": "db", "table": "dbo.orders", "cursor": "updated_at", "key": "id", "where": "status = 'paid'", "every": "30s", "max_rows": 10 }),
        )
        .unwrap();
        assert_eq!(t.every, Duration::from_secs(30));
        assert_eq!(t.key.as_deref(), Some("id"));
        for (kind, s, says) in [
            ("mysql", json!({ "credential": "db", "cursor": "id" }), "table"),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders; DROP TABLE x", "cursor": "id" }),
                "table",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "a.b.c", "cursor": "id" }),
                "table",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id`" }),
                "cursor",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders" }),
                "cursor",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id", "where": "1 = 1; DELETE FROM t" }),
                "where",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id", "where": "id > ?" }),
                "no values",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id", "every": "1s" }),
                "every",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id", "max_rows": 501 }),
                "max_rows",
            ),
            (
                "mysql",
                json!({ "table": "orders", "cursor": "id" }),
                "credential",
            ),
            (
                "mysql",
                json!({ "credential": "db", "table": "orders", "cursor": "id", "sql": "x" }),
                "not a setting",
            ),
        ] {
            let e = trigger(kind, s.clone()).unwrap_err();
            assert!(e.contains(says), "{s}: {e}");
        }
    }

    #[test]
    fn each_database_is_asked_in_its_own_sql() {
        let my = trigger(
            "mysql",
            json!({ "credential": "db", "table": "shop.orders", "cursor": "id" }),
        )
        .unwrap();
        assert_eq!(
            my.first_sql(),
            "SELECT MAX(`id`) AS openduct_last FROM `shop`.`orders` WHERE 1 = 1"
        );
        let seen = Seen {
            cursor: json!(41),
            ..Seen::default()
        };
        let (sql, params, limit) = my.next_sql(&seen);
        assert_eq!(
            sql,
            "SELECT * FROM `shop`.`orders` WHERE `id` > ? ORDER BY `id` LIMIT 50"
        );
        assert_eq!(params, vec![Param::Int(41)]);
        assert_eq!(limit, 50);
        let (sql, params, _) = my.next_sql(&Seen::default());
        assert_eq!(
            sql,
            "SELECT * FROM `shop`.`orders` WHERE `id` IS NOT NULL ORDER BY `id` LIMIT 50"
        );
        assert!(params.is_empty());
        let ms = trigger(
            "sqlserver",
            json!({ "credential": "db", "table": "dbo.invoices", "cursor": "updated_at", "key": "id", "where": "status = 'paid'", "max_rows": 10 }),
        )
        .unwrap();
        let seen = Seen {
            cursor: json!("2026-11-27T09:30:00.0000000"),
            items: vec![("7".into(), String::new())],
            ..Seen::default()
        };
        let (sql, params, limit) = ms.next_sql(&seen);
        assert_eq!(
            sql,
            "SELECT TOP (11) * FROM [dbo].[invoices] WHERE [updated_at] >= @p1 AND (status = 'paid') ORDER BY \
             [updated_at], [id]"
        );
        assert_eq!(params, vec![Param::Text("2026-11-27T09:30:00.0000000".into())]);
        assert_eq!(limit, 11);
        assert_eq!(
            ms.keys_sql("id"),
            "SELECT TOP (5000) [id] AS openduct_key FROM [dbo].[invoices] WHERE [updated_at] = @p1 AND (status = 'paid')"
        );
        let pg = trigger(
            "postgres",
            json!({ "credential": "db", "table": "orders", "cursor": "id" }),
        )
        .unwrap();
        assert_eq!(
            pg.next_sql(&Seen {
                cursor: json!(3),
                ..Seen::default()
            })
            .0,
            "SELECT * FROM \"orders\" WHERE \"id\" > $1 ORDER BY \"id\" LIMIT 50"
        );
    }

    #[test]
    fn rows_at_a_time_seen_already_are_not_taken_again() {
        let t = trigger(
            "mysql",
            json!({ "credential": "db", "table": "orders", "cursor": "updated_at", "key": "id", "max_rows": 2 }),
        )
        .unwrap();
        let before = Seen {
            cursor: json!("10:00"),
            items: vec![("1".into(), String::new())],
            ..Seen::default()
        };
        // Row 1 at 10:00 ran already; 2 is new at the same time; 3 and 4 later (one poll takes two).
        let (taken, seen) = take(
            &t,
            &before,
            rows(json!([
                { "id": 1, "updated_at": "10:00" },
                { "id": 2, "updated_at": "10:00" },
                { "id": 3, "updated_at": "10:05" },
                { "id": 4, "updated_at": "10:05" },
            ])),
        )
        .unwrap();
        let ids: Vec<&str> = taken.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, ["2", "3"]);
        assert_eq!(seen.cursor, json!("10:05"));
        assert_eq!(seen.items, vec![("3".to_owned(), String::new())]);
        // The next poll asks from 10:05 on, and leaves 3 out.
        let (taken, seen) = take(
            &t,
            &seen,
            rows(json!([{ "id": 3, "updated_at": "10:05" }, { "id": 4, "updated_at": "10:05" }])),
        )
        .unwrap();
        assert_eq!(taken[0].0, "4");
        assert_eq!(seen.items.len(), 2);
        // Nothing new: what was seen stays.
        let (taken, after) = take(&t, &seen, rows(json!([{ "id": 3, "updated_at": "10:05" }]))).unwrap();
        assert!(taken.is_empty());
        assert_eq!(after.cursor, seen.cursor);
        assert_eq!(after.items, seen.items);
        // A cursor the rows do not have is said plainly.
        let e = take(&t, &seen, rows(json!([{ "id": 9 }]))).unwrap_err();
        assert!(e.contains("no column updated_at"), "{e}");
    }

    #[test]
    fn an_id_cursor_takes_rows_past_the_last_one() {
        let t = trigger(
            "mysql",
            json!({ "credential": "db", "table": "orders", "cursor": "ID" }),
        )
        .unwrap();
        let (taken, seen) = take(
            &t,
            &Seen {
                cursor: json!(5),
                ..Seen::default()
            },
            rows(json!([{ "id": 6, "total": 9.5 }, { "id": 7, "total": 1 }])),
        )
        .unwrap();
        assert_eq!(taken.len(), 2);
        assert_eq!(taken[1].0, "7");
        assert_eq!(seen.cursor, json!(7));
        assert!(seen.items.is_empty());
    }
}
