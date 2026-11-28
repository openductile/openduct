// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SQL databases in workflows: one step for each - `mysql` (MySQL and MariaDB), `sqlserver`, and `postgres` with
//! the same settings beside its SQL - and a trigger for a new or changed row of a table (see [`trigger`]).
//!
//! ```yaml
//! - name: order
//!   mysql:
//!     credential: shop-db
//!     sql: SELECT id, total FROM orders WHERE id = ?
//!     params: [value.id]
//!     read_only: true          # an agent's tool that may only read
//!     max_rows: 100
//!     timeout: 10s
//! - name: pay
//!   sqlserver:
//!     credential: books
//!     transaction: true        # all of them, or (one failing) none
//!     statements:
//!       - { sql: "UPDATE accounts SET balance = balance - @p1 WHERE id = @p2", params: [value.amount, value.from] }
//!       - { sql: "UPDATE accounts SET balance = balance + @p1 WHERE id = @p2", params: [value.amount, value.to] }
//! ```
//!
//! Each step names its database by its kind, because its SQL is written for that database: its placeholders (`?`,
//! `@p1`, `$1`), its quoting, its functions. Values from expressions go as parameters of their own types, never into
//! the SQL; SQL with `{{ }}` in it is refused when the workflow is read.

pub(crate) mod check;
mod live;
pub mod trigger;

use std::time::Duration;

use openduct_connect::sql::{Answer, Civil, Limits, Mode, Param, Statement};
use openduct_expr::{Compiled, Env, Value};
use serde_json::{Map, Value as Json, json};

use crate::apps::Credential;
use crate::run::Effects;

pub(crate) use live::run as run_live;

/// The databases a step reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Db {
    Postgres,
    Mysql,
    SqlServer,
}

impl Db {
    /// The step's (and the trigger's, and the credential's) kind.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::SqlServer => "sqlserver",
        }
    }

    /// The database by its step's kind.
    #[must_use]
    pub fn of(kind: &str) -> Option<Self> {
        match kind {
            "postgres" => Some(Self::Postgres),
            "mysql" => Some(Self::Mysql),
            "sqlserver" => Some(Self::SqlServer),
            _ => None,
        }
    }
}

/// The most statements one step runs, and the longest SQL of one.
const MAX_STATEMENTS: usize = 20;
const MAX_SQL: usize = 100 * 1024;
/// The most values one statement is given, and their bytes together.
const MAX_PARAMS: usize = 100;
const MAX_PARAM_BYTES: usize = 16 * 1024 * 1024;
/// The most rows a statement gives back: by default, and at most.
pub(crate) const ROWS: usize = 1_000;
pub(crate) const MOST_ROWS: usize = 10_000;
/// How long a step may take: by default, and at most.
const TIME: Duration = Duration::from_secs(30);
const MOST_TIME: Duration = Duration::from_secs(300);
const SETTINGS: &[&str] = &[
    "credential",
    "sql",
    "params",
    "statements",
    "transaction",
    "read_only",
    "max_rows",
    "timeout",
];
/// The settings only the newer form of a `postgres` step has.
const NEWER: &[&str] = &["statements", "transaction", "read_only", "max_rows", "timeout"];

/// One statement of a step: its SQL, and an expression for each of its values.
#[derive(Debug, Clone)]
pub struct StatementDef {
    pub sql: String,
    pub params: Vec<Compiled>,
}

/// A database step.
#[derive(Debug, Clone)]
pub struct Query {
    pub db: Db,
    pub credential: String,
    pub statements: Vec<StatementDef>,
    pub transaction: bool,
    /// Only reads: the SQL is checked to, and runs where the database keeps it from writing.
    pub read_only: bool,
    pub max_rows: usize,
    pub timeout: Duration,
}

/// What a database step asks of a run's effects.
#[derive(Debug, Clone)]
pub struct DatabaseRequest {
    pub db: Db,
    /// The database's credential, its secrets in it.
    pub credential: Credential,
    pub statements: Vec<Statement>,
    pub mode: Mode,
    pub limits: Limits,
}

/// Whether a `postgres` step uses the settings beyond its first ones (credential, sql, params).
pub(crate) fn newer(body: &Json) -> bool {
    body.as_object()
        .is_some_and(|s| s.keys().any(|k| NEWER.contains(&k.as_str())))
}

fn sql_of(v: Option<&Json>, db: Db, at: &str) -> Result<String, String> {
    let sql = v
        .and_then(Json::as_str)
        .filter(|q| !q.trim().is_empty())
        .ok_or_else(|| {
            format!(
                "{at}: the SQL to run, with {} where its values go",
                check::placeholder(db)
            )
        })?;
    if sql.len() > MAX_SQL {
        return Err(format!("{at}: at most {} KiB", MAX_SQL / 1024));
    }
    if sql.contains("{{") {
        return Err(format!(
            "{at}: values are not written into SQL: put {} where they go, and the values in params",
            check::placeholder(db)
        ));
    }
    Ok(sql.to_owned())
}

/// Reads a `mysql`, `sqlserver` or (newer) `postgres` step; `expression` compiles an expression of the workflow's
/// language.
pub(crate) fn parse(
    kind: &str,
    body: &Json,
    at: &str,
    expression: &dyn Fn(&Json, &str) -> Result<Compiled, String>,
) -> Result<Query, String> {
    let db = Db::of(kind).ok_or_else(|| format!("{at}: {kind:?} is not a database"))?;
    let s = body
        .as_object()
        .ok_or_else(|| format!("{at}: a set of settings"))?;
    if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
        return Err(format!(
            "{at}.{k}: not a setting of {kind} ({})",
            SETTINGS.join(", ")
        ));
    }
    let credential = s
        .get("credential")
        .and_then(Json::as_str)
        .map(str::trim)
        .filter(|c| !c.is_empty() && c.len() <= 100)
        .ok_or_else(|| format!("{at}.credential: the database's credential (a {kind} one)"))?
        .to_owned();
    let params_of = |v: Option<&Json>, at: &str| -> Result<Vec<Compiled>, String> {
        let list = match v {
            None | Some(Json::Null) => return Ok(Vec::new()),
            Some(Json::Array(items)) => items,
            Some(_) => {
                return Err(format!(
                    "{at}: a list of expressions, one for each of {} in order",
                    check::placeholder(db)
                ));
            }
        };
        if list.len() > MAX_PARAMS {
            return Err(format!("{at}: at most {MAX_PARAMS}"));
        }
        list.iter()
            .enumerate()
            .map(|(i, v)| expression(v, &format!("{at}[{i}]")))
            .collect()
    };
    let mut statements = Vec::new();
    match (s.get("sql"), s.get("statements")) {
        (Some(sql), None) => statements.push((
            sql_of(Some(sql), db, &format!("{at}.sql"))?,
            params_of(s.get("params"), &format!("{at}.params"))?,
            format!("{at}.sql"),
        )),
        (None, Some(Json::Array(list))) if !list.is_empty() && list.len() <= MAX_STATEMENTS => {
            if s.contains_key("params") {
                return Err(format!("{at}.params: each statement has its own"));
            }
            for (i, one) in list.iter().enumerate() {
                let place = format!("{at}.statements[{i}]");
                let o = one
                    .as_object()
                    .ok_or_else(|| format!("{place}: {{sql, params}}"))?;
                if let Some(k) = o.keys().find(|k| !matches!(k.as_str(), "sql" | "params")) {
                    return Err(format!("{place}.{k}: not a setting of a statement (sql, params)"));
                }
                statements.push((
                    sql_of(o.get("sql"), db, &format!("{place}.sql"))?,
                    params_of(o.get("params"), &format!("{place}.params"))?,
                    format!("{place}.sql"),
                ));
            }
        }
        (None, Some(_)) => {
            return Err(format!(
                "{at}.statements: a list of 1 to {MAX_STATEMENTS} statements, each {{sql, params}}"
            ));
        }
        (Some(_), Some(_)) => return Err(format!("{at}: sql or statements, not both")),
        (None, None) => return Err(format!("{at}.sql: the SQL to run")),
    }
    let flag = |name: &str| -> Result<bool, String> {
        match s.get(name) {
            None | Some(Json::Null) => Ok(false),
            Some(Json::Bool(b)) => Ok(*b),
            Some(_) => Err(format!("{at}.{name}: true or false")),
        }
    };
    let read_only = flag("read_only")?;
    let transaction = flag("transaction")?;
    for (sql, params, place) in &statements {
        let wanted = check::places(sql, db).map_err(|e| format!("{place}: {e}"))?;
        if wanted != params.len() {
            return Err(format!(
                "{place}: the SQL has {wanted} place(s) for values ({}) and params gives {}",
                check::placeholder(db),
                params.len()
            ));
        }
        if read_only {
            check::read_only(sql, db).map_err(|e| format!("{place}: read_only: {e}"))?;
        }
    }
    let max_rows = match s.get("max_rows") {
        None | Some(Json::Null) => ROWS,
        Some(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| (1..=MOST_ROWS).contains(n))
            .ok_or_else(|| format!("{at}.max_rows: how many rows at most, 1 to {MOST_ROWS}"))?,
    };
    let timeout = match s.get("timeout") {
        None | Some(Json::Null) => TIME,
        Some(t) => t
            .as_str()
            .and_then(crate::model::duration)
            .filter(|d| *d >= Duration::from_secs(1) && *d <= MOST_TIME)
            .ok_or_else(|| format!("{at}.timeout: how long it may take, as 30s or 2m: from 1s to 5m"))?,
    };
    Ok(Query {
        db,
        credential,
        statements: statements
            .into_iter()
            .map(|(sql, params, _)| StatementDef { sql, params })
            .collect(),
        transaction,
        read_only,
        max_rows,
        timeout,
    })
}

/// A value as a statement is given it, in its own type.
pub(crate) fn param_of(v: &Value) -> Param {
    match v {
        Value::Null => Param::Null,
        Value::Bool(b) => Param::Bool(*b),
        Value::Number(n) => {
            if n.fract().is_zero()
                && let Some(i) = rust_decimal::prelude::ToPrimitive::to_i64(n)
            {
                Param::Int(i)
            } else {
                Param::Decimal(n.normalize().to_string())
            }
        }
        Value::BigNumber(n) => Param::Decimal(n.to_string()),
        Value::Text(t) => Param::Text(t.to_string()),
        Value::Time(t) => {
            use chrono::{Datelike, Timelike};
            Param::Time(Civil {
                year: t.year(),
                month: u8::try_from(t.month()).unwrap_or(1),
                day: u8::try_from(t.day()).unwrap_or(1),
                hour: u8::try_from(t.hour()).unwrap_or(0),
                minute: u8::try_from(t.minute()).unwrap_or(0),
                second: u8::try_from(t.second()).unwrap_or(0),
                micros: t.timestamp_subsec_micros().min(999_999),
            })
        }
        Value::List(_) | Value::Record(_) => Param::Text(v.to_json()),
    }
}

/// A parameter as a test run shows it.
fn shown_param(p: &Param) -> Json {
    match p {
        Param::Null => Json::Null,
        Param::Bool(b) => Json::Bool(*b),
        Param::Int(n) => Json::from(*n),
        Param::Decimal(t) => openduct_connect::sql::number(t),
        Param::Text(t) => Json::String(t.clone()),
        Param::Time(t) => Json::String(format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            t.year, t.month, t.day, t.hour, t.minute, t.second, t.micros
        )),
    }
}

/// Runs a database step: its values worked out, its statements run through the run's effects.
pub(crate) async fn run(effects: &dyn Effects, q: &Query, env: &Env<'_>) -> Result<Value, String> {
    let credential = effects.credential(&q.credential)?;
    if !matches!(credential.kind.as_str(), "test") && credential.kind != q.db.name() {
        return Err(format!(
            "{} is a {} credential: a {} step takes a {} one",
            q.credential,
            credential.kind,
            q.db.name(),
            q.db.name()
        ));
    }
    let mut statements = Vec::with_capacity(q.statements.len());
    let mut bytes = 0usize;
    for (n, s) in q.statements.iter().enumerate() {
        let mut params = Vec::with_capacity(s.params.len());
        for (i, p) in s.params.iter().enumerate() {
            let value = p.evaluate(env).map_err(|e| {
                if q.statements.len() > 1 {
                    format!("statements[{n}].params[{i}]: {e}")
                } else {
                    format!("params[{i}]: {e}")
                }
            })?;
            let param = param_of(&value);
            bytes = bytes.saturating_add(param.size());
            params.push(param);
        }
        statements.push(Statement {
            sql: s.sql.clone(),
            params,
        });
    }
    if bytes > MAX_PARAM_BYTES {
        return Err(format!(
            "the values come to more than {} MiB",
            MAX_PARAM_BYTES / (1024 * 1024)
        ));
    }
    effects
        .database(DatabaseRequest {
            db: q.db,
            credential,
            statements,
            mode: Mode {
                transaction: q.transaction,
                read_only: q.read_only,
            },
            limits: Limits {
                max_rows: q.max_rows,
                max_bytes: crate::run::MAX_RESULT,
                timeout: q.timeout,
            },
        })
        .await
}

fn answer_json(a: &Answer) -> Json {
    let mut out = json!({
        "rows": a.rows,
        "count": a.rows.len(),
        "columns": a.columns,
        "affected": a.affected,
    });
    if let (Some(id), Some(fields)) = (a.insert_id, out.as_object_mut()) {
        fields.insert("insert_id".to_owned(), Json::from(id));
    }
    out
}

/// A step's result from its statements' answers: the rows of the last result with columns (or the last result),
/// the rows changed in all, and, when there was more than one result, each of them.
pub(crate) fn result_of(answers: &[Vec<Answer>]) -> Value {
    let all: Vec<&Answer> = answers.iter().flatten().collect();
    let main = all
        .iter()
        .rev()
        .find(|a| !a.columns.is_empty())
        .or_else(|| all.last())
        .copied();
    let mut out = main.map_or_else(|| answer_json(&Answer::default()), answer_json);
    let changed: Vec<u64> = all
        .iter()
        .filter(|a| a.columns.is_empty())
        .filter_map(|a| a.affected)
        .collect();
    if let Some(fields) = out.as_object_mut() {
        fields.insert(
            "affected".to_owned(),
            if changed.is_empty() {
                main.and_then(|m| m.affected).map_or(Json::Null, Json::from)
            } else {
                Json::from(changed.iter().sum::<u64>())
            },
        );
        if all.len() > 1 {
            fields.insert(
                "results".to_owned(),
                Json::Array(all.iter().map(|a| answer_json(a)).collect()),
            );
        }
    }
    Value::from_json(&out)
}

/// Where a request goes, as a test run shows it: the database's kind, host and name, never its secrets.
fn place(r: &DatabaseRequest) -> Json {
    let f = |k: &str| r.credential.fields.get(k).cloned();
    match r.db {
        Db::Postgres => f("url").map_or(Json::Null, Json::String),
        _ => {
            let host = f("host").unwrap_or_default();
            let port = f("port").unwrap_or_default();
            let db = f("database").unwrap_or_default();
            Json::String(format!(
                "{}://{host}{}{}",
                r.db.name(),
                if port.is_empty() {
                    String::new()
                } else {
                    format!(":{port}")
                },
                if db.is_empty() {
                    String::new()
                } else {
                    format!("/{db}")
                }
            ))
        }
    }
}

/// A request as a test run or a preview shows it, not run: `test` says which.
pub(crate) fn shown(test: &str, r: &DatabaseRequest) -> Value {
    let statement = |s: &Statement| json!({ "sql": s.sql, "params": s.params.iter().map(shown_param).collect::<Vec<_>>() });
    let mut out = Map::new();
    out.insert("test".to_owned(), Json::String(test.to_owned()));
    out.insert("database".to_owned(), place(r));
    match r.statements.as_slice() {
        [one] => {
            if let Json::Object(fields) = statement(one) {
                out.extend(fields);
            }
        }
        many => {
            out.insert(
                "statements".to_owned(),
                Json::Array(many.iter().map(statement).collect()),
            );
        }
    }
    if r.mode.transaction {
        out.insert("transaction".to_owned(), Json::Bool(true));
    }
    Value::from_json(&Json::Object(out))
}

/// A preview's database step: SQL that only reads runs for real, to read only; SQL that writes is shown.
pub(crate) async fn preview(
    live: &dyn Effects,
    mut request: DatabaseRequest,
    hide: &(dyn Fn(&str) -> String + Sync),
) -> Result<Value, String> {
    let writes = request
        .statements
        .iter()
        .any(|s| check::read_only(&s.sql, request.db).is_err());
    let not_written = |r: &DatabaseRequest| {
        let mut hidden = r.clone();
        for s in &mut hidden.statements {
            s.sql = hide(&s.sql);
            for p in &mut s.params {
                if let Param::Text(t) = p {
                    *t = hide(t);
                }
            }
        }
        Ok(shown("not written", &hidden))
    };
    if writes {
        return not_written(&request);
    }
    request.mode.read_only = true;
    let shown_if_writes = request.clone();
    match live.database(request).await {
        Err(e) if e == crate::run::WRITES => not_written(&shown_if_writes),
        other => other,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn read(kind: &str, body: Json) -> Result<Query, String> {
        parse(kind, &body, "steps[0]", &|v, at| {
            let source = v.as_str().map_or_else(|| v.to_string(), str::to_owned);
            openduct_expr::compile("cel", &source).map_err(|e| format!("{at}: {e}"))
        })
    }

    #[test]
    fn a_step_is_read_with_its_settings_and_checked() {
        let q = read(
            "mysql",
            json!({ "credential": "db", "sql": "SELECT * FROM t WHERE id = ?", "params": ["value.id"], "read_only": true, "max_rows": 5, "timeout": "10s" }),
        )
        .unwrap();
        assert_eq!(q.db, Db::Mysql);
        assert!(q.read_only);
        assert_eq!(q.max_rows, 5);
        assert_eq!(q.timeout, Duration::from_secs(10));
        let many = read(
            "sqlserver",
            json!({ "credential": "db", "transaction": true, "statements": [
                { "sql": "UPDATE a SET n = n - @p1 WHERE id = @p2", "params": ["1", "2"] },
                { "sql": "UPDATE a SET n = n + @p1 WHERE id = @p2", "params": ["1", "3"] },
            ]}),
        )
        .unwrap();
        assert_eq!(many.statements.len(), 2);
        assert!(many.transaction);
        for (kind, body, says) in [
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT * FROM t WHERE id = '{{ value.id }}'" }),
                "not written into SQL",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT ?, ?", "params": ["1"] }),
                "has 2 place(s)",
            ),
            (
                "sqlserver",
                json!({ "credential": "db", "sql": "SELECT @p1", "params": [] }),
                "has 1 place(s)",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "DELETE FROM t", "read_only": true }),
                "read_only",
            ),
            ("mysql", json!({ "sql": "SELECT 1" }), "credential"),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "max_rows": 0 }),
                "max_rows",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "max_rows": 10_001 }),
                "max_rows",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "timeout": "10m" }),
                "timeout",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "statements": [] }),
                "not both",
            ),
            (
                "mysql",
                json!({ "credential": "db", "statements": [] }),
                "statements",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "nope": 1 }),
                "not a setting",
            ),
            (
                "mysql",
                json!({ "credential": "db", "sql": "SELECT 1", "read_only": "yes" }),
                "true or false",
            ),
        ] {
            let e = read(kind, body.clone()).unwrap_err();
            assert!(e.contains(says), "{body}: {e}");
        }
        assert!(newer(
            &json!({ "credential": "db", "sql": "SELECT 1", "read_only": true })
        ));
        assert!(!newer(
            &json!({ "credential": "db", "sql": "SELECT 1", "params": [] })
        ));
    }

    #[test]
    fn values_go_in_their_own_types() {
        let v = |j: &str| param_of(&Value::parse_json(j).unwrap());
        assert_eq!(v("42"), Param::Int(42));
        assert_eq!(v("-7"), Param::Int(-7));
        assert_eq!(v("19.90"), Param::Decimal("19.9".into()));
        assert_eq!(v("true"), Param::Bool(true));
        assert_eq!(v("null"), Param::Null);
        assert_eq!(
            v("\"x'); DROP TABLE t; --\""),
            Param::Text("x'); DROP TABLE t; --".into())
        );
        assert_eq!(v("[1,2]"), Param::Text("[1,2]".into()));
        assert_eq!(v("{\"a\":1}"), Param::Text("{\"a\":1}".into()));
        assert_eq!(
            v("123456789012345678901234567890"),
            Param::Decimal("123456789012345678901234567890".into())
        );
        let t = Value::Time(
            chrono::DateTime::parse_from_rfc3339("2026-11-27T09:30:05.25Z")
                .unwrap()
                .to_utc(),
        );
        assert_eq!(
            param_of(&t),
            Param::Time(Civil {
                year: 2026,
                month: 11,
                day: 27,
                hour: 9,
                minute: 30,
                second: 5,
                micros: 250_000
            })
        );
    }

    #[test]
    fn the_result_is_the_last_rows_and_what_changed() {
        let rows = Answer {
            columns: vec!["id".into()],
            rows: vec![[("id".to_owned(), Json::from(1))].into_iter().collect()],
            ..Answer::default()
        };
        let wrote = |n| Answer {
            affected: Some(n),
            ..Answer::default()
        };
        let one: Json = serde_json::from_str(&result_of(&[vec![rows.clone()]]).to_json()).unwrap();
        assert_eq!(
            one,
            json!({ "rows": [{ "id": 1 }], "count": 1, "columns": ["id"], "affected": null })
        );
        let many: Json =
            serde_json::from_str(&result_of(&[vec![wrote(2)], vec![wrote(3)], vec![rows]]).to_json())
                .unwrap();
        assert_eq!(many["rows"], json!([{ "id": 1 }]));
        assert_eq!(many["affected"], 5);
        assert_eq!(many["results"].as_array().unwrap().len(), 3);
        let insert: Json = serde_json::from_str(
            &result_of(&[vec![Answer {
                affected: Some(1),
                insert_id: Some(9),
                ..Answer::default()
            }]])
            .to_json(),
        )
        .unwrap();
        assert_eq!(
            insert,
            json!({ "rows": [], "count": 0, "columns": [], "affected": 1, "insert_id": 9 })
        );
    }
}
