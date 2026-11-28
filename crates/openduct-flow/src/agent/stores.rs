// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Where an agent's memory is kept, beside the broker (the default, [`super::LogMemory`]): a PostgreSQL table, Redis
//! keys, or an OpenDuct data table - named by the step's `memory.store`.
//!
//! ```yaml
//! memory:
//!   key: "{{ value.chat_id }}"
//!   window: 10
//!   forget_after: 7d
//!   store: postgres          # or redis, or table
//!   credential: app-db       # postgres and redis
//!   table: agent_memory      # postgres (with a schema: ai.agent_memory) and table
//!   create: true             # postgres: the table made when it is not there
//! ```
//!
//! Every store keeps the same thing under the same key as the broker does - the conversation's last turns, as JSON,
//! under `w.<workspace>.<workflow>/<step>/<key>`, so two workspaces sharing a database never share a memory - with the
//! same window and the same clear rule (both applied before a store is reached), and the same time to forget: a
//! PostgreSQL row's `forget_at` (rows past it are not read, and a few are deleted on each write), a Redis key's TTL,
//! a data table row's `forget_at`. A test run reads nothing and keeps nothing; a preview reads, and shows what it
//! would keep.
//!
//! - **postgres**: `(key text PRIMARY KEY, turns jsonb, forget_at timestamptz, updated_at timestamptz)`, made with
//!   an index on `forget_at` when `create: true`; reached through the run's SQL effect.
//! - **redis**: the key `openduct:memory:<key>`, its value the turns as JSON, set with a TTL; reached as the `redis`
//!   step reaches its server.
//! - **table**: a data table with the columns `key` (text), `turns` (JSON) and `forget_at` (date and time), which the
//!   studio offers to make; a row holds at most 64 KiB, so the oldest turns are let go to keep within it.

use std::sync::Arc;
use std::time::Duration;

use openduct_expr::{Record, Value};
use serde_json::{Map, Value as Json, json};

use crate::apps::Credential;
use crate::run::{Effects, SqlRequest};
use crate::tables::{Access, Changes, ColumnType, MAX_ROW_BYTES, Row, TableData};

/// The settings of `memory` that name its store.
pub(crate) const SETTINGS: &[&str] = &["store", "credential", "table", "create"];
/// The most conversations past their time one write forgets on the way.
const SWEEP: usize = 16;
/// The columns a data table that keeps memories has.
pub const TABLE_COLUMNS: [(&str, ColumnType); 3] = [
    ("key", ColumnType::Text),
    ("turns", ColumnType::Json),
    ("forget_at", ColumnType::DateTime),
];

/// Where an agent's memory is kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryStore {
    /// On the broker: the default.
    Broker,
    /// A table of a PostgreSQL database.
    Postgres {
        credential: String,
        schema: Option<String>,
        table: String,
        create: bool,
    },
    /// Keys of a Redis server.
    Redis { credential: String },
    /// A data table of the workspace's.
    Table { table: String },
}

fn text<'a>(s: &'a Map<String, Json>, name: &str) -> Option<&'a str> {
    s.get(name)
        .and_then(Json::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
}

/// The store a step's `memory` names, read when the workflow is.
pub(crate) fn read(s: &Map<String, Json>, at: &str) -> Result<MemoryStore, String> {
    let store = text(s, "store").unwrap_or("broker");
    let credential = text(s, "credential").map(str::to_owned);
    let table = text(s, "table");
    let create = match s.get("create") {
        None | Some(Json::Null) => false,
        Some(Json::Bool(b)) => *b,
        Some(_) => return Err(format!("{at}.create: true or false")),
    };
    let none_of = |names: &[&str]| -> Result<(), String> {
        match names.iter().find(|n| s.get(**n).is_some_and(|v| !v.is_null())) {
            Some(n) => Err(format!("{at}.{n}: not a setting of a memory kept in {store}")),
            None => Ok(()),
        }
    };
    let needs = |c: Option<String>, what: &str| c.ok_or_else(|| format!("{at}.credential: {what}"));
    Ok(match store {
        "broker" => {
            none_of(&["credential", "table", "create"])?;
            MemoryStore::Broker
        }
        "postgres" => {
            let written = table.unwrap_or("agent_memory");
            let (schema, table) = crate::retrieval::table_name(written, &format!("{at}.table"))?;
            MemoryStore::Postgres {
                credential: needs(credential, "the PostgreSQL database's credential")?,
                schema,
                table,
                create,
            }
        }
        "redis" => {
            none_of(&["table", "create"])?;
            MemoryStore::Redis {
                credential: needs(credential, "the Redis server's credential")?,
            }
        }
        "table" => {
            none_of(&["credential", "create"])?;
            let name =
                table.ok_or_else(|| format!("{at}.table: the data table its conversations are kept in"))?;
            if let Some(why) = crate::tables::name_problem(name) {
                return Err(format!("{at}.table: {why}"));
            }
            MemoryStore::Table {
                table: name.to_owned(),
            }
        }
        other => {
            return Err(format!(
                "{at}.store: broker, postgres, redis or table, not {other:?}"
            ));
        }
    })
}

/// The data table an agent step's memory is kept in, with the columns it needs: what the studio offers to make.
pub(crate) fn memory_table(step: &crate::model::Step) -> Option<(String, Vec<crate::tables::Column>)> {
    let crate::model::Kind::Agent(agent) = &step.kind else {
        return None;
    };
    let MemoryStore::Table { table } = &agent.memory.as_ref()?.store else {
        return None;
    };
    let columns = TABLE_COLUMNS
        .iter()
        .map(|(name, kind)| crate::tables::Column {
            name: (*name).to_owned(),
            kind: *kind,
        })
        .collect();
    Some((table.clone(), columns))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

fn shown(v: &Value) -> bool {
    matches!(v, Value::Record(r) if r.get("test").is_some())
}

/// The conversation under `key` (`<workflow>/<step>/<key>`), from the broker or the step's own store.
pub(crate) async fn recall_in(
    effects: &dyn Effects,
    store: &MemoryStore,
    key: &str,
) -> Result<Value, String> {
    match store {
        MemoryStore::Broker => effects.recall(key).await,
        other => recall(effects, other, key).await,
    }
}

/// Keeps (or, with no turns, forgets) the conversation under `key`, on the broker or in the step's own store.
pub(crate) async fn keep_in(
    effects: &dyn Effects,
    store: &MemoryStore,
    key: &str,
    turns: Option<Value>,
    keep: Duration,
) -> Result<Value, String> {
    match store {
        MemoryStore::Broker => effects.remember(key, turns, keep).await,
        other => remember(effects, other, key, turns, keep).await,
    }
}

/// The conversation `key` (as the broker names it: `<workflow>/<step>/<key>`) holds in `store`: its turns, or null.
pub(crate) async fn recall(effects: &dyn Effects, store: &MemoryStore, key: &str) -> Result<Value, String> {
    let key = format!("{}{key}", effects.memory_prefix());
    match store {
        MemoryStore::Broker => Err("the broker's memory is reached through the run's effects".to_owned()),
        MemoryStore::Postgres { .. } => Pg::of(effects, store)?.recall(&key).await,
        MemoryStore::Redis { credential } => redis_recall(effects, credential, &key).await,
        MemoryStore::Table { table } => table_recall(effects, table, &key).await,
    }
}

/// Keeps `turns` under `key` in `store` until `keep` from now; with none, forgets it. A test run's (or a preview's)
/// answer says it was not kept.
pub(crate) async fn remember(
    effects: &dyn Effects,
    store: &MemoryStore,
    key: &str,
    turns: Option<Value>,
    keep: Duration,
) -> Result<Value, String> {
    let full = format!("{}{key}", effects.memory_prefix());
    let answer = match store {
        MemoryStore::Broker => {
            return Err("the broker's memory is reached through the run's effects".to_owned());
        }
        MemoryStore::Postgres { .. } => {
            Pg::of(effects, store)?
                .remember(&full, turns.as_ref(), keep)
                .await?
        }
        MemoryStore::Redis { credential } => {
            redis_remember(effects, credential, &full, turns.as_ref(), keep).await?
        }
        MemoryStore::Table { table } => table_remember(effects, table, &full, turns.as_ref(), keep).await?,
    };
    Ok(match answer {
        Some(v) => v,
        None => super::not_kept(key, turns.as_ref()),
    })
}

/// The turns a kept JSON value holds: its `turns`, or the list itself.
fn turns_of(kept: &Json) -> Value {
    match kept {
        Json::Object(o) => Value::from_json(o.get("turns").unwrap_or(&Json::Null)),
        Json::Array(_) => Value::from_json(kept),
        _ => Value::Null,
    }
}

// --- PostgreSQL ---

struct Pg<'a> {
    effects: &'a dyn Effects,
    db: Credential,
    table: String,
    /// The forget_at index's name, in the table's schema.
    index: String,
    present: String,
    create: bool,
}

impl<'a> Pg<'a> {
    fn of(effects: &'a dyn Effects, store: &MemoryStore) -> Result<Self, String> {
        let MemoryStore::Postgres {
            credential,
            schema,
            table,
            create,
        } = store
        else {
            return Err("not a PostgreSQL memory".to_owned());
        };
        let db = effects.credential(credential)?;
        if db.kind != "postgres" && db.kind != "test" {
            return Err(format!(
                "{credential} is a {} credential: a memory kept in PostgreSQL takes a postgres one",
                db.kind
            ));
        }
        // The names were checked when the workflow was read: small letters, digits and _.
        let quoted = match schema {
            Some(s) => format!("\"{s}\".\"{table}\""),
            None => format!("\"{table}\""),
        };
        Ok(Self {
            effects,
            db,
            present: quoted.clone(),
            table: quoted,
            index: format!("\"{table}_forget_at_idx\""),
            create: *create,
        })
    }

    async fn sql(&self, sql: String, params: Vec<Option<String>>) -> Result<Value, String> {
        self.effects
            .sql(SqlRequest {
                credential: self.db.clone(),
                sql,
                params,
                read_only: false,
            })
            .await
    }

    async fn recall(&self, key: &str) -> Result<Value, String> {
        if self.create {
            let there = self
                .sql(
                    "SELECT to_regclass($1)::text AS kept".to_owned(),
                    vec![Some(self.present.clone())],
                )
                .await?;
            if shown(&there) || first_text(&there, "kept").is_none() {
                return Ok(Value::Null);
            }
        }
        let answer = self
            .sql(
                format!(
                    "SELECT turns::text AS turns FROM {} WHERE key = $1 AND forget_at > now()",
                    self.table
                ),
                vec![Some(key.to_owned())],
            )
            .await?;
        if shown(&answer) {
            return Ok(Value::Null);
        }
        Ok(first_text(&answer, "turns")
            .and_then(|t| serde_json::from_str::<Json>(&t).ok())
            .map_or(Value::Null, |j| turns_of(&j)))
    }

    /// What it kept (none: only shown).
    async fn remember(
        &self,
        key: &str,
        turns: Option<&Value>,
        keep: Duration,
    ) -> Result<Option<Value>, String> {
        let Some(turns) = turns else {
            let answer = self
                .sql(
                    format!("DELETE FROM {} WHERE key = $1", self.table),
                    vec![Some(key.to_owned())],
                )
                .await?;
            return Ok((!shown(&answer)).then(|| Value::from_json(&json!({ "kept": false }))));
        };
        if self.create {
            for statement in [
                format!(
                    "CREATE TABLE IF NOT EXISTS {} (key text PRIMARY KEY, turns jsonb NOT NULL, forget_at timestamptz \
                     NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())",
                    self.table
                ),
                format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} (forget_at)",
                    self.index, self.table
                ),
            ] {
                if shown(&self.sql(statement, Vec::new()).await?) {
                    return Ok(None);
                }
            }
        }
        // A few conversations past their time are forgotten on the way.
        let swept = self
            .sql(
                format!(
                    "DELETE FROM {t} WHERE key IN (SELECT key FROM {t} WHERE forget_at <= now() LIMIT {SWEEP})",
                    t = self.table
                ),
                Vec::new(),
            )
            .await?;
        if shown(&swept) {
            return Ok(None);
        }
        let kept = json!({ "schema": 1, "turns": serde_json::from_str::<Json>(&turns.to_json()).unwrap_or(Json::Null) });
        let answer = self
            .sql(
                format!(
                    "INSERT INTO {} (key, turns, forget_at, updated_at) VALUES ($1, $2::jsonb, now() + ($3::bigint * \
                     interval '1 millisecond'), now()) ON CONFLICT (key) DO UPDATE SET turns = EXCLUDED.turns, \
                     forget_at = EXCLUDED.forget_at, updated_at = now()",
                    self.table
                ),
                vec![Some(key.to_owned()), Some(kept.to_string()), Some(millis(keep).to_string())],
            )
            .await?;
        Ok((!shown(&answer)).then(|| Value::from_json(&json!({ "kept": true }))))
    }
}

fn first_text(answer: &Value, column: &str) -> Option<String> {
    let Value::Record(r) = answer else { return None };
    let Some(Value::List(rows)) = r.get("rows") else {
        return None;
    };
    match rows.first()? {
        Value::Record(row) => row
            .get(column)
            .filter(|v| !v.is_null())
            .map(crate::template::as_text),
        _ => None,
    }
}

// --- Redis ---

fn redis_key(key: &str) -> Vec<u8> {
    format!("openduct:memory:{key}").into_bytes()
}

fn redis_credential(effects: &dyn Effects, name: &str) -> Result<Credential, String> {
    let c = effects.credential(name)?;
    if c.kind != "redis" && c.kind != "test" {
        return Err(format!(
            "{name} is a {} credential: a memory kept in Redis takes a redis one",
            c.kind
        ));
    }
    Ok(c)
}

async fn redis_recall(effects: &dyn Effects, name: &str, key: &str) -> Result<Value, String> {
    use crate::storage::redis::{Conn, Reply};
    let c = redis_credential(effects, name)?;
    let Ok(places) = effects.storage().allows(false) else {
        return Ok(Value::Null);
    };
    if c.kind != "redis" {
        return Ok(Value::Null);
    }
    let mut conn = Conn::open(&c, places).await?;
    match conn.call(&[b"GET".to_vec(), redis_key(key)]).await? {
        Reply::Text(bytes) => {
            Ok(serde_json::from_slice::<Json>(&bytes).map_or(Value::Null, |j| turns_of(&j)))
        }
        Reply::Refused(e) => Err(format!("Redis refused to read the memory: {e}")),
        _ => Ok(Value::Null),
    }
}

async fn redis_remember(
    effects: &dyn Effects,
    name: &str,
    key: &str,
    turns: Option<&Value>,
    keep: Duration,
) -> Result<Option<Value>, String> {
    use crate::storage::redis::{Conn, Reply};
    let c = redis_credential(effects, name)?;
    let Ok(places) = effects.storage().allows(true) else {
        return Ok(None);
    };
    if c.kind != "redis" {
        return Ok(None);
    }
    let args = match turns {
        Some(t) => {
            let kept = json!({ "schema": 1, "turns": serde_json::from_str::<Json>(&t.to_json()).unwrap_or(Json::Null), "updated": now_ms() });
            vec![
                b"SET".to_vec(),
                redis_key(key),
                kept.to_string().into_bytes(),
                b"PX".to_vec(),
                millis(keep).max(1).to_string().into_bytes(),
            ]
        }
        None => vec![b"DEL".to_vec(), redis_key(key)],
    };
    let mut conn = Conn::open(&c, places).await?;
    if let Reply::Refused(e) = conn.call(&args).await? {
        return Err(format!("Redis refused to keep the memory: {e}"));
    }
    Ok(Some(Value::from_json(&json!({ "kept": turns.is_some() }))))
}

// --- a data table ---

/// Why a table cannot keep memories: a column it lacks.
fn table_problem(t: &TableData) -> Option<String> {
    if t.stand_in {
        return None;
    }
    let missing: Vec<String> = TABLE_COLUMNS
        .iter()
        .filter(|(name, kind)| t.columns.iter().all(|c| c.name != *name || c.kind != *kind))
        .map(|(name, kind)| format!("{name} ({})", kind.name()))
        .collect();
    (!missing.is_empty()).then(|| {
        format!(
            "the data table {} keeps an agent's memory in the columns key (text), turns (json) and forget_at \
             (datetime): it lacks {}",
            t.name,
            missing.join(", ")
        )
    })
}

fn row_key(row: &Row) -> Option<String> {
    row.values.get("key").map(crate::template::as_text)
}

fn row_forget_at(row: &Row) -> i64 {
    match row.values.get("forget_at") {
        Some(Value::Time(t)) => t.timestamp_millis(),
        _ => i64::MAX,
    }
}

async fn table_recall(effects: &dyn Effects, name: &str, key: &str) -> Result<Value, String> {
    let tables = match effects.tables() {
        Access::None => return Ok(Value::Null),
        Access::Read(t) | Access::Write(t) => t,
    };
    if !tables.has(name).await? {
        return Err(format!(
            "there is no data table {name}: make it with the columns key (text), turns (json) and forget_at \
             (datetime)"
        ));
    }
    let data = tables.read(name).await?;
    if let Some(why) = table_problem(&data) {
        return Err(why);
    }
    let now = now_ms();
    Ok(data
        .rows
        .values()
        .find(|r| row_key(r).as_deref() == Some(key) && row_forget_at(r) > now)
        .and_then(|r| r.values.get("turns").cloned())
        .unwrap_or(Value::Null))
}

/// The turns cut, the oldest first, to fit a row.
fn fitting(turns: &Value) -> Value {
    let Value::List(items) = turns else {
        return turns.clone();
    };
    let mut list: Vec<Value> = items.as_ref().clone();
    while list.len() > 2 && Value::list(list.clone()).to_json().len() > MAX_ROW_BYTES - 2_048 {
        list.drain(..2);
    }
    Value::list(list)
}

async fn table_remember(
    effects: &dyn Effects,
    name: &str,
    key: &str,
    turns: Option<&Value>,
    keep: Duration,
) -> Result<Option<Value>, String> {
    let Access::Write(tables) = effects.tables() else {
        return Ok(None);
    };
    if !tables.has(name).await? {
        return Err(format!(
            "there is no data table {name}: make it with the columns key (text), turns (json) and forget_at \
             (datetime)"
        ));
    }
    let key = key.to_owned();
    let turns = turns.map(fitting);
    let forget_at =
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(now_ms().saturating_add(millis(keep)))
            .unwrap_or_else(chrono::Utc::now);
    let kept = turns.is_some();
    let plan: crate::tables::Plan<'_> = Box::new(move |data: &TableData| {
        if let Some(why) = table_problem(data) {
            return Err(why);
        }
        let now = now_ms();
        let mut changes = Changes {
            next_id: data.next_id.max(1),
            ..Changes::default()
        };
        let mine = data
            .rows
            .values()
            .find(|r| row_key(r).as_deref() == Some(key.as_str()));
        changes.delete.extend(
            data.rows
                .values()
                .filter(|r| row_forget_at(r) <= now && Some(r.id) != mine.map(|m| m.id))
                .take(SWEEP)
                .map(|r| r.id),
        );
        match (&turns, mine) {
            (Some(t), _) => {
                let mut values = Record::new();
                values.insert("key".to_owned(), Value::text(key.as_str()));
                values.insert("turns".to_owned(), t.clone());
                values.insert("forget_at".to_owned(), Value::Time(forget_at));
                let (id, created) = match mine {
                    Some(m) => (m.id, m.created),
                    None => {
                        let id = changes.next_id;
                        changes.next_id += 1;
                        (id, now)
                    }
                };
                changes.put.push(Row {
                    id,
                    values: Arc::new(values),
                    created,
                    updated: now,
                });
            }
            (None, Some(m)) => changes.delete.push(m.id),
            (None, None) => {}
        }
        crate::tables::check_changes(data, &changes)?;
        Ok((changes, Value::from_json(&json!({ "kept": kept }))))
    });
    tables.change(name, plan).await.map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_json(j: Json) -> Result<MemoryStore, String> {
        read(j.as_object().unwrap(), "steps[0].agent.memory")
    }

    #[test]
    fn a_memory_s_store_is_read_with_what_it_needs() {
        assert_eq!(read_json(json!({})).unwrap(), MemoryStore::Broker);
        assert_eq!(
            read_json(
                json!({ "store": "postgres", "credential": "db", "table": "ai.memory", "create": true })
            )
            .unwrap(),
            MemoryStore::Postgres {
                credential: "db".into(),
                schema: Some("ai".into()),
                table: "memory".into(),
                create: true
            }
        );
        assert_eq!(
            read_json(json!({ "store": "redis", "credential": "cache" })).unwrap(),
            MemoryStore::Redis {
                credential: "cache".into()
            }
        );
        assert_eq!(
            read_json(json!({ "store": "table", "table": "chats" })).unwrap(),
            MemoryStore::Table {
                table: "chats".into()
            }
        );
        for (bad, says) in [
            (json!({ "store": "files" }), "broker, postgres, redis or table"),
            (json!({ "store": "postgres" }), "credential: the PostgreSQL"),
            (
                json!({ "store": "postgres", "credential": "db", "table": "Bad;" }),
                "is not a table name",
            ),
            (
                json!({ "store": "redis", "credential": "c", "table": "t" }),
                "table: not a setting of a memory kept in redis",
            ),
            (json!({ "store": "table" }), "table: the data table"),
            (json!({ "store": "table", "table": "1x" }), "letters, digits"),
            (
                json!({ "credential": "db" }),
                "credential: not a setting of a memory kept in broker",
            ),
            (
                json!({ "store": "postgres", "credential": "db", "create": "yes" }),
                "create: true or false",
            ),
        ] {
            let e = read_json(bad.clone()).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
    }

    #[test]
    fn turns_are_cut_to_fit_a_row_the_oldest_first() {
        let long = "x".repeat(8_000);
        let turns = Value::list((0..20).map(|_| Value::text(long.as_str())).collect());
        let Value::List(kept) = fitting(&turns) else {
            panic!()
        };
        assert!(kept.len() < 20 && kept.len() % 2 == 0, "{}", kept.len());
        assert!(Value::list(kept.as_ref().clone()).to_json().len() <= MAX_ROW_BYTES);
    }

    #[test]
    fn a_memory_kept_in_a_data_table_is_a_table_the_workflow_needs() {
        let w = crate::model::Workflow::parse(
            "name: chat\nsteps:\n  - name: helper\n    agent:\n      credential: model\n      input: hi\n      memory: { \
             key: k, store: table, table: chats }\n",
        )
        .unwrap();
        let sample = Value::Null;
        let env = openduct_expr::Env::of(&sample);
        let found = crate::tables::wanted::tables(&w.steps, &env);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "chats");
        let columns: Vec<(&str, &str)> = found[0]
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.kind.name()))
            .collect();
        assert_eq!(
            columns,
            [("key", "text"), ("turns", "json"), ("forget_at", "datetime")]
        );
    }
}
