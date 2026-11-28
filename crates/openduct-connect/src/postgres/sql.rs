// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One query to a Postgres database, for a workflow's `postgres` step: the SQL as written, its values passed as
//! parameters (`$1`, `$2` ...), never written into it - so a value from a message cannot change what the SQL does.
//! The rows come back as JSON, each column as its type reads: numbers as numbers, `true`/`false`, JSON as JSON, a
//! vector (`[0.1,0.2]`, as pgvector writes one) as a list, the rest as text.

use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use rustls_platform_verifier::BuilderVerifierExt;
use serde_json::{Map, Number, Value};

use super::wire::{Connection, Options};

/// The most rows one query gives back.
pub const MAX_ROWS: usize = 10_000;

/// Where the database is and how to sign in.
#[derive(Clone)]
pub struct Database {
    /// `postgres://host:port/database`.
    pub url: String,
    pub username: String,
    pub password: Option<String>,
    /// `disable`, or `require` (the default): TLS, the server's certificate checked against the system's roots, or
    /// against `ca`.
    pub tls: String,
    pub ca: Option<String>,
    /// The address to connect to, when the caller resolved the host to check where it leads.
    pub address: Option<std::net::SocketAddr>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("url", &self.url)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// Why a query gave no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// It writes, and it was run to read only (a preview).
    Writes,
    Failed(String),
}

/// A query's answer.
#[derive(Debug, Clone, Default)]
pub struct Answer {
    pub columns: Vec<String>,
    pub rows: Vec<Map<String, Value>>,
    /// What the server said it did: `INSERT 0 3`, `SELECT 5`.
    pub command: Option<String>,
}

impl Database {
    /// The host it names, for a caller that checks where a step may connect.
    pub fn host(&self) -> Result<(String, u16), String> {
        let (host, port, _) = super::endpoint(&self.url, "url")?;
        Ok((host, port))
    }

    fn tls(&self) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
        if self.tls.trim() == "disable" {
            return Ok(None);
        }
        let builder =
            rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| format!("cannot set up TLS: {e}"))?;
        let config = match self.ca.as_deref().filter(|c| !c.trim().is_empty()) {
            Some(pem) => {
                let mut roots = rustls::RootCertStore::empty();
                for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                    let cert = cert.map_err(|_| "ca: not PEM certificates".to_owned())?;
                    roots
                        .add(cert)
                        .map_err(|e| format!("ca: a certificate was not accepted: {e}"))?;
                }
                builder.with_root_certificates(roots).with_no_client_auth()
            }
            None => builder
                .with_platform_verifier()
                .map_err(|e| format!("cannot read the system's certificates: {e}"))?
                .with_no_client_auth(),
        };
        Ok(Some(Arc::new(config)))
    }
}

/// Runs `sql` with `params` (text, or null) and gives back its rows. With `read_only`, it runs in a read-only
/// transaction, rolled back: what writes (an INSERT, a function that changes a table, `nextval`) is refused by the
/// database itself, as [`Refusal::Writes`].
pub async fn query(
    db: &Database,
    sql: &str,
    params: &[Option<String>],
    read_only: bool,
) -> Result<Answer, Refusal> {
    let failed = |e: String| Refusal::Failed(e);
    let (host, port, database) = super::endpoint(&db.url, "url").map_err(failed)?;
    let options = Options {
        host,
        port,
        database,
        username: db.username.clone(),
        password: db.password.clone(),
        tls: db.tls().map_err(failed)?,
        address: db.address,
    };
    let mut connection = Connection::open(&options, false)
        .await
        .map_err(|e| failed(e.reason()))?;
    if read_only {
        connection
            .query("START TRANSACTION READ ONLY")
            .await
            .map_err(|e| failed(e.reason()))?;
    }
    let answered = connection.query_with(sql, params).await;
    if read_only {
        let _ = connection.query("ROLLBACK").await;
    }
    let rows = match answered {
        Ok(rows) => rows,
        Err(e) => {
            let reason = e.reason();
            return Err(if read_only && reason.contains("SQLSTATE 25006") {
                Refusal::Writes
            } else {
                failed(reason)
            });
        }
    };
    if rows.rows.len() > MAX_ROWS {
        return Err(failed(format!("more than {MAX_ROWS} rows: add a LIMIT")));
    }
    let columns: Vec<String> = rows.columns.iter().map(|(name, _)| name.clone()).collect();
    let mut out = Vec::with_capacity(rows.rows.len());
    for row in &rows.rows {
        let mut record = Map::new();
        for ((name, kind), value) in rows.columns.iter().zip(row) {
            let text = value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned());
            record.insert(name.clone(), typed(*kind, text));
        }
        out.push(record);
    }
    Ok(Answer {
        columns,
        rows: out,
        command: rows.tag,
    })
}

// sql databases: the `postgres` step with the settings every SQL step has - several statements, a transaction,
// read only, limits on rows and time.

/// A parameter as the server reads it: as text, in its place's type.
fn text_of(p: &crate::sql::Param) -> Option<String> {
    use crate::sql::Param;
    match p {
        Param::Null => None,
        Param::Bool(b) => Some(b.to_string()),
        Param::Int(n) => Some(n.to_string()),
        Param::Decimal(t) | Param::Text(t) => Some(t.clone()),
        Param::Time(t) => Some(format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}+00",
            t.year, t.month, t.day, t.hour, t.minute, t.second, t.micros
        )),
    }
}

/// The rows a command changed, from its tag (`INSERT 0 3`, `UPDATE 5`); none for one that reads.
fn affected(tag: Option<&str>) -> Option<u64> {
    let tag = tag?;
    let verb = tag.split_whitespace().next()?;
    if matches!(verb, "SELECT" | "SHOW" | "EXPLAIN" | "FETCH") {
        return None;
    }
    tag.rsplit(' ').next()?.parse().ok()
}

/// Runs `statements` in order on one connection, each one's answer in its place: in one transaction when asked;
/// to read only, in a read-only transaction rolled back (a write is refused by the database, as
/// [`crate::sql::Refusal::Writes`]).
pub async fn run(
    db: &Database,
    statements: &[crate::sql::Statement],
    mode: crate::sql::Mode,
    limits: crate::sql::Limits,
) -> Result<Vec<Vec<crate::sql::Answer>>, crate::sql::Refusal> {
    use crate::sql::Refusal;
    let what = db.host().map_or_else(
        |_| "the database".to_owned(),
        |(h, p)| format!("the database at {h}:{p}"),
    );
    match tokio::time::timeout(limits.timeout, run_all(db, statements, mode, limits)).await {
        Ok(done) => done,
        Err(_) => Err(Refusal::Failed(format!("{what} did not finish in time"))),
    }
}

async fn run_all(
    db: &Database,
    statements: &[crate::sql::Statement],
    mode: crate::sql::Mode,
    limits: crate::sql::Limits,
) -> Result<Vec<Vec<crate::sql::Answer>>, crate::sql::Refusal> {
    use crate::sql::{Answer, Budget, Refusal};
    let (host, port, database) = super::endpoint(&db.url, "url")?;
    let options = Options {
        host,
        port,
        database,
        username: db.username.clone(),
        password: db.password.clone(),
        tls: db.tls()?,
        address: db.address,
    };
    let mut c = Connection::open(&options, false).await.map_err(|e| e.reason())?;
    c.query(&format!(
        "SET statement_timeout = {}",
        limits.timeout.as_millis().max(1)
    ))
    .await
    .map_err(|e| e.reason())?;
    let begin = if mode.read_only {
        Some("START TRANSACTION READ ONLY")
    } else if mode.transaction {
        Some("BEGIN")
    } else {
        None
    };
    if let Some(begin) = begin {
        c.query(begin).await.map_err(|e| e.reason())?;
    }
    let mut budget = Budget::new(limits);
    let mut out = Vec::with_capacity(statements.len());
    let cap = i32::try_from(limits.max_rows.saturating_add(1)).unwrap_or(i32::MAX);
    for (i, s) in statements.iter().enumerate() {
        let params: Vec<Option<String>> = s.params.iter().map(text_of).collect();
        let failed = |reason: String| {
            if statements.len() > 1 {
                format!("statement {}: {reason}", i + 1)
            } else {
                reason
            }
        };
        let rows = match c.query_with_limit(&s.sql, &params, cap).await {
            Ok(rows) => rows,
            Err(e) => {
                let reason = e.reason();
                if begin.is_some() {
                    let _ = c.query("ROLLBACK").await;
                }
                return Err(if mode.read_only && reason.contains("SQLSTATE 25006") {
                    Refusal::Writes
                } else {
                    Refusal::Failed(failed(reason))
                });
            }
        };
        let mut answer = Answer {
            columns: rows.columns.iter().map(|(name, _)| name.clone()).collect(),
            affected: affected(rows.tag.as_deref()),
            ..Answer::default()
        };
        for (n, row) in rows.rows.iter().enumerate() {
            let size = row.iter().map(|v| v.as_ref().map_or(0, |b| b.len())).sum();
            if let Err(e) = budget.row(n + 1, size) {
                if begin.is_some() {
                    let _ = c.query("ROLLBACK").await;
                }
                return Err(Refusal::Failed(failed(e)));
            }
            let mut record = Map::new();
            for ((name, kind), value) in rows.columns.iter().zip(row) {
                let text = value.as_ref().map(|v| String::from_utf8_lossy(v).into_owned());
                record.insert(name.clone(), typed(*kind, text));
            }
            answer.rows.push(record);
        }
        out.push(vec![answer]);
    }
    if mode.read_only {
        let _ = c.query("ROLLBACK").await;
    } else if mode.transaction {
        c.query("COMMIT")
            .await
            .map_err(|e| format!("the transaction was not committed: {}", e.reason()))?;
    }
    Ok(out)
}

/// A column's value as its type reads, from the server's text.
fn typed(kind: u32, text: Option<String>) -> Value {
    let Some(text) = text else { return Value::Null };
    match kind {
        16 => Value::Bool(text == "t"),
        20 | 21 | 23 | 26 => text
            .parse::<i64>()
            .map_or(Value::String(text), |n| Value::Number(n.into())),
        700 | 701 | 1700 => match serde_json::from_str::<Number>(&text) {
            Ok(n) => Value::Number(n),
            Err(_) => Value::String(text),
        },
        114 | 3802 => serde_json::from_str(&text).unwrap_or(Value::String(text)),
        // A vector (pgvector's own type has no fixed number): `[0.1,0.2]` reads as a list of numbers.
        _ if text.starts_with('[') && text.ends_with(']') => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Array(items)) if items.iter().all(Value::is_number) => Value::Array(items),
            _ => Value::String(text),
        },
        _ => Value::String(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_go_as_text_and_tags_say_what_changed() {
        use crate::sql::{Civil, Param};
        assert_eq!(text_of(&Param::Null), None);
        assert_eq!(text_of(&Param::Bool(true)).as_deref(), Some("true"));
        assert_eq!(text_of(&Param::Int(-3)).as_deref(), Some("-3"));
        let t = Civil {
            year: 2026,
            month: 11,
            day: 27,
            hour: 9,
            minute: 30,
            second: 5,
            micros: 7,
        };
        assert_eq!(
            text_of(&Param::Time(t)).as_deref(),
            Some("2026-11-27 09:30:05.000007+00")
        );
        assert_eq!(affected(Some("INSERT 0 3")), Some(3));
        assert_eq!(affected(Some("UPDATE 5")), Some(5));
        assert_eq!(affected(Some("SELECT 5")), None);
        assert_eq!(affected(None), None);
    }

    #[test]
    fn columns_read_as_their_types() {
        assert_eq!(typed(23, Some("42".into())), Value::from(42));
        // A numeric keeps its digits, as the database gave them.
        assert_eq!(typed(1700, Some("19.90".into())).to_string(), "19.90");
        assert_eq!(typed(16, Some("t".into())), Value::Bool(true));
        assert_eq!(
            typed(3802, Some(r#"{"a": 1}"#.into())),
            serde_json::json!({ "a": 1 })
        );
        assert_eq!(
            typed(99_999, Some("[0.5,1,2]".into())),
            serde_json::json!([0.5, 1, 2])
        );
        assert_eq!(
            typed(25, Some("[not a vector]".into())),
            Value::String("[not a vector]".into())
        );
        assert_eq!(typed(25, None), Value::Null);
    }
}
