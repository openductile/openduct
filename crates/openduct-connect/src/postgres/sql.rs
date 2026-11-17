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
