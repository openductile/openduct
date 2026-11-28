// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Statements on a MySQL or MariaDB database, for a workflow's `mysql` step: each prepared as written, with `?`
//! where its values go, and run with its values as parameters of their own types - so a value from a message cannot
//! change what the SQL does. Rows come back as JSON, each column as its type reads (see [`super::statement`]).
//!
//! The session reads times in UTC (TIMESTAMP columns come back in UTC, as the stream gives them) and stops a SELECT
//! that runs past the step's time on the server too. Statements run one after another on one connection: in one
//! transaction when asked, and, to read only, in a read-only transaction the server keeps from writing, rolled back.

use std::net::SocketAddr;
use std::time::Duration;

use super::wire::{Connection, Options};
use crate::sql::{Answer, Budget, Limits, Mode, Refusal, Statement, tls_config};

/// Where the database is and how to sign in.
#[derive(Clone)]
pub struct Database {
    pub host: String,
    pub port: u16,
    /// The database statements run in, unless they name another.
    pub database: Option<String>,
    pub username: String,
    pub password: Option<String>,
    /// `require` (the default): TLS, the server's certificate checked; or `disable`.
    pub tls: String,
    /// The certificates to check the server's against (PEM), when not the system's.
    pub ca: Option<String>,
    /// The server's own certificate (PEM), trusted as it is: for a server with a certificate of its own making.
    pub certificate: Option<String>,
    /// The address to connect to, when the caller resolved the host to check where it leads.
    pub address: Option<SocketAddr>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// The default port.
pub const PORT: u16 = 3306;

/// What a write refused in a read-only transaction is answered with (ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION).
fn writes(reason: &str) -> bool {
    reason.contains("(MySQL error 1792)")
}

/// Runs `statements` in order, each one's answers (a procedure may give several) in its place.
pub async fn run(
    db: &Database,
    statements: &[Statement],
    mode: Mode,
    limits: Limits,
) -> Result<Vec<Vec<Answer>>, Refusal> {
    let what = format!("the MySQL server at {}:{}", db.host, db.port);
    match tokio::time::timeout(limits.timeout, run_all(db, statements, mode, limits)).await {
        Ok(answered) => answered,
        Err(_) => Err(Refusal::Failed(format!(
            "{what} did not finish within {}",
            shown(limits.timeout)
        ))),
    }
}

fn shown(d: Duration) -> String {
    if d.as_millis().is_multiple_of(1000) {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis())
    }
}

async fn run_all(
    db: &Database,
    statements: &[Statement],
    mode: Mode,
    limits: Limits,
) -> Result<Vec<Vec<Answer>>, Refusal> {
    let options = Options {
        host: db.host.clone(),
        port: db.port,
        database: db.database.clone().filter(|d| !d.trim().is_empty()),
        username: db.username.clone(),
        password: db.password.clone(),
        tls: tls_config(&db.tls, db.ca.as_deref(), db.certificate.as_deref())?,
        address: db.address,
    };
    let mut c = Connection::open(&options).await.map_err(|e| e.reason())?;
    c.query("SET time_zone = '+00:00'")
        .await
        .map_err(|e| e.reason())?;
    // A SELECT past the step's time is stopped by the server too (an older server has no such setting).
    let ms = limits.timeout.as_millis().max(1);
    let stop = if c.server_version.contains("MariaDB") {
        format!("SET SESSION max_statement_time = {:.3}", ms as f64 / 1000.0)
    } else {
        format!("SET SESSION max_execution_time = {ms}")
    };
    let _ = c.query(&stop).await;
    let begin = if mode.read_only {
        Some("START TRANSACTION READ ONLY")
    } else if mode.transaction {
        Some("START TRANSACTION")
    } else {
        None
    };
    if let Some(begin) = begin {
        c.query(begin).await.map_err(|e| e.reason())?;
    }
    let mut budget = Budget::new(limits);
    let mut out = Vec::with_capacity(statements.len());
    for (i, s) in statements.iter().enumerate() {
        match c.execute(&s.sql, &s.params, &mut budget).await {
            Ok(answers) => out.push(answers),
            Err(e) => {
                let reason = e.reason();
                if begin.is_some() {
                    let _ = c.query("ROLLBACK").await;
                }
                if mode.read_only && writes(&reason) {
                    return Err(Refusal::Writes);
                }
                return Err(Refusal::Failed(if statements.len() > 1 {
                    format!("statement {}: {reason}", i + 1)
                } else {
                    reason
                }));
            }
        }
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
