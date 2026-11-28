// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Statements on SQL Server (2012 and later, and Azure SQL), for a workflow's `sqlserver` step, with a client of
//! its own (TDS 7.4; TDS 8 with `tls: strict`): signed in with a SQL Server user and password, over TLS unless told
//! not to. Each statement runs through `sp_executesql`, its values (`@p1`, `@p2` ...) passed as parameters of their
//! own types - so a value from a message cannot change what the SQL does. Rows come back as JSON (see [`types`]).
//!
//! Statements run one after another on one connection: in one transaction when asked (XACT_ABORT on, so an error
//! rolls it all back). SQL Server has no read-only transaction: to read only, statements run in a transaction that
//! is always rolled back, so nothing they write is kept (the step also checks the SQL before it runs).

mod tokens;
mod types;
mod wire;

use std::net::SocketAddr;
use std::time::Duration;

use bytes::BufMut;
use tokio::time::Instant;

use self::wire::{Connection, Options, RPC, SQL_BATCH, Tls, all_headers, ucs2};
use crate::sql::{Answer, Budget, Limits, Mode, Param, Refusal, Statement, tls_config};

/// The default port.
pub const PORT: u16 = 1433;
/// `sp_executesql`, as the protocol numbers it.
const EXECUTESQL: u16 = 10;

/// Where the database is and how to sign in.
#[derive(Clone)]
pub struct Database {
    pub host: String,
    pub port: u16,
    pub database: Option<String>,
    pub username: String,
    pub password: Option<String>,
    /// `require` (the default): TLS agreed at the start, the server's certificate checked; `strict`: TLS before
    /// anything else (TDS 8); `disable`: none.
    pub tls: String,
    /// The certificates to check the server's against (PEM), when not the system's.
    pub ca: Option<String>,
    /// The server's own certificate (PEM), trusted as it is: for a server with a certificate of its own making.
    pub certificate: Option<String>,
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

/// The bytes an answer may take on the wire, for `max_bytes` of values: text there is UTF-16.
fn wire_limit(limits: &Limits) -> usize {
    limits.max_bytes.saturating_mul(3).saturating_add(1024 * 1024)
}

/// The RPC that runs `s` through sp_executesql, in `transaction`.
fn rpc_payload(s: &Statement, transaction: [u8; 8], collation: [u8; 5]) -> Vec<u8> {
    let mut out = all_headers(transaction);
    out.put_u16_le(0xffff);
    out.put_u16_le(EXECUTESQL);
    out.put_u16_le(0);
    // @stmt, and @params with each value's type, both by place; then the values by name.
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&types::param(&Param::Text(s.sql.clone()), collation).1);
    if s.params.is_empty() {
        return out;
    }
    let typed: Vec<(String, Vec<u8>)> = s.params.iter().map(|p| types::param(p, collation)).collect();
    let declared: Vec<String> = typed
        .iter()
        .enumerate()
        .map(|(i, (d, _))| format!("@p{} {d}", i + 1))
        .collect();
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&types::param(&Param::Text(declared.join(",")), collation).1);
    for (i, (_, bytes)) in typed.iter().enumerate() {
        let name = format!("@p{}", i + 1);
        out.push(u8::try_from(name.encode_utf16().count()).unwrap_or(0));
        out.extend_from_slice(&ucs2(&name));
        out.push(0);
        out.extend_from_slice(bytes);
    }
    out
}

fn batch_payload(sql: &str, transaction: [u8; 8]) -> Vec<u8> {
    let mut out = all_headers(transaction);
    out.extend_from_slice(&ucs2(sql));
    out
}

impl Connection {
    /// Sends `payload` as a message of `kind` and reads what it gives back.
    async fn ask(
        &mut self,
        kind: u8,
        payload: &[u8],
        limit: usize,
        until: Instant,
        budget: Option<&mut Budget>,
    ) -> Result<Vec<Answer>, String> {
        self.send(kind, payload).await?;
        let message = self.read_message(limit, Some(until)).await?;
        let said = tokens::read(&message, self, budget)?;
        match said.error {
            Some(e) => Err(e.shown()),
            None => Ok(said.answers),
        }
    }

    async fn batch(&mut self, sql: &str, until: Instant) -> Result<(), String> {
        let payload = batch_payload(sql, self.transaction);
        self.ask(SQL_BATCH, &payload, 1024 * 1024, until, None)
            .await
            .map(|_| ())
    }
}

/// Runs `statements` in order, each one's answers (a batch may give several) in its place.
pub async fn run(
    db: &Database,
    statements: &[Statement],
    mode: Mode,
    limits: Limits,
) -> Result<Vec<Vec<Answer>>, Refusal> {
    let until = Instant::now() + limits.timeout;
    let what = format!("the SQL Server at {}:{}", db.host, db.port);
    // The work is told to stop at `until`; a little after, the connection is given up whatever it is doing.
    match tokio::time::timeout_at(
        until + Duration::from_secs(2),
        run_all(db, statements, mode, limits, until),
    )
    .await
    {
        Ok(done) => done,
        Err(_) => Err(Refusal::Failed(format!("{what} did not finish in time"))),
    }
}

async fn run_all(
    db: &Database,
    statements: &[Statement],
    mode: Mode,
    limits: Limits,
    until: Instant,
) -> Result<Vec<Vec<Answer>>, Refusal> {
    let tls = match db.tls.trim() {
        "disable" => Tls::Off,
        mode => {
            let config = tls_config(mode, db.ca.as_deref(), db.certificate.as_deref())?
                .ok_or_else(|| "tls: require (the default), strict or disable".to_owned())?;
            if mode == "strict" {
                Tls::Strict(config)
            } else {
                Tls::On(config)
            }
        }
    };
    let options = Options {
        host: db.host.clone(),
        port: db.port,
        database: db.database.clone().filter(|d| !d.trim().is_empty()),
        username: db.username.clone(),
        password: db.password.clone().unwrap_or_default(),
        tls,
        address: db.address,
    };
    let mut c = Connection::open(&options).await?;
    let in_transaction = mode.read_only || mode.transaction;
    if in_transaction {
        c.batch("SET XACT_ABORT ON; BEGIN TRANSACTION", until).await?;
    }
    let mut budget = Budget::new(limits);
    let limit = wire_limit(&limits);
    let mut out = Vec::with_capacity(statements.len());
    for (i, s) in statements.iter().enumerate() {
        let payload = rpc_payload(s, c.transaction, c.collation);
        match c.ask(RPC, &payload, limit, until, Some(&mut budget)).await {
            Ok(answers) => out.push(answers),
            Err(e) => {
                if in_transaction {
                    let _ = c.batch("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION", until).await;
                }
                return Err(Refusal::Failed(if statements.len() > 1 {
                    format!("statement {}: {e}", i + 1)
                } else {
                    e
                }));
            }
        }
    }
    if mode.read_only {
        let _ = c.batch("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION", until).await;
    } else if mode.transaction {
        c.batch("COMMIT TRANSACTION", until)
            .await
            .map_err(|e| format!("the transaction was not committed: {e}"))?;
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn a_statement_goes_with_its_values_beside_it() {
        let s = Statement {
            sql: "SELECT * FROM orders WHERE id = @p1 AND note = @p2".into(),
            params: vec![Param::Int(42), Param::Text("x'; DROP TABLE orders; --".into())],
        };
        let p = rpc_payload(&s, [0; 8], [0; 5]);
        // ALL_HEADERS, then sp_executesql by number.
        assert_eq!(&p[22..26], &[0xff, 0xff, 10, 0]);
        let text = String::from_utf16_lossy(
            &p.as_chunks::<2>()
                .0
                .iter()
                .map(|c| u16::from_le_bytes(*c))
                .collect::<Vec<_>>(),
        );
        let shifted = String::from_utf16_lossy(
            &p[1..]
                .as_chunks::<2>()
                .0
                .iter()
                .map(|c| u16::from_le_bytes(*c))
                .collect::<Vec<_>>(),
        );
        let both = format!("{text}{shifted}");
        assert!(both.contains("SELECT * FROM orders WHERE id = @p1 AND note = @p2"));
        assert!(both.contains("@p1 bigint,@p2 nvarchar(4000)"));
        // The value is sent apart from the SQL, as itself.
        assert!(both.contains("x'; DROP TABLE orders; --"));
        assert!(!both.contains("id = 42"));
        let plain = rpc_payload(
            &Statement {
                sql: "SELECT 1".into(),
                params: vec![],
            },
            [0; 8],
            [0; 5],
        );
        assert!(plain.len() < p.len());
        assert_eq!(batch_payload("COMMIT", [0; 8]).len(), 22 + 12);
    }
}
