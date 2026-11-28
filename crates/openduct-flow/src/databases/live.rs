// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A database step run for real: where its credential says, refused - unless allowed - when the database's name
//! leads to this machine's own network (`--allow-private-network`), as an HTTP step is; connected to the address
//! that was checked, so a name cannot lead elsewhere between the check and the connection.

use std::net::SocketAddr;

use openduct_connect::sql::Refusal;
use openduct_expr::Value;

use super::{DatabaseRequest, Db, result_of};
use crate::effects::private;
use crate::run::WRITES;

/// The address `host:port` leads to: refused when it is on this machine's network, unless `allow_private`.
async fn address(host: &str, port: u16, allow_private: bool) -> Result<SocketAddr, String> {
    let found: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("cannot find the database {host}: {e}"))?
        .collect();
    if !allow_private && found.iter().any(|a| private(a.ip())) {
        return Err(format!(
            "{host} is on this machine's own network: start OpenDuct with --allow-private-network to let \
             workflows reach it"
        ));
    }
    found
        .first()
        .copied()
        .ok_or_else(|| format!("cannot find the database {host}"))
}

/// The host and port a MySQL or SQL Server credential names.
fn host_and_port(r: &DatabaseRequest, default: u16) -> Result<(String, u16), String> {
    let host = r
        .credential
        .fields
        .get("host")
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .ok_or("the credential has no host")?;
    if host.contains(['/', '@', '?', ' ']) || host.len() > 253 {
        return Err("the credential's host is a name or an address alone, as db.example.com".to_owned());
    }
    let port = match r
        .credential
        .fields
        .get("port")
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
    {
        None => default,
        Some(p) => p
            .parse::<u16>()
            .ok()
            .filter(|p| *p > 0)
            .ok_or_else(|| format!("the credential's port {p:?} is not a port"))?,
    };
    Ok((
        host.trim_start_matches('[').trim_end_matches(']').to_owned(),
        port,
    ))
}

/// Runs a database step's statements on its database.
pub(crate) async fn run(r: DatabaseRequest, allow_private: bool) -> Result<Value, String> {
    let field = |name: &str| {
        r.credential
            .fields
            .get(name)
            .filter(|v| !v.trim().is_empty())
            .cloned()
    };
    let answered = match r.db {
        Db::Postgres => {
            let mut db = openduct_connect::postgres::sql::Database {
                url: field("url").ok_or("the credential has no url: postgres://host:port/database")?,
                username: field("username").ok_or("the credential has no username")?,
                password: field("password"),
                tls: field("tls").unwrap_or_else(|| "require".to_owned()),
                ca: field("ca"),
                address: None,
            };
            let (host, port) = db.host()?;
            db.address = Some(address(&host, port, allow_private).await?);
            openduct_connect::postgres::sql::run(&db, &r.statements, r.mode, r.limits).await
        }
        Db::Mysql => {
            let (host, port) = host_and_port(&r, openduct_connect::mysql::sql::PORT)?;
            let db = openduct_connect::mysql::sql::Database {
                address: Some(address(&host, port, allow_private).await?),
                host,
                port,
                database: field("database"),
                username: field("username").ok_or("the credential has no username")?,
                password: field("password"),
                tls: field("tls").unwrap_or_else(|| "require".to_owned()),
                ca: field("ca"),
                certificate: field("certificate"),
            };
            openduct_connect::mysql::sql::run(&db, &r.statements, r.mode, r.limits).await
        }
        Db::SqlServer => {
            let (host, port) = host_and_port(&r, openduct_connect::sqlserver::PORT)?;
            let db = openduct_connect::sqlserver::Database {
                address: Some(address(&host, port, allow_private).await?),
                host,
                port,
                database: field("database"),
                username: field("username").ok_or("the credential has no username")?,
                password: field("password"),
                tls: field("tls").unwrap_or_else(|| "require".to_owned()),
                ca: field("ca"),
                certificate: field("certificate"),
            };
            openduct_connect::sqlserver::run(&db, &r.statements, r.mode, r.limits).await
        }
    };
    match answered {
        Ok(answers) => Ok(result_of(&answers)),
        Err(Refusal::Writes) => Err(WRITES.to_owned()),
        Err(Refusal::Failed(e)) => Err(e),
    }
}
