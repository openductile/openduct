// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: mysql_cdc:` - change data capture from MySQL 8, read from its binary log as a replica reads it, with
//! its own client for the parts of the protocol it needs (sign-in, text queries, the binary log), so nothing is
//! installed on the database.
//!
//! ```yaml
//! from:
//!   mysql_cdc:
//!     url: mysql://db.example.com:3306
//!     username: secret:MYSQL_USER
//!     password: secret:MYSQL_PASSWORD
//!     tls: true
//!     tables: [shop.orders, shop.customers]
//! ```

mod auth;
mod binlog;
mod json;
mod source;
mod stream;
mod values;
mod wire;

use std::sync::Arc;

use openduct_pipeline::Connector;

pub(crate) use self::source::MysqlCdcSource;
pub(crate) use self::stream::replay;
use self::wire::Options;
use crate::settings::{Limits, Settings};

const SETTINGS: &[&str] = &[
    "url",
    "username",
    "password",
    "tls",
    "ca",
    "client_cert",
    "client_key",
    "tables",
    "server_id",
    "snapshot",
];
const MAX_TABLES: usize = 500;

/// `mysql://host[:port]`.
fn endpoint(url: &str, at: &str) -> Result<(String, u16), String> {
    let rest = url
        .strip_prefix("mysql://")
        .ok_or_else(|| format!("{at}: is mysql://host:port"))?;
    let rest = rest.trim_end_matches('/');
    if rest.contains(['@', '/', '?']) {
        return Err(format!(
            "{at}: is only mysql://host:port; the username, password and TLS are settings of their own, and the \
             tables name their databases"
        ));
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(':') => (
            h.trim_start_matches('[').trim_end_matches(']'),
            p.parse::<u16>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| format!("{at}: {p:?} is not a port"))?,
        ),
        _ => (rest.trim_start_matches('[').trim_end_matches(']'), 3306),
    };
    if host.is_empty() {
        return Err(format!("{at}: names no host"));
    }
    Ok((host.to_owned(), port))
}

/// A name as MySQL stores it: 1 to 64 letters, digits, `_` and `$`.
fn identifier(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

fn table(text: &str, at: &str) -> Result<(String, String), String> {
    match text.split_once('.') {
        Some((db, t)) if identifier(db) && identifier(t) => Ok((db.to_owned(), t.to_owned())),
        _ => Err(format!(
            "{at}: {text:?} is database.table (letters, digits, _ and $)"
        )),
    }
}

/// A replica's server id from the pipeline's name, away from the small numbers servers are usually given.
fn default_server_id(pipeline: &str) -> u32 {
    let hash = pipeline.bytes().fold(0x811c_9dc5u32, |h, b| {
        (h ^ u32::from(b)).wrapping_mul(0x0100_0193)
    });
    1_000_000_000 + hash % 1_000_000_000
}

impl MysqlCdcSource {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let (host, port) = endpoint(&s.required("url")?, &s.path("url"))?;
        s.reach(&s.path("url"), &host, port)?;
        let username = s.required("username")?;
        if username.is_empty() && !s.checking() {
            return Err(format!("{}: is not empty", s.path("username")));
        }
        let password = s.text("password")?;
        let tls = if s.boolean("tls", false)? {
            Some(Arc::new(crate::tls::config(&s)?))
        } else {
            for name in ["ca", "client_cert", "client_key"] {
                if s.text(name)?.is_some() {
                    return Err(format!("{}: is for tls: true", s.path(name)));
                }
            }
            None
        };
        let at = s.path("tables");
        let tables = s
            .list("tables")?
            .iter()
            .flat_map(|t| t.split(','))
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(|t| table(t, &at))
            .collect::<Result<Vec<_>, _>>()?;
        if tables.is_empty() {
            return Err(format!("{at}: is required: one or more database.table"));
        }
        if tables.len() > MAX_TABLES {
            return Err(format!("{at}: is at most {MAX_TABLES} tables"));
        }
        let server_id = u32::try_from(s.number(
            "server_id",
            u64::from(default_server_id(pipeline)),
            1..=4_294_967_295,
        )?)
        .unwrap_or(u32::MAX);
        let snapshot = s.boolean("snapshot", true)?;
        let names: Vec<String> = tables.iter().map(|(d, t)| format!("{d}.{t}")).collect();
        let shown = format!("mysql://{host}:{port} {} as server {server_id}", names.join(", "));
        let options = Options {
            host,
            port,
            database: None,
            username,
            password,
            tls,
        };
        Ok(Self::new(shown, options, tables, server_id, snapshot))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn urls_tables_and_server_ids() {
        assert_eq!(
            endpoint("mysql://db.example.com", "u").unwrap(),
            ("db.example.com".to_owned(), 3306)
        );
        assert_eq!(endpoint("mysql://10.0.0.5:3307/", "u").unwrap().1, 3307);
        for bad in [
            "postgres://db",
            "mysql://u:p@db",
            "mysql://db/shop",
            "mysql://db:0",
            "mysql://",
        ] {
            assert!(endpoint(bad, "u").is_err(), "{bad}");
        }
        assert_eq!(
            table("shop.orders", "t").unwrap(),
            ("shop".to_owned(), "orders".to_owned())
        );
        for bad in ["orders", "shop.", "shop.o rders", "a.b.c", "x';--.y"] {
            assert!(table(bad, "t").is_err(), "{bad}");
        }
        let id = default_server_id("orders-in");
        assert!((1_000_000_000..2_000_000_000).contains(&id));
        assert_eq!(id, default_server_id("orders-in"));
        assert_ne!(id, default_server_id("orders-out"));
    }
}
