// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: postgres_cdc:` - change data capture from PostgreSQL 10 or later, with its own client for the parts of
//! the wire protocol it needs (sign-in, simple queries, the replication stream) and the server's built-in
//! `pgoutput` plugin, so nothing is installed on the database.
//!
//! ```yaml
//! from:
//!   postgres_cdc:
//!     url: postgres://db.example.com:5432/shop
//!     username: secret:PG_USER
//!     password: secret:PG_PASSWORD
//!     tls: true
//!     tables: [public.orders, public.customers]
//! ```

mod pgoutput;
mod source;
pub mod sql;
mod stream;
mod wire;

use std::sync::Arc;

use openduct_pipeline::Connector;

pub(crate) use self::source::PostgresCdcSource;
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
    "publication",
    "slot",
    "snapshot",
];
const MAX_TABLES: usize = 500;

/// `postgres://host[:port]/database`.
pub(crate) fn endpoint(url: &str, at: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .ok_or_else(|| format!("{at}: is postgres://host:port/database"))?;
    let (address, database) = rest
        .split_once('/')
        .ok_or_else(|| format!("{at}: names no database: postgres://host:port/database"))?;
    if address.contains('@') || database.contains(['?', '/', '@']) {
        return Err(format!(
            "{at}: is only postgres://host:port/database; the username, password and TLS are settings of their own"
        ));
    }
    let (host, port) = match address.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(':') => (
            h.trim_start_matches('[').trim_end_matches(']'),
            p.parse::<u16>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| format!("{at}: {p:?} is not a port"))?,
        ),
        _ => (address.trim_start_matches('[').trim_end_matches(']'), 5432),
    };
    if host.is_empty() || database.is_empty() {
        return Err(format!("{at}: is postgres://host:port/database"));
    }
    Ok((host.to_owned(), port, database.to_owned()))
}

/// A name PostgreSQL stores as written: 1 to 63 letters, digits, `_` and `$`, not starting with a digit.
fn identifier(name: &str) -> bool {
    (1..=63).contains(&name.len())
        && !name.starts_with(|c: char| c.is_ascii_digit() || c == '$')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

/// `schema.table`, or `table` in `public`.
fn table(text: &str, at: &str) -> Result<(String, String), String> {
    let (schema, name) = text.split_once('.').unwrap_or(("public", text));
    if identifier(schema) && identifier(name) {
        Ok((schema.to_owned(), name.to_owned()))
    } else {
        Err(format!(
            "{at}: {text:?} is not schema.table as PostgreSQL stores the names (letters, digits and _)"
        ))
    }
}

/// A slot or publication name: lower case letters, digits and `_`, as replication slots must be.
fn slot_name(s: &Settings<'_>, name: &str, default: &str) -> Result<String, String> {
    let value = s.text(name)?.unwrap_or_else(|| default.to_owned());
    if (1..=63).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        Ok(value)
    } else {
        Err(format!(
            "{}: {value:?} is 1 to 63 lower case letters, digits and _",
            s.path(name)
        ))
    }
}

/// `openduct_<pipeline>`, made into a slot name.
fn default_name(pipeline: &str) -> String {
    let mut name: String = format!("openduct_{pipeline}")
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() { c } else { '_' }
        })
        .collect();
    name.truncate(63);
    name
}

impl PostgresCdcSource {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let (host, port, database) = endpoint(&s.required("url")?, &s.path("url"))?;
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
        if tables.len() > MAX_TABLES {
            return Err(format!("{at}: is at most {MAX_TABLES} tables"));
        }
        let name = default_name(pipeline);
        let publication = slot_name(&s, "publication", &name)?;
        let slot = slot_name(&s, "slot", &name)?;
        let snapshot = s.boolean("snapshot", true)?;
        let what = if tables.is_empty() {
            format!("the tables of publication {publication}")
        } else {
            tables
                .iter()
                .map(|(schema, t)| format!("{schema}.{t}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let shown = format!("postgres://{host}:{port}/{database} {what} as slot {slot}");
        let options = Options {
            host,
            port,
            database,
            username,
            password,
            tls,
            address: None,
        };
        Ok(Self::new(shown, options, tables, publication, slot, snapshot))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_url_is_host_port_and_database() {
        assert_eq!(
            endpoint("postgres://db.example.com/shop", "url").unwrap(),
            ("db.example.com".to_owned(), 5432, "shop".to_owned())
        );
        assert_eq!(endpoint("postgresql://10.0.0.4:6432/erp", "url").unwrap().1, 6432);
        assert_eq!(endpoint("postgres://[::1]:5433/x", "url").unwrap().0, "::1");
        for bad in [
            "mysql://db/shop",
            "postgres://db",
            "postgres://user:pw@db/shop",
            "postgres://db/shop?sslmode=require",
            "postgres://db:0/shop",
            "postgres:///shop",
        ] {
            assert!(endpoint(bad, "url").is_err(), "{bad}");
        }
    }

    #[test]
    fn names_are_as_postgresql_stores_them() {
        assert_eq!(
            table("orders", "t").unwrap(),
            ("public".to_owned(), "orders".to_owned())
        );
        assert_eq!(table("sales.order_lines", "t").unwrap().0, "sales");
        for bad in ["sales.", "1orders", "o rders", "a.b.c", "drop table x;--"] {
            assert!(table(bad, "t").is_err(), "{bad}");
        }
        assert_eq!(default_name("Orders-In"), "openduct_orders_in");
        assert_eq!(default_name(&"x".repeat(100)).len(), 63);
    }
}
