// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: snowflake:` and `to: snowflake:` - one table of a Snowflake account, read as rows are added or changed,
//! and written by insert or merge, through Snowflake's SQL API.
//!
//! ```yaml
//! to:
//!   snowflake:
//!     account: myorg-myaccount
//!     user: LOADER
//!     private_key: file:/etc/openduct/snowflake.p8
//!     warehouse: LOADING
//!     database: SHOP
//!     schema: PUBLIC
//!     table: ORDERS
//! ```

mod session;
mod sink;
mod source;
mod values;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use openduct_pipeline::Connector;

use self::session::{Auth, Context, Session};
use self::sink::Mode;
pub(crate) use self::sink::SnowflakeSink;
pub(crate) use self::source::SnowflakeSource;
use self::values::plain_name;
use crate::settings::{Limits, Settings};

const SIGN_IN: &[&str] = &[
    "account",
    "url",
    "user",
    "private_key",
    "token",
    "warehouse",
    "database",
    "schema",
    "role",
];
const SOURCE: &[&str] = &["table", "columns", "track", "id", "where", "every"];
const SINK: &[&str] = &["table", "mode", "match"];

fn allowed(own: &[&'static str]) -> Vec<&'static str> {
    SIGN_IN.iter().chain(own).copied().collect()
}

/// The account as its address names it, and as a JWT names it. `myorg-myaccount` (or `myorg.myaccount`) is both;
/// a locator with its region (`xy12345.us-east-2.aws`) is named by the locator alone in a JWT.
fn account_names(account: &str) -> (String, String) {
    let with_region = account.split('.').nth(1).is_some_and(|r| r.contains('-'));
    if with_region {
        let locator = account.split('.').next().unwrap_or(account);
        (account.to_ascii_lowercase(), locator.to_ascii_uppercase())
    } else {
        let named = account.replace('.', "-");
        (named.to_ascii_lowercase(), named.to_ascii_uppercase())
    }
}

/// The key's fingerprint as Snowflake shows it (`DESCRIBE USER`'s `RSA_PUBLIC_KEY_FP`).
fn fingerprint(key: &ring::signature::RsaKeyPair) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, &crate::jwt::public_key_info(key));
    format!("SHA256:{}", STANDARD.encode(digest.as_ref()))
}

/// A name as Snowflake keeps an object, for where statements run: given as `SHOW` lists it.
fn object_name(s: &Settings<'_>, name: &str) -> Result<Option<String>, String> {
    match s.text(name)? {
        Some(v) if v.is_empty() || v.len() > 255 || v.chars().any(char::is_control) => {
            Err(format!("{}: is a name, as SHOW lists it", s.path(name)))
        }
        other => Ok(other),
    }
}

fn session(s: &Settings<'_>) -> Result<(Session, String), String> {
    let account = s.required("account")?;
    if account.is_empty()
        || account.len() > 255
        || account.starts_with('.')
        || account.ends_with('.')
        || !account
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(format!(
            "{}: is the account identifier, as in its address: myorg-myaccount",
            s.path("account")
        ));
    }
    let (host, jwt_account) = account_names(&account);
    let base = match s.text("url")? {
        None => format!("https://{host}.snowflakecomputing.com"),
        Some(url) => {
            let url = url.trim_end_matches('/').to_owned();
            let local = url.starts_with("http://127.0.0.1") || url.starts_with("http://localhost");
            if !(url.starts_with("https://") || local)
                || url.contains(['?', '#', '@'])
                || url.matches('/').count() > 2
            {
                return Err(format!(
                    "{}: is the account's address, https:// (a private link's, say)",
                    s.path("url")
                ));
            }
            url
        }
    };
    let auth = match (s.text("private_key")?, s.text("token")?) {
        (Some(pem), None) => {
            let user = s
                .text("user")?
                .filter(|u| !u.is_empty() || s.checking())
                .ok_or_else(|| format!("{}: is required with private_key", s.path("user")))?;
            if s.checking() {
                Auth::Token(String::new())
            } else {
                let key =
                    crate::jwt::private_key(&pem).map_err(|e| format!("{}: {e}", s.path("private_key")))?;
                Auth::KeyPair {
                    account: jwt_account,
                    user: user.to_ascii_uppercase(),
                    fingerprint: fingerprint(&key),
                    key: Arc::new(key),
                }
            }
        }
        (None, Some(token)) => Auth::Token(token),
        (Some(_), Some(_)) => {
            return Err(format!(
                "{} and {}: give one of them",
                s.path("private_key"),
                s.path("token")
            ));
        }
        (None, None) => {
            return Err(format!(
                "{}: is required (with user), or token: Snowflake's API takes a key pair or a token",
                s.path("private_key")
            ));
        }
    };
    let context = Context {
        warehouse: object_name(s, "warehouse")?,
        database: object_name(s, "database")?,
        schema: object_name(s, "schema")?,
        role: object_name(s, "role")?,
    };
    s.reach_url(&s.path("url"), &base)?;
    let host = base.split("://").nth(1).unwrap_or_default().to_owned();
    Ok((Session::new(base, auth, context, s.private_network())?, host))
}

/// The table: `ORDERS`, or with its schema and database (`SHOP.PUBLIC.ORDERS`).
fn table(s: &Settings<'_>) -> Result<String, String> {
    let table = s.required("table")?;
    let parts: Vec<&str> = table.split('.').collect();
    if parts.len() <= 3 && parts.iter().all(|p| plain_name(p)) {
        Ok(table)
    } else {
        Err(format!(
            "{}: {table:?} is a table's name (ORDERS, or SHOP.PUBLIC.ORDERS)",
            s.path("table")
        ))
    }
}

fn column_name(s: &Settings<'_>, setting: &str, name: &str) -> Result<String, String> {
    if plain_name(name) {
        Ok(name.to_owned())
    } else {
        Err(format!("{}: {name:?} is not a column's name", s.path(setting)))
    }
}

fn names(s: &Settings<'_>, setting: &str) -> Result<Vec<String>, String> {
    s.list(setting)?
        .iter()
        .flat_map(|c| c.split(','))
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(|c| column_name(s, setting, c))
        .collect()
}

impl SnowflakeSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SOURCE), limits)?;
        let (session, host) = session(&s)?;
        let table = table(&s)?;
        let wanted = names(&s, "columns")?;
        let track = column_name(&s, "track", &s.required("track")?)?;
        let id = s.text("id")?.map(|i| column_name(&s, "id", &i)).transpose()?;
        let filter = s.text("where")?;
        if filter
            .as_deref()
            .is_some_and(|f| f.contains(';') || f.to_ascii_uppercase().contains("ORDER BY"))
        {
            return Err(format!(
                "{}: is conditions only; the pipeline orders the rows itself",
                s.path("where")
            ));
        }
        let every = s.duration("every", Duration::from_secs(60))?;
        if every < Duration::from_secs(5) {
            return Err(format!("{}: is at least 5s", s.path("every")));
        }
        let shown = format!("{table} on {host} every {every:?}");
        Ok(Self::new(shown, session, table, wanted, track, id, filter, every))
    }
}

impl SnowflakeSink {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SINK), limits)?;
        let (session, host) = session(&s)?;
        let table = table(&s)?;
        let mode = match s.choice("mode", &["insert", "merge"])? {
            "merge" => Mode::Merge,
            _ => Mode::Insert,
        };
        let matching = names(&s, "match")?;
        match (mode, matching.is_empty()) {
            (Mode::Merge, true) => {
                return Err(format!(
                    "{}: is required to merge: the columns a row is found by",
                    s.path("match")
                ));
            }
            (Mode::Insert, false) => {
                return Err(format!("{}: is for mode: merge", s.path("match")));
            }
            _ => {}
        }
        let how = match mode {
            Mode::Insert => "insert".to_owned(),
            Mode::Merge => format!("merge by {}", matching.join(", ")),
        };
        let shown = format!("{table} on {host} ({how})");
        Ok(Self {
            shown,
            session,
            table,
            mode,
            matching,
            columns: None,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn accounts_are_named_as_the_address_and_the_jwt_need() {
        let cases = [
            ("myorg-myaccount", "myorg-myaccount", "MYORG-MYACCOUNT"),
            ("myorg.myaccount", "myorg-myaccount", "MYORG-MYACCOUNT"),
            ("MyOrg-My_Account", "myorg-my_account", "MYORG-MY_ACCOUNT"),
            ("xy12345.us-east-2.aws", "xy12345.us-east-2.aws", "XY12345"),
            ("xy12345.east-us-2.azure", "xy12345.east-us-2.azure", "XY12345"),
            ("xy12345", "xy12345", "XY12345"),
        ];
        for (given, host, jwt) in cases {
            assert_eq!(account_names(given), (host.to_owned(), jwt.to_owned()), "{given}");
        }
    }
}
