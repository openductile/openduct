// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: servicenow:` and `to: servicenow:` - one table of a ServiceNow instance (incidents, changes, the CMDB),
//! read as records are created and updated, and written by insert, update or upsert, through the REST Table API.
//!
//! ```yaml
//! from:
//!   servicenow:
//!     instance: https://acme.service-now.com
//!     username: secret:SN_USER
//!     password: secret:SN_PASSWORD
//!     table: incident
//! ```

mod session;
mod sink;
mod source;

use std::time::Duration;

use openduct_pipeline::Connector;

use self::session::{Auth, Session};
use self::sink::Mode;
pub(crate) use self::sink::ServiceNowSink;
pub(crate) use self::source::ServiceNowSource;
use crate::settings::{Limits, Settings};

const SIGN_IN: &[&str] = &["instance", "username", "password", "client_id", "client_secret"];
const SOURCE: &[&str] = &["table", "fields", "query", "every"];
const SINK: &[&str] = &["table", "mode", "match"];

fn allowed(own: &[&'static str]) -> Vec<&'static str> {
    SIGN_IN.iter().chain(own).copied().collect()
}

/// A table or field name as ServiceNow keeps it: lower case letters, digits and `_` (`incident`, `u_asset`,
/// `x_acme_app_order`).
fn name(text: &str) -> bool {
    (1..=80).contains(&text.len())
        && text.starts_with(|c: char| c.is_ascii_lowercase())
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn session(s: &Settings<'_>) -> Result<(Session, String), String> {
    let instance = s.required("instance")?.trim_end_matches('/').to_owned();
    let local = instance.starts_with("http://127.0.0.1") || instance.starts_with("http://localhost");
    if !(instance.starts_with("https://") || local)
        || instance.contains(['?', '#', '@'])
        || instance.matches('/').count() > 2
    {
        return Err(format!(
            "{}: is the instance's address: https://acme.service-now.com",
            s.path("instance")
        ));
    }
    let given = (
        s.text("username")?,
        s.text("password")?,
        s.text("client_id")?,
        s.text("client_secret")?,
    );
    let auth = match given {
        (Some(username), Some(password), Some(client_id), Some(client_secret)) => Auth::Password {
            client_id,
            client_secret,
            username,
            password,
        },
        (None, None, Some(client_id), Some(client_secret)) => Auth::ClientCredentials {
            client_id,
            client_secret,
        },
        (Some(username), Some(password), None, None) => Auth::Basic { username, password },
        _ => {
            return Err(format!(
                "{}: give username and password, or client_id and client_secret, or all four (OAuth with a user)",
                s.path("username")
            ));
        }
    };
    s.reach_url(&s.path("instance"), &instance)?;
    let host = instance.split("://").nth(1).unwrap_or_default().to_owned();
    Ok((Session::new(instance, auth, s.private_network())?, host))
}

fn table(s: &Settings<'_>) -> Result<String, String> {
    let table = s.required("table")?;
    if name(&table) {
        Ok(table)
    } else {
        Err(format!(
            "{}: {table:?} is a table's name (incident, u_asset)",
            s.path("table")
        ))
    }
}

impl ServiceNowSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SOURCE), limits)?;
        let (session, host) = session(&s)?;
        let table = table(&s)?;
        let mut fields: Vec<String> = s
            .list("fields")?
            .iter()
            .flat_map(|f| f.split(','))
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .map(str::to_owned)
            .collect();
        if let Some(bad) = fields.iter().find(|f| !name(f)) {
            return Err(format!("{}: {bad:?} is not a field's name", s.path("fields")));
        }
        if !fields.is_empty() {
            for needed in ["sys_id", "sys_updated_on"] {
                if !fields.iter().any(|f| f == needed) {
                    fields.push(needed.to_owned());
                }
            }
        }
        let query = s.text("query")?;
        if query
            .as_deref()
            .is_some_and(|q| q.contains("ORDERBY") || q.contains("^NQ"))
        {
            return Err(format!(
                "{}: is conditions only; the pipeline orders the records itself",
                s.path("query")
            ));
        }
        let every = s.duration("every", Duration::from_secs(60))?;
        if every < Duration::from_secs(5) {
            return Err(format!("{}: is at least 5s", s.path("every")));
        }
        let shown = format!("{table} on {host} every {every:?}");
        Ok(Self::new(shown, session, table, fields, query, every))
    }
}

impl ServiceNowSink {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SINK), limits)?;
        let (session, host) = session(&s)?;
        let table = table(&s)?;
        let mode = match s.choice("mode", &["insert", "update", "upsert"])? {
            "update" => Mode::Update,
            "upsert" => Mode::Upsert,
            _ => Mode::Insert,
        };
        let match_on = s.text("match")?;
        match (&mode, &match_on) {
            (Mode::Upsert, None) => {
                return Err(format!(
                    "{}: is required to upsert: the field a record is found by",
                    s.path("match")
                ));
            }
            (_, Some(f)) if !name(f) => {
                return Err(format!("{}: {f:?} is not a field's name", s.path("match")));
            }
            _ => {}
        }
        let how = match mode {
            Mode::Insert => "insert".to_owned(),
            Mode::Update => "update".to_owned(),
            Mode::Upsert => format!("upsert by {}", match_on.as_deref().unwrap_or("")),
        };
        let shown = format!("{table} on {host} ({how})");
        Ok(Self {
            shown,
            session,
            table,
            mode,
            match_on,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_as_servicenow_keeps_them() {
        for good in ["incident", "u_asset", "x_acme_app_order", "cmdb_ci2"] {
            assert!(name(good), "{good}");
        }
        for bad in ["", "Incident", "1incident", "incident^ORactive=false", "u asset"] {
            assert!(!name(bad), "{bad}");
        }
    }
}
