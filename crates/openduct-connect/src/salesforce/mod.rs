// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: salesforce:` and `to: salesforce:` - one Salesforce object's records, read as they change and written
//! by upsert, through the REST API with OAuth 2.0. Nothing is installed in Salesforce: a connected app (or an
//! external client app) gives the pipeline its client id.
//!
//! ```yaml
//! from:
//!   salesforce:
//!     login_url: https://acme.my.salesforce.com
//!     client_id: secret:SF_CLIENT_ID
//!     client_secret: secret:SF_CLIENT_SECRET
//!     object: Account
//! ```

mod session;
mod sink;
mod source;

use std::sync::Arc;
use std::time::Duration;

use openduct_pipeline::Connector;

use self::session::{Auth, Session};
use self::sink::Mode;
pub(crate) use self::sink::SalesforceSink;
pub(crate) use self::source::SalesforceSource;
use crate::settings::{Limits, Settings};

const SIGN_IN: &[&str] = &[
    "login_url",
    "client_id",
    "client_secret",
    "username",
    "private_key",
    "refresh_token",
    "api_version",
];
const SOURCE: &[&str] = &["object", "fields", "where", "every"];
const SINK: &[&str] = &["object", "mode", "external_id"];

fn allowed(own: &[&'static str]) -> Vec<&'static str> {
    SIGN_IN.iter().chain(own).copied().collect()
}

/// A Salesforce API name: an object or a field (`Account`, `Invoice__c`, `ns__Amount__c`).
fn api_name(name: &str) -> bool {
    (1..=80).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The login URL, the sign-in, and the API version, both sides alike.
fn session(s: &Settings<'_>) -> Result<(Session, String), String> {
    let login_url = s.required("login_url")?.trim_end_matches('/').to_owned();
    let local = login_url.starts_with("http://127.0.0.1") || login_url.starts_with("http://localhost");
    if !(login_url.starts_with("https://") || local) || login_url.contains(['?', '#', '@']) {
        return Err(format!(
            "{}: is https://, the org's My Domain (https://acme.my.salesforce.com) or https://login.salesforce.com",
            s.path("login_url")
        ));
    }
    let client_id = s.required("client_id")?;
    let client_secret = s.text("client_secret")?;
    let auth = match (s.text("private_key")?, s.text("refresh_token")?) {
        (Some(pem), None) => {
            let username = s
                .text("username")?
                .ok_or_else(|| format!("{}: is required with private_key", s.path("username")))?;
            let key = if s.checking() {
                None
            } else {
                Some(Arc::new(
                    crate::jwt::private_key(&pem).map_err(|e| format!("{}: {e}", s.path("private_key")))?,
                ))
            };
            match key {
                Some(key) => Auth::Jwt {
                    client_id,
                    username,
                    key,
                },
                None => Auth::ClientCredentials {
                    client_id,
                    client_secret: String::new(),
                },
            }
        }
        (None, Some(refresh_token)) => Auth::Refresh {
            client_id,
            client_secret,
            refresh_token,
        },
        (None, None) => {
            let client_secret = client_secret.ok_or_else(|| {
                format!(
                    "{}: is required, or private_key and username (a JWT), or refresh_token",
                    s.path("client_secret")
                )
            })?;
            Auth::ClientCredentials {
                client_id,
                client_secret,
            }
        }
        (Some(_), Some(_)) => {
            return Err(format!(
                "{} and {}: give one of them",
                s.path("private_key"),
                s.path("refresh_token")
            ));
        }
    };
    let api_version = s.text("api_version")?.unwrap_or_else(|| "64.0".to_owned());
    let ok = api_version
        .split_once('.')
        .is_some_and(|(m, n)| m.len() == 2 && m.bytes().all(|b| b.is_ascii_digit()) && n == "0");
    if !ok {
        return Err(format!("{}: is a version such as 64.0", s.path("api_version")));
    }
    s.reach_url(&s.path("login_url"), &login_url)?;
    let host = login_url.split("://").nth(1).unwrap_or_default().to_owned();
    Ok((
        Session::new(login_url, auth, api_version, s.private_network())?,
        host,
    ))
}

fn object(s: &Settings<'_>) -> Result<String, String> {
    let object = s.required("object")?;
    if api_name(&object) {
        Ok(object)
    } else {
        Err(format!(
            "{}: {object:?} is a Salesforce object's API name (Account, Invoice__c)",
            s.path("object")
        ))
    }
}

impl SalesforceSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SOURCE), limits)?;
        let (session, host) = session(&s)?;
        let object = object(&s)?;
        let fields = s
            .list("fields")?
            .iter()
            .flat_map(|f| f.split(','))
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if let Some(bad) = fields.iter().find(|f| !api_name(f)) {
            return Err(format!("{}: {bad:?} is not a field's API name", s.path("fields")));
        }
        let filter = s.text("where")?;
        let every = s.duration("every", Duration::from_secs(60))?;
        if every < Duration::from_secs(5) {
            return Err(format!(
                "{}: is at least 5s (Salesforce counts API calls)",
                s.path("every")
            ));
        }
        let shown = format!("{object} on {host} every {every:?}");
        Ok(Self::new(shown, session, object, fields, filter, every))
    }
}

impl SalesforceSink {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SINK), limits)?;
        let (session, host) = session(&s)?;
        let object = object(&s)?;
        let mode = match s.choice("mode", &["upsert", "insert", "update"])? {
            "insert" => Mode::Insert,
            "update" => Mode::Update,
            _ => Mode::Upsert,
        };
        let external_id = s.text("external_id")?;
        match (&mode, &external_id) {
            (Mode::Upsert, None) => {
                return Err(format!(
                    "{}: is required to upsert: the field records are matched by (an external id field, or Id)",
                    s.path("external_id")
                ));
            }
            (_, Some(id)) if !api_name(id) => {
                return Err(format!(
                    "{}: {id:?} is not a field's API name",
                    s.path("external_id")
                ));
            }
            _ => {}
        }
        let how = match mode {
            Mode::Upsert => format!("upsert by {}", external_id.as_deref().unwrap_or("Id")),
            Mode::Insert => "insert".to_owned(),
            Mode::Update => "update".to_owned(),
        };
        let shown = format!("{object} on {host} ({how})");
        Ok(Self {
            shown,
            session,
            object,
            mode,
            external_id,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn api_names() {
        for good in ["Account", "Invoice__c", "ns__Amount__c", "Id"] {
            assert!(api_name(good), "{good}");
        }
        for bad in [
            "",
            "1Account",
            "Account; DELETE",
            "Na me",
            "Name'",
            &"a".repeat(81),
        ] {
            assert!(!api_name(bad), "{bad}");
        }
    }
}
