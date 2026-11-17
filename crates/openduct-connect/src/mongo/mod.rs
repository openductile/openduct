// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: mongodb_cdc:` - change data capture from MongoDB (a replica set or a sharded cluster, MongoDB 6.0 or
//! later, MongoDB Atlas included) through its change streams, with MongoDB's own Rust driver.
//!
//! ```yaml
//! from:
//!   mongodb_cdc:
//!     url: mongodb+srv://cluster0.abcde.mongodb.net
//!     username: secret:MONGO_USER
//!     password: secret:MONGO_PASSWORD
//!     collections: [shop.orders, shop.customers]
//! ```

mod source;
mod values;

use std::path::PathBuf;
use std::time::Duration;

use mongodb::options::{ClientOptions, Credential, Tls, TlsOptions};
use openduct_pipeline::Connector;

pub(crate) use self::source::MongoCdcSource;
use crate::settings::{Limits, Settings};

const SETTINGS: &[&str] = &[
    "url",
    "username",
    "password",
    "auth_source",
    "tls",
    "ca",
    "client_cert",
    "client_key",
    "collections",
    "snapshot",
];
const MAX_COLLECTIONS: usize = 500;

/// How to reach the server, kept until the pipeline starts: an `mongodb+srv://` address is looked up in DNS then.
#[derive(Clone)]
pub(super) struct Connect {
    url: String,
    username: Option<String>,
    password: Option<String>,
    auth_source: String,
    tls: bool,
    ca: Option<String>,
    client_identity: Option<String>,
}

impl std::fmt::Debug for Connect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connect")
            .field("url", &self.url)
            .field("username", &self.username)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl Connect {
    /// The driver's options: the address's own (hosts, replica set, TLS from `+srv`), the login, and TLS.
    pub(super) async fn options(&self, pipeline: &str) -> Result<ClientOptions, String> {
        let mut options = ClientOptions::parse(&self.url)
            .await
            .map_err(|e| format!("cannot use the MongoDB address: {e}"))?;
        options.app_name = Some(format!("openduct-{pipeline}"));
        options.server_selection_timeout = Some(Duration::from_secs(10));
        options.connect_timeout = Some(Duration::from_secs(10));
        if let Some(username) = &self.username {
            options.credential = Some(
                Credential::builder()
                    .username(username.clone())
                    .password(self.password.clone())
                    .source(self.auth_source.clone())
                    .build(),
            );
        }
        if self.tls || self.ca.is_some() || self.client_identity.is_some() {
            let mut tls = TlsOptions::default();
            if let Some(pem) = &self.ca {
                tls.ca_file_path = Some(private_file("ca", pem)?);
            }
            if let Some(pem) = &self.client_identity {
                tls.cert_key_file_path = Some(private_file("client", pem)?);
            }
            options.tls = Some(Tls::Enabled(tls));
        }
        Ok(options)
    }
}

/// PEM the driver reads from a file, written where only this process's user may read it.
fn private_file(what: &str, pem: &str) -> Result<PathBuf, String> {
    use std::io::Write;
    let path = std::env::temp_dir().join(format!("openduct-{}-mongodb-{what}.pem", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|e| format!("cannot write the TLS {what} for MongoDB: {e}"))?;
    file.write_all(pem.as_bytes())
        .map_err(|e| format!("cannot write the TLS {what} for MongoDB: {e}"))?;
    Ok(path)
}

/// `mongodb://host[:port][,host...][/?options]` or `mongodb+srv://name[/?options]`, without a login in it.
fn check_url(url: &str, at: &str) -> Result<String, String> {
    let rest = url
        .strip_prefix("mongodb://")
        .or_else(|| url.strip_prefix("mongodb+srv://"))
        .ok_or_else(|| format!("{at}: is mongodb://host:port or mongodb+srv://name"))?;
    let hosts = rest.split(['/', '?']).next().unwrap_or_default();
    if hosts.contains('@') {
        return Err(format!(
            "{at}: has no username or password in it: they are settings of their own"
        ));
    }
    if rest.to_ascii_lowercase().contains("password=") {
        return Err(format!("{at}: has no password in it: it is a setting of its own"));
    }
    if hosts.is_empty() {
        return Err(format!("{at}: names no host"));
    }
    Ok(hosts.to_owned())
}

/// A database or collection name MongoDB takes: no spaces, `$`, `/`, `\`, `"` or `.` in a database name.
fn collection(text: &str, at: &str) -> Result<(String, String), String> {
    match text.split_once('.') {
        Some((db, coll))
            if (1..=63).contains(&db.len())
                && !db.contains(['/', '\\', ' ', '"', '$', '*', '<', '>', ':', '|', '?'])
                && (1..=200).contains(&coll.len())
                && !coll.contains(['$', '\0'])
                && !coll.starts_with("system.") =>
        {
            Ok((db.to_owned(), coll.to_owned()))
        }
        _ => Err(format!("{at}: {text:?} is database.collection")),
    }
}

impl MongoCdcSource {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let url = s.required("url")?;
        let hosts = check_url(&url, &s.path("url"))?;
        for host in hosts.split(',') {
            s.reach_address(&s.path("url"), host.trim(), 27017)?;
        }
        let username = s.text("username")?;
        let password = s.text("password")?;
        if password.is_some() && username.is_none() {
            return Err(format!("{}: needs a username too", s.path("password")));
        }
        let auth_source = s.text("auth_source")?.unwrap_or_else(|| "admin".to_owned());
        let tls = s.boolean("tls", false)?;
        let ca = s.text("ca")?;
        let client_identity = match (s.text("client_cert")?, s.text("client_key")?) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(format!("{cert}\n{key}\n")),
            _ => {
                return Err(format!(
                    "{} and {} go together",
                    s.path("client_cert"),
                    s.path("client_key")
                ));
            }
        };
        let at = s.path("collections");
        let collections = s
            .list("collections")?
            .iter()
            .flat_map(|c| c.split(','))
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .map(|c| collection(c, &at))
            .collect::<Result<Vec<_>, _>>()?;
        if collections.is_empty() {
            return Err(format!("{at}: is required: one or more database.collection"));
        }
        if collections.len() > MAX_COLLECTIONS {
            return Err(format!("{at}: is at most {MAX_COLLECTIONS} collections"));
        }
        let snapshot = s.boolean("snapshot", true)?;
        let names: Vec<String> = collections.iter().map(|(d, c)| format!("{d}.{c}")).collect();
        let shown = format!("{hosts} {}", names.join(", "));
        let connect = Connect {
            url,
            username,
            password,
            auth_source,
            tls,
            ca,
            client_identity,
        };
        Ok(Self::new(
            shown,
            connect,
            pipeline.to_owned(),
            collections,
            snapshot,
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn addresses_and_collections() {
        assert_eq!(
            check_url("mongodb+srv://cluster0.ab.mongodb.net", "u").unwrap(),
            "cluster0.ab.mongodb.net"
        );
        assert_eq!(
            check_url("mongodb://h1:27017,h2:27017/?replicaSet=rs0", "u").unwrap(),
            "h1:27017,h2:27017"
        );
        for bad in [
            "mongo://h",
            "mongodb://u:p@h",
            "mongodb://h/?password=x",
            "mongodb:///x",
            "postgres://h/db",
        ] {
            assert!(check_url(bad, "u").is_err(), "{bad}");
        }
        assert_eq!(
            collection("shop.order.lines", "c").unwrap(),
            ("shop".to_owned(), "order.lines".to_owned())
        );
        for bad in [
            "orders",
            "sh op.orders",
            "shop.",
            "shop.sys$",
            "shop.system.users",
            ".orders",
        ] {
            assert!(collection(bad, "c").is_err(), "{bad}");
        }
    }
}
