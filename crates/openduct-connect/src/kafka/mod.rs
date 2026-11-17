// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: kafka:` and `to: kafka:` - topics of another cluster that speaks the Kafka protocol (Kafka 2.4 or later,
//! or another OpenDuct), over TLS and SASL when it asks for them.
//!
//! Reading is as a member of a consumer group on that cluster, committing each partition's offset only once what
//! was read is stored; writing waits until every replica has a batch. A pipeline stopped and started again
//! carries on where it was, and nothing is lost on the way, though a message may come twice after a failure.
//!
//! ```yaml
//! from:
//!   kafka:
//!     brokers: b-1.example.com:9096, b-2.example.com:9096
//!     topics: [orders]
//!     tls: true
//!     sasl: scram-sha-512
//!     username: secret:KAFKA_USER
//!     password: secret:KAFKA_PASSWORD
//! ```

mod cluster;
mod group;
mod sink;
mod source;
mod wire;

use std::sync::Arc;

use kafka_protocol::records::Compression;
use openduct_pipeline::Connector;

use self::cluster::Cluster;
pub(crate) use self::sink::KafkaSink;
pub(crate) use self::source::KafkaSource;
use self::wire::{Access, Mechanism};
use crate::scram::Hash;
use crate::settings::{Limits, Settings};

const CONNECTION_SETTINGS: &[&str] = &[
    "brokers",
    "tls",
    "ca",
    "client_cert",
    "client_key",
    "sasl",
    "username",
    "password",
    "client_id",
];
const SOURCE_SETTINGS: &[&str] = &["topics", "group", "start"];
const SINK_SETTINGS: &[&str] = &["topic", "compression"];
const MAX_BROKERS: usize = 100;
const MAX_TOPICS: usize = 100;

fn allowed(own: &[&'static str]) -> Vec<&'static str> {
    CONNECTION_SETTINGS.iter().chain(own).copied().collect()
}

/// A list setting, written as a list or as text with commas (`b-1:9092, b-2:9092`), from `env:` or `file:` too.
/// Required: empty only when it comes from `env:` or `file:` and is only being checked.
fn names(s: &Settings<'_>, name: &str, what: &str) -> Result<Vec<String>, String> {
    let items = match s.text(name) {
        Ok(Some(text)) => vec![text],
        Ok(None) => return Err(format!("{}: is required: {what}", s.path(name))),
        Err(_) => s.list(name)?,
    };
    let names: Vec<String> = items
        .iter()
        .flat_map(|i| i.split([',', ' ', '\n']))
        .map(str::trim)
        .filter(|i| !i.is_empty())
        .map(str::to_owned)
        .collect();
    if names.is_empty() && !s.checking() {
        return Err(format!("{}: is required: {what}", s.path(name)));
    }
    Ok(names)
}

/// `host:port` (`[::1]:9092` for an IPv6 address); 9092 when no port is given.
fn broker(text: &str, at: &str) -> Result<(String, u16), String> {
    if text.contains("://") {
        return Err(format!(
            "{at}: {text:?} is host:port: how to connect goes in tls and sasl"
        ));
    }
    let (host, port) = match text.strip_prefix('[') {
        Some(rest) => {
            let (host, after) = rest
                .split_once(']')
                .ok_or_else(|| format!("{at}: {text:?} opens [ without closing it"))?;
            (host, after.strip_prefix(':'))
        }
        None => match text.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (text, None),
        },
    };
    let port = match port {
        None => 9092,
        Some(p) => p
            .parse()
            .ok()
            .filter(|p| *p > 0)
            .ok_or_else(|| format!("{at}: {p:?} is not a port"))?,
    };
    if host.is_empty() || host.contains(['/', '@', '?', '#']) {
        return Err(format!("{at}: {text:?} is not host:port"));
    }
    Ok((host.to_owned(), port))
}

/// A name a Kafka topic can have: 1 to 249 letters, digits, `.`, `_` and `-`.
fn topic_name(name: &str) -> bool {
    (1..=249).contains(&name.len())
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn plain_name(s: &Settings<'_>, name: &str, default: String) -> Result<String, String> {
    let value = s.text(name)?.unwrap_or(default);
    if value.is_empty() && s.checking() {
        return Ok(value);
    }
    if value.is_empty() || value.len() > 255 || value.chars().any(char::is_control) {
        return Err(format!("{}: is 1 to 255 characters", s.path(name)));
    }
    Ok(value)
}

/// The brokers and how to reach them, both sides alike.
fn cluster(s: &Settings<'_>, pipeline: &str) -> Result<(Cluster, String), String> {
    let at = s.path("brokers");
    let given = names(s, "brokers", "one or more host:port")?;
    for b in &given {
        s.reach_address(&s.path("brokers"), b, 9092)?;
    }
    if given.len() > MAX_BROKERS {
        return Err(format!("{at}: is at most {MAX_BROKERS} brokers"));
    }
    let brokers = given
        .iter()
        .map(|b| broker(b, &at))
        .collect::<Result<Vec<_>, _>>()?;
    let client_id = plain_name(s, "client_id", format!("openduct-{pipeline}"))?;
    let tls = if s.boolean("tls", false)? {
        Some(Arc::new(crate::tls::config(s)?))
    } else {
        for name in ["ca", "client_cert", "client_key"] {
            if s.text(name)?.is_some() {
                return Err(format!("{}: is for tls: true", s.path(name)));
            }
        }
        None
    };
    let username = s.text("username")?;
    let password = s.text("password")?;
    let sasl = match s.text("sasl")? {
        None => {
            if username.is_some() || password.is_some() {
                return Err(format!(
                    "{}: give sasl too: plain, scram-sha-256 or scram-sha-512",
                    s.path(if username.is_some() {
                        "username"
                    } else {
                        "password"
                    })
                ));
            }
            None
        }
        Some(given) => {
            let mechanism = match given.trim().to_ascii_lowercase().as_str() {
                "plain" => Mechanism::Plain,
                "scram-sha-256" => Mechanism::Scram(Hash::Sha256),
                "scram-sha-512" => Mechanism::Scram(Hash::Sha512),
                _ => {
                    return Err(format!(
                        "{}: is plain, scram-sha-256 or scram-sha-512, not {given:?}",
                        s.path("sasl")
                    ));
                }
            };
            if mechanism == Mechanism::Plain && tls.is_none() {
                return Err(format!(
                    "{}: plain sends the password as it is: use it with tls: true, or use scram-sha-512",
                    s.path("sasl")
                ));
            }
            let username =
                username.ok_or_else(|| format!("{}: is required with sasl", s.path("username")))?;
            let password =
                password.ok_or_else(|| format!("{}: is required with sasl", s.path("password")))?;
            if !s.checking() && (username.is_empty() || password.is_empty()) {
                return Err(format!("{}: and password are not empty", s.path("username")));
            }
            Some((mechanism, username, password))
        }
    };
    let first = brokers
        .first()
        .map(|(h, p)| format!("{h}:{p}"))
        .unwrap_or_default();
    let shown = match brokers.len() {
        0 => "the brokers given".to_owned(),
        1 => first,
        n => format!("{first} and {} more", n - 1),
    };
    let access = Access { tls, sasl, client_id };
    Ok((Cluster::new(brokers, access), shown))
}

fn topics(s: &Settings<'_>, name: &str) -> Result<Vec<String>, String> {
    let at = s.path(name);
    let topics = names(s, name, "one or more topics")?;
    if topics.len() > MAX_TOPICS {
        return Err(format!("{at}: is at most {MAX_TOPICS} topics"));
    }
    if let Some(bad) = topics.iter().find(|t| !topic_name(t)) {
        return Err(format!(
            "{at}: {bad:?} is not a Kafka topic name: 1 to 249 letters, digits, '.', '_' and '-'"
        ));
    }
    Ok(topics)
}

impl KafkaSource {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SOURCE_SETTINGS), limits)?;
        let (cluster, at) = cluster(&s, pipeline)?;
        let topics = topics(&s, "topics")?;
        let group = plain_name(&s, "group", format!("openduct-{pipeline}"))?;
        let start = match s.choice("start", &["earliest", "latest"])? {
            "latest" => -1,
            _ => -2,
        };
        let shown = format!("{} on {at} as group {group}", topics.join(", "));
        Ok(Self::new(shown, topics, group, start, cluster))
    }
}

impl KafkaSink {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SINK_SETTINGS), limits)?;
        let (cluster, at) = cluster(&s, pipeline)?;
        let topic = s.required("topic")?;
        if !topic_name(&topic) {
            return Err(format!(
                "{}: {topic:?} is not a Kafka topic name: 1 to 249 letters, digits, '.', '_' and '-'",
                s.path("topic")
            ));
        }
        let compression = match s.choice("compression", &["none", "gzip", "snappy", "lz4", "zstd"])? {
            "gzip" => Compression::Gzip,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            "zstd" => Compression::Zstd,
            _ => Compression::None,
        };
        let shown = format!("{topic} on {at}");
        Ok(Self::new(shown, topic, compression, cluster))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn brokers_are_host_and_port() {
        assert_eq!(
            broker("b-1.example.com:9096", "x").unwrap(),
            ("b-1.example.com".to_owned(), 9096)
        );
        assert_eq!(broker("kafka", "x").unwrap(), ("kafka".to_owned(), 9092));
        assert_eq!(broker("[::1]:19092", "x").unwrap(), ("::1".to_owned(), 19092));
        assert_eq!(broker("[fe80::1]", "x").unwrap(), ("fe80::1".to_owned(), 9092));
        for bad in [
            "SASL_SSL://b-1:9096",
            "b-1:0",
            "b-1:port",
            ":9092",
            "[::1",
            "user@b-1:9092",
        ] {
            assert!(broker(bad, "x").is_err(), "{bad}");
        }
    }

    #[test]
    fn topic_names_are_what_kafka_takes() {
        for good in ["orders", "factory.line-7_temps", &"a".repeat(249)] {
            assert!(topic_name(good), "{good}");
        }
        for bad in [
            "",
            ".",
            "..",
            "orders/eu",
            "has space",
            "ünïcode",
            &"a".repeat(250),
        ] {
            assert!(!topic_name(bad), "{bad}");
        }
    }
}
