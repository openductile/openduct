// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: opcua:` - named values of a machine, PLC or SCADA server, read as they change through OPC UA, signed and
//! encrypted unless the pipeline says otherwise.
//!
//! ```yaml
//! from:
//!   opcua:
//!     url: opc.tcp://10.0.4.21:4840
//!     device: press-07
//!     read:
//!       temperature: ns=2;s=Press7.Temperature
//!       running: ns=2;s=Press7.Running
//! ```

mod source;
mod values;

use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use opcua::crypto::SecurityPolicy;
use opcua::types::{MessageSecurityMode, NodeId};
use openduct_pipeline::Connector;

pub(crate) use self::source::OpcUaSource;
use self::source::{Settings as Read, User};
use crate::settings::{Limits, Settings};

const ALLOWED: &[&str] = &[
    "url", "read", "every", "deadband", "device", "security", "policy", "username", "password", "pki",
];

/// The most values one pipeline reads.
const MOST_VALUES: usize = 10_000;

pub(super) fn mode_name(mode: MessageSecurityMode) -> &'static str {
    match mode {
        MessageSecurityMode::Sign => "sign",
        MessageSecurityMode::SignAndEncrypt => "sign_and_encrypt",
        _ => "none",
    }
}

/// A policy by its name in the OPC UA specification.
pub(super) fn policy_name(policy: SecurityPolicy) -> &'static str {
    match policy {
        SecurityPolicy::Basic256Sha256 => "Basic256Sha256",
        SecurityPolicy::Aes128Sha256RsaOaep => "Aes128_Sha256_RsaOaep",
        SecurityPolicy::Aes256Sha256RsaPss => "Aes256_Sha256_RsaPss",
        _ => "None",
    }
}

/// A value's name: letters, digits, `_` and `-`.
fn value_name(text: &str) -> bool {
    (1..=100).contains(&text.len())
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

impl OpcUaSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, ALLOWED, limits)?;
        let url = s.required("url")?.trim_end_matches('/').to_owned();
        let address = url.strip_prefix("opc.tcp://").unwrap_or_default();
        let host_port = address.split('/').next().unwrap_or_default().to_owned();
        if host_port.is_empty() || address.contains(['@', '?', '#', ' ']) {
            return Err(format!(
                "{}: is the server's address: opc.tcp://host:4840",
                s.path("url")
            ));
        }
        let host_port = if host_port.contains(':') {
            host_port
        } else {
            format!("{host_port}:4840")
        };
        s.reach_address(&s.path("url"), &host_port, 4840)?;
        let path = address.find('/').and_then(|i| address.get(i..)).unwrap_or("");
        let url = format!("opc.tcp://{host_port}{path}");

        let mut read = Vec::new();
        let mut seen = HashSet::new();
        for (name, node) in s.pairs("read")? {
            if !value_name(&name) {
                return Err(format!(
                    "{}.{name}: a value's name is letters, digits, _ and -",
                    s.path("read")
                ));
            }
            let node = NodeId::from_str(node.trim()).map_err(|_| {
                format!(
                    "{}.{name}: {node:?} is not a node id: write ns=2;s=Press7.Temperature or ns=3;i=1001",
                    s.path("read")
                )
            })?;
            if !seen.insert(node.clone()) {
                return Err(format!("{}.{name}: {node} is read twice", s.path("read")));
            }
            read.push((name, node));
        }
        if read.is_empty() {
            return Err(format!(
                "{}: is required: each value's name and node id (temperature: ns=2;s=Press7.Temperature)",
                s.path("read")
            ));
        }
        if read.len() > MOST_VALUES {
            return Err(format!("{}: is at most {MOST_VALUES} values", s.path("read")));
        }

        let every = s.duration("every", Duration::from_secs(1))?;
        if every < Duration::from_millis(100) {
            return Err(format!("{}: is at least 100ms", s.path("every")));
        }
        let deadband = match s.text("deadband")? {
            None => None,
            Some(text) => Some(
                text.trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|d| d.is_finite() && *d >= 0.0)
                    .ok_or_else(|| {
                        format!("{}: is how much a number must change: 0.5", s.path("deadband"))
                    })?,
            ),
        };

        let mode = match s.choice("security", &["sign_and_encrypt", "sign", "none"])? {
            "sign" => MessageSecurityMode::Sign,
            "none" => MessageSecurityMode::None,
            _ => MessageSecurityMode::SignAndEncrypt,
        };
        let policy = match (mode, s.text("policy")?) {
            (MessageSecurityMode::None, Some(_)) => {
                return Err(format!(
                    "{}: is for security sign or sign_and_encrypt",
                    s.path("policy")
                ));
            }
            (MessageSecurityMode::None, None) => SecurityPolicy::None,
            _ => match s.choice(
                "policy",
                &["Basic256Sha256", "Aes128_Sha256_RsaOaep", "Aes256_Sha256_RsaPss"],
            )? {
                "Aes128_Sha256_RsaOaep" => SecurityPolicy::Aes128Sha256RsaOaep,
                "Aes256_Sha256_RsaPss" => SecurityPolicy::Aes256Sha256RsaPss,
                _ => SecurityPolicy::Basic256Sha256,
            },
        };
        let user = match (s.text("username")?, s.text("password")?) {
            (None, None) => User::Anonymous,
            (Some(user), Some(password)) => User::Password(user, password),
            _ => {
                return Err(format!(
                    "{}: give username and password together",
                    s.path("username")
                ));
            }
        };
        let pki = PathBuf::from(s.text("pki")?.unwrap_or_else(|| "opcua-pki".to_owned()));
        let device = s.text("device")?.unwrap_or_else(|| host_port.clone());
        let shown = format!(
            "{} values on {url} every {every:?} ({})",
            read.len(),
            match mode {
                MessageSecurityMode::None => "no security".to_owned(),
                _ => format!("{}, {}", mode_name(mode), policy_name(policy)),
            }
        );
        Ok(Self::new(
            shown,
            Read {
                url,
                device,
                read,
                every,
                deadband,
                mode,
                policy,
                user,
                pki,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_plain() {
        assert!(value_name("temperature") && value_name("line-2_speed"));
        assert!(!value_name("") && !value_name("a b") && !value_name("a.b"));
        assert_eq!(mode_name(MessageSecurityMode::SignAndEncrypt), "sign_and_encrypt");
    }
}
