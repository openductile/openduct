// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The tokens an AI assistant signs in to a workspace's MCP server with: made by the workspace's admins, shown once,
//! kept only as their SHA-256, each limited to some of the workspace's tools (or all) and to so many requests a
//! minute, and revoked with a click. A wrong token slows its address down, as a wrong password does.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};

/// Where tokens are kept.
pub(crate) const MCP_TOKENS: &str = "__openduct.model.mcp-tokens";
/// What every token begins with, so one found in a file is known for what it is.
const PREFIX: &str = "odmcp_";
/// Requests a minute a token makes: by default, and at most.
pub(crate) const PER_MINUTE: u32 = 60;
pub(crate) const MAX_PER_MINUTE: u32 = 1000;
/// The most tokens a workspace has at once.
pub(crate) const MAX_TOKENS: usize = 100;
/// How long a token found once is trusted before the record is read again (a revocation on another node).
const FRESH: Duration = Duration::from_secs(2);
/// The most calls running at once in a workspace, on each node.
pub(crate) const AT_ONCE: usize = 16;

/// A token, as kept: never the token itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct McpToken {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    /// What it is for, as its maker named it (an assistant, a person).
    pub name: String,
    /// The SHA-256 of the token, in hex: what it is found by.
    pub hash: String,
    /// Its last four characters, to tell it from others.
    pub hint: String,
    /// The workflows (by id) whose tools it may list and call; none: every tool of the workspace.
    #[serde(default)]
    pub tools: Option<Vec<String>>,
    pub per_minute: u32,
    pub created: u64,
    pub by: String,
    #[serde(default)]
    pub revoked: Option<u64>,
}

impl McpToken {
    /// Whether it may reach the tool of workflow `id`.
    pub(crate) fn reaches(&self, id: &str) -> bool {
        self.tools.as_ref().is_none_or(|t| t.iter().any(|w| w == id))
    }
}

/// The SHA-256 of a token, in hex.
pub(crate) fn hash(token: &str) -> String {
    digest::digest(&digest::SHA256, token.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A new token: 32 random bytes, URL-safe.
pub(crate) fn new_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "no randomness to make a token with".to_owned())?;
    Ok(format!(
        "{PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    ))
}

/// Whether a text could be a token at all: what is looked up is only ever one.
pub(crate) fn well_formed(token: &str) -> bool {
    token.len() == PREFIX.len() + 43
        && token.starts_with(PREFIX)
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// What each node keeps of the MCP server in memory: each token's recent requests, the calls running in each
/// workspace, failed sign-ins by address, and when the tokens were last read.
#[derive(Debug, Default)]
pub(crate) struct Limits {
    requests: Mutex<HashMap<String, VecDeque<Instant>>>,
    running: Mutex<HashMap<String, usize>>,
    pub(crate) failures: crate::auth::Failures,
    read: Mutex<Option<Instant>>,
}

/// A call counted as running in its workspace until dropped.
#[derive(Debug)]
pub(crate) struct Running<'a> {
    limits: &'a Limits,
    workspace: String,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        if let Ok(mut running) = self.limits.running.lock()
            && let Some(n) = running.get_mut(&self.workspace)
        {
            *n = n.saturating_sub(1);
            if *n == 0 {
                running.remove(&self.workspace);
            }
        }
    }
}

impl Limits {
    /// Whether the token may make one more request now (its count this minute under its limit); if not, in how many
    /// seconds it may.
    pub(crate) fn allow(&self, token: &McpToken) -> Result<(), u64> {
        let Ok(mut requests) = self.requests.lock() else {
            return Err(60);
        };
        if requests.len() > 100_000 {
            requests.retain(|_, seen| seen.back().is_some_and(|t| t.elapsed() < Duration::from_secs(60)));
        }
        let seen = requests.entry(token.id.clone()).or_default();
        while seen
            .front()
            .is_some_and(|t| t.elapsed() >= Duration::from_secs(60))
        {
            seen.pop_front();
        }
        let limit = usize::try_from(token.per_minute.clamp(1, MAX_PER_MINUTE)).unwrap_or(1);
        if seen.len() >= limit {
            let oldest = seen.front().map_or(Duration::ZERO, Instant::elapsed);
            return Err(60u64.saturating_sub(oldest.as_secs()).max(1));
        }
        seen.push_back(Instant::now());
        Ok(())
    }

    /// One more call running in `workspace`, if fewer than [`AT_ONCE`] are.
    pub(crate) fn start(&self, workspace: &str) -> Option<Running<'_>> {
        let mut running = self.running.lock().ok()?;
        let n = running.entry(workspace.to_owned()).or_insert(0);
        if *n >= AT_ONCE {
            return None;
        }
        *n += 1;
        Some(Running {
            limits: self,
            workspace: workspace.to_owned(),
        })
    }

    /// Whether the tokens should be read again before one is trusted.
    pub(crate) fn stale(&self) -> bool {
        let Ok(mut read) = self.read.lock() else {
            return true;
        };
        if read.is_some_and(|at| at.elapsed() < FRESH) {
            return false;
        }
        *read = Some(Instant::now());
        true
    }

    pub(crate) fn may_try(&self, ip: IpAddr) -> bool {
        self.failures.may_try(ip)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn token(per_minute: u32) -> McpToken {
        McpToken {
            schema: 1,
            id: "t1".into(),
            workspace: "w".into(),
            name: "assistant".into(),
            hash: String::new(),
            hint: String::new(),
            tools: Some(vec!["a".into()]),
            per_minute,
            created: 0,
            by: "u".into(),
            revoked: None,
        }
    }

    #[test]
    fn a_token_is_random_well_formed_and_kept_by_its_hash() {
        let (a, b) = (new_token().unwrap(), new_token().unwrap());
        assert_ne!(a, b);
        assert!(well_formed(&a) && well_formed(&b));
        assert!(
            !well_formed("odmcp_short")
                && !well_formed(&format!("{a}x"))
                && !well_formed(&a.replace('_', "/"))
        );
        assert_eq!(hash(&a).len(), 64);
        assert_ne!(hash(&a), hash(&b));
    }

    #[test]
    fn a_token_makes_so_many_requests_a_minute_and_reaches_only_its_tools() {
        let limits = Limits::default();
        let t = token(3);
        for _ in 0..3 {
            assert!(limits.allow(&t).is_ok());
        }
        let wait = limits.allow(&t).unwrap_err();
        assert!((1..=60).contains(&wait));
        assert!(t.reaches("a") && !t.reaches("b"));
        assert!(
            McpToken {
                tools: None,
                ..token(1)
            }
            .reaches("b")
        );
    }

    #[test]
    fn calls_running_at_once_are_counted_until_they_end() {
        let limits = Limits::default();
        let held: Vec<_> = (0..AT_ONCE).map(|_| limits.start("w").unwrap()).collect();
        assert!(limits.start("w").is_none());
        assert!(limits.start("other").is_some(), "each workspace its own");
        drop(held);
        assert!(limits.start("w").is_some());
        assert!(limits.stale() && !limits.stale());
    }
}
