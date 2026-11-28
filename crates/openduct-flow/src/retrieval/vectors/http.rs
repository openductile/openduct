// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A web store's requests, through the run's HTTP effect: JSON in and out, each bounded in time, a read marked as one
//! (a preview sends it), a store's refusal said in its own words, cut short.

use std::time::Duration;

use serde_json::Value as Json;

use super::{Got, StoreKind, label};
use crate::retrieval::{as_json, shown};
use crate::run::{Effects, HttpRequest};

/// How long a store may take over one request.
const WAIT: Duration = Duration::from_secs(60);
/// How much of a store's refusal is said.
const SAID: usize = 300;

/// A web store's API: its address, and the headers every request carries (its key).
pub(crate) struct Api<'a> {
    pub(crate) effects: &'a dyn Effects,
    pub(crate) base: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) kind: StoreKind,
}

impl std::fmt::Debug for Api<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api")
            .field("base", &self.base)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// An answer, or a test run's (or a preview's) showing of the request.
#[derive(Debug)]
pub(crate) enum Reply {
    Shown(Json),
    Got(u16, Json),
}

impl Api<'_> {
    /// Sends `method path` with `body`; `read` when it changes nothing.
    pub(crate) async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<&Json>,
        read: bool,
    ) -> Result<Reply, String> {
        self.send_to(&format!("{}{path}", self.base), method, body, read)
            .await
    }

    /// As [`Api::send`], to a whole address (Pinecone's index has its own).
    pub(crate) async fn send_to(
        &self,
        url: &str,
        method: &str,
        body: Option<&Json>,
        read: bool,
    ) -> Result<Reply, String> {
        let request = HttpRequest {
            read,
            method: method.to_owned(),
            url: url.to_owned(),
            headers: self.headers.clone(),
            query: Vec::new(),
            body: body.map(|b| ("application/json".to_owned(), b.to_string().into_bytes())),
            timeout: WAIT,
        };
        let answer = self.effects.http(request).await?;
        if shown(&answer) {
            return Ok(Reply::Shown(as_json(&answer)));
        }
        let answer = as_json(&answer);
        let status = answer
            .get("status")
            .and_then(Json::as_u64)
            .and_then(|s| u16::try_from(s).ok())
            .unwrap_or(0);
        Ok(Reply::Got(
            status,
            answer.get("body").cloned().unwrap_or(Json::Null),
        ))
    }

    /// As [`Api::send`], an answer of 400 or more refused.
    pub(crate) async fn ok(
        &self,
        method: &str,
        path: &str,
        body: Option<&Json>,
        read: bool,
    ) -> Result<Got<Json>, String> {
        let reply = self.send(method, path, body, read).await?;
        self.checked(reply)
    }

    /// A reply, refused when the store said no.
    pub(crate) fn checked(&self, reply: Reply) -> Result<Got<Json>, String> {
        match reply {
            Reply::Shown(s) => Ok(Got::Shown(s)),
            Reply::Got(status, body) if (200..300).contains(&status) => Ok(Got::Done(body)),
            Reply::Got(status, body) => Err(self.refused(status, &body)),
        }
    }

    /// Why the store refused, in its words.
    pub(crate) fn refused(&self, status: u16, body: &Json) -> String {
        let said = said(body);
        if said.is_empty() {
            format!("{} answered {status}", label(self.kind))
        } else {
            format!("{} answered {status}: {said}", label(self.kind))
        }
    }
}

/// What a store's error says: its message, wherever its API puts it.
pub(crate) fn said(body: &Json) -> String {
    let found = [
        "/error/message",
        "/status/error",
        "/message",
        "/errors/0/message",
        "/error/0/message",
        "/error",
        "/detail",
    ]
    .iter()
    .find_map(|p| {
        body.pointer(p)
            .filter(|v| !v.is_null() && !v.is_object() && !v.is_array())
    });
    let text = match (found, body) {
        (Some(Json::String(s)), _) => s.clone(),
        (Some(v), _) => v.to_string(),
        (None, Json::String(s)) => s.clone(),
        (None, Json::Null) => String::new(),
        (None, other) => other.to_string(),
    };
    text.chars().take(SAID).collect()
}

/// A path segment written into an address: only names already checked go in, but a stray character is escaped.
pub(crate) fn segment(name: &str) -> String {
    name.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_store_s_refusal_is_read_wherever_it_puts_it() {
        assert_eq!(
            said(&json!({ "status": { "error": "Not found: Collection `x` doesn't exist!" } })),
            "Not found: Collection `x` doesn't exist!"
        );
        assert_eq!(said(&json!({ "error": { "message": "bad", "code": 3 } })), "bad");
        assert_eq!(
            said(&json!({ "code": 1100, "message": "invalid filter" })),
            "invalid filter"
        );
        assert_eq!(
            said(&json!({ "error": [{ "message": "no such class" }] })),
            "no such class"
        );
        assert_eq!(said(&json!("plain")), "plain");
        assert_eq!(said(&json!("x".repeat(500))).len(), SAID);
        assert_eq!(segment("a b/c"), "a%20b%2Fc");
    }
}
