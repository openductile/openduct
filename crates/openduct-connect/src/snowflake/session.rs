// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A session with Snowflake's SQL API (`/api/v2/statements`): sign-in with a key pair (a JWT signed with the user's
//! private key, made again before its hour ends) or with a token (a programmatic access token or an OAuth token).
//! Each statement carries its own request id, so one sent again after a lost answer is not run twice. A statement
//! still running is waited for, and an answer in several partitions is read whole.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::{Client, Method, StatusCode};

use super::values::{Column, columns};
use crate::web;

/// The longest Snowflake is let run a statement.
const STATEMENT_TIMEOUT: u64 = 3_600;

#[derive(Clone)]
pub(super) enum Auth {
    /// Key-pair sign-in: the account and user as the JWT names them (upper case), and the key's fingerprint.
    KeyPair {
        account: String,
        user: String,
        key: Arc<ring::signature::RsaKeyPair>,
        fingerprint: String,
    },
    /// A programmatic access token or an OAuth token; Snowflake tells which.
    Token(String),
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::KeyPair { .. } => "KeyPair",
            Self::Token(_) => "Token",
        })
    }
}

/// Where statements run: the warehouse, database, schema and role (each the user's default when not given).
#[derive(Debug, Clone, Default)]
pub(super) struct Context {
    pub(super) warehouse: Option<String>,
    pub(super) database: Option<String>,
    pub(super) schema: Option<String>,
    pub(super) role: Option<String>,
}

/// A statement's answer: its columns, and its rows as the SQL API writes them.
#[derive(Debug, Default)]
pub(super) struct Answer {
    pub(super) columns: Vec<Column>,
    pub(super) rows: Vec<Vec<Option<String>>>,
}

/// Snowflake ran the statement and refused it: its SQL state (`22018`) and its words.
#[derive(Debug)]
pub(super) struct Refused {
    pub(super) state: String,
    pub(super) words: String,
}

impl Refused {
    /// A refusal of the data (a value not of the column's type, too long, missing where it is required), not of
    /// the statement: SQL states of class 22 and 23.
    pub(super) fn of_data(&self) -> bool {
        self.state.starts_with("22") || self.state.starts_with("23")
    }
}

#[derive(Debug)]
pub(super) struct Session {
    client: Client,
    /// `https://myorg-myaccount.snowflakecomputing.com`.
    base: String,
    auth: Auth,
    context: Context,
    jwt: Option<(String, Instant)>,
}

/// Snowflake's error answers (`{"code": "390144", "message": "JWT token is invalid."}`) in words, on one line.
pub(super) fn error_words(body: &[u8]) -> String {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        return String::from_utf8_lossy(body.get(..body.len().min(300)).unwrap_or_default()).into_owned();
    };
    let message = v
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    match v.get("code").and_then(|c| c.as_str()) {
        Some(code) if !message.is_empty() => format!("{message} (code {code})"),
        _ => message,
    }
}

/// A random request id (a version 4 UUID).
fn request_id() -> Result<String, String> {
    use ring::rand::SecureRandom;
    let mut b = [0u8; 16];
    ring::rand::SystemRandom::new()
        .fill(&mut b)
        .map_err(|_| "cannot make a request id".to_owned())?;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        hex.get(..8).unwrap_or_default(),
        hex.get(8..12).unwrap_or_default(),
        hex.get(12..16).unwrap_or_default(),
        hex.get(16..20).unwrap_or_default(),
        hex.get(20..).unwrap_or_default()
    ))
}

/// A statement handle as it may go into a path.
fn handle(v: &serde_json::Value) -> Option<String> {
    let h = v.get("statementHandle")?.as_str()?;
    ((1..=64).contains(&h.len()) && h.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'))
        .then(|| h.to_owned())
}

/// Rows of an answer's `data`.
fn rows(data: Option<&serde_json::Value>) -> Vec<Vec<Option<String>>> {
    data.and_then(|d| d.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.as_array())
                .map(|r| r.iter().map(|v| v.as_str().map(str::to_owned)).collect())
                .collect()
        })
        .unwrap_or_default()
}

/// A partition's body, which Snowflake sends compressed with gzip.
fn unzipped(bytes: Vec<u8>) -> Result<Vec<u8>, String> {
    if !bytes.starts_with(&[0x1f, 0x8b]) {
        return Ok(bytes);
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes.as_slice())
        .take(web::MAX_ANSWER as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| "Snowflake's answer could not be unzipped".to_owned())?;
    if out.len() > web::MAX_ANSWER {
        return Err("Snowflake answered with more than 64 MiB".to_owned());
    }
    Ok(out)
}

impl Session {
    pub(super) fn new(
        base: String,
        auth: Auth,
        context: Context,
        private_network: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            client: web::client(private_network)?,
            base,
            auth,
            context,
            jwt: None,
        })
    }

    /// The bearer token: the token given, or a JWT made again once it is 50 minutes old (Snowflake takes one
    /// for an hour at most).
    fn bearer(&mut self) -> Result<(String, Option<&'static str>), String> {
        let (account, user, key, fingerprint) = match &self.auth {
            Auth::Token(t) => return Ok((t.clone(), None)),
            Auth::KeyPair {
                account,
                user,
                key,
                fingerprint,
            } => (account, user, key, fingerprint),
        };
        if let Some((jwt, made)) = &self.jwt
            && made.elapsed() < Duration::from_secs(50 * 60)
        {
            return Ok((jwt.clone(), Some("KEYPAIR_JWT")));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let claims = serde_json::json!({
            "iss": format!("{account}.{user}.{fingerprint}"),
            "sub": format!("{account}.{user}"),
            "iat": now,
            "exp": now + 59 * 60,
        });
        let jwt = crate::jwt::signed(&claims, key)?;
        self.jwt = Some((jwt.clone(), Instant::now()));
        Ok((jwt, Some("KEYPAIR_JWT")))
    }

    async fn call(
        &mut self,
        method: Method,
        url: &str,
        body: Option<&str>,
        sent: &AtomicBool,
    ) -> Result<(StatusCode, Vec<u8>), String> {
        for attempt in 0..2 {
            let (token, kind) = self.bearer()?;
            let answer = web::send(
                || {
                    // A statement sent again (its answer lost) says so, and Snowflake does not run it twice.
                    let again = sent.swap(true, Ordering::Relaxed) && body.is_some();
                    let target = if again {
                        format!("{url}&retry=true")
                    } else {
                        url.to_owned()
                    };
                    let mut r = self
                        .client
                        .request(method.clone(), target)
                        .bearer_auth(&token)
                        .header(reqwest::header::ACCEPT, "application/json");
                    if let Some(kind) = kind {
                        r = r.header("X-Snowflake-Authorization-Token-Type", kind);
                    }
                    if let Some(b) = body {
                        r = r
                            .header(reqwest::header::CONTENT_TYPE, "application/json")
                            .body(b.to_owned());
                    }
                    r
                },
                "Snowflake",
            )
            .await?;
            let status = answer.status();
            let bytes = web::body(answer, "Snowflake").await?;
            if status == StatusCode::UNAUTHORIZED {
                if attempt == 0 && self.jwt.is_some() {
                    self.jwt = None;
                    continue;
                }
                return Err(format!("Snowflake refused the sign-in: {}", error_words(&bytes)));
            }
            return Ok((status, bytes));
        }
        Err("Snowflake refused the sign-in again after signing in again".to_owned())
    }

    /// Run one statement with its `?` bound, in order, to these texts. `Ok(Err(..))` is Snowflake refusing the
    /// statement; `Err` is anything else that went wrong.
    pub(super) async fn run(
        &mut self,
        statement: &str,
        binds: &[String],
    ) -> Result<Result<Answer, Refused>, String> {
        let mut body = serde_json::Map::new();
        body.insert("statement".to_owned(), serde_json::json!(statement));
        body.insert("timeout".to_owned(), serde_json::json!(STATEMENT_TIMEOUT));
        for (name, value) in [
            ("warehouse", &self.context.warehouse),
            ("database", &self.context.database),
            ("schema", &self.context.schema),
            ("role", &self.context.role),
        ] {
            if let Some(v) = value {
                body.insert(name.to_owned(), serde_json::json!(v));
            }
        }
        if !binds.is_empty() {
            let bindings: serde_json::Map<String, serde_json::Value> = binds
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        (i + 1).to_string(),
                        serde_json::json!({ "type": "TEXT", "value": v }),
                    )
                })
                .collect();
            body.insert("bindings".to_owned(), serde_json::Value::Object(bindings));
        }
        let url = format!("{}/api/v2/statements?requestId={}", self.base, request_id()?);
        let sent = AtomicBool::new(false);
        let (mut status, mut bytes) = self
            .call(
                Method::POST,
                &url,
                Some(&serde_json::Value::Object(body).to_string()),
                &sent,
            )
            .await?;
        let started = Instant::now();
        let mut wait = Duration::from_millis(250);
        loop {
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
            match status {
                StatusCode::OK => return self.answer(&v).await.map(Ok),
                StatusCode::UNPROCESSABLE_ENTITY => {
                    return Ok(Err(Refused {
                        state: v
                            .get("sqlState")
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_owned(),
                        words: error_words(&bytes),
                    }));
                }
                // Still running: ask again after a wait.
                StatusCode::ACCEPTED | StatusCode::TOO_MANY_REQUESTS => {
                    let Some(h) = handle(&v) else {
                        return Err(if status == StatusCode::TOO_MANY_REQUESTS {
                            format!("Snowflake is too busy: {}", error_words(&bytes))
                        } else {
                            "Snowflake gave no usable handle for a statement still running".to_owned()
                        });
                    };
                    if started.elapsed() > Duration::from_secs(STATEMENT_TIMEOUT + 60) {
                        return Err("Snowflake's statement is still running after an hour".to_owned());
                    }
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(5));
                    let url = format!("{}/api/v2/statements/{h}", self.base);
                    (status, bytes) = self.call(Method::GET, &url, None, &sent).await?;
                }
                other => {
                    return Err(format!(
                        "Snowflake refused the request: {} ({other})",
                        error_words(&bytes)
                    ));
                }
            }
        }
    }

    /// A finished answer, with the rows of its other partitions.
    async fn answer(&mut self, v: &serde_json::Value) -> Result<Answer, String> {
        let meta = v.get("resultSetMetaData").cloned().unwrap_or_default();
        let mut answer = Answer {
            columns: columns(&meta),
            rows: rows(v.get("data")),
        };
        let partitions = meta
            .get("partitionInfo")
            .and_then(|p| p.as_array())
            .map_or(1, Vec::len);
        if partitions > 1 {
            let h = handle(v).ok_or("Snowflake gave no handle for an answer in partitions")?;
            let sent = AtomicBool::new(false);
            for p in 1..partitions {
                let url = format!("{}/api/v2/statements/{h}?partition={p}", self.base);
                let (status, bytes) = self.call(Method::GET, &url, None, &sent).await?;
                if status != StatusCode::OK {
                    return Err(format!(
                        "Snowflake refused part {p} of the answer: {}",
                        error_words(&bytes)
                    ));
                }
                let part: serde_json::Value = serde_json::from_slice(&unzipped(bytes)?)
                    .map_err(|_| "Snowflake answered with something else".to_owned())?;
                let data = if part.is_array() {
                    Some(&part)
                } else {
                    part.get("data")
                };
                answer.rows.extend(rows(data));
            }
        }
        Ok(answer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_words_and_ids_are_uuids() {
        assert_eq!(
            error_words(br#"{"code":"002003","sqlState":"42S02","message":"SQL compilation error:\nTable 'ORDRES' does not exist or not authorized."}"#),
            "SQL compilation error: Table 'ORDRES' does not exist or not authorized. (code 002003)"
        );
        let id = request_id().unwrap();
        assert_eq!(
            (id.len(), id.matches('-').count(), id.get(14..15)),
            (36, 4, Some("4"))
        );
        assert_ne!(id, request_id().unwrap());
        assert_eq!(
            handle(&serde_json::json!({"statementHandle": "01b2-x/../"})),
            None
        );
        let state = |s: &str| Refused {
            state: s.to_owned(),
            words: String::new(),
        };
        assert!(state("22018").of_data() && state("23502").of_data());
        assert!(!state("42S02").of_data() && !state("42501").of_data());
    }
}
