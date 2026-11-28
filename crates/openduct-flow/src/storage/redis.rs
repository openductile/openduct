// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Redis (and the stores that speak its protocol: Valkey, KeyDB, Dragonfly) as a store of keys and values: a value got,
//! set (for a while, or only when missing), deleted, counted up; lists pushed to, popped from and read; messages
//! published to a channel - and, for the `redis` trigger, a channel's messages subscribed to ([`Subscription`]).
//!
//! RESP, Redis's protocol, is written here: commands go as lists of strings, never as text a value could break out
//! of, and every answer is bounded (its size, its items, how deep it goes).
//!
//! A `redis` credential holds a `url` (`redis://host:6379/0`, or `rediss://` for TLS), a `username` and `password`
//! when the server asks for them, and a `ca` for a server whose certificate the system does not trust.

use std::time::Duration;

use openduct_expr::{Env, Value};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::Places;
use crate::apps::Credential;
use crate::run::Effects;
use crate::template::Template;

/// The largest value read.
const MAX_VALUE: usize = 4 * 1024 * 1024;
/// The most items of one answer, and how deep answers go.
const MAX_ITEMS: usize = 10_000;
const MAX_DEPTH: usize = 4;
/// The most items a range gives.
pub(crate) const MAX_RANGE: i64 = 1_000;
/// How long one command may take.
const COMMAND_TIME: Duration = Duration::from_secs(30);
/// The longest a value is kept, when it is set for a while.
const MAX_TTL: Duration = Duration::from_secs(10 * 365 * 86_400);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisAction {
    Get,
    Set,
    Delete,
    Increment,
    Push,
    Pop,
    Range,
    Publish,
    Exists,
    Expire,
}

impl RedisAction {
    pub(crate) const ALL: &'static [&'static str] = &[
        "get",
        "set",
        "delete",
        "increment",
        "push",
        "pop",
        "range",
        "publish",
        "exists",
        "expire",
    ];

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "get" => Self::Get,
            "set" => Self::Set,
            "delete" => Self::Delete,
            "increment" => Self::Increment,
            "push" => Self::Push,
            "pop" => Self::Pop,
            "range" => Self::Range,
            "publish" => Self::Publish,
            "exists" => Self::Exists,
            "expire" => Self::Expire,
            _ => return None,
        })
    }

    fn name(self) -> &'static str {
        Self::ALL.get(self as usize).copied().unwrap_or("get")
    }

    fn writes(self) -> bool {
        !matches!(self, Self::Get | Self::Range | Self::Exists)
    }
}

/// A `redis` step, as read.
#[derive(Debug, Clone)]
pub struct RedisStep {
    pub credential: String,
    pub action: RedisAction,
    pub key: Option<Template>,
    /// What a set, a push or a publish sends: text as it is, a record or a list as JSON.
    pub value: Option<Template>,
    /// A set (or an expire): how long the value is kept.
    pub ttl: Option<Duration>,
    /// A set only when the key has no value yet.
    pub if_missing: bool,
    /// An increment: by how much (a whole number, 1 by default).
    pub by: Option<Template>,
    /// A push or a pop: which end of the list - `right` (the end; the default for a push) or `left` (the start; the
    /// default for a pop, so a list pushed to and popped from is a queue).
    pub left: Option<bool>,
    /// A range: from and to (counted from 0; -1 is the last).
    pub start: i64,
    pub stop: i64,
    pub channel: Option<Template>,
}

/// An answer of Redis's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reply {
    Text(Vec<u8>),
    Number(i64),
    Nothing,
    List(Vec<Reply>),
    Refused(String),
}

impl Reply {
    fn text(&self) -> Option<String> {
        match self {
            Self::Text(b) => Some(String::from_utf8_lossy(b).into_owned()),
            Self::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

/// A value as a step's result shows it: a record or a list when it is JSON's, text otherwise.
fn shown(reply: &Reply) -> Value {
    match reply {
        Reply::Nothing => Value::Null,
        Reply::Number(n) => Value::from_json(&json!(n)),
        Reply::Text(b) => {
            let text = String::from_utf8_lossy(b);
            let trimmed = text.trim_start();
            if (trimmed.starts_with('{') || trimmed.starts_with('['))
                && let Ok(v) = Value::parse_json(&text)
            {
                return v;
            }
            Value::text(text.as_ref())
        }
        Reply::List(items) => Value::list(items.iter().map(shown).collect()),
        Reply::Refused(e) => Value::text(e.as_str()),
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A connection to Redis.
pub(crate) struct Conn {
    stream: Box<dyn Io>,
    buffer: Vec<u8>,
    host: String,
}

/// Where a `redis` credential's server is: its host, port, database and whether it is reached over TLS.
fn address(c: &Credential) -> Result<(String, u16, u32, bool), String> {
    let url = c
        .fields
        .get("url")
        .map(|u| u.trim())
        .filter(|u| !u.is_empty())
        .ok_or("the redis credential has no url: redis://host:6379/0")?;
    let (tls, rest) = if let Some(r) = url.strip_prefix("rediss://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("redis://") {
        (false, r)
    } else {
        return Err(format!("url: {url:?} starts redis:// or rediss://"));
    };
    let (place, database) = rest.split_once('/').unwrap_or((rest, ""));
    if place.contains('@') {
        return Err("url: give the username and password in their own fields, not in the url".to_owned());
    }
    let (host, port) = match place.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || place.starts_with('[') => (
            h.to_owned(),
            p.parse::<u16>()
                .map_err(|_| format!("url: {p:?} is not a port"))?,
        ),
        _ => (place.to_owned(), 6379),
    };
    let database = match database.trim_end_matches('/') {
        "" => 0,
        d => d
            .parse()
            .map_err(|_| format!("url: {d:?} is not a database's number"))?,
    };
    Ok((host, port, database, tls))
}

impl Conn {
    pub(crate) async fn open(c: &Credential, places: &Places) -> Result<Self, String> {
        let (host, port, database, tls) = address(c)?;
        let (stream, _) = super::net::connect(&host, port, places.allow_private).await?;
        let stream: Box<dyn Io> = if tls {
            let config = super::net::tls_config(c.fields.get("ca").map(String::as_str))?;
            Box::new(super::net::tls(stream, &config, &host).await?)
        } else {
            Box::new(stream)
        };
        let mut conn = Self {
            stream,
            buffer: Vec::new(),
            host,
        };
        let field = |n: &str| c.fields.get(n).filter(|v| !v.is_empty()).cloned();
        if let Some(password) = field("password") {
            let mut auth = vec![b"AUTH".to_vec()];
            if let Some(user) = field("username") {
                auth.push(user.into_bytes());
            }
            auth.push(password.into_bytes());
            if let Reply::Refused(_) = conn.call(&auth).await? {
                return Err(format!("{} refused the sign-in", conn.host));
            }
        }
        if database != 0 {
            let reply = conn
                .call(&[b"SELECT".to_vec(), database.to_string().into_bytes()])
                .await?;
            if let Reply::Refused(e) = reply {
                return Err(format!("{} has no database {database}: {e}", conn.host));
            }
        }
        Ok(conn)
    }

    async fn fill(&mut self) -> Result<(), String> {
        let mut chunk = [0u8; 8192];
        let n = self
            .stream
            .read(&mut chunk)
            .await
            .map_err(|e| format!("{} could not be read: {e}", self.host))?;
        if n == 0 {
            return Err(format!("{} closed the connection", self.host));
        }
        self.buffer.extend_from_slice(chunk.get(..n).unwrap_or_default());
        Ok(())
    }

    async fn line(&mut self) -> Result<Vec<u8>, String> {
        loop {
            if let Some(at) = self.buffer.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = self.buffer.drain(..at + 2).take(at).collect();
                return Ok(line);
            }
            if self.buffer.len() > 64 * 1024 {
                return Err(format!("{} sent a line too long", self.host));
            }
            self.fill().await?;
        }
    }

    fn number(line: &[u8]) -> Result<i64, String> {
        std::str::from_utf8(line)
            .ok()
            .and_then(|t| t.parse().ok())
            .ok_or_else(|| "Redis answered in a way it does not".to_owned())
    }

    async fn reply(&mut self, depth: usize) -> Result<Reply, String> {
        if depth > MAX_DEPTH {
            return Err("Redis answered with lists too deep".to_owned());
        }
        let line = self.line().await?;
        let (kind, rest) = line.split_first().ok_or("Redis answered with nothing")?;
        Ok(match kind {
            b'+' => Reply::Text(rest.to_vec()),
            b'-' => Reply::Refused(String::from_utf8_lossy(rest).chars().take(300).collect()),
            b':' => Reply::Number(Self::number(rest)?),
            b'$' => {
                let n = Self::number(rest)?;
                if n < 0 {
                    return Ok(Reply::Nothing);
                }
                let n = usize::try_from(n).map_err(|e| e.to_string())?;
                if n > MAX_VALUE {
                    return Err(format!("a value of more than {MAX_VALUE} bytes"));
                }
                while self.buffer.len() < n + 2 {
                    self.fill().await?;
                }
                let value: Vec<u8> = self.buffer.drain(..n + 2).take(n).collect();
                Reply::Text(value)
            }
            b'*' => {
                let n = Self::number(rest)?;
                if n < 0 {
                    return Ok(Reply::Nothing);
                }
                let n = usize::try_from(n).map_err(|e| e.to_string())?;
                if n > MAX_ITEMS {
                    return Err(format!("an answer of more than {MAX_ITEMS} items"));
                }
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(Box::pin(self.reply(depth + 1)).await?);
                }
                Reply::List(items)
            }
            _ => return Err("Redis answered in a way it does not".to_owned()),
        })
    }

    async fn send(&mut self, args: &[Vec<u8>]) -> Result<(), String> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend(format!("${}\r\n", a.len()).as_bytes());
            out.extend(a);
            out.extend(b"\r\n");
        }
        self.stream
            .write_all(&out)
            .await
            .map_err(|e| format!("{} could not be written to: {e}", self.host))
    }

    /// A command, and its answer.
    pub(crate) async fn call(&mut self, args: &[Vec<u8>]) -> Result<Reply, String> {
        tokio::time::timeout(COMMAND_TIME, async {
            self.send(args).await?;
            self.reply(0).await
        })
        .await
        .map_err(|_| format!("{} did not answer in time", self.host))?
    }
}

/// A channel subscribed to (a pattern, with `*`, `?` or `[`): its messages as they come.
pub(crate) struct Subscription {
    conn: Conn,
}

impl Subscription {
    pub(crate) async fn open(c: &Credential, places: &Places, channel: &str) -> Result<Self, String> {
        let mut conn = Conn::open(c, places).await?;
        let pattern = channel.contains(['*', '?', '[']);
        let command: &[u8] = if pattern { b"PSUBSCRIBE" } else { b"SUBSCRIBE" };
        conn.send(&[command.to_vec(), channel.as_bytes().to_vec()])
            .await?;
        match tokio::time::timeout(COMMAND_TIME, conn.reply(0)).await {
            Ok(Ok(Reply::List(_))) => Ok(Self { conn }),
            Ok(Ok(Reply::Refused(e))) => Err(format!("{} refused the subscription: {e}", conn.host)),
            Ok(Ok(_)) => Err(format!("{} did not subscribe", conn.host)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(format!("{} did not answer in time", conn.host)),
        }
    }

    /// The next message: its channel and its text. Waits as long as it takes.
    pub(crate) async fn next(&mut self) -> Result<(String, Value), String> {
        loop {
            let Reply::List(items) = self.conn.reply(0).await? else {
                continue;
            };
            let texts: Vec<Option<String>> = items.iter().map(Reply::text).collect();
            match (texts.first().cloned().flatten().as_deref(), items.as_slice()) {
                (Some("message"), [_, channel, body]) | (Some("pmessage"), [_, _, channel, body]) => {
                    return Ok((channel.text().unwrap_or_default(), shown(body)));
                }
                _ => {}
            }
        }
    }
}

fn text_of(value: &Value) -> Vec<u8> {
    crate::template::as_text(value).into_bytes()
}

/// The step's result.
pub(crate) async fn run(effects: &dyn Effects, r: &RedisStep, env: &Env<'_>) -> Result<Value, String> {
    let render = |t: &Option<Template>, what: &str| -> Result<Option<String>, String> {
        t.as_ref()
            .map(|t| t.render(env).map_err(|e| format!("{what}: {e}")))
            .transpose()
    };
    let key = render(&r.key, "key")?;
    let channel = render(&r.channel, "channel")?;
    let value = r
        .value
        .as_ref()
        .map(|t| t.value(env).map_err(|e| format!("value: {e}")))
        .transpose()?;
    let by: i64 = match render(&r.by, "by")? {
        None => 1,
        Some(b) => b
            .trim()
            .parse()
            .map_err(|_| format!("by: {b:?} is not a whole number"))?,
    };
    let credential = effects.credential(&r.credential)?;
    if !matches!(credential.kind.as_str(), "redis" | "test") {
        return Err(format!(
            "{} is a {} credential: a redis step takes a redis one",
            r.credential, credential.kind
        ));
    }
    let needs_key = r.action != RedisAction::Publish;
    let key = match (needs_key, key) {
        (true, Some(k)) if !k.is_empty() && k.len() <= 1024 => k,
        (true, _) => return Err("key: the key, of 1 to 1,024 characters".to_owned()),
        (false, _) => String::new(),
    };
    let places = match (
        effects.storage().allows(r.action.writes()),
        credential.kind.as_str(),
    ) {
        (Ok(p), "redis") => p,
        (Ok(_) | Err(_), _) => {
            let test = effects
                .storage()
                .allows(r.action.writes())
                .err()
                .unwrap_or("not sent");
            return Ok(Value::from_json(&json!({
                "test": test, "redis": r.action.name(), "key": key, "channel": channel,
                "value": value.as_ref().map(crate::template::as_text), "found": false,
            })));
        }
    };
    let mut conn = Conn::open(&credential, places).await?;
    let k = key.clone().into_bytes();
    let left = r.left.unwrap_or(r.action == RedisAction::Pop);
    let args: Vec<Vec<u8>> = match r.action {
        RedisAction::Get => vec![b"GET".to_vec(), k],
        RedisAction::Exists => vec![b"EXISTS".to_vec(), k],
        RedisAction::Delete => vec![b"DEL".to_vec(), k],
        RedisAction::Increment => vec![b"INCRBY".to_vec(), k, by.to_string().into_bytes()],
        RedisAction::Pop => vec![if left { b"LPOP".to_vec() } else { b"RPOP".to_vec() }, k],
        RedisAction::Set | RedisAction::Push | RedisAction::Publish => {
            let value = value.as_ref().map(text_of).ok_or("value: what to send")?;
            if value.len() > MAX_VALUE {
                return Err(format!("value: at most {MAX_VALUE} bytes"));
            }
            match r.action {
                RedisAction::Set => {
                    let mut a = vec![b"SET".to_vec(), k, value];
                    if let Some(ttl) = r.ttl {
                        a.push(b"PX".to_vec());
                        a.push(ttl.min(MAX_TTL).as_millis().max(1).to_string().into_bytes());
                    }
                    if r.if_missing {
                        a.push(b"NX".to_vec());
                    }
                    a
                }
                RedisAction::Push => vec![
                    if left {
                        b"LPUSH".to_vec()
                    } else {
                        b"RPUSH".to_vec()
                    },
                    k,
                    value,
                ],
                _ => {
                    let channel = channel
                        .clone()
                        .filter(|c| !c.is_empty())
                        .ok_or("channel: where to publish")?;
                    vec![b"PUBLISH".to_vec(), channel.into_bytes(), value]
                }
            }
        }
        RedisAction::Range => {
            let stop = if r.start >= 0 && (r.stop < 0 || r.stop - r.start >= MAX_RANGE) {
                r.start + MAX_RANGE - 1
            } else {
                r.stop
            };
            vec![
                b"LRANGE".to_vec(),
                k,
                r.start.to_string().into_bytes(),
                stop.to_string().into_bytes(),
            ]
        }
        RedisAction::Expire => {
            let ttl = r.ttl.ok_or("ttl: how long the key is kept, as 1h")?;
            vec![
                b"PEXPIRE".to_vec(),
                k,
                ttl.min(MAX_TTL).as_millis().max(1).to_string().into_bytes(),
            ]
        }
    };
    let reply = conn.call(&args).await;
    let _ = conn.stream.shutdown().await;
    let reply = reply?;
    if let Reply::Refused(e) = &reply {
        return Err(format!("Redis refused to {} {key}: {e}", r.action.name()));
    }
    let number = match &reply {
        Reply::Number(n) => *n,
        _ => 0,
    };
    Ok(Value::from_json(&match r.action {
        RedisAction::Get | RedisAction::Pop => json!({
            "key": key, "found": reply != Reply::Nothing,
            "value": serde_json::from_str::<serde_json::Value>(&shown(&reply).to_json()).unwrap_or_default(),
        }),
        RedisAction::Set => json!({ "key": key, "set": reply != Reply::Nothing }),
        RedisAction::Exists => json!({ "key": key, "exists": number > 0 }),
        RedisAction::Delete => json!({ "key": key, "deleted": number > 0 }),
        RedisAction::Increment => json!({ "key": key, "value": number }),
        RedisAction::Push => json!({ "key": key, "length": number }),
        RedisAction::Expire => json!({ "key": key, "set": number > 0 }),
        RedisAction::Publish => json!({ "channel": channel, "received": number }),
        RedisAction::Range => {
            let items =
                serde_json::from_str::<serde_json::Value>(&shown(&reply).to_json()).unwrap_or_default();
            let count = items.as_array().map_or(0, Vec::len);
            json!({ "key": key, "items": items, "count": count })
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cred(url: &str) -> Credential {
        Credential {
            kind: "redis".into(),
            fields: [("url".to_owned(), url.to_owned())].into_iter().collect(),
        }
    }

    #[test]
    fn a_servers_address_is_read_from_its_url() {
        assert_eq!(
            address(&cred("redis://cache:6380/2")).unwrap(),
            ("cache".into(), 6380, 2, false)
        );
        assert_eq!(
            address(&cred("rediss://cache")).unwrap(),
            ("cache".into(), 6379, 0, true)
        );
        assert_eq!(address(&cred("redis://[::1]:7000")).unwrap().1, 7000);
        assert!(
            address(&cred("redis://u:secret@cache"))
                .unwrap_err()
                .contains("own fields")
        );
        assert!(address(&cred("http://cache")).is_err());
        assert!(address(&cred("redis://cache:port")).is_err());
        assert!(address(&cred("redis://cache/x")).is_err());
    }

    #[test]
    fn values_that_are_json_are_shown_as_records() {
        assert_eq!(shown(&Reply::Text(b"{\"a\":1}".to_vec())).to_json(), "{\"a\":1}");
        assert_eq!(shown(&Reply::Text(b"12".to_vec())).to_json(), "\"12\"");
        assert!(shown(&Reply::Nothing).is_null());
        assert_eq!(RedisAction::Expire.name(), "expire");
        assert_eq!(RedisAction::Get.name(), "get");
    }
}
