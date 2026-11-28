// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MongoDB's own protocol, as much as a vector store needs: a connection to the replica set's primary, signed in
//! with SCRAM-SHA-256, and commands sent as `OP_MSG` messages of one BSON document each.
//!
//! A credential's `url` is `mongodb+srv://<cluster>` (what MongoDB Atlas gives: its hosts and options are looked up
//! in DNS, the hosts checked to be in the cluster's own domain, TLS on) or `mongodb://host:port[,host:port]` (TLS when
//! `tls=true`, or the credential's `tls` says so). The username and password are the credential's own fields, never
//! inside the url. Every host is connected to by the storage steps' rule - this machine's own network refused unless
//! allowed, the address checked once resolved and that very address used - including a primary another host names.
//! Every answer is bounded in size and in time.

use std::time::Duration;

use bson::{Bson, Document, doc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::apps::Credential;
use crate::storage::Places;
use crate::storage::net;

/// `OP_MSG`'s operation code.
const OP_MSG: i32 = 2013;
/// The largest answer read.
const MAX_ANSWER: usize = 16 * 1024 * 1024;
/// How long one command may take.
const COMMAND_TIME: Duration = Duration::from_secs(60);
/// The most hosts tried, and SRV records read.
const MAX_HOSTS: usize = 8;

/// Where a cluster is: its hosts, whether TLS is used, where users are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Address {
    pub(crate) hosts: Vec<(String, u16)>,
    pub(crate) tls: bool,
    pub(crate) auth_source: String,
}

fn options(text: &str, tls: &mut bool, auth: &mut Option<String>) -> Result<(), String> {
    for pair in text.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        match k.to_ascii_lowercase().as_str() {
            "tls" | "ssl" => *tls = v.eq_ignore_ascii_case("true"),
            "authsource" => *auth = Some(v.to_owned()),
            // The others (replicaSet, retryWrites, w, appName ...) change nothing this client does.
            _ => {}
        }
    }
    Ok(())
}

/// A url read: whether it is `mongodb+srv`, its hosts, its options.
type Parsed = (bool, Vec<(String, u16)>, String);

/// The cluster a url names, before any lookup: `(srv, hosts, query)`.
pub(crate) fn parse(url: &str) -> Result<Parsed, String> {
    let (srv, rest) = if let Some(r) = url.strip_prefix("mongodb+srv://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("mongodb://") {
        (false, r)
    } else {
        return Err(format!("url: {url:?} starts mongodb+srv:// or mongodb://"));
    };
    let (place, query) = rest.split_once('?').unwrap_or((rest, ""));
    let place = place.split('/').next().unwrap_or_default();
    if place.contains('@') {
        return Err("url: give the username and password in their own fields, not in the url".to_owned());
    }
    let mut hosts = Vec::new();
    for h in place.split(',').filter(|h| !h.is_empty()).take(MAX_HOSTS) {
        let (host, port) = match h.rsplit_once(':') {
            Some((host, p)) if !host.ends_with(']') || h.starts_with('[') => (
                host.to_owned(),
                p.parse::<u16>()
                    .map_err(|_| format!("url: {p:?} is not a port"))?,
            ),
            _ => (h.to_owned(), 27017),
        };
        net::host_ok(&host)?;
        hosts.push((host, port));
    }
    if hosts.is_empty() {
        return Err("url: the cluster's host, as mongodb+srv://cluster0.abcde.mongodb.net".to_owned());
    }
    if srv && (hosts.len() != 1 || place.contains(':')) {
        return Err("url: mongodb+srv:// names one host, without a port".to_owned());
    }
    Ok((srv, hosts, query.to_owned()))
}

/// Whether an SRV record's target is in the cluster's own domain (MongoDB's rule for `mongodb+srv`).
pub(crate) fn in_domain(cluster: &str, target: &str) -> bool {
    let cluster = cluster.trim_end_matches('.').to_ascii_lowercase();
    let target = target.trim_end_matches('.').to_ascii_lowercase();
    let parent = match cluster.split_once('.') {
        Some((_, rest)) if rest.contains('.') => rest.to_owned(),
        _ => cluster.clone(),
    };
    target != parent && target.ends_with(&format!(".{parent}"))
}

/// The cluster's address: for `mongodb+srv`, its hosts and options looked up in DNS.
pub(crate) async fn address(credential: &Credential) -> Result<Address, String> {
    let url = super::super::field(credential, "url")
        .ok_or("the mongodb credential has no url: mongodb+srv://cluster0.abcde.mongodb.net")?;
    let (srv, mut hosts, query) = parse(&url)?;
    let mut tls = srv;
    let mut auth = None;
    if srv {
        let cluster = hosts.first().map(|h| h.0.clone()).unwrap_or_default();
        let (found, txt) = lookup(&cluster).await?;
        hosts = found
            .into_iter()
            .filter(|(h, _)| in_domain(&cluster, h))
            .take(MAX_HOSTS)
            .collect();
        if hosts.is_empty() {
            return Err(format!("{cluster} names no hosts of its own domain in DNS"));
        }
        options(&txt, &mut tls, &mut auth)?;
    }
    options(&query, &mut tls, &mut auth)?;
    if let Some(t) = super::super::field(credential, "tls") {
        tls = t.eq_ignore_ascii_case("true");
    }
    let auth_source = super::super::field(credential, "auth_source")
        .or(auth)
        .unwrap_or_else(|| "admin".to_owned());
    Ok(Address {
        hosts,
        tls,
        auth_source,
    })
}

/// A cluster's SRV hosts and TXT options.
async fn lookup(cluster: &str) -> Result<(Vec<(String, u16)>, String), String> {
    use hickory_resolver::proto::rr::{Name, RData};
    let resolver = hickory_resolver::TokioResolver::builder_tokio()
        .map_err(|e| format!("cannot look up {cluster} in DNS: {e}"))?
        .build()
        .map_err(|e| format!("cannot look up {cluster} in DNS: {e}"))?;
    let name = |n: &str| Name::from_str_relaxed(n).map_err(|e| format!("{n} is not a DNS name: {e}"));
    let srv = tokio::time::timeout(
        COMMAND_TIME,
        resolver.srv_lookup(name(&format!("_mongodb._tcp.{cluster}"))?),
    )
    .await
    .map_err(|_| format!("{cluster} was not found in DNS in time"))?
    .map_err(|e| format!("cannot find {cluster}'s hosts in DNS: {e}"))?;
    let mut hosts = Vec::new();
    for record in srv.answers().iter().take(MAX_HOSTS * 2) {
        if let RData::SRV(s) = &record.data {
            hosts.push((s.target.to_ascii().trim_end_matches('.').to_owned(), s.port));
        }
    }
    let mut options = String::new();
    if let Ok(Ok(txt)) = tokio::time::timeout(COMMAND_TIME, resolver.txt_lookup(name(cluster)?)).await {
        for record in txt.answers().iter().take(1) {
            if let RData::TXT(t) = &record.data {
                for part in &t.txt_data {
                    options.push_str(&String::from_utf8_lossy(part));
                }
            }
        }
    }
    Ok((hosts, options.chars().take(2_000).collect()))
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A connection to a cluster's primary, signed in.
pub(crate) struct Conn {
    stream: Box<dyn Io>,
    next: i32,
    host: String,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl Conn {
    async fn to(
        host: &str,
        port: u16,
        address: &Address,
        credential: &Credential,
        places: &Places,
    ) -> Result<Self, String> {
        let (stream, _) = net::connect(host, port, places.allow_private).await?;
        let stream: Box<dyn Io> = if address.tls {
            let config = net::tls_config(credential.fields.get("ca").map(String::as_str))?;
            Box::new(net::tls(stream, &config, host).await?)
        } else {
            Box::new(stream)
        };
        Ok(Self {
            stream,
            next: 1,
            host: host.to_owned(),
        })
    }

    /// A connection to the cluster's primary, signed in with the credential's username and password.
    pub(crate) async fn open(credential: &Credential, places: &Places) -> Result<Self, String> {
        let address = address(credential).await?;
        let mut last = String::from("no host answered");
        for (host, port) in &address.hosts {
            let mut conn = match Self::to(host, *port, &address, credential, places).await {
                Ok(c) => c,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            let hello = match conn.command("admin", doc! { "hello": 1 }).await {
                Ok(h) => h,
                Err(e) => {
                    last = e;
                    continue;
                }
            };
            if !hello.get_bool("isWritablePrimary").unwrap_or(false) {
                // Another host is the primary: connected to by the same rule, and asked again.
                let Ok(primary) = hello.get_str("primary") else {
                    last = format!("{host} is not the primary and names none");
                    continue;
                };
                let (h, p) = parse(&format!("mongodb://{primary}"))?
                    .1
                    .into_iter()
                    .next()
                    .ok_or("no primary")?;
                conn = Self::to(&h, p, &address, credential, places).await?;
                let again = conn.command("admin", doc! { "hello": 1 }).await?;
                if !again.get_bool("isWritablePrimary").unwrap_or(false) {
                    last = format!("{h} is not the primary");
                    continue;
                }
            }
            conn.sign_in(credential, &address.auth_source).await?;
            return Ok(conn);
        }
        Err(last)
    }

    async fn sign_in(&mut self, credential: &Credential, source: &str) -> Result<(), String> {
        let (Some(user), Some(password)) = (
            super::super::field(credential, "username"),
            credential
                .fields
                .get("password")
                .filter(|p| !p.is_empty())
                .cloned(),
        ) else {
            return Ok(());
        };
        use openduct_connect::scram::{Hash, Scram};
        let mut scram = Scram::new(Hash::Sha256, &user, &password, Scram::nonce()?);
        let start = doc! {
            "saslStart": 1, "mechanism": "SCRAM-SHA-256",
            "payload": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: scram.client_first() }),
            "options": { "skipEmptyExchange": true },
        };
        let first = self
            .command(source, start)
            .await
            .map_err(|e| self.refused_sign_in(&e))?;
        let id = first.get("conversationId").cloned().unwrap_or(Bson::Int32(1));
        let server_first = payload(&first)?;
        let proof = scram
            .client_final(&server_first)
            .map_err(|e| format!("{} {e}", self.host))?;
        let next = doc! {
            "saslContinue": 1, "conversationId": id.clone(),
            "payload": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: proof }),
        };
        let mut answer = self
            .command(source, next)
            .await
            .map_err(|e| self.refused_sign_in(&e))?;
        scram
            .verify(&payload(&answer)?)
            .map_err(|e| format!("{} {e}", self.host))?;
        // A server that did not skip the empty last step is sent it.
        for _ in 0..2 {
            if answer.get_bool("done").unwrap_or(true) {
                return Ok(());
            }
            let empty = doc! {
                "saslContinue": 1, "conversationId": id.clone(),
                "payload": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: Vec::new() }),
            };
            answer = self.command(source, empty).await?;
        }
        Ok(())
    }

    fn refused_sign_in(&self, e: &str) -> String {
        format!(
            "{} refused the username or password (SCRAM-SHA-256): {e}",
            self.host
        )
    }

    /// Sends a command to database `db`; its answer, refused when it says it did not do it.
    pub(crate) async fn command(&mut self, db: &str, mut body: Document) -> Result<Document, String> {
        body.insert("$db", db);
        let answer = tokio::time::timeout(COMMAND_TIME, self.exchange(&body))
            .await
            .map_err(|_| format!("{} did not answer in time", self.host))??;
        let ok = match answer.get("ok") {
            Some(Bson::Double(d)) => *d == 1.0,
            Some(Bson::Int32(n)) => *n == 1,
            Some(Bson::Int64(n)) => *n == 1,
            Some(Bson::Boolean(b)) => *b,
            _ => false,
        };
        if !ok {
            let said = answer.get_str("errmsg").unwrap_or("it gave no reason");
            let code = answer
                .get_i32("code")
                .map(|c| format!(" ({c})"))
                .unwrap_or_default();
            return Err(format!(
                "MongoDB refused{code}: {}",
                said.chars().take(300).collect::<String>()
            ));
        }
        if let Ok(errors) = answer.get_array("writeErrors")
            && let Some(Bson::Document(first)) = errors.first()
        {
            let said = first.get_str("errmsg").unwrap_or("a write failed");
            return Err(format!(
                "MongoDB refused a write: {}",
                said.chars().take(300).collect::<String>()
            ));
        }
        Ok(answer)
    }

    async fn exchange(&mut self, body: &Document) -> Result<Document, String> {
        let bytes = body
            .to_vec()
            .map_err(|e| format!("cannot write the command: {e}"))?;
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        let length = i32::try_from(16 + 4 + 1 + bytes.len()).map_err(|_| "a command too large")?;
        let mut message = Vec::with_capacity(bytes.len() + 21);
        for n in [length, id, 0, OP_MSG] {
            message.extend_from_slice(&n.to_le_bytes());
        }
        message.extend_from_slice(&0u32.to_le_bytes());
        message.push(0);
        message.extend_from_slice(&bytes);
        self.stream
            .write_all(&message)
            .await
            .map_err(|e| format!("{} could not be written to: {e}", self.host))?;
        let mut head = [0u8; 16];
        self.stream
            .read_exact(&mut head)
            .await
            .map_err(|e| format!("{} could not be read: {e}", self.host))?;
        let number = |at: usize| {
            head.get(at..at + 4)
                .and_then(|b| <[u8; 4]>::try_from(b).ok())
                .map_or(0, i32::from_le_bytes)
        };
        let total = usize::try_from(number(0)).unwrap_or(0);
        if !(21..=MAX_ANSWER).contains(&total) || number(12) != OP_MSG {
            return Err(format!(
                "{} answered in a way MongoDB does not, or too much",
                self.host
            ));
        }
        let mut rest = vec![0u8; total - 16];
        self.stream
            .read_exact(&mut rest)
            .await
            .map_err(|e| format!("{} could not be read: {e}", self.host))?;
        read_msg(&rest).map_err(|e| format!("{}: {e}", self.host))
    }
}

/// The body of an `OP_MSG` (after its header): the document of its first section of kind 0.
pub(crate) fn read_msg(rest: &[u8]) -> Result<Document, String> {
    let flags = rest
        .get(..4)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map_or(0, u32::from_le_bytes);
    // With a checksum, its last 4 bytes are one: not part of the sections.
    let end = if flags & 1 == 1 {
        rest.len().saturating_sub(4)
    } else {
        rest.len()
    };
    let mut at = 4;
    while at < end {
        let kind = rest.get(at).copied().ok_or("an answer cut short")?;
        let size = rest
            .get(at + 1..at + 5)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map_or(0, i32::from_le_bytes);
        let size = usize::try_from(size).map_err(|_| "an answer cut short")?;
        let section = rest.get(at + 1..at + 1 + size).ok_or("an answer cut short")?;
        if kind == 0 {
            return Document::from_reader(section).map_err(|e| format!("an answer that is not BSON: {e}"));
        }
        at += 1 + size;
    }
    Err("an answer with no document".to_owned())
}

fn payload(answer: &Document) -> Result<Vec<u8>, String> {
    match answer.get("payload") {
        Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
        _ => Err("MongoDB's sign-in answer has no payload".to_owned()),
    }
}

/// JSON as BSON: whole numbers as 32- or 64-bit integers, others as doubles.
pub(crate) fn to_bson(value: &serde_json::Value) -> Bson {
    use serde_json::Value as Json;
    match value {
        Json::Null => Bson::Null,
        Json::Bool(b) => Bson::Boolean(*b),
        Json::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => i32::try_from(i).map_or(Bson::Int64(i), Bson::Int32),
            (None, Some(f)) => Bson::Double(f),
            _ => Bson::Null,
        },
        Json::String(s) => Bson::String(s.clone()),
        Json::Array(items) => Bson::Array(items.iter().map(to_bson).collect()),
        Json::Object(fields) => Bson::Document(fields.iter().map(|(k, v)| (k.clone(), to_bson(v))).collect()),
    }
}

/// BSON as JSON: what JSON has no type for (an object id, a date) as its text.
pub(crate) fn to_json(value: &Bson) -> serde_json::Value {
    use serde_json::{Value as Json, json};
    match value {
        Bson::Null | Bson::Undefined => Json::Null,
        Bson::Boolean(b) => json!(b),
        Bson::Int32(n) => json!(n),
        Bson::Int64(n) => json!(n),
        Bson::Double(d) => super::super::filter::number_json(*d),
        Bson::String(s) => json!(s),
        Bson::Array(items) => Json::Array(items.iter().map(to_json).collect()),
        Bson::Document(d) => Json::Object(d.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
        Bson::ObjectId(id) => json!(id.to_hex()),
        Bson::DateTime(t) => json!(t.timestamp_millis()),
        other => json!(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cluster_s_url_is_read_and_a_login_in_it_refused() {
        assert_eq!(
            parse("mongodb+srv://cluster0.abcde.mongodb.net/?retryWrites=true").unwrap(),
            (
                true,
                vec![("cluster0.abcde.mongodb.net".to_owned(), 27017)],
                "retryWrites=true".to_owned()
            )
        );
        assert_eq!(
            parse("mongodb://a.example.com:27018,b.example.com/shop?tls=true")
                .unwrap()
                .1,
            [
                ("a.example.com".to_owned(), 27018),
                ("b.example.com".to_owned(), 27017)
            ]
        );
        assert!(
            parse("mongodb://user:secret@host")
                .unwrap_err()
                .contains("own fields")
        );
        assert!(parse("mongodb+srv://a.example.com:27017").is_err());
        assert!(parse("http://host").is_err());
        assert!(parse("mongodb://host:port").is_err());
    }

    #[test]
    fn srv_hosts_must_be_in_the_cluster_s_own_domain() {
        assert!(in_domain(
            "cluster0.abcde.mongodb.net",
            "ac-1-shard-00-00.abcde.mongodb.net"
        ));
        assert!(!in_domain("cluster0.abcde.mongodb.net", "evil.example.com"));
        assert!(!in_domain("cluster0.abcde.mongodb.net", "abcde.mongodb.net"));
        assert!(!in_domain(
            "cluster0.abcde.mongodb.net",
            "x.abcde.mongodb.net.example.com"
        ));
    }

    #[test]
    fn an_answer_is_read_from_its_first_document() {
        let body = doc! { "ok": 1.0, "n": 2 }.to_vec().unwrap();
        let mut rest = 0u32.to_le_bytes().to_vec();
        rest.push(0);
        rest.extend(&body);
        assert_eq!(read_msg(&rest).unwrap().get_i32("n").unwrap(), 2);
        assert!(read_msg(&rest[..rest.len() - 3]).is_err());
    }

    #[test]
    fn json_and_bson_go_both_ways() {
        let j = serde_json::json!({ "a": 1, "b": 5_000_000_000_i64, "c": 0.5, "d": [true, null], "e": { "f": "g" } });
        assert_eq!(to_json(&to_bson(&j)), j);
        assert_eq!(to_bson(&serde_json::json!(1)), Bson::Int32(1));
    }
}
