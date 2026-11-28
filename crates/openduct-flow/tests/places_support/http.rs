// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The stand-ins' HTTP: one server on this machine, each vendor by the first name of the path (`/api.box.com/...`),
//! requests read whole and answered, a connection kept open between requests.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

/// A request as it came.
#[derive(Debug, Clone)]
pub(crate) struct Request {
    pub method: String,
    /// The vendor's host, as the first name of the path.
    pub host: String,
    /// The rest of the path, encoded as it came (`/2.0/folders/0/items`).
    pub path: String,
    /// The query as it came (encoded).
    pub raw_query: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub(crate) fn q(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub(crate) fn bearer(&self) -> Option<&str> {
        self.header("authorization")
            .and_then(|a| a.strip_prefix("Bearer "))
    }
}

/// An answer: status, headers, body.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Reply {
    pub(crate) fn json(status: u16, body: &serde_json::Value) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: body.to_string().into_bytes(),
        }
    }

    pub(crate) fn bytes(status: u16, kind: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), kind.into())],
            body,
        }
    }

    pub(crate) fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub(crate) fn with(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

pub(crate) fn decode(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 3 <= b.len()
            && let Ok(v) = u8::from_str_radix(&text[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Encoded as a path: letters, digits, `-._~/` kept.
pub(crate) fn encode_path(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut words = line.split_whitespace();
    let method = words.next()?.to_owned();
    let target = words.next()?.to_owned();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).ok()?;
        if h.trim().is_empty() {
            break;
        }
        let (k, v) = h.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    let length: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .map_or(0, |(_, v)| v.parse().unwrap());
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    let (path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
    let query = raw_query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect();
    let rest = path.trim_start_matches('/');
    let (host, rest) = rest.split_once('/').unwrap_or((rest, ""));
    Some(Request {
        method,
        host: host.to_owned(),
        path: format!("/{rest}"),
        raw_query: raw_query.to_owned(),
        query,
        headers,
        body,
    })
}

fn write_reply(stream: &mut TcpStream, reply: &Reply, head: bool) {
    let reason = match reply.status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        302 => "Found",
        _ => "Status",
    };
    let mut out = format!(
        "HTTP/1.1 {} {reason}\r\ncontent-length: {}\r\n",
        reply.status,
        reply.body.len()
    );
    for (k, v) in &reply.headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes());
    if !head && reply.status != 204 {
        let _ = stream.write_all(&reply.body);
    }
}

/// What the server answers with: each vendor's stand-in, by its host.
pub(crate) type Handler = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

/// A server for `handler`, on this machine: its port. Every request is kept in `log` (method, host, path).
pub(crate) fn serve(handler: Handler, log: Arc<Mutex<Vec<String>>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let handler = Arc::clone(&handler);
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                let mut writer = stream.try_clone().unwrap();
                let mut reader = BufReader::new(stream);
                while let Some(r) = read_request(&mut reader) {
                    log.lock()
                        .unwrap()
                        .push(format!("{} {}{}", r.method, r.host, r.path));
                    let reply = handler(&r);
                    write_reply(&mut writer, &reply, r.method == "HEAD");
                    if r.header("connection") == Some("close") {
                        break;
                    }
                }
            });
        }
    });
    port
}
