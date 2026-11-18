// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `--metrics-listen`: what the broker measures (and, under `openduct start`, its workflows) for Prometheus, at
//! `GET /metrics`.
//!
//! A small HTTP/1.1 responder of its own rather than a web framework, so a broker-only build stays small: one
//! request per connection, a few connections at a time, each request read within a few seconds and at most 8 KiB.
//! Nothing a scraper - or a stranger - sends can hold the broker up. Off the loopback address it asks for a token
//! (`Authorization: Bearer ...`, read from an owner-only file), unless running insecurely was said on purpose.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openduct_broker::BrokerMetrics;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// The most of a request read: its first line and headers.
const MAX_HEAD: usize = 8 * 1024;
/// A request must arrive within this.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Connections served at once; more wait to be accepted.
const AT_ONCE: usize = 8;

/// More metrics beside the broker's (the workflows', under `openduct start`).
pub(crate) type Extra = fn() -> String;

/// The token Prometheus sends, from `path`: the file's first line, which only its owner may read.
pub(crate) fn read_token(path: &Path) -> Result<Arc<str>, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|e| format!("cannot read the metrics token file {}: {e}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "the metrics token file {} can be read by others: make it yours alone (chmod 600 {})",
                path.display(),
                path.display()
            ));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read the metrics token file {}: {e}", path.display()))?;
    let token = text.lines().next().unwrap_or_default().trim();
    if token.len() < 16 {
        return Err(format!(
            "the metrics token in {} is shorter than 16 characters",
            path.display()
        ));
    }
    Ok(Arc::from(token))
}

/// Starts serving metrics on `listen`; the task ends when it is aborted.
pub(crate) async fn start(
    listen: SocketAddr,
    token: Option<Arc<str>>,
    allow_insecure: bool,
    broker: BrokerMetrics,
    extra: Option<Extra>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    if !listen.ip().is_loopback() && token.is_none() && !allow_insecure {
        return Err(format!(
            "metrics on {listen} would tell anyone who asks your topics and groups: give --metrics-token-file, or \
             listen on 127.0.0.1"
        ));
    }
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("cannot listen for metrics on {listen}: {e}"))?;
    let at = listener.local_addr().map_err(|e| e.to_string())?;
    tracing::info!(address = %at, "metrics for Prometheus at http://{at}/metrics");
    Ok(tokio::spawn(serve(listener, token, broker, extra)))
}

async fn serve(listener: TcpListener, token: Option<Arc<str>>, broker: BrokerMetrics, extra: Option<Extra>) {
    let permits = Arc::new(Semaphore::new(AT_ONCE));
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let Ok((socket, _)) = listener.accept().await else {
            continue;
        };
        let (token, broker) = (token.clone(), broker.clone());
        tokio::spawn(async move {
            let _ = tokio::time::timeout(READ_TIMEOUT * 2, answer(socket, token.as_deref(), &broker, extra))
                .await;
            drop(permit);
        });
    }
}

async fn answer(
    mut socket: TcpStream,
    token: Option<&str>,
    broker: &BrokerMetrics,
    extra: Option<Extra>,
) -> std::io::Result<()> {
    let Ok(Some(head)) = tokio::time::timeout(READ_TIMEOUT, read_head(&mut socket)).await else {
        return Ok(());
    };
    let (status, body) = match route(&head, token) {
        Ok(head_only) => {
            let mut body = broker.render().await;
            if let Some(extra) = extra {
                body.push_str(&extra());
            }
            if head_only {
                ("200 OK", Body::Head(body.len()))
            } else {
                ("200 OK", Body::Full(body))
            }
        }
        Err(status) => (status, Body::Full(format!("{status}\n"))),
    };
    let (length, text) = match &body {
        Body::Full(text) => (text.len(), text.as_str()),
        Body::Head(length) => (*length, ""),
    };
    let challenge = if status.starts_with("401") {
        "WWW-Authenticate: Bearer\r\n"
    } else {
        ""
    };
    let reply = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {length}\r\n\
         {challenge}Cache-Control: no-store\r\nConnection: close\r\n\r\n{text}"
    );
    socket.write_all(reply.as_bytes()).await?;
    socket.shutdown().await
}

enum Body {
    Full(String),
    Head(usize),
}

/// The request's first line and headers, up to the blank line; `None` when it is longer than [`MAX_HEAD`] or the
/// connection closes first.
async fn read_head(socket: &mut TcpStream) -> Option<String> {
    let mut head = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        head.extend_from_slice(chunk.get(..n)?);
        if head.windows(4).any(|w| w == b"\r\n\r\n") {
            return String::from_utf8(head).ok();
        }
        if head.len() > MAX_HEAD {
            return None;
        }
    }
}

/// Whether the request may have the metrics (`Ok(true)` for HEAD: headers only), or the status refusing it.
fn route(head: &str, token: Option<&str>) -> Result<bool, &'static str> {
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or_default().split(' ');
    let (method, target) = (first.next().unwrap_or_default(), first.next().unwrap_or_default());
    let path = target.split('?').next().unwrap_or_default();
    if path != "/metrics" {
        return Err("404 Not Found");
    }
    let head_only = match method {
        "GET" => false,
        "HEAD" => true,
        _ => return Err("405 Method Not Allowed"),
    };
    if let Some(token) = token {
        let given = lines
            .filter_map(|l| l.split_once(':'))
            .find(|(name, _)| name.trim().eq_ignore_ascii_case("authorization"))
            .map(|(_, value)| value.trim())
            .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
            .unwrap_or_default();
        if !same(given.trim().as_bytes(), token.as_bytes()) {
            return Err("401 Unauthorized");
        }
    }
    Ok(head_only)
}

/// Equal, compared in time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn only_get_or_head_of_metrics_with_the_token() {
        let token = Some("a-token-sixteen-chars");
        let get = |headers: &str| route(&format!("GET /metrics HTTP/1.1\r\n{headers}\r\n"), token);
        assert_eq!(get("Authorization: Bearer a-token-sixteen-chars\r\n"), Ok(false));
        assert_eq!(
            get("authorization:   bearer a-token-sixteen-chars\r\n"),
            Ok(false)
        );
        assert_eq!(
            get("Authorization: Bearer wrong-token-0000000\r\n"),
            Err("401 Unauthorized")
        );
        assert_eq!(get(""), Err("401 Unauthorized"));
        assert_eq!(
            route("HEAD /metrics HTTP/1.1\r\n\r\n", None),
            Ok(true),
            "no token asked on the loopback address"
        );
        assert_eq!(
            route("POST /metrics HTTP/1.1\r\n\r\n", None),
            Err("405 Method Not Allowed")
        );
        assert_eq!(route("GET /admin HTTP/1.1\r\n\r\n", None), Err("404 Not Found"));
        assert_eq!(route("GET /metrics?x=1 HTTP/1.1\r\n\r\n", None), Ok(false));
    }

    #[test]
    fn tokens_are_compared_whole() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
    }
}
