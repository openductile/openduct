// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The studio's HTTP server: HTTP/1.1, every connection and request bounded, and the checks every request passes
//! before it is answered - the `Host` it names must be one of the studio's (so a site that points its own name at
//! this machine cannot reach it), and a change must come as JSON from the studio's own page.

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::State;

/// The most connections at once.
const MAX_CONNECTIONS: usize = 1024;
/// The largest request body, in bytes.
pub(crate) const MAX_BODY: usize = 1024 * 1024;
/// How long a request's headers may take to arrive.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) type Answer = Response<Full<Bytes>>;

/// A request, read and checked.
#[derive(Debug)]
pub(crate) struct Call {
    pub method: Method,
    pub path: String,
    /// What follows `?`, as sent.
    pub query: String,
    pub peer: IpAddr,
    pub cookie: Option<String>,
    pub csrf: Option<String>,
    /// The workspace the request is for (`x-openduct-workspace`).
    pub workspace: Option<String>,
    /// The body as JSON (a webhook's: `null` when it is not JSON).
    pub body: Value,
    /// A webhook's body as it came, its type, and its headers (names in small letters).
    pub raw: Bytes,
    pub content_type: String,
    pub headers: Vec<(String, String)>,
    /// The name (and port) the studio was reached by, as sent: what an address back to it is built from.
    pub host: String,
}

pub(crate) async fn serve(listener: TcpListener, state: Arc<State>, stop: impl Future<Output = ()>) {
    let slots = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    tokio::pin!(stop);
    loop {
        let accepted = tokio::select! {
            () = &mut stop => return,
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = match accepted {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "cannot accept a studio connection");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let state = Arc::clone(&state);
                async move { Ok::<_, Infallible>(answer(&state, peer, request).await) }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .max_buf_size(64 * 1024)
                .serve_connection(TokioIo::new(stream), service);
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "studio connection ended");
            }
            drop(slot);
        });
    }
}

async fn answer(state: &State, peer: SocketAddr, request: Request<Incoming>) -> Answer {
    let mut response = match checked(state, peer, request).await {
        Ok(call) => crate::api::route(state, call).await,
        Err(refused) => refused,
    };
    secure(response.headers_mut());
    response
}

/// The request, if it passes the checks every request must; the answer refusing it if not.
async fn checked(state: &State, peer: SocketAddr, request: Request<Incoming>) -> Result<Call, Answer> {
    // A webhook is called by another system, not the studio's page: it is reached by whatever name the caller
    // knows, sends what it sends, and is guarded by its workflow's own token instead.
    let hook = request.uri().path().starts_with("/hooks/");
    let reached = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(host_name);
    if !hook && !host.as_deref().is_some_and(|h| state.hosts.allows(h)) {
        return Err(problem(
            StatusCode::MISDIRECTED_REQUEST,
            "this studio is not reached by that name: start it with --studio-host <name> to allow one",
        ));
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().unwrap_or_default().to_owned();
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|c| c.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let headers: Vec<(String, String)> = if hook {
        request
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str().to_owned(), v.to_owned())))
            .collect()
    } else {
        Vec::new()
    };
    let cookie = session_cookie(request.headers());
    let csrf = request
        .headers()
        .get("x-openduct-csrf")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let workspace = request
        .headers()
        .get("x-openduct-workspace")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut raw = Bytes::new();
    let body = if method == Method::GET || method == Method::HEAD {
        Value::Null
    } else if hook {
        raw = match Limited::new(request.into_body(), MAX_BODY).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Err(problem(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("a delivery of at most {MAX_BODY} bytes"),
                ));
            }
        };
        serde_json::from_slice(&raw).unwrap_or(Value::Null)
    } else {
        // A change: from the studio's own page (a browser names the page's origin), as JSON (which another site
        // cannot send without the browser asking this server first, and this server never agrees).
        if let Some(origin) = request
            .headers()
            .get(header::ORIGIN)
            .and_then(|o| o.to_str().ok())
        {
            let origin_host = origin.split_once("://").map_or(origin, |(_, rest)| rest);
            if !state.hosts.allows(&host_name(origin_host)) {
                return Err(problem(StatusCode::FORBIDDEN, "a change from another site"));
            }
        }
        let json = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|c| c.to_str().ok())
            .is_some_and(|c| {
                c.split(';')
                    .next()
                    .is_some_and(|t| t.trim() == "application/json")
            });
        if !json {
            return Err(problem(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "a change is sent as application/json",
            ));
        }
        let bytes = match Limited::new(request.into_body(), MAX_BODY).collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => {
                return Err(problem(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("a request of at most {MAX_BODY} bytes"),
                ));
            }
        };
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|_| problem(StatusCode::BAD_REQUEST, "the body is not JSON"))?
        }
    };
    Ok(Call {
        method,
        path,
        query,
        peer: peer.ip(),
        cookie,
        csrf,
        workspace,
        body,
        raw,
        content_type,
        headers,
        host: reached,
    })
}

/// `name` of `name:port` or `[v6]:port`, in small letters.
fn host_name(value: &str) -> String {
    let value = value.trim();
    let name = if let Some(rest) = value.strip_prefix('[') {
        rest.split(']').next().unwrap_or(rest)
    } else {
        value.rsplit_once(':').map_or(value, |(name, port)| {
            if port.chars().all(|c| c.is_ascii_digit()) {
                name
            } else {
                value
            }
        })
    };
    name.to_ascii_lowercase()
}

fn session_cookie(headers: &hyper::HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .find(|(name, _)| *name == crate::api::COOKIE)
        .map(|(_, value)| value.to_owned())
}

/// Headers on every answer: no framing by another site, no guessing of types, no referrer, scripts from here only.
fn secure(headers: &mut hyper::HeaderMap) {
    let set = |headers: &mut hyper::HeaderMap, name: &'static str, value: &'static str| {
        headers.insert(name, HeaderValue::from_static(value));
    };
    set(
        headers,
        "content-security-policy",
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
         connect-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'; form-action 'self'",
    );
    set(headers, "x-content-type-options", "nosniff");
    set(headers, "x-frame-options", "DENY");
    set(headers, "referrer-policy", "no-referrer");
    set(headers, "cross-origin-opener-policy", "same-origin");
}

/// A JSON answer.
pub(crate) fn json(status: StatusCode, body: &Value) -> Answer {
    let mut response = Response::new(Full::new(Bytes::from(body.to_string())));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// An answer saying what is wrong, as `{"error": "..."}`.
pub(crate) fn problem(status: StatusCode, why: &str) -> Answer {
    json(status, &json!({ "error": why }))
}

/// The names the studio answers to: these machine names, its listen address, and those it was given.
#[derive(Debug, Clone)]
pub(crate) struct Hosts(Vec<String>);

impl Hosts {
    pub(crate) fn new(listen: SocketAddr, given: &[String]) -> Self {
        let mut names: Vec<String> = ["localhost", "127.0.0.1", "::1"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        if !listen.ip().is_unspecified() {
            names.push(listen.ip().to_string());
        }
        names.extend(given.iter().map(|g| host_name(g)));
        Self(names)
    }

    fn allows(&self, name: &str) -> bool {
        self.0.iter().any(|n| n == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_is_its_name_without_the_port() {
        assert_eq!(host_name("LocalHost:5480"), "localhost");
        assert_eq!(host_name("[::1]:5480"), "::1");
        assert_eq!(host_name("studio.example.com"), "studio.example.com");
        let hosts = Hosts::new("0.0.0.0:5480".parse().unwrap(), &["Flows.Example.com".to_owned()]);
        assert!(hosts.allows("localhost") && hosts.allows("flows.example.com"));
        assert!(!hosts.allows("0.0.0.0") && !hosts.allows("attacker.example"));
    }
}
