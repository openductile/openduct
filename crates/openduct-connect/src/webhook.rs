// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: webhook:` - receive HTTP POSTs, each one a message. A sender hears `202 Accepted` only once its message
//! is in the topic, so an acknowledged delivery is never lost.
//!
//! ```yaml
//! from:
//!   webhook:
//!     listen: 0.0.0.0:8080
//!     path: /hooks/payments
//!     secret: secret:WEBHOOK_SECRET         # check each body's HMAC-SHA256 signature
//!     signature_header: X-Signature
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hmac::{KeyInit, Mac};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use openduct_pipeline::Connector;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::settings::{Limits, Settings};
use crate::{SourceBatch, SourceMessage};

const SETTINGS: &[&str] = &[
    "listen",
    "path",
    "secret",
    "signature_header",
    "max_body_kib",
    "keep_headers",
    "format",
];
/// Deliveries waiting for the pipeline; past this many, senders are told to try again (503).
const QUEUE: usize = 1_000;
const MAX_CONNECTIONS: usize = 1_024;
/// How long a sender waits for its message to be written before hearing 503.
const WRITE_WAIT: Duration = Duration::from_secs(30);

struct Delivery {
    body: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
    ack: oneshot::Sender<Result<(), String>>,
}

struct Rules {
    path: String,
    secret: Option<Vec<u8>>,
    signature_header: String,
    max_body: usize,
    keep_headers: Vec<String>,
    json: bool,
    queue: mpsc::Sender<Delivery>,
}

#[derive(Debug)]
pub struct WebhookSource {
    listen: SocketAddr,
    path: String,
    secret: Option<Vec<u8>>,
    signature_header: String,
    max_body: usize,
    keep_headers: Vec<String>,
    json: bool,
    receiver: Option<mpsc::Receiver<Delivery>>,
    bound: Option<SocketAddr>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Delivery({} bytes)", self.body.len())
    }
}

impl WebhookSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let listen = s.text("listen")?.unwrap_or_else(|| "127.0.0.1:8080".to_owned());
        let listen: SocketAddr = listen
            .parse()
            .map_err(|_| format!("{}: is host:port, such as 0.0.0.0:8080", s.path("listen")))?;
        let path = s.text("path")?.unwrap_or_else(|| "/".to_owned());
        if !path.starts_with('/') {
            return Err(format!("{}: starts with /", s.path("path")));
        }
        Ok(Self {
            listen,
            path,
            secret: s.text("secret")?.map(String::into_bytes),
            signature_header: s
                .text("signature_header")?
                .unwrap_or_else(|| "X-Signature".to_owned())
                .to_lowercase(),
            max_body: usize::try_from(s.number("max_body_kib", 1_024, 1..=65_536)?).unwrap_or(1_024) * 1024,
            keep_headers: s
                .list("keep_headers")?
                .into_iter()
                .map(|h| h.to_lowercase())
                .collect(),
            json: s.choice("format", &["json", "text"])? == "json",
            receiver: None,
            bound: None,
        })
    }

    pub(crate) fn describe(&self) -> String {
        let at = self.bound.unwrap_or(self.listen);
        format!(
            "POST http://{at}{}{}",
            self.path,
            if self.secret.is_some() { " (signed)" } else { "" }
        )
    }

    /// The address it listens on, once started.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.bound
    }

    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let listener = TcpListener::bind(self.listen)
            .await
            .map_err(|e| format!("from.webhook: cannot listen on {}: {e}", self.listen))?;
        self.bound = listener.local_addr().ok();
        let (queue, receiver) = mpsc::channel(QUEUE);
        self.receiver = Some(receiver);
        let rules = Arc::new(Rules {
            path: self.path.clone(),
            secret: self.secret.clone(),
            signature_header: self.signature_header.clone(),
            max_body: self.max_body,
            keep_headers: self.keep_headers.clone(),
            json: self.json,
            queue,
        });
        let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    return;
                };
                let rules = Arc::clone(&rules);
                tokio::spawn(async move {
                    let service = service_fn(move |request| answer(Arc::clone(&rules), request));
                    // A sender that never finishes its headers is cut off rather than holding a connection open.
                    let _ = http1::Builder::new()
                        .timer(TokioTimer::new())
                        .header_read_timeout(Duration::from_secs(10))
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                    drop(permit);
                });
            }
        });
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> Result<SourceBatch, String> {
        let Some(receiver) = self.receiver.as_mut() else {
            return Err("the webhook was not started".to_owned());
        };
        let mut messages = Vec::new();
        match tokio::time::timeout(Duration::from_secs(1), receiver.recv()).await {
            Ok(Some(first)) => messages.push(message(first)),
            Ok(None) => return Err("the webhook stopped".to_owned()),
            Err(_) => {
                return Ok(SourceBatch {
                    messages,
                    position: None,
                });
            }
        }
        while messages.len() < 500 {
            match receiver.try_recv() {
                Ok(more) => messages.push(message(more)),
                Err(_) => break,
            }
        }
        Ok(SourceBatch {
            messages,
            position: None,
        })
    }
}

fn message(delivery: Delivery) -> SourceMessage {
    SourceMessage {
        key: None,
        value: Some(delivery.body),
        headers: delivery.headers,
        ack: Some(delivery.ack),
    }
}

fn reply(status: StatusCode, text: &str) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(format!("{text}\n"))));
    *response.status_mut() = status;
    response
}

async fn answer(
    rules: Arc<Rules>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    if request.uri().path() != rules.path {
        return Ok(reply(StatusCode::NOT_FOUND, "not found"));
    }
    if request.method() != Method::POST {
        return Ok(reply(StatusCode::METHOD_NOT_ALLOWED, "send a POST"));
    }
    let signature = request
        .headers()
        .get(&rules.signature_header)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().trim_start_matches("sha256=").to_owned());
    let kept: Vec<(String, Vec<u8>)> = rules
        .keep_headers
        .iter()
        .filter_map(|name| {
            request
                .headers()
                .get(name)
                .map(|v| (name.clone(), v.as_bytes().to_vec()))
        })
        .collect();
    let body = match Limited::new(request.into_body(), rules.max_body).collect().await {
        Ok(b) => b.to_bytes().to_vec(),
        Err(_) => return Ok(reply(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large")),
    };
    if let Some(secret) = &rules.secret {
        let Some(given) = signature.and_then(|s| hex(&s)) else {
            return Ok(reply(StatusCode::UNAUTHORIZED, "the signature is missing"));
        };
        let Ok(mut mac) = hmac::Hmac::<Sha256>::new_from_slice(secret) else {
            return Ok(reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the secret cannot be used",
            ));
        };
        mac.update(&body);
        // A constant-time comparison: how long it takes says nothing about how much of a guess was right.
        if mac.verify_slice(&given).is_err() {
            return Ok(reply(StatusCode::UNAUTHORIZED, "the signature does not match"));
        }
    }
    if rules.json
        && std::str::from_utf8(&body)
            .ok()
            .and_then(|t| openduct_expr::Value::parse_json(t).ok())
            .is_none()
    {
        return Ok(reply(StatusCode::BAD_REQUEST, "the body is not JSON"));
    }
    let (ack, written) = oneshot::channel();
    if rules
        .queue
        .try_send(Delivery {
            body,
            headers: kept,
            ack,
        })
        .is_err()
    {
        return Ok(reply(StatusCode::SERVICE_UNAVAILABLE, "busy: try again shortly"));
    }
    Ok(match tokio::time::timeout(WRITE_WAIT, written).await {
        Ok(Ok(Ok(()))) => reply(StatusCode::ACCEPTED, "accepted"),
        Ok(Ok(Err(_))) => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the message could not be stored",
        ),
        _ => reply(StatusCode::SERVICE_UNAVAILABLE, "not stored in time: try again"),
    })
}

fn hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| text.get(i..i + 2).and_then(|b| u8::from_str_radix(b, 16).ok()))
        .collect()
}
