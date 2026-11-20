// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The registry's HTTP listener: a small HTTP/1.1 responder of its own, as the broker's metrics listener is, so
//! a broker stays one small program.
//!
//! A connection is kept for the requests a client sends one after another on it (closed after a short rest, or a
//! thousand requests), a bounded number of connections at a time, each request read within seconds and no larger
//! than a schema could need. Off the
//! loopback address it asks for a token - as `Authorization: Bearer <token>`, or as the password of
//! `Authorization: Basic` (what Confluent's clients send for `basic.auth.user.info=<any name>:<token>`) - read
//! from a file only its owner can read. Or as one of the broker's own users, held to its access rules ([`SignIn`]). Given a certificate, it serves HTTPS (TLS 1.3); the token is still what
//! says who may ask.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

use std::net::IpAddr;

use openduct_broker::security::{Operation, Principal, Resource, Security, certificate_principal};

use crate::api::{Access, Act, CONTENT_TYPE, Everything, Request, Response};
use crate::service::Service;

/// The most of a request's first line and headers.
const MAX_HEAD: usize = 16 * 1024;
/// The most of a request's body: a schema, with room to spare.
const MAX_BODY: usize = 4 * 1024 * 1024;
/// A request must arrive within this.
const READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections served at once; more wait to be accepted.
const AT_ONCE: usize = 256;
/// How long a connection may rest between requests before it is closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(15);
/// The most requests one connection is served before it is asked to come again.
const MAX_REQUESTS: usize = 1_000;

/// Starts serving the registry on `listen`: the address it listens on (the port chosen, when 0 was asked) and
/// the task, which ends when it is aborted.
pub async fn start(
    listen: SocketAddr,
    sign_in: SignIn,
    allow_insecure: bool,
    tls: Option<TlsAcceptor>,
    service: Arc<Service>,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), String> {
    if !listen.ip().is_loopback() && !sign_in.asks() && !allow_insecure {
        return Err(format!(
            "the schema registry on {listen} would let anyone who reaches it change your schemas: give \
             --registry-token-file, or the broker a way to sign in, or listen on 127.0.0.1"
        ));
    }
    let token = Arc::new(sign_in);
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("cannot listen for the schema registry on {listen}: {e}"))?;
    let at = listener.local_addr().map_err(|e| e.to_string())?;
    let scheme = if tls.is_some() { "https" } else { "http" };
    tracing::info!(address = %at, "schema registry at {scheme}://{at}");
    Ok((at, tokio::spawn(serve(listener, token, tls, service))))
}

/// Who may ask the registry, and how they say who they are.
#[derive(Clone, Debug, Default)]
pub struct SignIn {
    /// A token whose holder may do everything: sent as a bearer token, or as the password of basic sign-in.
    pub token: Option<Arc<str>>,
    /// The broker's own users and rules: a client signs in as one of them - its name and password by basic
    /// sign-in (checked against the credentials SCRAM uses), or a signed token as a bearer token - and may do
    /// what the access rules allow its principal on `subject` resources (everything, when there are no rules).
    pub users: Option<Security>,
    /// The listener's TLS takes client certificates: a client that presents one the authority signed is the
    /// principal the certificate names - its first URI name, else its first DNS name, as on the broker - and is
    /// held to the same rules. One that presents none must say who it is another way.
    pub certificates: bool,
}

impl SignIn {
    /// Whether a caller has to say who it is.
    fn asks(&self) -> bool {
        self.token.is_some() || self.certificates || self.users.as_ref().is_some_and(Security::can_sign_in)
    }
}

/// What one of the broker's principals may do, by the access rules.
struct Ruled<'a> {
    security: &'a Security,
    principal: Principal,
    peer: IpAddr,
}

impl Access for Ruled<'_> {
    fn may(&self, subject: Option<&str>, act: Act) -> bool {
        let Some(rules) = &self.security.rules else {
            return true;
        };
        let (resource, name, operation) = match (subject, act) {
            (Some(subject), Act::Read) => (Resource::Subject, subject, Operation::Read),
            (Some(subject), Act::Write) => (Resource::Subject, subject, Operation::Write),
            (Some(subject), Act::Delete) => (Resource::Subject, subject, Operation::Delete),
            (Some(subject), Act::ReadLevel) => (Resource::Subject, subject, Operation::DescribeConfigs),
            (Some(subject), Act::WriteLevel) => (Resource::Subject, subject, Operation::AlterConfigs),
            // The registry as a whole is the cluster's.
            (None, Act::Read) => (Resource::Cluster, "kafka-cluster", Operation::Describe),
            (None, Act::ReadLevel) => (Resource::Cluster, "kafka-cluster", Operation::DescribeConfigs),
            (None, _) => (Resource::Cluster, "kafka-cluster", Operation::AlterConfigs),
        };
        rules.allows(&self.principal, self.peer, resource, name, operation)
    }
}

/// Who a request is from.
enum Caller {
    /// The token's holder, or anyone at all where nothing is asked.
    Trusted,
    Known(Principal),
    Nobody,
}

/// The name and password of basic sign-in.
fn basic(given: &str) -> Option<(String, Vec<u8>)> {
    let pair = given
        .strip_prefix("Basic ")
        .or_else(|| given.strip_prefix("basic "))
        .and_then(|encoded| base64(encoded.trim()))?;
    let colon = pair.iter().position(|b| *b == b':')?;
    let name = String::from_utf8(pair.get(..colon)?.to_vec()).ok()?;
    Some((name, pair.get(colon + 1..)?.to_vec()))
}

/// Who the request's `Authorization` says it is from. `private`: the connection is one a password may travel
/// on - TLS, or this machine alone.
fn caller(
    sign_in: &SignIn,
    head: &str,
    peer: IpAddr,
    private: bool,
    certificate: Option<Principal>,
) -> Caller {
    if !sign_in.asks() {
        return Caller::Trusted;
    }
    if sign_in
        .token
        .as_deref()
        .is_some_and(|token| signed_in(head, token))
    {
        return Caller::Trusted;
    }
    // A certificate the handshake verified says who this is, whatever the request's headers say.
    if let Some(principal) = certificate {
        return Caller::Known(principal);
    }
    let (Some(users), Some(given)) = (&sign_in.users, header(head, "authorization")) else {
        return Caller::Nobody;
    };
    let known = if let Some(bearer) = given
        .strip_prefix("Bearer ")
        .or_else(|| given.strip_prefix("bearer "))
    {
        users.principal_of_token(peer, bearer.trim())
    } else if private {
        basic(given).and_then(|(name, password)| {
            users.principal_of_password(peer, &name, &String::from_utf8_lossy(&password))
        })
    } else {
        // A password is not taken where anyone on the way could read it.
        None
    };
    known.map_or(Caller::Nobody, Caller::Known)
}

async fn serve(listener: TcpListener, token: Arc<SignIn>, tls: Option<TlsAcceptor>, service: Arc<Service>) {
    let permits = Arc::new(Semaphore::new(AT_ONCE));
    loop {
        let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let Ok((socket, from)) = listener.accept().await else {
            continue;
        };
        let (token, tls, service) = (Arc::clone(&token), tls.clone(), Arc::clone(&service));
        tokio::spawn(async move {
            let peer = from.ip();
            match tls {
                // The handshake, like the request after it, must come within the time a request is given.
                Some(tls) => {
                    if let Ok(Ok(secured)) = tokio::time::timeout(READ_TIMEOUT, tls.accept(socket)).await {
                        let certificate = secured
                            .get_ref()
                            .1
                            .peer_certificates()
                            .and_then(|chain| chain.first())
                            .and_then(certificate_principal);
                        let _ = answer(secured, &token, peer, true, certificate, &service).await;
                    }
                }
                None => {
                    let _ = answer(socket, &token, peer, peer.is_loopback(), None, &service).await;
                }
            }
            drop(permit);
        });
    }
}

/// A request as it was read: its first line and headers, and its body.
struct Read {
    head: String,
    body: Vec<u8>,
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then_some(value.trim())
    })
}

/// Reads one request. `carry` holds what was read past the one before it, and is left with what was read past
/// this one. `Ok(None)` when the client closed without sending another; `Err` with the status to answer when a
/// request cannot be taken.
async fn read(
    socket: &mut (impl AsyncRead + AsyncWrite + Unpin),
    carry: &mut Vec<u8>,
) -> Result<Option<Read>, &'static str> {
    let mut buffer = std::mem::take(carry);
    let end = loop {
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
        if buffer.len() > MAX_HEAD {
            return Err("431 Request Header Fields Too Large");
        }
        let mut chunk = [0u8; 2048];
        match socket.read(&mut chunk).await {
            Ok(0) if buffer.is_empty() => return Ok(None),
            Ok(0) | Err(_) => return Err("400 Bad Request"),
            Ok(n) => buffer.extend_from_slice(chunk.get(..n).unwrap_or_default()),
        }
    };
    let mut body = buffer.split_off(end);
    let head = String::from_utf8(buffer).map_err(|_| "400 Bad Request")?;
    if header(&head, "transfer-encoding").is_some() {
        return Err("411 Length Required");
    }
    let length = match header(&head, "content-length") {
        Some(length) => length.parse::<usize>().map_err(|_| "400 Bad Request")?,
        None => 0,
    };
    if length > MAX_BODY {
        return Err("413 Content Too Large");
    }
    if header(&head, "expect").is_some_and(|e| e.eq_ignore_ascii_case("100-continue")) && body.len() < length
    {
        socket
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .map_err(|_| "400 Bad Request")?;
    }
    while body.len() < length {
        let mut chunk = [0u8; 8192];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return Err("400 Bad Request"),
            Ok(n) => body.extend_from_slice(chunk.get(..n).unwrap_or_default()),
        }
    }
    // What came after this request's body is the start of the next.
    *carry = body.split_off(length);
    Ok(Some(Read { head, body }))
}

/// Compares without telling, by how long it takes, where two secrets first differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

fn base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let (mut bits, mut held) = (0u32, 0u32);
    for c in text.bytes().filter(|c| *c != b'=') {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => return None,
        };
        held = (held << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((held >> bits) & 0xff).ok()?);
        }
    }
    Some(out)
}

/// Whether the request carries the token: as a bearer token, or as the password of basic sign-in.
fn signed_in(head: &str, token: &str) -> bool {
    let Some(given) = header(head, "authorization") else {
        return false;
    };
    if let Some(bearer) = given
        .strip_prefix("Bearer ")
        .or_else(|| given.strip_prefix("bearer "))
    {
        return same(bearer.trim().as_bytes(), token.as_bytes());
    }
    given
        .strip_prefix("Basic ")
        .or_else(|| given.strip_prefix("basic "))
        .and_then(|encoded| base64(encoded.trim()))
        .is_some_and(|pair| {
            let password = pair
                .iter()
                .position(|b| *b == b':')
                .and_then(|at| pair.get(at + 1..));
            password.is_some_and(|password| same(password, token.as_bytes()))
        })
}

async fn answer(
    mut socket: impl AsyncRead + AsyncWrite + Unpin,
    sign_in: &SignIn,
    peer: IpAddr,
    private: bool,
    certificate: Option<Principal>,
    service: &Service,
) -> std::io::Result<()> {
    let mut carry = Vec::new();
    for served in 0..MAX_REQUESTS {
        // The first request must come promptly; between requests a connection may rest a little longer.
        let patience = if served == 0 { READ_TIMEOUT } else { IDLE_TIMEOUT };
        let (status, body, challenge, mut last) =
            match tokio::time::timeout(patience, read(&mut socket, &mut carry)).await {
                Err(_) | Ok(Ok(None)) => break,
                Ok(Err(status)) => (
                    status.to_owned(),
                    format!(
                        "{{\"error_code\":{},\"message\":{status:?}}}",
                        status.get(..3).unwrap_or("400")
                    ),
                    false,
                    true,
                ),
                Ok(Ok(Some(request))) => {
                    let line = request.head.lines().next().unwrap_or_default();
                    let mut first = line.split(' ');
                    let (method, target) =
                        (first.next().unwrap_or_default(), first.next().unwrap_or_default());
                    // HTTP/1.1 keeps a connection unless told to close it; HTTP/1.0 closes unless told to keep.
                    let asked_to = header(&request.head, "connection").map(str::to_ascii_lowercase);
                    let last = match asked_to.as_deref() {
                        Some(c) if c.contains("close") => true,
                        Some(c) if c.contains("keep-alive") => false,
                        _ => !line.ends_with("HTTP/1.1"),
                    };
                    let (path, query) = target.split_once('?').unwrap_or((target, ""));
                    let asked = Request {
                        method,
                        path,
                        query,
                        body: &request.body,
                    };
                    let answered = match caller(sign_in, &request.head, peer, private, certificate.clone()) {
                        Caller::Nobody => None,
                        Caller::Trusted => Some(service.handle_as(&asked, &Everything).await),
                        Caller::Known(principal) => match &sign_in.users {
                            Some(security) => {
                                let access = Ruled {
                                    security,
                                    principal,
                                    peer,
                                };
                                Some(service.handle_as(&asked, &access).await)
                            }
                            // No rules to hold it to: a signed-in principal may do everything, as on the broker.
                            None => Some(service.handle_as(&asked, &Everything).await),
                        },
                    };
                    match answered {
                        Some(Response { status, body }) => {
                            (format!("{status} {}", reason(status)), body, false, last)
                        }
                        // Whoever is not let in does not get to keep a connection either.
                        None => (
                            "401 Unauthorized".to_owned(),
                            "{\"error_code\":401,\"message\":\"Unauthorized\"}".to_owned(),
                            true,
                            true,
                        ),
                    }
                }
            };
        last |= served + 1 == MAX_REQUESTS;
        let challenge = if challenge {
            "WWW-Authenticate: Basic realm=\"schema registry\"\r\n"
        } else {
            ""
        };
        let connection = if last { "close" } else { "keep-alive" };
        let reply = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n{challenge}Cache-Control: \
             no-store\r\nConnection: {connection}\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(reply.as_bytes()).await?;
        if last {
            break;
        }
    }
    socket.shutdown().await
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        422 => "Unprocessable Content",
        500 => "Internal Server Error",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_token_is_taken_as_a_bearer_token_or_a_basic_password_and_nothing_else() {
        let token = "a-registry-token-0123456789";
        let head = |authorization: &str| {
            format!("GET /subjects HTTP/1.1\r\nHost: x\r\nAuthorization: {authorization}\r\n")
        };
        assert!(signed_in(&head(&format!("Bearer {token}")), token));
        // "anyone:a-registry-token-0123456789", as a Confluent client sends basic.auth.user.info.
        assert!(signed_in(
            &head("Basic YW55b25lOmEtcmVnaXN0cnktdG9rZW4tMDEyMzQ1Njc4OQ=="),
            token
        ));
        for wrong in [
            "Bearer a-registry-token-012345678",
            "Bearer",
            "Basic YW55b25lOndyb25n",
            "Basic !!!",
            "Digest whatever",
        ] {
            assert!(!signed_in(&head(wrong), token), "{wrong}");
        }
        assert!(!signed_in("GET /subjects HTTP/1.1\r\nHost: x\r\n", token));
        assert_eq!(
            header("GET / HTTP/1.1\r\ncontent-LENGTH:  12 \r\n", "Content-Length"),
            Some("12")
        );
    }
}
