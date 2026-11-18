// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Zero trust on the client port. No connection is trusted for where it comes from: a client proves who it is on
//! every connection (a TLS client certificate, or SASL - SCRAM, PLAIN over TLS, or OAUTHBEARER tokens) before any
//! request but ApiVersions and the sign-in itself is served, and then may do only what the access rules allow it
//! (deny by default). Sessions end: at the token's expiry, or after `max_session`, the client signs in again on the
//! same connection or is closed. Failed sign-ins close the connection, and an address that keeps failing is kept
//! out for a while. A broker that listens beyond the loopback address refuses to start without TLS, sign-in and
//! access rules unless it is told, in so many words, to run insecurely.

pub mod acl;
mod authorize;
pub mod credentials;
pub mod jwt;
pub mod quota;
pub mod scram;
mod tls;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use kafka_protocol::messages::{SaslAuthenticateResponse, SaslHandshakeResponse};
use kafka_protocol::protocol::StrBytes;
use parking_lot::Mutex;

pub use acl::{Authorizer, Operation, Pattern, Permission, Resource, Rule, Rules};
pub(crate) use authorize::{
    Authorize, CLUSTER_AUTHORIZATION_FAILED, GROUP_AUTHORIZATION_FAILED, TOPIC_AUTHORIZATION_FAILED,
};
pub use credentials::{Credentials, Users, add_user, remove_user};
pub use jwt::JwtSettings;
pub(crate) use tls::certificate_name;
pub use tls::{ClientCerts, ClientTls};

use scram::{ScramMechanism, ScramServer, Step};

const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ILLEGAL_SASL_STATE: i16 = 34;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

/// Who made a request: `User:<name>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal(String);

impl Principal {
    pub fn user(name: &str) -> Self {
        Self(format!("User:{name}"))
    }

    pub fn anonymous() -> Self {
        Self("User:ANONYMOUS".to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The client port's security. [`Security::open`] (no TLS, no sign-in, no rules) is only for a broker on the
/// loopback address or one told to run insecurely.
#[derive(Clone, Debug)]
pub struct Security {
    pub tls: Option<ClientTls>,
    /// SCRAM-SHA-256 and SCRAM-SHA-512 against these credentials.
    pub credentials: Option<Arc<Credentials>>,
    /// Also SASL PLAIN, checked against the same credentials - only with TLS.
    pub plain: bool,
    /// OAUTHBEARER tokens.
    pub tokens: Option<Arc<jwt::JwtVerifier>>,
    /// The access rules; `None` lets every signed-in principal do everything.
    pub rules: Option<Arc<Authorizer>>,
    /// Every connection must sign in (or present a client certificate); otherwise it is `User:ANONYMOUS`.
    pub require_authentication: bool,
    /// The longest a session lasts before the client must sign in again.
    pub max_session: Option<Duration>,
    /// Listen beyond the loopback address without TLS, sign-in or rules.
    pub allow_insecure: bool,
    /// The most connections one address may hold at once.
    pub max_connections_per_address: usize,
    pub(crate) meter: Arc<quota::Meter>,
    pub(crate) connections: Arc<quota::Connections>,
    decoy_key: Arc<[u8; 32]>,
    failures: Arc<Mutex<HashMap<IpAddr, (u32, Instant)>>>,
    /// Failed sign-ins since the broker started, for its metrics.
    failed_total: Arc<std::sync::atomic::AtomicU64>,
}

/// Sign-in failures from one address within this window ...
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
/// ... beyond this many keep it out until the window passes.
const MAX_FAILURES: u32 = 10;

impl Default for Security {
    fn default() -> Self {
        Self::open()
    }
}

impl Security {
    pub fn open() -> Self {
        let mut key = [0u8; 32];
        let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut key);
        Self {
            tls: None,
            credentials: None,
            plain: false,
            tokens: None,
            rules: None,
            require_authentication: false,
            max_session: None,
            allow_insecure: false,
            max_connections_per_address: 1000,
            meter: Arc::default(),
            connections: Arc::default(),
            decoy_key: Arc::new(key),
            failures: Arc::default(),
            failed_total: Arc::default(),
        }
    }

    /// Token sign-in with these settings.
    pub fn with_tokens(mut self, settings: JwtSettings) -> Result<Self, String> {
        self.tokens = Some(Arc::new(jwt::JwtVerifier::new(settings)?));
        Ok(self)
    }

    /// Whether the settings hold together, and whether they may listen on `listen`.
    pub(crate) fn check(&self, listen: SocketAddr) -> Result<(), String> {
        if self.plain && self.tls.is_none() {
            return Err("SASL PLAIN sends the password: it needs TLS".to_owned());
        }
        if self.plain && self.credentials.is_none() {
            return Err("SASL PLAIN needs a credentials file".to_owned());
        }
        let certs = self
            .tls
            .as_ref()
            .is_some_and(|t| t.client_certs != ClientCerts::None);
        if self.require_authentication && self.credentials.is_none() && self.tokens.is_none() && !certs {
            return Err("sign-in is required but no way to sign in is set (credentials, tokens or client certificates)".to_owned());
        }
        if !listen.ip().is_loopback() && !self.allow_insecure {
            let mut missing = Vec::new();
            if self.tls.is_none() {
                missing.push("TLS");
            }
            if !self.require_authentication {
                missing.push("sign-in");
            }
            if self.rules.is_none() {
                missing.push("access rules");
            }
            if !missing.is_empty() {
                return Err(format!(
                    "listening on {listen} without {}: set them, listen on 127.0.0.1, or run insecurely on purpose \
                     (OPENDUCT_ALLOW_INSECURE=true)",
                    missing.join(", ")
                ));
            }
        }
        Ok(())
    }

    fn mechanisms(&self) -> Vec<&'static str> {
        let mut m = Vec::new();
        if self.credentials.is_some() {
            m.extend(ScramMechanism::ALL.map(ScramMechanism::name));
            if self.plain && self.tls.is_some() {
                m.push("PLAIN");
            }
        }
        if self.tokens.is_some() {
            m.push("OAUTHBEARER");
        }
        m
    }

    /// Whether an address has failed to sign in too often lately.
    pub(crate) fn kept_out(&self, ip: IpAddr) -> bool {
        self.failures
            .lock()
            .get(&ip)
            .is_some_and(|(n, since)| *n >= MAX_FAILURES && since.elapsed() < FAILURE_WINDOW)
    }

    /// Failed sign-ins since the broker started.
    pub(crate) fn sign_in_failures(&self) -> u64 {
        self.failed_total.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn failed(&self, ip: IpAddr) {
        self.failed_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut f = self.failures.lock();
        if f.len() > 100_000 {
            f.retain(|_, (_, since)| since.elapsed() < FAILURE_WINDOW);
        }
        let entry = f.entry(ip).or_insert((0, Instant::now()));
        if entry.1.elapsed() >= FAILURE_WINDOW {
            *entry = (0, Instant::now());
        }
        entry.0 += 1;
        if entry.0 == MAX_FAILURES {
            tracing::warn!(%ip, "an address failed to sign in {MAX_FAILURES} times: kept out for a minute");
        }
    }
}

#[allow(clippy::large_enum_variant)] // one per connection signing in, for a moment
enum Exchange {
    Scram(ScramServer),
    Plain,
    Token,
}

/// One connection's sign-in state.
pub(crate) struct Session {
    pub(crate) peer: IpAddr,
    principal: Option<Principal>,
    expires: Option<Instant>,
    exchange: Option<Exchange>,
    /// Close the connection once the answers already queued are written.
    pub(crate) close: bool,
}

impl Session {
    pub(crate) fn new(peer: IpAddr, certificate: Option<String>, security: &Security) -> Self {
        let principal = match certificate {
            Some(name) => Some(Principal::user(&name)),
            None if !security.require_authentication => Some(Principal::anonymous()),
            None => None,
        };
        let expires = principal
            .as_ref()
            .and(security.max_session)
            .map(|d| Instant::now() + d);
        Self {
            peer,
            principal,
            expires,
            exchange: None,
            close: false,
        }
    }

    /// The principal of a request, or why the connection serves none.
    pub(crate) fn principal(&self) -> Result<&Principal, &'static str> {
        let p = self.principal.as_ref().ok_or("sign in first")?;
        if self.expires.is_some_and(|e| Instant::now() >= e) {
            return Err("the session has ended: sign in again");
        }
        Ok(p)
    }

    pub(crate) fn handshake(&mut self, security: &Security, mechanism: &str) -> SaslHandshakeResponse {
        let offered = security.mechanisms();
        let mut res = SaslHandshakeResponse::default()
            .with_mechanisms(offered.iter().map(|m| StrBytes::from_static_str(m)).collect());
        self.exchange = match mechanism {
            m if offered.contains(&m) && m.starts_with("SCRAM-") => {
                ScramMechanism::from_name(m).map(|m| Exchange::Scram(ScramServer::new(m)))
            }
            "PLAIN" if offered.contains(&"PLAIN") => Some(Exchange::Plain),
            "OAUTHBEARER" if offered.contains(&"OAUTHBEARER") => Some(Exchange::Token),
            _ => None,
        };
        if self.exchange.is_none() {
            res.error_code = UNSUPPORTED_SASL_MECHANISM;
            self.close = true;
        }
        res
    }

    pub(crate) fn authenticate(&mut self, security: &Security, bytes: &Bytes) -> SaslAuthenticateResponse {
        let fail = |s: &mut Self, why: &str| {
            security.failed(s.peer);
            s.close = true;
            s.exchange = None;
            tracing::info!(peer = %s.peer, reason = why, "sign-in refused");
            SaslAuthenticateResponse::default()
                .with_error_code(SASL_AUTHENTICATION_FAILED)
                .with_error_message(Some(StrBytes::from_string(format!("sign-in failed: {why}"))))
        };
        let Some(exchange) = self.exchange.take() else {
            self.close = true;
            return SaslAuthenticateResponse::default()
                .with_error_code(ILLEGAL_SASL_STATE)
                .with_error_message(Some(StrBytes::from_static_str(
                    "no SASL handshake on this connection",
                )));
        };
        let (name, lifetime, last) = match exchange {
            Exchange::Scram(mut server) => {
                let Some(creds) = &security.credentials else {
                    return fail(self, "no credentials");
                };
                let mech = server.mechanism();
                let step = server.step(
                    bytes,
                    |u| creds.lookup(mech, u),
                    security.decoy_key.as_slice(),
                    &scram::nonce(),
                );
                match step {
                    Step::Continue(next) => {
                        self.exchange = Some(Exchange::Scram(server));
                        return SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(next));
                    }
                    Step::Authenticated { user, last } => (user, security.max_session, last),
                    Step::Failed(why) => return fail(self, why),
                }
            }
            Exchange::Plain => {
                let Some(creds) = &security.credentials else {
                    return fail(self, "no credentials");
                };
                let mut parts = bytes.split(|b| *b == 0);
                let (Some(authzid), Some(user), Some(pass), None) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                else {
                    return fail(self, "malformed PLAIN message");
                };
                let (Ok(user), Ok(pass), Ok(authzid)) = (
                    std::str::from_utf8(user),
                    std::str::from_utf8(pass),
                    std::str::from_utf8(authzid),
                ) else {
                    return fail(self, "malformed PLAIN message");
                };
                if !authzid.is_empty() && authzid != user {
                    return fail(self, "acting as another name is not allowed");
                }
                let ok = creds
                    .lookup(ScramMechanism::Sha512, user)
                    .is_some_and(|c| c.matches(ScramMechanism::Sha512, pass));
                if !ok {
                    return fail(self, "wrong name or password");
                }
                (user.to_owned(), security.max_session, Vec::new())
            }
            Exchange::Token => {
                let Some(verifier) = &security.tokens else {
                    return fail(self, "no token settings");
                };
                let token = match jwt::bearer_token(bytes) {
                    Ok(t) => t,
                    Err(why) => return fail(self, why),
                };
                match verifier.verify(token, SystemTime::now()) {
                    Ok((name, exp)) => {
                        let left = exp.duration_since(SystemTime::now()).unwrap_or(Duration::ZERO);
                        let lifetime = Some(security.max_session.map_or(left, |m| m.min(left)));
                        (name, lifetime, Vec::new())
                    }
                    Err(why) => return fail(self, &why),
                }
            }
        };
        let principal = Principal::user(&name);
        // Signing in again on the same connection must keep the same principal.
        if self
            .principal
            .as_ref()
            .is_some_and(|p| *p != principal && *p != Principal::anonymous())
        {
            return fail(self, "a connection cannot change its principal");
        }
        self.expires = lifetime.map(|d| Instant::now() + d);
        tracing::debug!(peer = %self.peer, principal = %principal, "signed in");
        self.principal = Some(principal);
        let lifetime_ms = lifetime.map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        SaslAuthenticateResponse::default()
            .with_auth_bytes(Bytes::from(last))
            .with_session_lifetime_ms(lifetime_ms)
    }
}
