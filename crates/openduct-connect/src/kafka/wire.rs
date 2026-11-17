// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One connection to one broker of a Kafka-protocol cluster: TCP or TLS, signed in with SASL when asked, its API
//! versions checked once, then one request at a time. The size of every answer is checked before it is read, and a
//! request cut off midway (the pipeline stopping) leaves the connection unused from then on.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslHandshakeRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable, Request, StrBytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::scram::{Hash, Scram};

/// The largest answer read. A fetch asks for at most 8 MiB, so an answer near this is not one.
const MAX_ANSWER: usize = 64 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;
const CONNECT_WAIT: Duration = Duration::from_secs(10);
/// How long an ordinary request may take.
pub(super) const ANSWER_WAIT: Duration = Duration::from_secs(30);

/// The version of each API spoken: versions that every cluster from Kafka 2.4 on answers.
const VERSIONS: &[(ApiKey, i16)] = &[
    (ApiKey::Produce, 8),
    (ApiKey::Fetch, 11),
    (ApiKey::ListOffsets, 5),
    (ApiKey::Metadata, 9),
    (ApiKey::OffsetCommit, 8),
    (ApiKey::OffsetFetch, 6),
    (ApiKey::FindCoordinator, 3),
    (ApiKey::JoinGroup, 6),
    (ApiKey::Heartbeat, 4),
    (ApiKey::LeaveGroup, 4),
    (ApiKey::SyncGroup, 4),
];
const API_VERSIONS: i16 = 3;
const SASL_HANDSHAKE: i16 = 1;
const SASL_AUTHENTICATE: i16 = 2;

const UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const SASL_AUTHENTICATION_FAILED: i16 = 58;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Failure {
    /// Trying again will not help: a refused password, a topic that is not there.
    Lasting(String),
    /// Trying again may: a broken connection, a leader that moved.
    Passing(String),
}

impl Failure {
    pub(super) fn reason(self) -> String {
        match self {
            Self::Lasting(r) | Self::Passing(r) => r,
        }
    }

    /// What a Kafka error code means for the request that got it.
    pub(super) fn of(code: i16, doing: &str) -> Self {
        use kafka_protocol::error::ParseResponseErrorCode;
        let Some(error) = code.err() else {
            return Self::Passing(format!("{doing}: no error, yet no answer"));
        };
        let reason = format!("{doing}: {error} (Kafka error {code})");
        if error.is_retriable() {
            Self::Passing(reason)
        } else {
            Self::Lasting(reason)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mechanism {
    Plain,
    Scram(Hash),
}

impl Mechanism {
    fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::Scram(Hash::Sha256) => "SCRAM-SHA-256",
            Self::Scram(Hash::Sha512) => "SCRAM-SHA-512",
        }
    }
}

/// How to reach the cluster's brokers and sign in to them.
#[derive(Clone)]
pub(super) struct Access {
    pub(super) tls: Option<Arc<rustls::ClientConfig>>,
    pub(super) sasl: Option<(Mechanism, String, String)>,
    pub(super) client_id: String,
}

impl std::fmt::Debug for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Access")
            .field("tls", &self.tls.is_some())
            .field("sasl", &self.sasl.as_ref().map(|(m, _, _)| m))
            .field("client_id", &self.client_id)
            .finish()
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub(super) struct Connection {
    stream: Box<dyn Io>,
    /// `the Kafka broker at host:port`, for messages.
    what: String,
    client_id: StrBytes,
    correlation: i32,
    /// A request went out and its answer was not read to the end.
    cut: bool,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("what", &self.what)
            .field("cut", &self.cut)
            .finish_non_exhaustive()
    }
}

impl Connection {
    pub(super) async fn open(host: &str, port: u16, access: &Access) -> Result<Self, Failure> {
        let what = format!("the Kafka broker at {host}:{port}");
        timeout(CONNECT_WAIT, Self::handshake(host, port, what.clone(), access))
            .await
            .map_err(|_| Failure::Passing(format!("{what} did not answer within 10 seconds")))?
    }

    async fn handshake(host: &str, port: u16, what: String, access: &Access) -> Result<Self, Failure> {
        let tcp = TcpStream::connect((host, port))
            .await
            .map_err(|e| Failure::Passing(format!("cannot connect to {what}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn Io> = match &access.tls {
            None => Box::new(tcp),
            Some(config) => {
                let name = rustls::pki_types::ServerName::try_from(host.to_owned())
                    .map_err(|_| Failure::Lasting(format!("{host} is not a name TLS can check")))?;
                let tls = tokio_rustls::TlsConnector::from(Arc::clone(config))
                    .connect(name, tcp)
                    .await
                    .map_err(|e| match crate::tls::failure(&e, &what) {
                        (true, reason) => Failure::Lasting(reason),
                        (false, reason) => Failure::Passing(reason),
                    })?;
                Box::new(tls)
            }
        };
        let mut conn = Self {
            stream,
            what,
            client_id: StrBytes::from_string(access.client_id.clone()),
            correlation: 0,
            cut: false,
        };
        conn.check_versions().await?;
        if let Some((mechanism, username, password)) = &access.sasl {
            conn.sign_in(*mechanism, username, password).await?;
        }
        Ok(conn)
    }

    /// Whether the broker answers every request this client sends, at the version it sends it.
    async fn check_versions(&mut self) -> Result<(), Failure> {
        let request = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("openduct"))
            .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")));
        let too_old = |what: &str| {
            Failure::Lasting(format!(
                "{what} does not speak the Kafka protocol of Kafka 2.4 or later, which the kafka connector needs"
            ))
        };
        let mut body = self.round_trip(&request, API_VERSIONS, ANSWER_WAIT).await?;
        let answer = ApiVersionsResponse::decode(&mut body, API_VERSIONS).map_err(|_| too_old(&self.what))?;
        if answer.error_code != 0 {
            return Err(too_old(&self.what));
        }
        for (key, version) in VERSIONS {
            let found = answer.api_keys.iter().find(|a| a.api_key == *key as i16);
            match found {
                Some(api) if (api.min_version..=api.max_version).contains(version) => {}
                Some(api) => {
                    return Err(Failure::Lasting(format!(
                        "{} does not answer {key:?} version {version} (it answers {} to {}): the kafka connector \
                         needs Kafka 2.4 or later",
                        self.what, api.min_version, api.max_version
                    )));
                }
                None => {
                    return Err(Failure::Lasting(format!(
                        "{} does not answer {key:?} requests, which the kafka connector needs",
                        self.what
                    )));
                }
            }
        }
        Ok(())
    }

    async fn sign_in(&mut self, mechanism: Mechanism, username: &str, password: &str) -> Result<(), Failure> {
        let request =
            SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(mechanism.name()));
        let answer = self.exchange(&request, SASL_HANDSHAKE, ANSWER_WAIT).await?;
        match answer.error_code {
            0 => {}
            UNSUPPORTED_SASL_MECHANISM => {
                let offered: Vec<String> = answer.mechanisms.iter().map(|m| m.to_ascii_lowercase()).collect();
                return Err(Failure::Lasting(format!(
                    "{} does not take sasl {} (it takes {})",
                    self.what,
                    mechanism.name().to_ascii_lowercase(),
                    if offered.is_empty() {
                        "none".to_owned()
                    } else {
                        offered.join(", ")
                    }
                )));
            }
            code => {
                return Err(Failure::Lasting(format!(
                    "{} refused to start signing in (Kafka error {code})",
                    self.what
                )));
            }
        }
        match mechanism {
            Mechanism::Plain => {
                let mut auth = vec![0];
                auth.extend_from_slice(username.as_bytes());
                auth.push(0);
                auth.extend_from_slice(password.as_bytes());
                self.authenticate(auth).await?;
            }
            Mechanism::Scram(hash) => {
                let nonce = Scram::nonce().map_err(Failure::Lasting)?;
                let mut scram = Scram::new(hash, username, password, nonce);
                let challenge = self.authenticate(scram.client_first()).await?;
                let proof = scram
                    .client_final(&challenge)
                    .map_err(|e| Failure::Lasting(format!("{} {e}", self.what)))?;
                let last = self.authenticate(proof).await?;
                scram
                    .verify(&last)
                    .map_err(|e| Failure::Lasting(format!("{} {e}", self.what)))?;
            }
        }
        Ok(())
    }

    async fn authenticate(&mut self, bytes: Vec<u8>) -> Result<Bytes, Failure> {
        let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(bytes));
        let answer = self.exchange(&request, SASL_AUTHENTICATE, ANSWER_WAIT).await?;
        match answer.error_code {
            0 => Ok(answer.auth_bytes),
            SASL_AUTHENTICATION_FAILED => Err(Failure::Lasting(format!(
                "{} refused the username or password",
                self.what
            ))),
            code => Err(Failure::Lasting(format!(
                "{} refused signing in (Kafka error {code})",
                self.what
            ))),
        }
    }

    /// Send `request` and read its answer.
    pub(super) async fn call<R: Request>(&mut self, request: &R) -> Result<R::Response, Failure> {
        self.call_waiting(request, ANSWER_WAIT).await
    }

    /// [`Connection::call`], for a request the broker may rightly hold for longer (a group join waits for the
    /// other members).
    pub(super) async fn call_waiting<R: Request>(
        &mut self,
        request: &R,
        wait: Duration,
    ) -> Result<R::Response, Failure> {
        let version = VERSIONS
            .iter()
            .find(|(key, _)| *key as i16 == R::KEY)
            .map(|(_, v)| *v)
            .ok_or_else(|| Failure::Lasting(format!("API {} is not one this client speaks", R::KEY)))?;
        self.exchange(request, version, wait).await
    }

    async fn exchange<R: Request>(
        &mut self,
        request: &R,
        version: i16,
        wait: Duration,
    ) -> Result<R::Response, Failure> {
        let mut body = self.round_trip(request, version, wait).await?;
        R::Response::decode(&mut body, version).map_err(|e| {
            Failure::Lasting(format!(
                "{} sent an answer to {:?} that is not understood: {e}",
                self.what,
                ApiKey::try_from(R::KEY).unwrap_or(ApiKey::ApiVersions)
            ))
        })
    }

    /// Send `request` and read its answer's body, still encoded.
    async fn round_trip<R: Request>(
        &mut self,
        request: &R,
        version: i16,
        wait: Duration,
    ) -> Result<Bytes, Failure> {
        let what = self.what.clone();
        if self.cut {
            return Err(Failure::Passing(format!("the connection to {what} was cut off")));
        }
        let key = ApiKey::try_from(R::KEY)
            .map_err(|()| Failure::Lasting(format!("API {} is not known", R::KEY)))?;
        let unwritable = |e: anyhow::Error| Failure::Lasting(format!("cannot write a {key:?} request: {e}"));
        self.correlation = self.correlation.wrapping_add(1);
        let correlation = self.correlation;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(correlation)
            .with_client_id(Some(self.client_id.clone()));
        let mut body = BytesMut::new();
        header
            .encode(&mut body, key.request_header_version(version))
            .map_err(unwritable)?;
        request.encode(&mut body, version).map_err(unwritable)?;
        let size = i32::try_from(body.len())
            .map_err(|_| Failure::Lasting("a request too large to send".to_owned()))?;
        self.cut = true;
        let stream = &mut self.stream;
        let exchange = async {
            stream.write_all(&size.to_be_bytes()).await?;
            stream.write_all(&body).await?;
            stream.flush().await?;
            let mut head = [0u8; 4];
            stream.read_exact(&mut head).await?;
            let length = usize::try_from(i32::from_be_bytes(head))
                .ok()
                .filter(|n| *n <= MAX_ANSWER);
            let Some(length) = length else {
                return Ok(Err(not_an_answer(head, &what)));
            };
            let mut frame = BytesMut::with_capacity(length.min(CHUNK));
            while frame.len() < length {
                frame.reserve((length - frame.len()).min(CHUNK));
                let left = u64::try_from(length - frame.len()).unwrap_or(0);
                if (&mut *stream).take(left).read_buf(&mut frame).await? == 0 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
            }
            Ok::<_, std::io::Error>(Ok(frame.freeze()))
        };
        let mut frame = match timeout(wait, exchange).await {
            Err(_) => {
                return Err(Failure::Passing(format!(
                    "{what} did not answer a {key:?} request within {wait:?}"
                )));
            }
            Ok(Err(e)) => return Err(Failure::Passing(format!("the connection to {what} broke: {e}"))),
            Ok(Ok(Err(failure))) => return Err(failure),
            Ok(Ok(Ok(frame))) => frame,
        };
        let not_understood = |e: anyhow::Error| {
            Failure::Lasting(format!(
                "{what} sent an answer to {key:?} that is not understood: {e}"
            ))
        };
        let answer_header = ResponseHeader::decode(&mut frame, key.response_header_version(version))
            .map_err(not_understood)?;
        if answer_header.correlation_id != correlation {
            return Err(Failure::Passing(format!("{what} answered another request")));
        }
        self.cut = false;
        Ok(frame)
    }
}

/// What it means when the first four bytes of an answer are not a size a broker would send.
fn not_an_answer(head: [u8; 4], what: &str) -> Failure {
    Failure::Lasting(match head {
        [0x15, 0x03, ..] | [0x16, 0x03, ..] => format!("{what} speaks TLS: set tls: true"),
        [b'H', b'T', b'T', b'P'] => format!("{what} is a web server, not a Kafka broker"),
        _ => format!(
            "{what} sent an answer of more than 64 MiB, or of a negative size: it is not a Kafka broker, or not one \
             that speaks to clients on this port"
        ),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    /// A stand-in broker that sends `answer` to whatever it is asked.
    async fn stand_in(answer: &'static [u8]) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut size = [0u8; 4];
                if socket.read_exact(&mut size).await.is_ok() {
                    let mut request = vec![0u8; usize::try_from(i32::from_be_bytes(size)).unwrap_or(0)];
                    let _ = socket.read_exact(&mut request).await;
                    let _ = socket.write_all(answer).await;
                }
            }
        });
        port
    }

    fn plain() -> Access {
        Access {
            tls: None,
            sasl: None,
            client_id: "test".to_owned(),
        }
    }

    #[tokio::test]
    async fn what_is_not_a_kafka_broker_is_named() {
        for (answer, why) in [
            (&b"\x15\x03\x03\x00\x02\x02\x46"[..], "speaks TLS"),
            (b"HTTP/1.1 400 Bad Request\r\n\r\n", "web server"),
            (b"\x7f\xff\xff\xff", "more than 64 MiB"),
            (b"\xff\xff\xff\xfe", "negative size"),
        ] {
            let port = stand_in(answer).await;
            let error = Connection::open("127.0.0.1", port, &plain()).await.unwrap_err();
            assert!(
                matches!(&error, Failure::Lasting(m) if m.contains(why)),
                "{error:?}"
            );
        }
        // An answer cut short is a broken connection, which passes.
        let port = stand_in(b"\x00\x00\x00\x10\x00\x00").await;
        assert!(matches!(
            Connection::open("127.0.0.1", port, &plain()).await.unwrap_err(),
            Failure::Passing(m) if m.contains("broke")
        ));
    }

    #[tokio::test]
    async fn a_broker_too_old_is_named() {
        // ApiVersions v3 answer: correlation 1, no error, Produce 0-7 only, no tags.
        let port = stand_in(
            b"\x00\x00\x00\x13\x00\x00\x00\x01\x00\x00\x02\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00",
        )
        .await;
        let error = Connection::open("127.0.0.1", port, &plain()).await.unwrap_err();
        assert!(
            matches!(&error, Failure::Lasting(m) if m.contains("Produce version 8 (it answers 0 to 7)")),
            "{error:?}"
        );
        // An answer whose list of APIs claims 4,294,967,294 entries is refused, not made room for.
        let port = stand_in(b"\x00\x00\x00\x0b\x00\x00\x00\x01\x00\x00\xff\xff\xff\xff\x0f").await;
        let error = Connection::open("127.0.0.1", port, &plain()).await.unwrap_err();
        assert!(
            matches!(&error, Failure::Lasting(m) if m.contains("does not speak the Kafka protocol")),
            "{error:?}"
        );
    }
}
