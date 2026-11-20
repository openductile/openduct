// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A small Kafka-protocol client: enough for the `openduct` command (health, topics, send, read) and for tests that
//! speak the protocol directly. Applications use any Kafka client.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, DeleteTopicsRequest,
    MetadataRequest, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, Request, StrBytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

mod group;
mod messages;
pub mod records;
mod routed;
mod secure;
mod shared;
pub use group::{GroupState, Membership, range_assignment};
pub use messages::{EARLIEST, Fetched, LATEST, Outgoing};
pub use routed::Routed;
pub use secure::{ClientSecurity, SignIn};
pub use shared::{Lease, Shared};

trait Connection: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection for T {}

/// The largest answer taken from a broker. The command asks for at most 8 MiB of messages at a time, so a larger
/// answer is from something that is not a broker, or one that cannot be trusted.
pub const MAX_ANSWER: usize = 64 * 1024 * 1024;
/// How much more memory an answer may take for each read.
const CHUNK: usize = 64 * 1024;

/// Why the first four bytes of an answer are not the size of one.
fn not_an_answer(head: [u8; 4]) -> ClientError {
    ClientError::Protocol(match head {
        [0x15 | 0x16, 0x03, ..] => "the broker speaks TLS on this port: connect with TLS".to_owned(),
        [b'H', b'T', b'T', b'P'] => "this address is a web server, not a broker".to_owned(),
        _ => "the answer claims more than 64 MiB, or a negative size: this is not a broker that speaks to clients \
              on this port"
            .to_owned(),
    })
}

/// Why a client call failed.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("cannot reach {addr}: {source}")]
    Connect {
        addr: String,
        #[source]
        source: std::io::Error,
    },
    #[error("network error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("the broker did not answer within {0:?}")]
    Timeout(Duration),
    #[error("{}", match message { Some(m) if !m.is_empty() => m.clone(), _ => format!("the broker refused (error code {code})") })]
    Refused { code: i16, message: Option<String> },
}

/// One connection to a broker.
pub struct Client {
    stream: Box<dyn Connection>,
    correlation: i32,
    client_id: String,
    timeout: Duration,
}

/// A topic as Metadata reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: usize,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connect to `addr` (`host:port`), in plaintext without signing in.
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Self, ClientError> {
        Self::connect_with(addr, timeout, &ClientSecurity::default()).await
    }

    /// Connect to `addr` with TLS and sign in, as `security` says.
    pub async fn connect_with(
        addr: &str,
        timeout: Duration,
        security: &ClientSecurity,
    ) -> Result<Self, ClientError> {
        let connect_err = |source| ClientError::Connect {
            addr: addr.to_owned(),
            source,
        };
        let tcp = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Timeout(timeout))?
            .map_err(connect_err)?;
        tcp.set_nodelay(true)?;
        let stream: Box<dyn Connection> = match &security.tls {
            None => Box::new(tcp),
            Some(tls) => {
                let host = security.server_name.clone().unwrap_or_else(|| {
                    addr.rsplit_once(':')
                        .map_or(addr, |(h, _)| h)
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_owned()
                });
                let name = rustls::pki_types::ServerName::try_from(host)
                    .map_err(|e| ClientError::Protocol(format!("no server name for {addr}: {e}")))?;
                let connector = tokio_rustls::TlsConnector::from(Arc::clone(tls));
                let secured = tokio::time::timeout(timeout, connector.connect(name, tcp))
                    .await
                    .map_err(|_| ClientError::Timeout(timeout))?
                    .map_err(connect_err)?;
                Box::new(secured)
            }
        };
        let mut client = Self {
            stream,
            correlation: 0,
            client_id: "openduct-cli".to_owned(),
            timeout,
        };
        if let Some(how) = &security.sign_in {
            client.sign_in(how).await?;
        }
        Ok(client)
    }

    /// Send one request at `version` and wait for its answer.
    pub async fn send<R: Request>(&mut self, request: &R, version: i16) -> Result<R::Response, ClientError> {
        let timeout = self.timeout;
        self.send_waiting(request, version, timeout).await
    }

    /// [`Client::send`], for a request the broker may rightly hold for longer than usual (a group join waits for
    /// the other members).
    pub async fn send_waiting<R: Request>(
        &mut self,
        request: &R,
        version: i16,
        timeout: Duration,
    ) -> Result<R::Response, ClientError> {
        let key = ApiKey::try_from(R::KEY)
            .map_err(|()| ClientError::Protocol(format!("unknown API {}", R::KEY)))?;
        self.correlation = self.correlation.wrapping_add(1);
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation)
            .with_client_id(Some(StrBytes::from_string(self.client_id.clone())));
        let mut body = BytesMut::new();
        header
            .encode(&mut body, key.request_header_version(version))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        request
            .encode(&mut body, version)
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let len = i32::try_from(body.len()).map_err(|_| ClientError::Protocol("request too large".into()))?;
        let correlation = self.correlation;
        let exchange = async {
            self.stream.write_all(&len.to_be_bytes()).await?;
            self.stream.write_all(&body).await?;
            let mut head = [0u8; 4];
            self.stream.read_exact(&mut head).await?;
            let size = usize::try_from(i32::from_be_bytes(head))
                .ok()
                .filter(|n| *n <= MAX_ANSWER)
                .ok_or_else(|| not_an_answer(head))?;
            // Memory grows only as bytes arrive: a size the other side claims is never reserved in advance.
            let mut frame = BytesMut::with_capacity(size.min(CHUNK));
            while frame.len() < size {
                frame.reserve((size - frame.len()).min(CHUNK));
                let left = u64::try_from(size - frame.len()).unwrap_or(0);
                if (&mut self.stream).take(left).read_buf(&mut frame).await? == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
            }
            Ok::<Bytes, ClientError>(frame.freeze())
        };
        let mut frame = tokio::time::timeout(timeout, exchange)
            .await
            .map_err(|_| ClientError::Timeout(timeout))??;
        let response_header = ResponseHeader::decode(&mut frame, key.response_header_version(version))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        if response_header.correlation_id != correlation {
            return Err(ClientError::Protocol(format!(
                "answer for request {} arrived for request {correlation}",
                response_header.correlation_id
            )));
        }
        R::Response::decode(&mut frame, version).map_err(|e| ClientError::Protocol(e.to_string()))
    }

    /// Ask the broker which APIs and versions it supports.
    pub async fn api_versions(&mut self) -> Result<ApiVersionsResponse, ClientError> {
        let response = self.send(&ApiVersionsRequest::default(), 3).await?;
        if response.error_code != 0 {
            return Err(ClientError::Refused {
                code: response.error_code,
                message: None,
            });
        }
        Ok(response)
    }

    /// Every topic and its partition count.
    pub async fn topics(&mut self) -> Result<Vec<TopicSummary>, ClientError> {
        let response = self
            .send(&MetadataRequest::default().with_topics(None), 12)
            .await?;
        let mut topics: Vec<TopicSummary> = response
            .topics
            .into_iter()
            .filter_map(|t| {
                t.name.map(|n| TopicSummary {
                    name: n.0.to_string(),
                    partitions: t.partitions.len(),
                })
            })
            .collect();
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(topics)
    }

    /// Create a topic; `retention_ms` of `None` keeps its data until deleted.
    pub async fn create_topic(
        &mut self,
        name: &str,
        partitions: i32,
        retention_ms: Option<i64>,
    ) -> Result<(), ClientError> {
        self.create_topic_with(name, partitions, retention_ms, &[]).await
    }

    /// As [`Client::create_topic`], with the topic's other settings by Kafka's names (`cleanup.policy`, ...).
    pub async fn create_topic_with(
        &mut self,
        name: &str,
        partitions: i32,
        retention_ms: Option<i64>,
        settings: &[(&str, &str)],
    ) -> Result<(), ClientError> {
        let mut topic = CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_string(name.to_owned())))
            .with_num_partitions(partitions)
            .with_replication_factor(-1);
        let retention = retention_ms.map(|ms| ms.to_string());
        topic.configs = retention
            .iter()
            .map(|ms| ("retention.ms", ms.as_str()))
            .chain(settings.iter().copied())
            .map(|(name, value)| {
                CreatableTopicConfig::default()
                    .with_name(StrBytes::from_string(name.to_owned()))
                    .with_value(Some(StrBytes::from_string(value.to_owned())))
            })
            .collect();
        let request = CreateTopicsRequest::default()
            .with_topics(vec![topic])
            .with_timeout_ms(30_000);
        let response = self.send(&request, 7).await?;
        match response.topics.first() {
            Some(t) if t.error_code == 0 => Ok(()),
            Some(t) => Err(ClientError::Refused {
                code: t.error_code,
                message: t.error_message.as_ref().map(|m| m.to_string()),
            }),
            None => Err(ClientError::Protocol("no answer for the topic".into())),
        }
    }

    /// Delete a topic and everything in it.
    pub async fn delete_topic(&mut self, name: &str) -> Result<(), ClientError> {
        let request = DeleteTopicsRequest::default()
            .with_topic_names(vec![TopicName(StrBytes::from_string(name.to_owned()))])
            .with_timeout_ms(30_000);
        let response = self.send(&request, 5).await?;
        match response.responses.first() {
            Some(t) if t.error_code == 0 => Ok(()),
            Some(t) => Err(ClientError::Refused {
                code: t.error_code,
                message: t.error_message.as_ref().map(|m| m.to_string()),
            }),
            None => Err(ClientError::Protocol("no answer for the topic".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A stand-in that reads one request and sends `answer` back, then holds the connection open.
    async fn stand_in(answer: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut size = [0u8; 4];
            socket.read_exact(&mut size).await.unwrap();
            let mut request = vec![0u8; usize::try_from(i32::from_be_bytes(size)).unwrap()];
            socket.read_exact(&mut request).await.unwrap();
            socket.write_all(answer).await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        addr
    }

    async fn ask(answer: &'static [u8]) -> ClientError {
        let addr = stand_in(answer).await;
        let mut client = Client::connect(&addr, Duration::from_secs(5)).await.unwrap();
        client.api_versions().await.unwrap_err()
    }

    #[tokio::test]
    async fn an_answer_claiming_gigabytes_is_refused_before_any_memory_is_taken() {
        let error = ask(&[0x7f, 0xff, 0xff, 0xff]).await;
        assert!(error.to_string().contains("more than 64 MiB"), "{error}");
    }

    #[tokio::test]
    async fn a_negative_size_is_refused() {
        let error = ask(&[0xff, 0xff, 0xff, 0xf0]).await;
        assert!(error.to_string().contains("negative size"), "{error}");
    }

    #[tokio::test]
    async fn a_web_server_is_named() {
        let error = ask(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
        assert!(error.to_string().contains("web server"), "{error}");
    }

    #[tokio::test]
    async fn a_tls_port_is_named() {
        let error = ask(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x50]).await;
        assert!(error.to_string().contains("speaks TLS"), "{error}");
    }

    #[tokio::test]
    async fn an_answer_cut_short_is_an_error_not_a_hang() {
        // Claims 100 bytes, sends 3, and the stand-in's connection then stays open: the timeout ends the wait.
        let addr = stand_in(&[0, 0, 0, 100, 1, 2, 3]).await;
        let mut client = Client::connect(&addr, Duration::from_millis(300)).await.unwrap();
        let error = client.api_versions().await.unwrap_err();
        assert!(matches!(error, ClientError::Timeout(_)), "{error}");
    }
}
