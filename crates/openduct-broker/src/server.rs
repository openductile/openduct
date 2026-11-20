// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The network side: accept connections, read Kafka frames, hand each request to its handler, write the answer.
//!
//! A frame is `[size: i32][request]`. The size is checked against `max_request_bytes` before a byte of the request
//! is read, so a client cannot make the broker allocate more than that. Requests on one connection are answered in
//! the order they arrived, as the Kafka protocol requires. Anything the broker cannot decode closes that one
//! connection; nothing a client sends can stop the broker.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, ApiKey, ApiVersionsRequest, CreateTopicsRequest, DeleteGroupsRequest,
    DeleteTopicsRequest, DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest, FetchRequest,
    FindCoordinatorRequest, HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest, LeaveGroupRequest,
    ListGroupsRequest, ListOffsetsRequest, ListPartitionReassignmentsRequest, MetadataRequest,
    OffsetCommitRequest, OffsetDeleteRequest, OffsetFetchRequest, OffsetForLeaderEpochRequest,
    ProduceRequest, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslHandshakeRequest,
    SyncGroupRequest, UnregisterBrokerRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use openduct_storage::Store;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::config::BrokerConfig;
use crate::groups::Coordinator;
use crate::handlers::{self, State};
use crate::security::quota::Kind;
use crate::security::{Authorize, ClientCerts, Operation, Principal, Session};

/// Why the broker could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("network error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid configuration: {0}")]
    Config(String),
}

/// A running broker. Dropping it does not stop it; call [`Broker::shutdown`].
#[derive(Debug)]
pub struct Broker {
    local_addr: SocketAddr,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    state: Arc<State>,
}

impl Broker {
    /// Start a broker on `store`. Returns once it is listening.
    pub async fn start(config: BrokerConfig, store: Arc<dyn Store>) -> Result<Self, BrokerError> {
        validate(&config)?;
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|source| BrokerError::Bind {
                addr: config.listen,
                source,
            })?;
        let local_addr = listener.local_addr()?;
        let mut config = config;
        if config.security.rules.is_some() && config.auto_create_topics {
            // With access rules, a topic exists only once someone allowed to create it has.
            tracing::info!("access rules are on: topics are not created implicitly");
            config.auto_create_topics = false;
        }
        // Listening on port 0 picks a free port: advertise the one actually taken.
        if config.advertised_port == 0 {
            config.advertised_port = i32::from(local_addr.port());
        }
        let (stop, stopped) = watch::channel(false);
        let state = Arc::new(State {
            groups: Coordinator::new(
                config.group_initial_rebalance_delay,
                config.group_min_session_timeout,
                config.group_max_session_timeout,
            ),
            store,
            appended: watch::channel(0u64).0,
            stopping: stopped.clone(),
            request_memory: Arc::new(Semaphore::new(
                config.max_request_memory.min(Semaphore::MAX_PERMITS),
            )),
            metrics: crate::metrics::Metrics::new(),
            transactions: handlers::transactions::Transactions::default(),
            config,
        });
        let task = tokio::spawn(run(listener, Arc::clone(&state), stopped));
        tracing::info!(%local_addr, "OpenDuct is listening");
        Ok(Self {
            local_addr,
            stop,
            task,
            state,
        })
    }

    /// What the broker measures, for an endpoint that serves it (`openduct serve --metrics-listen`).
    pub fn metrics(&self) -> crate::metrics::BrokerMetrics {
        crate::metrics::BrokerMetrics {
            state: Arc::clone(&self.state),
        }
    }

    /// The address the broker listens on (with the real port when started on port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop accepting connections, let requests in flight finish, and wait for the broker to stop.
    pub async fn shutdown(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}

fn validate(config: &BrokerConfig) -> Result<(), BrokerError> {
    let refuse = |why: String| Err(BrokerError::Config(why));
    if config.max_request_bytes < 8 {
        return refuse("max_request_bytes must be at least 8".into());
    }
    if config.max_request_bytes > config.max_request_memory {
        return refuse(format!(
            "max_request_bytes ({}) is over max_request_memory ({}): such a request could never be read",
            config.max_request_bytes, config.max_request_memory
        ));
    }
    if u32::try_from(config.max_request_bytes).is_err() {
        return refuse("max_request_bytes must be under 4 GiB".into());
    }
    if config.max_connections == 0 || config.max_fetch_bytes == 0 {
        return refuse("max_connections and max_fetch_bytes must be above 0".into());
    }
    if config.idle_timeout.is_zero() || config.request_read_timeout.is_zero() {
        return refuse("idle_timeout and request_read_timeout must be above 0".into());
    }
    if config.group_min_session_timeout > config.group_max_session_timeout {
        return refuse("group_min_session_timeout is over group_max_session_timeout".into());
    }
    if let Err(why) = config.security.check(config.listen) {
        return refuse(why);
    }
    if config.security.allow_insecure && !config.listen.ip().is_loopback() {
        tracing::warn!(listen = %config.listen, "running INSECURELY on purpose: no TLS, sign-in or access rules required");
    }
    Ok(())
}

async fn run(listener: TcpListener, state: Arc<State>, mut stopped: watch::Receiver<bool>) {
    let permits = Arc::new(Semaphore::new(state.config.max_connections));
    let mut background = tokio::task::JoinSet::new();
    background.spawn(coordinator_clock(Arc::clone(&state), stopped.clone()));
    background.spawn(retention(Arc::clone(&state), stopped.clone()));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = stopped.changed() => break,
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        continue;
                    }
                };
                let security = &state.config.security;
                if security.kept_out(peer.ip())
                    || !security.connections.open(peer.ip(), security.max_connections_per_address)
                {
                    // Failed to sign in too often lately, or holding its share of connections: not even a TLS
                    // handshake.
                    state.metrics.connection_refused();
                    drop(socket);
                    continue;
                }
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else { break };
                let state = Arc::clone(&state);
                let stopped = stopped.clone();
                state.metrics.connection_accepted();
                connections.spawn(async move {
                    if let Err(e) = accept_connection(socket, peer, &state, stopped).await {
                        tracing::debug!(%peer, error = %e, "connection closed");
                    }
                    state.config.security.connections.close(peer.ip());
                    state.metrics.connection_closed();
                    drop(permit);
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    tracing::info!("OpenDuct is stopping: letting requests in flight finish");
    while connections.join_next().await.is_some() {}
    background.shutdown().await;
}

/// Kafka's answers to a principal the access rules do not let use a transactional id, or a group.
const TRANSACTIONAL_ID_DENIED: i16 = 53;
const CLUSTER_DENIED: i16 = 31;
const GROUP_DENIED: i16 = 30;

async fn coordinator_clock(state: Arc<State>, mut stopped: watch::Receiver<bool>) {
    let mut every = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut ticks = 0u32;
    // Transactions no coordinator will end are looked for a few times within the time one may stay open.
    let hanging_every = u32::try_from(state.config.hanging_transaction_after.as_millis() / 400)
        .unwrap_or(u32::MAX)
        .clamp(10, 300);
    loop {
        tokio::select! {
            _ = stopped.changed() => return,
            _ = every.tick() => {
                state.groups.tick();
                // Once a second: transactions left open past their timeout.
                ticks = ticks.wrapping_add(1);
                if ticks.is_multiple_of(10) {
                    handlers::transactions::expire(&state).await;
                }
                if ticks.is_multiple_of(hanging_every) {
                    handlers::transactions::abort_hanging(&state).await;
                }
            }
        }
    }
}

async fn retention(state: Arc<State>, mut stopped: watch::Receiver<bool>) {
    let mut every = tokio::time::interval(state.config.retention_check_interval);
    loop {
        tokio::select! {
            _ = stopped.changed() => return,
            _ = every.tick() => {
                let store = Arc::clone(&state.store);
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                match tokio::task::spawn_blocking(move || store.enforce_retention(now)).await {
                    Ok(Ok(0)) => {}
                    Ok(Ok(removed)) => tracing::info!(removed, "retention removed old batches"),
                    Ok(Err(e)) => tracing::error!(error = %e, "retention failed"),
                    Err(e) => tracing::error!(error = %e, "retention task failed"),
                }
                // Then compaction, on the topics that ask for it.
                let store = Arc::clone(&state.store);
                match tokio::task::spawn_blocking(move || store.compact(now)).await {
                    Ok(Ok(0)) => {}
                    Ok(Ok(removed)) => tracing::info!(removed, "compaction removed replaced records"),
                    Ok(Err(e)) => tracing::error!(error = %e, "compaction failed"),
                    Err(e) => tracing::error!(error = %e, "compaction task failed"),
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ConnectionError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("a request of {0} bytes is over the limit")]
    TooLarge(i64),
    #[error("a malformed request: {0}")]
    Malformed(String),
    #[error("an unsupported request: API {key} version {version}")]
    Unsupported { key: i16, version: i16 },
    #[error("closed a slow client: {0}")]
    Slow(&'static str),
    #[error("refused: {0}")]
    Refused(&'static str),
}

/// How long a client has to finish its TLS handshake.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// A new connection: TLS first when the port has it (a verified client certificate names the principal), then the
/// Kafka conversation.
async fn accept_connection(
    socket: TcpStream,
    peer: SocketAddr,
    state: &State,
    stopped: watch::Receiver<bool>,
) -> Result<(), ConnectionError> {
    socket.set_nodelay(true)?;
    let security = &state.config.security;
    let Some(tls) = &security.tls else {
        let session = Session::new(peer.ip(), None, security);
        let (reader, writer) = socket.into_split();
        return serve_connection(reader, writer, session, state, stopped).await;
    };
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.acceptor.accept(socket))
        .await
        .map_err(|_| ConnectionError::Slow("no TLS handshake in time"))??;
    let offered = stream.get_ref().1.peer_certificates().and_then(|c| c.first());
    let name = offered.and_then(crate::security::certificate_name);
    if tls.client_certs != ClientCerts::None && offered.is_some() && name.is_none() {
        return Err(ConnectionError::Refused(
            "a client certificate without a URI or DNS name",
        ));
    }
    let session = Session::new(peer.ip(), name, security);
    let (reader, writer) = tokio::io::split(stream);
    serve_connection(reader, writer, session, state, stopped).await
}

/// The most requests of one connection in flight at once. A produce request is written in order and answered once
/// its batches are on the device; meanwhile the next requests are read and written (clients send several at a
/// time), so their flushes are shared. Answers keep the order the requests came in.
const MAX_IN_FLIGHT: usize = 16;

/// A request's answer, still to come (a produce request waiting for its flush) or already there.
type Pending<'a> = Pin<Box<dyn Future<Output = Result<Option<BytesMut>, ConnectionError>> + Send + 'a>>;

async fn serve_connection<R, W>(
    mut reader: R,
    mut writer: W,
    mut session: Session,
    state: &State,
    mut stopped: watch::Receiver<bool>,
) -> Result<(), ConnectionError>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let (queue, mut answers) =
        tokio::sync::mpsc::channel::<(Pending<'_>, OwnedSemaphorePermit)>(MAX_IN_FLIGHT);
    let reading = async move {
        loop {
            let mut size = [0u8; 4];
            tokio::select! {
                _ = stopped.changed() => return Ok(()),
                read = tokio::time::timeout(state.config.idle_timeout, reader.read_exact(&mut size)) => {
                    match read {
                        Err(_) => return Ok(()), // idle too long
                        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                        Ok(Err(e)) => return Err(e.into()),
                        Ok(Ok(_)) => {}
                    }
                }
            }
            let size = i64::from(i32::from_be_bytes(size));
            let len = usize::try_from(size).map_err(|_| ConnectionError::TooLarge(size))?;
            if len < 8 || len > state.config.max_request_bytes {
                return Err(ConnectionError::TooLarge(size));
            }
            let (frame, memory) = tokio::select! {
                _ = stopped.changed() => return Ok(()),
                read = read_request(&mut reader, len, state) => read?,
            };
            let pending = start(state, frame, &mut session).await?;
            // The request's memory is held until its answer is written; a full queue waits for answers to go out.
            if queue.send((pending, memory)).await.is_err() {
                return Ok(());
            }
            if session.close {
                // A failed sign-in: its answer goes out, then the connection closes.
                return Ok(());
            }
        }
    };
    let writing = async {
        while let Some((pending, memory)) = answers.recv().await {
            if let Some(reply) = pending.await? {
                let length = i32::try_from(reply.len())
                    .map_err(|_| ConnectionError::Malformed("reply too large".into()))?;
                // A client that stops reading its answers is closed, rather than holding the answer forever.
                let write = async {
                    writer.write_all(&length.to_be_bytes()).await?;
                    writer.write_all(&reply).await
                };
                tokio::time::timeout(state.config.idle_timeout, write)
                    .await
                    .map_err(|_| ConnectionError::Slow("not reading its answers"))??;
            }
            drop(memory);
        }
        Ok::<(), ConnectionError>(())
    };
    // Reading ends (the client is done, or the broker stops): the answers already queued are still written.
    tokio::try_join!(reading, writing).map(|_| ())
}

/// Start a request: a produce request is written now and answered once it is on the device; any other is answered
/// at once. Nothing but ApiVersions and the sign-in is served before the connection has signed in, and every request
/// is held to the access rules.
/// Starts a request (see [`begin`]), measuring it: its API, whether it was answered, and how long that took.
async fn start<'a>(
    state: &'a State,
    frame: Bytes,
    session: &mut Session,
) -> Result<Pending<'a>, ConnectionError> {
    let key = frame
        .get(0..2)
        .and_then(|b| <[u8; 2]>::try_from(b).ok())
        .map_or(-1, i16::from_be_bytes);
    let begun = std::time::Instant::now();
    match begin(state, frame, session, key).await {
        Ok(pending) => Ok(Box::pin(async move {
            let answer = pending.await;
            state.metrics.request(key, begun.elapsed(), answer.is_ok());
            answer
        })),
        Err(e) => {
            state.metrics.request(key, begun.elapsed(), false);
            Err(e)
        }
    }
}

async fn begin<'a>(
    state: &'a State,
    frame: Bytes,
    session: &mut Session,
    key: i16,
) -> Result<Pending<'a>, ConnectionError> {
    if key == ApiKey::SaslHandshake as i16 || key == ApiKey::SaslAuthenticate as i16 {
        let answer = sign_in(state, frame, session);
        return Ok(Box::pin(async move { answer }));
    }
    let principal = if key == ApiKey::ApiVersions as i16 {
        Principal::anonymous()
    } else {
        session.principal().map_err(ConnectionError::Refused)?.clone()
    };
    let authz = Authorize {
        rules: state.config.security.rules.as_deref(),
        principal: &principal,
        host: session.peer,
    };
    if key != ApiKey::Produce as i16 {
        let answer = handle(state, frame, &authz).await;
        return Ok(Box::pin(async move { answer }));
    }
    let mut frame = frame;
    let (key, version, correlation_id) = match header(&mut frame)? {
        Parsed::Request {
            key,
            version,
            correlation_id,
            ..
        } => (key, version, correlation_id),
        Parsed::Answer(answer) => return Ok(Box::pin(async move { Ok(answer) })),
    };
    let mut req: ProduceRequest = decode(&mut frame, version)?;
    let denied = authz.produce(&mut req);
    let bytes = produced_bytes(&req);
    state.metrics.produced(bytes);
    let held = throttle(state, &principal, Kind::Produce, bytes);
    let started = handlers::produce_begin(state, &req, version).await;
    Ok(Box::pin(async move {
        match handlers::produce_finish(state, started).await {
            Some(mut res) => {
                Authorize::produced(&mut res, denied);
                // Held back for a principal over its share: this connection's answers wait. The hold is the
                // throttle; throttle_time_ms stays 0, since some clients resend a request answered with one
                // (a produce written twice, charged twice).
                tokio::time::sleep(held).await;
                encode_reply(key, version, correlation_id, &res)
            }
            None => Ok(None),
        }
    }))
}

/// How long a principal's answer is held back for its quota (none without rules, or without a quota).
fn throttle(state: &State, principal: &Principal, kind: Kind, bytes: u64) -> std::time::Duration {
    let Some(rules) = &state.config.security.rules else {
        return std::time::Duration::ZERO;
    };
    let quota = rules.quota(principal);
    let rate = match kind {
        Kind::Produce => quota.produce_bytes_per_sec,
        Kind::Fetch => quota.fetch_bytes_per_sec,
    };
    rate.map_or(std::time::Duration::ZERO, |r| {
        state.config.security.meter.charge(principal, kind, bytes, r)
    })
}

fn produced_bytes(req: &ProduceRequest) -> u64 {
    req.topic_data
        .iter()
        .flat_map(|t| &t.partition_data)
        .map(|p| p.records.as_ref().map_or(0, |r| r.len() as u64))
        .sum()
}

fn fetched_bytes(res: &kafka_protocol::messages::FetchResponse) -> u64 {
    res.responses
        .iter()
        .flat_map(|t| &t.partitions)
        .map(|p| p.records.as_ref().map_or(0, |r| r.len() as u64))
        .sum()
}

/// SaslHandshake and SaslAuthenticate: the connection's sign-in.
fn sign_in(
    state: &State,
    mut frame: Bytes,
    session: &mut Session,
) -> Result<Option<BytesMut>, ConnectionError> {
    let (key, version, correlation_id) = match header(&mut frame)? {
        Parsed::Request {
            key,
            version,
            correlation_id,
            ..
        } => (key, version, correlation_id),
        Parsed::Answer(answer) => return Ok(answer),
    };
    let security = &state.config.security;
    if key == ApiKey::SaslHandshake {
        let req: SaslHandshakeRequest = decode(&mut frame, version)?;
        let res = session.handshake(security, req.mechanism.as_str());
        return encode_reply(key, version, correlation_id, &res);
    }
    let req: SaslAuthenticateRequest = decode(&mut frame, version)?;
    let res = session.authenticate(security, &req.auth_bytes);
    encode_reply(key, version, correlation_id, &res)
}

/// Read a request body of `len` bytes. Its size is reserved from the broker-wide request budget first (whole, so
/// requests can never deadlock each other holding parts of it); memory is then filled only as bytes arrive, and
/// the body must arrive within `request_read_timeout`. The reservation is held until the answer is written.
async fn read_request(
    socket: &mut (impl AsyncRead + Unpin),
    len: usize,
    state: &State,
) -> Result<(Bytes, OwnedSemaphorePermit), ConnectionError> {
    const CHUNK: usize = 64 * 1024;
    let permits = u32::try_from(len).map_err(|_| ConnectionError::TooLarge(i64::MAX))?;
    let memory = Arc::clone(&state.request_memory)
        .acquire_many_owned(permits)
        .await
        .map_err(|_| ConnectionError::Slow("the broker is stopping"))?;
    let read = async {
        let mut frame = BytesMut::with_capacity(len.min(CHUNK));
        let mut body = (&mut *socket).take(u64::try_from(len).unwrap_or(u64::MAX));
        while frame.len() < len {
            frame.reserve((len - frame.len()).min(CHUNK));
            if body.read_buf(&mut frame).await? == 0 {
                return Err(ConnectionError::Io(std::io::ErrorKind::UnexpectedEof.into()));
            }
        }
        Ok(frame.freeze())
    };
    let frame = tokio::time::timeout(state.config.request_read_timeout, read)
        .await
        .map_err(|_| ConnectionError::Slow("did not send its request in time"))??;
    Ok((frame, memory))
}

fn encode_reply<R: Encodable>(
    key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: &R,
) -> Result<Option<BytesMut>, ConnectionError> {
    let mut out = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut out, key.response_header_version(version))
        .map_err(|e| ConnectionError::Malformed(e.to_string()))?;
    response
        .encode(&mut out, version)
        .map_err(|e| ConnectionError::Malformed(format!("cannot encode the answer: {e}")))?;
    Ok(Some(out))
}

fn decode<T: Decodable>(body: &mut Bytes, version: i16) -> Result<T, ConnectionError> {
    T::decode(body, version).map_err(|e| ConnectionError::Malformed(e.to_string()))
}

/// A request's header, read off the front of its frame - or, for an ApiVersions in a version OpenDuct does not speak,
/// the answer already (in v0, which every client reads, so it can retry with one OpenDuct speaks).
enum Parsed {
    Request {
        key: ApiKey,
        version: i16,
        correlation_id: i32,
        client_id: String,
    },
    Answer(Option<BytesMut>),
}

fn header(frame: &mut Bytes) -> Result<Parsed, ConnectionError> {
    let key_raw = frame
        .get(0..2)
        .and_then(|b| <[u8; 2]>::try_from(b).ok())
        .map(i16::from_be_bytes)
        .unwrap_or(-1);
    let version = frame
        .get(2..4)
        .and_then(|b| <[u8; 2]>::try_from(b).ok())
        .map(i16::from_be_bytes)
        .unwrap_or(-1);
    let correlation_id = frame
        .get(4..8)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(i32::from_be_bytes)
        .unwrap_or(0);
    let key = ApiKey::try_from(key_raw).map_err(|()| ConnectionError::Unsupported {
        key: key_raw,
        version,
    })?;
    if !handlers::supports(key, version) {
        if key == ApiKey::ApiVersions {
            return encode_reply(key, 0, correlation_id, &handlers::api_versions_unsupported())
                .map(Parsed::Answer);
        }
        return Err(ConnectionError::Unsupported {
            key: key_raw,
            version,
        });
    }
    let header = RequestHeader::decode(frame, key.request_header_version(version))
        .map_err(|e| ConnectionError::Malformed(e.to_string()))?;
    let client_id = header
        .client_id
        .as_ref()
        .map(|c| c.to_string())
        .unwrap_or_default();
    Ok(Parsed::Request {
        key,
        version,
        correlation_id,
        client_id,
    })
}

/// The hostile-input tests' door (see `crate::hostile`): a frame's header read as [`handle`] reads it, checked against
/// the APIs and versions served; the request's API, version and body when they are served.
pub(crate) fn read_header(frame: &[u8]) -> Option<(ApiKey, i16, Bytes)> {
    let mut frame = Bytes::copy_from_slice(frame);
    match header(&mut frame) {
        Ok(Parsed::Request { key, version, .. }) => Some((key, version, frame)),
        _ => None,
    }
}

/// Decode one request, run it, and encode the answer (`None` when none is sent: acks=0).
async fn handle(
    state: &State,
    mut frame: Bytes,
    authz: &Authorize<'_>,
) -> Result<Option<BytesMut>, ConnectionError> {
    let (key, version, correlation_id, client_id) = match header(&mut frame)? {
        Parsed::Request {
            key,
            version,
            correlation_id,
            client_id,
        } => (key, version, correlation_id, client_id),
        Parsed::Answer(answer) => return Ok(answer),
    };
    let body = &mut frame;
    match key {
        ApiKey::ApiVersions => {
            let req: ApiVersionsRequest = decode(body, version)?;
            encode_reply(
                key,
                version,
                correlation_id,
                &handlers::api_versions(&req, version, state.store.transactions_ready()),
            )
        }
        ApiKey::Metadata => {
            let mut req: MetadataRequest = decode(body, version)?;
            let listed_all = req.topics.is_none();
            let denied = authz.metadata(&mut req);
            let mut res = handlers::metadata(state, &req, version).await;
            authz.described(&mut res, listed_all, denied);
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::CreateTopics => {
            let mut req: CreateTopicsRequest = decode(body, version)?;
            let denied = authz.create_topics(&mut req);
            let mut res = handlers::create_topics(state, &req, version).await;
            Authorize::created(&mut res, denied);
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::DeleteTopics => {
            let mut req: DeleteTopicsRequest = decode(body, version)?;
            let known = handlers::topic_list(state).await;
            let denied = authz.delete_topics(&mut req, |id| {
                known.iter().find(|t| t.id == *id).map(|t| t.name.clone())
            });
            let mut res = handlers::delete_topics(state, &req, version).await;
            Authorize::deleted(&mut res, denied);
            encode_reply(key, version, correlation_id, &res)
        }
        // Alter on each topic.
        ApiKey::CreatePartitions => {
            let req: kafka_protocol::messages::CreatePartitionsRequest = decode(body, version)?;
            let res = handlers::create_partitions(state, &req, |t| {
                authz.may(crate::security::Resource::Topic, t, Operation::Alter)
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::AlterPartitionReassignments => {
            let req: AlterPartitionReassignmentsRequest = decode(body, version)?;
            let res = if authz.cluster(Operation::Alter) {
                handlers::alter_partition_reassignments(state, &req).await
            } else {
                handlers::reassignments_refused(crate::security::CLUSTER_AUTHORIZATION_FAILED)
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::ListPartitionReassignments => {
            let req: ListPartitionReassignmentsRequest = decode(body, version)?;
            let res = if authz.cluster(Operation::Describe) {
                handlers::list_partition_reassignments(state, &req)
            } else {
                handlers::reassignment_list_refused(crate::security::CLUSTER_AUTHORIZATION_FAILED)
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::UnregisterBroker => {
            let req: UnregisterBrokerRequest = decode(body, version)?;
            let res = if authz.cluster(Operation::Alter) {
                handlers::unregister_broker(state, &req).await
            } else {
                handlers::unregister_refused(crate::security::CLUSTER_AUTHORIZATION_FAILED)
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::Produce => {
            let mut req: ProduceRequest = decode(body, version)?;
            let denied = authz.produce(&mut req);
            let started = handlers::produce_begin(state, &req, version).await;
            match handlers::produce_finish(state, started).await {
                Some(mut res) => {
                    Authorize::produced(&mut res, denied);
                    encode_reply(key, version, correlation_id, &res)
                }
                None => Ok(None),
            }
        }
        ApiKey::Fetch => {
            let mut req: FetchRequest = decode(body, version)?;
            let denied = authz.fetch(&mut req);
            let mut res = handlers::fetch(state, &req, version).await;
            Authorize::fetched(&mut res, denied);
            let bytes = fetched_bytes(&res);
            state.metrics.fetched(bytes);
            let held = throttle(state, authz.principal, Kind::Fetch, bytes);
            tokio::time::sleep(held).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::ListOffsets => {
            let mut req: ListOffsetsRequest = decode(body, version)?;
            let denied = authz.list_offsets(&mut req);
            let mut res = handlers::list_offsets(state, &req, version).await;
            Authorize::listed(&mut res, denied);
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::DescribeCluster => {
            let req: DescribeClusterRequest = decode(body, version)?;
            let res = handlers::describe_cluster(state, &req, version, |op| authz.cluster(op));
            encode_reply(key, version, correlation_id, &res)
        }
        // A topic's settings: DescribeConfigs on the topic; a broker's: DescribeConfigs on the cluster.
        ApiKey::DescribeConfigs => {
            let req: DescribeConfigsRequest = decode(body, version)?;
            let res = handlers::describe_configs(state, &req, version, |topic| match topic {
                Some(t) => authz.may(crate::security::Resource::Topic, t, Operation::DescribeConfigs),
                None => authz.cluster(Operation::DescribeConfigs),
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        // A topic's settings: AlterConfigs on the topic; a broker's: AlterConfigs on the cluster.
        ApiKey::IncrementalAlterConfigs => {
            let req: kafka_protocol::messages::IncrementalAlterConfigsRequest = decode(body, version)?;
            let res = handlers::incremental_alter_configs(state, &req, |topic| match topic {
                Some(t) => authz.may(crate::security::Resource::Topic, t, Operation::AlterConfigs),
                None => authz.cluster(Operation::AlterConfigs),
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        // The older whole-replacement form of the same: AlterConfigs on the topic, or on the cluster.
        ApiKey::AlterConfigs => {
            let req: kafka_protocol::messages::AlterConfigsRequest = decode(body, version)?;
            let res = handlers::alter_configs(state, &req, |topic| match topic {
                Some(t) => authz.may(crate::security::Resource::Topic, t, Operation::AlterConfigs),
                None => authz.cluster(Operation::AlterConfigs),
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        // Delete on the topic.
        ApiKey::DeleteRecords => {
            let req: kafka_protocol::messages::DeleteRecordsRequest = decode(body, version)?;
            let res = handlers::delete_records(state, &req, |t| {
                authz.may(crate::security::Resource::Topic, t, Operation::Delete)
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::OffsetForLeaderEpoch => {
            let mut req: OffsetForLeaderEpochRequest = decode(body, version)?;
            let denied = authz.epochs(&mut req);
            let mut res = handlers::offset_for_leader_epoch(state, &req).await;
            Authorize::epochs_found(&mut res, denied);
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::InitProducerId => {
            let req: InitProducerIdRequest = decode(body, version)?;
            let res = match authz.init_producer(req.transactional_id.as_ref().map(|t| t.0.as_str())) {
                Ok(()) => handlers::init_producer_id(state, &req).await,
                Err(code) => handlers::init_producer_refused(code),
            };
            encode_reply(key, version, correlation_id, &res)
        }
        // A transaction's requests: Write on the transactional id.
        ApiKey::AddPartitionsToTxn => {
            let req: kafka_protocol::messages::AddPartitionsToTxnRequest = decode(body, version)?;
            // A producer adds to its own transaction; only a broker asks about another's.
            let denied = |id: &str, verify_only: bool| {
                if verify_only {
                    (!authz.cluster(Operation::ClusterAction)).then_some(CLUSTER_DENIED)
                } else {
                    (!authz.may(crate::security::Resource::TransactionalId, id, Operation::Write))
                        .then_some(TRANSACTIONAL_ID_DENIED)
                }
            };
            let res = handlers::transactions::add_partitions(state, &req, version, &denied).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::AddOffsetsToTxn => {
            let req: kafka_protocol::messages::AddOffsetsToTxnRequest = decode(body, version)?;
            let denied = (!authz.may(
                crate::security::Resource::TransactionalId,
                req.transactional_id.0.as_str(),
                Operation::Write,
            ))
            .then_some(TRANSACTIONAL_ID_DENIED);
            let res = handlers::transactions::add_offsets(state, &req, denied).await;
            encode_reply(key, version, correlation_id, &res)
        }
        // And Read on the group whose offsets it commits.
        ApiKey::TxnOffsetCommit => {
            let req: kafka_protocol::messages::TxnOffsetCommitRequest = decode(body, version)?;
            let denied = if !authz.may(
                crate::security::Resource::TransactionalId,
                req.transactional_id.0.as_str(),
                Operation::Write,
            ) {
                Some(TRANSACTIONAL_ID_DENIED)
            } else if !authz.may(
                crate::security::Resource::Group,
                req.group_id.0.as_str(),
                Operation::Read,
            ) {
                Some(GROUP_DENIED)
            } else {
                None
            };
            let res = handlers::transactions::offset_commit(state, &req, denied).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::EndTxn => {
            let req: kafka_protocol::messages::EndTxnRequest = decode(body, version)?;
            let denied = (!authz.may(
                crate::security::Resource::TransactionalId,
                req.transactional_id.0.as_str(),
                Operation::Write,
            ))
            .then_some(TRANSACTIONAL_ID_DENIED);
            let res = handlers::transactions::end(state, &req, version, denied).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::WriteTxnMarkers => {
            let req: kafka_protocol::messages::WriteTxnMarkersRequest = decode(body, version)?;
            let allowed = authz.cluster(Operation::ClusterAction) || authz.cluster(Operation::Alter);
            let res = handlers::transactions::write_markers(state, &req, allowed).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::ListTransactions => {
            let req: kafka_protocol::messages::ListTransactionsRequest = decode(body, version)?;
            let may = |id: &str| {
                authz.may(
                    crate::security::Resource::TransactionalId,
                    id,
                    Operation::Describe,
                )
            };
            let res = handlers::transactions::list(state, &req, &may).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::DescribeTransactions => {
            let req: kafka_protocol::messages::DescribeTransactionsRequest = decode(body, version)?;
            let may = |id: &str| {
                authz.may(
                    crate::security::Resource::TransactionalId,
                    id,
                    Operation::Describe,
                )
            };
            let res = handlers::transactions::describe(state, &req, &may).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::DescribeProducers => {
            let req: kafka_protocol::messages::DescribeProducersRequest = decode(body, version)?;
            let may = |topic: &str| authz.may(crate::security::Resource::Topic, topic, Operation::Read);
            let res = handlers::transactions::describe_producers(state, &req, &may).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::FindCoordinator => {
            let req: FindCoordinatorRequest = decode(body, version)?;
            // A transactional id's coordinator leads the coordinator's topic: there must be one to lead.
            if req.key_type == 1 {
                let _ = handlers::transactions::ensure_topic(state).await;
            }
            let mut res = handlers::find_coordinator(state, &req, version);
            authz.coordinators_found(&req, &mut res, version);
            encode_reply(key, version, correlation_id, &res)
        }
        // Groups live on the leader of a shared store: another broker sends the client looking for it.
        ApiKey::JoinGroup => {
            let req: JoinGroupRequest = decode(body, version)?;
            let refused = (!authz.group(req.group_id.0.as_str(), Operation::Read)).then_some(GROUP_DENIED);
            let res = match refused.or_else(|| handlers::not_coordinator(state, req.group_id.0.as_str())) {
                Some(code) => handlers::join_refused(code, version),
                None => handlers::join_group(state, &req, version, &client_id, authz.host).await,
            };
            encode_reply(key, version, correlation_id, &res)
        }
        // Describe on the cluster shows every group; otherwise those the principal may describe.
        ApiKey::ListGroups => {
            let req: ListGroupsRequest = decode(body, version)?;
            let everything = authz.cluster(Operation::Describe);
            let res = handlers::list_groups(state, &req, version, |g| {
                everything || authz.group(g, Operation::Describe)
            })
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::DescribeGroups => {
            let req: DescribeGroupsRequest = decode(body, version)?;
            let res = handlers::describe_groups(state, &req, version, |g, op| authz.group(g, op)).await;
            encode_reply(key, version, correlation_id, &res)
        }
        // Delete on the group; for its offsets of a topic, Read on the topic too.
        ApiKey::DeleteGroups => {
            let req: DeleteGroupsRequest = decode(body, version)?;
            let res = handlers::delete_groups(state, &req, |g| authz.group(g, Operation::Delete)).await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::OffsetDelete => {
            let req: OffsetDeleteRequest = decode(body, version)?;
            let res = handlers::delete_offsets(
                state,
                &req,
                |g| authz.group(g, Operation::Delete),
                |t| authz.may(crate::security::Resource::Topic, t, Operation::Read),
            )
            .await;
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::SyncGroup => {
            let req: SyncGroupRequest = decode(body, version)?;
            let refused = (!authz.group(req.group_id.0.as_str(), Operation::Read)).then_some(GROUP_DENIED);
            let res = match refused.or_else(|| handlers::not_coordinator(state, req.group_id.0.as_str())) {
                Some(code) => handlers::sync_refused(code),
                None => handlers::sync_group(state, &req, version).await,
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::Heartbeat => {
            let req: HeartbeatRequest = decode(body, version)?;
            let refused = (!authz.group(req.group_id.0.as_str(), Operation::Read)).then_some(GROUP_DENIED);
            let res = match refused.or_else(|| handlers::not_coordinator(state, req.group_id.0.as_str())) {
                Some(code) => handlers::heartbeat_refused(code),
                None => handlers::heartbeat(state, &req),
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::LeaveGroup => {
            let req: LeaveGroupRequest = decode(body, version)?;
            let refused = (!authz.group(req.group_id.0.as_str(), Operation::Read)).then_some(GROUP_DENIED);
            let res = match refused.or_else(|| handlers::not_coordinator(state, req.group_id.0.as_str())) {
                Some(code) => handlers::leave_refused(code),
                None => handlers::leave_group(state, &req, version),
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::OffsetCommit => {
            let mut req: OffsetCommitRequest = decode(body, version)?;
            let res = match handlers::not_coordinator(state, req.group_id.0.as_str()) {
                Some(code) => handlers::offset_commit_refused(code, &req),
                None => {
                    let denied = authz.offset_commit(&mut req);
                    let mut res = handlers::offset_commit(state, &req).await;
                    Authorize::committed(&mut res, denied);
                    res
                }
            };
            encode_reply(key, version, correlation_id, &res)
        }
        ApiKey::OffsetFetch => {
            let req: OffsetFetchRequest = decode(body, version)?;
            // A request for several groups (v8) is answered here only if every one of them is coordinated here.
            let not_here = if version >= 8 {
                req.groups
                    .iter()
                    .find_map(|g| handlers::not_coordinator(state, g.group_id.0.as_str()))
            } else {
                handlers::not_coordinator(state, req.group_id.0.as_str())
            };
            let res = match not_here {
                Some(code) => handlers::offset_fetch_refused(code, &req, version),
                None => {
                    let mut res = handlers::offset_fetch(state, &req, version).await;
                    authz.offsets_fetched(&req, &mut res, version);
                    res
                }
            };
            encode_reply(key, version, correlation_id, &res)
        }
        _ => Err(ConnectionError::Unsupported {
            key: key as i16,
            version,
        }),
    }
    .inspect(|_| {
        // Bytes left over after a request means the client and broker disagree on the format: say so in the log.
        if body.has_remaining() {
            tracing::debug!(
                ?key,
                version,
                left = body.remaining(),
                "request had trailing bytes"
            );
        }
    })
}
