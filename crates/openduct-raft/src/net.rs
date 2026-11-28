// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! How brokers reach each other. Between two brokers there are two connections carrying every group's messages -
//! one for log entries, one for heartbeats and votes, so a large append never holds up a heartbeat - and one more
//! for each snapshot sent. Messages are framed by their length and encoded with postcard; each names its group and
//! carries an id its answer repeats, so many are in flight at once.
//!
//! A connection opens with a hello naming the cluster and carrying its shared secret; a broker answers nothing else
//! to a connection whose secret does not match. With TLS (see `tls`) every connection is encrypted and both ends
//! show a certificate the cluster's authority signed; without it everything, the secret too, travels as written,
//! so the replication port then belongs on a private network.
//!
//! Besides Raft's own messages: writes for the controller that a broker forwards to its leader, and the leaders of
//! the groups a broker holds, which the others ask for to name the leaders of partitions they hold no copy of.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use openduct_storage::{Manifest, checked_path, sync_checkpoint};
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, StreamAppendError, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkV2};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::features::{self, Levels};
use crate::members::{Forwarded, LeaderOf, Member, Members, admit, dismiss, lead_write};
use crate::snapshot::SnapshotFiles;
use crate::stable;
use crate::types::{self, Broker, Command, GroupId, Reply, SnapshotMeta, TypeConfig, Vote};

/// The largest message: Raft sends a few entries at a time, each at most one batch at the broker's limit.
const MAX_MESSAGE: usize = 1 << 30;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Who may join: the cluster's name and its shared secret - and, with TLS, a certificate the cluster's authority
/// signed.
#[derive(Debug, Clone)]
pub struct ClusterAuth {
    pub cluster: String,
    pub secret: String,
    /// TLS on every connection between brokers; `None` sends everything as written (a private network only).
    pub tls: Option<crate::tls::ReplicationTls>,
}

impl ClusterAuth {
    fn admits(&self, hello: &Hello) -> bool {
        hello.cluster == self.cluster && same(&hello.secret, &self.secret)
    }
}

/// Compares two secrets in time that does not depend on where they differ.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Purpose {
    Messages,
    Snapshot,
}

/// What a broker answers a hello with: which copy of itself it is (a new one each time it starts with an empty
/// disk), and how far its controller has committed (none before it is part of a cluster).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct PeerInfo {
    pub(crate) incarnation: uuid::Uuid,
    pub(crate) controller_committed: Option<u64>,
    /// The protocol the two brokers agreed on.
    pub(crate) protocol: u16,
    /// The feature level its build supports, and the one its cluster uses.
    pub(crate) supported_level: u32,
    pub(crate) active_level: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Hello {
    /// The protocol versions the broker speaks, oldest and newest.
    pub(crate) protocols: (u16, u16),
    /// The feature level the broker's build supports.
    pub(crate) supported_level: u32,
    pub(crate) cluster: String,
    pub(crate) secret: String,
    pub(crate) from: u64,
    pub(crate) purpose: Purpose,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    Append(stable::Append),
    Vote(stable::VoteAsk),
    PreVote(stable::VoteAsk),
    TransferLeader(stable::Handover),
    Forward(Command),
    /// A broker asks to join the cluster, or is taken out of it (on the controller's leader).
    Join {
        id: u64,
        broker: Broker,
    },
    Leave {
        id: u64,
    },
    Leaders,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Response {
    Append(Result<Option<stable::LogId>, stable::AppendRefusal>),
    Vote(stable::VoteAnswer),
    /// The vote the follower answered a snapshot with.
    Snapshot(stable::Vote),
    TransferLeader(Result<(), stable::HandoverRefusal>),
    Membership(Result<(), Forwarded>),
    /// The leader's answer and the index of the write's entry.
    Forward(Result<(Reply, u64), Forwarded>),
    Leaders(Vec<LeaderOf>),
    /// The broker could not handle the message: it does not hold the group (yet), or the group is stopping.
    Failed(String),
}

#[derive(Debug, Serialize)]
pub(crate) struct EnvelopeRef<'a> {
    pub(crate) id: u64,
    pub(crate) group: GroupId,
    pub(crate) request: &'a Request,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Envelope {
    pub(crate) id: u64,
    pub(crate) group: GroupId,
    pub(crate) request: Request,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Answer {
    pub(crate) id: u64,
    pub(crate) response: Response,
}

/// What a snapshot connection sends first, before the files' bytes.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotHeader {
    pub(crate) group: GroupId,
    pub(crate) vote: stable::Vote,
    pub(crate) meta: stable::SnapshotMeta,
    pub(crate) manifest: Manifest,
}

fn io_other(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

fn encode<T: Serialize>(message: &T) -> std::io::Result<Vec<u8>> {
    let body = postcard::to_stdvec(message).map_err(|e| io_other(format!("cannot encode a message: {e}")))?;
    let len = u32::try_from(body.len()).map_err(|_| io_other("a message too large to send"))?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

async fn send<T: Serialize>(w: &mut (impl AsyncWrite + Unpin), message: &T) -> std::io::Result<()> {
    w.write_all(&encode(message)?).await?;
    w.flush().await
}

async fn receive<T: DeserializeOwned>(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<T> {
    let len = r.read_u32().await? as usize;
    if len > MAX_MESSAGE {
        return Err(io_other(format!("a message of {len} bytes is over the limit")));
    }
    // Grown as bytes arrive: a length on the wire never sizes an allocation by itself.
    let mut body = Vec::with_capacity(len.min(1 << 20));
    let read = r.take(len as u64).read_to_end(&mut body).await?;
    if read != len {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    postcard::from_bytes(&body).map_err(|e| io_other(format!("cannot decode a message: {e}")))
}

/// A connection, in TLS or not.
trait Duplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Duplex for T {}
type Stream = Box<dyn Duplex>;
type Reader = BufReader<ReadHalf<Stream>>;
type Writer = BufWriter<WriteHalf<Stream>>;

async fn connect(
    addr: &str,
    auth: &ClusterAuth,
    levels: &Levels,
    from: u64,
    purpose: Purpose,
) -> std::io::Result<(Reader, Writer, PeerInfo)> {
    let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io_other(format!("no answer from {addr}")))??;
    stream.set_nodelay(true)?;
    let stream: Stream = match &auth.tls {
        Some(tls) => {
            let connector = tokio_rustls::TlsConnector::from(Arc::clone(&tls.client));
            let secured = tokio::time::timeout(
                CONNECT_TIMEOUT,
                connector.connect(crate::tls::server_name(addr)?, stream),
            )
            .await
            .map_err(|_| io_other(format!("no TLS answer from {addr}")))??;
            Box::new(secured)
        }
        None => Box::new(stream),
    };
    let (r, w) = tokio::io::split(stream);
    let (mut r, mut w) = (BufReader::new(r), BufWriter::new(w));
    let hello = Hello {
        protocols: (*features::PROTOCOLS.start(), *features::PROTOCOLS.end()),
        supported_level: levels.supported(),
        cluster: auth.cluster.clone(),
        secret: auth.secret.clone(),
        from,
        purpose,
    };
    send(&mut w, &hello).await?;
    match tokio::time::timeout(CONNECT_TIMEOUT, receive::<Result<PeerInfo, String>>(&mut r))
        .await
        .map_err(|_| io_other(format!("{addr} did not answer the hello")))??
    {
        Ok(info) => {
            if !features::PROTOCOLS.contains(&info.protocol) {
                return Err(io_other(format!(
                    "{addr} chose protocol {}, which this build does not speak",
                    info.protocol
                )));
            }
            // A broker whose build is older than the cluster's features gets nothing it would misread.
            if let Some(why) = levels.refuses(info.supported_level) {
                return Err(io_other(format!("{addr} is not sent to: {why}")));
            }
            levels.set_active(info.active_level);
            Ok((r, w, info))
        }
        Err(why) => Err(io_other(format!("{addr} refused this broker: {why}"))),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The client side.

/// One open connection carrying many messages.
#[derive(Debug)]
struct Mux {
    frames: mpsc::UnboundedSender<Vec<u8>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Response>>>,
    next: AtomicU64,
    alive: AtomicBool,
}

impl Mux {
    fn close(&self) {
        self.alive.store(false, Ordering::Release);
        // The callers still waiting see their answer channel close.
        self.pending.lock().clear();
    }
}

/// Which copy of each other broker this one last spoke to. A broker that comes back with a new one lost its disk:
/// the groups led here are told to take its emptied log, before anything is sent to it.
#[derive(Debug)]
struct Seen {
    me: u64,
    incarnations: tokio::sync::Mutex<HashMap<u64, uuid::Uuid>>,
    members: Arc<Members>,
}

impl Seen {
    async fn observe(&self, target: u64, incarnation: uuid::Uuid) {
        let mut known = self.incarnations.lock().await;
        let before = known.insert(target, incarnation);
        if before.is_some_and(|b| b != incarnation) {
            // Held across the reset, so no connection to the broker sends anything before it is done.
            self.members.allow_reset(self.me, target).await;
        }
    }
}

/// The connection to one broker for one lane, opened again whenever it breaks.
#[derive(Debug)]
struct Link {
    target: u64,
    addr: String,
    me: u64,
    auth: Arc<ClusterAuth>,
    seen: Arc<Seen>,
    mux: tokio::sync::Mutex<Option<Arc<Mux>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Lane {
    Entries,
    Heartbeats,
}

impl Link {
    async fn open(&self) -> std::io::Result<Arc<Mux>> {
        let mut held = self.mux.lock().await;
        if let Some(m) = held.as_ref().filter(|m| m.alive.load(Ordering::Acquire)) {
            return Ok(Arc::clone(m));
        }
        let (mut r, mut w, info) = connect(
            &self.addr,
            &self.auth,
            self.seen.members.levels(),
            self.me,
            Purpose::Messages,
        )
        .await?;
        self.seen.observe(self.target, info.incarnation).await;
        let (frames, mut queued) = mpsc::unbounded_channel::<Vec<u8>>();
        let mux = Arc::new(Mux {
            frames,
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
            alive: AtomicBool::new(true),
        });
        let writer = Arc::clone(&mux);
        tokio::spawn(async move {
            'writing: while let Some(frame) = queued.recv().await {
                if w.write_all(&frame).await.is_err() {
                    break;
                }
                while let Ok(more) = queued.try_recv() {
                    if w.write_all(&more).await.is_err() {
                        break 'writing;
                    }
                }
                if w.flush().await.is_err() {
                    break;
                }
            }
            writer.close();
        });
        let reader = Arc::clone(&mux);
        tokio::spawn(async move {
            while let Ok(answer) = receive::<Answer>(&mut r).await {
                if let Some(waiting) = reader.pending.lock().remove(&answer.id) {
                    let _ = waiting.send(answer.response);
                }
            }
            reader.close();
        });
        *held = Some(Arc::clone(&mux));
        Ok(mux)
    }

    async fn call(
        &self,
        group: GroupId,
        request: &Request,
        ttl: Duration,
    ) -> Result<Response, RPCError<TypeConfig>> {
        let mux = self
            .open()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let id = mux.next.fetch_add(1, Ordering::Relaxed);
        let frame = encode(&EnvelopeRef { id, group, request })
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let (tx, rx) = oneshot::channel();
        mux.pending.lock().insert(id, tx);
        if mux.frames.send(frame).is_err() {
            mux.pending.lock().remove(&id);
            return Err(RPCError::Network(NetworkError::from_string(
                "the connection closed",
            )));
        }
        match tokio::time::timeout(ttl, rx).await {
            Ok(Ok(Response::Failed(why))) => Err(RPCError::Unreachable(Unreachable::from_string(why))),
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(RPCError::Network(NetworkError::from_string(format!(
                "the connection to {} closed",
                self.addr
            )))),
            Err(_) => {
                mux.pending.lock().remove(&id);
                Err(RPCError::Network(NetworkError::from_string(format!(
                    "no answer from {} within {ttl:?}",
                    self.addr
                ))))
            }
        }
    }
}

/// This broker's connections to the others, shared by all its groups.
#[derive(Debug)]
pub struct Links {
    me: u64,
    auth: Arc<ClusterAuth>,
    seen: Arc<Seen>,
    links: Mutex<HashMap<(u64, Lane), Arc<Link>>>,
}

impl Links {
    pub(crate) fn new(me: u64, auth: ClusterAuth, members: Arc<Members>) -> Arc<Self> {
        Arc::new(Self {
            me,
            auth: Arc::new(auth),
            seen: Arc::new(Seen {
                me,
                incarnations: tokio::sync::Mutex::new(HashMap::new()),
                members,
            }),
            links: Mutex::new(HashMap::new()),
        })
    }

    /// Asks `addr` which copy of itself it is and how far its controller has committed, without staying.
    pub(crate) async fn probe(&self, addr: &str) -> Option<PeerInfo> {
        connect(
            addr,
            &self.auth,
            self.seen.members.levels(),
            self.me,
            Purpose::Messages,
        )
        .await
        .ok()
        .map(|(_, _, info)| info)
    }

    fn link(&self, target: u64, addr: &str, lane: Lane) -> Arc<Link> {
        let fresh = || {
            Arc::new(Link {
                target,
                addr: addr.to_owned(),
                me: self.me,
                auth: Arc::clone(&self.auth),
                seen: Arc::clone(&self.seen),
                mux: tokio::sync::Mutex::new(None),
            })
        };
        let mut links = self.links.lock();
        let link = links.entry((target, lane)).or_insert_with(fresh);
        if link.addr != addr {
            // The broker moved: a new link to its new address.
            *link = fresh();
        }
        Arc::clone(link)
    }

    /// The factory a group's Raft uses to reach the other brokers.
    pub(crate) fn network(self: &Arc<Self>, group: GroupId) -> Network {
        Network {
            group,
            links: Arc::clone(self),
        }
    }

    /// Sends a controller write to its leader `target`: the answer and the index of its entry. A leader that cannot
    /// be reached is reported as no leader (the write was never sent, so it may be tried again); a lost answer after
    /// sending is an error, since the write may have been kept.
    pub(crate) async fn forward(
        &self,
        target: u64,
        addr: &str,
        group: GroupId,
        command: Command,
        ttl: Duration,
    ) -> Result<(Reply, u64), Forwarded> {
        match self
            .link(target, addr, Lane::Heartbeats)
            .call(group, &Request::Forward(command), ttl)
            .await
        {
            Ok(Response::Forward(answer)) => answer,
            Ok(_) => Err(Forwarded::Unavailable(
                "the leader answered with something else".to_owned(),
            )),
            Err(RPCError::Unreachable(_)) => Err(Forwarded::NotLeader { leader: None }),
            Err(e) => Err(Forwarded::Unavailable(e.to_string())),
        }
    }

    /// Asks `target`, the controller's leader, to take a broker in (`join`) or out of the cluster. Taking one in waits
    /// until it has caught up.
    pub(crate) async fn membership(
        &self,
        target: u64,
        addr: &str,
        id: u64,
        join: Option<Broker>,
    ) -> Result<(), Forwarded> {
        let request = match join {
            Some(broker) => Request::Join { id, broker },
            None => Request::Leave { id },
        };
        match self
            .link(target, addr, Lane::Heartbeats)
            .call(GroupId::Controller, &request, Duration::from_secs(120))
            .await
        {
            Ok(Response::Membership(answer)) => answer,
            Ok(_) => Err(Forwarded::Unavailable(
                "the leader answered with something else".to_owned(),
            )),
            Err(RPCError::Unreachable(_)) => Err(Forwarded::NotLeader { leader: None }),
            Err(e) => Err(Forwarded::Unavailable(e.to_string())),
        }
    }

    /// The leaders of the groups `target` holds, as it knows them.
    pub(crate) async fn leaders(&self, target: u64, addr: &str) -> Option<Vec<LeaderOf>> {
        match self
            .link(target, addr, Lane::Heartbeats)
            .call(GroupId::Controller, &Request::Leaders, Duration::from_secs(2))
            .await
        {
            Ok(Response::Leaders(l)) => Some(l),
            _ => None,
        }
    }
}

/// One group's way to the other brokers.
#[derive(Debug, Clone)]
pub struct Network {
    group: GroupId,
    links: Arc<Links>,
}

/// One other broker, for one group.
#[derive(Debug)]
pub struct Peer {
    group: GroupId,
    target: u64,
    addr: String,
    link: Arc<Link>,
    links: Arc<Links>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Peer;

    async fn new_client(&mut self, target: u64, node: &Broker) -> Peer {
        self.peer(target, node, Lane::Entries)
    }

    async fn new_heartbeat_client(&mut self, target: u64, node: &Broker) -> Peer {
        self.peer(target, node, Lane::Heartbeats)
    }
}

impl Network {
    fn peer(&self, target: u64, node: &Broker, lane: Lane) -> Peer {
        Peer {
            group: self.group,
            target,
            addr: node.raft.clone(),
            link: self.links.link(target, &node.raft, lane),
            links: Arc::clone(&self.links),
        }
    }
}

impl Peer {
    fn unexpected(&self) -> RPCError<TypeConfig> {
        RPCError::Network(NetworkError::from_string(format!(
            "broker {} answered with the wrong message",
            self.target
        )))
    }

    async fn vote_call(
        &mut self,
        request: Request,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.link.call(self.group, &request, option.soft_ttl()).await? {
            Response::Vote(v) => Ok(v.into()),
            _ => Err(self.unexpected()),
        }
    }

    async fn send_snapshot(
        &self,
        vote: Vote,
        meta: SnapshotMeta,
        files: Arc<SnapshotFiles>,
    ) -> std::io::Result<Response> {
        // A connection of its own: a snapshot can take a while, and nothing else waits behind it.
        let (mut r, mut w, _) = connect(
            &self.addr,
            &self.links.auth,
            self.links.seen.members.levels(),
            self.links.me,
            Purpose::Snapshot,
        )
        .await?;
        let header = SnapshotHeader {
            group: self.group,
            vote: vote.into(),
            meta: (&meta).into(),
            manifest: files.manifest.clone(),
        };
        send(&mut w, &header).await?;
        for f in &files.manifest.files {
            let path = files
                .dir
                .join(checked_path(&f.path).map_err(|e| io_other(e.to_string()))?);
            let file: tokio::fs::File = tokio::fs::File::open(&path).await?;
            let copied = tokio::io::copy(&mut file.take(f.len), &mut w).await?;
            if copied != f.len {
                return Err(io_other(format!(
                    "{} is shorter than its snapshot says",
                    path.display()
                )));
            }
        }
        w.flush().await?;
        receive::<Response>(&mut r).await
    }
}

impl RaftNetworkV2<TypeConfig> for Peer {
    type SnapshotData = Arc<SnapshotFiles>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        match self
            .link
            .call(self.group, &Request::Append((&rpc).into()), option.soft_ttl())
            .await?
        {
            Response::Append(result) => Ok(result
                .map(|l| l.map(Into::into))
                .map_err(|e| StreamAppendError::from(e).into())),
            _ => Err(self.unexpected()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_call(Request::Vote((&rpc).into()), option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_call(Request::PreVote((&rpc).into()), option).await
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self
            .link
            .call(
                self.group,
                &Request::TransferLeader((&req).into()),
                option.soft_ttl(),
            )
            .await?
        {
            Response::TransferLeader(answer) => Ok(answer.map_err(Into::into)),
            _ => Err(self.unexpected()),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: openraft::alias::SnapshotOf<TypeConfig, Arc<SnapshotFiles>>,
        cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let sending = self.send_snapshot(vote, snapshot.meta, snapshot.snapshot);
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            sent = sending => match sent {
                Ok(Response::Snapshot(vote)) => Ok(stable::snapshot_answer(vote)),
                Ok(Response::Failed(why)) => Err(StreamingError::Unreachable(Unreachable::from_string(why))),
                Ok(_) => Err(StreamingError::Network(NetworkError::from_string("a snapshot answered with the wrong message"))),
                Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
            },
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The server side.

/// Answers the other brokers on `listener` until the task is stopped - which closes every connection it took, so
/// the others notice at once. `incarnation`: which copy of this broker answers.
pub(crate) async fn serve(
    listener: TcpListener,
    members: Arc<Members>,
    auth: Arc<ClusterAuth>,
    incarnation: uuid::Uuid,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        // Finished connections are let go of as new ones come.
        while connections.try_join_next().is_some() {}
        let (stream, from) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(error = %e, "cannot accept a replication connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let (members, auth) = (Arc::clone(&members), Arc::clone(&auth));
        connections.spawn(async move {
            if let Err(e) = serve_connection(stream, members, auth, incarnation).await
                && e.kind() != std::io::ErrorKind::UnexpectedEof
            {
                tracing::debug!(%from, error = %e, "replication connection closed");
            }
        });
    }
}

async fn serve_connection(
    stream: TcpStream,
    members: Arc<Members>,
    auth: Arc<ClusterAuth>,
    incarnation: uuid::Uuid,
) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let stream: Stream = match &auth.tls {
        Some(tls) => {
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&tls.server));
            Box::new(
                tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream))
                    .await
                    .map_err(|_| io_other("no TLS hello"))??,
            )
        }
        None => Box::new(stream),
    };
    let (r, w) = tokio::io::split(stream);
    let (mut r, mut w) = (BufReader::new(r), BufWriter::new(w));
    let hello: Hello = tokio::time::timeout(CONNECT_TIMEOUT, receive(&mut r))
        .await
        .map_err(|_| io_other("no hello"))??;
    if !auth.admits(&hello) {
        tracing::warn!(
            from = hello.from,
            "refused a broker with another cluster name or secret"
        );
        send(&mut w, &Err::<PeerInfo, String>("not of this cluster".to_owned())).await?;
        return Ok(());
    }
    let Some(protocol) = features::agree(&features::PROTOCOLS, hello.protocols) else {
        tracing::warn!(from = hello.from, theirs = ?hello.protocols, "refused a broker that speaks no protocol this build speaks");
        let why = format!(
            "this broker speaks protocols {} to {}; upgrade one of the two",
            features::PROTOCOLS.start(),
            features::PROTOCOLS.end()
        );
        send(&mut w, &Err::<PeerInfo, String>(why)).await?;
        return Ok(());
    };
    let levels = members.levels();
    if let Some(why) = levels.refuses(hello.supported_level) {
        tracing::warn!(
            from = hello.from,
            "refused a broker whose build is older than the cluster's features"
        );
        send(&mut w, &Err::<PeerInfo, String>(why)).await?;
        return Ok(());
    }
    let info = PeerInfo {
        incarnation,
        controller_committed: members.controller_committed(),
        protocol,
        supported_level: levels.supported(),
        active_level: levels.active(),
    };
    send(&mut w, &Ok::<PeerInfo, String>(info)).await?;
    match hello.purpose {
        Purpose::Snapshot => serve_snapshot(&mut r, &mut w, &members).await,
        Purpose::Messages => serve_messages(r, w, members).await,
    }
}

async fn serve_messages(mut r: Reader, mut w: Writer, members: Arc<Members>) -> std::io::Result<()> {
    let (answers, mut queued) = mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        while let Some(frame) = queued.recv().await {
            w.write_all(&frame).await?;
            while let Ok(more) = queued.try_recv() {
                w.write_all(&more).await?;
            }
            w.flush().await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let result = loop {
        let envelope: Envelope = match receive(&mut r).await {
            Ok(e) => e,
            Err(e) => break Err(e),
        };
        let (members, answers) = (Arc::clone(&members), answers.clone());
        // Each message on its own: one group's slow answer holds up no other.
        tokio::spawn(async move {
            let response = handle(&members, envelope.group, envelope.request).await;
            if let Ok(frame) = encode(&Answer {
                id: envelope.id,
                response,
            }) {
                let _ = answers.send(frame);
            }
        });
    };
    writer.abort();
    result
}

/// Whether an append sent by another broker is one a broker writes itself: a transaction's marker (a control
/// batch), or a record of the transaction coordinator's own topic.
fn own_append(members: &Members, group: &GroupId, command: &Command) -> bool {
    let Command::Append { batch } = command else {
        return false;
    };
    if openduct_storage::batch::marker_of(batch).is_some() {
        return true;
    }
    let (GroupId::Partition { topic, .. }, Some(Member::Controller(_, control))) =
        (group, members.get(&GroupId::Controller))
    else {
        return false;
    };
    control
        .state()
        .topics
        .get(openduct_storage::TRANSACTIONS_TOPIC)
        .is_some_and(|t| t.info.id == *topic)
}

async fn handle(members: &Members, group: GroupId, request: Request) -> Response {
    if let Request::Leaders = request {
        return Response::Leaders(members.hosted_leaders());
    }
    let Some(member) = members.get(&group) else {
        return Response::Failed(format!("this broker holds no copy of {group} (yet)"));
    };
    match request {
        Request::Append(rpc) => match AppendEntriesRequest::try_from(rpc) {
            Ok(rpc) => match member.append(rpc).await {
                Ok(result) => {
                    // Taken from a leader this copy accepts: it is being led (what "ready" asks, members.rs).
                    if result.is_ok() {
                        members.heard_from_leader(&group);
                    }
                    Response::Append(result.map(|l| l.map(Into::into)).map_err(Into::into))
                }
                Err(e) => Response::Failed(e),
            },
            Err(e) => Response::Failed(e),
        },
        Request::Vote(_) | Request::PreVote(_) if !members.may_vote(&group, &member) => Response::Failed(
            format!("this broker's copy of {group} is catching up after losing its disk"),
        ),
        Request::Vote(rpc) => member
            .vote(rpc.into(), false)
            .await
            .map_or_else(Response::Failed, |v| Response::Vote((&v).into())),
        Request::PreVote(rpc) => member
            .vote(rpc.into(), true)
            .await
            .map_or_else(Response::Failed, |v| Response::Vote((&v).into())),
        // A broker that is stopping takes no leadership: it would lead until it is killed (members.rs `leave`).
        Request::TransferLeader(_) if members.is_leaving() => {
            Response::Failed("this broker is stopping: it takes no leadership".to_owned())
        }
        Request::TransferLeader(req) => match member.take_leadership(req.into()).await {
            Ok(answer) => Response::TransferLeader(answer.map_err(Into::into)),
            Err(e) => Response::Failed(e),
        },
        Request::Join { id, broker } => match &member {
            Member::Controller(raft, _) => Response::Membership(admit(raft, id, broker).await),
            Member::Partition(..) => Response::Failed("brokers join the controller".to_owned()),
        },
        Request::Leave { id } => match &member {
            Member::Controller(raft, _) => Response::Membership(dismiss(raft, id).await),
            Member::Partition(..) => Response::Failed("brokers leave the controller".to_owned()),
        },
        Request::Forward(command) => match &member {
            Member::Controller(raft, _) => Response::Forward(lead_write(raft, command).await),
            // To a partition, only what a broker itself writes: a consumer group's commit or deletion (a
            // partition of the offsets topic), a transaction's marker, a record of the transaction coordinator's
            // topic. Nothing else reaches a partition's log except through its leader's own client.
            Member::Partition(raft, _)
                if matches!(
                    command,
                    Command::CommitOffsets { .. } | Command::DeleteOffsets { .. }
                ) || own_append(members, &group, &command) =>
            {
                Response::Forward(lead_write(raft, command).await)
            }
            Member::Partition(..) => {
                Response::Failed("only offset commits are forwarded to a partition".to_owned())
            }
        },
        Request::Leaders => Response::Leaders(members.hosted_leaders()),
    }
}

/// Takes a snapshot's files off the connection into a new directory, then has the group's Raft install it.
async fn serve_snapshot(r: &mut Reader, w: &mut Writer, members: &Members) -> std::io::Result<()> {
    let header: SnapshotHeader = receive(r).await?;
    let Some(member) = members.get(&header.group) else {
        return send(
            w,
            &Response::Failed(format!("this broker holds no copy of {}", header.group)),
        )
        .await;
    };
    let dir = member.incoming_dir()?;
    for f in &header.manifest.files {
        let rel = checked_path(&f.path).map_err(|e| io_other(e.to_string()))?;
        let target = dir.join(rel);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut out = tokio::fs::File::create(&target).await?;
        let copied = tokio::io::copy(&mut (&mut *r).take(f.len), &mut out).await?;
        if copied != f.len {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
    }
    let (synced_dir, manifest) = (dir.clone(), header.manifest.clone());
    let synced = tokio::task::spawn_blocking(move || sync_checkpoint(&synced_dir, &manifest))
        .await
        .map_err(|e| io_other(e.to_string()))?;
    if let Err(e) = synced {
        return send(w, &Response::Failed(format!("cannot keep the snapshot: {e}"))).await;
    }
    let files = Arc::new(SnapshotFiles::new(dir, header.manifest));
    // Moved in among the group's snapshots once installed; removed if Raft does not want it.
    files.discard_unless_taken_in();
    let meta = match types::SnapshotMeta::try_from(header.meta) {
        Ok(meta) => meta,
        Err(e) => return send(w, &Response::Failed(e)).await,
    };
    let response = match member.install(header.vote.into(), meta, files).await {
        Ok(answer) => Response::Snapshot((&answer).into()),
        Err(e) => Response::Failed(e),
    };
    send(w, &response).await
}
