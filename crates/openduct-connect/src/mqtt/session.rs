// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One connection to an MQTT broker, kept up: connecting again after a failure that passes, up to `ATTEMPTS` in a
//! row (waits of half a second, then 1, 2, 4 and 8 seconds), and ending with the reason after one that lasts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep_until, timeout};

use super::codec::{self, Incoming, Publish};
use crate::web::ATTEMPTS;

/// The largest MQTT packet read or written.
pub(crate) const MAX_PACKET: usize = 16 * 1024 * 1024;
/// How long connecting may take, the TLS handshake and the MQTT broker's answer included.
const CONNECT_WAIT: Duration = Duration::from_secs(10);
/// The most messages written and not yet acknowledged.
const MAX_INFLIGHT: usize = 100;

/// Where and how to connect.
#[derive(Clone)]
pub(super) struct Options {
    pub host: String,
    pub port: u16,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    pub clean_session: bool,
}

impl std::fmt::Debug for Options {
    // Never the password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls.is_some())
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

/// Why a connection ended.
#[derive(Debug)]
enum Failure {
    /// Trying again will not help: a refused login, say.
    Lasting(String),
    /// Trying again may: a dropped connection, say.
    Passing(String),
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

struct Connection {
    reader: ReadHalf<Box<dyn Io>>,
    writer: WriteHalf<Box<dyn Io>>,
    buf: BytesMut,
    session_present: bool,
}

impl Connection {
    async fn open(o: &Options) -> Result<Self, Failure> {
        timeout(CONNECT_WAIT, Self::handshake(o))
            .await
            .map_err(|_| Failure::Passing("the MQTT broker did not answer within 10 seconds".to_owned()))?
    }

    async fn handshake(o: &Options) -> Result<Self, Failure> {
        let tcp = TcpStream::connect((o.host.as_str(), o.port))
            .await
            .map_err(|e| Failure::Passing(format!("cannot connect to the MQTT broker: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn Io> = match &o.tls {
            None => Box::new(tcp),
            Some(config) => {
                let name = rustls::pki_types::ServerName::try_from(o.host.clone())
                    .map_err(|_| Failure::Lasting(format!("{} is not a name TLS can check", o.host)))?;
                let tls = tokio_rustls::TlsConnector::from(Arc::clone(config))
                    .connect(name, tcp)
                    .await
                    .map_err(tls_failure)?;
                Box::new(tls)
            }
        };
        let (reader, writer) = tokio::io::split(stream);
        let mut conn = Self {
            reader,
            writer,
            buf: BytesMut::with_capacity(8 * 1024),
            session_present: false,
        };
        let hello = codec::connect(&codec::Connect {
            client_id: &o.client_id,
            keep_alive_secs: u16::try_from(o.keep_alive.as_secs()).unwrap_or(u16::MAX),
            clean_session: o.clean_session,
            username: o.username.as_deref(),
            password: o.password.as_deref(),
        })
        .map_err(Failure::Lasting)?;
        conn.send(&hello).await?;
        match conn.read().await? {
            Incoming::ConnAck {
                session_present,
                code: 0,
            } => {
                conn.session_present = session_present;
                Ok(conn)
            }
            Incoming::ConnAck { code: 3, .. } => Err(Failure::Passing(
                "the MQTT broker is not taking connections now".to_owned(),
            )),
            Incoming::ConnAck { code, .. } => Err(Failure::Lasting(refused(code))),
            _ => Err(Failure::Lasting(
                "the MQTT broker answered the connection with something else than CONNACK".to_owned(),
            )),
        }
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        self.writer.write_all(bytes).await.map_err(broke)?;
        self.writer.flush().await.map_err(broke)
    }

    /// The next packet. Safe to drop part way: what has arrived stays in the buffer.
    async fn read(&mut self) -> Result<Incoming, Failure> {
        loop {
            match codec::decode(&mut self.buf, MAX_PACKET) {
                Ok(Some(packet)) => return Ok(packet),
                Ok(None) => {}
                Err(e) => {
                    return Err(Failure::Lasting(format!(
                        "the MQTT broker was not understood: {e}"
                    )));
                }
            }
            if self.buf.capacity() == self.buf.len() {
                self.buf.reserve(8 * 1024);
            }
            match self.reader.read_buf(&mut self.buf).await {
                Ok(0) => {
                    return Err(Failure::Passing(
                        "the MQTT broker closed the connection".to_owned(),
                    ));
                }
                Ok(_) => {}
                Err(e) => return Err(broke(e)),
            }
        }
    }
}

fn broke(e: std::io::Error) -> Failure {
    Failure::Passing(format!("the connection to the MQTT broker broke: {e}"))
}

fn tls_failure(e: std::io::Error) -> Failure {
    match crate::tls::failure(&e, "the MQTT broker") {
        (true, reason) => Failure::Lasting(reason),
        (false, reason) => Failure::Passing(reason),
    }
}

fn refused(code: u8) -> String {
    match code {
        1 => "the MQTT broker does not speak MQTT 3.1.1".to_owned(),
        2 => "the MQTT broker refused the client id".to_owned(),
        4 => "the MQTT broker refused the username or password".to_owned(),
        5 => "the MQTT broker refused: not authorised".to_owned(),
        _ => format!("the MQTT broker refused the connection (code {code})"),
    }
}

/// Connect, again after a failure that passes; the reason once it cannot.
async fn connect(o: &Options, failures: &mut u32) -> Result<Connection, String> {
    loop {
        match Connection::open(o).await {
            Ok(conn) => {
                *failures = 0;
                return Ok(conn);
            }
            Err(Failure::Lasting(reason)) => return Err(reason),
            Err(Failure::Passing(reason)) => wait_to_retry(failures, reason).await?,
        }
    }
}

/// Wait before connecting again, or the reason when it has failed [`ATTEMPTS`] times in a row.
async fn wait_to_retry(failures: &mut u32, reason: String) -> Result<(), String> {
    *failures += 1;
    if *failures >= ATTEMPTS {
        return Err(reason);
    }
    tracing::warn!(%reason, "MQTT connection lost; connecting again");
    let wait = Duration::from_millis(500) * 2u32.saturating_pow(*failures - 1);
    tokio::time::sleep(wait.min(Duration::from_secs(10))).await;
    Ok(())
}

/// When the next ping is due, and whether the last one is still unanswered.
struct Pinger {
    every: Duration,
    due: Instant,
    waiting: bool,
}

impl Pinger {
    fn new(every: Duration) -> Self {
        Self {
            every,
            due: Instant::now() + every,
            waiting: false,
        }
    }

    /// Ping, or say the MQTT broker stopped answering.
    async fn ping(&mut self, conn: &mut Connection) -> Result<(), Failure> {
        if self.waiting {
            return Err(Failure::Passing("the MQTT broker stopped answering".to_owned()));
        }
        conn.send(&codec::pingreq()).await?;
        self.waiting = true;
        self.due = Instant::now() + self.every;
        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------------ reading

/// A message received, with the connection it came on: an acknowledgement is only sent on that one.
#[derive(Debug)]
pub(super) struct Received {
    pub publish: Publish,
    pub connection: u64,
}

#[derive(Debug)]
pub(super) enum Command {
    Ack { connection: u64, pkid: u16 },
    Stop,
}

/// Receive messages for `filters` until stopped. Each is handed on; it is acknowledged when its `Ack` comes.
pub(super) async fn subscriber(
    o: Options,
    filters: Vec<(String, u8)>,
    tx: mpsc::Sender<Result<Received, String>>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut failures = 0;
    let mut connection = 0u64;
    let mut subscribed = false;
    loop {
        let mut conn = match connect(&o, &mut failures).await {
            Ok(conn) => conn,
            Err(reason) => {
                let _ = tx.send(Err(reason)).await;
                return;
            }
        };
        connection += 1;
        // The first time always (the topics may have changed since the session began), then when it was lost.
        let mut result = Ok(());
        if !subscribed || !conn.session_present {
            match codec::subscribe(1, &filters) {
                Ok(packet) => result = conn.send(&packet).await,
                Err(e) => result = Err(Failure::Lasting(e)),
            }
        }
        if result.is_ok() {
            result = receive(
                &mut conn,
                connection,
                &o,
                &filters,
                &tx,
                &mut commands,
                &mut subscribed,
            )
            .await;
        }
        match result {
            Ok(()) => return,
            Err(Failure::Lasting(reason)) => {
                let _ = tx.send(Err(reason)).await;
                return;
            }
            Err(Failure::Passing(reason)) => {
                if let Err(reason) = wait_to_retry(&mut failures, reason).await {
                    let _ = tx
                        .send(Err(format!("cannot reach the MQTT broker: {reason}")))
                        .await;
                    return;
                }
            }
        }
    }
}

async fn receive(
    conn: &mut Connection,
    connection: u64,
    o: &Options,
    filters: &[(String, u8)],
    tx: &mpsc::Sender<Result<Received, String>>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    subscribed: &mut bool,
) -> Result<(), Failure> {
    let mut pinger = Pinger::new(o.keep_alive);
    loop {
        tokio::select! {
            packet = conn.read() => match packet? {
                Incoming::Publish(publish) => {
                    if publish.qos > 1 {
                        return Err(Failure::Lasting("the MQTT broker sent a QoS 2 message, which was not asked for".to_owned()));
                    }
                    // Not handed on means not acknowledged: the MQTT broker sends it again.
                    let _ = tx.send(Ok(Received { publish, connection })).await;
                }
                Incoming::SubAck { codes, .. } => {
                    if let Some(i) = codes.iter().position(|c| *c == codec::REFUSED) {
                        let filter = filters.get(i).map(|(f, _)| f.as_str()).unwrap_or_default();
                        return Err(Failure::Lasting(format!("the MQTT broker refused the subscription to {filter}")));
                    }
                    *subscribed = true;
                }
                Incoming::PingResp => pinger.waiting = false,
                Incoming::ConnAck { .. } | Incoming::PubAck(_) | Incoming::UnsubAck => {}
            },
            command = commands.recv() => match command {
                Some(Command::Ack { connection: c, pkid }) if c == connection => conn.send(&codec::puback(pkid)).await?,
                // From an earlier connection: the MQTT broker sends that message again.
                Some(Command::Ack { .. }) => {}
                Some(Command::Stop) | None => {
                    let _ = conn.send(&codec::disconnect()).await;
                    return Ok(());
                }
            },
            () = sleep_until(pinger.due) => pinger.ping(conn).await?,
        }
    }
}

// ------------------------------------------------------------------------------------------------------ writing

/// What the connection has heard back.
#[derive(Debug, Default)]
pub(super) struct Progress {
    pub acked: u64,
    pub failed: Option<String>,
}

#[derive(Debug)]
pub(super) enum Outbound {
    Publish(Publish),
    Stop,
}

/// Publish what comes, counting what the MQTT broker acknowledges. What was not acknowledged when a connection
/// broke is sent again on the next.
pub(super) async fn publisher(
    o: Options,
    mut requests: mpsc::Receiver<Outbound>,
    progress: watch::Sender<Progress>,
) {
    let mut failures = 0;
    let mut inflight: BTreeMap<u16, Publish> = BTreeMap::new();
    let mut next_pkid = 0u16;
    loop {
        let outcome = match connect(&o, &mut failures).await {
            Ok(mut conn) => {
                publish_on(
                    &mut conn,
                    &o,
                    &mut requests,
                    &progress,
                    &mut inflight,
                    &mut next_pkid,
                )
                .await
            }
            Err(reason) => Err(Failure::Lasting(reason)),
        };
        match outcome {
            Ok(()) => return,
            Err(Failure::Lasting(reason)) => {
                progress.send_modify(|p| p.failed = Some(reason));
                return;
            }
            Err(Failure::Passing(reason)) => {
                if let Err(reason) = wait_to_retry(&mut failures, reason).await {
                    progress
                        .send_modify(|p| p.failed = Some(format!("cannot reach the MQTT broker: {reason}")));
                    return;
                }
            }
        }
    }
}

async fn publish_on(
    conn: &mut Connection,
    o: &Options,
    requests: &mut mpsc::Receiver<Outbound>,
    progress: &watch::Sender<Progress>,
    inflight: &mut BTreeMap<u16, Publish>,
    next_pkid: &mut u16,
) -> Result<(), Failure> {
    for publish in inflight.values_mut() {
        publish.dup = true;
        let packet = codec::publish(publish).map_err(Failure::Lasting)?;
        conn.send(&packet).await?;
    }
    let mut pinger = Pinger::new(o.keep_alive);
    loop {
        tokio::select! {
            packet = conn.read() => match packet? {
                Incoming::PubAck(pkid) => {
                    if inflight.remove(&pkid).is_some() {
                        progress.send_modify(|p| p.acked += 1);
                    }
                }
                Incoming::PingResp => pinger.waiting = false,
                _ => {}
            },
            request = requests.recv(), if inflight.len() < MAX_INFLIGHT => match request {
                Some(Outbound::Publish(mut publish)) => {
                    let at_most_once = publish.qos == 0;
                    if !at_most_once {
                        publish.pkid = free_pkid(inflight, next_pkid);
                    }
                    let packet = codec::publish(&publish).map_err(Failure::Lasting)?;
                    if !at_most_once {
                        inflight.insert(publish.pkid, publish);
                    }
                    conn.send(&packet).await?;
                    // QoS 0 has no acknowledgement: a message is as delivered as it will be once it is sent.
                    if at_most_once {
                        progress.send_modify(|p| p.acked += 1);
                    }
                }
                Some(Outbound::Stop) | None => {
                    let _ = conn.send(&codec::disconnect()).await;
                    return Ok(());
                }
            },
            () = sleep_until(pinger.due) => pinger.ping(conn).await?,
        }
    }
}

/// A packet id not in use, 1 to 65,535.
fn free_pkid(inflight: &BTreeMap<u16, Publish>, next: &mut u16) -> u16 {
    loop {
        *next = next.checked_add(1).unwrap_or(1);
        if !inflight.contains_key(next) {
            return *next;
        }
    }
}
