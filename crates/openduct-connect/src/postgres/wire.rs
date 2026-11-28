// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A connection to PostgreSQL in its wire protocol (version 3.0): TCP, or TLS after asking for it; signed in with
//! SCRAM-SHA-256, MD5, or a password in clear over TLS only; then simple queries and the replication stream. The
//! size of every message is checked before it is read, and reading can stop part way (a pipeline stopping) without
//! losing a message.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::scram::{Hash, Scram};

/// The largest message read. A row holding more than this cannot be read.
pub(super) const MAX_MESSAGE: usize = 128 * 1024 * 1024;
const CONNECT_WAIT: Duration = Duration::from_secs(10);
/// How long a query may take.
pub(super) const QUERY_WAIT: Duration = Duration::from_secs(60);
const PROTOCOL: i32 = 196_608;
const SSL_REQUEST: i32 = 80_877_103;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Failure {
    /// Trying again will not help: a refused password, a table that is not there.
    Lasting(String),
    /// Trying again may: a broken connection, a server starting up.
    Passing(String),
}

impl Failure {
    pub(super) fn reason(self) -> String {
        match self {
            Self::Lasting(r) | Self::Passing(r) => r,
        }
    }
}

/// An error the server sent: its SQLSTATE code and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServerError {
    pub(super) code: String,
    pub(super) message: String,
    pub(super) hint: Option<String>,
}

impl ServerError {
    fn parse(mut body: &[u8]) -> Self {
        let mut error = Self {
            code: String::new(),
            message: String::new(),
            hint: None,
        };
        while let Some((&field, rest)) = body.split_first() {
            if field == 0 {
                break;
            }
            let Some(end) = rest.iter().position(|b| *b == 0) else {
                break;
            };
            let text = String::from_utf8_lossy(rest.get(..end).unwrap_or_default()).into_owned();
            match field {
                b'C' => error.code = text,
                b'M' => error.message = text,
                b'H' => error.hint = Some(text),
                _ => {}
            }
            body = rest.get(end + 1..).unwrap_or_default();
        }
        error
    }

    pub(super) fn shown(&self) -> String {
        match &self.hint {
            Some(hint) => format!("{} (SQLSTATE {}; {hint})", self.message, self.code),
            None => format!("{} (SQLSTATE {})", self.message, self.code),
        }
    }

    /// Whether trying again may help: the server busy, starting, or shutting down.
    fn passing(&self) -> bool {
        matches!(
            self.code.as_str(),
            "53300" | "57P01" | "57P02" | "57P03" | "08006" | "40001" | "40P01"
        )
    }
}

/// How to reach the database and sign in.
#[derive(Clone)]
pub(super) struct Options {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) database: String,
    pub(super) username: String,
    pub(super) password: Option<String>,
    pub(super) tls: Option<Arc<rustls::ClientConfig>>,
    /// The address to connect to, when the caller resolved `host` itself (to check where it leads): TLS still
    /// checks the certificate against `host`.
    pub(super) address: Option<std::net::SocketAddr>,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("username", &self.username)
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A result set: the columns (name and type) and the rows, each value as the server's text, or null.
#[derive(Debug, Default, Clone)]
pub(super) struct Rows {
    pub(super) columns: Vec<(String, u32)>,
    pub(super) rows: Vec<Vec<Option<Bytes>>>,
    /// What the server said it did (`INSERT 0 3`, `SELECT 5`).
    pub(super) tag: Option<String>,
}

impl Rows {
    /// The first row's value of `column`, as text.
    pub(super) fn first(&self, column: &str) -> Option<String> {
        let at = self.columns.iter().position(|(n, _)| n == column)?;
        self.rows
            .first()
            .and_then(|r| r.get(at))
            .and_then(Option::as_ref)
            .map(|v| String::from_utf8_lossy(v).into_owned())
    }
}

pub(super) struct Connection {
    stream: Box<dyn Io>,
    buf: BytesMut,
    /// `the PostgreSQL server at host:port`, for messages.
    pub(super) what: String,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("what", &self.what)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Connect and sign in; with `replication`, as a replication connection that also takes SQL.
    pub(super) async fn open(o: &Options, replication: bool) -> Result<Self, Failure> {
        let what = format!("the PostgreSQL server at {}:{}", o.host, o.port);
        timeout(CONNECT_WAIT, Self::handshake(o, replication, what.clone()))
            .await
            .map_err(|_| Failure::Passing(format!("{what} did not answer within 10 seconds")))?
    }

    async fn handshake(o: &Options, replication: bool, what: String) -> Result<Self, Failure> {
        let connected = match o.address {
            Some(address) => TcpStream::connect(address).await,
            None => TcpStream::connect((o.host.as_str(), o.port)).await,
        };
        let mut tcp = connected.map_err(|e| Failure::Passing(format!("cannot connect to {what}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn Io> = match &o.tls {
            None => Box::new(tcp),
            Some(config) => {
                let mut request = Vec::with_capacity(8);
                request.put_i32(8);
                request.put_i32(SSL_REQUEST);
                tcp.write_all(&request)
                    .await
                    .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))?;
                let mut answer = [0u8; 1];
                tcp.read_exact(&mut answer)
                    .await
                    .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))?;
                match answer {
                    [b'S'] => {}
                    [b'N'] => return Err(Failure::Lasting(format!("{what} does not take TLS connections"))),
                    _ => {
                        return Err(Failure::Lasting(format!(
                            "{what} did not answer as PostgreSQL does"
                        )));
                    }
                }
                let name = rustls::pki_types::ServerName::try_from(o.host.clone())
                    .map_err(|_| Failure::Lasting(format!("{} is not a name TLS can check", o.host)))?;
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
            buf: BytesMut::with_capacity(16 * 1024),
            what,
        };
        conn.send(&startup(o, replication)).await?;
        conn.sign_in(o).await?;
        Ok(conn)
    }

    async fn sign_in(&mut self, o: &Options) -> Result<(), Failure> {
        let mut scram: Option<Scram> = None;
        let what = self.what.clone();
        let password = || {
            o.password
                .clone()
                .ok_or_else(|| Failure::Lasting(format!("{what} asks for a password, and none is given")))
        };
        loop {
            let (kind, mut body) = self.read().await?;
            match kind {
                b'R' => {
                    let code = take_i32(&mut body).ok_or_else(|| self.not_understood())?;
                    match code {
                        0 => {}
                        3 => {
                            if o.tls.is_none() {
                                return Err(Failure::Lasting(format!(
                                    "{} asks for the password in clear on a connection without TLS: set tls: true",
                                    self.what
                                )));
                            }
                            let mut message = password()?.into_bytes();
                            message.push(0);
                            self.send(&frame(b'p', &message)).await?;
                        }
                        5 => {
                            let salt: [u8; 4] = body
                                .get(..4)
                                .and_then(|s| s.try_into().ok())
                                .ok_or_else(|| self.not_understood())?;
                            let mut message = md5_password(&o.username, &password()?, salt).into_bytes();
                            message.push(0);
                            self.send(&frame(b'p', &message)).await?;
                        }
                        10 => {
                            let offered = cstrings(&body);
                            if !offered.iter().any(|m| m == "SCRAM-SHA-256") {
                                return Err(Failure::Lasting(format!(
                                    "{} offers only {} for signing in, which OpenDuct does not use",
                                    self.what,
                                    offered.join(", ")
                                )));
                            }
                            let nonce = Scram::nonce().map_err(Failure::Lasting)?;
                            // The server takes the user from the start of the connection, not from SCRAM.
                            let s = Scram::new(Hash::Sha256, "", &password()?, nonce);
                            let first = s.client_first();
                            let mut message = b"SCRAM-SHA-256\0".to_vec();
                            message.put_i32(i32::try_from(first.len()).unwrap_or(i32::MAX));
                            message.extend_from_slice(&first);
                            self.send(&frame(b'p', &message)).await?;
                            scram = Some(s);
                        }
                        11 => {
                            let s = scram.as_mut().ok_or_else(|| self.not_understood())?;
                            let proof = s
                                .client_final(&body)
                                .map_err(|e| Failure::Lasting(format!("{} {e}", self.what)))?;
                            self.send(&frame(b'p', &proof)).await?;
                        }
                        12 => {
                            let s = scram.as_ref().ok_or_else(|| self.not_understood())?;
                            s.verify(&body)
                                .map_err(|e| Failure::Lasting(format!("{} {e}", self.what)))?;
                        }
                        other => {
                            return Err(Failure::Lasting(format!(
                                "{} asks to sign in a way OpenDuct does not use (method {other}): give the user a \
                                 password stored as SCRAM-SHA-256",
                                self.what
                            )));
                        }
                    }
                }
                b'E' => return Err(self.sign_in_refused(&ServerError::parse(&body))),
                b'Z' => return Ok(()),
                _ => {}
            }
        }
    }

    fn sign_in_refused(&self, e: &ServerError) -> Failure {
        match e.code.as_str() {
            "28P01" | "28000" => Failure::Lasting(format!(
                "{} refused the username or password, or does not let this user connect from here ({})",
                self.what, e.code
            )),
            "3D000" => Failure::Lasting(format!("{}: {}", self.what, e.message)),
            _ if e.passing() => Failure::Passing(format!("{}: {}", self.what, e.shown())),
            _ => Failure::Lasting(format!("{}: {}", self.what, e.shown())),
        }
    }

    fn not_understood(&self) -> Failure {
        Failure::Lasting(format!("{} sent a message that is not understood", self.what))
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        let what = &self.what;
        self.stream
            .write_all(bytes)
            .await
            .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))?;
        self.stream
            .flush()
            .await
            .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))
    }

    /// The next whole message: its type and body. Safe to stop part way: what was read stays for the next call.
    pub(super) async fn read(&mut self) -> Result<(u8, Bytes), Failure> {
        loop {
            if let Some(message) = split_message(&mut self.buf)
                .map_err(|why| Failure::Lasting(format!("{} sent {why}", self.what)))?
            {
                return Ok(message);
            }
            if self.buf.capacity() - self.buf.len() < 8 * 1024 {
                self.buf.reserve(64 * 1024);
            }
            match self.stream.read_buf(&mut self.buf).await {
                Ok(0) => {
                    return Err(Failure::Passing(format!("{} closed the connection", self.what)));
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(Failure::Passing(format!(
                        "the connection to {} broke: {e}",
                        self.what
                    )));
                }
            }
        }
    }

    /// Run SQL with the simple query protocol and gather what it gives back. For small answers: a query giving
    /// many rows is read with a cursor, a part at a time.
    pub(super) async fn query(&mut self, sql: &str) -> Result<Rows, Failure> {
        let mut text = sql.as_bytes().to_vec();
        text.push(0);
        self.send(&frame(b'Q', &text)).await?;
        let what = self.what.clone();
        timeout(QUERY_WAIT, self.gather())
            .await
            .map_err(|_| Failure::Passing(format!("{what} did not answer a query within {QUERY_WAIT:?}")))?
    }

    async fn gather(&mut self) -> Result<Rows, Failure> {
        let mut rows = Rows::default();
        let mut error: Option<ServerError> = None;
        loop {
            let (kind, mut body) = self.read().await?;
            match kind {
                b'T' => rows.columns = row_description(&mut body).ok_or_else(|| self.not_understood())?,
                b'D' => rows
                    .rows
                    .push(data_row(&mut body).ok_or_else(|| self.not_understood())?),
                b'E' => error = Some(ServerError::parse(&body)),
                b'C' => rows.tag = take_cstring(&mut body),
                b'Z' => break,
                _ => {}
            }
        }
        match error {
            None => Ok(rows),
            Some(e) if e.passing() => Err(Failure::Passing(format!("{}: {}", self.what, e.shown()))),
            Some(e) => Err(Failure::Lasting(format!("{}: {}", self.what, e.shown()))),
        }
    }

    /// Run SQL with the extended query protocol, its parameters (`$1`, `$2` ...) and its results all as text: the
    /// server reads each parameter as the type its place calls for (`$1::vector` takes `[0.1,0.2]`), so no value is
    /// ever written into the SQL itself.
    pub(super) async fn query_with(&mut self, sql: &str, params: &[Option<String>]) -> Result<Rows, Failure> {
        self.query_with_limit(sql, params, 0).await
    }

    /// As [`Connection::query_with`], the server sending at most `limit` rows (0: all). // sql databases
    pub(super) async fn query_with_limit(
        &mut self,
        sql: &str,
        params: &[Option<String>],
        limit: i32,
    ) -> Result<Rows, Failure> {
        let mut parse = Vec::with_capacity(sql.len() + 8);
        parse.push(0);
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0i16.to_be_bytes());
        let count =
            i16::try_from(params.len()).map_err(|_| Failure::Lasting("too many parameters".to_owned()))?;
        let mut bind = vec![0, 0];
        bind.extend_from_slice(&0i16.to_be_bytes());
        bind.extend_from_slice(&count.to_be_bytes());
        for param in params {
            match param {
                None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(text) => {
                    let len = i32::try_from(text.len())
                        .map_err(|_| Failure::Lasting("a parameter too large".to_owned()))?;
                    bind.extend_from_slice(&len.to_be_bytes());
                    bind.extend_from_slice(text.as_bytes());
                }
            }
        }
        bind.extend_from_slice(&0i16.to_be_bytes());
        let mut out = frame(b'P', &parse);
        out.extend_from_slice(&frame(b'B', &bind));
        out.extend_from_slice(&frame(b'D', b"P\0"));
        let mut execute = vec![0];
        execute.extend_from_slice(&limit.to_be_bytes());
        out.extend_from_slice(&frame(b'E', &execute));
        out.extend_from_slice(&frame(b'S', &[]));
        self.send(&out).await?;
        let what = self.what.clone();
        timeout(QUERY_WAIT, self.gather())
            .await
            .map_err(|_| Failure::Passing(format!("{what} did not answer a query within {QUERY_WAIT:?}")))?
    }

    /// Start the replication stream (`START_REPLICATION ...`): from here, [`Connection::copy_data`] reads it.
    pub(super) async fn start_streaming(&mut self, command: &str) -> Result<(), Failure> {
        let mut text = command.as_bytes().to_vec();
        text.push(0);
        self.send(&frame(b'Q', &text)).await?;
        loop {
            let (kind, body) = self.read().await?;
            match kind {
                b'W' => return Ok(()),
                b'E' => {
                    let e = ServerError::parse(&body);
                    return Err(if e.code == "55006" {
                        Failure::Passing(format!("{}: {}", self.what, e.shown()))
                    } else {
                        Failure::Lasting(format!("{}: {}", self.what, e.shown()))
                    });
                }
                _ => {}
            }
        }
    }

    /// The next message of the replication stream; `None` when the server ends it.
    pub(super) async fn copy_data(&mut self) -> Result<Option<Bytes>, Failure> {
        loop {
            let (kind, body) = self.read().await?;
            match kind {
                b'd' => return Ok(Some(body)),
                b'c' => return Ok(None),
                b'E' => {
                    let e = ServerError::parse(&body);
                    return Err(Failure::Passing(format!("{}: {}", self.what, e.shown())));
                }
                _ => {}
            }
        }
    }

    /// Send a message on the replication stream (a standby status update).
    pub(super) async fn send_copy_data(&mut self, data: &[u8]) -> Result<(), Failure> {
        self.send(&frame(b'd', data)).await
    }
}

/// `[type][length: i32][body]`, the length counting itself.
fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(kind);
    out.put_i32(i32::try_from(body.len() + 4).unwrap_or(i32::MAX));
    out.extend_from_slice(body);
    out
}

fn startup(o: &Options, replication: bool) -> Vec<u8> {
    let mut body = Vec::new();
    body.put_i32(PROTOCOL);
    let mut parameters = vec![
        ("user", o.username.as_str()),
        ("database", o.database.as_str()),
        ("application_name", "openduct"),
        ("client_encoding", "UTF8"),
    ];
    if replication {
        parameters.push(("replication", "database"));
    }
    for (name, value) in parameters {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = Vec::with_capacity(body.len() + 4);
    out.put_i32(i32::try_from(body.len() + 4).unwrap_or(i32::MAX));
    out.extend_from_slice(&body);
    out
}

/// `md5` followed by md5(md5(password + user) + salt), in hex, as PostgreSQL's MD5 sign-in asks.
fn md5_password(user: &str, password: &str, salt: [u8; 4]) -> String {
    use md5::{Digest, Md5};
    let inner = hex(&Md5::digest([password.as_bytes(), user.as_bytes()].concat()));
    let outer = hex(&Md5::digest([inner.as_bytes(), &salt].concat()));
    format!("md5{outer}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A whole message off the front of `buf`, if one has arrived.
fn split_message(buf: &mut BytesMut) -> Result<Option<(u8, Bytes)>, String> {
    let Some(&[kind, a, b, c, d]) = buf.get(..5).and_then(|h| <&[u8; 5]>::try_from(h).ok()) else {
        return Ok(None);
    };
    let length = i32::from_be_bytes([a, b, c, d]);
    let length = usize::try_from(length)
        .ok()
        .filter(|n| (4..=MAX_MESSAGE).contains(n))
        .ok_or_else(|| format!("a message claiming {length} bytes, which is not one PostgreSQL sends"))?;
    if buf.len() < length + 1 {
        buf.reserve(length + 1 - buf.len());
        return Ok(None);
    }
    let mut message = buf.split_to(length + 1).freeze();
    message.advance(5);
    Ok(Some((kind, message)))
}

pub(super) fn take_i32(body: &mut Bytes) -> Option<i32> {
    (body.remaining() >= 4).then(|| body.get_i32())
}

fn take_i16(body: &mut Bytes) -> Option<i16> {
    (body.remaining() >= 2).then(|| body.get_i16())
}

/// A zero-ended text off the front of `body`.
pub(super) fn take_cstring(body: &mut Bytes) -> Option<String> {
    let end = body.iter().position(|b| *b == 0)?;
    let text = String::from_utf8_lossy(body.get(..end)?).into_owned();
    body.advance(end + 1);
    Some(text)
}

fn cstrings(body: &[u8]) -> Vec<String> {
    body.split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect()
}

fn row_description(body: &mut Bytes) -> Option<Vec<(String, u32)>> {
    let count = usize::try_from(take_i16(body)?).ok()?;
    let mut columns = Vec::with_capacity(count.min(body.remaining()));
    for _ in 0..count {
        let name = take_cstring(body)?;
        if body.remaining() < 18 {
            return None;
        }
        let _table = body.get_u32();
        let _column = body.get_i16();
        let type_oid = body.get_u32();
        body.advance(8);
        columns.push((name, type_oid));
    }
    Some(columns)
}

fn data_row(body: &mut Bytes) -> Option<Vec<Option<Bytes>>> {
    let count = usize::try_from(take_i16(body)?).ok()?;
    let mut values = Vec::with_capacity(count.min(body.remaining()));
    for _ in 0..count {
        let length = take_i32(body)?;
        if length < 0 {
            values.push(None);
            continue;
        }
        let length = usize::try_from(length).ok()?;
        if body.remaining() < length {
            return None;
        }
        values.push(Some(body.split_to(length)));
    }
    Some(values)
}

/// `"name"`, with any `"` doubled: an identifier as SQL takes it.
pub(super) fn quote_name(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `'text'`, with any `'` doubled: a literal as SQL takes it.
pub(super) fn quote_text(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn md5_sign_in_is_what_postgresql_expects() {
        // Worked out apart from this code: "md5" + hex(md5(hex(md5("secret" + "shop")) + [1, 2, 3, 4])).
        assert_eq!(
            md5_password("shop", "secret", [1, 2, 3, 4]),
            "md509c7baab7568cd5d6bd39754f079adc5"
        );
    }

    #[test]
    fn messages_are_split_when_whole_and_refused_when_impossible() {
        let mut buf = BytesMut::from(&b"Z\x00\x00\x00\x05IT\x00\x00"[..]);
        assert_eq!(
            split_message(&mut buf).unwrap(),
            Some((b'Z', Bytes::from_static(b"I")))
        );
        // Part of the next one: wait for the rest.
        assert_eq!(split_message(&mut buf).unwrap(), None);
        let mut huge = BytesMut::from(&b"D\x7f\xff\xff\xff"[..]);
        assert!(
            split_message(&mut huge)
                .unwrap_err()
                .contains("not one PostgreSQL sends")
        );
        let mut short = BytesMut::from(&b"D\x00\x00\x00\x02"[..]);
        assert!(split_message(&mut short).is_err());
    }

    #[test]
    fn errors_and_rows_are_read() {
        let e = ServerError::parse(b"SERROR\0C42P01\0Mrelation \"x\" does not exist\0HCreate it first.\0\0");
        assert_eq!(e.code, "42P01");
        assert_eq!(
            e.shown(),
            "relation \"x\" does not exist (SQLSTATE 42P01; Create it first.)"
        );
        let mut row = Bytes::from_static(b"\x00\x02\x00\x00\x00\x0242\xff\xff\xff\xff");
        assert_eq!(
            data_row(&mut row).unwrap(),
            vec![Some(Bytes::from_static(b"42")), None]
        );
        // A value longer than the row, and a count of columns the row does not have.
        assert!(data_row(&mut Bytes::from_static(b"\x00\x01\x00\x00\x00\x09ab")).is_none());
        assert!(data_row(&mut Bytes::from_static(b"\x7f\xff")).is_none());
        assert_eq!(quote_name("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_text("o'brien"), "'o''brien'");
    }

    #[test]
    fn the_start_names_the_user_database_and_replication() {
        let o = Options {
            host: "db".to_owned(),
            port: 5432,
            database: "shop".to_owned(),
            username: "reader".to_owned(),
            password: None,
            tls: None,
            address: None,
        };
        let bytes = startup(&o, true);
        assert_eq!(
            i32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len()
        );
        let text = String::from_utf8_lossy(&bytes[8..]).into_owned();
        assert!(
            text.contains("user\0reader\0database\0shop\0") && text.ends_with("replication\0database\0\0")
        );
    }
}
