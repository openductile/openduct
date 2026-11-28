// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A connection to MySQL in its client/server protocol: TCP, or TLS asked for in the handshake; signed in with
//! `caching_sha2_password` (MySQL 8's default) or `mysql_native_password`; then text queries, read a row at a
//! time, and the binary log. Every packet's size is checked before it is read, and reading can stop part way (a
//! pipeline stopping) without losing a packet.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::auth;

/// The largest message read: a row event holding more than this cannot be read.
const MAX_MESSAGE: usize = 256 * 1024 * 1024;
const CONNECT_WAIT: Duration = Duration::from_secs(10);
pub(super) const QUERY_WAIT: Duration = Duration::from_secs(60);
/// utf8mb4, with its default collation.
const UTF8MB4: u8 = 45;

const LONG_PASSWORD: u32 = 0x1;
const LONG_FLAG: u32 = 0x4;
const CONNECT_WITH_DB: u32 = 0x8;
const PROTOCOL_41: u32 = 0x200;
const SSL: u32 = 0x800;
const TRANSACTIONS: u32 = 0x2000;
const SECURE_CONNECTION: u32 = 0x8000;
const MULTI_RESULTS: u32 = 0x2_0000;
const PLUGIN_AUTH: u32 = 0x8_0000;
const PLUGIN_AUTH_LENENC_DATA: u32 = 0x20_0000;
pub(super) const DEPRECATE_EOF: u32 = 0x100_0000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Failure {
    /// Trying again will not help.
    Lasting(String),
    /// Trying again may.
    Passing(String),
}

impl Failure {
    pub(super) fn reason(self) -> String {
        match self {
            Self::Lasting(r) | Self::Passing(r) => r,
        }
    }
}

#[derive(Clone)]
pub(super) struct Options {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) database: Option<String>,
    pub(super) username: String,
    pub(super) password: Option<String>,
    pub(super) tls: Option<Arc<rustls::ClientConfig>>,
    /// The address to connect to, when the caller resolved the host to check where it leads. // sql databases
    pub(super) address: Option<std::net::SocketAddr>,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A column of a result: its name and its MySQL type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResultColumn {
    pub(super) name: String,
    pub(super) kind: u8,
    pub(super) flags: u16,
    pub(super) charset: u16,
}

pub(super) struct Connection {
    stream: Box<dyn Io>,
    buf: BytesMut,
    sequence: u8,
    /// `the MySQL server at host:port`, for messages.
    pub(super) what: String,
    pub(super) server_version: String,
    pub(super) capabilities: u32,
    /// The columns of a result still being read a row at a time.
    reading: Option<Vec<ResultColumn>>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("what", &self.what)
            .field("server_version", &self.server_version)
            .finish_non_exhaustive()
    }
}

/// The code of an error packet.
fn error_code(body: &[u8]) -> Option<u16> {
    match body {
        [0xff, a, b, ..] => Some(u16::from_le_bytes([*a, *b])),
        _ => None,
    }
}

/// An error packet's code and message, as words for a person.
pub(super) fn server_error(what: &str, mut body: Bytes) -> Failure {
    if body.remaining() < 3 {
        return Failure::Lasting(format!("{what} refused, without saying why"));
    }
    body.advance(1);
    let code = body.get_u16_le();
    if body.first() == Some(&b'#') && body.remaining() >= 6 {
        body.advance(6);
    }
    let message = String::from_utf8_lossy(&body).into_owned();
    let shown = format!("{what}: {message} (MySQL error {code})");
    match code {
        // Access denied: for the user, to the database, or a wrong password.
        1044 | 1045 | 1227 | 1698 => Failure::Lasting(shown),
        // Too many connections, the server shutting down, a lock wait, a deadlock: these pass.
        1040 | 1053 | 1205 | 1213 | 2013 => Failure::Passing(shown),
        _ => Failure::Lasting(shown),
    }
}

impl Connection {
    pub(super) async fn open(o: &Options) -> Result<Self, Failure> {
        let what = format!("the MySQL server at {}:{}", o.host, o.port);
        timeout(CONNECT_WAIT, Self::handshake(o, what.clone()))
            .await
            .map_err(|_| Failure::Passing(format!("{what} did not answer within 10 seconds")))?
    }

    async fn handshake(o: &Options, what: String) -> Result<Self, Failure> {
        // sql databases: the address the caller checked, when it gave one.
        let tcp = match o.address {
            Some(address) => TcpStream::connect(address).await,
            None => TcpStream::connect((o.host.as_str(), o.port)).await,
        }
        .map_err(|e| Failure::Passing(format!("cannot connect to {what}: {e}")))?;
        let _ = tcp.set_nodelay(true);
        let mut conn = Self {
            stream: Box::new(tcp),
            buf: BytesMut::with_capacity(16 * 1024),
            sequence: 0,
            what,
            server_version: String::new(),
            capabilities: 0,
            reading: None,
        };
        let mut greeting = conn.read_packet().await?;
        if greeting.first() == Some(&0xff) {
            return Err(server_error(&conn.what, greeting));
        }
        let hello = Greeting::parse(&mut greeting)
            .ok_or_else(|| Failure::Lasting(format!("{} did not greet as MySQL does", conn.what)))?;
        conn.server_version = hello.version.clone();
        let mut capabilities = LONG_PASSWORD
            | LONG_FLAG
            | PROTOCOL_41
            | TRANSACTIONS
            | SECURE_CONNECTION
            | MULTI_RESULTS
            | PLUGIN_AUTH
            | PLUGIN_AUTH_LENENC_DATA
            | DEPRECATE_EOF;
        if o.database.is_some() {
            capabilities |= CONNECT_WITH_DB;
        }
        capabilities &= hello.capabilities | CONNECT_WITH_DB;
        if capabilities & PROTOCOL_41 == 0 || capabilities & PLUGIN_AUTH == 0 {
            return Err(Failure::Lasting(format!(
                "{} is older than MySQL 5.7, which OpenDuct reads",
                conn.what
            )));
        }
        if let Some(config) = &o.tls {
            if hello.capabilities & SSL == 0 {
                return Err(Failure::Lasting(format!(
                    "{} does not take TLS connections",
                    conn.what
                )));
            }
            capabilities |= SSL;
            let mut request = Vec::with_capacity(32);
            request.put_u32_le(capabilities);
            request.put_u32_le(16 * 1024 * 1024);
            request.put_u8(UTF8MB4);
            request.extend_from_slice(&[0u8; 23]);
            conn.write_packet(&request).await?;
            let name = rustls::pki_types::ServerName::try_from(o.host.clone())
                .map_err(|_| Failure::Lasting(format!("{} is not a name TLS can check", o.host)))?;
            let plain = std::mem::replace(&mut conn.stream, Box::new(tokio::io::duplex(1).0));
            let tls = tokio_rustls::TlsConnector::from(Arc::clone(config))
                .connect(name, plain)
                .await
                .map_err(|e| match crate::tls::failure(&e, &conn.what) {
                    (true, reason) => Failure::Lasting(reason),
                    (false, reason) => Failure::Passing(reason),
                })?;
            conn.stream = Box::new(tls);
        }
        conn.capabilities = capabilities;
        let password = o.password.clone().unwrap_or_default();
        let mut plugin = hello.plugin.clone();
        let mut nonce = hello.nonce.clone();
        let first = scramble(&plugin, password.as_bytes(), &nonce)?;
        let mut response = Vec::with_capacity(128);
        response.put_u32_le(capabilities);
        response.put_u32_le(16 * 1024 * 1024);
        response.put_u8(UTF8MB4);
        response.extend_from_slice(&[0u8; 23]);
        response.extend_from_slice(o.username.as_bytes());
        response.push(0);
        put_lenenc_bytes(&mut response, &first);
        if let Some(db) = &o.database {
            response.extend_from_slice(db.as_bytes());
            response.push(0);
        }
        response.extend_from_slice(plugin.as_bytes());
        response.push(0);
        conn.write_packet(&response).await?;
        // The server says it is done, asks for another plugin, or (caching_sha2_password) asks for more.
        loop {
            let mut answer = conn.read_packet().await?;
            match answer.first().copied() {
                Some(0x00) => break,
                Some(0xff) if error_code(&answer) == Some(1045) => {
                    return Err(Failure::Lasting(format!(
                        "{} refused the username or password, or does not let this user connect from here",
                        conn.what
                    )));
                }
                Some(0xff) => return Err(server_error(&conn.what, answer)),
                Some(0xfe) => {
                    answer.advance(1);
                    plugin = take_cstring(&mut answer).unwrap_or_default();
                    nonce = answer.to_vec();
                    if nonce.last() == Some(&0) {
                        nonce.pop();
                    }
                    let again = scramble(&plugin, password.as_bytes(), &nonce)?;
                    conn.write_packet(&again).await?;
                }
                Some(0x01) if plugin == "caching_sha2_password" => {
                    match answer.get(1).copied() {
                        // Fast sign-in: the server had the password's hash; its OK follows.
                        Some(0x03) => {}
                        // Full sign-in: the password itself, over TLS, or encrypted to the server's key.
                        Some(0x04) => {
                            if o.tls.is_some() {
                                let mut clear = password.clone().into_bytes();
                                clear.push(0);
                                conn.write_packet(&clear).await?;
                            } else {
                                conn.write_packet(&[0x02]).await?;
                                let key = conn.read_packet().await?;
                                let key = key.get(1..).unwrap_or_default();
                                let encrypted = auth::encrypt_password(password.as_bytes(), &nonce, key)
                                    .map_err(|e| Failure::Lasting(format!("{}: {e}", conn.what)))?;
                                conn.write_packet(&encrypted).await?;
                            }
                        }
                        _ => {
                            return Err(Failure::Lasting(format!(
                                "{} answered the sign-in in a way not known",
                                conn.what
                            )));
                        }
                    }
                }
                _ => {
                    return Err(Failure::Lasting(format!(
                        "{} answered the sign-in in a way not known",
                        conn.what
                    )));
                }
            }
        }
        Ok(conn)
    }

    /// The next whole packet's payload (packets of 16 MiB - 1 bytes continue in the next). Safe to stop part way.
    pub(super) async fn read_packet(&mut self) -> Result<Bytes, Failure> {
        let mut whole = BytesMut::new();
        loop {
            let part = self.read_part().await?;
            let more = part.len() == 0xff_ffff;
            if whole.is_empty() && !more {
                return Ok(part);
            }
            if whole.len() + part.len() > MAX_MESSAGE {
                return Err(Failure::Lasting(format!(
                    "{} sent a message larger than {} MiB",
                    self.what,
                    MAX_MESSAGE / (1024 * 1024)
                )));
            }
            whole.extend_from_slice(&part);
            if !more {
                return Ok(whole.freeze());
            }
        }
    }

    async fn read_part(&mut self) -> Result<Bytes, Failure> {
        loop {
            if let Some(&[a, b, c, sequence]) = self.buf.get(..4).and_then(|h| <&[u8; 4]>::try_from(h).ok()) {
                let length = usize::from(a) | usize::from(b) << 8 | usize::from(c) << 16;
                if self.buf.len() >= 4 + length {
                    self.sequence = sequence.wrapping_add(1);
                    let mut packet = self.buf.split_to(4 + length);
                    packet.advance(4);
                    return Ok(packet.freeze());
                }
                self.buf.reserve(4 + length - self.buf.len());
            } else if self.buf.capacity() - self.buf.len() < 8 * 1024 {
                self.buf.reserve(64 * 1024);
            }
            match self.stream.read_buf(&mut self.buf).await {
                Ok(0) => return Err(Failure::Passing(format!("{} closed the connection", self.what))),
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

    async fn write_packet(&mut self, payload: &[u8]) -> Result<(), Failure> {
        let mut out = Vec::with_capacity(payload.len() + 4);
        for chunk in payload
            .chunks(0xff_ffff)
            .chain(if payload.len().is_multiple_of(0xff_ffff) {
                Some(&[][..])
            } else {
                None
            })
        {
            let length = chunk.len();
            out.extend_from_slice(&[
                length as u8,
                (length >> 8) as u8,
                (length >> 16) as u8,
                self.sequence,
            ]);
            out.extend_from_slice(chunk);
            self.sequence = self.sequence.wrapping_add(1);
        }
        let what = &self.what;
        self.stream
            .write_all(&out)
            .await
            .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))?;
        self.stream
            .flush()
            .await
            .map_err(|e| Failure::Passing(format!("the connection to {what} broke: {e}")))
    }

    /// Send a command (its first byte is the command).
    pub(super) async fn command(&mut self, payload: &[u8]) -> Result<(), Failure> {
        self.sequence = 0;
        self.write_packet(payload).await
    }

    /// Run a statement and read every row of what it gives back. For small answers.
    pub(super) async fn query(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<ResultColumn>, Vec<Vec<Option<Bytes>>>), Failure> {
        let what = self.what.clone();
        timeout(QUERY_WAIT, async {
            let columns = self.start_query(sql).await?;
            let mut rows = Vec::new();
            while let Some(row) = self.next_row().await? {
                rows.push(row);
            }
            Ok((columns, rows))
        })
        .await
        .map_err(|_| Failure::Passing(format!("{what} did not answer a query within {QUERY_WAIT:?}")))?
    }

    /// Run a statement; its rows are then read with [`Connection::next_row`], one at a time.
    pub(super) async fn start_query(&mut self, sql: &str) -> Result<Vec<ResultColumn>, Failure> {
        if self.reading.is_some() {
            while self.next_row().await?.is_some() {}
        }
        let mut payload = vec![0x03];
        payload.extend_from_slice(sql.as_bytes());
        self.command(&payload).await?;
        let mut first = self.read_packet().await?;
        match first.first().copied() {
            Some(0x00) => return Ok(Vec::new()),
            Some(0xff) => return Err(server_error(&self.what, first)),
            _ => {}
        }
        let count = take_lenenc(&mut first).ok_or_else(|| self.not_understood())?;
        if count > 4096 {
            return Err(self.not_understood());
        }
        let mut columns = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for _ in 0..count {
            let mut definition = self.read_packet().await?;
            columns.push(column_definition(&mut definition).ok_or_else(|| self.not_understood())?);
        }
        if self.capabilities & DEPRECATE_EOF == 0 {
            let _eof = self.read_packet().await?;
        }
        self.reading = Some(columns.clone());
        Ok(columns)
    }

    /// The next row of the result being read, each value as the server's text or null; `None` at the end.
    pub(super) async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, Failure> {
        let Some(count) = self.reading.as_ref().map(Vec::len) else {
            return Ok(None);
        };
        let mut packet = self.read_packet().await?;
        match packet.first().copied() {
            Some(0xfe) if packet.len() < 0xff_ffff => {
                self.reading = None;
                return Ok(None);
            }
            Some(0xff) => {
                self.reading = None;
                return Err(server_error(&self.what, packet));
            }
            _ => {}
        }
        let mut row = Vec::with_capacity(count);
        for _ in 0..count {
            if packet.first() == Some(&0xfb) {
                packet.advance(1);
                row.push(None);
            } else {
                row.push(Some(
                    take_lenenc_bytes(&mut packet).ok_or_else(|| self.not_understood())?,
                ));
            }
        }
        Ok(Some(row))
    }

    /// Ask for the binary log from `file` at `position`, as a replica numbered `server_id` would.
    pub(super) async fn dump_binlog(
        &mut self,
        file: &str,
        position: u32,
        server_id: u32,
    ) -> Result<(), Failure> {
        let mut payload = vec![0x12];
        payload.put_u32_le(position);
        payload.put_u16_le(0);
        payload.put_u32_le(server_id);
        payload.extend_from_slice(file.as_bytes());
        self.command(&payload).await
    }

    /// The next binary log event (without the stream's leading OK byte); `None` when the server ends the stream.
    pub(super) async fn next_event(&mut self) -> Result<Option<Bytes>, Failure> {
        let mut packet = self.read_packet().await?;
        match packet.first().copied() {
            Some(0x00) => {
                packet.advance(1);
                Ok(Some(packet))
            }
            Some(0xfe) if packet.len() < 9 => Ok(None),
            // The binary log the position is in has been removed (the server's binlog retention).
            Some(0xff) if error_code(&packet) == Some(1236) => Err(Failure::Lasting(format!(
                "{}: the binary log the pipeline had reached is gone, so the changes in it are lost to it (a new \
                 pipeline name starts again from the tables as they are)",
                server_error(&self.what, packet).reason()
            ))),
            Some(0xff) => Err(server_error(&self.what, packet)),
            _ => Err(self.not_understood()),
        }
    }

    pub(super) fn not_understood(&self) -> Failure {
        Failure::Lasting(format!("{} sent something that is not understood", self.what))
    }
}

/// The server's first packet.
#[derive(Debug)]
struct Greeting {
    version: String,
    capabilities: u32,
    nonce: Vec<u8>,
    plugin: String,
}

impl Greeting {
    fn parse(b: &mut Bytes) -> Option<Self> {
        if b.get_u8_checked()? != 10 {
            return None;
        }
        let version = take_cstring(b)?;
        if b.remaining() < 4 + 8 + 1 + 2 {
            return None;
        }
        let _connection = b.get_u32_le();
        let mut nonce = b.split_to(8).to_vec();
        b.advance(1);
        let low = u32::from(b.get_u16_le());
        if b.remaining() < 1 + 2 + 2 + 1 + 10 {
            return None;
        }
        let _charset = b.get_u8();
        let _status = b.get_u16_le();
        let high = u32::from(b.get_u16_le());
        let capabilities = low | high << 16;
        let nonce_length = usize::from(b.get_u8());
        b.advance(10);
        if capabilities & SECURE_CONNECTION != 0 {
            let more = nonce_length.saturating_sub(8).max(13);
            if b.remaining() < more {
                return None;
            }
            let rest = b.split_to(more);
            nonce.extend_from_slice(rest.get(..more - 1).unwrap_or_default());
        }
        let plugin = if capabilities & PLUGIN_AUTH != 0 {
            take_cstring(b).unwrap_or_else(|| String::from_utf8_lossy(b).into_owned())
        } else {
            "mysql_native_password".to_owned()
        };
        Some(Self {
            version,
            capabilities,
            nonce,
            plugin,
        })
    }
}

trait CheckedBuf {
    fn get_u8_checked(&mut self) -> Option<u8>;
}

impl CheckedBuf for Bytes {
    fn get_u8_checked(&mut self) -> Option<u8> {
        self.has_remaining().then(|| self.get_u8())
    }
}

fn scramble(plugin: &str, password: &[u8], nonce: &[u8]) -> Result<Vec<u8>, Failure> {
    match plugin {
        "caching_sha2_password" => Ok(auth::caching_sha2(password, nonce)),
        "mysql_native_password" => Ok(auth::native(password, nonce)),
        other => Err(Failure::Lasting(format!(
            "the MySQL server asks to sign in with {other}, which OpenDuct does not use: give the user \
             caching_sha2_password or mysql_native_password"
        ))),
    }
}

fn column_definition(b: &mut Bytes) -> Option<ResultColumn> {
    for _ in 0..4 {
        take_lenenc_bytes(b)?;
    }
    let name = String::from_utf8_lossy(&take_lenenc_bytes(b)?).into_owned();
    take_lenenc_bytes(b)?;
    take_lenenc(b)?;
    if b.remaining() < 2 + 4 + 1 + 2 {
        return None;
    }
    let charset = b.get_u16_le();
    let _length = b.get_u32_le();
    let kind = b.get_u8();
    let flags = b.get_u16_le();
    Some(ResultColumn {
        name,
        kind,
        flags,
        charset,
    })
}

/// A length-encoded integer.
pub(super) fn take_lenenc(b: &mut Bytes) -> Option<u64> {
    let first = b.get_u8_checked()?;
    let size = match first {
        0..=0xfa => return Some(u64::from(first)),
        0xfc => 2,
        0xfd => 3,
        0xfe => 8,
        _ => return None,
    };
    if b.remaining() < size {
        return None;
    }
    let mut n = 0u64;
    for i in 0..size {
        n |= u64::from(b.get_u8()) << (8 * i);
    }
    Some(n)
}

/// A length-encoded string, refused when longer than what is there.
pub(super) fn take_lenenc_bytes(b: &mut Bytes) -> Option<Bytes> {
    let length = usize::try_from(take_lenenc(b)?).ok()?;
    (b.remaining() >= length).then(|| b.split_to(length))
}

pub(super) fn put_lenenc_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let n = bytes.len();
    if n < 251 {
        out.push(n as u8);
    } else if n < 1 << 16 {
        out.push(0xfc);
        out.put_u16_le(n as u16);
    } else {
        out.push(0xfd);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    }
    out.extend_from_slice(bytes);
}

pub(super) fn take_cstring(b: &mut Bytes) -> Option<String> {
    let end = b.iter().position(|x| *x == 0)?;
    let text = String::from_utf8_lossy(b.get(..end)?).into_owned();
    b.advance(end + 1);
    Some(text)
}

/// `` `name` ``, with any `` ` `` doubled: an identifier as MySQL takes it.
pub(super) fn quote_name(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A MySQL 8.4 greeting, as the server sends it.
    fn greeting() -> Bytes {
        let mut g = vec![10];
        g.extend_from_slice(b"8.4.11\0");
        g.put_u32_le(42);
        g.extend_from_slice(b"abcdefgh");
        g.push(0);
        g.put_u16_le(0xffff);
        g.push(255);
        g.put_u16_le(2);
        g.put_u16_le(0xdfff);
        g.push(21);
        g.extend_from_slice(&[0u8; 10]);
        g.extend_from_slice(b"ijklmnopqrst\0");
        g.extend_from_slice(b"caching_sha2_password\0");
        Bytes::from(g)
    }

    #[test]
    fn the_greeting_is_read() {
        let hello = Greeting::parse(&mut greeting()).unwrap();
        assert_eq!(hello.version, "8.4.11");
        assert_eq!(hello.nonce, b"abcdefghijklmnopqrst");
        assert_eq!(hello.plugin, "caching_sha2_password");
        assert!(hello.capabilities & PLUGIN_AUTH != 0);
        // Cut short anywhere, it is refused rather than misread.
        let full = greeting();
        for cut in 0..full.len() - 23 {
            assert!(Greeting::parse(&mut full.slice(..cut)).is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn lengths_are_read_and_impossible_ones_refused() {
        assert_eq!(take_lenenc(&mut Bytes::from_static(&[0xfa])), Some(250));
        assert_eq!(
            take_lenenc(&mut Bytes::from_static(&[0xfc, 0x01, 0x02])),
            Some(0x0201)
        );
        assert_eq!(
            take_lenenc(&mut Bytes::from_static(&[0xfe, 1, 0, 0, 0, 0, 0, 0, 0])),
            Some(1)
        );
        assert_eq!(take_lenenc(&mut Bytes::from_static(&[0xfc, 0x01])), None);
        assert_eq!(take_lenenc(&mut Bytes::from_static(&[0xff])), None);
        assert!(take_lenenc_bytes(&mut Bytes::from_static(&[0x05, b'a'])).is_none());
        let mut out = Vec::new();
        put_lenenc_bytes(&mut out, &[7u8; 300]);
        assert_eq!(take_lenenc_bytes(&mut Bytes::from(out)).unwrap().len(), 300);
        assert_eq!(quote_name("we`ird"), "`we``ird`");
    }

    #[test]
    fn errors_are_words_and_say_whether_they_pass() {
        let mut e = vec![0xff];
        e.put_u16_le(1045);
        e.extend_from_slice(b"#28000Access denied for user 'x'");
        assert!(
            matches!(server_error("the server", Bytes::from(e)), Failure::Lasting(m) if m.contains("Access denied") && m.contains("1045"))
        );
        let mut e = vec![0xff];
        e.put_u16_le(1040);
        e.extend_from_slice(b"Too many connections");
        assert!(matches!(server_error("s", Bytes::from(e)), Failure::Passing(_)));
    }
}
