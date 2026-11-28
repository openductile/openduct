// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A connection to SQL Server in its own protocol (TDS 7.4, and TDS 8 with `tls: strict`): messages cut into
//! packets; PRELOGIN, which agrees on TLS; TLS itself, carried inside PRELOGIN packets until it is set up (and
//! around everything after); LOGIN7, signing in with SQL Server's own user and password. Every packet's length is
//! checked before it is read, and a whole answer is bounded.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// Message types.
pub(super) const SQL_BATCH: u8 = 0x01;
pub(super) const RPC: u8 = 0x03;
const REPLY: u8 = 0x04;
const ATTENTION: u8 = 0x06;
const LOGIN7: u8 = 0x10;
const PRELOGIN: u8 = 0x12;
/// The last packet of a message.
const END: u8 = 0x01;
const HEADER: usize = 8;
/// The packet size asked for, and the largest one taken from the server.
const PACKET: usize = 4096;
const LARGEST_PACKET: usize = 32_767;
const CONNECT_WAIT: Duration = Duration::from_secs(10);
/// TDS 7.4.
const VERSION: u32 = 0x7400_0004;

// What PRELOGIN's ENCRYPTION says.
const ENCRYPT_OFF: u8 = 0;
const ENCRYPT_ON: u8 = 1;
const ENCRYPT_NOT_SUP: u8 = 2;
const ENCRYPT_REQ: u8 = 3;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// How the connection is protected.
#[derive(Debug, Clone)]
pub(super) enum Tls {
    /// No TLS: the password goes only lightly disguised.
    Off,
    /// TLS agreed in PRELOGIN (TDS 7.4), for the whole connection.
    On(Arc<rustls::ClientConfig>),
    /// TLS before anything else (TDS 8, SQL Server 2022 and later).
    Strict(Arc<rustls::ClientConfig>),
}

#[derive(Clone)]
pub(super) struct Options {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) database: Option<String>,
    pub(super) username: String,
    pub(super) password: String,
    pub(super) tls: Tls,
    pub(super) address: Option<std::net::SocketAddr>,
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// A TCP stream that, while TLS is being set up, carries what TLS sends inside PRELOGIN packets (TDS 7.x), and
/// after that passes everything through as it is.
struct Prelogin<S> {
    inner: S,
    /// TLS is still being set up.
    wrapping: bool,
    /// What TLS wrote since the last flush, after room for a packet header.
    out: Vec<u8>,
    /// The header of the packet being read, and how many bytes of its body are still to come.
    header: [u8; HEADER],
    have: usize,
    left: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Prelogin<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if !this.wrapping {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        while this.left == 0 {
            let mut header = ReadBuf::new(this.header.get_mut(this.have..).unwrap_or_default());
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut header))?;
            let n = header.filled().len();
            if n == 0 {
                return Poll::Ready(Ok(()));
            }
            this.have += n;
            if this.have == HEADER {
                this.have = 0;
                let length = usize::from(u16::from_be_bytes([this.header[2], this.header[3]]));
                this.left = length.saturating_sub(HEADER);
            }
        }
        let room = buf.remaining().min(this.left);
        let mut part = ReadBuf::new(buf.initialize_unfilled_to(room));
        ready!(Pin::new(&mut this.inner).poll_read(cx, &mut part))?;
        let n = part.filled().len();
        this.left -= n;
        buf.advance(n);
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Prelogin<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;
        if !this.wrapping {
            return Pin::new(&mut this.inner).poll_write(cx, data);
        }
        if this.out.is_empty() {
            this.out.extend_from_slice(&[0; HEADER]);
        }
        this.out.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if this.wrapping && this.out.len() > HEADER {
            let length = u16::try_from(this.out.len())
                .map_err(|_| std::io::Error::other("a TLS message too large for one packet"))?;
            let [hi, lo] = length.to_be_bytes();
            if let Some(h) = this.out.get_mut(..HEADER) {
                h.copy_from_slice(&[PRELOGIN, END, hi, lo, 0, 0, 1, 0]);
            }
            while !this.out.is_empty() {
                let n = ready!(Pin::new(&mut this.inner).poll_write(cx, &this.out))?;
                if n == 0 {
                    return Poll::Ready(Err(std::io::ErrorKind::WriteZero.into()));
                }
                this.out.drain(..n);
            }
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(super) struct Connection {
    stream: Box<dyn Io>,
    packet: usize,
    /// The transaction this connection is in (from the server's ENVCHANGE), sent with every request.
    pub(super) transaction: [u8; 8],
    /// The server's collation, for the text values sent as parameters.
    pub(super) collation: [u8; 5],
    /// `the SQL Server at host:port`, for messages.
    pub(super) what: String,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("what", &self.what)
            .finish_non_exhaustive()
    }
}

/// What was said in PRELOGIN: the encryption the server takes.
pub(super) fn prelogin_payload(encryption: u8) -> Vec<u8> {
    // VERSION, ENCRYPTION, INSTOPT, THREADID, MARS, then the terminator; their data after.
    let options: [(u8, Vec<u8>); 5] = [
        (0x00, vec![0, 1, 0, 0, 0, 0]),
        (0x01, vec![encryption]),
        (0x02, vec![0]),
        (0x03, std::process::id().to_be_bytes().to_vec()),
        (0x04, vec![0]),
    ];
    let mut at = options.len() * 5 + 1;
    let mut out = Vec::new();
    let mut data = Vec::new();
    for (kind, value) in &options {
        out.push(*kind);
        out.put_u16(u16::try_from(at).unwrap_or(0));
        out.put_u16(u16::try_from(value.len()).unwrap_or(0));
        at += value.len();
        data.extend_from_slice(value);
    }
    out.push(0xff);
    out.extend_from_slice(&data);
    out
}

/// The ENCRYPTION the server answered PRELOGIN with.
pub(super) fn encryption_of(answer: &[u8]) -> Option<u8> {
    let mut b = answer;
    loop {
        if b.first() == Some(&0xff) {
            return None;
        }
        let [kind, a1, a2, l1, l2, ..] = *b else {
            return None;
        };
        let at = usize::from(u16::from_be_bytes([a1, a2]));
        let length = usize::from(u16::from_be_bytes([l1, l2]));
        if kind == 0x01 && length >= 1 {
            return answer.get(at).copied();
        }
        b = b.get(5..)?;
    }
}

/// UTF-16 little-endian, as TDS sends text.
pub(super) fn ucs2(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

/// LOGIN7: who signs in, to which database, from where.
pub(super) fn login_payload(o: &Options, version: u32, pid: u32) -> Vec<u8> {
    // The variable parts, in their order: host, user, password, app, server, (extension), library, language,
    // database. The password is disguised as the protocol asks: each byte's halves swapped, then XOR 0xA5.
    let password: Vec<u8> = ucs2(&o.password)
        .iter()
        .map(|b| b.rotate_left(4) ^ 0xa5)
        .collect();
    let parts: [(Vec<u8>, usize); 9] = [
        (ucs2("openduct"), 8),
        (ucs2(&o.username), o.username.encode_utf16().count()),
        (password, o.password.encode_utf16().count()),
        (ucs2("OpenDuct"), 8),
        (ucs2(&o.host), o.host.encode_utf16().count()),
        (Vec::new(), 0),
        (ucs2("OpenDuct"), 8),
        (Vec::new(), 0),
        (
            ucs2(o.database.as_deref().unwrap_or_default()),
            o.database.as_deref().unwrap_or_default().encode_utf16().count(),
        ),
    ];
    login_bytes(version, PACKET as u32, 0, pid, 0xe0, 0x03, 0x409, &parts, [0; 6])
}

#[allow(clippy::too_many_arguments)] // the record's own fields, one by one, so a test can give the vendor's sample
fn login_bytes(
    version: u32,
    packet: u32,
    program: u32,
    pid: u32,
    flags1: u8,
    flags2: u8,
    lcid: u32,
    parts: &[(Vec<u8>, usize); 9],
    client: [u8; 6],
) -> Vec<u8> {
    const FIXED: usize = 94;
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    for (bytes, count) in parts {
        let at = FIXED + data.len();
        offsets.put_u16_le(u16::try_from(at).unwrap_or(0));
        offsets.put_u16_le(u16::try_from(*count).unwrap_or(0));
        data.extend_from_slice(bytes);
    }
    let end = u16::try_from(FIXED + data.len()).unwrap_or(0);
    let mut out = Vec::with_capacity(FIXED + data.len());
    out.put_u32_le(u32::try_from(FIXED + data.len()).unwrap_or(0));
    out.put_u32_le(version);
    out.put_u32_le(packet);
    out.put_u32_le(program);
    out.put_u32_le(pid);
    out.put_u32_le(0);
    out.extend_from_slice(&[flags1, flags2, 0, 0]);
    out.put_i32_le(0);
    out.put_u32_le(lcid);
    // Host, user, password, app, server, extension, library, language, database.
    out.extend_from_slice(&offsets);
    out.extend_from_slice(&client);
    // SSPI, a file to attach, a new password: none, each at the end; and no long SSPI.
    for _ in 0..3 {
        out.put_u16_le(end);
        out.put_u16_le(0);
    }
    out.put_u32_le(0);
    out.extend_from_slice(&data);
    out
}

impl Connection {
    pub(super) async fn open(o: &Options) -> Result<Self, String> {
        let what = format!("the SQL Server at {}:{}", o.host, o.port);
        tokio::time::timeout(CONNECT_WAIT, Self::connect(o, what.clone()))
            .await
            .map_err(|_| format!("{what} did not answer within 10 seconds"))?
    }

    async fn connect(o: &Options, what: String) -> Result<Self, String> {
        let tcp = match o.address {
            Some(a) => TcpStream::connect(a).await,
            None => TcpStream::connect((o.host.as_str(), o.port)).await,
        }
        .map_err(|e| format!("cannot connect to {what}: {e}"))?;
        let _ = tcp.set_nodelay(true);
        let name = || {
            rustls::pki_types::ServerName::try_from(o.host.clone())
                .map_err(|_| format!("{} is not a name TLS can check", o.host))
        };
        let tls_failed = |e: std::io::Error| crate::tls::failure(&e, &what).1;
        let mut conn = Self {
            stream: Box::new(tokio::io::duplex(1).0),
            packet: PACKET,
            transaction: [0; 8],
            collation: [0; 5],
            what: what.clone(),
        };
        match &o.tls {
            Tls::Strict(config) => {
                let mut config = (**config).clone();
                config.alpn_protocols = vec![b"tds/8.0".to_vec()];
                let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
                    .connect(name()?, tcp)
                    .await
                    .map_err(tls_failed)?;
                conn.stream = Box::new(tls);
                conn.send(PRELOGIN, &prelogin_payload(ENCRYPT_NOT_SUP)).await?;
                conn.read_message(64 * 1024, None).await?;
            }
            Tls::On(config) => {
                conn.stream = Box::new(tcp);
                conn.send(PRELOGIN, &prelogin_payload(ENCRYPT_ON)).await?;
                let answer = conn.read_message(64 * 1024, None).await?;
                match encryption_of(&answer) {
                    Some(ENCRYPT_ON | ENCRYPT_REQ) => {}
                    _ => {
                        return Err(format!(
                            "{what} does not take TLS connections: set tls to disable to sign in without"
                        ));
                    }
                }
                let plain = std::mem::replace(&mut conn.stream, Box::new(tokio::io::duplex(1).0));
                let wrapped = Prelogin {
                    inner: plain,
                    wrapping: true,
                    out: Vec::new(),
                    header: [0; HEADER],
                    have: 0,
                    left: 0,
                };
                let mut tls = tokio_rustls::TlsConnector::from(Arc::clone(config))
                    .connect(name()?, wrapped)
                    .await
                    .map_err(tls_failed)?;
                tls.get_mut().0.wrapping = false;
                conn.stream = Box::new(tls);
            }
            Tls::Off => {
                conn.stream = Box::new(tcp);
                conn.send(PRELOGIN, &prelogin_payload(ENCRYPT_NOT_SUP)).await?;
                let answer = conn.read_message(64 * 1024, None).await?;
                match encryption_of(&answer) {
                    Some(ENCRYPT_NOT_SUP | ENCRYPT_OFF) | None => {}
                    Some(_) => {
                        return Err(format!(
                            "{what} takes only connections with TLS: set tls to require (or strict)"
                        ));
                    }
                }
            }
        }
        conn.send(LOGIN7, &login_payload(o, VERSION, std::process::id()))
            .await?;
        let answer = conn.read_message(1024 * 1024, None).await?;
        let said = super::tokens::read(&answer, &mut conn, None)?;
        if let Some(e) = said.error {
            return Err(if e.number == 18456 {
                format!("{what} refused the user or password, or does not let this user connect")
            } else {
                format!("{what}: {}", e.shown())
            });
        }
        if !said.signed_in {
            return Err(format!("{what} did not say it signed in"));
        }
        Ok(conn)
    }

    /// A connection to nothing, for tests of what is read.
    #[cfg(test)]
    pub(super) fn for_tests() -> Self {
        Self {
            stream: Box::new(tokio::io::duplex(1).0),
            packet: PACKET,
            transaction: [0; 8],
            collation: [0; 5],
            what: "the SQL Server at test:1433".to_owned(),
        }
    }

    /// The packet size the server agreed on.
    pub(super) fn set_packet(&mut self, size: usize) {
        if (512..=LARGEST_PACKET).contains(&size) {
            self.packet = size;
        }
    }

    /// Sends a message of `kind`, cut into packets.
    pub(super) async fn send(&mut self, kind: u8, payload: &[u8]) -> Result<(), String> {
        let room = self.packet - HEADER;
        let chunks: Vec<&[u8]> = if payload.is_empty() {
            vec![&[]]
        } else {
            payload.chunks(room).collect()
        };
        let last = chunks.len().saturating_sub(1);
        let mut out = Vec::with_capacity(payload.len() + HEADER * chunks.len());
        for (i, chunk) in chunks.iter().enumerate() {
            let length = u16::try_from(chunk.len() + HEADER).unwrap_or(u16::MAX);
            out.push(kind);
            out.push(if i == last { END } else { 0 });
            out.put_u16(length);
            out.put_u16(0);
            out.push(u8::try_from((i + 1) % 256).unwrap_or(0));
            out.push(0);
            out.extend_from_slice(chunk);
        }
        let what = &self.what;
        self.stream
            .write_all(&out)
            .await
            .map_err(|e| format!("the connection to {what} broke: {e}"))?;
        self.stream
            .flush()
            .await
            .map_err(|e| format!("the connection to {what} broke: {e}"))
    }

    /// The next whole answer, its packets joined: refused past `limit` bytes, and, past `until`, given up (the
    /// server told to stop what it is doing).
    pub(super) async fn read_message(
        &mut self,
        limit: usize,
        until: Option<tokio::time::Instant>,
    ) -> Result<Bytes, String> {
        let mut whole = BytesMut::new();
        loop {
            let read = self.read_packet();
            let (status, body) = match until {
                Some(deadline) => match tokio::time::timeout_at(deadline, read).await {
                    Ok(r) => r?,
                    Err(_) => {
                        let _ = self.stream.write_all(&[ATTENTION, END, 0, 8, 0, 0, 1, 0]).await;
                        let _ = self.stream.flush().await;
                        return Err(format!(
                            "{} did not finish in time: it was told to stop",
                            self.what
                        ));
                    }
                },
                None => read.await?,
            };
            if whole.len() + body.len() > limit {
                return Err(format!(
                    "{} answered with more than {} MiB: ask for fewer rows or columns",
                    self.what,
                    limit / (1024 * 1024)
                ));
            }
            whole.extend_from_slice(&body);
            if status & END != 0 {
                return Ok(whole.freeze());
            }
        }
    }

    async fn read_packet(&mut self) -> Result<(u8, Bytes), String> {
        let what = &self.what;
        let broke = |e: std::io::Error| format!("the connection to {what} broke: {e}");
        let mut header = [0u8; HEADER];
        self.stream.read_exact(&mut header).await.map_err(broke)?;
        let [kind, status, hi, lo, ..] = header;
        let length = usize::from(u16::from_be_bytes([hi, lo]));
        if !matches!(kind, REPLY | PRELOGIN) || !(HEADER..=LARGEST_PACKET + HEADER).contains(&length) {
            return Err(format!("{what} sent something that is not understood"));
        }
        let mut body = vec![0u8; length - HEADER];
        self.stream.read_exact(&mut body).await.map_err(broke)?;
        Ok((status, Bytes::from(body)))
    }
}

/// The header TDS puts before a request: the transaction it belongs to, and one request outstanding.
pub(super) fn all_headers(transaction: [u8; 8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(22);
    out.put_u32_le(22);
    out.put_u32_le(18);
    out.put_u16_le(2);
    out.extend_from_slice(&transaction);
    out.put_u32_le(1);
    out
}

/// Takes `n` bytes, or none when fewer are there.
pub(super) fn take(b: &mut Bytes, n: usize) -> Option<Bytes> {
    (b.remaining() >= n).then(|| b.split_to(n))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// The vendor's sample LOGIN7 (MS-TDS 4.2), byte for byte, from the fields it gives.
    #[test]
    fn login7_is_the_record_the_protocol_describes() {
        let sample: Vec<u8> = "88 00 00 00 02 00 09 72 00 10 00 00 00 00 00 07 00 01 00 00 00 00 00 00 \
             E0 03 00 00 00 00 00 00 09 04 00 00 5E 00 08 00 6E 00 02 00 72 00 00 00 72 00 07 00 80 00 00 00 \
             80 00 00 00 80 00 04 00 88 00 00 00 88 00 00 00 00 50 8B E2 B7 8F 88 00 00 00 88 00 00 00 88 00 \
             00 00 00 00 00 00 73 00 6B 00 6F 00 73 00 74 00 6F 00 76 00 31 00 73 00 61 00 4F 00 53 00 51 00 \
             4C 00 2D 00 33 00 32 00 4F 00 44 00 42 00 43 00"
            .split_whitespace()
            .map(|h| u8::from_str_radix(h, 16).unwrap())
            .collect();
        let parts: [(Vec<u8>, usize); 9] = [
            (ucs2("skostov1"), 8),
            (ucs2("sa"), 2),
            (Vec::new(), 0),
            (ucs2("OSQL-32"), 7),
            (Vec::new(), 0),
            (Vec::new(), 0),
            (ucs2("ODBC"), 4),
            (Vec::new(), 0),
            (Vec::new(), 0),
        ];
        let made = login_bytes(
            0x7209_0002,
            4096,
            0x0700_0000,
            0x0100,
            0xe0,
            0x03,
            0x409,
            &parts,
            [0x00, 0x50, 0x8b, 0xe2, 0xb7, 0x8f],
        );
        assert_eq!(made, sample);
    }

    #[test]
    fn the_password_is_disguised_as_the_protocol_says() {
        let o = Options {
            host: "db".into(),
            port: 1433,
            database: Some("shop".into()),
            username: "bob".into(),
            password: "a".into(),
            tls: Tls::Off,
            address: None,
        };
        let login = login_payload(&o, VERSION, 1);
        let at = usize::from(u16::from_le_bytes([login[44], login[45]]));
        // 'a' is 0x61 0x00: halves swapped (0x16, 0x00), then XOR 0xA5.
        assert_eq!(&login[at..at + 2], &[0x16 ^ 0xa5, 0xa5]);
        assert_eq!(u16::from_le_bytes([login[46], login[47]]), 1);
        assert!(
            !login.windows(2).any(|w| w == [0x61, 0x00]),
            "the password is not there as it is"
        );
    }

    #[test]
    fn prelogin_says_what_encryption_and_reads_the_answer() {
        let p = prelogin_payload(ENCRYPT_ON);
        assert_eq!(p[0], 0x00);
        assert_eq!(p[25], 0xff);
        assert_eq!(encryption_of(&p), Some(ENCRYPT_ON));
        assert_eq!(
            encryption_of(&prelogin_payload(ENCRYPT_NOT_SUP)),
            Some(ENCRYPT_NOT_SUP)
        );
        assert_eq!(encryption_of(&[0x01, 0x00]), None);
        assert_eq!(encryption_of(&[0x01, 0x00, 0x40, 0x00, 0x01]), None);
        assert_eq!(all_headers([1; 8]).len(), 22);
    }
}
