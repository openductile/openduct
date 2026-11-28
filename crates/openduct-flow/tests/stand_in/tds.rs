// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A stand-in for SQL Server, as far as the `sqlserver` step speaks to one: PRELOGIN, TLS carried in PRELOGIN
//! packets (when it has a certificate), LOGIN7 with the password checked, SQL batches for transactions (each
//! beginning one reported with a transaction descriptor the requests after must carry), and statements run through
//! sp_executesql with typed values. What it answers a statement is the test's to say.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

pub const PASSWORD: &str = "s3cret-pass";
const DESCRIPTOR: [u8; 8] = [9, 8, 7, 6, 5, 4, 3, 2];

/// A value a statement was given, as the server read it: its declared type and value.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    Null,
    Int(i64),
    Bit(bool),
    Text(String),
    /// Sign, magnitude and scale.
    Decimal(bool, u128, u8),
    DateTime2(Vec<u8>),
}

/// A column the server sends, and its cells.
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Int(i64),
    Text(String),
    /// decimal(18, 2): its value in hundredths.
    Cents(i64),
    Bit(bool),
}

#[derive(Debug, Clone, Copy)]
pub enum Kind {
    BigInt,
    NVarChar,
    Decimal,
    Bit,
}

#[derive(Debug, Clone)]
pub enum Reply {
    Rows(Vec<(&'static str, Kind)>, Vec<Vec<Cell>>),
    Done(u64),
    Error(u32, String),
}

/// What a request was: its SQL, its declaration of values, the values, and whether it carried the transaction.
#[derive(Debug, Clone)]
pub struct Request {
    pub sql: String,
    pub declared: String,
    pub values: Vec<Val>,
    pub in_transaction: bool,
}

pub type Answer = dyn Fn(&Request) -> Reply + Send + Sync;

pub struct Tds {
    pub addr: String,
    pub log: Arc<Mutex<Vec<String>>>,
    pub requests: Arc<Mutex<Vec<Request>>>,
}

impl Tds {
    /// `tls`: a certificate and key (DER) to set up TLS with, as SQL Server does when asked.
    pub async fn start(answer: Arc<Answer>, tls: Option<(Vec<u8>, Vec<u8>)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let acceptor = tls.map(|(cert, key)| {
            let _ = rustls::crypto::ring::default_provider().install_default();
            // TLS 1.2, as SQL Server sets it up inside PRELOGIN.
            let config = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
                .with_no_client_auth()
                .with_single_cert(
                    vec![rustls::pki_types::CertificateDer::from(cert)],
                    rustls::pki_types::PrivateKeyDer::Pkcs8(key.into()),
                )
                .unwrap();
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        });
        let (l, r) = (Arc::clone(&log), Arc::clone(&requests));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (answer, l, r, acceptor) = (
                    Arc::clone(&answer),
                    Arc::clone(&l),
                    Arc::clone(&r),
                    acceptor.clone(),
                );
                tokio::spawn(async move {
                    let _ = serve(stream, answer, l, r, acceptor).await;
                });
            }
        });
        Self { addr, log, requests }
    }

    pub fn port(&self) -> String {
        self.addr.rsplit(':').next().unwrap().to_owned()
    }

    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// The server's side of TLS inside PRELOGIN packets: what TLS writes goes out in packets, what comes in is
/// unwrapped; after the handshake, everything passes as it is.
struct Wrapped {
    inner: TcpStream,
    wrapping: bool,
    out: Vec<u8>,
    header: [u8; 8],
    have: usize,
    left: usize,
}

impl AsyncRead for Wrapped {
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
            let mut header = ReadBuf::new(&mut this.header[this.have..]);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut header))?;
            let n = header.filled().len();
            if n == 0 {
                return Poll::Ready(Ok(()));
            }
            this.have += n;
            if this.have == 8 {
                this.have = 0;
                assert_eq!(this.header[0], 0x12, "TLS comes in PRELOGIN packets");
                this.left = usize::from(u16::from_be_bytes([this.header[2], this.header[3]])) - 8;
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

impl AsyncWrite for Wrapped {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.wrapping {
            return Pin::new(&mut self.inner).poll_write(cx, data);
        }
        self.out.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        if this.wrapping && !this.out.is_empty() {
            let mut packet = vec![0x12, 1];
            packet.extend_from_slice(&((this.out.len() + 8) as u16).to_be_bytes());
            packet.extend_from_slice(&[0, 0, 1, 0]);
            packet.append(&mut this.out);
            this.out = packet;
            while !this.out.is_empty() {
                let n = ready!(Pin::new(&mut this.inner).poll_write(cx, &this.out))?;
                this.out.drain(..n);
            }
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn read_message(s: &mut (dyn Io + '_)) -> Option<(u8, Vec<u8>)> {
    let mut whole = Vec::new();
    loop {
        let mut head = [0u8; 8];
        s.read_exact(&mut head).await.ok()?;
        let length = usize::from(u16::from_be_bytes([head[2], head[3]]));
        let mut body = vec![0u8; length - 8];
        s.read_exact(&mut body).await.ok()?;
        whole.extend_from_slice(&body);
        if head[1] & 1 == 1 {
            return Some((head[0], whole));
        }
    }
}

async fn reply(s: &mut (dyn Io + '_), body: &[u8]) {
    let mut out = vec![0x04, 1];
    out.extend_from_slice(&((body.len() + 8) as u16).to_be_bytes());
    out.extend_from_slice(&[0, 0, 1, 0]);
    out.extend_from_slice(body);
    s.write_all(&out).await.unwrap();
    s.flush().await.unwrap();
}

fn ucs2(t: &str) -> Vec<u8> {
    t.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn utf16(b: &[u8]) -> String {
    String::from_utf16_lossy(
        &b.as_chunks::<2>()
            .0
            .iter()
            .map(|p| u16::from_le_bytes(*p))
            .collect::<Vec<_>>(),
    )
}

fn b_varchar(out: &mut Vec<u8>, t: &str) {
    out.push(t.encode_utf16().count() as u8);
    out.extend_from_slice(&ucs2(t));
}

fn env(out: &mut Vec<u8>, kind: u8, new: &[u8], old: &[u8]) {
    out.push(0xe3);
    out.extend_from_slice(&((3 + new.len() + old.len()) as u16).to_le_bytes());
    out.push(kind);
    out.push(new.len() as u8);
    out.extend_from_slice(new);
    out.push(old.len() as u8);
    out.extend_from_slice(old);
}

fn done(out: &mut Vec<u8>, token: u8, status: u16, count: u64) {
    out.push(token);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&0xc1u16.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
}

fn error(out: &mut Vec<u8>, number: u32, text: &str) {
    let mut body = number.to_le_bytes().to_vec();
    body.extend_from_slice(&[1, 16]);
    body.extend_from_slice(&(text.encode_utf16().count() as u16).to_le_bytes());
    body.extend_from_slice(&ucs2(text));
    b_varchar(&mut body, "stand-in");
    b_varchar(&mut body, "");
    body.extend_from_slice(&1u32.to_le_bytes());
    out.push(0xaa);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

const COLLATION: [u8; 5] = [0x09, 0x04, 0xd0, 0x00, 0x34];

fn rows(out: &mut Vec<u8>, columns: &[(&str, Kind)], cells: &[Vec<Cell>]) {
    out.push(0x81);
    out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for (name, kind) in columns {
        out.extend_from_slice(&[0, 0, 0, 0, 0x09, 0x00]);
        match kind {
            Kind::BigInt => out.extend_from_slice(&[0x26, 8]),
            Kind::NVarChar => {
                out.extend_from_slice(&[0xe7, 0xc8, 0x00]);
                out.extend_from_slice(&COLLATION);
            }
            Kind::Decimal => out.extend_from_slice(&[0x6a, 9, 18, 2]),
            Kind::Bit => out.extend_from_slice(&[0x68, 1]),
        }
        b_varchar(out, name);
    }
    for row in cells {
        out.push(0xd1);
        for ((_, kind), cell) in columns.iter().zip(row) {
            match (kind, cell) {
                (Kind::NVarChar, Cell::Null) => out.extend_from_slice(&[0xff, 0xff]),
                (_, Cell::Null) => out.push(0),
                (_, Cell::Int(n)) => {
                    out.push(8);
                    out.extend_from_slice(&n.to_le_bytes());
                }
                (_, Cell::Text(t)) => {
                    let b = ucs2(t);
                    out.extend_from_slice(&(b.len() as u16).to_le_bytes());
                    out.extend_from_slice(&b);
                }
                (_, Cell::Cents(c)) => {
                    out.push(9);
                    out.push(u8::from(*c >= 0));
                    out.extend_from_slice(&c.unsigned_abs().to_le_bytes());
                }
                (_, Cell::Bit(b)) => out.extend_from_slice(&[1, u8::from(*b)]),
            }
        }
    }
    done(out, 0xff, 0x10, cells.len() as u64);
}

/// A parameter of an RPC: its TYPE_INFO and value.
fn param(b: &[u8], at: &mut usize) -> Val {
    let kind = b[*at];
    *at += 1;
    match kind {
        0xe7 => {
            let max = u16::from_le_bytes([b[*at], b[*at + 1]]);
            *at += 2 + 5;
            if max == 0xffff {
                let total = u64::from_le_bytes(b[*at..*at + 8].try_into().unwrap());
                *at += 8;
                if total == u64::MAX {
                    return Val::Null;
                }
                let mut bytes = Vec::new();
                loop {
                    let n = u32::from_le_bytes(b[*at..*at + 4].try_into().unwrap()) as usize;
                    *at += 4;
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&b[*at..*at + n]);
                    *at += n;
                }
                return Val::Text(utf16(&bytes));
            }
            let n = u16::from_le_bytes([b[*at], b[*at + 1]]);
            *at += 2;
            if n == 0xffff {
                return Val::Null;
            }
            let t = utf16(&b[*at..*at + usize::from(n)]);
            *at += usize::from(n);
            Val::Text(t)
        }
        0x26 => {
            let n = usize::from(b[*at + 1]);
            *at += 2;
            let mut raw = [0u8; 8];
            raw[..n].copy_from_slice(&b[*at..*at + n]);
            *at += n;
            Val::Int(i64::from_le_bytes(raw))
        }
        0x68 => {
            *at += 2;
            *at += 1;
            Val::Bit(b[*at - 1] != 0)
        }
        0x6a => {
            let scale = b[*at + 2];
            let n = usize::from(b[*at + 3]);
            *at += 4;
            let positive = b[*at] == 1;
            let magnitude = u128::from_le_bytes(b[*at + 1..*at + 17].try_into().unwrap());
            *at += n;
            Val::Decimal(positive, magnitude, scale)
        }
        0x2a => {
            let n = usize::from(b[*at + 1]);
            *at += 2;
            let v = b[*at..*at + n].to_vec();
            *at += n;
            Val::DateTime2(v)
        }
        other => panic!("a type the stand-in does not read: {other:#x}"),
    }
}

fn rpc(b: &[u8]) -> Request {
    let headers = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
    let in_transaction = b[10..18] == DESCRIPTOR;
    let mut at = headers;
    assert_eq!(&b[at..at + 4], &[0xff, 0xff, 10, 0], "through sp_executesql");
    at += 6;
    let mut list = Vec::new();
    while at < b.len() {
        let name_len = usize::from(b[at]);
        at += 1 + 2 * name_len + 1;
        list.push(param(b, &mut at));
    }
    let text = |v: &Val| match v {
        Val::Text(t) => t.clone(),
        other => panic!("not text: {other:?}"),
    };
    Request {
        sql: text(&list[0]),
        declared: list.get(1).map(text).unwrap_or_default(),
        values: list.into_iter().skip(2).collect(),
        in_transaction,
    }
}

async fn serve(
    tcp: TcpStream,
    answer: Arc<Answer>,
    log: Arc<Mutex<Vec<String>>>,
    requests: Arc<Mutex<Vec<Request>>>,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> Option<()> {
    let mut wrapped = Wrapped {
        inner: tcp,
        wrapping: false,
        out: Vec::new(),
        header: [0; 8],
        have: 0,
        left: 0,
    };
    let (kind, hello) = read_message(&mut wrapped).await?;
    assert_eq!(kind, 0x12);
    // The client's ENCRYPTION, the second option.
    let at = usize::from(u16::from_be_bytes([hello[6], hello[7]]));
    log.lock()
        .unwrap()
        .push(format!("prelogin: encryption {}", hello[at]));
    let mine = if acceptor.is_some() { 1 } else { 2 };
    let mut answer_prelogin = vec![0x00, 0, 11, 0, 6, 0x01, 0, 17, 0, 1, 0xff];
    answer_prelogin.extend_from_slice(&[16, 0, 0, 0, 0, 0, mine]);
    reply(&mut wrapped, &answer_prelogin).await;
    let mut stream: Box<dyn Io> = match acceptor {
        Some(acceptor) => {
            wrapped.wrapping = true;
            let mut tls = acceptor.accept(wrapped).await.ok()?;
            tls.get_mut().0.wrapping = false;
            log.lock().unwrap().push("tls".to_owned());
            Box::new(tls)
        }
        None => Box::new(wrapped),
    };
    let (kind, login) = read_message(&mut *stream).await?;
    assert_eq!(kind, 0x10);
    let field = |i: usize| {
        let at = usize::from(u16::from_le_bytes([login[36 + 4 * i], login[37 + 4 * i]]));
        let n = usize::from(u16::from_le_bytes([login[38 + 4 * i], login[39 + 4 * i]]));
        login[at..at + 2 * n].to_vec()
    };
    let user = utf16(&field(1));
    let password = utf16(
        &field(2)
            .iter()
            .map(|b| (b ^ 0xa5).rotate_left(4))
            .collect::<Vec<_>>(),
    );
    let database = utf16(&field(8));
    log.lock().unwrap().push(format!("login: {user} to {database}"));
    let mut out = Vec::new();
    if user != "books" || password != PASSWORD {
        error(&mut out, 18456, &format!("Login failed for user '{user}'."));
        done(&mut out, 0xfd, 0x02, 0);
        reply(&mut *stream, &out).await;
        return None;
    }
    env(&mut out, 7, &COLLATION, &[]);
    let size = ucs2("4096");
    let mut size_env = vec![(size.len() / 2) as u8];
    size_env.extend_from_slice(&size);
    out.push(0xe3);
    out.extend_from_slice(&((1 + size_env.len() + 1) as u16).to_le_bytes());
    out.push(4);
    out.extend_from_slice(&size_env);
    out.push(0);
    let mut ack = vec![1];
    ack.extend_from_slice(&0x7400_0004u32.to_be_bytes());
    b_varchar(&mut ack, "Stand-in SQL Server");
    ack.extend_from_slice(&[16, 0, 0, 0]);
    out.push(0xad);
    out.extend_from_slice(&(ack.len() as u16).to_le_bytes());
    out.extend_from_slice(&ack);
    done(&mut out, 0xfd, 0, 0);
    reply(&mut *stream, &out).await;
    loop {
        let (kind, body) = read_message(&mut *stream).await?;
        let mut out = Vec::new();
        match kind {
            0x01 => {
                let headers = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
                let sql = utf16(&body[headers..]);
                log.lock().unwrap().push(format!("batch: {sql}"));
                if sql.contains("BEGIN TRANSACTION") {
                    env(&mut out, 8, &DESCRIPTOR, &[]);
                } else if sql.contains("COMMIT") {
                    env(&mut out, 9, &[], &DESCRIPTOR);
                } else if sql.contains("ROLLBACK") {
                    env(&mut out, 10, &[], &DESCRIPTOR);
                }
                done(&mut out, 0xfd, 0, 0);
            }
            0x03 => {
                let request = rpc(&body);
                log.lock().unwrap().push(format!("rpc: {}", request.sql));
                requests.lock().unwrap().push(request.clone());
                match answer(&request) {
                    Reply::Rows(columns, cells) => rows(&mut out, &columns, &cells),
                    Reply::Done(n) => done(&mut out, 0xff, 0x10, n),
                    Reply::Error(number, text) => {
                        error(&mut out, number, &text);
                        done(&mut out, 0xff, 0x02, 0);
                    }
                }
                out.extend_from_slice(&[0x79, 0, 0, 0, 0]);
                done(&mut out, 0xfe, 0, 0);
            }
            other => panic!("a message the stand-in does not take: {other:#x}"),
        }
        reply(&mut *stream, &out).await;
    }
}
