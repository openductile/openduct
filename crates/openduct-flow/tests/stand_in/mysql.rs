// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A stand-in for a MySQL server, as far as the `mysql` step speaks to one: the greeting, sign-in with
//! mysql_native_password (the password checked), text queries for the session and transactions, prepared statements
//! run with their typed values, and binary rows. What it answers a statement is the test's to say.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub const PASSWORD: &str = "s3cret-pass";

// Column types.
pub const LONGLONG: u8 = 8;
pub const DOUBLE: u8 = 5;
pub const NEWDECIMAL: u8 = 246;
pub const VAR_STRING: u8 = 253;
pub const DATETIME: u8 = 12;
pub const JSON: u8 = 245;
pub const TINY: u8 = 1;
pub const BINARY_CHARSET: u16 = 63;

/// A value a statement was given, as the server read it.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    Null,
    Tiny(u8),
    Int(i64),
    Decimal(String),
    Text(String),
    DateTime(String),
}

/// A cell of a row the server sends.
#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Int(i64),
    Double(f64),
    Text(String),
    Bytes(Vec<u8>),
    DateTime(u16, u8, u8, u8, u8, u8, u32),
}

#[derive(Debug, Clone)]
pub struct Col {
    pub name: String,
    pub kind: u8,
    pub flags: u16,
    pub charset: u16,
    pub decimals: u8,
}

pub fn col(name: &str, kind: u8) -> Col {
    Col {
        name: name.to_owned(),
        kind,
        flags: 0,
        charset: if matches!(kind, VAR_STRING | JSON) {
            45
        } else {
            BINARY_CHARSET
        },
        decimals: if kind == DATETIME { 3 } else { 0 },
    }
}

#[derive(Debug, Clone)]
pub enum Reply {
    Rows(Vec<Col>, Vec<Vec<Cell>>),
    Done {
        affected: u64,
        insert_id: u64,
    },
    Error(u16, &'static str, String),
    /// Wait this long, then answer the reply.
    After(Duration, Box<Reply>),
}

/// The session a statement runs in.
#[derive(Debug, Default, Clone)]
pub struct Session {
    pub read_only: bool,
    pub in_transaction: bool,
}

pub type Answer = dyn Fn(&str, &[Val], &Session) -> Reply + Send + Sync;
/// The statements run, each with its values.
pub type Executed = Arc<Mutex<Vec<(String, Vec<Val>)>>>;

/// The stand-in: where it listens, and everything it was sent (`query: ...`, `execute: ... | values`).
pub struct MySql {
    pub addr: String,
    pub log: Arc<Mutex<Vec<String>>>,
    pub executed: Executed,
}

impl MySql {
    pub async fn start(answer: Arc<Answer>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let log = Arc::new(Mutex::new(Vec::new()));
        let executed = Arc::new(Mutex::new(Vec::new()));
        let (l, e) = (Arc::clone(&log), Arc::clone(&executed));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (answer, l, e) = (Arc::clone(&answer), Arc::clone(&l), Arc::clone(&e));
                tokio::spawn(async move {
                    let _ = serve(stream, answer, l, e).await;
                });
            }
        });
        Self { addr, log, executed }
    }

    pub fn port(&self) -> String {
        self.addr.rsplit(':').next().unwrap().to_owned()
    }

    pub fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

async fn read_packet(s: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut head = [0u8; 4];
    s.read_exact(&mut head).await.ok()?;
    let length = usize::from(head[0]) | usize::from(head[1]) << 8 | usize::from(head[2]) << 16;
    let mut body = vec![0u8; length];
    s.read_exact(&mut body).await.ok()?;
    Some((head[3], body))
}

async fn write_packet(s: &mut TcpStream, seq: &mut u8, body: &[u8]) {
    let n = body.len();
    let mut out = vec![n as u8, (n >> 8) as u8, (n >> 16) as u8, *seq];
    out.extend_from_slice(body);
    *seq = seq.wrapping_add(1);
    s.write_all(&out).await.unwrap();
}

fn lenenc(out: &mut Vec<u8>, n: u64) {
    if n < 251 {
        out.push(n as u8);
    } else if n < 1 << 16 {
        out.push(0xfc);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else {
        out.push(0xfe);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

fn lenenc_bytes(out: &mut Vec<u8>, b: &[u8]) {
    lenenc(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn take_lenenc(b: &[u8], at: &mut usize) -> u64 {
    let first = b[*at];
    *at += 1;
    match first {
        0..=0xfa => u64::from(first),
        0xfc => {
            let n = u64::from(u16::from_le_bytes([b[*at], b[*at + 1]]));
            *at += 2;
            n
        }
        _ => panic!("a length the stand-in does not read"),
    }
}

fn sha1(parts: &[&[u8]]) -> Vec<u8> {
    let mut c = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    for p in parts {
        c.update(p);
    }
    c.finish().as_ref().to_vec()
}

fn ok(affected: u64, insert_id: u64) -> Vec<u8> {
    let mut out = vec![0];
    lenenc(&mut out, affected);
    lenenc(&mut out, insert_id);
    out.extend_from_slice(&[2, 0, 0, 0]);
    out
}

fn err(code: u16, state: &str, message: &str) -> Vec<u8> {
    let mut out = vec![0xff];
    out.extend_from_slice(&code.to_le_bytes());
    out.push(b'#');
    out.extend_from_slice(state.as_bytes());
    out.extend_from_slice(message.as_bytes());
    out
}

fn column_definition(c: &Col) -> Vec<u8> {
    let mut out = Vec::new();
    for part in ["def", "", "t", "t"] {
        lenenc_bytes(&mut out, part.as_bytes());
    }
    lenenc_bytes(&mut out, c.name.as_bytes());
    lenenc_bytes(&mut out, c.name.as_bytes());
    out.push(0x0c);
    out.extend_from_slice(&c.charset.to_le_bytes());
    out.extend_from_slice(&255u32.to_le_bytes());
    out.push(c.kind);
    out.extend_from_slice(&c.flags.to_le_bytes());
    out.push(c.decimals);
    out.extend_from_slice(&[0, 0]);
    out
}

fn binary_row(columns: &[Col], cells: &[Cell]) -> Vec<u8> {
    let mut out = vec![0];
    let mut nulls = vec![0u8; (columns.len() + 9) / 8];
    let mut values = Vec::new();
    for (i, (c, cell)) in columns.iter().zip(cells).enumerate() {
        match cell {
            Cell::Null => nulls[(i + 2) / 8] |= 1 << ((i + 2) % 8),
            Cell::Int(n) => match c.kind {
                TINY => values.push(*n as u8),
                _ => values.extend_from_slice(&n.to_le_bytes()),
            },
            Cell::Double(f) => values.extend_from_slice(&f.to_le_bytes()),
            Cell::Text(t) => lenenc_bytes(&mut values, t.as_bytes()),
            Cell::Bytes(b) => lenenc_bytes(&mut values, b),
            Cell::DateTime(y, mo, d, h, mi, s, us) => {
                values.push(11);
                values.extend_from_slice(&y.to_le_bytes());
                values.extend_from_slice(&[*mo, *d, *h, *mi, *s]);
                values.extend_from_slice(&us.to_le_bytes());
            }
        }
    }
    out.extend_from_slice(&nulls);
    out.extend_from_slice(&values);
    out
}

/// The values of a COM_STMT_EXECUTE.
fn values(body: &[u8], count: usize) -> Vec<Val> {
    if count == 0 {
        return Vec::new();
    }
    let mut at = 10;
    let nulls = body[at..at + count.div_ceil(8)].to_vec();
    at += count.div_ceil(8);
    assert_eq!(body[at], 1, "the types are sent");
    at += 1;
    let types: Vec<u8> = (0..count).map(|i| body[at + 2 * i]).collect();
    at += 2 * count;
    let mut out = Vec::new();
    for (i, kind) in types.iter().enumerate() {
        if nulls[i / 8] & (1 << (i % 8)) != 0 {
            out.push(Val::Null);
            continue;
        }
        out.push(match *kind {
            TINY => {
                at += 1;
                Val::Tiny(body[at - 1])
            }
            LONGLONG => {
                at += 8;
                Val::Int(i64::from_le_bytes(body[at - 8..at].try_into().unwrap()))
            }
            NEWDECIMAL | VAR_STRING => {
                let n = take_lenenc(body, &mut at) as usize;
                at += n;
                let text = String::from_utf8(body[at - n..at].to_vec()).unwrap();
                if *kind == NEWDECIMAL {
                    Val::Decimal(text)
                } else {
                    Val::Text(text)
                }
            }
            DATETIME => {
                let n = usize::from(body[at]);
                let b = &body[at + 1..at + 1 + n];
                at += 1 + n;
                let micros = u32::from_le_bytes(b[7..11].try_into().unwrap());
                Val::DateTime(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{micros:06}",
                    u16::from_le_bytes([b[0], b[1]]),
                    b[2],
                    b[3],
                    b[4],
                    b[5],
                    b[6]
                ))
            }
            6 => Val::Null,
            other => panic!("a type the stand-in does not read: {other}"),
        });
    }
    out
}

async fn serve(
    mut s: TcpStream,
    answer: Arc<Answer>,
    log: Arc<Mutex<Vec<String>>>,
    executed: Executed,
) -> Option<()> {
    let nonce: Vec<u8> = (1..=20).collect();
    let caps: u32 = 0x1 | 0x4 | 0x8 | 0x200 | 0x2000 | 0x8000 | 0x2_0000 | 0x8_0000 | 0x20_0000 | 0x100_0000;
    let mut hello = vec![10];
    hello.extend_from_slice(b"8.4.0-stand-in\0");
    hello.extend_from_slice(&7u32.to_le_bytes());
    hello.extend_from_slice(&nonce[..8]);
    hello.push(0);
    hello.extend_from_slice(&(caps as u16).to_le_bytes());
    hello.push(45);
    hello.extend_from_slice(&2u16.to_le_bytes());
    hello.extend_from_slice(&((caps >> 16) as u16).to_le_bytes());
    hello.push(21);
    hello.extend_from_slice(&[0; 10]);
    hello.extend_from_slice(&nonce[8..]);
    hello.push(0);
    hello.extend_from_slice(b"mysql_native_password\0");
    let mut seq = 0;
    write_packet(&mut s, &mut seq, &hello).await;
    let (got, response) = read_packet(&mut s).await?;
    seq = got.wrapping_add(1);
    // caps, max packet, charset, 23 zeros, then the user's name.
    let mut at = 32;
    let end = at + response[at..].iter().position(|b| *b == 0).unwrap();
    let user = String::from_utf8(response[at..end].to_vec()).unwrap();
    at = end + 1;
    let n = take_lenenc(&response, &mut at) as usize;
    let scramble = response[at..at + n].to_vec();
    let stage1 = sha1(&[PASSWORD.as_bytes()]);
    let stage2 = sha1(&[&stage1]);
    let mix = sha1(&[&nonce, &stage2]);
    let expected: Vec<u8> = stage1.iter().zip(&mix).map(|(a, b)| a ^ b).collect();
    if user != "shop" || scramble != expected {
        write_packet(&mut s, &mut seq, &err(1045, "28000", "Access denied for user")).await;
        return None;
    }
    write_packet(&mut s, &mut seq, &ok(0, 0)).await;
    let mut session = Session::default();
    let mut prepared: Vec<(String, usize)> = Vec::new();
    loop {
        let (_, body) = read_packet(&mut s).await?;
        let mut seq = 1;
        match body.first()? {
            0x01 => return Some(()),
            0x03 => {
                let sql = String::from_utf8_lossy(&body[1..]).into_owned();
                log.lock().unwrap().push(format!("query: {sql}"));
                match sql.as_str() {
                    "START TRANSACTION READ ONLY" => {
                        session = Session {
                            read_only: true,
                            in_transaction: true,
                        };
                    }
                    "START TRANSACTION" => session.in_transaction = true,
                    "COMMIT" | "ROLLBACK" => session = Session::default(),
                    _ => {}
                }
                write_packet(&mut s, &mut seq, &ok(0, 0)).await;
            }
            0x16 => {
                let sql = String::from_utf8_lossy(&body[1..]).into_owned();
                let count = sql.matches('?').count();
                prepared.push((sql, count));
                let mut p = vec![0];
                p.extend_from_slice(&(prepared.len() as u32).to_le_bytes());
                p.extend_from_slice(&0u16.to_le_bytes());
                p.extend_from_slice(&(count as u16).to_le_bytes());
                p.extend_from_slice(&[0, 0, 0]);
                write_packet(&mut s, &mut seq, &p).await;
                for _ in 0..count {
                    write_packet(&mut s, &mut seq, &column_definition(&col("?", VAR_STRING))).await;
                }
            }
            0x17 => {
                let id = u32::from_le_bytes(body[1..5].try_into().unwrap()) as usize;
                let (sql, count) = prepared[id - 1].clone();
                let given = values(&body, count);
                log.lock().unwrap().push(format!("execute: {sql}"));
                executed.lock().unwrap().push((sql.clone(), given.clone()));
                let mut reply = answer(&sql, &given, &session);
                while let Reply::After(wait, then) = reply {
                    tokio::time::sleep(wait).await;
                    reply = *then;
                }
                match reply {
                    Reply::Rows(columns, rows) => {
                        let mut count = Vec::new();
                        lenenc(&mut count, columns.len() as u64);
                        write_packet(&mut s, &mut seq, &count).await;
                        for c in &columns {
                            write_packet(&mut s, &mut seq, &column_definition(c)).await;
                        }
                        for r in &rows {
                            write_packet(&mut s, &mut seq, &binary_row(&columns, r)).await;
                        }
                        write_packet(&mut s, &mut seq, &[0xfe, 0, 0, 2, 0, 0, 0]).await;
                    }
                    Reply::Done { affected, insert_id } => {
                        write_packet(&mut s, &mut seq, &ok(affected, insert_id)).await;
                    }
                    Reply::Error(code, state, message) => {
                        write_packet(&mut s, &mut seq, &err(code, state, &message)).await;
                    }
                    Reply::After(..) => unreachable!(),
                }
            }
            0x19 => {}
            other => {
                write_packet(
                    &mut s,
                    &mut seq,
                    &err(1047, "08S01", &format!("unknown command {other}")),
                )
                .await;
            }
        }
    }
}
