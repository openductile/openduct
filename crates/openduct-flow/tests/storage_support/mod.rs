// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Stand-ins for the storage tests, each a server on this machine in a thread of its own: an S3 store that checks
//! every request's signature with a signer of its own (so the step's signer and this one check each other), an FTP
//! server whose passive answers name an address that is not its own, and a Redis server (RESP) with channels.

#![allow(
    dead_code,
    clippy::too_many_arguments,
    clippy::collapsible_if,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

// ------------------------------------------------------------------------------------------------------------- S3

pub(crate) const ACCESS: &str = "AKIDSTANDIN";
pub(crate) const SECRET: &str = "standin/secret+key";

#[derive(Debug, Default)]
pub(crate) struct S3State {
    pub(crate) objects: BTreeMap<String, (Vec<u8>, String)>,
    pub(crate) refused_signatures: usize,
    pub(crate) requests: Vec<String>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha(bytes: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    ring::hmac::sign(&k, data.as_bytes()).as_ref().to_vec()
}

fn decode(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 3 <= b.len() {
            if let Ok(v) = u8::from_str_radix(&text[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn encode(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// The signature S3 would compute: from the request as it came, by this stand-in's own reading of the rules.
fn expected(
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &[(String, String)],
    signed: &str,
    payload: &str,
    stamp: &str,
    scope: &str,
) -> String {
    let mut q: Vec<(String, String)> = query
        .iter()
        .filter(|(k, _)| k != "X-Amz-Signature")
        .map(|(k, v)| (encode(k), encode(v)))
        .collect();
    q.sort();
    let q = q
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let mut canonical_headers = String::new();
    for name in signed.split(';') {
        let value = headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.trim().to_owned())
            .unwrap_or_default();
        canonical_headers.push_str(&format!("{name}:{value}\n"));
    }
    let canonical = format!("{method}\n{path}\n{q}\n{canonical_headers}\n{signed}\n{payload}");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        sha(canonical.as_bytes())
    );
    let parts: Vec<&str> = scope.split('/').collect();
    let key = hmac(
        &hmac(
            &hmac(&hmac(format!("AWS4{SECRET}").as_bytes(), parts[0]), parts[1]),
            parts[2],
        ),
        "aws4_request",
    );
    hex(&hmac(&key, &to_sign))
}

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut words = line.split_whitespace();
    let method = words.next()?.to_owned();
    let target = words.next()?.to_owned();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).ok()?;
        if h.trim().is_empty() {
            break;
        }
        let (k, v) = h.split_once(':')?;
        headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
    }
    let length: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .map_or(0, |(_, v)| v.parse().unwrap());
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let query = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect();
    Some(Request {
        method,
        path: path.to_owned(),
        query,
        headers,
        body,
    })
}

fn answer(stream: &mut TcpStream, status: &str, headers: &[(&str, String)], body: &[u8], head: bool) {
    let mut out = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", body.len());
    for (k, v) in headers {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes());
    if !head {
        let _ = stream.write_all(body);
    }
}

fn signed_ok(r: &Request) -> bool {
    let header = |n: &str| r.headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
    if let Some(auth) = header("authorization") {
        let credential = auth
            .split("Credential=")
            .nth(1)
            .and_then(|c| c.split(',').next())
            .unwrap_or_default();
        let (access, scope) = credential.split_once('/').unwrap_or_default();
        let signed = auth
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|c| c.split(',').next())
            .unwrap_or_default();
        let given = auth.split("Signature=").nth(1).unwrap_or_default();
        let payload = header("x-amz-content-sha256").unwrap_or_default();
        if payload != sha(&r.body) || access != ACCESS {
            return false;
        }
        let stamp = header("x-amz-date").unwrap_or_default();
        return expected(
            &r.method, &r.path, &r.query, &r.headers, signed, &payload, &stamp, scope,
        ) == given;
    }
    let q = |n: &str| {
        r.query
            .iter()
            .find(|(k, _)| k == n)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let (access, scope) = q("X-Amz-Credential")
        .split_once('/')
        .map(|(a, s)| (a.to_owned(), s.to_owned()))
        .unwrap_or_default();
    access == ACCESS
        && expected(
            &r.method,
            &r.path,
            &r.query,
            &r.headers,
            &q("X-Amz-SignedHeaders"),
            "UNSIGNED-PAYLOAD",
            &q("X-Amz-Date"),
            &scope,
        ) == q("X-Amz-Signature")
}

fn xml_escape(t: &str) -> String {
    t.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn serve_s3(mut stream: TcpStream, state: Arc<Mutex<S3State>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut closing = false;
    loop {
        if closing {
            return;
        }
        let Some(r) = read_request(&mut reader) else {
            return;
        };
        let head = r.method == "HEAD";
        closing = r
            .headers
            .iter()
            .any(|(k, v)| k == "connection" && v.eq_ignore_ascii_case("close"));
        state
            .lock()
            .unwrap()
            .requests
            .push(format!("{} {}", r.method, r.path));
        if !signed_ok(&r) {
            state.lock().unwrap().refused_signatures += 1;
            answer(
                &mut stream,
                "403 Forbidden",
                &[],
                b"<Error><Code>SignatureDoesNotMatch</Code></Error>",
                head,
            );
            continue;
        }
        let path = decode(&r.path);
        let (bucket, key) = path
            .trim_start_matches('/')
            .split_once('/')
            .unwrap_or((path.trim_start_matches('/'), ""));
        if bucket != "orders" {
            answer(
                &mut stream,
                "404 Not Found",
                &[],
                b"<Error><Code>NoSuchBucket</Code></Error>",
                head,
            );
            continue;
        }
        let header = |n: &str| r.headers.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone());
        let mut s = state.lock().unwrap();
        match (r.method.as_str(), key.is_empty()) {
            ("GET", true) => {
                let q = |n: &str| {
                    r.query
                        .iter()
                        .find(|(k, _)| k == n)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default()
                };
                let (prefix, delimiter) = (q("prefix"), q("delimiter"));
                let mut contents = String::new();
                let mut folders = std::collections::BTreeSet::new();
                for (k, (bytes, _)) in s.objects.iter().filter(|(k, _)| k.starts_with(&prefix)) {
                    let rest = &k[prefix.len()..];
                    if !delimiter.is_empty()
                        && let Some(at) = rest.find('/')
                    {
                        folders.insert(format!("{prefix}{}", &rest[..=at]));
                        continue;
                    }
                    contents.push_str(&format!(
                        "<Contents><Key>{}</Key><LastModified>2026-11-27T10:00:00.000Z</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size></Contents>",
                        xml_escape(k), &sha(bytes)[..16], bytes.len()
                    ));
                }
                let prefixes: String = folders
                    .iter()
                    .map(|f| {
                        format!(
                            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                            xml_escape(f)
                        )
                    })
                    .collect();
                let body = format!(
                    "<ListBucketResult><IsTruncated>false</IsTruncated>{contents}{prefixes}</ListBucketResult>"
                );
                answer(
                    &mut stream,
                    "200 OK",
                    &[("content-type", "application/xml".into())],
                    body.as_bytes(),
                    head,
                );
            }
            ("GET" | "HEAD", false) => match s.objects.get(key) {
                Some((bytes, kind)) => {
                    let mut out = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: {kind}\r\nlast-modified: Fri, 27 Nov 2026 10:00:00 GMT\r\n\r\n",
                        bytes.len()
                    );
                    if head {
                        let _ = stream.write_all(out.as_bytes());
                    } else {
                        let mut all = std::mem::take(&mut out).into_bytes();
                        all.extend_from_slice(bytes);
                        let _ = stream.write_all(&all);
                    }
                }
                None => answer(
                    &mut stream,
                    "404 Not Found",
                    &[],
                    b"<Error><Code>NoSuchKey</Code></Error>",
                    head,
                ),
            },
            ("PUT", false) => {
                if header("if-none-match").as_deref() == Some("*") && s.objects.contains_key(key) {
                    answer(
                        &mut stream,
                        "412 Precondition Failed",
                        &[],
                        b"<Error><Code>PreconditionFailed</Code></Error>",
                        head,
                    );
                } else if let Some(source) = header("x-amz-copy-source") {
                    let source = decode(&source);
                    let from = source
                        .trim_start_matches('/')
                        .split_once('/')
                        .map(|(_, k)| k.to_owned())
                        .unwrap_or_default();
                    match s.objects.get(&from).cloned() {
                        Some(o) => {
                            s.objects.insert(key.to_owned(), o);
                            answer(
                                &mut stream,
                                "200 OK",
                                &[],
                                b"<CopyObjectResult></CopyObjectResult>",
                                head,
                            );
                        }
                        None => answer(
                            &mut stream,
                            "404 Not Found",
                            &[],
                            b"<Error><Code>NoSuchKey</Code></Error>",
                            head,
                        ),
                    }
                } else {
                    let kind = header("content-type").unwrap_or_default();
                    s.objects.insert(key.to_owned(), (r.body.clone(), kind));
                    answer(&mut stream, "200 OK", &[], b"", head);
                }
            }
            ("DELETE", false) => {
                s.objects.remove(key);
                answer(&mut stream, "204 No Content", &[], b"", head);
            }
            _ => answer(&mut stream, "400 Bad Request", &[], b"", head),
        }
    }
}

/// An S3 stand-in: its address, and what it holds.
pub(crate) fn s3() -> (String, Arc<Mutex<S3State>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(S3State::default()));
    let serving = Arc::clone(&state);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let state = Arc::clone(&serving);
            std::thread::spawn(move || serve_s3(stream, state));
        }
    });
    (address, state)
}

/// A presigned address fetched: its status and body.
pub(crate) fn fetch(url: &str) -> (u16, Vec<u8>) {
    let rest = url.strip_prefix("http://").unwrap();
    let (host, target) = rest.split_once('/').unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    write!(
        stream,
        "GET /{target} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n\r\n"
    )
    .unwrap();
    let mut all = Vec::new();
    stream.read_to_end(&mut all).unwrap();
    let text = String::from_utf8_lossy(&all).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.as_bytes().to_vec())
        .unwrap_or_default();
    (status, body)
}

// ------------------------------------------------------------------------------------------------------------ FTP

#[derive(Debug, Default)]
pub(crate) struct FtpState {
    pub(crate) commands: Vec<String>,
}

fn place(root: &Path, arg: &str) -> PathBuf {
    let rel = arg.trim_start_matches('/');
    if rel.is_empty() || rel == "." {
        root.to_path_buf()
    } else {
        root.join(rel)
    }
}

fn serve_ftp(stream: TcpStream, root: PathBuf, state: Arc<Mutex<FtpState>>) {
    let mut out = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let say = |out: &mut TcpStream, line: &str| {
        let _ = out.write_all(format!("{line}\r\n").as_bytes());
    };
    say(&mut out, "220 stand-in ready");
    let mut data: Option<TcpListener> = None;
    let mut rename_from: Option<PathBuf> = None;
    let accept = |data: &mut Option<TcpListener>| data.take().and_then(|l| l.accept().ok()).map(|(s, _)| s);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let line = line.trim_end().to_owned();
        let (command, arg) = line.split_once(' ').unwrap_or((&line, ""));
        let command = command.to_ascii_uppercase();
        state.lock().unwrap().commands.push(if command == "PASS" {
            "PASS ***".into()
        } else {
            line.clone()
        });
        let path = place(&root, arg);
        match command.as_str() {
            "AUTH" => say(&mut out, "502 TLS is not offered here"),
            "USER" => say(&mut out, "331 password, please"),
            "PASS" => say(&mut out, if arg == "secret" { "230 in" } else { "530 no" }),
            "TYPE" | "OPTS" => say(&mut out, "200 ok"),
            "PWD" => say(&mut out, "257 \"/\" is the folder"),
            "EPSV" => say(&mut out, "500 not understood"),
            "PASV" => {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let port = l.local_addr().unwrap().port();
                data = Some(l);
                // An address that is not this server's: a client that went there would never connect.
                say(
                    &mut out,
                    &format!(
                        "227 Entering Passive Mode (10,9,9,9,{},{})",
                        port / 256,
                        port % 256
                    ),
                );
            }
            "MLSD" | "LIST" => {
                if !path.is_dir() {
                    say(&mut out, "550 no such folder");
                    data = None;
                    continue;
                }
                say(&mut out, "150 here it comes");
                let mut d = accept(&mut data).unwrap();
                let mut names: Vec<_> = std::fs::read_dir(&path).unwrap().flatten().collect();
                names.sort_by_key(|e| e.file_name());
                for e in names {
                    let m = e.metadata().unwrap();
                    let kind = if m.is_dir() { "dir" } else { "file" };
                    let _ = write!(
                        d,
                        "type={kind};size={};modify=20261127100000; {}\r\n",
                        m.len(),
                        e.file_name().to_string_lossy()
                    );
                }
                drop(d);
                say(&mut out, "226 done");
            }
            "RETR" => match std::fs::read(&path) {
                Ok(bytes) => {
                    say(&mut out, "150 here it comes");
                    let mut d = accept(&mut data).unwrap();
                    d.write_all(&bytes).unwrap();
                    drop(d);
                    say(&mut out, "226 done");
                }
                Err(_) => {
                    data = None;
                    say(&mut out, "550 no such file");
                }
            },
            "STOR" => {
                if !path.parent().is_some_and(Path::is_dir) {
                    data = None;
                    say(&mut out, "553 no such folder");
                    continue;
                }
                say(&mut out, "150 send it");
                let mut d = accept(&mut data).unwrap();
                let mut bytes = Vec::new();
                d.read_to_end(&mut bytes).unwrap();
                std::fs::write(&path, bytes).unwrap();
                say(&mut out, "226 kept");
            }
            "SIZE" => match std::fs::metadata(&path) {
                Ok(m) if m.is_file() => say(&mut out, &format!("213 {}", m.len())),
                _ => say(&mut out, "550 no such file"),
            },
            "MLST" => match std::fs::metadata(&path) {
                Ok(m) => {
                    let kind = if m.is_dir() { "dir" } else { "file" };
                    say(
                        &mut out,
                        &format!("250-facts\r\n type={kind};size={}; {arg}\r\n250 end", m.len()),
                    );
                }
                Err(_) => say(&mut out, "550 nothing there"),
            },
            "CWD" => say(&mut out, if path.is_dir() { "250 ok" } else { "550 no" }),
            "MKD" => match std::fs::create_dir(&path) {
                Ok(()) => say(&mut out, &format!("257 \"{arg}\" made")),
                Err(_) => say(&mut out, "550 cannot"),
            },
            "DELE" => say(
                &mut out,
                if path.is_file() && std::fs::remove_file(&path).is_ok() {
                    "250 gone"
                } else {
                    "550 cannot"
                },
            ),
            "RMD" => say(
                &mut out,
                if std::fs::remove_dir(&path).is_ok() {
                    "250 gone"
                } else {
                    "550 cannot"
                },
            ),
            "RNFR" => {
                rename_from = Some(path);
                say(&mut out, "350 and where to");
            }
            "RNTO" => match rename_from.take().map(|from| std::fs::rename(from, &path)) {
                Some(Ok(())) => say(&mut out, "250 moved"),
                _ => say(&mut out, "550 cannot"),
            },
            "QUIT" => {
                say(&mut out, "221 bye");
                return;
            }
            _ => say(&mut out, "502 not here"),
        }
    }
}

/// An FTP stand-in over `root`: its port, and the commands it was sent.
pub(crate) fn ftp(root: &Path) -> (u16, Arc<Mutex<FtpState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = Arc::new(Mutex::new(FtpState::default()));
    let (serving, root) = (Arc::clone(&state), root.to_path_buf());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (state, root) = (Arc::clone(&serving), root.clone());
            std::thread::spawn(move || serve_ftp(stream, root, state));
        }
    });
    (port, state)
}

// ---------------------------------------------------------------------------------------------------------- Redis

#[derive(Debug, Clone)]
enum Held {
    Text(Vec<u8>),
    List(Vec<Vec<u8>>),
}

#[derive(Default)]
pub(crate) struct RedisState {
    data: HashMap<Vec<u8>, Held>,
    pub(crate) ttls: HashMap<Vec<u8>, u64>,
    subscribers: Vec<(String, bool, TcpStream)>,
    pub(crate) subscriptions: usize,
    pub(crate) commands: Vec<String>,
}

fn bulk(b: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", b.len()).into_bytes();
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
    out
}

fn array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for i in items {
        out.extend(bulk(i));
    }
    out
}

fn read_command(reader: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::new();
    for _ in 0..n {
        let mut len = String::new();
        reader.read_line(&mut len).ok()?;
        let len: usize = len.trim().strip_prefix('$')?.parse().ok()?;
        let mut b = vec![0; len + 2];
        reader.read_exact(&mut b).ok()?;
        b.truncate(len);
        args.push(b);
    }
    Some(args)
}

fn glob(pattern: &str, text: &str) -> bool {
    match pattern.split_once('*') {
        Some((head, tail)) => {
            text.starts_with(head) && text.ends_with(tail) && text.len() >= head.len() + tail.len()
        }
        None => pattern == text,
    }
}

fn serve_redis(stream: TcpStream, state: Arc<Mutex<RedisState>>, password: &'static str) {
    let mut out = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let mut signed_in = password.is_empty();
    while let Some(args) = read_command(&mut reader) {
        let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        let arg = |i: usize| args.get(i).cloned().unwrap_or_default();
        let text = |i: usize| String::from_utf8_lossy(&arg(i)).into_owned();
        let mut s = state.lock().unwrap();
        s.commands.push(if name == "AUTH" {
            "AUTH ***".into()
        } else {
            format!("{name} {}", text(1))
        });
        if name == "AUTH" {
            signed_in = text(args.len() - 1) == password;
            let _ = out.write_all(if signed_in {
                b"+OK\r\n"
            } else {
                b"-WRONGPASS invalid password\r\n"
            });
            continue;
        }
        if !signed_in {
            let _ = out.write_all(b"-NOAUTH Authentication required.\r\n");
            continue;
        }
        let reply: Vec<u8> = match name.as_str() {
            "SELECT" => b"+OK\r\n".to_vec(),
            "GET" => match s.data.get(&arg(1)) {
                Some(Held::Text(t)) => bulk(t),
                Some(Held::List(_)) => b"-WRONGTYPE not a value\r\n".to_vec(),
                None => b"$-1\r\n".to_vec(),
            },
            "SET" => {
                let rest: Vec<String> = (3..args.len()).map(text).collect();
                if rest.iter().any(|r| r == "NX") && s.data.contains_key(&arg(1)) {
                    b"$-1\r\n".to_vec()
                } else {
                    if let Some(at) = rest.iter().position(|r| r == "PX") {
                        let ms = rest[at + 1].parse().unwrap();
                        s.ttls.insert(arg(1), ms);
                    }
                    s.data.insert(arg(1), Held::Text(arg(2)));
                    b"+OK\r\n".to_vec()
                }
            }
            "DEL" => format!(":{}\r\n", u8::from(s.data.remove(&arg(1)).is_some())).into_bytes(),
            "EXISTS" => format!(":{}\r\n", u8::from(s.data.contains_key(&arg(1)))).into_bytes(),
            "PEXPIRE" => {
                let had = s.data.contains_key(&arg(1));
                if had {
                    s.ttls.insert(arg(1), text(2).parse().unwrap());
                }
                format!(":{}\r\n", u8::from(had)).into_bytes()
            }
            "INCRBY" => {
                let now: i64 = match s.data.get(&arg(1)) {
                    Some(Held::Text(t)) => String::from_utf8_lossy(t).parse().unwrap_or(0),
                    _ => 0,
                };
                let next = now + text(2).parse::<i64>().unwrap();
                s.data.insert(arg(1), Held::Text(next.to_string().into_bytes()));
                format!(":{next}\r\n").into_bytes()
            }
            "LPUSH" | "RPUSH" => {
                let entry = s.data.entry(arg(1)).or_insert_with(|| Held::List(Vec::new()));
                let Held::List(list) = entry else {
                    panic!("not a list")
                };
                if name == "LPUSH" {
                    list.insert(0, arg(2))
                } else {
                    list.push(arg(2))
                }
                format!(":{}\r\n", list.len()).into_bytes()
            }
            "LPOP" | "RPOP" => match s.data.get_mut(&arg(1)) {
                Some(Held::List(list)) if !list.is_empty() => {
                    let v = if name == "LPOP" {
                        list.remove(0)
                    } else {
                        list.pop().unwrap()
                    };
                    bulk(&v)
                }
                _ => b"$-1\r\n".to_vec(),
            },
            "LRANGE" => match s.data.get(&arg(1)) {
                Some(Held::List(list)) => {
                    let len = list.len() as i64;
                    let at = |i: i64| if i < 0 { (len + i).max(0) } else { i.min(len) };
                    let (from, to) = (
                        at(text(2).parse().unwrap()),
                        at(text(3).parse::<i64>().unwrap()) + 1,
                    );
                    array(&list[from as usize..(to.min(len)).max(from) as usize])
                }
                _ => b"*0\r\n".to_vec(),
            },
            "PUBLISH" => {
                let (channel, message) = (text(1), arg(2));
                let mut received = 0;
                for (wanted, pattern, sub) in &mut s.subscribers {
                    let hit = if *pattern {
                        glob(wanted, &channel)
                    } else {
                        *wanted == channel
                    };
                    if hit {
                        let frame = if *pattern {
                            array(&[
                                b"pmessage".to_vec(),
                                wanted.clone().into_bytes(),
                                channel.clone().into_bytes(),
                                message.clone(),
                            ])
                        } else {
                            array(&[b"message".to_vec(), channel.clone().into_bytes(), message.clone()])
                        };
                        if sub.write_all(&frame).is_ok() {
                            received += 1;
                        }
                    }
                }
                format!(":{received}\r\n").into_bytes()
            }
            "SUBSCRIBE" | "PSUBSCRIBE" => {
                let pattern = name == "PSUBSCRIBE";
                s.subscribers.push((text(1), pattern, out.try_clone().unwrap()));
                s.subscriptions += 1;
                let kind: &[u8] = if pattern { b"psubscribe" } else { b"subscribe" };
                let mut r = b"*3\r\n".to_vec();
                r.extend(bulk(kind));
                r.extend(bulk(&arg(1)));
                r.extend(b":1\r\n");
                r
            }
            _ => b"-ERR unknown command\r\n".to_vec(),
        };
        drop(s);
        let _ = out.write_all(&reply);
    }
}

/// A Redis stand-in (with `password` when it is not empty): its port, and what it holds.
pub(crate) fn redis(password: &'static str) -> (u16, Arc<Mutex<RedisState>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = Arc::new(Mutex::new(RedisState::default()));
    let serving = Arc::clone(&state);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let state = Arc::clone(&serving);
            std::thread::spawn(move || serve_redis(stream, state, password));
        }
    });
    (port, state)
}

/// Publishes `message` to `channel` of the stand-in at `port`: how many subscribers got it.
pub(crate) fn publish(port: u16, channel: &str, message: &str) -> i64 {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(&array(&[
            b"PUBLISH".to_vec(),
            channel.as_bytes().to_vec(),
            message.as_bytes().to_vec(),
        ]))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line.trim().trim_start_matches(':').parse().unwrap_or(-1)
}
