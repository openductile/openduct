// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A stand-in IMAP server, on this machine, speaking as much of IMAP (RFC 9051 and 3501) as the mailbox step and
//! trigger use: LOGIN and AUTHENTICATE XOAUTH2, SELECT and EXAMINE, UID SEARCH, UID FETCH (headers, whole messages),
//! UID STORE, UID MOVE, UID COPY, UID EXPUNGE, LIST, IDLE (news of a message added while a client waits), LOGOUT -
//! over plain TCP or TLS. Each command is kept, so a test can say what was sent.

#![allow(dead_code, unreachable_pub, clippy::unwrap_used, clippy::indexing_slicing)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;

#[derive(Debug, Clone)]
pub struct Message {
    pub uid: u64,
    pub flags: Vec<String>,
    pub raw: Vec<u8>,
}

#[derive(Debug)]
pub struct Folder {
    pub uidvalidity: u64,
    pub uidnext: u64,
    pub messages: Vec<Message>,
}

impl Folder {
    fn new(uidvalidity: u64) -> Self {
        Self {
            uidvalidity,
            uidnext: 1,
            messages: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct State {
    pub folders: BTreeMap<String, Folder>,
    pub commands: Vec<String>,
    pub password: String,
    pub tokens: Vec<String>,
    pub capabilities: Vec<String>,
    pub sessions: usize,
    /// Answers a FETCH of a whole message by claiming a literal far larger than it sends (a hostile server).
    pub lie_about_size: bool,
}

impl State {
    pub fn add(&mut self, folder: &str, raw: &str) -> u64 {
        let f = self
            .folders
            .entry(folder.to_owned())
            .or_insert_with(|| Folder::new(7));
        let uid = f.uidnext;
        f.uidnext += 1;
        f.messages.push(Message {
            uid,
            flags: Vec::new(),
            raw: raw.replace('\n', "\r\n").into_bytes(),
        });
        uid
    }

    /// The folder rebuilt: its messages numbered again from 1, under another UIDVALIDITY.
    pub fn rebuild(&mut self, folder: &str, uidvalidity: u64) {
        if let Some(f) = self.folders.get_mut(folder) {
            f.uidvalidity = uidvalidity;
            f.uidnext = 1;
            for m in &mut f.messages {
                m.uid = f.uidnext;
                f.uidnext += 1;
            }
        }
    }
}

pub type Shared = Arc<Mutex<State>>;

pub fn message(from: &str, subject: &str, body: &str) -> String {
    format!(
        "From: {from}\nTo: accounts@example.com\nSubject: {subject}\nDate: Mon, 23 Nov 2026 10:00:00 +0000\n\
         Message-ID: <{}@example.com>\nContent-Type: text/plain; charset=utf-8\n\n{body}\n",
        subject.len()
    )
}

/// A message with a CSV attached.
pub fn invoice(number: &str, total: &str) -> String {
    let csv = base64::engine::general_purpose::STANDARD.encode(format!("number,total\n{number},{total}\n"));
    format!(
        "From: Billing <billing@example.com>\nTo: accounts@example.com\nSubject: Invoice {number}\n\
         Date: Mon, 23 Nov 2026 10:00:00 +0000\nMIME-Version: 1.0\nContent-Type: multipart/mixed; boundary=b1\n\n\
         --b1\nContent-Type: text/plain\n\nInvoice {number} is attached.\n\
         --b1\nContent-Type: text/csv; name=invoice-{number}.csv\nContent-Disposition: attachment; \
         filename=invoice-{number}.csv\nContent-Transfer-Encoding: base64\n\n{csv}\n--b1--\n"
    )
}

/// Starts the stand-in; its port and state. With `tls`, the certificate and key it shows (DER).
pub fn start(tls: Option<(Vec<u8>, Vec<u8>)>) -> (u16, Shared) {
    let state: Shared = Arc::new(Mutex::new(State {
        password: "s3cret".into(),
        capabilities: ["IMAP4rev1", "IDLE", "MOVE", "UIDPLUS", "AUTH=XOAUTH2"]
            .map(str::to_owned)
            .to_vec(),
        ..State::default()
    }));
    state
        .lock()
        .unwrap()
        .folders
        .insert("INBOX".into(), Folder::new(7));
    state
        .lock()
        .unwrap()
        .folders
        .insert("Archive".into(), Folder::new(9));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = tls.map(|(cert, key)| {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![rustls::pki_types::CertificateDer::from(cert)],
                    rustls::pki_types::PrivateKeyDer::try_from(key).unwrap(),
                )
                .unwrap(),
        )
    });
    let s = Arc::clone(&state);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let state = Arc::clone(&s);
            let config = config.clone();
            std::thread::spawn(move || {
                state.lock().unwrap().sessions += 1;
                match config {
                    Some(c) => {
                        let conn = rustls::ServerConnection::new(c).unwrap();
                        let tls = rustls::StreamOwned::new(conn, stream.try_clone().unwrap());
                        serve(tls, &stream, &state);
                    }
                    None => {
                        let io = stream.try_clone().unwrap();
                        serve(io, &stream, &state);
                    }
                }
            });
        }
    });
    (port, state)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Atom(String),
    Str(String),
    List(Vec<Token>),
}

fn tokens(line: &str) -> Vec<Token> {
    fn parse(chars: &mut std::iter::Peekable<std::str::Chars<'_>>, inside: bool) -> Vec<Token> {
        let mut out = Vec::new();
        while let Some(&c) = chars.peek() {
            match c {
                ' ' => {
                    chars.next();
                }
                '(' => {
                    chars.next();
                    out.push(Token::List(parse(chars, true)));
                }
                ')' if inside => {
                    chars.next();
                    return out;
                }
                '"' => {
                    chars.next();
                    let mut s = String::new();
                    while let Some(c) = chars.next() {
                        match c {
                            '\\' => s.extend(chars.next()),
                            '"' => break,
                            _ => s.push(c),
                        }
                    }
                    out.push(Token::Str(s));
                }
                _ => {
                    let mut s = String::new();
                    let mut square = 0;
                    while let Some(&c) = chars.peek() {
                        if (c == ' ' || c == ')' || c == '(') && square == 0 {
                            break;
                        }
                        if c == '[' {
                            square += 1;
                        }
                        if c == ']' {
                            square -= 1;
                        }
                        s.push(c);
                        chars.next();
                    }
                    out.push(Token::Atom(s));
                }
            }
        }
        out
    }
    parse(&mut line.chars().peekable(), false)
}

fn text(t: &Token) -> String {
    match t {
        Token::Atom(a) | Token::Str(a) => a.clone(),
        Token::List(_) => String::new(),
    }
}

/// Reads one command: literals (`{n}`) asked for with `+` and put back in the line, quoted.
fn command<S: Read + Write>(reader: &mut BufReader<S>) -> Option<String> {
    let mut whole = String::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            return None;
        }
        let line = line.trim_end_matches(['\r', '\n']).to_owned();
        if let Some(open) = line.rfind('{')
            && line.ends_with('}')
            && let Ok(n) = line[open + 1..line.len() - 1].parse::<usize>()
        {
            whole.push_str(&line[..open]);
            let writer = reader.get_mut();
            writer.write_all(b"+ go on\r\n").ok()?;
            writer.flush().ok()?;
            let mut bytes = vec![0u8; n];
            reader.read_exact(&mut bytes).ok()?;
            let literal = String::from_utf8_lossy(&bytes)
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            whole.push('"');
            whole.push_str(&literal);
            whole.push('"');
            continue;
        }
        whole.push_str(&line);
        return Some(whole);
    }
}

fn uids_of(set: &str, folder: &Folder) -> Vec<u64> {
    let top = folder.messages.iter().map(|m| m.uid).max().unwrap_or(0);
    let value = |s: &str| if s == "*" { top } else { s.parse().unwrap_or(0) };
    let mut out = Vec::new();
    for part in set.split(',') {
        let (a, b) = match part.split_once(':') {
            Some((a, b)) => (value(a), value(b)),
            None => (value(part), value(part)),
        };
        let (lo, hi) = (a.min(b), a.max(b));
        out.extend(
            folder
                .messages
                .iter()
                .map(|m| m.uid)
                .filter(|u| *u >= lo && *u <= hi),
        );
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn header_of(raw: &[u8], name: &str) -> String {
    let text = String::from_utf8_lossy(raw);
    let head = text.split("\r\n\r\n").next().unwrap_or_default();
    head.lines()
        .find_map(|l| {
            let (n, v) = l.split_once(':')?;
            n.eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
        })
        .unwrap_or_default()
}

fn matches(m: &Message, criteria: &[Token], folder: &Folder) -> bool {
    let mut i = 0;
    while i < criteria.len() {
        let key = text(&criteria[i]).to_ascii_uppercase();
        let arg = criteria.get(i + 1).map(text).unwrap_or_default();
        let ok = match key.as_str() {
            "ALL" => true,
            "CHARSET" => {
                i += 2;
                continue;
            }
            "UNSEEN" => !m.flags.iter().any(|f| f == "\\Seen"),
            "SEEN" => m.flags.iter().any(|f| f == "\\Seen"),
            "FLAGGED" => m.flags.iter().any(|f| f == "\\Flagged"),
            "UID" => {
                i += 1;
                uids_of(&arg, folder).contains(&m.uid)
            }
            "FROM" | "TO" | "SUBJECT" => {
                i += 1;
                header_of(&m.raw, &key)
                    .to_lowercase()
                    .contains(&arg.to_lowercase())
            }
            "TEXT" => {
                i += 1;
                String::from_utf8_lossy(&m.raw)
                    .to_lowercase()
                    .contains(&arg.to_lowercase())
            }
            "SINCE" | "BEFORE" => {
                i += 1;
                true
            }
            _ => false,
        };
        if !ok {
            return false;
        }
        i += 1;
    }
    true
}

fn fetch_item(m: &Message, item: &str, out: &mut Vec<u8>, lie: bool) {
    let upper = item.to_ascii_uppercase();
    let literal = |out: &mut Vec<u8>, name: &str, bytes: &[u8], claim: Option<usize>| {
        out.extend_from_slice(format!("{name} {{{}}}\r\n", claim.unwrap_or(bytes.len())).as_bytes());
        out.extend_from_slice(bytes);
    };
    if upper == "UID" {
        out.extend_from_slice(format!("UID {}", m.uid).as_bytes());
    } else if upper == "FLAGS" {
        out.extend_from_slice(format!("FLAGS ({})", m.flags.join(" ")).as_bytes());
    } else if upper == "RFC822.SIZE" {
        out.extend_from_slice(format!("RFC822.SIZE {}", m.raw.len()).as_bytes());
    } else if upper == "BODY.PEEK[]" {
        literal(out, "BODY[]", &m.raw, lie.then_some(900_000_000));
    } else if let Some(fields) = upper.strip_prefix("BODY.PEEK[HEADER.FIELDS (") {
        let names: Vec<&str> = fields.trim_end_matches(")]").split(' ').collect();
        let text = String::from_utf8_lossy(&m.raw);
        let head = text.split("\r\n\r\n").next().unwrap_or_default();
        let mut kept = String::new();
        for l in head.split("\r\n") {
            if let Some((n, _)) = l.split_once(':')
                && names.iter().any(|w| w.eq_ignore_ascii_case(n))
            {
                kept.push_str(l);
                kept.push_str("\r\n");
            }
        }
        kept.push_str("\r\n");
        literal(
            out,
            &format!("BODY[HEADER.FIELDS ({fields}"),
            kept.as_bytes(),
            None,
        );
    }
}

/// The items a FETCH asks for, from its parenthesised list as written.
fn items_of(rest: &str) -> Vec<String> {
    let inner = rest.trim().trim_start_matches('(');
    let inner = inner.strip_suffix(')').unwrap_or(inner);
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    for c in inner.chars() {
        match c {
            '[' | '(' => {
                depth += 1;
                current.push(c);
            }
            ']' | ')' => {
                depth -= 1;
                current.push(c);
            }
            ' ' if depth == 0 => out.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    out.push(current);
    out.into_iter().filter(|s| !s.is_empty()).collect()
}

fn serve<S: Read + Write>(io: S, raw: &TcpStream, state: &Shared) {
    let mut reader = BufReader::new(io);
    macro_rules! say {
        ($($t:tt)*) => {{
            let w = reader.get_mut();
            let _ = w.write_all(format!($($t)*).as_bytes());
            let _ = w.flush();
        }};
    }
    say!("* OK [CAPABILITY IMAP4rev1] stand-in ready\r\n");
    let mut selected: Option<String> = None;
    loop {
        let Some(line) = command(&mut reader) else {
            return;
        };
        let (tag, rest) = line.split_once(' ').unwrap_or((&line, ""));
        let tag = tag.to_owned();
        let upper = rest.to_ascii_uppercase();
        state.lock().unwrap().commands.push(rest.to_owned());
        if upper == "CAPABILITY" {
            let caps = state.lock().unwrap().capabilities.join(" ");
            say!("* CAPABILITY {caps}\r\n{tag} OK done\r\n");
        } else if upper.starts_with("LOGIN ") {
            let t = tokens(&rest[6..]);
            let ok = t.len() == 2
                && text(&t[0]) == "ann@example.com"
                && text(&t[1]) == state.lock().unwrap().password;
            if ok {
                say!("{tag} OK signed in\r\n");
            } else {
                say!("{tag} NO [AUTHENTICATIONFAILED] Invalid credentials\r\n");
            }
        } else if upper.starts_with("AUTHENTICATE XOAUTH2 ") {
            let given = base64::engine::general_purpose::STANDARD
                .decode(rest[21..].trim())
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            let token = given
                .split('\x01')
                .find_map(|p| p.strip_prefix("auth=Bearer "))
                .unwrap_or_default();
            if state.lock().unwrap().tokens.iter().any(|t| t == token) {
                say!("{tag} OK signed in\r\n");
            } else {
                say!("+ eyJzdGF0dXMiOiI0MDAifQ==\r\n");
                let mut empty = String::new();
                let _ = reader.read_line(&mut empty);
                say!("{tag} NO [AUTHENTICATIONFAILED] Invalid credentials (Failure)\r\n");
            }
        } else if upper.starts_with("SELECT ") || upper.starts_with("EXAMINE ") {
            let name = text(&tokens(rest.split_once(' ').unwrap().1)[0]);
            let s = state.lock().unwrap();
            match s.folders.get(&name) {
                Some(f) => {
                    say!(
                        "* {} EXISTS\r\n* OK [UIDVALIDITY {}] ok\r\n* OK [UIDNEXT {}] ok\r\n{tag} OK done\r\n",
                        f.messages.len(),
                        f.uidvalidity,
                        f.uidnext
                    );
                    selected = Some(name);
                }
                None => say!("{tag} NO [NONEXISTENT] no such folder\r\n"),
            }
        } else if upper.starts_with("UID SEARCH") {
            let s = state.lock().unwrap();
            let f = &s.folders[selected.as_ref().unwrap()];
            let criteria = tokens(&rest[10..]);
            let found: Vec<String> = f
                .messages
                .iter()
                .filter(|m| matches(m, &criteria, f))
                .map(|m| m.uid.to_string())
                .collect();
            say!("* SEARCH {}\r\n{tag} OK done\r\n", found.join(" "));
        } else if upper.starts_with("UID FETCH ") {
            let (set, items) = rest[10..].split_once(' ').unwrap();
            let s = state.lock().unwrap();
            let f = &s.folders[selected.as_ref().unwrap()];
            let mut out = Vec::new();
            for uid in uids_of(set, f) {
                let (seq, m) = f.messages.iter().enumerate().find(|(_, m)| m.uid == uid).unwrap();
                out.extend_from_slice(format!("* {} FETCH (", seq + 1).as_bytes());
                for (i, item) in items_of(items).iter().enumerate() {
                    if i > 0 {
                        out.push(b' ');
                    }
                    fetch_item(m, item, &mut out, s.lie_about_size);
                }
                out.extend_from_slice(b")\r\n");
            }
            out.extend_from_slice(format!("{tag} OK done\r\n").as_bytes());
            drop(s);
            let w = reader.get_mut();
            let _ = w.write_all(&out);
            let _ = w.flush();
        } else if upper.starts_with("UID STORE ") {
            let parts: Vec<&str> = rest[10..].splitn(3, ' ').collect();
            let flags: Vec<String> = parts[2]
                .trim_matches(['(', ')'])
                .split(' ')
                .map(str::to_owned)
                .collect();
            let mut s = state.lock().unwrap();
            let name = selected.clone().unwrap();
            let f = s.folders.get_mut(&name).unwrap();
            let uids = uids_of(parts[0], f);
            for m in f.messages.iter_mut().filter(|m| uids.contains(&m.uid)) {
                if parts[1].starts_with('+') {
                    for fl in &flags {
                        if !m.flags.contains(fl) {
                            m.flags.push(fl.clone());
                        }
                    }
                } else {
                    m.flags.retain(|x| !flags.contains(x));
                }
            }
            say!("{tag} OK done\r\n");
        } else if upper.starts_with("UID MOVE ") || upper.starts_with("UID COPY ") {
            let moving = upper.starts_with("UID MOVE ");
            let (set, target) = rest[9..].split_once(' ').unwrap();
            let target = text(&tokens(target)[0]);
            let mut s = state.lock().unwrap();
            let name = selected.clone().unwrap();
            if !s.folders.contains_key(&target) {
                say!("{tag} NO [TRYCREATE] no such folder\r\n");
                continue;
            }
            let uids = uids_of(set, &s.folders[&name]);
            let taken: Vec<Message> = s.folders[&name]
                .messages
                .iter()
                .filter(|m| uids.contains(&m.uid))
                .cloned()
                .collect();
            if moving {
                s.folders
                    .get_mut(&name)
                    .unwrap()
                    .messages
                    .retain(|m| !uids.contains(&m.uid));
            }
            let t = s.folders.get_mut(&target).unwrap();
            for mut m in taken {
                m.uid = t.uidnext;
                t.uidnext += 1;
                t.messages.push(m);
            }
            say!("{tag} OK done\r\n");
        } else if upper.starts_with("UID EXPUNGE ") {
            let mut s = state.lock().unwrap();
            let name = selected.clone().unwrap();
            let f = s.folders.get_mut(&name).unwrap();
            let uids = uids_of(&rest[12..], f);
            f.messages
                .retain(|m| !(uids.contains(&m.uid) && m.flags.iter().any(|x| x == "\\Deleted")));
            say!("{tag} OK done\r\n");
        } else if upper.starts_with("LIST ") {
            let names: Vec<String> = state.lock().unwrap().folders.keys().cloned().collect();
            for n in names {
                say!("* LIST (\\HasNoChildren) \"/\" \"{n}\"\r\n");
            }
            say!("{tag} OK done\r\n");
        } else if upper == "IDLE" {
            say!("+ idling\r\n");
            let count = |s: &State| {
                s.folders
                    .get(selected.as_deref().unwrap_or("INBOX"))
                    .map_or(0, |f| f.messages.len())
            };
            let mut known = count(&state.lock().unwrap());
            let _ = raw.set_read_timeout(Some(Duration::from_millis(50)));
            loop {
                let mut done = String::new();
                match reader.read_line(&mut done) {
                    Ok(0) => return,
                    Ok(_) if done.trim().eq_ignore_ascii_case("DONE") => break,
                    _ => {}
                }
                let now = count(&state.lock().unwrap());
                if now > known {
                    say!("* {now} EXISTS\r\n");
                    known = now;
                }
            }
            let _ = raw.set_read_timeout(None);
            say!("{tag} OK idle done\r\n");
        } else if upper == "LOGOUT" {
            say!("* BYE bye\r\n{tag} OK done\r\n");
            return;
        } else {
            say!("{tag} BAD not known\r\n");
        }
    }
}
