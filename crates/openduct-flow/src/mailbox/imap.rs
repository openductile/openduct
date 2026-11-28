// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An IMAP client, written here - IMAP is a text protocol, and a mailbox step needs only part of it: signing in with
//! a password (`LOGIN`) or an OAuth token (`AUTHENTICATE XOAUTH2`, as Google and Microsoft document it), choosing a
//! folder (`SELECT`, or `EXAMINE` to only read), finding messages by UID (`UID SEARCH`), reading them (`UID FETCH`
//! with `BODY.PEEK`, so reading never marks one seen), flags (`UID STORE`), moving (`UID MOVE`, RFC 6851, or a copy
//! and `UID EXPUNGE` of that message alone, RFC 4315), the folders (`LIST`), and waiting for news (`IDLE`, RFC 2177).
//!
//! TLS is the default: from the start on port 993 (`security: tls`), or upgraded with `STARTTLS` on 143 (`starttls`,
//! which refuses a server that will not); `none` only when the credential says so. Nothing the server sent before TLS
//! is kept. Every answer is bounded: a line, a literal, the bytes one answer holds, how many answers a command gets.

use std::time::Duration;

use base64::Engine as _;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::wire::{self, Arg, Item, Raw};
use crate::apps::Credential;
use crate::storage::{Places, net};

/// How long one answer may take to come.
const ANSWER_TIME: Duration = Duration::from_secs(60);
/// The longest line of an answer (a `SEARCH` of many messages is one line), the largest literal (a whole message),
/// and the most bytes the answers to one command may hold together.
const MAX_LINE: usize = 4 * 1024 * 1024;
pub(crate) const MAX_MESSAGE: usize = 32 * 1024 * 1024;
const MAX_HELD: usize = 64 * 1024 * 1024;
/// The most answers one command gets before its end.
const MAX_ANSWERS: usize = 100_000;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// How a command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Ok,
    No,
    Bad,
}

/// What a command got: the answers before its end, and how it ended.
#[derive(Debug)]
pub(crate) struct Answer {
    pub untagged: Vec<Raw>,
    pub status: Status,
    pub text: String,
}

impl Answer {
    fn ok(self, what: &str, host: &str) -> Result<Vec<Raw>, String> {
        match self.status {
            Status::Ok => Ok(self.untagged),
            _ => Err(format!("{host} refused to {what}: {}", shown(&self.text))),
        }
    }
}

/// A server's words, as shown: bounded, one line.
fn shown(text: &str) -> String {
    let t: String = text.chars().filter(|c| !c.is_control()).take(300).collect();
    if t.is_empty() {
        "no reason given".to_owned()
    } else {
        t
    }
}

/// The folder chosen, as the server said when it was.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Selected {
    pub folder: String,
    pub uidvalidity: u64,
    pub exists: u64,
    pub uidnext: Option<u64>,
}

/// One message's answer to a `FETCH`: its items by name (in capitals).
#[derive(Debug, Default)]
pub(crate) struct Fetched {
    pub items: Vec<(String, Item)>,
}

impl Fetched {
    pub(crate) fn get(&self, name: &str) -> Option<&Item> {
        self.items.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub(crate) fn uid(&self) -> Option<u64> {
        self.get("UID").and_then(Item::number)
    }

    /// The bytes of a `BODY[...]` item (whatever section was asked).
    pub(crate) fn body(&self) -> Option<&[u8]> {
        self.items
            .iter()
            .find(|(n, _)| n.starts_with("BODY["))
            .and_then(|(_, v)| v.bytes())
    }

    pub(crate) fn flags(&self) -> Vec<String> {
        match self.get("FLAGS") {
            Some(Item::List(f)) => f.iter().filter_map(Item::text).take(100).collect(),
            _ => Vec::new(),
        }
    }
}

/// How a session signs in.
#[derive(Debug, Clone)]
pub(crate) enum SignIn {
    Password(String),
    /// An OAuth 2.0 access token, through XOAUTH2.
    Token(String),
}

pub(crate) struct Session {
    io: Box<dyn Io>,
    buffer: Vec<u8>,
    pub host: String,
    tag: u32,
    pub capabilities: Vec<String>,
    pub selected: Option<Selected>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

pub(crate) fn field(c: &Credential, name: &str) -> Option<String> {
    c.fields
        .get(name)
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// A set of UIDs as IMAP writes it: `3,5:9,12`.
pub(crate) fn uid_set(uids: &[u64]) -> String {
    let mut sorted: Vec<u64> = uids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut out = String::new();
    let mut i = 0;
    while let Some(&start) = sorted.get(i) {
        let mut end = start;
        while sorted.get(i + 1) == Some(&(end + 1)) {
            end += 1;
            i += 1;
        }
        if !out.is_empty() {
            out.push(',');
        }
        if end > start {
            out.push_str(&format!("{start}:{end}"));
        } else {
            out.push_str(&start.to_string());
        }
        i += 1;
    }
    out
}

impl Session {
    /// A session with the server the credential names, signed in.
    pub(crate) async fn open(c: &Credential, places: &Places, sign_in: &SignIn) -> Result<Self, String> {
        let host = field(c, "host").ok_or("the imap credential has no host")?;
        let security = field(c, "security").unwrap_or_else(|| "tls".to_owned());
        if !matches!(security.as_str(), "tls" | "starttls" | "none") {
            return Err(format!(
                "security: tls (the default), starttls or none - not {security:?}"
            ));
        }
        let port = match field(c, "port") {
            Some(p) => p
                .parse::<u16>()
                .ok()
                .filter(|p| *p > 0)
                .ok_or_else(|| format!("port: {p:?} is not a port"))?,
            None if security == "tls" => 993,
            None => 143,
        };
        let (stream, _) = net::connect(&host, port, places.allow_private).await?;
        let config = if security == "none" {
            None
        } else {
            Some(net::tls_config(c.fields.get("ca").map(String::as_str))?)
        };
        let io: Box<dyn Io> = match (&config, security.as_str()) {
            (Some(cfg), "tls") => Box::new(net::tls(stream, cfg, &host).await?),
            _ => Box::new(stream),
        };
        let mut s = Self {
            io,
            buffer: Vec::new(),
            host: host.clone(),
            tag: 0,
            capabilities: Vec::new(),
            selected: None,
        };
        let greeting = s.read().await?;
        let words = String::from_utf8_lossy(&greeting.text).to_ascii_uppercase();
        if words.starts_with("* BYE") {
            return Err(format!("{host} would not take a session: {}", shown(&words)));
        }
        let preauth = words.starts_with("* PREAUTH");
        if !preauth && !words.starts_with("* OK") {
            return Err(format!("{host} did not greet as an IMAP server does"));
        }
        if security == "starttls" {
            s.capability().await?;
            if !s.has("STARTTLS") {
                return Err(format!(
                    "{host} does not offer STARTTLS: use security tls (port 993), or none for a server without TLS"
                ));
            }
            s.command(&[Arg::Raw("STARTTLS".into())])
                .await?
                .ok("start TLS", &host)?;
            if !s.buffer.is_empty() {
                return Err(format!("{host} said more than it should before TLS"));
            }
            let plain = std::mem::replace(&mut s.io, Box::new(tokio::io::duplex(1).0));
            let cfg = config.as_ref().ok_or("no TLS")?;
            s.io = Box::new(net::tls(plain, cfg, &host).await?);
        }
        s.capability().await?;
        if !preauth {
            let user = field(c, "username").ok_or("the imap credential has no username")?;
            s.sign_in(&user, sign_in).await?;
            s.capability().await?;
        }
        Ok(s)
    }

    pub(crate) fn has(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|c| c.eq_ignore_ascii_case(capability))
    }

    async fn sign_in(&mut self, user: &str, how: &SignIn) -> Result<(), String> {
        wire::safe(user, "the username")?;
        match how {
            SignIn::Password(password) => {
                wire::safe(password, "the password")?;
                if self.has("LOGINDISABLED") {
                    return Err(format!(
                        "{} takes no password on this connection: use security tls or starttls",
                        self.host
                    ));
                }
                let answer = self
                    .command(&[
                        Arg::Raw("LOGIN".into()),
                        Arg::Text(user.to_owned()),
                        Arg::Text(password.clone()),
                    ])
                    .await?;
                if answer.status != Status::Ok {
                    return Err(format!(
                        "{} refused the sign-in of {user}: {}",
                        self.host,
                        shown(&answer.text)
                    ));
                }
                Ok(())
            }
            SignIn::Token(token) => {
                wire::safe(token, "the access token")?;
                if !self.has("AUTH=XOAUTH2") {
                    return Err(format!(
                        "{} does not take an OAuth token (XOAUTH2): sign in with a password",
                        self.host
                    ));
                }
                // The SASL XOAUTH2 initial response: user=..^Aauth=Bearer ..^A^A, in Base64.
                let response = base64::engine::general_purpose::STANDARD
                    .encode(format!("user={user}\x01auth=Bearer {token}\x01\x01"));
                self.tag += 1;
                let tag = format!("a{}", self.tag);
                self.write(format!("{tag} AUTHENTICATE XOAUTH2 {response}\r\n").as_bytes())
                    .await?;
                for _ in 0..50 {
                    let raw = self.read().await?;
                    if raw.text.first() == Some(&b'+') {
                        // A failure is told as a challenge holding the reason; an empty line ends it.
                        self.write(b"\r\n").await?;
                        continue;
                    }
                    if let Some(rest) = raw.text.strip_prefix(format!("{tag} ").as_bytes()) {
                        let words = String::from_utf8_lossy(rest).into_owned();
                        if words.to_ascii_uppercase().starts_with("OK") {
                            return Ok(());
                        }
                        return Err(format!(
                            "{} refused the OAuth token of {user}: {} (renew the token, or check it allows IMAP)",
                            self.host,
                            shown(words.get(2..).unwrap_or_default().trim())
                        ));
                    }
                }
                Err(format!("{} did not finish the sign-in", self.host))
            }
        }
    }

    async fn capability(&mut self) -> Result<(), String> {
        let answers = self
            .command(&[Arg::Raw("CAPABILITY".into())])
            .await?
            .ok("say what it can do", &self.host.clone())?;
        let mut caps = Vec::new();
        for raw in &answers {
            let items = wire::parse(raw, 0)?;
            if items.get(1).and_then(Item::name).as_deref() == Some("CAPABILITY") {
                caps.extend(items.iter().skip(2).filter_map(Item::text).take(500));
            }
        }
        self.capabilities = caps;
        Ok(())
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), String> {
        let host = self.host.clone();
        tokio::time::timeout(ANSWER_TIME, async {
            self.io.write_all(bytes).await?;
            self.io.flush().await
        })
        .await
        .map_err(|_| format!("{host} did not take what was sent in time"))?
        .map_err(|e| format!("{host} could not be written to: {e}"))
    }

    async fn fill(&mut self) -> Result<(), String> {
        let mut chunk = vec![0u8; 64 * 1024];
        let n = tokio::time::timeout(ANSWER_TIME, self.io.read(&mut chunk))
            .await
            .map_err(|_| format!("{} did not answer in time", self.host))?
            .map_err(|e| format!("{} could not be read: {e}", self.host))?;
        if n == 0 {
            return Err(format!("{} closed the connection", self.host));
        }
        self.buffer.extend_from_slice(chunk.get(..n).unwrap_or_default());
        Ok(())
    }

    /// One line, without its line break.
    async fn line(&mut self) -> Result<Vec<u8>, String> {
        let mut searched = 0;
        loop {
            if let Some(at) = self
                .buffer
                .get(searched..)
                .and_then(|b| b.iter().position(|x| *x == b'\n'))
            {
                let end = searched + at;
                let mut line: Vec<u8> = self.buffer.drain(..=end).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(line);
            }
            searched = self.buffer.len();
            if self.buffer.len() > MAX_LINE {
                return Err(format!("{} sent a line longer than {MAX_LINE} bytes", self.host));
            }
            self.fill().await?;
        }
    }

    async fn exact(&mut self, n: usize) -> Result<Vec<u8>, String> {
        while self.buffer.len() < n {
            self.fill().await?;
        }
        Ok(self.buffer.drain(..n).collect())
    }

    /// One whole answer, its literals read.
    pub(crate) async fn read(&mut self) -> Result<Raw, String> {
        let mut raw = Raw::default();
        let mut held = 0usize;
        loop {
            let line = self.line().await?;
            held += line.len();
            let literal = wire::literal_size(&line);
            raw.text.extend_from_slice(&line);
            let Some(n) = literal else {
                return Ok(raw);
            };
            held += n;
            if n > MAX_MESSAGE || held > MAX_HELD {
                return Err(format!(
                    "{} sent more than {} MB in one answer",
                    self.host,
                    MAX_HELD / (1024 * 1024)
                ));
            }
            raw.literals.push(self.exact(n).await?);
            if raw.literals.len() > wire::MAX_PARTS {
                return Err(format!("{} sent too many parts in one answer", self.host));
            }
        }
    }

    /// Sends a command and reads its answers up to its end. Text that quotes cannot hold goes as a literal, once the
    /// server says to go on.
    pub(crate) async fn command(&mut self, args: &[Arg]) -> Result<Answer, String> {
        self.tag += 1;
        let tag = format!("a{}", self.tag);
        let mut line = tag.clone();
        for arg in args {
            line.push(' ');
            match arg {
                Arg::Raw(r) => {
                    wire::safe(r, "a command")?;
                    line.push_str(r);
                }
                Arg::Text(t) => {
                    wire::safe(t, "a command's text")?;
                    match wire::quoted(t) {
                        Some(q) => line.push_str(&q),
                        None => {
                            line.push_str(&format!("{{{}}}\r\n", t.len()));
                            self.write(line.as_bytes()).await?;
                            let go = self.read().await?;
                            if go.text.first() != Some(&b'+') {
                                return Err(format!(
                                    "{} would not take text outside ASCII: {}",
                                    self.host,
                                    shown(&String::from_utf8_lossy(&go.text))
                                ));
                            }
                            line = t.clone();
                        }
                    }
                }
            }
        }
        line.push_str("\r\n");
        self.write(line.as_bytes()).await?;
        self.answers(&tag).await
    }

    async fn answers(&mut self, tag: &str) -> Result<Answer, String> {
        let mut untagged = Vec::new();
        let mut held = 0usize;
        let prefix = format!("{tag} ");
        loop {
            let raw = self.read().await?;
            if let Some(rest) = raw.text.strip_prefix(prefix.as_bytes()) {
                let words = String::from_utf8_lossy(rest).into_owned();
                let upper = words.to_ascii_uppercase();
                let (status, text) = if upper.starts_with("OK") {
                    (Status::Ok, words.get(2..).unwrap_or_default())
                } else if upper.starts_with("NO") {
                    (Status::No, words.get(2..).unwrap_or_default())
                } else {
                    (Status::Bad, words.get(3..).unwrap_or_default())
                };
                return Ok(Answer {
                    untagged,
                    status,
                    text: text.trim().to_owned(),
                });
            }
            if raw.text.starts_with(b"* BYE") {
                return Err(format!(
                    "{} ended the session: {}",
                    self.host,
                    shown(&String::from_utf8_lossy(raw.text.get(5..).unwrap_or_default()))
                ));
            }
            if raw.text.first() == Some(&b'+') {
                return Err(format!("{} asked for more than this command sends", self.host));
            }
            held += raw.text.len() + raw.literals.iter().map(Vec::len).sum::<usize>();
            if untagged.len() >= MAX_ANSWERS || held > MAX_HELD {
                return Err(format!("{} answered with too much at once", self.host));
            }
            untagged.push(raw);
        }
    }

    /// Chooses a folder: to change what is in it (`SELECT`) or only to read it (`EXAMINE`).
    pub(crate) async fn select(&mut self, folder: &str, write: bool) -> Result<Selected, String> {
        let verb = if write { "SELECT" } else { "EXAMINE" };
        let answer = self
            .command(&[Arg::Raw(verb.into()), Arg::Text(wire::folder_name(folder))])
            .await?;
        if answer.status != Status::Ok {
            return Err(format!(
                "{} has no folder {folder:?} to open: {}",
                self.host,
                shown(&answer.text)
            ));
        }
        let mut selected = Selected {
            folder: folder.to_owned(),
            ..Selected::default()
        };
        for raw in &answer.untagged {
            let text = String::from_utf8_lossy(&raw.text).to_ascii_uppercase();
            let code = |name: &str| -> Option<u64> {
                let at = text.find(&format!("[{name} "))? + name.len() + 2;
                text.get(at..)?
                    .split(|c: char| !c.is_ascii_digit())
                    .next()?
                    .parse()
                    .ok()
            };
            if let Some(v) = code("UIDVALIDITY") {
                selected.uidvalidity = v;
            }
            if let Some(v) = code("UIDNEXT") {
                selected.uidnext = Some(v);
            }
            let words: Vec<&str> = text.split_whitespace().collect();
            if words.get(2) == Some(&"EXISTS")
                && let Some(n) = words.get(1).and_then(|n| n.parse().ok())
            {
                selected.exists = n;
            }
        }
        self.selected = Some(selected.clone());
        Ok(selected)
    }

    /// The UIDs of the folder's messages that meet `criteria` (`UID SEARCH`), lowest first.
    pub(crate) async fn search(&mut self, criteria: &[Arg]) -> Result<Vec<u64>, String> {
        let outside = criteria
            .iter()
            .any(|a| matches!(a, Arg::Text(t) if wire::quoted(t).is_none()));
        let mut args = vec![Arg::Raw("UID SEARCH".into())];
        if outside {
            args.push(Arg::Raw("CHARSET UTF-8".into()));
        }
        args.extend_from_slice(criteria);
        if criteria.is_empty() {
            args.push(Arg::Raw("ALL".into()));
        }
        let host = self.host.clone();
        let answers = self.command(&args).await?.ok("search the folder", &host)?;
        let mut uids = Vec::new();
        for raw in &answers {
            let items = wire::parse(raw, 0)?;
            if items.get(1).and_then(Item::name).as_deref() == Some("SEARCH") {
                uids.extend(items.iter().skip(2).filter_map(Item::number));
            }
        }
        uids.sort_unstable();
        uids.dedup();
        Ok(uids)
    }

    /// The messages with these UIDs, each with the items asked (`(UID FLAGS BODY.PEEK[])`).
    pub(crate) async fn fetch(&mut self, uids: &[u64], items: &str) -> Result<Vec<Fetched>, String> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }
        let host = self.host.clone();
        let answers = self
            .command(&[
                Arg::Raw("UID FETCH".into()),
                Arg::Raw(uid_set(uids)),
                Arg::Raw(items.into()),
            ])
            .await?
            .ok("read the messages", &host)?;
        let mut out = Vec::new();
        for raw in &answers {
            let parts = wire::parse(raw, 0)?;
            if parts.get(2).and_then(Item::name).as_deref() != Some("FETCH") {
                continue;
            }
            let Some(Item::List(list)) = parts.get(3) else {
                continue;
            };
            let mut fetched = Fetched::default();
            let mut pairs = list.iter();
            while let (Some(name), Some(value)) = (pairs.next(), pairs.next()) {
                if let Some(n) = name.name() {
                    fetched.items.push((n, value.clone()));
                }
            }
            out.push(fetched);
        }
        Ok(out)
    }

    /// Adds (or takes away) flags of these messages.
    pub(crate) async fn store(&mut self, uids: &[u64], add: bool, flags: &[&str]) -> Result<(), String> {
        let how = if add { "+FLAGS.SILENT" } else { "-FLAGS.SILENT" };
        let host = self.host.clone();
        self.command(&[
            Arg::Raw("UID STORE".into()),
            Arg::Raw(uid_set(uids)),
            Arg::Raw(how.into()),
            Arg::Raw(format!("({})", flags.join(" "))),
        ])
        .await?
        .ok("change the messages' flags", &host)?;
        Ok(())
    }

    /// Moves these messages to `folder`: with `MOVE`, or a copy and the originals expunged one by one (`UIDPLUS`).
    pub(crate) async fn move_to(&mut self, uids: &[u64], folder: &str) -> Result<(), String> {
        let host = self.host.clone();
        let target = Arg::Text(wire::folder_name(folder));
        if self.has("MOVE") {
            let answer = self
                .command(&[Arg::Raw("UID MOVE".into()), Arg::Raw(uid_set(uids)), target])
                .await?;
            return answer
                .ok(&format!("move the messages to {folder:?}"), &host)
                .map(|_| ());
        }
        if !self.has("UIDPLUS") {
            return Err(format!(
                "{host} can neither move a message (MOVE) nor remove one alone (UIDPLUS): it cannot be moved"
            ));
        }
        self.command(&[Arg::Raw("UID COPY".into()), Arg::Raw(uid_set(uids)), target])
            .await?
            .ok(&format!("copy the messages to {folder:?}"), &host)?;
        self.expunge(uids).await.map(|_| ())
    }

    /// Marks these messages deleted and removes them: those alone (`UID EXPUNGE`) when the server can, otherwise they
    /// are left marked deleted, for the server to remove. Whether they were removed.
    pub(crate) async fn expunge(&mut self, uids: &[u64]) -> Result<bool, String> {
        self.store(uids, true, &["\\Deleted"]).await?;
        if !self.has("UIDPLUS") {
            return Ok(false);
        }
        let host = self.host.clone();
        self.command(&[Arg::Raw("UID EXPUNGE".into()), Arg::Raw(uid_set(uids))])
            .await?
            .ok("remove the messages", &host)?;
        Ok(true)
    }

    /// The folders, as people read their names, with what the server says of each (`\Sent`, `\Trash` ...).
    pub(crate) async fn folders(&mut self, most: usize) -> Result<Vec<(String, Vec<String>)>, String> {
        let host = self.host.clone();
        let answers = self
            .command(&[
                Arg::Raw("LIST".into()),
                Arg::Text(String::new()),
                Arg::Text("*".into()),
            ])
            .await?
            .ok("list its folders", &host)?;
        let mut out = Vec::new();
        for raw in &answers {
            let items = wire::parse(raw, 0)?;
            if items.get(1).and_then(Item::name).as_deref() != Some("LIST") {
                continue;
            }
            let attributes = match items.get(2) {
                Some(Item::List(a)) => a.iter().filter_map(Item::text).take(20).collect(),
                _ => Vec::new(),
            };
            if let Some(name) = items.get(4).and_then(Item::text) {
                out.push((wire::folder_text(&name), attributes));
            }
            if out.len() >= most {
                break;
            }
        }
        Ok(out)
    }

    /// Waits, with `IDLE`, until the folder has new messages (true) or `until` passes (false).
    pub(crate) async fn idle(&mut self, until: Duration) -> Result<bool, String> {
        self.tag += 1;
        let tag = format!("a{}", self.tag);
        self.write(format!("{tag} IDLE\r\n").as_bytes()).await?;
        let go = self.read().await?;
        if go.text.first() != Some(&b'+') {
            return Err(format!("{} would not wait for news (IDLE)", self.host));
        }
        let deadline = tokio::time::Instant::now() + until;
        let mut news = false;
        while !news {
            let Ok(raw) = tokio::time::timeout_at(deadline, self.read_news()).await else {
                break;
            };
            let raw = raw?;
            let text = String::from_utf8_lossy(&raw.text).to_ascii_uppercase();
            news = text.ends_with(" EXISTS") || text.ends_with(" RECENT");
        }
        self.write(b"DONE\r\n").await?;
        self.answers(&tag).await?.ok("stop waiting", &self.host.clone())?;
        Ok(news)
    }

    /// An answer while waiting: no time limit of its own (the wait has one).
    async fn read_news(&mut self) -> Result<Raw, String> {
        loop {
            if self.buffer.contains(&b'\n') {
                return self.read().await;
            }
            let mut chunk = vec![0u8; 16 * 1024];
            let n = self
                .io
                .read(&mut chunk)
                .await
                .map_err(|e| format!("{} could not be read: {e}", self.host))?;
            if n == 0 {
                return Err(format!("{} closed the connection", self.host));
            }
            self.buffer.extend_from_slice(chunk.get(..n).unwrap_or_default());
            if self.buffer.len() > MAX_LINE {
                return Err(format!("{} sent too much while waiting", self.host));
            }
        }
    }

    /// Ends the session, politely; a server that does not answer is left.
    pub(crate) async fn logout(mut self) {
        let _ =
            tokio::time::timeout(Duration::from_secs(5), self.command(&[Arg::Raw("LOGOUT".into())])).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn uid_sets_are_written_short() {
        assert_eq!(uid_set(&[9, 3, 5, 6, 7, 12, 5]), "3,5:7,9,12");
        assert_eq!(uid_set(&[1]), "1");
        assert_eq!(uid_set(&[]), "");
    }
}
