// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SFTP (version 3, as OpenSSH and nearly every server speak it) over the SSH client of [`ssh`], written here on
//! `ring` alone: no SSH library meets the dependency rule (pure Rust, permissive licence, no open advisory - the RSA
//! crate the Rust SSH libraries use has one, RUSTSEC-2023-0071).
//!
//! The server's host key is checked every time: against the one the credential pins (`host_key`: the key itself, as
//! `ssh-keyscan` prints it, or its `SHA256:` fingerprint), or - the first time, when it pins none - recorded in the
//! credential once the sign-in succeeds, and from then on required. A server whose key then changes is refused, saying
//! so. Writes go to a file of their own beside the one written and are renamed over it, so a reader on the server never
//! sees half a file.
//!
//! An `sftp` credential holds `host`, `port` (22), `username`, a `password` or a `private_key` (OpenSSH's form or PEM;
//! not protected by a passphrase), `host_key`, and a `folder` the paths are inside.

use std::collections::BTreeMap;

use super::Places;
use super::paths::Rel;
use super::remote::Listed;
use crate::apps::Credential;
use crate::run::Effects;

mod keys;
mod ssh;
mod wire;

use keys::{Pin, UserKey};
use wire::{In, Out};

const INIT: u8 = 1;
const VERSION: u8 = 2;
const OPEN: u8 = 3;
const CLOSE: u8 = 4;
const READ: u8 = 5;
const WRITE: u8 = 6;
const LSTAT: u8 = 7;
const OPENDIR: u8 = 11;
const READDIR: u8 = 12;
const REMOVE: u8 = 13;
const MKDIR: u8 = 14;
const RMDIR: u8 = 15;
const STAT: u8 = 17;
const RENAME: u8 = 18;
const STATUS: u8 = 101;
const HANDLE: u8 = 102;
const DATA: u8 = 103;
const NAME: u8 = 104;
const ATTRS: u8 = 105;
const EXTENDED: u8 = 200;

const EOF: u32 = 1;
const NO_SUCH_FILE: u32 = 2;
const PERMISSION_DENIED: u32 = 3;

const READ_FLAG: u32 = 0x01;
const WRITE_FLAG: u32 = 0x02;
const CREATE: u32 = 0x08;
const TRUNCATE: u32 = 0x10;
const EXCLUSIVE: u32 = 0x20;

/// How much one read or write asks for.
const CHUNK: usize = 32 * 1024;
/// The largest SFTP packet taken.
const MAX_SFTP_PACKET: usize = 1024 * 1024;
/// The most entries one folder's listing is read with.
const MAX_FOLDER: usize = 20_000;
/// What the files a write makes before it is done end in.
const PART: &str = ".openduct-part";

#[derive(Debug, Clone, Copy, Default)]
struct Attrs {
    size: Option<u64>,
    perms: Option<u32>,
    mtime: Option<u32>,
}

impl Attrs {
    fn read(r: &mut In<'_>) -> Result<Self, String> {
        let flags = r.u32()?;
        let mut a = Self::default();
        if flags & 0x1 != 0 {
            a.size = Some(r.u64()?);
        }
        if flags & 0x2 != 0 {
            let _ = (r.u32()?, r.u32()?);
        }
        if flags & 0x4 != 0 {
            a.perms = Some(r.u32()?);
        }
        if flags & 0x8 != 0 {
            let _atime = r.u32()?;
            a.mtime = Some(r.u32()?);
        }
        if flags & 0x8000_0000 != 0 {
            for _ in 0..r.u32()?.min(1000) {
                let _ = (r.string()?, r.string()?);
            }
        }
        Ok(a)
    }

    fn kind(&self) -> u32 {
        self.perms.unwrap_or(0) & 0o170_000
    }

    fn folder(&self) -> bool {
        self.kind() == 0o040_000
    }

    fn link(&self) -> bool {
        self.kind() == 0o120_000
    }

    fn millis(&self) -> Option<i64> {
        self.mtime.map(|t| i64::from(t) * 1000)
    }
}

/// A path as the server is sent it.
fn path_of(rel: &Rel) -> String {
    let text = rel.text();
    if text.is_empty() { ".".to_owned() } else { text }
}

pub(crate) struct Session {
    ssh: ssh::Transport,
    host: String,
    next_id: u32,
    posix_rename: bool,
    pub base: Rel,
}

/// A refusal: the server's status code and what it said.
type Refused = (u32, String);

impl Session {
    /// Connects, checks the server's key, signs in and starts SFTP; a key seen the first time is recorded in the
    /// credential called `name`.
    pub(crate) async fn open(
        effects: &dyn Effects,
        name: &str,
        c: &Credential,
        places: &Places,
    ) -> Result<Self, String> {
        let field = |n: &str| {
            c.fields
                .get(n)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        let host = field("host").ok_or("the sftp credential has no host")?;
        let port = match field("port") {
            Some(p) => p
                .parse::<u16>()
                .map_err(|_| format!("port: {p:?} is not a port"))?,
            None => 22,
        };
        let user = field("username").ok_or("the sftp credential has no username")?;
        let pin = field("host_key").map(|k| Pin::parse(&k)).transpose()?;
        let key = c
            .fields
            .get("private_key")
            .filter(|k| !k.trim().is_empty())
            .map(|k| UserKey::parse(k))
            .transpose()?;
        let password = c.fields.get("password").filter(|p| !p.is_empty()).cloned();
        let base =
            Rel::parse(&field("folder").unwrap_or_default(), true).map_err(|e| format!("folder: {e}"))?;
        let (stream, _) = super::net::connect(&host, port, places.allow_private).await?;
        let mut ssh = ssh::Transport::start(stream, &host, pin.clone()).await?;
        ssh.sign_in(&user, key.as_ref(), password.as_deref()).await?;
        ssh.open_sftp().await?;
        let mut session = Self {
            ssh,
            host: host.clone(),
            next_id: 1,
            posix_rename: false,
            base,
        };
        session.start().await?;
        if pin.is_none() {
            // The first time: the key is kept in the credential, and every connection after this one must see it.
            let recorded = Pin::recorded(&session.ssh.host_key)?;
            let fields = BTreeMap::from([("host_key".to_owned(), recorded)]);
            effects
                .keep_credential(name, fields)
                .await
                .map_err(|e| format!("{host}'s host key could not be kept in {name} for next time: {e}"))?;
        }
        Ok(session)
    }

    async fn start(&mut self) -> Result<(), String> {
        let init = Out::new(INIT).u32(3).0;
        self.send_packet(&init).await?;
        let reply = self.read_packet().await?;
        let mut r = In::new(&reply);
        if r.byte()? != VERSION {
            return Err(format!("{} did not start SFTP", self.host));
        }
        let _version = r.u32()?;
        while !r.rest().is_empty() {
            let name = r.text()?;
            let _data = r.string()?;
            if name == "posix-rename@openssh.com" {
                self.posix_rename = true;
            }
        }
        Ok(())
    }

    async fn send_packet(&mut self, body: &[u8]) -> Result<(), String> {
        let mut packet = u32::try_from(body.len())
            .map_err(|_| "a request too large".to_owned())?
            .to_be_bytes()
            .to_vec();
        packet.extend_from_slice(body);
        self.ssh.write_channel(&packet).await
    }

    async fn read_packet(&mut self) -> Result<Vec<u8>, String> {
        let head = self.ssh.read_channel(4).await?;
        let length = In::new(&head).u32()?;
        let length = usize::try_from(length).map_err(|_| "an answer too large".to_owned())?;
        if length == 0 || length > MAX_SFTP_PACKET {
            return Err(format!("{} sent an SFTP answer of {length} bytes", self.host));
        }
        self.ssh.read_channel(length).await
    }

    /// A request, and its answer: its kind and what follows its id.
    async fn request(&mut self, kind: u8, body: Out) -> Result<(u8, Vec<u8>), String> {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let packet = Out::new(kind).u32(id).raw(&body.0).0;
        self.send_packet(&packet).await?;
        let reply = self.read_packet().await?;
        let mut r = In::new(&reply);
        let answer = r.byte()?;
        if r.u32()? != id {
            return Err(format!("{} answered another request", self.host));
        }
        Ok((answer, r.rest().to_vec()))
    }

    fn status(kind: u8, rest: &[u8]) -> Result<Option<Refused>, String> {
        if kind != STATUS {
            return Err("an unexpected answer".to_owned());
        }
        let mut r = In::new(rest);
        let code = r.u32()?;
        let message = r.text().unwrap_or_default();
        Ok((code != 0).then_some((code, message)))
    }

    fn refused(&self, what: &str, rel: &Rel, (code, message): Refused) -> String {
        match code {
            NO_SUCH_FILE => format!("there is nothing at {} on {}", rel.text(), self.host),
            PERMISSION_DENIED => format!("{} does not let OpenDuct {what} {}", self.host, rel.text()),
            _ => {
                let said: String = message.chars().take(200).collect();
                format!("{} would not {what} {}: {said}", self.host, rel.text())
            }
        }
    }

    /// Sends `kind` about one path and expects a plain status.
    async fn simple(&mut self, kind: u8, body: Out) -> Result<Option<Refused>, String> {
        let (answer, rest) = self.request(kind, body).await?;
        Self::status(answer, &rest)
    }

    async fn stat_with(&mut self, kind: u8, rel: &Rel) -> Result<Option<Attrs>, String> {
        let (answer, rest) = self.request(kind, Out::default().text(&path_of(rel))).await?;
        if answer == ATTRS {
            return Attrs::read(&mut In::new(&rest)).map(Some);
        }
        match Self::status(answer, &rest)? {
            Some((NO_SUCH_FILE, _)) | None => Ok(None),
            Some(refused) => Err(self.refused("look at", rel, refused)),
        }
    }

    async fn stat(&mut self, rel: &Rel) -> Result<Option<Attrs>, String> {
        self.stat_with(STAT, rel).await
    }

    async fn handle(
        &mut self,
        kind: u8,
        body: Out,
        what: &str,
        rel: &Rel,
    ) -> Result<Result<Vec<u8>, Refused>, String> {
        let (answer, rest) = self.request(kind, body).await?;
        if answer == HANDLE {
            return Ok(Ok(In::new(&rest).string()?.to_vec()));
        }
        match Self::status(answer, &rest)? {
            Some(refused) => Ok(Err(refused)),
            None => Err(format!("{} gave no handle to {what} {}", self.host, rel.text())),
        }
    }

    async fn close_handle(&mut self, handle: &[u8]) {
        let _ = self.simple(CLOSE, Out::default().string(handle)).await;
    }

    async fn entries(&mut self, dir: &Rel) -> Result<Vec<(String, Attrs)>, String> {
        let handle = match self
            .handle(OPENDIR, Out::default().text(&path_of(dir)), "list", dir)
            .await?
        {
            Ok(h) => h,
            Err(refused) => return Err(self.refused("list", dir, refused)),
        };
        let mut out = Vec::new();
        let listed = loop {
            let (answer, rest) = match self.request(READDIR, Out::default().string(&handle)).await {
                Ok(a) => a,
                Err(e) => break Err(e),
            };
            if answer != NAME {
                break match Self::status(answer, &rest) {
                    Ok(Some((EOF, _))) | Ok(None) => Ok(()),
                    Ok(Some(refused)) => Err(self.refused("list", dir, refused)),
                    Err(e) => Err(e),
                };
            }
            let mut r = In::new(&rest);
            let count = r.u32()?;
            for _ in 0..count {
                let name = r.text()?;
                let _long = r.string()?;
                let attrs = Attrs::read(&mut r)?;
                if name != "." && name != ".." && !name.ends_with(PART) {
                    out.push((name, attrs));
                }
            }
            if out.len() > MAX_FOLDER {
                break Err(format!("{} holds more than {MAX_FOLDER} entries", dir.text()));
            }
        };
        self.close_handle(&handle).await;
        listed.map(|()| out)
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        depth: usize,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let mut out = Vec::new();
        let mut folders = vec![(dir.clone(), depth)];
        while let Some((at, left)) = folders.pop() {
            let mut entries = self.entries(&at).await?;
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, mut attrs) in entries {
                if out.len() >= most {
                    return Ok((out, true));
                }
                let Ok(child) = Rel::parse(&name, false).map(|n| at.child(n.name().unwrap_or_default()))
                else {
                    continue;
                };
                // A link is listed as what it leads to, and never gone into.
                let linked = attrs.link();
                if linked {
                    match self.stat(&child).await {
                        Ok(Some(target)) => attrs = target,
                        _ => continue,
                    }
                }
                if attrs.folder() && !linked && left > 1 {
                    folders.push((child.clone(), left - 1));
                }
                out.push(Listed {
                    rel: child,
                    folder: attrs.folder(),
                    size: if attrs.folder() {
                        0
                    } else {
                        attrs.size.unwrap_or(0)
                    },
                    modified: attrs.millis(),
                    version: format!("{}-{}", attrs.size.unwrap_or(0), attrs.mtime.unwrap_or(0)),
                });
            }
        }
        out.sort_by(|a, b| a.rel.parts.cmp(&b.rel.parts));
        Ok((out, false))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let attrs = self
            .stat(rel)
            .await?
            .ok_or_else(|| format!("there is nothing at {} on {}", rel.text(), self.host))?;
        if attrs.folder() {
            return Err(format!("{} is a folder, not a file", rel.text()));
        }
        let limit = u64::try_from(most).unwrap_or(u64::MAX);
        if attrs.size.is_some_and(|s| s > limit) {
            return Err(format!(
                "{} is {} bytes: a run reads files of at most {most}",
                rel.text(),
                attrs.size.unwrap_or(0)
            ));
        }
        let open = Out::default().text(&path_of(rel)).u32(READ_FLAG).u32(0);
        let handle = match self.handle(OPEN, open, "read", rel).await? {
            Ok(h) => h,
            Err(refused) => return Err(self.refused("read", rel, refused)),
        };
        let mut out = Vec::new();
        let read = loop {
            let ask = Out::default()
                .string(&handle)
                .u64(u64::try_from(out.len()).unwrap_or(0))
                .u32(u32::try_from(CHUNK).unwrap_or(32_768));
            let (answer, rest) = match self.request(READ, ask).await {
                Ok(a) => a,
                Err(e) => break Err(e),
            };
            if answer == DATA {
                let data = In::new(&rest).string()?;
                if data.is_empty() {
                    break Ok(());
                }
                out.extend_from_slice(data);
                if out.len() > most {
                    break Err(format!("{} grew past {most} bytes as it was read", rel.text()));
                }
                continue;
            }
            break match Self::status(answer, &rest) {
                Ok(Some((EOF, _))) | Ok(None) => Ok(()),
                Ok(Some(refused)) => Err(self.refused("read", rel, refused)),
                Err(e) => Err(e),
            };
        };
        self.close_handle(&handle).await;
        read.map(|()| (out, attrs.millis()))
    }

    /// Writes `bytes` to a new file at `rel` (`flags` saying how it is opened).
    async fn put(&mut self, rel: &Rel, bytes: &[u8], flags: u32) -> Result<Result<(), Refused>, String> {
        let open = Out::default().text(&path_of(rel)).u32(flags).u32(0x4).u32(0o644);
        let handle = match self.handle(OPEN, open, "write", rel).await? {
            Ok(h) => h,
            Err(refused) => return Ok(Err(refused)),
        };
        let mut done = Ok(Ok(()));
        for (i, chunk) in bytes.chunks(CHUNK).enumerate() {
            let at = u64::try_from(i * CHUNK).unwrap_or(0);
            let body = Out::default().string(&handle).u64(at).string(chunk);
            match self.simple(WRITE, body).await {
                Ok(None) => {}
                Ok(Some(refused)) => {
                    done = Ok(Err(refused));
                    break;
                }
                Err(e) => {
                    done = Err(e);
                    break;
                }
            }
        }
        let closed = self.simple(CLOSE, Out::default().string(&handle)).await;
        match (done, closed) {
            (Ok(Ok(())), Ok(Some(refused))) => Ok(Err(refused)),
            (Ok(Ok(())), Err(e)) => Err(e),
            (done, _) => done,
        }
    }

    /// `from` renamed to `to`: replacing what is there when `replace`, never otherwise.
    async fn rename_to(&mut self, from: &Rel, to: &Rel, replace: bool) -> Result<Option<Refused>, String> {
        if replace && self.posix_rename {
            let body = Out::default()
                .text("posix-rename@openssh.com")
                .text(&path_of(from))
                .text(&path_of(to));
            return self.simple(EXTENDED, body).await;
        }
        if replace
            && self.stat_with(LSTAT, to).await?.is_some()
            && let Some(refused) = self.simple(REMOVE, Out::default().text(&path_of(to))).await?
        {
            return Ok(Some(refused));
        }
        self.simple(RENAME, Out::default().text(&path_of(from)).text(&path_of(to)))
            .await
    }

    fn exists_already(rel: &Rel) -> String {
        format!(
            "there is a file at {} already: set overwrite to replace it",
            rel.text()
        )
    }

    pub(crate) async fn write(&mut self, rel: &Rel, bytes: &[u8], overwrite: bool) -> Result<(), String> {
        if let Some(a) = self.stat(rel).await? {
            if a.folder() {
                return Err(format!("there is a folder at {}", rel.text()));
            }
            if !overwrite {
                return Err(Self::exists_already(rel));
            }
        }
        let name = rel.name().unwrap_or("file");
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let short: String = name.chars().take(100).collect();
        let part = rel.parent().child(&format!(".{short}.{stamp:x}{PART}"));
        let mut made = self.put(&part, bytes, WRITE_FLAG | CREATE | EXCLUSIVE).await?;
        if let Err((NO_SUCH_FILE, _)) = made {
            self.make_folder(&rel.parent()).await?;
            made = self.put(&part, bytes, WRITE_FLAG | CREATE | EXCLUSIVE).await?;
        }
        match made {
            Ok(()) => match self.rename_to(&part, rel, overwrite).await? {
                None => Ok(()),
                Some(refused) => {
                    let _ = self.simple(REMOVE, Out::default().text(&path_of(&part))).await;
                    if !overwrite && self.stat(rel).await?.is_some() {
                        Err(Self::exists_already(rel))
                    } else {
                        Err(self.refused("write", rel, refused))
                    }
                }
            },
            // A server that lets no file be made beside it: the file written in place.
            Err((PERMISSION_DENIED, _)) => {
                let flags = WRITE_FLAG | CREATE | if overwrite { TRUNCATE } else { EXCLUSIVE };
                match self.put(rel, bytes, flags).await? {
                    Ok(()) => Ok(()),
                    Err(refused) => Err(self.refused("write", rel, refused)),
                }
            }
            Err(refused) => Err(self.refused("write", rel, refused)),
        }
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        if self.stat_with(LSTAT, from).await?.is_none() {
            return Err(format!("there is nothing at {} on {}", from.text(), self.host));
        }
        if let Some(a) = self.stat(to).await? {
            if a.folder() {
                return Err(format!("there is a folder at {}", to.text()));
            }
            if !overwrite {
                return Err(Self::exists_already(to));
            }
        }
        let mut done = self.rename_to(from, to, overwrite).await?;
        if let Some((NO_SUCH_FILE, _)) = done {
            self.make_folder(&to.parent()).await?;
            done = self.rename_to(from, to, overwrite).await?;
        }
        match done {
            None => Ok(()),
            Some(refused) => Err(self.refused("move", from, refused)),
        }
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        let Some(attrs) = self.stat_with(LSTAT, rel).await? else {
            return Ok(false);
        };
        let (kind, what) = if attrs.folder() {
            (RMDIR, "delete the folder (only an empty folder is deleted)")
        } else {
            (REMOVE, "delete")
        };
        match self.simple(kind, Out::default().text(&path_of(rel))).await? {
            None => Ok(true),
            Some((NO_SUCH_FILE, _)) => Ok(false),
            Some(refused) => Err(self.refused(what, rel, refused)),
        }
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        let mut made = false;
        let mut at = Rel {
            parts: Vec::new(),
            absolute: rel.absolute,
        };
        for part in &rel.parts {
            at = at.child(part);
            made = false;
            match self.stat(&at).await? {
                Some(a) if a.folder() => continue,
                Some(_) => return Err(format!("there is a file at {}", at.text())),
                None => {}
            }
            if let Some(refused) = self
                .simple(MKDIR, Out::default().text(&path_of(&at)).u32(0))
                .await?
            {
                return Err(self.refused("make the folder", &at, refused));
            }
            made = true;
        }
        Ok(made)
    }

    pub(crate) async fn close(mut self) {
        self.ssh.close().await;
    }
}
