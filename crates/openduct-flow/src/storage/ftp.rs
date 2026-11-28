// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! FTP, and FTP over TLS (FTPS): the commands of RFC 959 with RFC 2228's TLS (`AUTH TLS`, `PROT P`), RFC 2428's
//! passive mode (`EPSV`) and RFC 3659's listings (`MLSD`, `MLST`, `SIZE`), written here - a short line protocol.
//!
//! TLS is asked for by default (`security: starttls`; `tls` for a server that starts with it, on 990; `none` only when
//! the credential says so), and the data connections are over TLS too. A server's answer to `PASV` names an address
//! to connect to: it is not taken - the data connection goes to the address the control connection went to, on the
//! port it names - so a server cannot send OpenDuct to another machine.
//!
//! An `ftp` credential holds `host`, `port` (21, or 990 with `security: tls`), `username` (anonymous) and `password`,
//! `security`, a `ca` for a server whose certificate the system does not trust, and a `folder` the paths are inside.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::Places;
use super::paths::Rel;
use super::remote::Listed;
use crate::apps::Credential;

/// How long an answer may take.
const ANSWER_TIME: Duration = Duration::from_secs(30);
/// The longest line of an answer, and the most lines one has.
const MAX_LINE: usize = 4096;
const MAX_LINES: usize = 500;
/// The largest listing read.
const MAX_LISTING: usize = 8 * 1024 * 1024;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub(crate) struct Session {
    control: Box<dyn Io>,
    /// What was read of the control connection past the last answer.
    buffer: Vec<u8>,
    peer: SocketAddr,
    host: String,
    /// The data connections' TLS, when the server was asked to protect them.
    tls: Option<Arc<rustls::ClientConfig>>,
    /// Where the server started this session: a check that changes folder comes back to it.
    home: Option<String>,
    pub base: Rel,
}

fn field(c: &Credential, name: &str) -> Option<String> {
    c.fields
        .get(name)
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// A path as the server is sent it.
fn path_of(rel: &Rel) -> String {
    rel.text()
}

impl Session {
    pub(crate) async fn open(c: &Credential, places: &Places) -> Result<Self, String> {
        let host = field(c, "host").ok_or("the ftp credential has no host")?;
        let security = field(c, "security").unwrap_or_else(|| "starttls".to_owned());
        if !matches!(security.as_str(), "starttls" | "tls" | "none") {
            return Err(format!(
                "security: starttls (the default), tls or none - not {security:?}"
            ));
        }
        let port = match field(c, "port") {
            Some(p) => p
                .parse::<u16>()
                .map_err(|_| format!("port: {p:?} is not a port"))?,
            None if security == "tls" => 990,
            None => 21,
        };
        let (stream, peer) = super::net::connect(&host, port, places.allow_private).await?;
        let config = if security == "none" {
            None
        } else {
            Some(super::net::tls_config(c.fields.get("ca").map(String::as_str))?)
        };
        let control: Box<dyn Io> = match (&config, security.as_str()) {
            (Some(cfg), "tls") => Box::new(super::net::tls(stream, cfg, &host).await?),
            _ => Box::new(stream),
        };
        let mut session = Self {
            control,
            buffer: Vec::new(),
            peer,
            host: host.clone(),
            tls: None,
            home: None,
            base: Rel::parse(&field(c, "folder").unwrap_or_default(), true)
                .map_err(|e| format!("folder: {e}"))?,
        };
        session.expect(&[220], "greet").await?;
        if security == "starttls" {
            let (code, text) = session.command("AUTH TLS").await?;
            if code != 234 {
                return Err(format!(
                    "{host} does not offer TLS ({code} {text}): set security to none in the credential for a server \
                     without it"
                ));
            }
            if !session.buffer.is_empty() {
                return Err(format!("{host} said more than it should before TLS"));
            }
            let plain = std::mem::replace(&mut session.control, Box::new(tokio::io::duplex(1).0));
            let cfg = config.as_ref().ok_or("no TLS")?;
            session.control = Box::new(super::net::tls(plain, cfg, &host).await?);
        }
        if let Some(cfg) = config {
            session.expect_after("PBSZ 0", &[200], "set up TLS").await?;
            session
                .expect_after("PROT P", &[200], "protect its data connections")
                .await?;
            session.tls = Some(cfg);
        }
        let user = field(c, "username").unwrap_or_else(|| "anonymous".to_owned());
        let password = c.fields.get("password").cloned().unwrap_or_default();
        if user.contains(['\r', '\n']) || password.contains(['\r', '\n']) {
            return Err("the credential's username or password holds a line break".to_owned());
        }
        let (code, _) = session.command(&format!("USER {user}")).await?;
        let code = if code == 331 {
            session.command_quiet(&format!("PASS {password}")).await?.0
        } else {
            code
        };
        if code != 230 {
            return Err(format!("{host} refused the sign-in of {user} ({code})"));
        }
        session
            .expect_after("TYPE I", &[200], "send files as they are")
            .await?;
        let _ = session.command("OPTS UTF8 ON").await;
        if let Ok((257, text)) = session.command("PWD").await {
            session.home = text.split('"').nth(1).map(str::to_owned);
        }
        Ok(session)
    }

    async fn line(&mut self) -> Result<String, String> {
        loop {
            if let Some(at) = self.buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = self.buffer.drain(..=at).collect();
                return Ok(String::from_utf8_lossy(&line).trim_end().to_owned());
            }
            if self.buffer.len() > MAX_LINE {
                return Err(format!("{} sent a line longer than {MAX_LINE} bytes", self.host));
            }
            let mut chunk = [0u8; 1024];
            let n = tokio::time::timeout(ANSWER_TIME, self.control.read(&mut chunk))
                .await
                .map_err(|_| format!("{} did not answer in time", self.host))?
                .map_err(|e| format!("{} could not be read: {e}", self.host))?;
            if n == 0 {
                return Err(format!("{} closed the connection", self.host));
            }
            self.buffer.extend_from_slice(chunk.get(..n).unwrap_or_default());
        }
    }

    /// The next answer: its code and its text (the lines of a long one together).
    async fn answer(&mut self) -> Result<(u16, String), String> {
        let first = self.line().await?;
        let code: u16 = first
            .get(..3)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("{} answered in a way FTP does not: {first:?}", self.host))?;
        let mut text = first.get(4..).unwrap_or_default().to_owned();
        if first.as_bytes().get(3) == Some(&b'-') {
            let end = format!("{code} ");
            for _ in 0..MAX_LINES {
                let next = self.line().await?;
                if next.starts_with(&end) {
                    break;
                }
                if text.len() < MAX_LINE {
                    text.push(' ');
                    text.push_str(next.trim());
                }
            }
        }
        Ok((code, text))
    }

    async fn send(&mut self, command: &str) -> Result<(), String> {
        if command.contains(['\r', '\n']) {
            return Err("a command holds a line break".to_owned());
        }
        self.control
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .map_err(|e| format!("{} could not be written to: {e}", self.host))?;
        self.control.flush().await.map_err(|e| e.to_string())
    }

    async fn command(&mut self, command: &str) -> Result<(u16, String), String> {
        self.send(command).await?;
        self.answer().await
    }

    /// A command that holds a secret: never said in an error.
    async fn command_quiet(&mut self, command: &str) -> Result<(u16, String), String> {
        self.send(command)
            .await
            .map_err(|_| format!("{} could not be written to", self.host))?;
        self.answer().await
    }

    async fn expect(&mut self, codes: &[u16], what: &str) -> Result<String, String> {
        let (code, text) = self.answer().await?;
        if codes.contains(&code) {
            Ok(text)
        } else {
            Err(format!("{} would not {what}: {code} {}", self.host, short(&text)))
        }
    }

    async fn expect_after(&mut self, command: &str, codes: &[u16], what: &str) -> Result<String, String> {
        self.send(command).await?;
        self.expect(codes, what).await
    }

    /// A data connection, passive: to the control connection's own address, on the port the server names.
    async fn data_port(&mut self) -> Result<u16, String> {
        let (code, text) = self.command("EPSV").await?;
        if code == 229 {
            let port = text
                .split('|')
                .filter_map(|p| p.parse::<u16>().ok())
                .next_back()
                .ok_or_else(|| format!("{} named no port: {text}", self.host))?;
            return Ok(port);
        }
        let (code, text) = self.command("PASV").await?;
        if code != 227 {
            return Err(format!(
                "{} opens no data connection: {code} {}",
                self.host,
                short(&text)
            ));
        }
        let inside = text
            .split(['(', ')'])
            .nth(1)
            .ok_or_else(|| format!("{} named no port: {text}", self.host))?;
        let numbers: Vec<u16> = inside.split(',').filter_map(|n| n.trim().parse().ok()).collect();
        match numbers.as_slice() {
            [_, _, _, _, high, low] if *high < 256 && *low < 256 => Ok(high * 256 + low),
            _ => Err(format!("{} named no port: {text}", self.host)),
        }
    }

    /// A transfer: `command` sent on a data connection opened for it, which is handed back once the server says it
    /// is starting.
    async fn transfer(&mut self, command: &str) -> Result<Result<Box<dyn Io>, (u16, String)>, String> {
        let port = self.data_port().await?;
        let stream = super::net::connect_again(self.peer, port).await?;
        let (code, text) = self.command(command).await?;
        if !matches!(code, 125 | 150) {
            return Ok(Err((code, text)));
        }
        let data: Box<dyn Io> = match &self.tls {
            Some(cfg) => Box::new(super::net::tls(stream, cfg, &self.host).await?),
            None => Box::new(stream),
        };
        Ok(Ok(data))
    }

    async fn read_data(
        &mut self,
        command: &str,
        most: usize,
    ) -> Result<Result<Vec<u8>, (u16, String)>, String> {
        let mut data = match self.transfer(command).await? {
            Ok(d) => d,
            Err(refused) => return Ok(Err(refused)),
        };
        let mut out = Vec::new();
        let read = tokio::time::timeout(
            Duration::from_secs(300),
            (&mut data)
                .take(u64::try_from(most).unwrap_or(u64::MAX) + 1)
                .read_to_end(&mut out),
        )
        .await;
        drop(data);
        match read {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(format!("the transfer from {} broke: {e}", self.host)),
            Err(_) => return Err(format!("the transfer from {} took too long", self.host)),
        }
        if out.len() > most {
            return Err(format!(
                "more than {most} bytes: a run reads files of at most that"
            ));
        }
        self.expect(&[226, 250], "finish the transfer").await?;
        Ok(Ok(out))
    }

    /// The size of a file there; `None` when there is none (or the server does not say).
    async fn size(&mut self, rel: &Rel) -> Result<Option<u64>, String> {
        let (code, text) = self.command(&format!("SIZE {}", path_of(rel))).await?;
        Ok((code == 213).then(|| text.trim().parse().ok()).flatten())
    }

    /// Whether something is there, and whether it is a folder.
    async fn kind(&mut self, rel: &Rel) -> Result<Option<bool>, String> {
        let (code, text) = self.command(&format!("MLST {}", path_of(rel))).await?;
        if code == 250 {
            return Ok(Some(text.to_ascii_lowercase().contains("type=dir")));
        }
        if self.size(rel).await?.is_some() {
            return Ok(Some(false));
        }
        // A folder, where MLST is not known: going into it, and back.
        let (code, _) = self.command(&format!("CWD {}", path_of(rel))).await?;
        if code == 250 {
            let back = self.home.clone().unwrap_or_else(|| "/".to_owned());
            let _ = self.command(&format!("CWD {back}")).await?;
            return Ok(Some(true));
        }
        Ok(None)
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
            let target = path_of(&at);
            let command = if target.is_empty() {
                "MLSD".to_owned()
            } else {
                format!("MLSD {target}")
            };
            let entries = match self.read_data(&command, MAX_LISTING).await? {
                Ok(bytes) => parse_mlsd(&bytes),
                Err((500 | 501 | 502 | 504, _)) => {
                    let command = if target.is_empty() {
                        "LIST".to_owned()
                    } else {
                        format!("LIST {target}")
                    };
                    match self.read_data(&command, MAX_LISTING).await? {
                        Ok(bytes) => parse_list(&bytes),
                        Err((code, text)) => return Err(self.refused("list", &at, code, &text)),
                    }
                }
                Err((code, text)) => return Err(self.refused("list", &at, code, &text)),
            };
            for (name, folder, size, modified, version) in entries {
                if out.len() >= most {
                    return Ok((out, true));
                }
                let Ok(child) = Rel::parse(&name, false).map(|n| at.child(n.name().unwrap_or_default()))
                else {
                    continue;
                };
                if folder && left > 1 {
                    folders.push((child.clone(), left - 1));
                }
                out.push(Listed {
                    rel: child,
                    folder,
                    size,
                    modified,
                    version,
                });
            }
        }
        out.sort_by(|a, b| a.rel.parts.cmp(&b.rel.parts));
        Ok((out, false))
    }

    fn refused(&self, what: &str, rel: &Rel, code: u16, text: &str) -> String {
        if code == 550 {
            format!("{} refused to {what} {}: {}", self.host, rel.text(), short(text))
        } else {
            format!(
                "{} would not {what} {}: {code} {}",
                self.host,
                rel.text(),
                short(text)
            )
        }
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        if let Some(size) = self.size(rel).await?
            && size > u64::try_from(most).unwrap_or(u64::MAX)
        {
            return Err(format!(
                "{} is {size} bytes: a run reads files of at most {most}",
                rel.text()
            ));
        }
        match self.read_data(&format!("RETR {}", path_of(rel)), most).await? {
            Ok(bytes) => Ok((bytes, None)),
            Err((code, text)) => match self.kind(rel).await? {
                None => Err(format!("there is nothing at {} on {}", rel.text(), self.host)),
                Some(true) => Err(format!("{} is a folder, not a file", rel.text())),
                Some(false) => Err(self.refused("give", rel, code, &text)),
            },
        }
    }

    async fn store(&mut self, rel: &Rel, bytes: &[u8]) -> Result<Result<(), (u16, String)>, String> {
        let mut data = match self.transfer(&format!("STOR {}", path_of(rel))).await? {
            Ok(d) => d,
            Err(refused) => return Ok(Err(refused)),
        };
        let written = tokio::time::timeout(Duration::from_secs(300), async {
            data.write_all(bytes).await?;
            data.shutdown().await
        })
        .await;
        drop(data);
        match written {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("the transfer to {} broke: {e}", self.host)),
            Err(_) => return Err(format!("the transfer to {} took too long", self.host)),
        }
        self.expect(&[226, 250], "finish the transfer").await?;
        Ok(Ok(()))
    }

    pub(crate) async fn write(&mut self, rel: &Rel, bytes: &[u8], overwrite: bool) -> Result<(), String> {
        if !overwrite && self.kind(rel).await?.is_some() {
            return Err(format!(
                "there is a file at {} already: set overwrite to replace it",
                rel.text()
            ));
        }
        if let Err((code, text)) = self.store(rel, bytes).await? {
            if !matches!(code, 550 | 553) {
                return Err(self.refused("store", rel, code, &text));
            }
            // Its folder may not be there yet.
            self.make_folder(&rel.parent()).await?;
            if let Err((code, text)) = self.store(rel, bytes).await? {
                return Err(self.refused("store", rel, code, &text));
            }
        }
        Ok(())
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        if !overwrite && self.kind(to).await?.is_some() {
            return Err(format!(
                "there is a file at {} already: set overwrite to replace it",
                to.text()
            ));
        }
        for attempt in 0..2 {
            let (code, text) = self.command(&format!("RNFR {}", path_of(from))).await?;
            if code != 350 {
                return Err(self.refused("move", from, code, &text));
            }
            let (code, text) = self.command(&format!("RNTO {}", path_of(to))).await?;
            match code {
                250 => return Ok(()),
                550 | 553 if attempt == 0 => self.make_folder(&to.parent()).await.map(|_| ())?,
                _ => return Err(self.refused("move", from, code, &text)),
            }
        }
        Err(format!("{} would not move {}", self.host, from.text()))
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        let (code, _) = self.command(&format!("DELE {}", path_of(rel))).await?;
        if code == 250 {
            return Ok(true);
        }
        match self.kind(rel).await? {
            None => Ok(false),
            Some(true) => {
                let (code, text) = self.command(&format!("RMD {}", path_of(rel))).await?;
                if code == 250 {
                    Ok(true)
                } else {
                    Err(format!(
                        "{} would not delete the folder {} (only an empty folder is deleted): {}",
                        self.host,
                        rel.text(),
                        short(&text)
                    ))
                }
            }
            Some(false) => Err(format!("{} would not delete {}", self.host, rel.text())),
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
            let (code, text) = self.command(&format!("MKD {}", path_of(&at))).await?;
            made = code == 257;
            if !made && self.kind(&at).await? != Some(true) {
                return Err(self.refused("make the folder", &at, code, &text));
            }
        }
        Ok(made)
    }

    pub(crate) async fn close(mut self) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.command("QUIT")).await;
        let _ = self.control.shutdown().await;
    }
}

fn short(text: &str) -> String {
    text.chars().take(200).collect()
}

type Parsed = (String, bool, u64, Option<i64>, String);

/// The entries of an `MLSD` listing: `type=file;size=12;modify=20261127100000; name`.
fn parse_mlsd(bytes: &[u8]) -> Vec<Parsed> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    for line in text.lines() {
        let Some((facts, name)) = line.split_once(' ') else {
            continue;
        };
        let mut kind = "";
        let mut size = 0;
        let mut modify = String::new();
        for fact in facts.split(';') {
            let Some((k, v)) = fact.split_once('=') else {
                continue;
            };
            match k.to_ascii_lowercase().as_str() {
                "type" => {
                    kind = if v.eq_ignore_ascii_case("file") {
                        "file"
                    } else if v.eq_ignore_ascii_case("dir") {
                        "dir"
                    } else {
                        ""
                    }
                }
                "size" => size = v.parse().unwrap_or(0),
                "modify" => modify = v.to_owned(),
                _ => {}
            }
        }
        if kind.is_empty() || name.is_empty() {
            continue;
        }
        let modified =
            chrono::NaiveDateTime::parse_from_str(modify.get(..14).unwrap_or_default(), "%Y%m%d%H%M%S")
                .ok()
                .map(|t| t.and_utc().timestamp_millis());
        out.push((
            name.to_owned(),
            kind == "dir",
            size,
            modified,
            format!("{size}-{modify}"),
        ));
    }
    out
}

/// The entries of a `LIST` listing as Unix servers write it: `-rw-r--r-- 1 user group 12 Nov 27 10:00 name`.
fn parse_list(bytes: &[u8]) -> Vec<Parsed> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = Vec::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let (Some(mode), Some(size)) = (fields.first(), fields.get(4)) else {
            continue;
        };
        let folder = mode.starts_with('d');
        if !folder && !mode.starts_with('-') {
            continue;
        }
        // The name is what follows the eighth field, spaces and all.
        let mut rest = line;
        for _ in 0..8 {
            rest = rest.trim_start();
            rest = rest.split_once(char::is_whitespace).map_or("", |(_, r)| r);
        }
        let name = rest.trim_start();
        if name.is_empty() || name == "." || name == ".." {
            continue;
        }
        let when = fields.get(5..8).map(|w| w.join(" ")).unwrap_or_default();
        let size: u64 = size.parse().unwrap_or(0);
        out.push((
            name.to_owned(),
            folder,
            if folder { 0 } else { size },
            None,
            format!("{size}-{when}"),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listings_are_read_in_either_form() {
        let mlsd =
            b"type=cdir;modify=20261127; .\r\ntype=file;size=12;modify=20261127100000;perm=r; a b.csv\r\n\
                     type=dir;modify=20261127100000; sub\r\nbroken line\r\n";
        let got = parse_mlsd(mlsd);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, "a b.csv");
        assert_eq!(got[0].2, 12);
        assert_eq!(got[0].3, Some(1_795_773_600_000));
        assert!(got[1].1);
        let list = b"drwxr-xr-x 2 ftp ftp 4096 Nov 27 10:00 sub\r\n-rw-r--r-- 1 ftp ftp 12 Nov 27 10:00 a  b.csv\r\n\
                     lrwxrwxrwx 1 ftp ftp 4 Nov 27 10:00 link -> x\r\ntotal 8\r\n";
        let got = parse_list(list);
        assert_eq!(got.len(), 2);
        assert_eq!((got[1].0.as_str(), got[1].2), ("a  b.csv", 12));
        assert!(got[0].1);
    }
}
