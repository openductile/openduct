// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The SSH client an SFTP session runs over (RFC 4253, 4252, 4254), on `ring` alone: an elliptic-curve key exchange
//! (curve25519-sha256, ecdh-sha2-nistp256), the server's host key proven by its signature and checked against the
//! credential's, packets sealed with AES-GCM or ChaCha20-Poly1305 (authenticated ciphers only, so no separate MAC),
//! OpenSSH's strict key exchange (which closes the "Terrapin" prefix-truncation attack), a sign-in by password or key,
//! and one channel to the server's `sftp` subsystem. Another key exchange the server asks for mid-session is done.
//! Every packet is bounded, and every wait timed.

use std::time::Duration;

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, chacha20_poly1305_openssh as chacha};
use ring::agreement;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::keys::{self, Pin, UserKey};
use super::wire::{In, Out};

/// The largest packet taken.
const MAX_PACKET: usize = 256 * 1024;
/// How long one packet may take to come.
const READ_TIME: Duration = Duration::from_secs(60);
/// The window this side gives the server's channel data, and the most data one of its packets carries.
const WINDOW: u32 = 2 * 1024 * 1024;
const OUR_MAX_PACKET: u32 = 32 * 1024;

const KEX: &[&str] = &[
    "curve25519-sha256",
    "curve25519-sha256@libssh.org",
    "ecdh-sha2-nistp256",
];
const CIPHERS: &[&str] = &[
    "aes256-gcm@openssh.com",
    "aes128-gcm@openssh.com",
    "chacha20-poly1305@openssh.com",
];
const STRICT_CLIENT: &str = "kex-strict-c-v00@openssh.com";
const STRICT_SERVER: &str = "kex-strict-s-v00@openssh.com";

const DISCONNECT: u8 = 1;
const IGNORE: u8 = 2;
const UNIMPLEMENTED: u8 = 3;
const DEBUG: u8 = 4;
const SERVICE_REQUEST: u8 = 5;
const SERVICE_ACCEPT: u8 = 6;
const EXT_INFO: u8 = 7;
const KEXINIT: u8 = 20;
const NEWKEYS: u8 = 21;
const KEX_ECDH_INIT: u8 = 30;
const KEX_ECDH_REPLY: u8 = 31;
const USERAUTH_REQUEST: u8 = 50;
const USERAUTH_FAILURE: u8 = 51;
const USERAUTH_SUCCESS: u8 = 52;
const USERAUTH_BANNER: u8 = 53;
const GLOBAL_REQUEST: u8 = 80;
const REQUEST_FAILURE: u8 = 82;
const CHANNEL_OPEN: u8 = 90;
const CHANNEL_OPEN_CONFIRMATION: u8 = 91;
const CHANNEL_OPEN_FAILURE: u8 = 92;
const CHANNEL_WINDOW_ADJUST: u8 = 93;
const CHANNEL_DATA: u8 = 94;
const CHANNEL_EXTENDED_DATA: u8 = 95;
const CHANNEL_EOF: u8 = 96;
const CHANNEL_CLOSE: u8 = 97;
const CHANNEL_REQUEST: u8 = 98;
const CHANNEL_SUCCESS: u8 = 99;
const CHANNEL_FAILURE: u8 = 100;

enum Sealer {
    Gcm(Box<LessSafeKey>, [u8; 12]),
    ChaCha(chacha::SealingKey),
}

enum Opener {
    Gcm(Box<LessSafeKey>, [u8; 12]),
    ChaCha(chacha::OpeningKey),
}

fn next_iv(iv: &mut [u8; 12]) {
    let (_, counter) = iv.split_at_mut(4);
    let mut n = [0u8; 8];
    n.copy_from_slice(counter);
    counter.copy_from_slice(&u64::from_be_bytes(n).wrapping_add(1).to_be_bytes());
}

fn sha256(parts: &[&[u8]]) -> Vec<u8> {
    let mut c = ring::digest::Context::new(&ring::digest::SHA256);
    for p in parts {
        c.update(p);
    }
    c.finish().as_ref().to_vec()
}

/// A key derived from the exchange's secret (RFC 4253 section 7.2): `letter` says which.
fn derive(k: &[u8], h: &[u8], session: &[u8], letter: u8, len: usize) -> Vec<u8> {
    let mut out = sha256(&[k, h, &[letter], session]);
    while out.len() < len {
        let more = sha256(&[k, h, &out]);
        out.extend(more);
    }
    out.truncate(len);
    out
}

fn choose<'a>(ours: &[&'a str], theirs: &str) -> Option<&'a str> {
    let theirs: Vec<&str> = theirs.split(',').collect();
    ours.iter().copied().find(|o| theirs.contains(o))
}

/// The server's channel to its `sftp` subsystem, as this side keeps it.
struct Channel {
    remote: u32,
    /// How much more the server will take.
    remote_window: u64,
    remote_max: usize,
    /// How much more this side said it will take.
    local_window: u32,
    input: Vec<u8>,
    closed: bool,
}

/// An SSH connection.
pub(crate) struct Transport {
    stream: TcpStream,
    host: String,
    buffer: Vec<u8>,
    send_seq: u32,
    recv_seq: u32,
    sealer: Option<Sealer>,
    opener: Option<Opener>,
    session_id: Vec<u8>,
    client_version: Vec<u8>,
    server_version: Vec<u8>,
    host_keys: Vec<&'static str>,
    /// The server's host key, as it sent it.
    pub host_key: Vec<u8>,
    pin: Option<Pin>,
    channel: Option<Channel>,
    rng: SystemRandom,
}

impl Transport {
    /// Connects over `stream` and agrees keys; the server's host key is checked against `pin` when there is one.
    pub(crate) async fn start(stream: TcpStream, host: &str, pin: Option<Pin>) -> Result<Self, String> {
        let mut t = Self {
            stream,
            host: host.to_owned(),
            buffer: Vec::new(),
            send_seq: 0,
            recv_seq: 0,
            sealer: None,
            opener: None,
            session_id: Vec::new(),
            client_version: format!("SSH-2.0-OpenDuct_{}", env!("CARGO_PKG_VERSION")).into_bytes(),
            server_version: Vec::new(),
            host_keys: Pin::algorithms(pin.as_ref()),
            host_key: Vec::new(),
            pin,
            channel: None,
            rng: SystemRandom::new(),
        };
        let mut hello = t.client_version.clone();
        hello.extend(b"\r\n");
        t.write(&hello).await?;
        t.server_version = t.version().await?;
        t.kex(None).await?;
        Ok(t)
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(|e| format!("{} could not be written to: {e}", self.host))
    }

    async fn fill(&mut self, want: usize) -> Result<(), String> {
        while self.buffer.len() < want {
            let mut chunk = vec![0u8; (want - self.buffer.len()).clamp(4096, 64 * 1024)];
            let n = tokio::time::timeout(READ_TIME, self.stream.read(&mut chunk))
                .await
                .map_err(|_| format!("{} did not answer in time", self.host))?
                .map_err(|e| format!("{} could not be read: {e}", self.host))?;
            if n == 0 {
                return Err(format!("{} closed the connection", self.host));
            }
            self.buffer.extend_from_slice(chunk.get(..n).unwrap_or_default());
        }
        Ok(())
    }

    fn take(&mut self, n: usize) -> Vec<u8> {
        self.buffer.drain(..n.min(self.buffer.len())).collect()
    }

    /// The server's version line, past any lines it sends before it.
    async fn version(&mut self) -> Result<Vec<u8>, String> {
        for _ in 0..50 {
            let line = loop {
                if let Some(at) = self.buffer.iter().position(|b| *b == b'\n') {
                    break self.take(at + 1);
                }
                if self.buffer.len() > 1024 {
                    return Err(format!("{} is not an SSH server", self.host));
                }
                self.fill(self.buffer.len() + 1).await?;
            };
            let text: Vec<u8> = line
                .iter()
                .copied()
                .filter(|b| *b != b'\r' && *b != b'\n')
                .collect();
            if text.starts_with(b"SSH-2.0-") || text.starts_with(b"SSH-1.99-") {
                return Ok(text);
            }
            if text.starts_with(b"SSH-") {
                return Err(format!("{} speaks an old SSH that is not spoken here", self.host));
            }
        }
        Err(format!("{} is not an SSH server", self.host))
    }

    /// Sends one packet's payload.
    pub(crate) async fn send(&mut self, payload: &[u8]) -> Result<(), String> {
        let (block, aead_len) = match &self.sealer {
            Some(Sealer::Gcm(..)) => (16, true),
            Some(Sealer::ChaCha(_)) => (8, true),
            None => (8, false),
        };
        let counted = 1 + payload.len() + if aead_len { 0 } else { 4 };
        let mut pad = block - counted % block;
        if pad < 4 {
            pad += block;
        }
        let length = u32::try_from(1 + payload.len() + pad).map_err(|_| "a packet too large".to_owned())?;
        let mut packet = Vec::with_capacity(4 + 1 + payload.len() + pad + 16);
        packet.extend_from_slice(&length.to_be_bytes());
        packet.push(u8::try_from(pad).unwrap_or(4));
        packet.extend_from_slice(payload);
        let mut padding = vec![0u8; pad];
        self.rng
            .fill(&mut padding)
            .map_err(|_| "no randomness".to_owned())?;
        packet.extend_from_slice(&padding);
        match &mut self.sealer {
            None => {}
            Some(Sealer::Gcm(key, iv)) => {
                let (head, body) = packet.split_at_mut(4);
                let nonce = Nonce::assume_unique_for_key(*iv);
                let tag = key
                    .seal_in_place_separate_tag(nonce, Aad::from(&*head), body)
                    .map_err(|_| "a packet could not be sealed".to_owned())?;
                packet.extend_from_slice(tag.as_ref());
                next_iv(iv);
            }
            Some(Sealer::ChaCha(key)) => {
                let mut tag = [0u8; chacha::TAG_LEN];
                key.seal_in_place(self.send_seq, &mut packet, &mut tag);
                packet.extend_from_slice(&tag);
            }
        }
        self.send_seq = self.send_seq.wrapping_add(1);
        self.write(&packet).await
    }

    /// Reads one packet's payload.
    async fn read_packet(&mut self) -> Result<Vec<u8>, String> {
        self.fill(4).await?;
        let first: [u8; 4] = self
            .buffer
            .get(..4)
            .and_then(|b| b.try_into().ok())
            .ok_or("a packet cut short")?;
        let length = match &self.opener {
            Some(Opener::ChaCha(key)) => u32::from_be_bytes(key.decrypt_packet_length(self.recv_seq, first)),
            _ => u32::from_be_bytes(first),
        };
        let length = usize::try_from(length).map_err(|_| "a packet too large".to_owned())?;
        let (block, tag) = match &self.opener {
            Some(Opener::Gcm(..)) => (16, 16),
            Some(Opener::ChaCha(_)) => (8, 16),
            None => (8, 0),
        };
        let counted = if self.opener.is_some() { length } else { length + 4 };
        if !(5..=MAX_PACKET).contains(&length) || counted % block != 0 {
            return Err(format!("{} sent a packet that is not SSH's", self.host));
        }
        self.fill(4 + length + tag).await?;
        let mut packet = self.take(4 + length + tag);
        let tag_bytes = packet.split_off(4 + length);
        let plain: Vec<u8> = match &mut self.opener {
            None => packet.get(4..).unwrap_or_default().to_vec(),
            Some(Opener::Gcm(key, iv)) => {
                let (head, body) = packet.split_at_mut(4);
                let mut sealed = body.to_vec();
                sealed.extend_from_slice(&tag_bytes);
                let nonce = Nonce::assume_unique_for_key(*iv);
                let opened = key
                    .open_in_place(nonce, Aad::from(&*head), &mut sealed)
                    .map_err(|_| format!("a packet from {} was tampered with", self.host))?
                    .to_vec();
                next_iv(iv);
                opened
            }
            Some(Opener::ChaCha(key)) => {
                let tag: [u8; chacha::TAG_LEN] = tag_bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| "a packet cut short".to_owned())?;
                key.open_in_place(self.recv_seq, &mut packet, &tag)
                    .map_err(|_| format!("a packet from {} was tampered with", self.host))?
                    .to_vec()
            }
        };
        self.recv_seq = self.recv_seq.wrapping_add(1);
        let (pad, rest) = plain.split_first().ok_or("an empty packet")?;
        let keep = rest
            .len()
            .checked_sub(usize::from(*pad))
            .filter(|_| *pad >= 4)
            .ok_or_else(|| format!("{} sent a packet that is not SSH's", self.host))?;
        Ok(rest.get(..keep).unwrap_or_default().to_vec())
    }

    /// The next message for the layers above: the transport's own (a debug note, another key exchange) are dealt
    /// with here.
    pub(crate) async fn next(&mut self) -> Result<Vec<u8>, String> {
        loop {
            let payload = self.read_packet().await?;
            match payload.first().copied() {
                Some(IGNORE | DEBUG | UNIMPLEMENTED | EXT_INFO) => {}
                Some(DISCONNECT) => {
                    let mut r = In::new(payload.get(1..).unwrap_or_default());
                    let _code = r.u32().unwrap_or(0);
                    let why: String = r.text().unwrap_or_default().chars().take(200).collect();
                    return Err(format!("{} ended the connection: {why}", self.host));
                }
                Some(KEXINIT) => self.kex(Some(payload)).await?,
                Some(GLOBAL_REQUEST) => {
                    let mut r = In::new(payload.get(1..).unwrap_or_default());
                    let _name = r.string()?;
                    if r.bool()? {
                        self.send(&[REQUEST_FAILURE]).await?;
                    }
                }
                Some(_) => return Ok(payload),
                None => return Err("an empty message".to_owned()),
            }
        }
    }

    fn kexinit(&self, first: bool) -> Result<Vec<u8>, String> {
        let mut cookie = [0u8; 16];
        self.rng
            .fill(&mut cookie)
            .map_err(|_| "no randomness".to_owned())?;
        let mut kex: Vec<&str> = KEX.to_vec();
        if first {
            kex.push("ext-info-c");
            kex.push(STRICT_CLIENT);
        }
        let ciphers = CIPHERS.join(",");
        Ok(Out::new(KEXINIT)
            .raw(&cookie)
            .text(&kex.join(","))
            .text(&self.host_keys.join(","))
            .text(&ciphers)
            .text(&ciphers)
            .text("hmac-sha2-256,hmac-sha2-512")
            .text("hmac-sha2-256,hmac-sha2-512")
            .text("none")
            .text("none")
            .text("")
            .text("")
            .bool(false)
            .u32(0)
            .0)
    }

    /// A key exchange: the first, or one the server asked for with its `KEXINIT` (`theirs`).
    async fn kex(&mut self, theirs: Option<Vec<u8>>) -> Result<(), String> {
        let first = self.session_id.is_empty();
        let ours = self.kexinit(first)?;
        self.send(&ours).await?;
        let theirs = match theirs {
            Some(t) => t,
            None => {
                let p = self.read_packet().await?;
                if p.first() != Some(&KEXINIT) {
                    return Err(format!("{} did not start the key exchange", self.host));
                }
                p
            }
        };
        let mut r = In::new(theirs.get(17..).ok_or("a key exchange cut short")?);
        let (kex_list, host_list) = (r.text()?, r.text()?);
        let (enc_out, enc_in) = (r.text()?, r.text()?);
        let (_, _, _, _, _, _) = (r.text()?, r.text()?, r.text()?, r.text()?, r.text()?, r.text()?);
        let guessed = r.bool()?;
        let mismatch = |what: &str, theirs: &str| {
            let shown: String = theirs.chars().take(200).collect();
            format!("{} and OpenDuct share no {what} (it offers {shown})", self.host)
        };
        let kex = choose(KEX, &kex_list).ok_or_else(|| mismatch("key exchange", &kex_list))?;
        let host_alg = choose(&self.host_keys, &host_list).ok_or_else(|| {
            if self.pin.is_some() {
                format!(
                    "{} does not offer the kind of key the credential's host_key pins",
                    self.host
                )
            } else {
                mismatch("kind of host key", &host_list)
            }
        })?;
        let send_cipher = choose(CIPHERS, &enc_out).ok_or_else(|| mismatch("cipher", &enc_out))?;
        let recv_cipher = choose(CIPHERS, &enc_in).ok_or_else(|| mismatch("cipher", &enc_in))?;
        let strict = first && kex_list.split(',').any(|k| k == STRICT_SERVER);
        if guessed
            && (kex_list.split(',').next() != Some(kex) || host_list.split(',').next() != Some(host_alg))
        {
            // A wrong guess's packet is passed by.
            let _ = self.read_packet().await?;
        }
        let curve = if kex == "ecdh-sha2-nistp256" {
            &agreement::ECDH_P256
        } else {
            &agreement::X25519
        };
        let private = agreement::EphemeralPrivateKey::generate(curve, &self.rng)
            .map_err(|_| "no randomness".to_owned())?;
        let public = private
            .compute_public_key()
            .map_err(|_| "no key".to_owned())?
            .as_ref()
            .to_vec();
        self.send(&Out::new(KEX_ECDH_INIT).string(&public).0).await?;
        let reply = self.read_packet().await?;
        if reply.first() != Some(&KEX_ECDH_REPLY) {
            return Err(format!("{} did not go on with the key exchange", self.host));
        }
        let mut r = In::new(reply.get(1..).unwrap_or_default());
        let (host_key, server_public, signature) =
            (r.string()?.to_vec(), r.string()?.to_vec(), r.string()?.to_vec());
        let shared = agreement::agree_ephemeral(
            private,
            &agreement::UnparsedPublicKey::new(curve, &server_public),
            |k| k.to_vec(),
        )
        .map_err(|_| format!("{} sent a key exchange that does not hold", self.host))?;
        if shared.iter().all(|b| *b == 0) {
            return Err(format!("{} sent a key exchange that does not hold", self.host));
        }
        let k = Out::default().mpint(&shared).0;
        let hash = sha256(&[
            &Out::default()
                .string(&self.client_version)
                .string(&self.server_version)
                .string(&ours)
                .string(&theirs)
                .string(&host_key)
                .string(&public)
                .string(&server_public)
                .0,
            &k,
        ]);
        keys::verify(host_alg, &host_key, &signature, &hash)?;
        if first {
            if let Some(pin) = &self.pin
                && keys::fingerprint(&host_key) != pin.fingerprint
            {
                return Err(format!(
                    "{}'s host key is not the one the credential knows: it is {}, not {} - if the server's key was \
                     changed on purpose, put the new one in host_key; otherwise something is between OpenDuct and it",
                    self.host,
                    keys::fingerprint(&host_key),
                    pin.fingerprint
                ));
            }
            self.host_key = host_key;
            self.session_id = hash.clone();
        } else if host_key != self.host_key {
            return Err(format!("{} changed its host key mid-session", self.host));
        }
        self.send(&[NEWKEYS]).await?;
        let session = self.session_id.clone();
        self.sealer = Some(sealer(
            send_cipher,
            &derive(&k, &hash, &session, b'A', 12),
            &derive(&k, &hash, &session, b'C', 64),
        )?);
        if strict {
            self.send_seq = 0;
        }
        let done = self.read_packet().await?;
        if done.first() != Some(&NEWKEYS) {
            return Err(format!("{} did not finish the key exchange", self.host));
        }
        self.opener = Some(opener(
            recv_cipher,
            &derive(&k, &hash, &session, b'B', 12),
            &derive(&k, &hash, &session, b'D', 64),
        )?);
        if strict {
            self.recv_seq = 0;
        }
        Ok(())
    }

    /// Signs in as `user`, with a key or a password.
    pub(crate) async fn sign_in(
        &mut self,
        user: &str,
        key: Option<&UserKey>,
        password: Option<&str>,
    ) -> Result<(), String> {
        self.send(&Out::new(SERVICE_REQUEST).text("ssh-userauth").0)
            .await?;
        let accepted = self.next().await?;
        if accepted.first() != Some(&SERVICE_ACCEPT) {
            return Err(format!("{} would not let OpenDuct sign in", self.host));
        }
        let mut tried = Vec::new();
        if let Some(key) = key {
            let blob = key.blob();
            let signed = Out::default()
                .string(&self.session_id)
                .byte(USERAUTH_REQUEST)
                .text(user)
                .text("ssh-connection")
                .text("publickey")
                .bool(true)
                .text(key.algorithm())
                .string(&blob)
                .0;
            let request = Out::new(USERAUTH_REQUEST)
                .text(user)
                .text("ssh-connection")
                .text("publickey")
                .bool(true)
                .text(key.algorithm())
                .string(&blob)
                .string(&key.sign(&signed)?)
                .0;
            self.send(&request).await?;
            if self.auth_answer().await? {
                return Ok(());
            }
            tried.push("its key");
        }
        if let Some(password) = password {
            let request = Out::new(USERAUTH_REQUEST)
                .text(user)
                .text("ssh-connection")
                .text("password")
                .bool(false)
                .text(password)
                .0;
            self.send(&request).await?;
            if self.auth_answer().await? {
                return Ok(());
            }
            tried.push("its password");
        }
        if tried.is_empty() {
            return Err("the sftp credential has neither a password nor a private_key".to_owned());
        }
        Err(format!(
            "{} refused the sign-in of {user} with {}",
            self.host,
            tried.join(" or ")
        ))
    }

    async fn auth_answer(&mut self) -> Result<bool, String> {
        loop {
            let m = self.next().await?;
            match m.first().copied() {
                Some(USERAUTH_SUCCESS) => return Ok(true),
                Some(USERAUTH_FAILURE) => return Ok(false),
                Some(USERAUTH_BANNER) => {}
                _ => {
                    return Err(format!(
                        "{} answered the sign-in in a way SSH does not",
                        self.host
                    ));
                }
            }
        }
    }

    /// Opens a channel to the server's `sftp` subsystem.
    pub(crate) async fn open_sftp(&mut self) -> Result<(), String> {
        let open = Out::new(CHANNEL_OPEN)
            .text("session")
            .u32(0)
            .u32(WINDOW)
            .u32(OUR_MAX_PACKET)
            .0;
        self.send(&open).await?;
        let m = self.next().await?;
        let mut r = In::new(m.get(1..).unwrap_or_default());
        match m.first().copied() {
            Some(CHANNEL_OPEN_CONFIRMATION) => {
                let _ours = r.u32()?;
                let remote = r.u32()?;
                let window = r.u32()?;
                let max = r.u32()?;
                self.channel = Some(Channel {
                    remote,
                    remote_window: u64::from(window),
                    remote_max: usize::try_from(max).unwrap_or(32_768).clamp(1024, 32_768),
                    local_window: WINDOW,
                    input: Vec::new(),
                    closed: false,
                });
            }
            Some(CHANNEL_OPEN_FAILURE) => return Err(format!("{} would not open a channel", self.host)),
            _ => return Err(format!("{} answered in a way SSH does not", self.host)),
        }
        let remote = self.channel.as_ref().map_or(0, |c| c.remote);
        let request = Out::new(CHANNEL_REQUEST)
            .u32(remote)
            .text("subsystem")
            .bool(true)
            .text("sftp")
            .0;
        self.send(&request).await?;
        loop {
            let m = self.next().await?;
            match m.first().copied() {
                Some(CHANNEL_SUCCESS) => return Ok(()),
                Some(CHANNEL_FAILURE) => return Err(format!("{} offers no SFTP", self.host)),
                _ => self.channel_message(&m).await?,
            }
        }
    }

    /// A message of the channel's: data kept, the server's window grown, the channel closed.
    async fn channel_message(&mut self, m: &[u8]) -> Result<(), String> {
        let mut r = In::new(m.get(1..).unwrap_or_default());
        let kind = m.first().copied();
        let _channel = r.u32()?;
        let Some(c) = self.channel.as_mut() else {
            return Err(format!(
                "{} sent channel data before a channel was open",
                self.host
            ));
        };
        let mut adjust = None;
        match kind {
            Some(CHANNEL_DATA | CHANNEL_EXTENDED_DATA) => {
                if kind == Some(CHANNEL_EXTENDED_DATA) {
                    let _stream = r.u32()?;
                }
                let data = r.string()?;
                let n = u32::try_from(data.len()).unwrap_or(u32::MAX);
                c.local_window = c
                    .local_window
                    .checked_sub(n)
                    .ok_or_else(|| format!("{} sent more than it was let", self.host))?;
                if kind == Some(CHANNEL_DATA) {
                    if c.input.len() + data.len() > 4 * MAX_PACKET {
                        return Err(format!("{} sent too much at once", self.host));
                    }
                    c.input.extend_from_slice(data);
                }
                if c.local_window < WINDOW / 2 {
                    adjust = Some((c.remote, WINDOW - c.local_window));
                    c.local_window = WINDOW;
                }
            }
            Some(CHANNEL_WINDOW_ADJUST) => {
                c.remote_window = c
                    .remote_window
                    .saturating_add(u64::from(r.u32()?))
                    .min(u64::from(u32::MAX));
            }
            Some(CHANNEL_EOF | CHANNEL_CLOSE) => c.closed = true,
            Some(CHANNEL_REQUEST) => {
                let _name = r.string()?;
                if r.bool()? {
                    let remote = c.remote;
                    self.send(&Out::new(CHANNEL_FAILURE).u32(remote).0).await?;
                }
            }
            _ => {}
        }
        if let Some((remote, add)) = adjust {
            self.send(&Out::new(CHANNEL_WINDOW_ADJUST).u32(remote).u32(add).0)
                .await?;
        }
        Ok(())
    }

    /// Writes to the channel, as much at a time as the server lets.
    pub(crate) async fn write_channel(&mut self, mut data: &[u8]) -> Result<(), String> {
        while !data.is_empty() {
            let (remote, window, max, closed) = match &self.channel {
                Some(c) => (c.remote, c.remote_window, c.remote_max, c.closed),
                None => return Err("no channel".to_owned()),
            };
            if closed {
                return Err(format!("{} closed the SFTP channel", self.host));
            }
            if window == 0 {
                let m = self.next().await?;
                self.channel_message(&m).await?;
                continue;
            }
            let n = data.len().min(max).min(usize::try_from(window).unwrap_or(max));
            let (now, rest) = data.split_at(n);
            self.send(&Out::new(CHANNEL_DATA).u32(remote).string(now).0)
                .await?;
            if let Some(c) = self.channel.as_mut() {
                c.remote_window -= u64::try_from(n).unwrap_or(0);
            }
            data = rest;
        }
        Ok(())
    }

    /// Exactly `n` bytes of the channel's data, as they come.
    pub(crate) async fn read_channel(&mut self, n: usize) -> Result<Vec<u8>, String> {
        loop {
            let Some(c) = self.channel.as_mut() else {
                return Err("no channel".to_owned());
            };
            if c.input.len() >= n {
                return Ok(c.input.drain(..n).collect());
            }
            if c.closed {
                return Err(format!("{} closed the SFTP channel", self.host));
            }
            let m = self.next().await?;
            self.channel_message(&m).await?;
        }
    }

    /// Says goodbye.
    pub(crate) async fn close(&mut self) {
        let goodbye = Out::new(DISCONNECT).u32(11).text("done").text("").0;
        let _ = tokio::time::timeout(Duration::from_secs(2), self.send(&goodbye)).await;
        let _ = self.stream.shutdown().await;
    }
}

fn gcm_key(name: &str, key: &[u8]) -> Result<LessSafeKey, String> {
    let (alg, len) = if name == "aes128-gcm@openssh.com" {
        (&aead::AES_128_GCM, 16)
    } else {
        (&aead::AES_256_GCM, 32)
    };
    let unbound =
        UnboundKey::new(alg, key.get(..len).ok_or("a key too short")?).map_err(|_| "a bad key".to_owned())?;
    Ok(LessSafeKey::new(unbound))
}

fn iv_of(iv: &[u8]) -> Result<[u8; 12], String> {
    iv.get(..12)
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| "an IV too short".to_owned())
}

fn sealer(name: &str, iv: &[u8], key: &[u8]) -> Result<Sealer, String> {
    if name == "chacha20-poly1305@openssh.com" {
        let k: [u8; 64] = key.try_into().map_err(|_| "a key too short".to_owned())?;
        return Ok(Sealer::ChaCha(chacha::SealingKey::new(&k)));
    }
    Ok(Sealer::Gcm(Box::new(gcm_key(name, key)?), iv_of(iv)?))
}

fn opener(name: &str, iv: &[u8], key: &[u8]) -> Result<Opener, String> {
    if name == "chacha20-poly1305@openssh.com" {
        let k: [u8; 64] = key.try_into().map_err(|_| "a key too short".to_owned())?;
        return Ok(Opener::ChaCha(chacha::OpeningKey::new(&k)));
    }
    Ok(Opener::Gcm(Box::new(gcm_key(name, key)?), iv_of(iv)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algorithms_are_chosen_as_the_client_prefers() {
        assert_eq!(
            choose(
                CIPHERS,
                "aes128-ctr,chacha20-poly1305@openssh.com,aes256-gcm@openssh.com"
            ),
            Some("aes256-gcm@openssh.com")
        );
        assert_eq!(choose(KEX, "diffie-hellman-group14-sha256"), None);
        assert_eq!(
            choose(KEX, "sntrup761x25519-sha512,curve25519-sha256@libssh.org"),
            Some("curve25519-sha256@libssh.org")
        );
    }

    #[test]
    fn keys_are_derived_long_enough_and_ivs_count_up() {
        let k = derive(b"k", b"h", b"s", b'C', 64);
        assert_eq!(k.len(), 64);
        assert_eq!(k.get(..32), Some(sha256(&[b"k", b"h", b"C", b"s"]).as_slice()));
        assert_ne!(
            derive(b"k", b"h", b"s", b'D', 32),
            derive(b"k", b"h", b"s", b'C', 32)
        );
        let mut iv = [0u8, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0xff];
        next_iv(&mut iv);
        assert_eq!(iv, [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0]);
        let mut top = [9u8, 9, 9, 9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        next_iv(&mut top);
        assert_eq!(top, [9, 9, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0], "the fixed part stays");
    }

    #[test]
    fn a_sealed_packet_opens_with_the_same_keys_and_not_once_changed() {
        for name in CIPHERS {
            let key = derive(b"k", b"h", b"s", b'C', 64);
            let iv = derive(b"k", b"h", b"s", b'A', 12);
            let seal = sealer(name, &iv, &key).unwrap();
            let open = opener(name, &iv, &key).unwrap();
            let mut packet = vec![0, 0, 0, 16, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            let original = packet.clone();
            let tag = match seal {
                Sealer::Gcm(k, iv) => {
                    let (head, body) = packet.split_at_mut(4);
                    k.seal_in_place_separate_tag(Nonce::assume_unique_for_key(iv), Aad::from(&*head), body)
                        .unwrap()
                        .as_ref()
                        .to_vec()
                }
                Sealer::ChaCha(k) => {
                    let mut tag = [0u8; 16];
                    k.seal_in_place(3, &mut packet, &mut tag);
                    tag.to_vec()
                }
            };
            assert_ne!(packet.get(4..), original.get(4..), "{name}: sealed");
            match open {
                Opener::Gcm(k, iv) => {
                    let mut sealed = packet.get(4..).unwrap().to_vec();
                    sealed.extend(&tag);
                    let mut bad = sealed.clone();
                    bad[0] ^= 1;
                    assert!(
                        k.open_in_place(
                            Nonce::assume_unique_for_key(iv),
                            Aad::from(&packet[..4]),
                            &mut bad
                        )
                        .is_err()
                    );
                    let plain = k
                        .open_in_place(
                            Nonce::assume_unique_for_key(iv),
                            Aad::from(&packet[..4]),
                            &mut sealed,
                        )
                        .unwrap();
                    assert_eq!(plain, &original[4..]);
                }
                Opener::ChaCha(k) => {
                    let tag: [u8; 16] = tag.try_into().unwrap();
                    let mut bad = packet.clone();
                    bad[6] ^= 1;
                    assert!(k.open_in_place(3, &mut bad, &tag).is_err());
                    assert!(
                        k.open_in_place(4, &mut packet.clone(), &tag).is_err(),
                        "another sequence number"
                    );
                    assert_eq!(k.open_in_place(3, &mut packet, &tag).unwrap(), &original[4..]);
                }
            }
        }
    }
}
