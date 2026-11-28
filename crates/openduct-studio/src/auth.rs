// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Signing in: passwords kept as PBKDF2-HMAC-SHA256 hashes (600,000 rounds, a salt each), sessions as signed cookies
//! (so any node of a cluster can check one), and a token for every change, so that another site a person has open
//! cannot act for them.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use ring::rand::{SecureRandom, SystemRandom};
use ring::{hmac, pbkdf2};

/// PBKDF2 rounds, as OWASP advises for HMAC-SHA256.
const ROUNDS: u32 = 600_000;
/// How long a session lasts.
pub(crate) const SESSION: Duration = Duration::from_secs(7 * 24 * 3600);
/// The shortest password taken.
pub(crate) const MIN_PASSWORD: usize = 10;
/// The longest password taken (hashing has a cost: no one may send megabytes of it).
pub(crate) const MAX_PASSWORD: usize = 256;
/// Failed sign-ins from one address in a minute before it waits.
const FAILURES: u32 = 10;

/// The key sessions are signed with.
#[derive(Debug)]
pub(crate) struct Keys {
    session: hmac::Key,
}

impl Keys {
    /// The key kept in `dir` (made the first time), or one for this run alone.
    pub(crate) fn load(dir: Option<&Path>) -> Result<Self, String> {
        Ok(Self::from_key(&key(dir, "session.key")?))
    }

    /// Sessions signed with `bytes`: a cluster's shared key.
    pub(crate) fn from_key(bytes: &[u8]) -> Self {
        Self {
            session: hmac::Key::new(hmac::HMAC_SHA256, bytes),
        }
    }

    /// A session cookie's value for `email`, at the account's session generation `generation`.
    pub(crate) fn session(&self, email: &str, generation: u64) -> String {
        let expires = now().saturating_add(SESSION.as_secs());
        let payload = format!("{}|{expires}|{generation}", B64.encode(email));
        let tag = hmac::sign(&self.session, payload.as_bytes());
        format!("{payload}|{}", B64.encode(tag.as_ref()))
    }

    /// The account and generation a cookie's value was made for, if it is ours and has not expired.
    pub(crate) fn check(&self, value: &str) -> Option<(String, u64)> {
        let (payload, tag) = value.rsplit_once('|')?;
        let tag = B64.decode(tag).ok()?;
        hmac::verify(&self.session, payload.as_bytes(), &tag).ok()?;
        let mut parts = payload.split('|');
        let email = String::from_utf8(B64.decode(parts.next()?).ok()?).ok()?;
        let expires: u64 = parts.next()?.parse().ok()?;
        let generation: u64 = parts.next()?.parse().ok()?;
        (expires > now()).then_some((email, generation))
    }

    /// The token a session's changes carry (`x-openduct-csrf`).
    pub(crate) fn csrf(&self, session: &str) -> String {
        let tag = hmac::sign(&self.session, format!("csrf|{session}").as_bytes());
        B64.encode(tag.as_ref())
    }

    /// Whether `token` is the session's.
    pub(crate) fn csrf_ok(&self, session: &str, token: &str) -> bool {
        B64.decode(token)
            .is_ok_and(|t| hmac::verify(&self.session, format!("csrf|{session}").as_bytes(), &t).is_ok())
    }

    /// A signature of `text`: a form's page token, which any node can check.
    // forms-and-resume
    pub(crate) fn sign(&self, text: &str) -> String {
        B64.encode(hmac::sign(&self.session, format!("sign|{text}").as_bytes()).as_ref())
    }

    /// Whether `signature` is [`Keys::sign`]'s of `text`.
    pub(crate) fn signed(&self, text: &str, signature: &str) -> bool {
        B64.decode(signature)
            .is_ok_and(|t| hmac::verify(&self.session, format!("sign|{text}").as_bytes(), &t).is_ok())
    }
}

/// The 32-byte key kept as `name` in `dir` (made the first time, readable by this user only), or one for this run
/// alone when there is no directory.
pub(crate) fn key(dir: Option<&Path>, name: &str) -> Result<Vec<u8>, String> {
    match dir {
        Some(dir) => key_file(dir, name),
        None => Ok(random::<32>()?.to_vec()),
    }
}

/// The key kept as `name` in `dir`, if there is one.
pub(crate) fn existing_key(dir: &Path, name: &str) -> Result<Option<Vec<u8>>, String> {
    let file = dir.join(name);
    match std::fs::read(&file) {
        Ok(bytes) if bytes.len() == 32 => Ok(Some(bytes)),
        Ok(_) => Err(format!("{} is not a key OpenDuct made", file.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("cannot read {}: {e}", file.display())),
    }
}

fn key_file(dir: &Path, name: &str) -> Result<Vec<u8>, String> {
    let file = dir.join(name);
    match std::fs::read(&file) {
        Ok(bytes) if bytes.len() == 32 => return Ok(bytes),
        Ok(_) => return Err(format!("{} is not a key OpenDuct made", file.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("cannot read {}: {e}", file.display())),
    }
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot make {}: {e}", dir.display()))?;
    let bytes = random::<32>()?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut out = options
        .open(&file)
        .map_err(|e| format!("cannot make {}: {e}", file.display()))?;
    std::io::Write::write_all(&mut out, &bytes)
        .map_err(|e| format!("cannot write {}: {e}", file.display()))?;
    out.sync_all()
        .map_err(|e| format!("cannot write {}: {e}", file.display()))?;
    Ok(bytes.to_vec())
}

fn random<const N: usize>() -> Result<[u8; N], String> {
    let mut bytes = [0u8; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "the system gave no random bytes".to_owned())?;
    Ok(bytes)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A password as it is kept: `pbkdf2-sha256$<rounds>$<salt>$<hash>`.
pub(crate) fn hash_password(password: &str) -> Result<String, String> {
    let salt = random::<16>()?;
    let mut hash = [0u8; 32];
    let rounds = NonZeroU32::new(ROUNDS).ok_or("no rounds")?;
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        rounds,
        &salt,
        password.as_bytes(),
        &mut hash,
    );
    Ok(format!(
        "pbkdf2-sha256${ROUNDS}${}${}",
        B64.encode(salt),
        B64.encode(hash)
    ))
}

/// Whether `password` is the one kept as `stored`.
pub(crate) fn password_ok(stored: &str, password: &str) -> bool {
    let mut parts = stored.split('$');
    let (Some("pbkdf2-sha256"), Some(rounds), Some(salt), Some(hash)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let (Some(rounds), Ok(salt), Ok(hash)) = (
        rounds.parse::<u32>().ok().and_then(NonZeroU32::new),
        B64.decode(salt),
        B64.decode(hash),
    ) else {
        return false;
    };
    pbkdf2::verify(
        pbkdf2::PBKDF2_HMAC_SHA256,
        rounds,
        &salt,
        password.as_bytes(),
        &hash,
    )
    .is_ok()
}

/// Failed sign-ins by address: after [`FAILURES`] in a minute, an address waits out the minute.
#[derive(Debug, Default)]
pub(crate) struct Failures {
    seen: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl Failures {
    /// Whether `ip` may try now.
    pub(crate) fn may_try(&self, ip: IpAddr) -> bool {
        let Ok(seen) = self.seen.lock() else { return false };
        seen.get(&ip)
            .is_none_or(|(since, count)| since.elapsed() > Duration::from_secs(60) || *count < FAILURES)
    }

    pub(crate) fn failed(&self, ip: IpAddr) {
        let Ok(mut seen) = self.seen.lock() else { return };
        if seen.len() > 100_000 {
            seen.retain(|_, (since, _)| since.elapsed() <= Duration::from_secs(60));
        }
        let entry = seen.entry(ip).or_insert((Instant::now(), 0));
        if entry.0.elapsed() > Duration::from_secs(60) {
            *entry = (Instant::now(), 0);
        }
        entry.1 = entry.1.saturating_add(1);
    }

    pub(crate) fn succeeded(&self, ip: IpAddr) {
        if let Ok(mut seen) = self.seen.lock() {
            seen.remove(&ip);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_is_kept_as_a_salted_hash_and_checked() {
        let kept = hash_password("correct horse battery").unwrap();
        assert!(kept.starts_with("pbkdf2-sha256$600000$"));
        assert!(!kept.contains("correct"));
        assert!(password_ok(&kept, "correct horse battery"));
        assert!(!password_ok(&kept, "correct horse batterz"));
        assert_ne!(
            kept,
            hash_password("correct horse battery").unwrap(),
            "a salt each"
        );
        assert!(!password_ok("anything else", "x"));
    }

    #[test]
    fn a_session_is_ours_only_untouched_and_unexpired() {
        let keys = Keys::load(None).unwrap();
        let value = keys.session("ada@example.com", 3);
        assert_eq!(keys.check(&value), Some(("ada@example.com".to_owned(), 3)));
        let other = Keys::load(None).unwrap();
        assert_eq!(other.check(&value), None, "another key's");
        let forged = value.replacen("|3|", "|4|", 1);
        assert_eq!(keys.check(&forged), None, "changed");
        let csrf = keys.csrf(&value);
        assert!(keys.csrf_ok(&value, &csrf));
        assert!(!keys.csrf_ok(&keys.session("grace@example.com", 1), &csrf));
    }

    #[test]
    fn an_address_that_keeps_failing_waits() {
        let failures = Failures::default();
        let ip: IpAddr = "192.0.2.7".parse().unwrap();
        for _ in 0..FAILURES {
            assert!(failures.may_try(ip));
            failures.failed(ip);
        }
        assert!(!failures.may_try(ip));
        assert!(
            failures.may_try("192.0.2.8".parse().unwrap()),
            "only that address"
        );
        failures.succeeded(ip);
        assert!(failures.may_try(ip));
    }
}
