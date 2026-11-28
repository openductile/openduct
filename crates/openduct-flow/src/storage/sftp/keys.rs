// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The keys of an SSH connection: the server's (its host key, whose signature proves the server holds it, checked
//! against the one the credential pins or recorded), and the client's own, when it signs in with a key rather than a
//! password. Ed25519, ECDSA on P-256 and RSA (SHA-2 signatures only), all by `ring`.

use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{self, KeyPair as _};

use super::wire::{In, Out};

/// The host key algorithms offered, best first.
pub(crate) const HOST_KEYS: &[&str] = &[
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "rsa-sha2-512",
    "rsa-sha2-256",
];

/// A host key's fingerprint as `ssh-keygen -l` shows it: `SHA256:` and its hash in Base64.
pub(crate) fn fingerprint(blob: &[u8]) -> String {
    let hash = ring::digest::digest(&ring::digest::SHA256, blob);
    format!(
        "SHA256:{}",
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(hash.as_ref())
    )
}

/// The type a key's blob says it is (`ssh-ed25519`, `ssh-rsa` ...).
pub(crate) fn blob_type(blob: &[u8]) -> Result<String, String> {
    In::new(blob).text()
}

/// The host key a credential pins: its fingerprint, and its type when given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pin {
    pub kind: Option<String>,
    pub fingerprint: String,
}

impl Pin {
    /// A pin as given: `SHA256:...`, `<type> SHA256:...`, or the key itself as `known_hosts` and `ssh-keyscan` write
    /// it (`<type> <base64>`, a host name before it allowed).
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let words: Vec<&str> = text.split_whitespace().collect();
        let is_type = |w: &str| w.starts_with("ssh-") || w.starts_with("ecdsa-") || w.starts_with("rsa-");
        let words = match words.as_slice() {
            [host, kind, ..] if !is_type(host) && !host.starts_with("SHA256:") && is_type(kind) => {
                words.get(1..).unwrap_or_default()
            }
            _ => words.as_slice(),
        };
        let bad = || {
            "host_key: the server's key, as ssh-keyscan gives it (ssh-ed25519 AAAA...) or its fingerprint \
             (SHA256:...)"
                .to_owned()
        };
        match words {
            [fp] if fp.starts_with("SHA256:") => Ok(Self {
                kind: None,
                fingerprint: (*fp).to_owned(),
            }),
            [kind, fp, ..] if fp.starts_with("SHA256:") => Ok(Self {
                kind: Some((*kind).to_owned()),
                fingerprint: (*fp).to_owned(),
            }),
            [kind, key, ..] => {
                let blob = base64::engine::general_purpose::STANDARD
                    .decode(key)
                    .map_err(|_| bad())?;
                if blob_type(&blob).ok().as_deref() != Some(kind) {
                    return Err(bad());
                }
                Ok(Self {
                    kind: Some((*kind).to_owned()),
                    fingerprint: fingerprint(&blob),
                })
            }
            _ => Err(bad()),
        }
    }

    /// The host key algorithms to offer: the pinned key's own, or every one.
    pub(crate) fn algorithms(pin: Option<&Self>) -> Vec<&'static str> {
        match pin.and_then(|p| p.kind.as_deref()) {
            Some("ssh-rsa") => vec!["rsa-sha2-512", "rsa-sha2-256"],
            Some(kind) => HOST_KEYS.iter().copied().filter(|k| *k == kind).collect(),
            None => HOST_KEYS.to_vec(),
        }
    }

    /// How a key seen the first time is kept: `<type> SHA256:...`.
    pub(crate) fn recorded(blob: &[u8]) -> Result<String, String> {
        Ok(format!("{} {}", blob_type(blob)?, fingerprint(blob)))
    }
}

fn fixed(number: &[u8], width: usize) -> Result<Vec<u8>, String> {
    if number.len() > width {
        return Err("a signature of the wrong size".to_owned());
    }
    let mut out = vec![0u8; width - number.len()];
    out.extend_from_slice(number);
    Ok(out)
}

/// Whether `signature` (SSH's blob) is the server's, by host key `blob` (of algorithm `algorithm`), over `data`.
pub(crate) fn verify(algorithm: &str, blob: &[u8], signature: &[u8], data: &[u8]) -> Result<(), String> {
    let refused =
        || "the server's signature does not match its key: something is between OpenDuct and it".to_owned();
    let mut key = In::new(blob);
    let kind = key.text()?;
    let mut sig = In::new(signature);
    let sig_kind = sig.text()?;
    let sig_bytes = sig.string()?;
    if sig_kind != algorithm {
        return Err(refused());
    }
    match (algorithm, kind.as_str()) {
        ("ssh-ed25519", "ssh-ed25519") => {
            signature::UnparsedPublicKey::new(&signature::ED25519, key.string()?)
                .verify(data, sig_bytes)
                .map_err(|_| refused())
        }
        ("ecdsa-sha2-nistp256", "ecdsa-sha2-nistp256") => {
            let _curve = key.string()?;
            let point = key.string()?;
            let mut parts = In::new(sig_bytes);
            let mut rs = fixed(parts.mpint()?, 32)?;
            rs.extend(fixed(parts.mpint()?, 32)?);
            signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, point)
                .verify(data, &rs)
                .map_err(|_| refused())
        }
        ("rsa-sha2-256" | "rsa-sha2-512", "ssh-rsa") => {
            let e = key.mpint()?;
            let n = key.mpint()?;
            let params = if algorithm == "rsa-sha2-256" {
                &signature::RSA_PKCS1_2048_8192_SHA256
            } else {
                &signature::RSA_PKCS1_2048_8192_SHA512
            };
            signature::RsaPublicKeyComponents { n, e }
                .verify(params, data, sig_bytes)
                .map_err(|_| refused())
        }
        _ => Err(refused()),
    }
}

/// The key a client signs in with.
#[derive(Debug)]
pub(crate) enum UserKey {
    Ed25519(signature::Ed25519KeyPair),
    Ecdsa(signature::EcdsaKeyPair),
    Rsa(signature::RsaKeyPair),
}

const ENCRYPTED: &str = "private_key: a key protected by a passphrase is not read here: give it without one \
                         (ssh-keygen -p -N \"\" -f <key>)";

fn pem_body(text: &str, label: &str) -> Option<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = text.find(&begin)? + begin.len();
    let stop = text.get(start..)?.find(&end)? + start;
    let body: String = text
        .get(start..stop)?
        .lines()
        .filter(|l| !l.contains(':'))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD.decode(body).ok()
}

/// `a` modulo `m`, both unsigned big-endian: the remainder of long division, bit by bit.
fn remainder(a: &[u8], m: &[u8]) -> Vec<u8> {
    let width = m.len() + 1;
    let mut r = vec![0u8; width];
    let m_wide: Vec<u8> = std::iter::once(0).chain(m.iter().copied()).collect();
    for byte in a {
        for bit in (0..8).rev() {
            let mut carry = (byte >> bit) & 1;
            for b in r.iter_mut().rev() {
                let next = *b >> 7;
                *b = (*b << 1) | carry;
                carry = next;
            }
            if r >= m_wide {
                let mut borrow = 0i16;
                for (x, y) in r.iter_mut().rev().zip(m_wide.iter().rev()) {
                    let mut v = i16::from(*x) - i16::from(*y) - borrow;
                    borrow = 0;
                    if v < 0 {
                        v += 256;
                        borrow = 1;
                    }
                    *x = u8::try_from(v).unwrap_or(0);
                }
            }
        }
    }
    let start = r.iter().position(|b| *b != 0).unwrap_or(r.len());
    r.get(start..).unwrap_or_default().to_vec()
}

fn minus_one(n: &[u8]) -> Vec<u8> {
    let mut out = n.to_vec();
    for b in out.iter_mut().rev() {
        if *b == 0 {
            *b = 0xff;
        } else {
            *b -= 1;
            break;
        }
    }
    out
}

impl UserKey {
    /// A private key as given: OpenSSH's own form, or PEM (PKCS#1 RSA, PKCS#8), never protected by a passphrase.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let rng = SystemRandom::new();
        if text.contains("ENCRYPTED") {
            return Err(ENCRYPTED.to_owned());
        }
        if let Some(der) = pem_body(text, "OPENSSH PRIVATE KEY") {
            return Self::openssh(&der, &rng);
        }
        if let Some(der) = pem_body(text, "RSA PRIVATE KEY") {
            return signature::RsaKeyPair::from_der(&der)
                .map(Self::Rsa)
                .map_err(|e| format!("private_key: the RSA key was not accepted: {e}"));
        }
        if let Some(der) = pem_body(text, "PRIVATE KEY") {
            if let Ok(k) = signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der) {
                return Ok(Self::Ed25519(k));
            }
            if let Ok(k) =
                signature::EcdsaKeyPair::from_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, &der, &rng)
            {
                return Ok(Self::Ecdsa(k));
            }
            return signature::RsaKeyPair::from_pkcs8(&der)
                .map(Self::Rsa)
                .map_err(|e| format!("private_key: the key was not accepted: {e}"));
        }
        Err("private_key: an OpenSSH private key, the whole file as ssh-keygen makes it".to_owned())
    }

    fn openssh(der: &[u8], rng: &SystemRandom) -> Result<Self, String> {
        let body = der
            .strip_prefix(b"openssh-key-v1\0")
            .ok_or("private_key: not an OpenSSH private key")?;
        let mut r = In::new(body);
        let cipher = r.text()?;
        let _kdf = r.text()?;
        let _options = r.string()?;
        if cipher != "none" {
            return Err(ENCRYPTED.to_owned());
        }
        if r.u32()? != 1 {
            return Err("private_key: one key, not several".to_owned());
        }
        let _public = r.string()?;
        let mut p = In::new(r.string()?);
        if p.u32()? != p.u32()? {
            return Err("private_key: a damaged key".to_owned());
        }
        let rejected = |e: ring::error::KeyRejected| format!("private_key: the key was not accepted: {e}");
        match p.text()?.as_str() {
            "ssh-ed25519" => {
                let public = p.string()?;
                let private = p.string()?;
                let seed = private.get(..32).ok_or("private_key: a damaged key")?;
                signature::Ed25519KeyPair::from_seed_and_public_key(seed, public)
                    .map(Self::Ed25519)
                    .map_err(rejected)
            }
            "ecdsa-sha2-nistp256" => {
                let _curve = p.string()?;
                let point = p.string()?;
                let scalar = fixed(p.mpint()?, 32)?;
                signature::EcdsaKeyPair::from_private_key_and_public_key(
                    &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
                    &scalar,
                    point,
                    rng,
                )
                .map(Self::Ecdsa)
                .map_err(rejected)
            }
            "ssh-rsa" => {
                let (n, e, d, iqmp, pp, q) = (
                    p.mpint()?,
                    p.mpint()?,
                    p.mpint()?,
                    p.mpint()?,
                    p.mpint()?,
                    p.mpint()?,
                );
                let dp = remainder(d, &minus_one(pp));
                let dq = remainder(d, &minus_one(q));
                let parts = ring::rsa::KeyPairComponents {
                    public_key: ring::rsa::PublicKeyComponents { n, e },
                    d,
                    p: pp,
                    q,
                    dP: dp.as_slice(),
                    dQ: dq.as_slice(),
                    qInv: iqmp,
                };
                signature::RsaKeyPair::from_components(&parts)
                    .map(Self::Rsa)
                    .map_err(rejected)
            }
            other => Err(format!(
                "private_key: a {other} key is not read here: ed25519, ecdsa (P-256) or rsa"
            )),
        }
    }

    /// The algorithm its signatures are made with.
    pub(crate) fn algorithm(&self) -> &'static str {
        match self {
            Self::Ed25519(_) => "ssh-ed25519",
            Self::Ecdsa(_) => "ecdsa-sha2-nistp256",
            Self::Rsa(_) => "rsa-sha2-512",
        }
    }

    /// Its public key, as SSH sends one.
    pub(crate) fn blob(&self) -> Vec<u8> {
        match self {
            Self::Ed25519(k) => {
                Out::default()
                    .text("ssh-ed25519")
                    .string(k.public_key().as_ref())
                    .0
            }
            Self::Ecdsa(k) => {
                Out::default()
                    .text("ecdsa-sha2-nistp256")
                    .text("nistp256")
                    .string(k.public_key().as_ref())
                    .0
            }
            Self::Rsa(k) => {
                let parts: ring::rsa::PublicKeyComponents<Vec<u8>> = k.public().into();
                Out::default().text("ssh-rsa").mpint(&parts.e).mpint(&parts.n).0
            }
        }
    }

    /// Its signature over `data`, as SSH sends one.
    pub(crate) fn sign(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let rng = SystemRandom::new();
        let failed = |_| "the key could not sign".to_owned();
        let raw = match self {
            Self::Ed25519(k) => k.sign(data).as_ref().to_vec(),
            Self::Ecdsa(k) => {
                let rs = k.sign(&rng, data).map_err(failed)?;
                let (r, s) = rs.as_ref().split_at(32);
                Out::default().mpint(r).mpint(s).0
            }
            Self::Rsa(k) => {
                let mut out = vec![0u8; k.public().modulus_len()];
                k.sign(&signature::RSA_PKCS1_SHA512, &rng, data, &mut out)
                    .map_err(failed)?;
                out
            }
        };
        Ok(Out::default().text(self.algorithm()).string(&raw).0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pin_is_read_in_each_form_it_comes_in() {
        let key = signature::Ed25519KeyPair::from_seed_unchecked(&[7u8; 32]).unwrap();
        let blob = Out::default()
            .text("ssh-ed25519")
            .string(key.public_key().as_ref())
            .0;
        let line = format!(
            "ssh-ed25519 {}",
            base64::engine::general_purpose::STANDARD.encode(&blob)
        );
        let fp = fingerprint(&blob);
        assert!(fp.starts_with("SHA256:") && !fp.ends_with('='));
        for given in [
            line.clone(),
            format!("files.example.com {line} comment"),
            format!("ssh-ed25519 {fp}"),
            Pin::recorded(&blob).unwrap(),
        ] {
            let pin = Pin::parse(&given).unwrap();
            assert_eq!(pin.fingerprint, fp, "{given}");
            assert_eq!(pin.kind.as_deref(), Some("ssh-ed25519"), "{given}");
        }
        assert_eq!(Pin::parse(&fp).unwrap().kind, None);
        assert!(Pin::parse("nonsense").is_err());
        assert!(Pin::parse("ssh-rsa AAAA").is_err());
        assert!(
            Pin::parse(&format!("ssh-rsa {}", line.split(' ').nth(1).unwrap())).is_err(),
            "the wrong type"
        );
        assert_eq!(
            Pin::algorithms(Some(&Pin::parse(&line).unwrap())),
            ["ssh-ed25519"]
        );
        let rsa = Pin {
            kind: Some("ssh-rsa".into()),
            fingerprint: fp,
        };
        assert_eq!(Pin::algorithms(Some(&rsa)), ["rsa-sha2-512", "rsa-sha2-256"]);
        assert_eq!(Pin::algorithms(None).len(), 4);
    }

    #[test]
    fn a_signature_is_verified_and_a_forged_one_refused() {
        let key = signature::Ed25519KeyPair::from_seed_unchecked(&[9u8; 32]).unwrap();
        let blob = Out::default()
            .text("ssh-ed25519")
            .string(key.public_key().as_ref())
            .0;
        let user = UserKey::Ed25519(signature::Ed25519KeyPair::from_seed_unchecked(&[9u8; 32]).unwrap());
        assert_eq!(user.blob(), blob);
        let sig = user.sign(b"exchange hash").unwrap();
        assert!(verify("ssh-ed25519", &blob, &sig, b"exchange hash").is_ok());
        assert!(verify("ssh-ed25519", &blob, &sig, b"another hash").is_err());
        assert!(verify("ecdsa-sha2-nistp256", &blob, &sig, b"exchange hash").is_err());
        assert!(verify("ssh-ed25519", &blob, b"\0\0\0\x01", b"x").is_err());
    }

    #[test]
    fn a_remainder_is_long_division() {
        assert_eq!(remainder(&[0x01, 0x00], &[0x07]), [0x04]); // 256 mod 7
        assert_eq!(remainder(&[0x0a], &[0x0a]), Vec::<u8>::new());
        assert_eq!(remainder(&[0xff, 0xff, 0xff], &[0x01, 0x00]), [0xff]);
        assert_eq!(minus_one(&[0x01, 0x00]), [0x00, 0xff]);
        let n = 0x1234_5678_9abc_u64;
        let m = 0x00ff_fff1_u64;
        let got = remainder(&n.to_be_bytes(), &m.to_be_bytes()[4..]);
        let mut want = (n % m).to_be_bytes().to_vec();
        want.retain(|_| true);
        let start = want.iter().position(|b| *b != 0).unwrap_or(want.len());
        assert_eq!(got, &want[start..]);
    }

    #[test]
    fn a_protected_or_unknown_key_is_said_plainly() {
        assert!(
            UserKey::parse("Proc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n")
                .unwrap_err()
                .contains("passphrase")
        );
        assert!(
            UserKey::parse("hello")
                .unwrap_err()
                .contains("OpenSSH private key")
        );
    }
}
