// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Signing in to MySQL: the scrambles of `mysql_native_password` and `caching_sha2_password`, and, for the latter's
//! full sign-in on a connection without TLS, the password encrypted to the server's RSA public key (RSA-OAEP with
//! SHA-1, as MySQL does). Only the public-key side of RSA is needed, so it is written here with plain big-number
//! arithmetic rather than a whole RSA library.

use num_bigint::BigUint;
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// `SHA1(password) XOR SHA1(nonce + SHA1(SHA1(password)))`.
pub(super) fn native(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let first = Sha1::digest(password);
    let second = Sha1::digest(first);
    let mut third = Sha1::new();
    third.update(nonce);
    third.update(second);
    first.iter().zip(third.finalize()).map(|(a, b)| a ^ b).collect()
}

/// `SHA256(password) XOR SHA256(SHA256(SHA256(password)) + nonce)`.
pub(super) fn caching_sha2(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let first = Sha256::digest(password);
    let second = Sha256::digest(first);
    let mut third = Sha256::new();
    third.update(second);
    third.update(nonce);
    first.iter().zip(third.finalize()).map(|(a, b)| a ^ b).collect()
}

/// The password, ended with a zero and mixed with the nonce, encrypted to `public_key` (a PEM `PUBLIC KEY`), as
/// `caching_sha2_password` asks for on a connection without TLS.
pub(super) fn encrypt_password(password: &[u8], nonce: &[u8], public_key: &[u8]) -> Result<Vec<u8>, String> {
    let (n, e) = public_key_from_pem(public_key)?;
    let mut message = password.to_vec();
    message.push(0);
    if !nonce.is_empty() {
        for (i, b) in message.iter_mut().enumerate() {
            *b ^= nonce.get(i % nonce.len()).copied().unwrap_or(0);
        }
    }
    let k = usize::try_from(n.bits().div_ceil(8)).map_err(|_| "a key too large")?;
    let seed = random_seed()?;
    let padded = oaep_sha1(&message, k, &seed)?;
    let c = BigUint::from_bytes_be(&padded).modpow(&e, &n).to_bytes_be();
    // Left-padded to the key's length, as RSA ciphertexts are.
    let mut out = vec![0u8; k.saturating_sub(c.len())];
    out.extend_from_slice(&c);
    Ok(out)
}

fn random_seed() -> Result<[u8; 20], String> {
    let mut seed = [0u8; 20];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut seed)
        .map_err(|_| "cannot make random bytes for encrypting the password".to_owned())?;
    Ok(seed)
}

/// EME-OAEP encoding (RFC 8017, section 7.1.1) with SHA-1 and an empty label.
fn oaep_sha1(message: &[u8], k: usize, seed: &[u8; 20]) -> Result<Vec<u8>, String> {
    const H: usize = 20;
    if k < 2 * H + 2 || message.len() > k - 2 * H - 2 {
        return Err("the password is too long for the server's key".to_owned());
    }
    let mut db = Sha1::digest([]).to_vec();
    db.resize(k - H - 1 - message.len() - 1, 0);
    db.push(1);
    db.extend_from_slice(message);
    let db_mask = mgf1_sha1(seed, db.len());
    for (b, m) in db.iter_mut().zip(db_mask) {
        *b ^= m;
    }
    let seed_mask = mgf1_sha1(&db, H);
    let masked_seed: Vec<u8> = seed.iter().zip(seed_mask).map(|(s, m)| s ^ m).collect();
    let mut out = Vec::with_capacity(k);
    out.push(0);
    out.extend_from_slice(&masked_seed);
    out.extend_from_slice(&db);
    Ok(out)
}

fn mgf1_sha1(seed: &[u8], length: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(length + 20);
    let mut counter = 0u32;
    while out.len() < length {
        let mut h = Sha1::new();
        h.update(seed);
        h.update(counter.to_be_bytes());
        out.extend_from_slice(&h.finalize());
        counter += 1;
    }
    out.truncate(length);
    out
}

/// The modulus and exponent of a PEM `PUBLIC KEY` (SubjectPublicKeyInfo) or `RSA PUBLIC KEY` (PKCS #1).
fn public_key_from_pem(pem: &[u8]) -> Result<(BigUint, BigUint), String> {
    use base64::Engine;
    let text = std::str::from_utf8(pem).map_err(|_| "the server's public key is not PEM")?;
    let body: String = text
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .flat_map(|l| l.trim().chars())
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .map_err(|_| "the server's public key is not PEM")?;
    let bad = || "the server's public key is not an RSA key OpenDuct can read".to_owned();
    let mut outer = Der::new(&der).sequence().ok_or_else(bad)?;
    // SubjectPublicKeyInfo: SEQUENCE { AlgorithmIdentifier, BIT STRING { RSAPublicKey } }; or RSAPublicKey itself.
    let key = if outer.peek() == Some(0x30) {
        let _algorithm = outer.sequence().ok_or_else(bad)?;
        let bits = outer.element(0x03).ok_or_else(bad)?;
        let (&unused, key) = bits.split_first().ok_or_else(bad)?;
        if unused != 0 {
            return Err(bad());
        }
        Der::new(key).sequence().ok_or_else(bad)?
    } else {
        outer
    };
    let mut key = key;
    let n = BigUint::from_bytes_be(key.element(0x02).ok_or_else(bad)?);
    let e = BigUint::from_bytes_be(key.element(0x02).ok_or_else(bad)?);
    if n.bits() < 1024 || n.bits() > 16_384 || e < BigUint::from(3u8) {
        return Err(bad());
    }
    Ok((n, e))
}

/// Just enough DER to read an RSA public key: a cursor over elements.
struct Der<'a> {
    rest: &'a [u8],
}

impl<'a> Der<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    fn peek(&self) -> Option<u8> {
        self.rest.first().copied()
    }

    /// The contents of the next element, which must have tag `tag`.
    fn element(&mut self, tag: u8) -> Option<&'a [u8]> {
        let (&t, rest) = self.rest.split_first()?;
        if t != tag {
            return None;
        }
        let (&first, mut rest) = rest.split_first()?;
        let length = if first < 0x80 {
            usize::from(first)
        } else {
            let count = usize::from(first & 0x7f);
            if count == 0 || count > 4 {
                return None;
            }
            let (bytes, after) = (rest.get(..count)?, rest.get(count..)?);
            rest = after;
            bytes.iter().fold(0usize, |n, b| (n << 8) | usize::from(*b))
        };
        let (contents, after) = (rest.get(..length)?, rest.get(length..)?);
        self.rest = after;
        Some(contents)
    }

    fn sequence(&mut self) -> Option<Der<'a>> {
        self.element(0x30).map(Der::new)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn scrambles_are_what_the_servers_compute() {
        // Worked out apart from this code, with nonce 01..14 and password "secret".
        let nonce: Vec<u8> = (1..=20).collect();
        assert_eq!(hex(&native(b"secret", &nonce)), NATIVE);
        assert_eq!(hex(&caching_sha2(b"secret", &nonce)), CACHING_SHA2);
        assert!(native(b"", &nonce).is_empty());
    }

    const NATIVE: &str = "b32bb3a583e1340c0a1108d58b1be49781ad8c2f";
    const CACHING_SHA2: &str = "746ebe205d56a0707acb3e796e834e0dd7b1d61743b26bd5202c7a623230c7c9";

    #[test]
    fn oaep_padding_is_the_size_of_the_key_and_masked() {
        let seed = [7u8; 20];
        let padded = oaep_sha1(b"secret\0", 128, &seed).unwrap();
        assert_eq!((padded.len(), padded[0]), (128, 0));
        assert!(oaep_sha1(&[0u8; 100], 128, &seed).is_err());
    }

    #[test]
    fn keys_that_are_not_rsa_public_keys_are_refused() {
        assert!(
            public_key_from_pem(b"-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n").is_err()
        );
        assert!(public_key_from_pem(b"\xff").is_err());
        let mut der = Der::new(&[0x30, 0x84, 0xff, 0xff, 0xff, 0xff]);
        assert!(der.sequence().is_none());
    }
}
