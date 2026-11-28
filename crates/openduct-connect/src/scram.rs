// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802 and RFC 7677), for Kafka clusters and PostgreSQL: signing in without
//! the password crossing the network, and checking that the server knows it too, so a stand-in that only pretends
//! to be the server is found out.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{KeyInit, Mac};
use sha2::Digest;

/// The fewest iterations taken: fewer would make the password easy to guess from what crosses the network.
const MIN_ITERATIONS: u32 = 4_096;
/// The most: a server asking for more is asking for minutes of work at every connection.
const MAX_ITERATIONS: u32 = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hash {
    Sha256,
    Sha512,
}

/// One sign-in, from the first message to checking the server's proof.
pub struct Scram {
    hash: Hash,
    password: String,
    nonce: String,
    client_first_bare: String,
    server_signature: Option<Vec<u8>>,
}

impl std::fmt::Debug for Scram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scram")
            .field("hash", &self.hash)
            .finish_non_exhaustive()
    }
}

impl Scram {
    /// `nonce` is fresh and random for every sign-in (it is fixed only in tests).
    pub fn new(hash: Hash, username: &str, password: &str, nonce: String) -> Self {
        let name = username.replace('=', "=3D").replace(',', "=2C");
        Self {
            hash,
            password: password.to_owned(),
            client_first_bare: format!("n={name},r={nonce}"),
            nonce,
            server_signature: None,
        }
    }

    /// A nonce of 24 random bytes, as text.
    pub fn nonce() -> Result<String, String> {
        let mut bytes = [0u8; 24];
        rustls::crypto::ring::default_provider()
            .secure_random
            .fill(&mut bytes)
            .map_err(|_| "cannot make a random nonce for signing in".to_owned())?;
        Ok(STANDARD.encode(bytes))
    }

    pub fn client_first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// The answer to the server's challenge: proof of the password, not the password.
    pub fn client_final(&mut self, server_first: &[u8]) -> Result<Vec<u8>, String> {
        let text =
            std::str::from_utf8(server_first).map_err(|_| "sent a SCRAM challenge that is not text")?;
        let (mut nonce, mut salt, mut iterations) = (None, None, None);
        for part in text.split(',') {
            match part.split_once('=') {
                Some(("r", v)) => nonce = Some(v),
                Some(("s", v)) => salt = Some(v),
                Some(("i", v)) => iterations = Some(v),
                Some(("e", why)) => return Err(format!("refused signing in: {why}")),
                Some(("m", _)) => return Err("asked for a SCRAM extension OpenDuct does not know".to_owned()),
                _ => {}
            }
        }
        let nonce = nonce
            .filter(|n| n.len() > self.nonce.len() && n.starts_with(&self.nonce))
            .ok_or("answered the SCRAM challenge with a nonce that is not ours")?;
        let salt = salt
            .and_then(|s| STANDARD.decode(s).ok())
            .filter(|s| !s.is_empty())
            .ok_or("sent a SCRAM challenge without a salt")?;
        let iterations: u32 = iterations
            .and_then(|i| i.parse().ok())
            .filter(|i| (MIN_ITERATIONS..=MAX_ITERATIONS).contains(i))
            .ok_or_else(|| {
                format!("asked for a SCRAM iteration count outside {MIN_ITERATIONS} to {MAX_ITERATIONS}")
            })?;
        let salted = hi(self.hash, self.password.as_bytes(), &salt, iterations);
        let client_key = hmac(self.hash, &salted, b"Client Key");
        let stored_key = digest(self.hash, &client_key);
        // `biws` is "n,," in base64: no channel binding, no separate authorisation name.
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{},{text},{without_proof}", self.client_first_bare);
        let signature = hmac(self.hash, &stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&signature).map(|(k, s)| k ^ s).collect();
        let server_key = hmac(self.hash, &salted, b"Server Key");
        self.server_signature = Some(hmac(self.hash, &server_key, auth_message.as_bytes()));
        Ok(format!("{without_proof},p={}", STANDARD.encode(proof)).into_bytes())
    }

    /// The server's last word: its own proof that it knows the password.
    pub fn verify(&self, server_final: &[u8]) -> Result<(), String> {
        let text = std::str::from_utf8(server_final)
            .map_err(|_| "ended signing in with something that is not text")?;
        if let Some(why) = text.strip_prefix("e=") {
            return Err(format!("refused signing in: {why}"));
        }
        let given = text
            .strip_prefix("v=")
            .map(|v| v.split(',').next().unwrap_or(v))
            .and_then(|v| STANDARD.decode(v).ok())
            .ok_or("ended signing in without its proof")?;
        let expected = self.server_signature.as_deref().unwrap_or_default();
        let same = given.len() == expected.len()
            && given.iter().zip(expected).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0;
        if same {
            Ok(())
        } else {
            Err("did not prove it knows the password: it may not be the server it claims to be".to_owned())
        }
    }
}

fn hmac(hash: Hash, key: &[u8], data: &[u8]) -> Vec<u8> {
    match hash {
        Hash::Sha256 => {
            let Ok(mut mac) = hmac::Hmac::<sha2::Sha256>::new_from_slice(key) else {
                return Vec::new();
            };
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
        Hash::Sha512 => {
            let Ok(mut mac) = hmac::Hmac::<sha2::Sha512>::new_from_slice(key) else {
                return Vec::new();
            };
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    }
}

fn digest(hash: Hash, data: &[u8]) -> Vec<u8> {
    match hash {
        Hash::Sha256 => sha2::Sha256::digest(data).to_vec(),
        Hash::Sha512 => sha2::Sha512::digest(data).to_vec(),
    }
}

/// PBKDF2 with HMAC, one block: what SCRAM calls Hi.
fn hi(hash: Hash, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut first = salt.to_vec();
    first.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(hash, password, &first);
    let mut out = u.clone();
    for _ in 1..iterations {
        u = hmac(hash, password, &u);
        for (o, x) in out.iter_mut().zip(&u) {
            *o ^= x;
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The exchange in RFC 7677, section 3.
    #[test]
    fn the_rfc_exchange_is_reproduced() {
        let mut scram = Scram::new(Hash::Sha256, "user", "pencil", "rOprNGfwEbeRWgbNEkqO".to_owned());
        assert_eq!(scram.client_first(), b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first =
            b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        assert_eq!(
            String::from_utf8(scram.client_final(server_first).unwrap()).unwrap(),
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        scram
            .verify(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
            .unwrap();
        assert!(
            scram
                .verify(b"v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
                .unwrap_err()
                .contains("did not prove")
        );
    }

    #[test]
    fn a_challenge_that_is_not_right_is_refused() {
        let fresh = || Scram::new(Hash::Sha512, "a=b,c", "secret", "abc".to_owned());
        assert_eq!(fresh().client_first(), b"n,,n=a=3Db=2Cc,r=abc");
        for (challenge, why) in [
            (&b"r=xyz123,s=c2FsdA==,i=4096"[..], "not ours"),
            (b"r=abc,s=c2FsdA==,i=4096", "not ours"),
            (b"r=abc123,i=4096", "without a salt"),
            (b"r=abc123,s=c2FsdA==,i=1", "iteration count"),
            (b"r=abc123,s=c2FsdA==,i=99999999", "iteration count"),
            (b"m=ext,r=abc123,s=c2FsdA==,i=4096", "extension"),
            (b"e=unknown-user", "unknown-user"),
            (b"\xff", "not text"),
        ] {
            let error = fresh().client_final(challenge).unwrap_err();
            assert!(error.contains(why), "{error}");
        }
        let mut scram = fresh();
        scram.client_final(b"r=abc123,s=c2FsdA==,i=4096").unwrap();
        assert!(
            scram
                .verify(b"e=invalid-proof")
                .unwrap_err()
                .contains("invalid-proof")
        );
        assert!(scram.verify(b"v=").is_err());
        assert!(scram.verify(b"").is_err());
        assert!(Scram::nonce().unwrap() != Scram::nonce().unwrap());
    }
}
