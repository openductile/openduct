// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SCRAM (RFC 5802, RFC 7677): a client proves it knows its password without sending it, and the broker proves it
//! holds the client's credential without storing the password - only a salted, iterated key from which the password
//! cannot be recovered. A name the broker does not know is answered like a known one (a salt and count derived from
//! the name) and refused only at the end, so the exchange does not tell an outsider which names exist.

use std::num::NonZeroU32;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ring::rand::SecureRandom as _;
use ring::{digest, hmac, pbkdf2};
use serde::{Deserialize, Serialize};

/// The fewest iterations a credential may have (RFC 7677 asks for at least 4,096).
pub const MIN_ITERATIONS: u32 = 4096;
/// The iterations a new credential gets.
pub const DEFAULT_ITERATIONS: u32 = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ScramMechanism {
    #[serde(rename = "SCRAM-SHA-256")]
    Sha256,
    #[serde(rename = "SCRAM-SHA-512")]
    Sha512,
}

impl ScramMechanism {
    pub const ALL: [Self; 2] = [Self::Sha256, Self::Sha512];

    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.name() == name)
    }

    fn digest(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    fn hmac(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn pbkdf2(self) -> pbkdf2::Algorithm {
        match self {
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    fn len(self) -> usize {
        self.digest().output_len()
    }
}

/// What the broker keeps for one name and mechanism. The password is not in it and cannot be computed from it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredential {
    #[serde(with = "b64")]
    pub salt: Vec<u8>,
    pub iterations: u32,
    #[serde(with = "b64")]
    pub stored_key: Vec<u8>,
    #[serde(with = "b64")]
    pub server_key: Vec<u8>,
}

impl std::fmt::Debug for ScramCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScramCredential({} iterations)", self.iterations)
    }
}

impl ScramCredential {
    /// A new credential for `password`, with a fresh random salt.
    pub fn new(mechanism: ScramMechanism, password: &str, iterations: u32) -> Result<Self, String> {
        let mut salt = vec![0u8; 24];
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| "no randomness for a salt".to_owned())?;
        Self::derive(mechanism, password, salt, iterations)
    }

    pub fn derive(
        mechanism: ScramMechanism,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<Self, String> {
        if iterations < MIN_ITERATIONS {
            return Err(format!("a credential needs at least {MIN_ITERATIONS} iterations"));
        }
        let salted = salted_password(mechanism, password.as_bytes(), &salt, iterations);
        let client_key = hmac_of(mechanism, &salted, b"Client Key");
        Ok(Self {
            stored_key: digest::digest(mechanism.digest(), &client_key).as_ref().to_vec(),
            server_key: hmac_of(mechanism, &salted, b"Server Key"),
            salt,
            iterations,
        })
    }

    /// Whether `password` is the one this credential was made from (for SASL PLAIN over TLS).
    pub fn matches(&self, mechanism: ScramMechanism, password: &str) -> bool {
        let salted = salted_password(mechanism, password.as_bytes(), &self.salt, self.iterations);
        let client_key = hmac_of(mechanism, &salted, b"Client Key");
        let stored = digest::digest(mechanism.digest(), &client_key);
        same(stored.as_ref(), &self.stored_key)
    }
}

fn salted_password(mechanism: ScramMechanism, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = vec![0u8; mechanism.len()];
    let iterations = NonZeroU32::new(iterations).unwrap_or(NonZeroU32::MIN);
    pbkdf2::derive(mechanism.pbkdf2(), iterations, salt, password, &mut out);
    out
}

fn hmac_of(mechanism: ScramMechanism, key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(mechanism.hmac(), key), data)
        .as_ref()
        .to_vec()
}

/// The client's side: its proof, the signature the broker must answer with, and the final message without the
/// proof.
pub(crate) fn client_proof(
    mechanism: ScramMechanism,
    password: &str,
    salt: &[u8],
    iterations: u32,
    client_first_bare: &str,
    server_first: &str,
    nonce: &str,
) -> (Vec<u8>, Vec<u8>, String) {
    let salted = salted_password(mechanism, password.as_bytes(), salt, iterations);
    let client_key = hmac_of(mechanism, &salted, b"Client Key");
    let stored = digest::digest(mechanism.digest(), &client_key);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let signature = hmac_of(mechanism, stored.as_ref(), auth_message.as_bytes());
    let proof = client_key.iter().zip(&signature).map(|(k, s)| k ^ s).collect();
    let server_key = hmac_of(mechanism, &salted, b"Server Key");
    (
        proof,
        hmac_of(mechanism, &server_key, auth_message.as_bytes()),
        without_proof,
    )
}

/// Equal, in time that does not depend on where they differ.
pub(crate) fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// One SCRAM exchange on one connection, from the broker's side.
#[derive(Debug)]
pub(crate) struct ScramServer {
    mechanism: ScramMechanism,
    state: State,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // one per connection signing in, for a moment
enum State {
    Start,
    Sent {
        user: String,
        credential: Option<ScramCredential>,
        decoy: ScramCredential,
        nonce: String,
        gs2: String,
        auth_message_start: String,
    },
    Done,
}

/// How a step of the exchange ended.
#[derive(Debug)]
pub(crate) enum Step {
    /// Send this to the client and wait for its next message.
    Continue(Vec<u8>),
    /// The client proved who it is: send this final message.
    Authenticated {
        user: String,
        last: Vec<u8>,
    },
    Failed(&'static str),
}

impl ScramServer {
    pub(crate) fn new(mechanism: ScramMechanism) -> Self {
        Self {
            mechanism,
            state: State::Start,
        }
    }

    pub(crate) fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    /// The next client message. `lookup` finds a name's credential; `decoy_key` makes stand-in salts for unknown
    /// names (stable for this broker, so asking twice gives the same answer).
    pub(crate) fn step(
        &mut self,
        message: &[u8],
        lookup: impl Fn(&str) -> Option<ScramCredential>,
        decoy_key: &[u8],
        server_nonce: &str,
    ) -> Step {
        let Ok(text) = std::str::from_utf8(message) else {
            return Step::Failed("not UTF-8");
        };
        match std::mem::replace(&mut self.state, State::Done) {
            State::Start => self.client_first(text, lookup, decoy_key, server_nonce),
            State::Sent {
                user,
                credential,
                decoy,
                nonce,
                gs2,
                auth_message_start,
            } => self.client_final(text, &user, credential, &decoy, &nonce, &gs2, &auth_message_start),
            State::Done => Step::Failed("the exchange is over"),
        }
    }

    fn client_first(
        &mut self,
        text: &str,
        lookup: impl Fn(&str) -> Option<ScramCredential>,
        decoy_key: &[u8],
        server_nonce: &str,
    ) -> Step {
        // gs2-header: "n,," or "y,," (no channel binding), optionally "n,a=authzid,". "p=" (binding) is refused.
        let mut parts = text.splitn(3, ',');
        let (Some(flag), Some(authzid), Some(bare)) = (parts.next(), parts.next(), parts.next()) else {
            return Step::Failed("malformed first message");
        };
        if flag != "n" && flag != "y" {
            return Step::Failed("channel binding is not offered");
        }
        let gs2 = format!("{flag},{authzid},");
        let mut user = None;
        let mut client_nonce = None;
        for attr in bare.split(',') {
            match attr.split_once('=') {
                Some(("n", v)) => user = sasl_name(v),
                Some(("r", v)) if !v.is_empty() => client_nonce = Some(v.to_owned()),
                Some(("m", _)) => return Step::Failed("mandatory extensions are not supported"),
                _ => {}
            }
        }
        let (Some(user), Some(client_nonce)) = (user, client_nonce) else {
            return Step::Failed("the first message needs a name and a nonce");
        };
        if !authzid.is_empty() && authzid != format!("a={user}") {
            return Step::Failed("acting as another name is not allowed");
        }
        let decoy = decoy_for(self.mechanism, decoy_key, &user);
        let credential = lookup(&user);
        let shown = credential.as_ref().unwrap_or(&decoy);
        let nonce = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={nonce},s={},i={}", B64.encode(&shown.salt), shown.iterations);
        let auth_message_start = format!("{bare},{server_first}");
        self.state = State::Sent {
            user,
            credential,
            decoy,
            nonce,
            gs2,
            auth_message_start,
        };
        Step::Continue(server_first.into_bytes())
    }

    #[allow(clippy::too_many_arguments)]
    fn client_final(
        &mut self,
        text: &str,
        user: &str,
        credential: Option<ScramCredential>,
        decoy: &ScramCredential,
        nonce: &str,
        gs2: &str,
        auth_message_start: &str,
    ) -> Step {
        let Some((without_proof, proof)) = text.rsplit_once(",p=") else {
            return Step::Failed("the final message has no proof");
        };
        let mut binding = None;
        let mut their_nonce = None;
        for attr in without_proof.split(',') {
            match attr.split_once('=') {
                Some(("c", v)) => binding = Some(v),
                Some(("r", v)) => their_nonce = Some(v),
                _ => {}
            }
        }
        if binding != Some(B64.encode(gs2.as_bytes()).as_str()) || their_nonce != Some(nonce) {
            return Step::Failed("the final message does not match the exchange");
        }
        let Ok(proof) = B64.decode(proof) else {
            return Step::Failed("the proof is not base64");
        };
        let auth_message = format!("{auth_message_start},{without_proof}");
        let known = credential.is_some();
        let cred = credential.as_ref().unwrap_or(decoy);
        let signature = hmac_of(self.mechanism, &cred.stored_key, auth_message.as_bytes());
        if proof.len() != signature.len() {
            return Step::Failed("wrong name or password");
        }
        let client_key: Vec<u8> = proof.iter().zip(&signature).map(|(p, s)| p ^ s).collect();
        let stored = digest::digest(self.mechanism.digest(), &client_key);
        if !(same(stored.as_ref(), &cred.stored_key) && known) {
            return Step::Failed("wrong name or password");
        }
        let server_signature = hmac_of(self.mechanism, &cred.server_key, auth_message.as_bytes());
        Step::Authenticated {
            user: user.to_owned(),
            last: format!("v={}", B64.encode(server_signature)).into_bytes(),
        }
    }
}

/// A SCRAM name: `=2C` is a comma, `=3D` an equals sign; any other `=` is malformed.
fn sasl_name(v: &str) -> Option<String> {
    let mut out = String::with_capacity(v.len());
    let mut rest = v;
    while let Some(i) = rest.find('=') {
        out.push_str(&rest[..i]);
        match rest.get(i + 1..i + 3) {
            Some("2C") => out.push(','),
            Some("3D") => out.push('='),
            _ => return None,
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    (!out.is_empty()).then_some(out)
}

/// A stand-in credential for a name the broker does not know: the same name always gets the same salt.
fn decoy_for(mechanism: ScramMechanism, key: &[u8], user: &str) -> ScramCredential {
    let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), user.as_bytes());
    let salt: Vec<u8> = tag.as_ref().iter().take(24).copied().collect();
    let n = mechanism.len();
    ScramCredential {
        salt,
        iterations: DEFAULT_ITERATIONS,
        stored_key: vec![0; n],
        server_key: vec![0; n],
    }
}

/// A fresh server nonce.
pub(crate) fn nonce() -> String {
    let mut bytes = [0u8; 24];
    let _ = ring::rand::SystemRandom::new().fill(&mut bytes);
    B64.encode(bytes)
}

mod b64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example of RFC 7677, section 3.
    #[test]
    fn the_rfc_7677_exchange_is_accepted() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let cred = ScramCredential::derive(ScramMechanism::Sha256, "pencil", salt, 4096).unwrap();
        let mut server = ScramServer::new(ScramMechanism::Sha256);
        let lookup = |u: &str| (u == "user").then(|| cred.clone());
        let first = server.step(
            b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO",
            lookup,
            b"k",
            "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
        );
        let Step::Continue(first) = first else {
            panic!("{first:?}")
        };
        assert_eq!(
            String::from_utf8(first).unwrap(),
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let last = server.step(
            b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            lookup,
            b"k",
            "",
        );
        let Step::Authenticated { user, last } = last else {
            panic!("{last:?}")
        };
        assert_eq!(user, "user");
        assert_eq!(
            String::from_utf8(last).unwrap(),
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn a_wrong_proof_and_an_unknown_name_fail_alike() {
        let cred = ScramCredential::new(ScramMechanism::Sha512, "right", DEFAULT_ITERATIONS).unwrap();
        for user in ["known", "stranger"] {
            let mut server = ScramServer::new(ScramMechanism::Sha512);
            let lookup = |u: &str| (u == "known").then(|| cred.clone());
            let Step::Continue(first) =
                server.step(format!("n,,n={user},r=abc").as_bytes(), lookup, b"k", "xyz")
            else {
                panic!("first step")
            };
            let first = String::from_utf8(first).unwrap();
            assert!(first.contains(&format!("i={DEFAULT_ITERATIONS}")), "{first}");
            let proof = B64.encode([7u8; 64]);
            let step = server.step(format!("c=biws,r=abcxyz,p={proof}").as_bytes(), lookup, b"k", "");
            assert!(
                matches!(step, Step::Failed("wrong name or password")),
                "{user}: {step:?}"
            );
        }
    }

    #[test]
    fn a_credential_keeps_no_password_and_checks_one() {
        let cred = ScramCredential::new(ScramMechanism::Sha512, "s3cret!", DEFAULT_ITERATIONS).unwrap();
        let json = serde_json::to_string(&cred).unwrap();
        assert!(!json.contains("s3cret"));
        assert!(cred.matches(ScramMechanism::Sha512, "s3cret!"));
        assert!(!cred.matches(ScramMechanism::Sha512, "s3cret?"));
        assert!(ScramCredential::new(ScramMechanism::Sha256, "x", 100).is_err());
    }

    #[test]
    fn names_are_decoded_and_binding_or_impersonation_refused() {
        assert_eq!(sasl_name("a=2Cb=3Dc").as_deref(), Some("a,b=c"));
        assert_eq!(sasl_name("bad=XX"), None);
        let mut server = ScramServer::new(ScramMechanism::Sha256);
        assert!(matches!(
            server.step(b"p=tls-unique,,n=u,r=1", |_| None, b"k", "2"),
            Step::Failed(_)
        ));
        let mut server = ScramServer::new(ScramMechanism::Sha256);
        assert!(matches!(
            server.step(b"n,a=admin,n=u,r=1", |_| None, b"k", "2"),
            Step::Failed(_)
        ));
    }
}
