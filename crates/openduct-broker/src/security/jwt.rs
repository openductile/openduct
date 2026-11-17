// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OAUTHBEARER (RFC 7628) with JSON Web Tokens: a service signs in with a short-lived token its identity provider
//! issued, and the broker checks it against the provider's published keys (a JWKS file, re-read when it changes so
//! keys rotate without a restart). Only asymmetric signatures are accepted - RS256/384/512, ES256/384, EdDSA - never
//! `none` and never a shared secret, so a broker that checks tokens cannot make one. The token's issuer and audience
//! must be the configured ones, it must not have expired (a small leeway for clocks), and its lifetime is the
//! session's: the client signs in again before it ends or the connection is closed.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use parking_lot::Mutex;
use ring::signature::{self, UnparsedPublicKey};
use serde::Deserialize;

/// How tokens are checked.
#[derive(Debug, Clone)]
pub struct JwtSettings {
    /// The `iss` every token must carry.
    pub issuer: String,
    /// A value the token's `aud` must hold.
    pub audience: String,
    /// The identity provider's public keys, as a JWKS document (`{"keys": [...]}`).
    pub jwks_file: PathBuf,
    /// The claim that names the principal (`sub` unless set).
    pub principal_claim: String,
    /// How far clocks may disagree.
    pub leeway: Duration,
}

#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    kty: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    alg: Option<String>,
    #[serde(default, rename = "use")]
    usage: Option<String>,
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Header {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
    #[serde(default)]
    crit: Option<serde_json::Value>,
}

/// Checks tokens; the keys are re-read when the JWKS file changes.
#[derive(Debug)]
pub struct JwtVerifier {
    settings: JwtSettings,
    keys: Mutex<(Option<SystemTime>, Arc<Vec<Jwk>>)>,
}

impl JwtVerifier {
    pub fn new(settings: JwtSettings) -> Result<Self, String> {
        let v = Self {
            settings,
            keys: Mutex::new((None, Arc::new(Vec::new()))),
        };
        if v.keys()?.is_empty() {
            return Err(format!("{} holds no keys", v.settings.jwks_file.display()));
        }
        Ok(v)
    }

    fn keys(&self) -> Result<Arc<Vec<Jwk>>, String> {
        let path = &self.settings.jwks_file;
        let modified = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let mut held = self.keys.lock();
        if held.0 != Some(modified) {
            let raw = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let jwks: Jwks = serde_json::from_slice(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
            *held = (Some(modified), Arc::new(jwks.keys));
        }
        Ok(Arc::clone(&held.1))
    }

    /// The principal a token names and when the token expires; or why it is refused.
    pub(crate) fn verify(&self, token: &str, now: SystemTime) -> Result<(String, SystemTime), String> {
        let mut parts = token.split('.');
        let (Some(h), Some(c), Some(s), None) = (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("not a signed token".to_owned());
        };
        let header: Header = decode_json(h)?;
        if header.crit.is_some() {
            return Err("critical header extensions are not supported".to_owned());
        }
        let signature = B64URL.decode(s).map_err(|_| "malformed signature".to_owned())?;
        let keys = self.keys()?;
        let candidates: Vec<&Jwk> = keys
            .iter()
            .filter(|k| k.usage.as_deref().is_none_or(|u| u == "sig"))
            .filter(|k| match (&header.kid, &k.kid) {
                (Some(want), Some(have)) => want == have,
                (Some(_), None) => false,
                (None, _) => true,
            })
            .collect();
        let signed = &token[..h.len() + 1 + c.len()];
        let verified = candidates
            .iter()
            .any(|k| check_signature(k, &header.alg, signed.as_bytes(), &signature));
        if !verified {
            return Err("the signature does not verify with any of the provider's keys".to_owned());
        }
        let claims: serde_json::Value = decode_json(c)?;
        self.check_claims(&claims, now)
    }

    fn check_claims(
        &self,
        claims: &serde_json::Value,
        now: SystemTime,
    ) -> Result<(String, SystemTime), String> {
        let at = |name: &str| claims.get(name).and_then(serde_json::Value::as_u64);
        let now_s = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let leeway = self.settings.leeway.as_secs();
        let exp = at("exp").ok_or("the token has no expiry")?;
        if exp + leeway <= now_s {
            return Err("the token has expired".to_owned());
        }
        if at("nbf").is_some_and(|nbf| nbf > now_s + leeway) {
            return Err("the token is not valid yet".to_owned());
        }
        if claims.get("iss").and_then(serde_json::Value::as_str) != Some(self.settings.issuer.as_str()) {
            return Err("the token is from another issuer".to_owned());
        }
        let aud_ok = match claims.get("aud") {
            Some(serde_json::Value::String(a)) => *a == self.settings.audience,
            Some(serde_json::Value::Array(all)) => {
                all.iter().any(|a| a.as_str() == Some(&self.settings.audience))
            }
            _ => false,
        };
        if !aud_ok {
            return Err("the token is for another audience".to_owned());
        }
        let principal = claims
            .get(&self.settings.principal_claim)
            .and_then(serde_json::Value::as_str)
            .filter(|p| !p.is_empty() && p.len() <= 256)
            .ok_or_else(|| format!("the token has no {} claim", self.settings.principal_claim))?;
        Ok((principal.to_owned(), UNIX_EPOCH + Duration::from_secs(exp)))
    }
}

fn decode_json<T: serde::de::DeserializeOwned>(part: &str) -> Result<T, String> {
    let raw = B64URL.decode(part).map_err(|_| "malformed token".to_owned())?;
    serde_json::from_slice(&raw).map_err(|_| "malformed token".to_owned())
}

fn check_signature(key: &Jwk, alg: &str, signed: &[u8], sig: &[u8]) -> bool {
    if key.alg.as_deref().is_some_and(|a| a != alg) {
        return false;
    }
    let b = |v: &Option<String>| v.as_deref().and_then(|s| B64URL.decode(s).ok());
    match (key.kty.as_str(), alg) {
        ("RSA", "RS256" | "RS384" | "RS512") => {
            let (Some(n), Some(e)) = (b(&key.n), b(&key.e)) else {
                return false;
            };
            let params = match alg {
                "RS256" => &signature::RSA_PKCS1_2048_8192_SHA256,
                "RS384" => &signature::RSA_PKCS1_2048_8192_SHA384,
                _ => &signature::RSA_PKCS1_2048_8192_SHA512,
            };
            signature::RsaPublicKeyComponents { n: &n, e: &e }
                .verify(params, signed, sig)
                .is_ok()
        }
        ("EC", "ES256" | "ES384") => {
            let (Some(x), Some(y)) = (b(&key.x), b(&key.y)) else {
                return false;
            };
            let (crv, params): (&str, &'static signature::EcdsaVerificationAlgorithm) = if alg == "ES256" {
                ("P-256", &signature::ECDSA_P256_SHA256_FIXED)
            } else {
                ("P-384", &signature::ECDSA_P384_SHA384_FIXED)
            };
            if key.crv.as_deref() != Some(crv) {
                return false;
            }
            let point: Vec<u8> = std::iter::once(4u8).chain(x).chain(y).collect();
            UnparsedPublicKey::new(params, point).verify(signed, sig).is_ok()
        }
        ("OKP", "EdDSA") if key.crv.as_deref() == Some("Ed25519") => {
            let Some(x) = b(&key.x) else { return false };
            UnparsedPublicKey::new(&signature::ED25519, x)
                .verify(signed, sig)
                .is_ok()
        }
        _ => false,
    }
}

/// The token in an OAUTHBEARER client message: `n,[a=name],` then `\x01auth=Bearer <token>\x01...\x01`.
pub(crate) fn bearer_token(message: &[u8]) -> Result<&str, &'static str> {
    let text = std::str::from_utf8(message).map_err(|_| "not UTF-8")?;
    let rest = text
        .strip_prefix("n,")
        .or_else(|| text.strip_prefix("y,"))
        .ok_or("malformed message")?;
    let (_authzid, kvpairs) = rest.split_once(',').ok_or("malformed message")?;
    kvpairs
        .split('\x01')
        .find_map(|kv| kv.strip_prefix("auth="))
        .and_then(|auth| {
            auth.strip_prefix("Bearer ")
                .or_else(|| auth.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or("no bearer token")
}

#[cfg(test)]
pub(crate) mod tests {
    use ring::rand::SystemRandom;
    use ring::signature::{EcdsaKeyPair, Ed25519KeyPair, KeyPair};

    use super::*;

    /// A token signed with `key`, as a test identity provider would issue it.
    pub(crate) fn token(key: &Ed25519KeyPair, kid: &str, claims: &serde_json::Value) -> String {
        let header = B64URL.encode(serde_json::json!({"alg": "EdDSA", "kid": kid, "typ": "JWT"}).to_string());
        let body = B64URL.encode(claims.to_string());
        let signed = format!("{header}.{body}");
        format!("{signed}.{}", B64URL.encode(key.sign(signed.as_bytes())))
    }

    /// A test identity provider: its key pair and its JWKS file.
    pub(crate) fn provider(dir: &std::path::Path, kid: &str) -> (Ed25519KeyPair, PathBuf) {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        let jwks = serde_json::json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": kid, "use": "sig",
            "x": B64URL.encode(key.public_key().as_ref())}]});
        let path = dir.join("jwks.json");
        std::fs::write(&path, jwks.to_string()).unwrap();
        (key, path)
    }

    fn settings(jwks_file: PathBuf) -> JwtSettings {
        JwtSettings {
            issuer: "https://id.example".to_owned(),
            audience: "openduct".to_owned(),
            jwks_file,
            principal_claim: "sub".to_owned(),
            leeway: Duration::from_secs(30),
        }
    }

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    #[test]
    fn a_token_from_the_provider_names_its_principal_until_it_expires() {
        let dir = tempfile::tempdir().unwrap();
        let (key, jwks) = provider(dir.path(), "k1");
        let v = JwtVerifier::new(settings(jwks)).unwrap();
        let good = serde_json::json!({"iss": "https://id.example", "aud": ["openduct"], "sub": "gateway", "exp": now() + 300});
        let (who, until) = v.verify(&token(&key, "k1", &good), SystemTime::now()).unwrap();
        assert_eq!(who, "gateway");
        assert!(until > SystemTime::now());
        for (bad, why) in [
            (
                serde_json::json!({"iss": "https://id.example", "aud": "openduct", "sub": "x", "exp": now() - 60}),
                "expired",
            ),
            (
                serde_json::json!({"iss": "https://evil.example", "aud": "openduct", "sub": "x", "exp": now() + 60}),
                "issuer",
            ),
            (
                serde_json::json!({"iss": "https://id.example", "aud": "other", "sub": "x", "exp": now() + 60}),
                "audience",
            ),
            (
                serde_json::json!({"iss": "https://id.example", "aud": "openduct", "sub": "x"}),
                "expiry",
            ),
        ] {
            let e = v.verify(&token(&key, "k1", &bad), SystemTime::now()).unwrap_err();
            assert!(e.contains(why), "{why}: {e}");
        }
    }

    #[test]
    fn forged_unsigned_and_foreign_tokens_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (key, jwks) = provider(dir.path(), "k1");
        let v = JwtVerifier::new(settings(jwks)).unwrap();
        let claims = serde_json::json!({"iss": "https://id.example", "aud": "openduct", "sub": "admin", "exp": now() + 60});
        // Another key with the same id.
        let other = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert!(
            v.verify(&token(&other, "k1", &claims), SystemTime::now())
                .is_err()
        );
        // alg none.
        let none = format!(
            "{}.{}.",
            B64URL.encode(r#"{"alg":"none"}"#),
            B64URL.encode(claims.to_string())
        );
        assert!(v.verify(&none, SystemTime::now()).is_err());
        // A changed claim under a valid signature.
        let good = token(&key, "k1", &claims);
        let mut parts: Vec<&str> = good.split('.').collect();
        let tampered = B64URL.encode(claims.to_string().replace("admin", "root"));
        parts[1] = &tampered;
        assert!(v.verify(&parts.join("."), SystemTime::now()).is_err());
    }

    #[test]
    fn an_ecdsa_key_verifies_es256() {
        let dir = tempfile::tempdir().unwrap();
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let key = EcdsaKeyPair::from_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        let public = key.public_key().as_ref();
        let jwks = serde_json::json!({"keys": [{"kty": "EC", "crv": "P-256", "kid": "e1",
            "x": B64URL.encode(&public[1..33]), "y": B64URL.encode(&public[33..65])}]});
        let path = dir.path().join("jwks.json");
        std::fs::write(&path, jwks.to_string()).unwrap();
        let v = JwtVerifier::new(settings(path)).unwrap();
        let header = B64URL.encode(r#"{"alg":"ES256","kid":"e1"}"#);
        let body = B64URL.encode(
            serde_json::json!({"iss": "https://id.example", "aud": "openduct", "sub": "worker", "exp": now() + 60}).to_string(),
        );
        let signed = format!("{header}.{body}");
        let sig = key.sign(&rng, signed.as_bytes()).unwrap();
        let t = format!("{signed}.{}", B64URL.encode(sig.as_ref()));
        assert_eq!(v.verify(&t, SystemTime::now()).unwrap().0, "worker");
    }

    #[test]
    fn the_bearer_token_is_read_from_the_client_message() {
        assert_eq!(
            bearer_token(b"n,,\x01auth=Bearer abc.def.ghi\x01\x01").unwrap(),
            "abc.def.ghi"
        );
        assert_eq!(
            bearer_token(b"n,a=svc,\x01host=x\x01auth=Bearer t\x01\x01").unwrap(),
            "t"
        );
        assert!(bearer_token(b"n,,\x01\x01").is_err());
    }
}
