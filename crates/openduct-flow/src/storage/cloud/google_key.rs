// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Google service account's key file, used as Google documents ("Using OAuth 2.0 for server to server
//! applications", HTTP/REST): a JWT whose header is `{"alg":"RS256","typ":"JWT"}` and whose claims name the account
//! (`iss`), the `scope`, the token address (`aud`) and an hour (`iat`, `exp`), signed with the key (RSASSA-PKCS1-v1_5
//! with SHA-256, on `ring`) and traded at `https://oauth2.googleapis.com/token` for an access token - kept here, in
//! this process, until a minute before it runs out. And Cloud Storage's V4 signing ("V4 signing process with your own
//! program"): a signed URL's canonical request, its string-to-sign, its signature in hex.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::pki_types::pem::PemObject;
use serde_json::{Value as Json, json};

use super::web;
use crate::run::{Effects, HttpRequest};

/// Where a service account's JWT is traded for a token.
pub(crate) const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
/// How long a JWT asks for: an hour, the most Google takes.
const LIFETIME: i64 = 3600;
/// The most tokens kept in this process.
const MOST_KEPT: usize = 256;

/// A service account's key: its address and its private key.
pub(crate) struct Key {
    pub email: String,
    kid: Option<String>,
    pair: RsaKeyPair,
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("email", &self.email)
            .finish_non_exhaustive()
    }
}

impl Key {
    /// The key file as Google gives it (JSON): its `client_email`, `private_key` (PEM) and `private_key_id`.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let file: Json = serde_json::from_str(text.trim())
            .map_err(|_| "key_file: the service account's key file as Google gives it (JSON)".to_owned())?;
        let field = |n: &str| {
            file.get(n)
                .and_then(Json::as_str)
                .map(str::trim)
                .filter(|v| !v.is_empty())
        };
        let email = field("client_email")
            .ok_or("key_file: it names no client_email")?
            .to_owned();
        if email.len() > 320
            || !email.contains('@')
            || email.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err("key_file: its client_email is not an address".to_owned());
        }
        let pem = field("private_key").ok_or("key_file: it holds no private_key")?;
        let der = PrivatePkcs8KeyDer::from_pem_slice(pem.as_bytes())
            .map_err(|_| "key_file: its private_key is not a PEM private key (PKCS #8)".to_owned())?;
        let pair = RsaKeyPair::from_pkcs8(der.secret_pkcs8_der())
            .map_err(|_| "key_file: its private_key is not an RSA key ring takes".to_owned())?;
        let kid = field("private_key_id")
            .filter(|k| k.len() <= 128 && k.bytes().all(|b| b.is_ascii_alphanumeric()))
            .map(str::to_owned);
        Ok(Self { email, kid, pair })
    }

    /// `text` signed with the key: RSASSA-PKCS1-v1_5 with SHA-256.
    pub(crate) fn sign(&self, text: &[u8]) -> Result<Vec<u8>, String> {
        let mut signature = vec![0u8; self.pair.public().modulus_len()];
        self.pair
            .sign(
                &RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                text,
                &mut signature,
            )
            .map_err(|_| "the key could not sign".to_owned())?;
        Ok(signature)
    }

    /// The JWT that asks for a token of `scope`, issued at `now` (seconds since 1970).
    pub(crate) fn assertion(&self, scope: &str, now: i64) -> Result<String, String> {
        let mut header = json!({ "alg": "RS256", "typ": "JWT" });
        if let Some(kid) = &self.kid {
            crate::apps::put(&mut header, "kid", json!(kid));
        }
        let claims = json!({
            "iss": self.email, "scope": scope, "aud": TOKEN_URL, "exp": now + LIFETIME, "iat": now,
        });
        let input = format!(
            "{}.{}",
            B64URL.encode(header.to_string()),
            B64URL.encode(claims.to_string())
        );
        let signature = self.sign(input.as_bytes())?;
        Ok(format!("{input}.{}", B64URL.encode(signature)))
    }
}

fn kept() -> &'static Mutex<HashMap<String, (String, i64)>> {
    static KEPT: std::sync::OnceLock<Mutex<HashMap<String, (String, i64)>>> = std::sync::OnceLock::new();
    KEPT.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What a token is kept under: a hash of the key file and the scope, so the key itself is not.
fn kept_as(key_file: &str, scope: &str) -> String {
    let d = ring::digest::digest(&ring::digest::SHA256, format!("{scope}\n{key_file}").as_bytes());
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// An access token for `scope`, from the key file `key_file`: one kept, or a fresh one.
pub(crate) async fn token(effects: &dyn Effects, key_file: &str, scope: &str) -> Result<String, String> {
    let now = chrono::Utc::now().timestamp();
    let id = kept_as(key_file, scope);
    if let Ok(held) = kept().lock()
        && let Some((token, until)) = held.get(&id)
        && *until > now + 60
    {
        return Ok(token.clone());
    }
    let key = Key::parse(key_file)?;
    let body = web::query(&[
        (
            "grant_type",
            "urn:ietf:params:oauth:grant-type:jwt-bearer".to_owned(),
        ),
        ("assertion", key.assertion(scope, now)?),
    ]);
    let request = HttpRequest {
        // Getting a token changes nothing: a preview may ask for one.
        read: true,
        method: "POST".to_owned(),
        url: TOKEN_URL.to_owned(),
        headers: vec![("accept".to_owned(), "application/json".to_owned())],
        query: Vec::new(),
        body: Some(("application/x-www-form-urlencoded".to_owned(), body.into_bytes())),
        timeout: web::REQUEST_TIME,
    };
    let answer = web::send(effects, request, "Google").await?;
    let got = answer.json();
    let Some(token) = web::text(&got, "access_token").filter(|_| answer.ok()) else {
        let why = web::text(&got, "error_description")
            .or_else(|| web::text(&got, "error"))
            .unwrap_or("no token in its answer");
        return Err(format!(
            "Google gave no token for {} ({}): {why}",
            key.email, answer.status
        ));
    };
    let lasts = i64::try_from(web::number(&got, "expires_in").unwrap_or(3600)).unwrap_or(3600);
    if let Ok(mut held) = kept().lock() {
        if held.len() >= MOST_KEPT {
            held.retain(|_, (_, until)| *until > now);
            if held.len() >= MOST_KEPT {
                held.clear();
            }
        }
        held.insert(id, (token.to_owned(), now + lasts));
    }
    Ok(token.to_owned())
}

/// A V4 signed URL's query for `GET` or `PUT` of `path` (`/bucket/object`, encoded) on `host`, signed at `now` for
/// `seconds`: the canonical query string, and the signature to put after it.
pub(crate) fn signed_url(
    key: &Key,
    method: &str,
    host: &str,
    path: &str,
    now: chrono::DateTime<chrono::Utc>,
    seconds: u64,
) -> Result<String, String> {
    let stamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let scope = format!("{}/auto/storage/goog4_request", now.format("%Y%m%d"));
    let mut params = [
        ("X-Goog-Algorithm", "GOOG4-RSA-SHA256".to_owned()),
        ("X-Goog-Credential", format!("{}/{scope}", key.email)),
        ("X-Goog-Date", stamp.clone()),
        ("X-Goog-Expires", seconds.to_string()),
        ("X-Goog-SignedHeaders", "host".to_owned()),
    ];
    params.sort_by(|a, b| a.0.cmp(b.0));
    let query = web::query(&params);
    let canonical = [
        method,
        path,
        &query,
        &format!("host:{host}\n"),
        "host",
        "UNSIGNED-PAYLOAD",
    ]
    .join("\n");
    let hash = openduct_connect::sigv4::sha256_hex(canonical.as_bytes());
    let to_sign = ["GOOG4-RSA-SHA256", &stamp, &scope, &hash].join("\n");
    let signature: String = key
        .sign(to_sign.as_bytes())?
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(format!("{query}&X-Goog-Signature={signature}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_file_that_is_not_one_is_said() {
        // A PEM block of three zero bytes, not a key: its lines put together here.
        let (begin, end) = ("-----BEGIN PRIVATE ", "-----END PRIVATE ");
        let pem = format!("{begin}KEY-----\nAAAA\n{end}KEY-----\n");
        let not_rsa = serde_json::json!({ "client_email": "a@b.example", "private_key": pem }).to_string();
        for (bad, says) in [
            ("not json", "JSON"),
            ("{}", "client_email"),
            (r#"{"client_email":"a@b.example"}"#, "private_key"),
            (not_rsa.as_str(), "RSA key"),
        ] {
            let e = Key::parse(bad).unwrap_err();
            assert!(e.contains(says), "{bad}: {e}");
        }
    }
}
