// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Google Cloud Storage stand-in, and Google's token address: a service account's JWT checked against the stand-in
//! key's public half (its header, claims and RS256 signature) before a token is given; the JSON API's objects in one
//! bucket, listed two at a time; a rewrite done in two calls, so its token is followed; and a V4 signed address
//! checked as Google would.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use ring::signature::{KeyPair, RSA_PKCS1_2048_8192_SHA256, RsaKeyPair, UnparsedPublicKey};
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::pki_types::pem::PemObject;
use serde_json::{Value as Json, json};

use super::http::{Reply, Request, decode, encode_path};

pub(crate) const EMAIL: &str = "flows@standin-project.iam.gserviceaccount.com";
pub(crate) const BUCKET: &str = "orders";
const TOKEN: &str = "test-token-gcs";

/// The stand-in key, PEM.
pub(crate) fn pem() -> String {
    std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/standin-service-account.pem"
    ))
    .unwrap()
}

/// A key file as Google gives one, for the stand-in key.
pub(crate) fn key_file() -> String {
    json!({
        "type": "service_account", "project_id": "standin-project", "private_key_id": "abc123",
        "private_key": pem(), "client_email": EMAIL, "token_uri": "https://oauth2.googleapis.com/token",
    })
    .to_string()
}

fn public() -> Vec<u8> {
    let der = PrivatePkcs8KeyDer::from_pem_slice(pem().as_bytes()).unwrap();
    let pair = RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()).unwrap();
    pair.public_key().as_ref().to_vec()
}

fn verify(text: &[u8], signature: &[u8]) -> bool {
    UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, public())
        .verify(text, signature)
        .is_ok()
}

fn error(status: u16, message: &str) -> Reply {
    Reply::json(
        status,
        &json!({ "error": { "code": status, "message": message, "errors": [] } }),
    )
}

#[derive(Debug, Default)]
pub(crate) struct Gcs {
    /// name -> (bytes, type, generation)
    pub objects: BTreeMap<String, (Vec<u8>, String, u64)>,
    pub tokens_given: usize,
    pub jwt_refused: usize,
    generation: u64,
}

impl Gcs {
    /// `oauth2.googleapis.com`: a JWT for a token.
    pub(crate) fn token(&mut self, r: &Request) -> Reply {
        let form: BTreeMap<String, String> = String::from_utf8_lossy(&r.body)
            .split('&')
            .filter_map(|p| p.split_once('='))
            .map(|(k, v)| (decode(k), decode(v)))
            .collect();
        let ok = (|| {
            if form.get("grant_type")?.as_str() != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
                return None;
            }
            let jwt = form.get("assertion")?;
            let (input, sig) = jwt.rsplit_once('.')?;
            let (h, c) = input.split_once('.')?;
            let header: Json = serde_json::from_slice(&B64URL.decode(h).ok()?).ok()?;
            let claims: Json = serde_json::from_slice(&B64URL.decode(c).ok()?).ok()?;
            let lasts = claims["exp"].as_i64()? - claims["iat"].as_i64()?;
            let good = header["alg"] == "RS256"
                && header["typ"] == "JWT"
                && claims["iss"] == EMAIL
                && claims["aud"] == "https://oauth2.googleapis.com/token"
                && claims["scope"].as_str()?.contains("devstorage")
                && (1..=3600).contains(&lasts)
                && verify(input.as_bytes(), &B64URL.decode(sig).ok()?);
            good.then_some(())
        })();
        if ok.is_none() {
            self.jwt_refused += 1;
            return Reply::json(
                400,
                &json!({ "error": "invalid_grant", "error_description": "Invalid JWT Signature." }),
            );
        }
        self.tokens_given += 1;
        Reply::json(
            200,
            &json!({ "access_token": TOKEN, "expires_in": 3599, "token_type": "Bearer" }),
        )
    }

    fn resource(&self, name: &str) -> Json {
        let (bytes, kind, generation) = &self.objects[name];
        json!({
            "kind": "storage#object", "name": name, "bucket": BUCKET, "size": bytes.len().to_string(),
            "contentType": kind, "generation": generation.to_string(), "updated": "2026-11-27T10:00:00.000Z",
            "etag": format!("E{generation}"),
        })
    }

    /// `storage.googleapis.com`.
    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if r.q("X-Goog-Signature").is_some() {
            return self.signed(r);
        }
        if r.bearer() != Some(TOKEN) {
            return error(401, "Invalid Credentials");
        }
        let api = format!("/storage/v1/b/{BUCKET}/o");
        let upload = format!("/upload/storage/v1/b/{BUCKET}/o");
        if r.path == upload && r.method == "POST" {
            let name = r.q("name").unwrap_or_default().to_owned();
            if r.q("ifGenerationMatch") == Some("0") && self.objects.contains_key(&name) {
                return error(
                    412,
                    "At least one of the pre-conditions you specified did not hold.",
                );
            }
            self.generation += 1;
            let kind = r.header("content-type").unwrap_or_default().to_owned();
            self.objects
                .insert(name.clone(), (r.body.clone(), kind, self.generation));
            return Reply::json(200, &self.resource(&name));
        }
        if r.path == api && r.method == "GET" {
            return self.list(r);
        }
        let Some(rest) = r.path.strip_prefix(&format!("{api}/")) else {
            return error(404, "Not Found");
        };
        if let Some((source, target)) = rest.split_once(&format!("/rewriteTo/b/{BUCKET}/o/")) {
            let (source, target) = (decode(source), decode(target));
            let Some(found) = self.objects.get(&source).cloned() else {
                return error(404, "No such object");
            };
            if r.q("ifGenerationMatch") == Some("0") && self.objects.contains_key(&target) {
                return error(
                    412,
                    "At least one of the pre-conditions you specified did not hold.",
                );
            }
            if r.q("rewriteToken").is_none() {
                return Reply::json(
                    200,
                    &json!({ "kind": "storage#rewriteResponse", "done": false, "rewriteToken": "t1", "totalBytesRewritten": "0" }),
                );
            }
            self.generation += 1;
            self.objects
                .insert(target.clone(), (found.0, found.1, self.generation));
            return Reply::json(
                200,
                &json!({ "kind": "storage#rewriteResponse", "done": true, "resource": self.resource(&target) }),
            );
        }
        let name = decode(rest);
        match r.method.as_str() {
            "GET" if !self.objects.contains_key(&name) => error(404, "No such object"),
            "GET" if r.q("alt") == Some("media") => {
                let (bytes, kind, _) = &self.objects[&name];
                Reply::bytes(200, kind, bytes.clone()).with("last-modified", "Fri, 27 Nov 2026 10:00:00 GMT")
            }
            "GET" => Reply::json(200, &self.resource(&name)),
            "DELETE" => match self.objects.remove(&name) {
                Some(_) => Reply::empty(204),
                None => error(404, "No such object"),
            },
            _ => error(405, "Method not allowed"),
        }
    }

    fn list(&self, r: &Request) -> Reply {
        let prefix = r.q("prefix").unwrap_or_default();
        let delimiter = r.q("delimiter");
        let mut items = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        for name in self.objects.keys().filter(|k| k.starts_with(prefix)) {
            let rest = &name[prefix.len()..];
            match delimiter.and_then(|d| rest.find(d).map(|i| (i, d))) {
                Some((i, d)) => {
                    let p = format!("{prefix}{}{d}", &rest[..i]);
                    if !prefixes.contains(&p) {
                        prefixes.push(p);
                    }
                }
                None => items.push(name.clone()),
            }
        }
        let all: Vec<(String, bool)> = prefixes
            .into_iter()
            .map(|p| (p, true))
            .chain(items.into_iter().map(|i| (i, false)))
            .collect();
        let start = r
            .q("pageToken")
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let page: Vec<&(String, bool)> = all.iter().skip(start).take(2).collect();
        let mut body = json!({
            "kind": "storage#objects",
            "prefixes": page.iter().filter(|(_, f)| *f).map(|(p, _)| p.clone()).collect::<Vec<_>>(),
            "items": page.iter().filter(|(_, f)| !*f).map(|(n, _)| self.resource(n)).collect::<Vec<_>>(),
        });
        if start + 2 < all.len() {
            body["nextPageToken"] = json!((start + 2).to_string());
        }
        Reply::json(200, &body)
    }

    /// A V4 signed address: its canonical request worked out again, its signature checked.
    fn signed(&self, r: &Request) -> Reply {
        let mut params: Vec<(String, String)> = r
            .query
            .iter()
            .filter(|(k, _)| k != "X-Goog-Signature")
            .cloned()
            .collect();
        params.sort();
        let enc = |t: &str| {
            t.bytes()
                .map(|b| {
                    if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                        (b as char).to_string()
                    } else {
                        format!("%{b:02X}")
                    }
                })
                .collect::<String>()
        };
        let query: Vec<String> = params
            .iter()
            .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
            .collect();
        let path = encode_path(&decode(&r.path));
        let canonical = [
            r.method.as_str(),
            &path,
            &query.join("&"),
            "host:storage.googleapis.com\n",
            "host",
            "UNSIGNED-PAYLOAD",
        ]
        .join("\n");
        let hash: String = ring::digest::digest(&ring::digest::SHA256, canonical.as_bytes())
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let q = |n: &str| r.q(n).unwrap_or_default().to_owned();
        let scope = q("X-Goog-Credential")
            .split_once('/')
            .map(|(_, s)| s.to_owned())
            .unwrap_or_default();
        let to_sign = ["GOOG4-RSA-SHA256", &q("X-Goog-Date"), &scope, &hash].join("\n");
        let signature: Vec<u8> = (0..q("X-Goog-Signature").len() / 2)
            .filter_map(|i| u8::from_str_radix(&q("X-Goog-Signature")[i * 2..i * 2 + 2], 16).ok())
            .collect();
        if !verify(to_sign.as_bytes(), &signature) || !q("X-Goog-Credential").starts_with(EMAIL) {
            return error(403, "SignatureDoesNotMatch");
        }
        let name = decode(r.path.trim_start_matches(&format!("/{BUCKET}/")));
        match self.objects.get(&name) {
            Some((bytes, kind, _)) => Reply::bytes(200, kind, bytes.clone()),
            None => error(404, "No such object"),
        }
    }
}
