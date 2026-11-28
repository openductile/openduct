// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! AWS Signature Version 4, as Amazon S3 and the stores that speak its API check it: a request's headers signed, or
//! an address signed for later use (a presigned URL). The `to: s3:` sink and the workflows' storage steps sign with
//! it.

use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use hmac::{KeyInit, Mac};
use sha2::{Digest, Sha256};

/// The SHA-256 of nothing: the payload hash of a request without a body.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Who signs: an access key, its secret, and a session token when the keys are temporary ones.
#[derive(Debug, Clone, Copy)]
pub struct Keys<'a> {
    pub access: &'a str,
    pub secret: &'a str,
    pub token: Option<&'a str>,
}

/// What is signed.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub method: &'a str,
    /// The host as the request names it: with `:port` when it is not the scheme's own.
    pub host: &'a str,
    /// The path as it is sent: each part percent-encoded ([`encode_path`]).
    pub path: &'a str,
    /// The query, not encoded yet.
    pub query: &'a [(String, String)],
    pub region: &'a str,
    pub service: &'a str,
}

/// Bytes as small hexadecimal digits.
pub fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// The SHA-256 of `bytes`, in hexadecimal.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let Ok(mut mac) = hmac::Hmac::<Sha256>::new_from_slice(key) else {
        return Vec::new();
    };
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn encoded(text: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') || (keep_slash && b == b'/') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// Text as a query's name or value is signed: every byte but letters, digits and `-_.~` percent-encoded.
pub fn encode(text: &str) -> String {
    encoded(text, false)
}

/// An object key as a path is signed: encoded as [`encode`] does, its `/` kept.
pub fn encode_path(key: &str) -> String {
    encoded(key, true)
}

/// The query as signed and sent: encoded, ordered by name and then value.
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query.iter().map(|(k, v)| (encode(k), encode(v))).collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn scope(r: &Request<'_>, date: &str) -> String {
    format!("{date}/{}/{}/aws4_request", r.region, r.service)
}

fn signature(r: &Request<'_>, keys: &Keys<'_>, date: &str, stamp: &str, canonical: &str) -> String {
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{}\n{}",
        scope(r, date),
        sha256_hex(canonical.as_bytes())
    );
    let key = hmac(
        &hmac(
            &hmac(&hmac(format!("AWS4{}", keys.secret).as_bytes(), date), r.region),
            r.service,
        ),
        "aws4_request",
    );
    hex(&hmac(&key, &to_sign))
}

/// The headers that sign a request whose body hashes to `payload_sha256` (hex, or `UNSIGNED-PAYLOAD`): `x-amz-date`,
/// `x-amz-content-sha256`, `x-amz-security-token` with temporary keys, and `Authorization`. The `extra` headers are
/// signed too, and sent by the caller as they are; `host` is signed and left for the HTTP client to send.
pub fn headers(
    r: &Request<'_>,
    extra: &[(String, String)],
    payload_sha256: &str,
    keys: &Keys<'_>,
    now: DateTime<Utc>,
) -> Vec<(String, String)> {
    let stamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let mut own = vec![
        ("x-amz-content-sha256".to_owned(), payload_sha256.to_owned()),
        ("x-amz-date".to_owned(), stamp.clone()),
    ];
    if let Some(token) = keys.token {
        own.push(("x-amz-security-token".to_owned(), token.to_owned()));
    }
    let mut signed: Vec<(String, String)> = vec![("host".to_owned(), r.host.to_owned())];
    signed.extend(extra.iter().map(|(k, v)| {
        (
            k.to_ascii_lowercase(),
            v.split_whitespace().collect::<Vec<_>>().join(" "),
        )
    }));
    signed.extend(own.iter().cloned());
    signed.sort();
    let canonical_headers: String = signed.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let names = signed
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{names}\n{payload_sha256}",
        r.method,
        r.path,
        canonical_query(r.query)
    );
    let signature = signature(r, keys, &date, &stamp, &canonical);
    own.push((
        "Authorization".to_owned(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={names}, Signature={signature}",
            keys.access,
            scope(r, &date)
        ),
    ));
    own
}

/// The query of a presigned address for `r`, good for `expires` seconds from `now`: its own query, then the
/// signature's, encoded - what goes after the `?`.
pub fn presign(r: &Request<'_>, keys: &Keys<'_>, now: DateTime<Utc>, expires: u64) -> String {
    let stamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let mut query: Vec<(String, String)> = r.query.to_vec();
    query.push(("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()));
    query.push((
        "X-Amz-Credential".to_owned(),
        format!("{}/{}", keys.access, scope(r, &date)),
    ));
    query.push(("X-Amz-Date".to_owned(), stamp.clone()));
    query.push(("X-Amz-Expires".to_owned(), expires.to_string()));
    if let Some(token) = keys.token {
        query.push(("X-Amz-Security-Token".to_owned(), token.to_owned()));
    }
    query.push(("X-Amz-SignedHeaders".to_owned(), "host".to_owned()));
    let canonical_query = canonical_query(&query);
    let canonical = format!(
        "{}\n{}\n{canonical_query}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
        r.method, r.path, r.host
    );
    let signature = signature(r, keys, &date, &stamp, &canonical);
    format!("{canonical_query}&X-Amz-Signature={signature}")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The examples of Amazon S3's own documentation of Signature Version 4.
    const KEYS: Keys<'static> = Keys {
        // AWS's documented examples, with the access key id of AWS's own signature test suite in place of the S3 pages'
        // one (which has the shape of a real key id, so the Community export's secret check stops on it). A header's
        // signature does not cover the key id, so those stay as documented; a presigned address's does, so its
        // signature below was worked out again, with a calculation that gives the documented one for the documented id.
        access: "AKIDEXAMPLE",
        secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        token: None,
    };

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z")
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_default()
    }

    fn get(path: &str, query: &[(String, String)]) -> Vec<(String, String)> {
        let r = Request {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path,
            query,
            region: "us-east-1",
            service: "s3",
        };
        let extra = if query.is_empty() {
            vec![("Range".to_owned(), "bytes=0-9".to_owned())]
        } else {
            Vec::new()
        };
        headers(&r, &extra, EMPTY_SHA256, &KEYS, at())
    }

    fn signature_of(headers: &[(String, String)]) -> String {
        headers
            .iter()
            .find(|(k, _)| k == "Authorization")
            .and_then(|(_, v)| v.rsplit("Signature=").next())
            .unwrap_or_default()
            .to_owned()
    }

    #[test]
    fn requests_are_signed_as_the_documentation_signs_them() {
        let object = get("/test.txt", &[]);
        assert_eq!(
            signature_of(&object),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        assert!(
            object.iter().any(|(k, v)| k == "Authorization"
                && v.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date")),
            "{object:?}"
        );
        let listing = get(
            "/",
            &[
                ("prefix".to_owned(), "J".to_owned()),
                ("max-keys".to_owned(), "2".to_owned()),
            ],
        );
        assert_eq!(
            signature_of(&listing),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn an_address_is_presigned_as_the_documentation_presigns_it() {
        let r = Request {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            query: &[],
            region: "us-east-1",
            service: "s3",
        };
        let query = presign(&r, &KEYS, at(), 86_400);
        assert!(
            query.ends_with(
                "X-Amz-Signature=6e848bd1eb6999ce153840e19741f7bc2fec9621c2ddebde65c394d3f09db377"
            ),
            "{query}"
        );
        assert!(query.contains("X-Amz-Credential=AKIDEXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request"));
    }

    #[test]
    fn names_and_keys_are_encoded_as_signed() {
        assert_eq!(encode_path("a b/ü+x"), "a%20b/%C3%BC%2Bx");
        assert_eq!(encode("a/b=c"), "a%2Fb%3Dc");
        assert_eq!(
            canonical_query(&[("b".into(), "2".into()), ("a".into(), "x y".into())]),
            "a=x%20y&b=2"
        );
    }
}
