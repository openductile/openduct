// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the places reached over a vendor's web API share: a request sent through the run's HTTP effects (so the
//! guard against this machine's own network holds as for any HTTP step) and its answer read whole, a budget of
//! requests a step may make, names percent-encoded for an address, the times vendors write, and a link a vendor
//! gives (a next page, an upload's address) followed only to the vendor's own host.

use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Map, Value as Json};

use crate::run::{Effects, HttpRequest};

/// How long one request may take.
pub(crate) const REQUEST_TIME: Duration = Duration::from_secs(60);
/// The most requests one step (or one look of a trigger) makes of a place.
pub(crate) const MOST_REQUESTS: usize = 400;

/// The status and headers of an answer, and its body.
#[derive(Debug, Default)]
pub(crate) struct Answer {
    pub status: u16,
    pub headers: Map<String, Json>,
    pub body: Vec<u8>,
}

impl Answer {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(Json::as_str)
    }

    /// The body as JSON (null when it is not).
    pub(crate) fn json(&self) -> Json {
        serde_json::from_slice(&self.body).unwrap_or(Json::Null)
    }

    pub(crate) fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body as text, at most `most` characters: for saying why a vendor refused.
    pub(crate) fn said(&self, most: usize) -> String {
        String::from_utf8_lossy(&self.body).chars().take(most).collect()
    }
}

/// The requests a step has made of a place, counted: one that would make more than [`MOST_REQUESTS`] stops.
#[derive(Debug, Default)]
pub(crate) struct Budget(usize);

impl Budget {
    pub(crate) fn spend(&mut self, vendor: &str) -> Result<(), String> {
        self.0 += 1;
        if self.0 > MOST_REQUESTS {
            return Err(format!(
                "this step would ask {vendor} more than {MOST_REQUESTS} times: list a smaller folder"
            ));
        }
        Ok(())
    }
}

/// A request as the places make it: method, address (query in it, encoded as the vendor signs or reads it),
/// headers and body.
pub(crate) fn request(
    method: &str,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<(String, Vec<u8>)>,
) -> HttpRequest {
    HttpRequest {
        read: matches!(method, "GET" | "HEAD"),
        method: method.to_owned(),
        url,
        headers,
        query: Vec::new(),
        body,
        timeout: REQUEST_TIME,
    }
}

/// Sends `request` through the run's effects; its status, headers and body.
pub(crate) async fn send(
    effects: &dyn Effects,
    request: HttpRequest,
    vendor: &str,
) -> Result<Answer, String> {
    let raw = effects.http_bytes(request).await?;
    let Value::Record(answer) = &raw.answer else {
        return Err(format!("{vendor} gave no answer"));
    };
    let json: Json = serde_json::from_str(&Value::record((**answer).clone()).to_json()).unwrap_or_default();
    let status = json
        .get("status")
        .and_then(Json::as_u64)
        .and_then(|s| u16::try_from(s).ok())
        .ok_or_else(|| format!("{vendor} gave no answer"))?;
    let body = match raw.body {
        Some(b) => b,
        // Effects that answer whole (a stand-in's `{status, body}`): the body as it was given.
        None => match json.get("body") {
            Some(Json::String(s)) => s.clone().into_bytes(),
            Some(Json::Null) | None => Vec::new(),
            Some(other) => other.to_string().into_bytes(),
        },
    };
    Ok(Answer {
        status,
        headers: json
            .get("headers")
            .and_then(Json::as_object)
            .cloned()
            .unwrap_or_default(),
        body,
    })
}

/// `text` percent-encoded: letters, digits, `-._~` and the bytes of `keep` as they are, everything else as %XX.
pub(crate) fn encode(text: &str, keep: &[u8]) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') || keep.contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `pairs` as a query string, each name and value encoded.
pub(crate) fn query(pairs: &[(&str, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k, &[]), encode(v, &[])))
        .collect::<Vec<_>>()
        .join("&")
}

/// A time as RFC 3339 writes it (`2026-11-27T10:00:00Z`), in milliseconds since 1970.
pub(crate) fn rfc3339_ms(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text.trim())
        .ok()
        .map(|t| t.timestamp_millis())
}

/// A time as HTTP headers write it (`Wed, 09 Sep 2009 09:20:02 GMT`), in milliseconds since 1970.
pub(crate) fn rfc1123_ms(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc2822(text.trim())
        .ok()
        .map(|t| t.timestamp_millis())
}

/// `link` (a next page, an upload's address), when it is an https address on `host`: a vendor's answer may not send
/// a request, with its token, anywhere else.
pub(crate) fn on_host(link: &str, host: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(link).map_err(|_| format!("{host} gave a link that is not an address"))?;
    if url.scheme() != "https" || url.host_str() != Some(host) || url.port().is_some() {
        return Err(format!(
            "{host} gave a link to somewhere else, which is not followed"
        ));
    }
    Ok(url.to_string())
}

/// A JSON value as text, for a header: every character beyond ASCII (and DEL) written as `\uXXXX`.
pub(crate) fn ascii_json(value: &Json) -> String {
    let text = value.to_string();
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_ascii() && c != '\u{7f}' {
            out.push(c);
        } else {
            let mut units = [0u16; 2];
            for unit in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{unit:04x}"));
            }
        }
    }
    out
}

/// A text field of a JSON record.
pub(crate) fn text<'j>(json: &'j Json, name: &str) -> Option<&'j str> {
    json.get(name).and_then(Json::as_str)
}

/// A whole number of a JSON record, written as a number or as text (Google writes sizes as text).
pub(crate) fn number(json: &Json, name: &str) -> Option<u64> {
    match json.get(name)? {
        Json::Number(n) => n.as_u64(),
        Json::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// A body of `multipart/form-data`: its parts (name, file name, content type, bytes) and the content type that
/// names its boundary.
pub(crate) fn form_data(parts: &[(&str, Option<&str>, &str, &[u8])]) -> Result<(String, Vec<u8>), String> {
    let boundary = format!("openduct-{}", random_hex(12)?);
    let mut body = Vec::new();
    for (name, file, kind, bytes) in parts {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        let disposition = match file {
            Some(f) => format!(
                "Content-Disposition: form-data; name=\"{name}\"; filename=\"{}\"\r\n",
                f.replace(['"', '\r', '\n', '\\'], "_")
            ),
            None => format!("Content-Disposition: form-data; name=\"{name}\"\r\n"),
        };
        body.extend_from_slice(disposition.as_bytes());
        body.extend_from_slice(format!("Content-Type: {kind}\r\n\r\n").as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    Ok((format!("multipart/form-data; boundary={boundary}"), body))
}

/// `n` random bytes as hex.
pub(crate) fn random_hex(n: usize) -> Result<String, String> {
    use ring::rand::SecureRandom;
    let mut bytes = vec![0u8; n];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| "the system gave no random bytes".to_owned())?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_encoded_and_headers_get_ascii_json() {
        assert_eq!(encode("in/a b&c.csv", b"/"), "in/a%20b%26c.csv");
        assert_eq!(encode("x/y", &[]), "x%2Fy");
        assert_eq!(
            ascii_json(&serde_json::json!({ "path": "/über\u{7f}😀" })),
            r#"{"path":"/\u00fcber\u007f\ud83d\ude00"}"#
        );
        assert_eq!(query(&[("prefix", "in/".to_owned())]), "prefix=in%2F");
    }

    #[test]
    fn a_vendor_s_link_is_followed_only_to_its_host() {
        assert!(
            on_host(
                "https://graph.microsoft.com/v1.0/me/drive/root/delta?token=x",
                "graph.microsoft.com"
            )
            .is_ok()
        );
        for bad in [
            "http://graph.microsoft.com/x",
            "https://graph.microsoft.com.evil.example/x",
            "https://evil.example/x",
            "https://graph.microsoft.com:8443/x",
            "not a link",
        ] {
            assert!(on_host(bad, "graph.microsoft.com").is_err(), "{bad}");
        }
    }

    #[test]
    fn times_and_numbers_are_read_as_vendors_write_them() {
        assert_eq!(rfc3339_ms("2026-11-27T10:00:00Z"), Some(1_795_773_600_000));
        assert_eq!(
            rfc1123_ms("Fri, 27 Nov 2026 10:00:00 GMT"),
            Some(1_795_773_600_000)
        );
        let j = serde_json::json!({ "a": "12", "b": 7, "c": true });
        assert_eq!(
            (number(&j, "a"), number(&j, "b"), number(&j, "c")),
            (Some(12), Some(7), None)
        );
        let (kind, body) = form_data(&[
            ("attributes", None, "application/json", b"{}"),
            ("file", Some("a\"b.csv"), "text/csv", b"x"),
        ])
        .unwrap();
        let text = String::from_utf8(body).unwrap();
        assert!(kind.starts_with("multipart/form-data; boundary=openduct-"));
        assert!(text.contains("name=\"file\"; filename=\"a_b.csv\""), "{text}");
        let mut b = Budget::default();
        assert!((0..MOST_REQUESTS).all(|_| b.spend("x").is_ok()) && b.spend("x").is_err());
    }
}
