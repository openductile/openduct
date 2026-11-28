// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An Azure Blob Storage stand-in: one container, every request's Shared Key checked by this stand-in's own reading of
//! Microsoft's rules (so the step's signer and this one check each other), a SAS-signed address checked too, and List
//! Blobs answered two at a time so its NextMarker is followed.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;

use super::http::{Reply, Request, decode};

pub(crate) const ACCOUNT: &str = "devstoreaccount1";
/// The stand-in's key, as a portal would give it (base64).
pub(crate) const KEY: &str = "c3RhbmRpbi1henVyZS1rZXktZm9yLXRlc3Rz";
pub(crate) const CONTAINER: &str = "orders";
/// A SAS token the stand-in takes (its signature is not worked out: only the key's are).
pub(crate) const SAS: &str = "sv=2021-12-02&ss=b&srt=sco&sp=rwdlac&se=2030-01-01T00%3A00%3A00Z&sig=STANDIN";

#[derive(Debug, Default)]
pub(crate) struct Azure {
    pub blobs: BTreeMap<String, (Vec<u8>, String, u64)>,
    pub refused: usize,
    version: u64,
}

fn hmac(key: &[u8], text: &str) -> String {
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    B64.encode(ring::hmac::sign(&k, text.as_bytes()).as_ref())
}

fn error(status: u16, code: &str) -> Reply {
    Reply::bytes(
        status,
        "application/xml",
        format!("<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>{code}</Code><Message>{code} here</Message></Error>").into_bytes(),
    )
    .with("x-ms-error-code", code)
}

/// The signature Azure would work out for `r`, by this stand-in's reading of "Authorize with Shared Key".
fn expected(r: &Request) -> String {
    let get = |n: &str| r.header(n).unwrap_or_default().to_owned();
    let mut lines = vec![r.method.clone()];
    for h in [
        "content-encoding",
        "content-language",
        "content-length",
        "content-md5",
        "content-type",
        "date",
        "if-modified-since",
        "if-match",
        "if-none-match",
        "if-unmodified-since",
        "range",
    ] {
        let v = get(h);
        lines.push(if (h == "content-length" && v == "0") || h == "date" {
            String::new()
        } else {
            v
        });
    }
    let mut ms: Vec<(String, String)> = r
        .headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-ms-"))
        .cloned()
        .collect();
    ms.sort();
    let mut text = lines.join("\n");
    text.push('\n');
    for (k, v) in ms {
        text.push_str(&format!("{k}:{v}\n"));
    }
    // The address as it came: `/azure` (the stand-in's host) then the endpoint's path.
    text.push_str(&format!("/{ACCOUNT}/azure{}", r.path));
    let mut q: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in &r.query {
        q.entry(k.to_ascii_lowercase()).or_default().push(v.clone());
    }
    for (k, mut vs) in q {
        vs.sort();
        text.push_str(&format!("\n{k}:{}", vs.join(",")));
    }
    hmac(&B64.decode(KEY).unwrap(), &text)
}

fn signed(r: &Request) -> bool {
    if r.q("sig") == Some("STANDIN") {
        return true;
    }
    if let Some(sig) = r.q("sig") {
        // A service SAS this stand-in checks: permissions, expiry, the blob, the version.
        let blob = decode(r.path.trim_start_matches(&format!("/{ACCOUNT}/{CONTAINER}/")));
        let to_sign = [
            r.q("sp").unwrap_or_default(),
            "",
            r.q("se").unwrap_or_default(),
            &format!("/blob/{ACCOUNT}/{CONTAINER}/{blob}"),
            "",
            "",
            r.q("spr").unwrap_or_default(),
            r.q("sv").unwrap_or_default(),
            r.q("sr").unwrap_or_default(),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        .join("\n");
        return hmac(&B64.decode(KEY).unwrap(), &to_sign) == sig;
    }
    let Some(auth) = r.header("authorization") else {
        return false;
    };
    auth == format!("SharedKey {ACCOUNT}:{}", expected(r))
}

impl Azure {
    pub(crate) fn handle(&mut self, r: &Request) -> Reply {
        if !signed(r) {
            self.refused += 1;
            return error(403, "AuthenticationFailed");
        }
        let prefix = format!("/{ACCOUNT}/{CONTAINER}");
        let Some(rest) = r.path.strip_prefix(&prefix) else {
            return error(404, "ContainerNotFound");
        };
        let blob = decode(rest.trim_start_matches('/'));
        let exists = self.blobs.contains_key(&blob);
        match (r.method.as_str(), blob.is_empty()) {
            ("GET", true) if r.q("comp") == Some("list") => self.list(r),
            ("HEAD", false) | ("GET", false) => match self.blobs.get(&blob) {
                None => error(404, "BlobNotFound"),
                Some((bytes, kind, v)) => {
                    let reply = if r.method == "HEAD" {
                        Reply::bytes(200, kind, Vec::new()).with("x-ms-copy-status", "success")
                    } else {
                        Reply::bytes(200, kind, bytes.clone())
                    };
                    reply
                        .with("etag", &format!("\"0x{v}\""))
                        .with("last-modified", "Fri, 27 Nov 2026 10:00:00 GMT")
                }
            },
            ("PUT", false) => {
                if r.header("if-none-match") == Some("*") && exists {
                    return error(409, "BlobAlreadyExists");
                }
                let (bytes, kind) = match r.header("x-ms-copy-source") {
                    Some(source) => {
                        let path = source
                            .split_once(&format!("{prefix}/"))
                            .map(|(_, b)| b)
                            .unwrap_or_default();
                        let path = path.split('?').next().unwrap_or_default();
                        match self.blobs.get(&decode(path)) {
                            Some((b, k, _)) => (b.clone(), k.clone()),
                            None => return error(404, "CannotVerifyCopySource"),
                        }
                    }
                    None if r.header("x-ms-blob-type") == Some("BlockBlob") => (
                        r.body.clone(),
                        r.header("content-type")
                            .unwrap_or("application/octet-stream")
                            .to_owned(),
                    ),
                    None => return error(400, "MissingRequiredHeader"),
                };
                self.version += 1;
                let copied = r.header("x-ms-copy-source").is_some();
                self.blobs.insert(blob, (bytes, kind, self.version));
                if copied {
                    Reply::empty(202).with("x-ms-copy-status", "success")
                } else {
                    Reply::empty(201)
                }
            }
            ("DELETE", false) => {
                if self.blobs.remove(&blob).is_some() {
                    Reply::empty(202)
                } else {
                    error(404, "BlobNotFound")
                }
            }
            _ => error(400, "UnsupportedHttpVerb"),
        }
    }

    fn list(&self, r: &Request) -> Reply {
        let prefix = r.q("prefix").unwrap_or_default();
        let delimiter = r.q("delimiter");
        let mut entries: Vec<(String, bool)> = Vec::new();
        for name in self.blobs.keys().filter(|k| k.starts_with(prefix)) {
            let rest = &name[prefix.len()..];
            match delimiter.and_then(|d| rest.find(d).map(|i| (i, d))) {
                Some((i, d)) => {
                    let p = format!("{prefix}{}{d}", &rest[..i]);
                    if !entries.contains(&(p.clone(), true)) {
                        entries.push((p, true));
                    }
                }
                None => entries.push((name.clone(), false)),
            }
        }
        let start = r.q("marker").and_then(|m| m.parse::<usize>().ok()).unwrap_or(0);
        let page: Vec<&(String, bool)> = entries.iter().skip(start).take(2).collect();
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs>");
        for (name, folder) in &page {
            let n = name.replace('&', "&amp;").replace('<', "&lt;");
            if *folder {
                xml.push_str(&format!("<BlobPrefix><Name>{n}</Name></BlobPrefix>"));
            } else {
                let (bytes, kind, v) = &self.blobs[name.as_str()];
                xml.push_str(&format!(
                    "<Blob><Name>{n}</Name><Properties><Last-Modified>Fri, 27 Nov 2026 10:00:00 GMT</Last-Modified>\
                     <Etag>0x{v}</Etag><Content-Length>{}</Content-Length><Content-Type>{kind}</Content-Type>\
                     </Properties></Blob>",
                    bytes.len()
                ));
            }
        }
        xml.push_str("</Blobs>");
        if start + 2 < entries.len() {
            xml.push_str(&format!("<NextMarker>{}</NextMarker>", start + 2));
        } else {
            xml.push_str("<NextMarker />");
        }
        xml.push_str("</EnumerationResults>");
        Reply::bytes(200, "application/xml", xml.into_bytes())
    }
}
