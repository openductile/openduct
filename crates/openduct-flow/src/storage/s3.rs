// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! S3, and the stores that speak its API (MinIO, Cloudflare R2, Wasabi, Backblaze B2, Ceph): objects listed, got,
//! put, copied and deleted, and addresses presigned - each request signed with Signature Version 4
//! ([`openduct_connect::sigv4`], which the S3 sink signs with too) and sent through the run's HTTP effects, so the guard
//! against this machine's own network holds as for any HTTP step.
//!
//! An `s3` credential holds `access_key`, `secret_key` (and a `session_token` for temporary keys), `region`
//! (us-east-1), `endpoint` for a store other than AWS (`https://<account>.r2.cloudflarestorage.com`), `path_style`
//! (true by default with an endpoint), and optionally the `bucket` and a `prefix` every key is inside.

use std::time::Duration;

use openduct_connect::sigv4;
use openduct_expr::Value;
use serde_json::json;

use super::paths::Rel;
use super::remote::Listed;
use crate::apps::Credential;
use crate::files::FileData;
use crate::run::{Effects, HttpRequest};

/// How long one request may take.
const REQUEST_TIME: Duration = Duration::from_secs(60);
/// The longest a presigned address works: a week, as S3 allows.
pub(crate) const MAX_EXPIRES: Duration = Duration::from_secs(7 * 86_400);
/// The most pages a listing reads.
const MAX_PAGES: usize = 20;

pub(crate) struct Session<'a> {
    effects: &'a dyn Effects,
    access: String,
    secret: String,
    token: Option<String>,
    region: String,
    scheme: String,
    host: String,
    path_style: bool,
    bucket: String,
    /// The prefix every key is inside.
    pub base: Rel,
}

/// A bucket's name as S3 allows it: 3 to 63 small letters, digits, `.` and `-` - so it is safe in a host name.
fn bucket_ok(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        && !name.starts_with(['.', '-'])
        && !name.ends_with(['.', '-'])
}

/// The text of each `<tag>` in `xml`, its entities read.
pub(super) fn tags<'x>(xml: &'x str, tag: &str) -> Vec<&'x str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = rest.get(start + open.len()..).unwrap_or_default();
        let Some(end) = after.find(&close) else { break };
        out.push(after.get(..end).unwrap_or_default());
        rest = after.get(end + close.len()..).unwrap_or_default();
    }
    out
}

pub(super) fn tag(xml: &str, name: &str) -> Option<String> {
    tags(xml, name).first().map(|t| unescape(t))
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#13;", "\r")
        .replace("&#10;", "\n")
        .replace("&amp;", "&")
}

fn iso_ms(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text.trim())
        .ok()
        .map(|t| t.timestamp_millis())
}

/// The status and headers of an answer, and its body.
struct Answer {
    status: u16,
    headers: serde_json::Map<String, serde_json::Value>,
    body: Vec<u8>,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(serde_json::Value::as_str)
    }

    /// Why S3 refused: its code and message.
    fn refusal(&self, what: &str) -> String {
        let text = String::from_utf8_lossy(&self.body);
        let code = tag(&text, "Code").unwrap_or_default();
        let message = tag(&text, "Message")
            .map(|m| format!(" ({m})"))
            .unwrap_or_default();
        let shown: String = format!("{code}{message}").chars().take(300).collect();
        format!("S3 refused to {what}: {} {shown}", self.status)
    }
}

impl<'a> Session<'a> {
    pub(crate) fn new(
        effects: &'a dyn Effects,
        c: &Credential,
        bucket: Option<&str>,
    ) -> Result<Self, String> {
        let field = |name: &str| {
            c.fields
                .get(name)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        let access = field("access_key").ok_or("the s3 credential has no access_key")?;
        let secret = field("secret_key").ok_or("the s3 credential has no secret_key")?;
        let region = field("region").unwrap_or_else(|| "us-east-1".to_owned());
        if !region.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') || region.len() > 40 {
            return Err(format!("region: {region:?} is not a region, as us-east-1"));
        }
        let custom = field("endpoint");
        let endpoint = custom
            .clone()
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let url = reqwest::Url::parse(&endpoint)
            .map_err(|e| format!("endpoint: {endpoint:?} is not an address: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(format!("endpoint: {endpoint:?} is an http or https address"));
        }
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        let path_style = match field("path_style").as_deref() {
            None => custom.is_some(),
            Some("true" | "yes") => true,
            Some("false" | "no") => false,
            Some(other) => return Err(format!("path_style: true or false, not {other:?}")),
        };
        let bucket = bucket
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .map(str::to_owned)
            .or_else(|| field("bucket"))
            .ok_or("bucket: which bucket (the step's bucket, or the credential's)")?;
        if !bucket_ok(&bucket) {
            return Err(format!(
                "bucket: {bucket:?} is not a bucket's name (3 to 63 small letters, digits, . and -)"
            ));
        }
        let base =
            Rel::parse(&field("prefix").unwrap_or_default(), false).map_err(|e| format!("prefix: {e}"))?;
        Ok(Self {
            effects,
            access,
            secret,
            token: field("session_token"),
            region,
            scheme: url.scheme().to_owned(),
            host,
            path_style,
            bucket,
            base,
        })
    }

    fn keys(&self) -> sigv4::Keys<'_> {
        sigv4::Keys {
            access: &self.access,
            secret: &self.secret,
            token: self.token.as_deref(),
        }
    }

    /// The host a request goes to, and its path (the key encoded).
    fn place(&self, key: &str) -> (String, String) {
        let encoded = sigv4::encode_path(key);
        if self.path_style {
            (self.host.clone(), format!("/{}/{encoded}", self.bucket))
        } else {
            (format!("{}.{}", self.bucket, self.host), format!("/{encoded}"))
        }
    }

    async fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(String, String)],
        mut headers: Vec<(String, String)>,
        body: Option<(String, Vec<u8>)>,
    ) -> Result<Answer, String> {
        let (host, path) = self.place(key);
        let payload = sigv4::sha256_hex(body.as_ref().map_or(&[][..], |(_, b)| b.as_slice()));
        if let Some((kind, _)) = &body {
            headers.push(("content-type".to_owned(), kind.clone()));
        }
        let request = sigv4::Request {
            method,
            host: &host,
            path: &path,
            query,
            region: &self.region,
            service: "s3",
        };
        let mut signed = sigv4::headers(&request, &headers, &payload, &self.keys(), chrono::Utc::now());
        headers.retain(|(k, _)| k != "content-type");
        signed.extend(headers);
        let canonical = sigv4::canonical_query(query);
        let url = if canonical.is_empty() {
            format!("{}://{host}{path}", self.scheme)
        } else {
            format!("{}://{host}{path}?{canonical}", self.scheme)
        };
        let raw = self
            .effects
            .http_bytes(HttpRequest {
                read: matches!(method, "GET" | "HEAD"),
                method: method.to_owned(),
                url,
                headers: signed,
                query: Vec::new(),
                body,
                timeout: REQUEST_TIME,
            })
            .await?;
        let Value::Record(answer) = &raw.answer else {
            return Err("S3 gave no answer".to_owned());
        };
        let json: serde_json::Value =
            serde_json::from_str(&Value::record((**answer).clone()).to_json()).unwrap_or_default();
        let status = json
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .and_then(|s| u16::try_from(s).ok())
            .ok_or("S3 gave no answer")?;
        Ok(Answer {
            status,
            headers: json
                .get("headers")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default(),
            body: raw.body.unwrap_or_default(),
        })
    }

    fn key(rel: &Rel) -> String {
        rel.parts.join("/")
    }

    async fn exists(&self, key: &str) -> Result<bool, String> {
        let answer = self.send("HEAD", key, &[], Vec::new(), None).await?;
        match answer.status {
            200..=299 => Ok(true),
            404 => Ok(false),
            _ => Err(answer.refusal(&format!("look at {key}"))),
        }
    }

    pub(crate) async fn list(
        &self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let mut prefix = Self::key(dir);
        if !prefix.is_empty() {
            prefix.push('/');
        }
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut query = vec![
                ("list-type".to_owned(), "2".to_owned()),
                ("prefix".to_owned(), prefix.clone()),
                ("max-keys".to_owned(), "1000".to_owned()),
            ];
            if !recursive {
                query.push(("delimiter".to_owned(), "/".to_owned()));
            }
            if let Some(t) = &token {
                query.push(("continuation-token".to_owned(), t.clone()));
            }
            let answer = self.send("GET", "", &query, Vec::new(), None).await?;
            if answer.status >= 300 {
                return Err(answer.refusal(&format!("list {}", self.bucket)));
            }
            let xml = String::from_utf8_lossy(&answer.body).into_owned();
            for folder in tags(&xml, "CommonPrefixes") {
                let Some(p) = tag(folder, "Prefix") else { continue };
                if let Ok(rel) = Rel::parse(p.trim_end_matches('/'), false) {
                    out.push(Listed {
                        rel,
                        folder: true,
                        size: 0,
                        modified: None,
                        version: String::new(),
                    });
                }
            }
            for object in tags(&xml, "Contents") {
                let Some(key) = tag(object, "Key") else { continue };
                // A folder's own marker (an empty object named `folder/`) is the folder, not a file in it.
                if key.ends_with('/') {
                    continue;
                }
                let Ok(rel) = Rel::parse(&key, false) else {
                    continue;
                };
                out.push(Listed {
                    rel,
                    folder: false,
                    size: tag(object, "Size").and_then(|s| s.parse().ok()).unwrap_or(0),
                    modified: tag(object, "LastModified").and_then(|t| iso_ms(&t)),
                    version: tag(object, "ETag")
                        .unwrap_or_default()
                        .trim_matches('"')
                        .to_owned(),
                });
            }
            if out.len() >= most {
                out.truncate(most);
                return Ok((out, true));
            }
            match (
                tag(&xml, "IsTruncated").as_deref(),
                tag(&xml, "NextContinuationToken"),
            ) {
                (Some("true"), Some(next)) => token = Some(next),
                _ => return Ok((out, false)),
            }
        }
        Ok((out, true))
    }

    pub(crate) async fn read(&self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let key = Self::key(rel);
        let answer = self.send("GET", &key, &[], Vec::new(), None).await?;
        if answer.status == 404 {
            return Err(format!("there is nothing at {key} in {}", self.bucket));
        }
        if answer.status >= 300 {
            return Err(answer.refusal(&format!("read {key}")));
        }
        if answer.body.len() > most {
            return Err(format!(
                "{key} is more than {most} bytes: a run reads files of at most that"
            ));
        }
        let modified = answer
            .header("last-modified")
            .and_then(|t| chrono::DateTime::parse_from_rfc2822(t).ok())
            .map(|t| t.timestamp_millis());
        Ok((answer.body, modified))
    }

    pub(crate) async fn write(&self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let key = Self::key(rel);
        let mut headers = Vec::new();
        if !overwrite {
            if self.exists(&key).await? {
                return Err(format!(
                    "there is an object at {key} already: set overwrite to replace it"
                ));
            }
            // Where the store keeps to it, one written meanwhile is not replaced either.
            headers.push(("if-none-match".to_owned(), "*".to_owned()));
        }
        let body = Some((crate::files::clean_type(&file.media_type), file.bytes.to_vec()));
        let answer = self.send("PUT", &key, &[], headers, body).await?;
        if answer.status == 412 {
            return Err(format!(
                "there is an object at {key} already: set overwrite to replace it"
            ));
        }
        if answer.status >= 300 {
            return Err(answer.refusal(&format!("store {key}")));
        }
        Ok(())
    }

    pub(crate) async fn copy(&self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let (source, target) = (Self::key(from), Self::key(to));
        if !overwrite && self.exists(&target).await? {
            return Err(format!(
                "there is an object at {target} already: set overwrite to replace it"
            ));
        }
        let copy_source = format!("/{}/{}", self.bucket, sigv4::encode_path(&source));
        let answer = self
            .send(
                "PUT",
                &target,
                &[],
                vec![("x-amz-copy-source".to_owned(), copy_source)],
                None,
            )
            .await?;
        let text = String::from_utf8_lossy(&answer.body).into_owned();
        // A copy can fail after S3 has answered 200: the error is then in its body.
        if answer.status >= 300 || text.contains("<Error>") {
            return Err(if answer.status == 404 {
                format!("there is nothing at {source} in {}", self.bucket)
            } else {
                answer.refusal(&format!("copy {source}"))
            });
        }
        let size = self.size_of(&target).await.unwrap_or(0);
        Ok(size)
    }

    async fn size_of(&self, key: &str) -> Result<u64, String> {
        let answer = self.send("HEAD", key, &[], Vec::new(), None).await?;
        Ok(answer
            .header("content-length")
            .and_then(|l| l.parse().ok())
            .unwrap_or(0))
    }

    pub(crate) async fn delete(&self, rel: &Rel) -> Result<bool, String> {
        let key = Self::key(rel);
        if !self.exists(&key).await? {
            return Ok(false);
        }
        let answer = self.send("DELETE", &key, &[], Vec::new(), None).await?;
        if answer.status >= 300 && answer.status != 404 {
            return Err(answer.refusal(&format!("delete {key}")));
        }
        Ok(true)
    }

    /// A folder, as S3 keeps one: an empty object named `folder/`.
    pub(crate) async fn make_folder(&self, rel: &Rel) -> Result<bool, String> {
        let key = format!("{}/", Self::key(rel));
        if self.exists(&key).await? {
            return Ok(false);
        }
        let answer = self
            .send(
                "PUT",
                &key,
                &[],
                Vec::new(),
                Some(("application/x-directory".to_owned(), Vec::new())),
            )
            .await?;
        if answer.status >= 300 {
            return Err(answer.refusal(&format!("make the folder {key}")));
        }
        Ok(true)
    }

    /// An address that lets its holder get (or put) the object without the credential, until `expires` from now.
    pub(crate) fn presign(&self, rel: &Rel, method: &str, expires: Duration) -> Result<Value, String> {
        let key = Self::key(rel);
        let (host, path) = self.place(&key);
        let request = sigv4::Request {
            method,
            host: &host,
            path: &path,
            query: &[],
            region: &self.region,
            service: "s3",
        };
        let now = chrono::Utc::now();
        let seconds = expires.as_secs().clamp(1, MAX_EXPIRES.as_secs());
        let query = sigv4::presign(&request, &self.keys(), now, seconds);
        let until = now.timestamp_millis() + i64::try_from(seconds * 1000).unwrap_or(0);
        Ok(Value::from_json(&json!({
            "url": format!("{}://{host}{path}?{query}", self.scheme),
            "method": method,
            "path": key,
            "expires_at": crate::runner::iso(until),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listing_is_read_from_its_xml() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>in/a&amp;b.csv</Key>\
                   <LastModified>2026-11-27T10:00:00.000Z</LastModified><ETag>&quot;abc&quot;</ETag><Size>12</Size>\
                   </Contents><CommonPrefixes><Prefix>in/sub/</Prefix></CommonPrefixes>\
                   <NextContinuationToken>t+1</NextContinuationToken></ListBucketResult>";
        assert_eq!(tag(xml, "Key").as_deref(), Some("in/a&b.csv"));
        assert_eq!(tag(xml, "ETag").as_deref(), Some("\"abc\""));
        assert_eq!(tags(xml, "Contents").len(), 1);
        assert_eq!(tag(xml, "NextContinuationToken").as_deref(), Some("t+1"));
        assert_eq!(iso_ms("2026-11-27T10:00:00.000Z"), Some(1_795_773_600_000));
        assert!(tag("<a>", "a").is_none());
    }

    #[test]
    fn only_a_bucket_name_s3_allows_is_taken() {
        for good in ["orders", "my.bucket-1", "abc"] {
            assert!(bucket_ok(good), "{good}");
        }
        for bad in [
            "ab",
            "Orders",
            "a_b",
            "-ab",
            "ab.",
            "evil.com/x",
            "a b",
            &"a".repeat(64),
        ] {
            assert!(!bucket_ok(bad), "{bad}");
        }
    }
}
