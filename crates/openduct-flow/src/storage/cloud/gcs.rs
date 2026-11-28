// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Google Cloud Storage over its JSON API: objects listed (objects.list), got (objects.get, `alt=media`), put (a
//! single-request upload), copied (objects.rewrite, called again while Google says it is not done), deleted
//! (objects.delete), and addresses signed (V4, with the service account's key) - each request with an access token
//! the service account's key file is traded for ([`super::google_key`]).
//!
//! A `gcs` credential holds the service account's `key_file` (sealed, as every secret is), the `bucket` (or the
//! step's `bucket`) and a `prefix` every name is inside. A write that must not replace an object asks
//! `ifGenerationMatch=0`, so one written meanwhile is not replaced either. A folder is an empty object named
//! `folder/`, as Google's console makes one.

use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Value as Json, json};

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::google_key::{self, Key};
use super::web::{self, Answer, Budget};
use crate::apps::Credential;
use crate::files::FileData;
use crate::run::Effects;

const API: &str = "https://storage.googleapis.com/storage/v1";
const UPLOAD: &str = "https://storage.googleapis.com/upload/storage/v1";
const HOST: &str = "storage.googleapis.com";
const SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
/// The most pages a listing reads, and the most calls a copy makes.
const MAX_PAGES: usize = 20;
const MAX_REWRITES: usize = 10;

pub(crate) struct Session<'a> {
    effects: &'a dyn Effects,
    key_file: String,
    token: String,
    bucket: String,
    pub base: Rel,
    budget: Budget,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("gcs::Session")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

/// A bucket's name as Google allows it: 3 to 63 characters (222 with dots) of small letters, digits, `-`, `_`, `.`.
fn bucket_ok(name: &str) -> bool {
    let most = if name.contains('.') { 222 } else { 63 };
    (3..=most).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.'))
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name.ends_with(|c: char| c.is_ascii_alphanumeric())
}

fn refusal(answer: &Answer, what: &str) -> String {
    let json = answer.json();
    let message = json
        .pointer("/error/message")
        .and_then(Json::as_str)
        .map_or_else(|| answer.said(200), str::to_owned);
    let shown: String = message.chars().take(300).collect();
    format!(
        "Google Cloud Storage refused to {what}: {} {shown}",
        answer.status
    )
}

impl<'a> Session<'a> {
    pub(crate) async fn open(
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
        let key_file = field("key_file").ok_or("the gcs credential has no key_file")?;
        // Read here, so a key file that is not one is said before anything is sent.
        Key::parse(&key_file)?;
        let bucket = bucket
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .map(str::to_owned)
            .or_else(|| field("bucket"))
            .ok_or("bucket: which bucket (the step's bucket, or the credential's)")?;
        if !bucket_ok(&bucket) {
            return Err(format!(
                "bucket: {bucket:?} is not a bucket's name (3 to 63 small letters, digits, -, _ and .)"
            ));
        }
        let base =
            Rel::parse(&field("prefix").unwrap_or_default(), false).map_err(|e| format!("prefix: {e}"))?;
        let token = google_key::token(effects, &key_file, SCOPE).await?;
        Ok(Self {
            effects,
            key_file,
            token,
            bucket,
            base,
            budget: Budget::default(),
        })
    }

    fn name_of(rel: &Rel) -> String {
        rel.parts.join("/")
    }

    /// An object's address in the JSON API: its name encoded whole, `/` too.
    fn object(&self, name: &str) -> String {
        format!("{API}/b/{}/o/{}", self.bucket, web::encode(name, &[]))
    }

    async fn send(
        &mut self,
        method: &str,
        url: String,
        body: Option<(String, Vec<u8>)>,
    ) -> Result<Answer, String> {
        self.budget.spend("Google Cloud Storage")?;
        let headers = vec![("authorization".to_owned(), format!("Bearer {}", self.token))];
        web::send(
            self.effects,
            web::request(method, url, headers, body),
            "Google Cloud Storage",
        )
        .await
    }

    async fn metadata(&mut self, name: &str) -> Result<Option<Json>, String> {
        let url = self.object(name);
        let answer = self.send("GET", url, None).await?;
        match answer.status {
            200..=299 => Ok(Some(answer.json())),
            404 => Ok(None),
            _ => Err(refusal(&answer, &format!("look at {name}"))),
        }
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let mut prefix = Self::name_of(dir);
        if !prefix.is_empty() {
            prefix.push('/');
        }
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut q = vec![("prefix", prefix.clone()), ("maxResults", "1000".to_owned())];
            if !recursive {
                q.push(("delimiter", "/".to_owned()));
            }
            if let Some(p) = &page {
                q.push(("pageToken", p.clone()));
            }
            let url = format!("{API}/b/{}/o?{}", self.bucket, web::query(&q));
            let answer = self.send("GET", url, None).await?;
            if !answer.ok() {
                return Err(refusal(&answer, &format!("list {}", self.bucket)));
            }
            let json = answer.json();
            for p in json
                .get("prefixes")
                .and_then(Json::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(rel) = p
                    .as_str()
                    .and_then(|p| Rel::parse(p.trim_end_matches('/'), false).ok())
                {
                    out.push(Listed {
                        rel,
                        folder: true,
                        size: 0,
                        modified: None,
                        version: String::new(),
                    });
                }
            }
            for item in json.get("items").and_then(Json::as_array).into_iter().flatten() {
                let Some(name) = web::text(item, "name") else {
                    continue;
                };
                if name.ends_with('/') {
                    continue;
                }
                let Ok(rel) = Rel::parse(name, false) else {
                    continue;
                };
                out.push(Listed {
                    rel,
                    folder: false,
                    size: web::number(item, "size").unwrap_or(0),
                    modified: web::text(item, "updated").and_then(web::rfc3339_ms),
                    version: web::text(item, "generation")
                        .or_else(|| web::text(item, "etag"))
                        .unwrap_or_default()
                        .to_owned(),
                });
            }
            if out.len() >= most {
                out.truncate(most);
                return Ok((out, true));
            }
            match web::text(&json, "nextPageToken") {
                Some(next) if !next.is_empty() => page = Some(next.to_owned()),
                _ => {
                    out.sort_by(|a, b| a.rel.parts.cmp(&b.rel.parts));
                    return Ok((out, false));
                }
            }
        }
        Ok((out, true))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let name = Self::name_of(rel);
        let url = format!("{}?alt=media", self.object(&name));
        let answer = self.send("GET", url, None).await?;
        if answer.status == 404 {
            return Err(format!("there is nothing at {} in {}", rel.text(), self.bucket));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("read {name}")));
        }
        if answer.body.len() > most {
            return Err(format!(
                "{name} is more than {most} bytes: a run reads files of at most that"
            ));
        }
        let modified = answer.header("last-modified").and_then(web::rfc1123_ms);
        Ok((answer.body, modified))
    }

    async fn upload(
        &mut self,
        name: &str,
        kind: String,
        bytes: Vec<u8>,
        only_new: bool,
    ) -> Result<Answer, String> {
        let mut q = vec![("uploadType", "media".to_owned()), ("name", name.to_owned())];
        if only_new {
            q.push(("ifGenerationMatch", "0".to_owned()));
        }
        let url = format!("{UPLOAD}/b/{}/o?{}", self.bucket, web::query(&q));
        self.send("POST", url, Some((kind, bytes))).await
    }

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let name = Self::name_of(rel);
        let kind = crate::files::clean_type(&file.media_type);
        let answer = self.upload(&name, kind, file.bytes.to_vec(), !overwrite).await?;
        if answer.status == 412 {
            return Err(format!(
                "there is an object at {} already: set overwrite to replace it",
                rel.text()
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("store {name}")));
        }
        Ok(())
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let (source, target) = (Self::name_of(from), Self::name_of(to));
        let mut token: Option<String> = None;
        for _ in 0..MAX_REWRITES {
            let mut q = Vec::new();
            if !overwrite {
                q.push(("ifGenerationMatch", "0".to_owned()));
            }
            if let Some(t) = &token {
                q.push(("rewriteToken", t.clone()));
            }
            let mut url = format!(
                "{API}/b/{0}/o/{1}/rewriteTo/b/{0}/o/{2}",
                self.bucket,
                web::encode(&source, &[]),
                web::encode(&target, &[])
            );
            if !q.is_empty() {
                url = format!("{url}?{}", web::query(&q));
            }
            let answer = self
                .send("POST", url, Some(("application/json".to_owned(), b"{}".to_vec())))
                .await?;
            if answer.status == 404 {
                return Err(format!("there is nothing at {} in {}", from.text(), self.bucket));
            }
            if answer.status == 412 {
                return Err(format!(
                    "there is an object at {} already: set overwrite to replace it",
                    to.text()
                ));
            }
            if !answer.ok() {
                return Err(refusal(&answer, &format!("copy {source}")));
            }
            let json = answer.json();
            if json.get("done").and_then(Json::as_bool).unwrap_or(false) {
                return Ok(json
                    .get("resource")
                    .and_then(|r| web::number(r, "size"))
                    .or_else(|| web::number(&json, "objectSize"))
                    .unwrap_or(0));
            }
            token = web::text(&json, "rewriteToken").map(str::to_owned);
            if token.is_none() {
                return Err("Google Cloud Storage said the copy is not done, and gave no way on".to_owned());
            }
        }
        Err(format!(
            "Google Cloud Storage is still copying {source}: try a smaller object"
        ))
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        self.copy(from, to, overwrite).await?;
        self.delete(from).await.map(|_| ())
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        let name = Self::name_of(rel);
        let target = if self.metadata(&name).await?.is_some() {
            name
        } else {
            let (inside, _) = self.list(rel, true, 2).await?;
            if !inside.is_empty() {
                return Err(format!(
                    "{} is a folder with files in it: only an empty folder is deleted",
                    rel.text()
                ));
            }
            let marker = format!("{name}/");
            if self.metadata(&marker).await?.is_none() {
                return Ok(false);
            }
            marker
        };
        let url = self.object(&target);
        let answer = self.send("DELETE", url, None).await?;
        if !answer.ok() && answer.status != 404 {
            return Err(refusal(&answer, &format!("delete {target}")));
        }
        Ok(answer.status != 404)
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        let marker = format!("{}/", Self::name_of(rel));
        let answer = self
            .upload(&marker, "application/x-directory".to_owned(), Vec::new(), true)
            .await?;
        if answer.status == 412 {
            return Ok(false);
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {marker}")));
        }
        Ok(true)
    }

    /// A V4 signed URL that lets its holder get (or put) the object until `expires` from now (at most 7 days).
    pub(crate) fn presign(&self, rel: &Rel, method: &str, expires: Duration) -> Result<Value, String> {
        let key = Key::parse(&self.key_file)?;
        let name = Self::name_of(rel);
        let path = format!("/{}/{}", self.bucket, web::encode(&name, b"/"));
        let now = chrono::Utc::now();
        let seconds = expires
            .as_secs()
            .clamp(1, super::super::s3::MAX_EXPIRES.as_secs());
        let query = google_key::signed_url(&key, method, HOST, &path, now, seconds)?;
        let until = now.timestamp_millis() + i64::try_from(seconds * 1000).unwrap_or(0);
        Ok(Value::from_json(&json!({
            "url": format!("https://{HOST}{path}?{query}"),
            "method": method,
            "path": name,
            "expires_at": crate::runner::iso(until),
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_google_allows_are_taken() {
        for good in ["orders", "my_bucket-1", "a.b.c"] {
            assert!(bucket_ok(good), "{good}");
        }
        for bad in ["ab", "Orders", "-ab", "ab_", "a b", "evil/x", &"a".repeat(64)] {
            assert!(!bucket_ok(bad), "{bad}");
        }
    }
}
