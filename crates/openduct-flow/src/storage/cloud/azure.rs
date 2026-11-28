// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Azure Blob Storage over its REST API: blobs listed (List Blobs), got (Get Blob), put (Put Blob, a block blob),
//! copied (Copy Blob, from the same account), deleted (Delete Blob), and addresses made with a service SAS - each
//! request signed with the account's Shared Key ([`super::azure_key`]) or carrying the credential's SAS token, and
//! sent through the run's HTTP effects.
//!
//! An `azure_blob` credential holds the storage `account`, its `shared_key` or a `sas_token`, the `container` (or
//! the step's `bucket`), an `endpoint` for a store other than Azure's own (Azurite:
//! `http://127.0.0.1:10000/devstoreaccount1`), and a `prefix` every name is inside. A folder is kept, as S3 keeps one,
//! as an empty blob named `folder/`.

use std::time::Duration;

use openduct_expr::Value;
use serde_json::json;

use super::super::paths::Rel;
use super::super::remote::Listed;
use super::super::s3::{tag, tags};
use super::azure_key;
use super::web::{self, Answer, Budget};
use crate::apps::Credential;
use crate::files::FileData;
use crate::run::Effects;

/// The service version every request asks for.
const VERSION: &str = "2021-12-02";
/// The most pages a listing reads.
const MAX_PAGES: usize = 20;
/// How long a copy Azure finishes on its own is waited for.
const COPY_WAIT: Duration = Duration::from_secs(30);

pub(crate) struct Session<'a> {
    effects: &'a dyn Effects,
    account: String,
    key: Option<Vec<u8>>,
    /// A SAS token's query, when the credential holds one instead of the key.
    sas: Option<String>,
    scheme: String,
    host: String,
    /// The endpoint's own path (an emulator's `/devstoreaccount1`).
    root: String,
    container: String,
    pub base: Rel,
    budget: Budget,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("azure::Session")
            .field("account", &self.account)
            .field("container", &self.container)
            .finish_non_exhaustive()
    }
}

/// A container's name as Azure allows it: 3 to 63 small letters, digits and single hyphens.
fn container_ok(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
}

/// An account's name: 3 to 24 small letters and digits.
fn account_ok(name: &str) -> bool {
    (3..=24).contains(&name.len()) && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn refusal(answer: &Answer, what: &str) -> String {
    let text = String::from_utf8_lossy(&answer.body);
    let code = tag(&text, "Code")
        .or_else(|| answer.header("x-ms-error-code").map(str::to_owned))
        .unwrap_or_default();
    let message = tag(&text, "Message")
        .map(|m| format!(" ({})", m.lines().next().unwrap_or_default()))
        .unwrap_or_default();
    let shown: String = format!("{code}{message}").chars().take(300).collect();
    format!("Azure refused to {what}: {} {shown}", answer.status)
}

impl<'a> Session<'a> {
    pub(crate) fn new(
        effects: &'a dyn Effects,
        c: &Credential,
        container: Option<&str>,
    ) -> Result<Self, String> {
        let field = |name: &str| {
            c.fields
                .get(name)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        let account = field("account").ok_or("the azure_blob credential has no account")?;
        if !account_ok(&account) {
            return Err(format!(
                "account: {account:?} is not a storage account's name (3 to 24 small letters and digits)"
            ));
        }
        let key = field("shared_key").map(|k| azure_key::key(&k)).transpose()?;
        let sas = field("sas_token").map(|t| t.trim_start_matches('?').to_owned());
        if let Some(t) = &sas
            && (!t.contains("sig=") || t.len() > 4096 || t.chars().any(|c| c.is_whitespace() || c == '#'))
        {
            return Err("sas_token: a SAS token as Azure gives it (sv=...&sig=...)".to_owned());
        }
        if key.is_none() && sas.is_none() {
            return Err("the azure_blob credential needs its shared_key or a sas_token".to_owned());
        }
        let endpoint =
            field("endpoint").unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
        let url = reqwest::Url::parse(&endpoint)
            .map_err(|e| format!("endpoint: {endpoint:?} is not an address: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() || url.query().is_some() {
            return Err(format!("endpoint: {endpoint:?} is an http or https address"));
        }
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        let container = container
            .map(str::trim)
            .filter(|b| !b.is_empty())
            .map(str::to_owned)
            .or_else(|| field("container"))
            .ok_or("bucket: which container (the step's bucket, or the credential's container)")?;
        if !container_ok(&container) {
            return Err(format!(
                "container: {container:?} is not a container's name (3 to 63 small letters, digits and hyphens)"
            ));
        }
        let base =
            Rel::parse(&field("prefix").unwrap_or_default(), false).map_err(|e| format!("prefix: {e}"))?;
        Ok(Self {
            effects,
            account,
            key,
            sas,
            scheme: url.scheme().to_owned(),
            host,
            root: url.path().trim_end_matches('/').to_owned(),
            container,
            base,
            budget: Budget::default(),
        })
    }

    fn key_of(rel: &Rel) -> String {
        rel.parts.join("/")
    }

    /// The path of a blob (or of the container, for an empty name), encoded as in the address.
    fn path(&self, blob: &str) -> String {
        if blob.is_empty() {
            format!("{}/{}", self.root, self.container)
        } else {
            format!("{}/{}/{}", self.root, self.container, web::encode(blob, b"/"))
        }
    }

    fn url(&self, path: &str, query: &[(String, String)]) -> String {
        let mut q: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if let Some(sas) = &self.sas {
            q.push(sas.clone());
        }
        if q.is_empty() {
            format!("{}://{}{path}", self.scheme, self.host)
        } else {
            format!("{}://{}{path}?{}", self.scheme, self.host, q.join("&"))
        }
    }

    /// Sends a request: `query` as raw names and values, `headers` beside the date, version and signature.
    async fn send(
        &mut self,
        method: &str,
        blob: &str,
        query: &[(&str, String)],
        mut headers: Vec<(String, String)>,
        body: Option<(String, Vec<u8>)>,
    ) -> Result<Answer, String> {
        self.budget.spend("Azure")?;
        let path = self.path(blob);
        let encoded: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (web::encode(k, &[]), web::encode(v, &[])))
            .collect();
        let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        headers.push(("x-ms-date".to_owned(), date));
        headers.push(("x-ms-version".to_owned(), VERSION.to_owned()));
        if let Some(key) = &self.key {
            let mut signed = headers.clone();
            if let Some((kind, bytes)) = &body {
                signed.push(("content-type".to_owned(), kind.clone()));
                signed.push(("content-length".to_owned(), bytes.len().to_string()));
            }
            let auth = azure_key::authorization(key, method, &signed, &self.account, &path, &encoded);
            headers.push(("authorization".to_owned(), auth));
        }
        let url = self.url(&path, &encoded);
        web::send(self.effects, web::request(method, url, headers, body), "Azure").await
    }

    async fn exists(&mut self, blob: &str) -> Result<Option<Answer>, String> {
        let answer = self.send("HEAD", blob, &[], Vec::new(), None).await?;
        match answer.status {
            200..=299 => Ok(Some(answer)),
            404 => Ok(None),
            _ => Err(refusal(&answer, &format!("look at {blob}"))),
        }
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        let mut prefix = Self::key_of(dir);
        if !prefix.is_empty() {
            prefix.push('/');
        }
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut query = vec![
                ("restype", "container".to_owned()),
                ("comp", "list".to_owned()),
                ("prefix", prefix.clone()),
                ("maxresults", "1000".to_owned()),
            ];
            if !recursive {
                query.push(("delimiter", "/".to_owned()));
            }
            if let Some(m) = &marker {
                query.push(("marker", m.clone()));
            }
            let answer = self.send("GET", "", &query, Vec::new(), None).await?;
            if !answer.ok() {
                return Err(refusal(&answer, &format!("list {}", self.container)));
            }
            let xml = String::from_utf8_lossy(&answer.body).into_owned();
            for folder in tags(&xml, "BlobPrefix") {
                let Some(name) = tag(folder, "Name") else { continue };
                if let Ok(rel) = Rel::parse(name.trim_end_matches('/'), false) {
                    out.push(Listed {
                        rel,
                        folder: true,
                        size: 0,
                        modified: None,
                        version: String::new(),
                    });
                }
            }
            for blob in tags(&xml, "Blob") {
                let Some(name) = tag(blob, "Name") else { continue };
                // A folder's own marker is the folder, not a file in it.
                if name.ends_with('/') {
                    continue;
                }
                let Ok(rel) = Rel::parse(&name, false) else {
                    continue;
                };
                out.push(Listed {
                    rel,
                    folder: false,
                    size: tag(blob, "Content-Length")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0),
                    modified: tag(blob, "Last-Modified").and_then(|t| web::rfc1123_ms(&t)),
                    version: tag(blob, "Etag").unwrap_or_default().trim_matches('"').to_owned(),
                });
            }
            if out.len() >= most {
                out.truncate(most);
                return Ok((out, true));
            }
            match tag(&xml, "NextMarker").filter(|m| !m.is_empty()) {
                Some(next) => marker = Some(next),
                None => {
                    out.sort_by(|a, b| a.rel.parts.cmp(&b.rel.parts));
                    return Ok((out, false));
                }
            }
        }
        Ok((out, true))
    }

    pub(crate) async fn read(&mut self, rel: &Rel, most: usize) -> Result<(Vec<u8>, Option<i64>), String> {
        let blob = Self::key_of(rel);
        let answer = self.send("GET", &blob, &[], Vec::new(), None).await?;
        if answer.status == 404 {
            return Err(format!(
                "there is nothing at {} in {}",
                rel.text(),
                self.container
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("read {blob}")));
        }
        if answer.body.len() > most {
            return Err(format!(
                "{blob} is more than {most} bytes: a run reads files of at most that"
            ));
        }
        let modified = answer.header("last-modified").and_then(web::rfc1123_ms);
        Ok((answer.body, modified))
    }

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        let blob = Self::key_of(rel);
        let mut headers = vec![("x-ms-blob-type".to_owned(), "BlockBlob".to_owned())];
        if !overwrite {
            headers.push(("if-none-match".to_owned(), "*".to_owned()));
        }
        let body = Some((crate::files::clean_type(&file.media_type), file.bytes.to_vec()));
        let answer = self.send("PUT", &blob, &[], headers, body).await?;
        if matches!(answer.status, 409 | 412) && !overwrite {
            return Err(format!(
                "there is a blob at {} already: set overwrite to replace it",
                rel.text()
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("store {blob}")));
        }
        Ok(())
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        let (source, target) = (Self::key_of(from), Self::key_of(to));
        if self.exists(&source).await?.is_none() {
            return Err(format!(
                "there is nothing at {} in {}",
                from.text(),
                self.container
            ));
        }
        let source_url = self.url(&self.path(&source), &[]);
        let mut headers = vec![("x-ms-copy-source".to_owned(), source_url)];
        if !overwrite {
            headers.push(("if-none-match".to_owned(), "*".to_owned()));
        }
        let empty = Some(("application/octet-stream".to_owned(), Vec::new()));
        let answer = self.send("PUT", &target, &[], headers, empty).await?;
        if matches!(answer.status, 409 | 412) && !overwrite {
            return Err(format!(
                "there is a blob at {} already: set overwrite to replace it",
                to.text()
            ));
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("copy {source}")));
        }
        // A copy within an account is mostly done at once; one Azure makes later is waited for, a while.
        let mut status = answer.header("x-ms-copy-status").unwrap_or("success").to_owned();
        let waited = std::time::Instant::now();
        while status == "pending" {
            if waited.elapsed() > COPY_WAIT {
                return Err(format!(
                    "Azure is still copying {source} to {target}: it finishes on its own; look again later"
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            let props = self.exists(&target).await?.ok_or("the copy is gone")?;
            status = props.header("x-ms-copy-status").unwrap_or("success").to_owned();
        }
        if status != "success" {
            return Err(format!("Azure could not copy {source}: the copy {status}"));
        }
        Ok(self
            .exists(&target)
            .await?
            .and_then(|a| a.header("content-length").and_then(|l| l.parse().ok()))
            .unwrap_or(0))
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        self.copy(from, to, overwrite).await?;
        self.delete(from).await.map(|_| ())
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        let blob = Self::key_of(rel);
        let target = if self.exists(&blob).await?.is_some() {
            blob
        } else {
            // A folder: only its own marker, when nothing else is in it.
            let (inside, _) = self.list(rel, true, 2).await?;
            let marker = format!("{blob}/");
            if !inside.is_empty() {
                return Err(format!(
                    "{} is a folder with files in it: only an empty folder is deleted",
                    rel.text()
                ));
            }
            if self.exists(&marker).await?.is_none() {
                return Ok(false);
            }
            marker
        };
        let answer = self.send("DELETE", &target, &[], Vec::new(), None).await?;
        if !answer.ok() && answer.status != 404 {
            return Err(refusal(&answer, &format!("delete {target}")));
        }
        Ok(answer.status != 404)
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        let marker = format!("{}/", Self::key_of(rel));
        let headers = vec![
            ("x-ms-blob-type".to_owned(), "BlockBlob".to_owned()),
            ("if-none-match".to_owned(), "*".to_owned()),
        ];
        let body = Some(("application/x-directory".to_owned(), Vec::new()));
        let answer = self.send("PUT", &marker, &[], headers, body).await?;
        if matches!(answer.status, 409 | 412) {
            return Ok(false);
        }
        if !answer.ok() {
            return Err(refusal(&answer, &format!("make the folder {marker}")));
        }
        Ok(true)
    }

    /// An address that lets its holder get (or put) the blob without the credential, until `expires` from now: a
    /// service SAS, which needs the account's key.
    pub(crate) fn presign(&self, rel: &Rel, method: &str, expires: Duration) -> Result<Value, String> {
        let key = self
            .key
            .as_ref()
            .ok_or("presign: an address of its own needs the account's shared_key, not a SAS token")?;
        let blob = Self::key_of(rel);
        let seconds = expires
            .as_secs()
            .clamp(1, super::super::s3::MAX_EXPIRES.as_secs());
        let until = chrono::Utc::now() + chrono::Duration::seconds(i64::try_from(seconds).unwrap_or(3600));
        let expiry = until.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let query = azure_key::service_sas(
            key,
            &azure_key::Sas {
                permissions: if method == "PUT" { "cw" } else { "r" },
                expiry: &expiry,
                account: &self.account,
                container: &self.container,
                blob: &blob,
                protocol: if self.scheme == "https" {
                    "https"
                } else {
                    "https,http"
                },
                version: VERSION,
            },
        );
        let url = format!("{}://{}{}?{query}", self.scheme, self.host, self.path(&blob));
        let mut out = json!({
            "url": url, "method": method, "path": blob, "expires_at": crate::runner::iso(until.timestamp_millis()),
        });
        if method == "PUT" {
            // Azure takes a blob put to such an address only with its type said.
            crate::apps::put(&mut out, "headers", json!({ "x-ms-blob-type": "BlockBlob" }));
        }
        Ok(Value::from_json(&out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_azure_allows_are_taken() {
        assert!(container_ok("orders") && container_ok("a-b-1"));
        for bad in ["ab", "Orders", "a--b", "-ab", "ab-", "a_b", &"a".repeat(64)] {
            assert!(!container_ok(bad), "{bad}");
        }
        assert!(account_ok("devstoreaccount1") && !account_ok("My-Account") && !account_ok("ab"));
    }
}
