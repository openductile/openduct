// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The few S3 calls tiered storage makes - put, get and delete one object, and create a bucket - signed with AWS
//! Signature Version 4. AWS itself (virtual-hosted addresses) or any S3-compatible store at an address of its own
//! (path-style addresses: MinIO, Ceph, others).

use std::fmt::Write as _;
use std::time::Duration;

use chrono::Utc;
use hmac::{KeyInit, Mac};
use reqwest::{Client, Method, Url};
use sha2::{Digest, Sha256};

use super::credentials::{Credentials, Keys, percent};

/// Where tiered segments are kept.
#[derive(Debug, Clone)]
pub struct S3Settings {
    pub bucket: String,
    pub region: String,
    /// An S3-compatible store's address (`http://minio:9000`); none for AWS itself.
    pub endpoint: Option<String>,
    /// An access key and its secret; none finds credentials as the AWS tools do (see `credentials`).
    pub keys: Option<(String, String)>,
}

/// An S3 bucket.
#[derive(Debug)]
pub struct S3 {
    client: Client,
    credentials: Credentials,
    bucket: String,
    region: String,
    base: Url,
    path_style: bool,
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let Ok(mut mac) = hmac::Hmac::<Sha256>::new_from_slice(key) else {
        return Vec::new();
    };
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// An object key as S3 signs it in a path: every part percent-encoded, `/` kept.
fn encode_key(key: &str) -> String {
    key.split('/').map(percent).collect::<Vec<_>>().join("/")
}

impl S3 {
    pub fn new(settings: S3Settings) -> Result<Self, String> {
        // rustls needs a cryptography provider chosen once per process; a second call finds it chosen already.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .user_agent(concat!("openduct/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| format!("cannot set up HTTP: {e}"))?;
        let (base, path_style) = match &settings.endpoint {
            Some(e) => (
                Url::parse(e).map_err(|err| format!("{e:?} is not an address: {err}"))?,
                true,
            ),
            None => (
                Url::parse(&format!(
                    "https://{}.s3.{}.amazonaws.com",
                    settings.bucket, settings.region
                ))
                .map_err(|e| e.to_string())?,
                false,
            ),
        };
        Ok(Self {
            credentials: Credentials::new(client.clone(), settings.region.clone(), settings.keys.clone()),
            client,
            bucket: settings.bucket,
            region: settings.region,
            base,
            path_style,
        })
    }

    /// The path a request for `key` (none: the bucket) goes to, as signed.
    fn path(&self, key: Option<&str>) -> String {
        match (self.path_style, key) {
            (true, Some(k)) => format!("/{}/{}", self.bucket, encode_key(k)),
            (true, None) => format!("/{}", self.bucket),
            (false, Some(k)) => format!("/{}", encode_key(k)),
            (false, None) => "/".to_owned(),
        }
    }

    fn host(&self) -> String {
        let name = self.base.host_str().unwrap_or_default();
        match self.base.port() {
            Some(port) => format!("{name}:{port}"),
            None => name.to_owned(),
        }
    }

    /// Signed headers for one request (Signature Version 4).
    fn sign(&self, keys: &Keys, method: &Method, path: &str, payload: &str) -> Vec<(String, String)> {
        let now = Utc::now();
        let stamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let mut headers = vec![
            ("host".to_owned(), self.host()),
            ("x-amz-content-sha256".to_owned(), payload.to_owned()),
            ("x-amz-date".to_owned(), stamp.clone()),
        ];
        if let Some(token) = &keys.token {
            headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        let canonical: String = headers
            .iter()
            .map(|(n, v)| format!("{n}:{}\n", v.trim()))
            .collect();
        let signed = headers
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let request = format!("{method}\n{path}\n\n{canonical}\n{signed}\n{payload}");
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
            hex(&Sha256::digest(request.as_bytes()))
        );
        let key = hmac(
            &hmac(
                &hmac(
                    &hmac(format!("AWS4{}", keys.secret).as_bytes(), &date),
                    &self.region,
                ),
                "s3",
            ),
            "aws4_request",
        );
        let signature = hex(&hmac(&key, &to_sign));
        let mut out: Vec<(String, String)> = headers.into_iter().filter(|(n, _)| n != "host").collect();
        out.push((
            "Authorization".to_owned(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
                keys.access
            ),
        ));
        out
    }

    async fn call(
        &self,
        method: Method,
        key: Option<&str>,
        body: Vec<u8>,
    ) -> Result<reqwest::Response, String> {
        let keys = self.credentials.keys().await?;
        let path = self.path(key);
        let payload = hex(&Sha256::digest(&body));
        let url = format!("{}://{}{path}", self.base.scheme(), self.host());
        let mut wait = Duration::from_millis(200);
        let mut last = String::new();
        // A few tries: object storage drops a request now and then.
        for _ in 0..4 {
            let mut request = self.client.request(method.clone(), &url).body(body.clone());
            for (name, value) in self.sign(&keys, &method, &path, &payload) {
                request = request.header(name, value);
            }
            match request.send().await {
                Ok(answer) if answer.status().is_server_error() => {
                    last = format!("S3 answered {}", answer.status())
                }
                Ok(answer) => return Ok(answer),
                Err(e) => last = format!("S3 could not be reached: {e}"),
            }
            tokio::time::sleep(wait).await;
            wait *= 2;
        }
        Err(last)
    }

    async fn refused(answer: reqwest::Response, what: &str) -> String {
        let status = answer.status();
        let text = answer.text().await.unwrap_or_default();
        let code = text
            .split("<Code>")
            .nth(1)
            .and_then(|c| c.split("</Code>").next())
            .unwrap_or("");
        format!("S3 refused to {what}: {status} {code}")
    }

    pub async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        let answer = self.call(Method::PUT, Some(key), body).await?;
        if answer.status().is_success() {
            Ok(())
        } else {
            Err(Self::refused(answer, &format!("store {key}")).await)
        }
    }

    pub async fn get(&self, key: &str) -> Result<Vec<u8>, String> {
        let answer = self.call(Method::GET, Some(key), Vec::new()).await?;
        if !answer.status().is_success() {
            return Err(Self::refused(answer, &format!("return {key}")).await);
        }
        answer
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| e.to_string())
    }

    /// Deletes an object; one already gone is no error.
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        let answer = self.call(Method::DELETE, Some(key), Vec::new()).await?;
        if answer.status().is_success() || answer.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(Self::refused(answer, &format!("delete {key}")).await)
        }
    }

    /// Creates the bucket (on an S3-compatible store; one that exists already is no error).
    pub async fn create_bucket(&self) -> Result<(), String> {
        let answer = self.call(Method::PUT, None, Vec::new()).await?;
        if answer.status().is_success() || answer.status() == reqwest::StatusCode::CONFLICT {
            Ok(())
        } else {
            Err(Self::refused(answer, "create the bucket").await)
        }
    }
}
