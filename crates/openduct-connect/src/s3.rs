// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: s3:` - messages gathered into objects (one JSON value per line, gzip-compressed by default) in Amazon S3 or
//! any store that speaks its API: MinIO, Ceph, Cloudflare R2, Wasabi. A batch's progress is committed only once its
//! object is stored.
//!
//! ```yaml
//! to:
//!   s3:
//!     bucket: data-lake
//!     prefix: orders/
//!     region: eu-west-1
//!     access_key: secret:S3_ACCESS_KEY
//!     secret_key: secret:S3_SECRET_KEY
//!     every: 5m                     # an object at least this often, when there is something to write
//!     max_mib: 64                   # or when it reaches this size
//! ```

use std::collections::BTreeMap;
use std::io::Write as _;
use std::time::{Duration, Instant};

use chrono::Utc;
use openduct_pipeline::Connector;
use reqwest::{Client, Url};

use crate::settings::{Limits, Settings};
use crate::{SinkOutcome, SinkRecord, web};

const SETTINGS: &[&str] = &[
    "bucket",
    "prefix",
    "region",
    "endpoint",
    "path_style",
    "access_key",
    "secret_key",
    "session_token",
    "every",
    "max_mib",
    "compress",
];

#[derive(Debug)]
pub struct S3Sink {
    client: Client,
    pipeline: String,
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Url,
    path_style: bool,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    every: Duration,
    max_bytes: usize,
    gzip: bool,
    buffer: Vec<u8>,
    count: u64,
    first: Option<(i32, i64)>,
    opened: Option<Instant>,
    held: BTreeMap<i32, i64>,
}

impl S3Sink {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let region = s.text("region")?.unwrap_or_else(|| "us-east-1".to_owned());
        let custom = s.text("endpoint")?;
        let endpoint = custom
            .clone()
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint =
            Url::parse(&endpoint).map_err(|e| format!("{}: is not a URL: {e}", s.path("endpoint")))?;
        s.reach_url(&s.path("endpoint"), endpoint.as_str())?;
        // From its settings alone (secret:NAME): never the environment.
        let (Some(access_key), Some(secret_key)) = (s.text("access_key")?, s.text("secret_key")?) else {
            return Err(format!(
                "{}: needs access_key and secret_key, each written secret:NAME",
                connector.path
            ));
        };
        let mut prefix = s.text("prefix")?.unwrap_or_default();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Ok(Self {
            client: s.web_client()?,
            pipeline: pipeline.to_owned(),
            bucket: s.required("bucket")?,
            prefix,
            region,
            path_style: s.boolean("path_style", custom.is_some())?,
            endpoint,
            access_key,
            secret_key,
            session_token: s.text("session_token")?,
            every: s.duration("every", Duration::from_secs(60))?,
            max_bytes: usize::try_from(s.number("max_mib", 64, 1..=4_096)?).unwrap_or(64) * 1024 * 1024,
            gzip: s.choice("compress", &["gzip", "none"])? == "gzip",
            buffer: Vec::new(),
            count: 0,
            first: None,
            opened: None,
            held: BTreeMap::new(),
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!(
            "s3://{}/{} (an object every {:?} or {} MiB)",
            self.bucket,
            self.prefix,
            self.every,
            self.max_bytes / 1024 / 1024
        )
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        if records.is_empty() && self.buffer.is_empty() {
            // Nothing waiting to be written: what was read so far is as safe as it will get.
            return Ok(SinkOutcome {
                durable: through,
                rejected: Vec::new(),
            });
        }
        for record in records {
            self.first.get_or_insert((record.partition, record.offset));
            self.opened.get_or_insert_with(Instant::now);
            self.buffer.extend_from_slice(&record.value);
            self.buffer.push(b'\n');
            self.count += 1;
        }
        self.held.extend(through);
        if self.buffer.len() >= self.max_bytes {
            self.flush().await
        } else {
            Ok(SinkOutcome::default())
        }
    }

    pub(crate) async fn tick(&mut self) -> Result<SinkOutcome, String> {
        match self.opened {
            Some(opened) if opened.elapsed() >= self.every => self.flush().await,
            _ => Ok(SinkOutcome::default()),
        }
    }

    pub(crate) async fn close(&mut self) -> Result<SinkOutcome, String> {
        self.flush().await
    }

    async fn flush(&mut self) -> Result<SinkOutcome, String> {
        if self.buffer.is_empty() {
            return Ok(SinkOutcome {
                durable: std::mem::take(&mut self.held),
                rejected: Vec::new(),
            });
        }
        let now = Utc::now();
        let (p, o) = self.first.unwrap_or((0, 0));
        let key = format!(
            "{}{}/{}-{}-{p}-{o}.jsonl{}",
            self.prefix,
            now.format("%Y/%m/%d"),
            self.pipeline,
            now.format("%Y%m%dT%H%M%SZ"),
            if self.gzip { ".gz" } else { "" }
        );
        let body = if self.gzip {
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gz.write_all(&self.buffer)
                .map_err(|e| format!("cannot compress: {e}"))?;
            gz.finish().map_err(|e| format!("cannot compress: {e}"))?
        } else {
            self.buffer.clone()
        };
        self.put(&key, body).await?;
        tracing::info!(bucket = %self.bucket, key, messages = self.count, "object written");
        self.buffer.clear();
        self.count = 0;
        self.first = None;
        self.opened = None;
        Ok(SinkOutcome {
            durable: std::mem::take(&mut self.held),
            rejected: Vec::new(),
        })
    }

    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        let path = if self.path_style {
            format!("/{}/{}", self.bucket, encode_key(key))
        } else {
            format!("/{}", encode_key(key))
        };
        let host_name = self.endpoint.host_str().unwrap_or_default();
        let host = match (self.path_style, self.endpoint.port()) {
            (true, Some(port)) => format!("{host_name}:{port}"),
            (true, None) => host_name.to_owned(),
            (false, Some(port)) => format!("{}.{host_name}:{port}", self.bucket),
            (false, None) => format!("{}.{host_name}", self.bucket),
        };
        let url = format!("{}://{host}{path}", self.endpoint.scheme());
        let content_type = if self.gzip {
            "application/gzip"
        } else {
            "application/x-ndjson"
        };
        let answer = web::send(
            || {
                let signed = sign(self, &host, &path, &body, Utc::now());
                let mut request = self
                    .client
                    .put(&url)
                    .header("Content-Type", content_type)
                    .body(body.clone());
                for (name, value) in signed {
                    request = request.header(name, value);
                }
                request
            },
            "S3",
        )
        .await?;
        if !answer.status().is_success() {
            let status = answer.status();
            let text = web::body(answer, "S3").await.unwrap_or_default();
            let code = String::from_utf8_lossy(&text);
            let code = code
                .split("<Code>")
                .nth(1)
                .and_then(|c| c.split("</Code>").next())
                .unwrap_or("");
            return Err(format!("S3 refused to store {key}: {status} {code}"));
        }
        Ok(())
    }
}

/// An object key as a URL path: every byte but the unreserved ones and `/` percent-encoded, as S3 signs it.
fn encode_key(key: &str) -> String {
    crate::sigv4::encode_path(key)
}

/// The headers that sign a PUT with AWS Signature Version 4.
fn sign(
    s3: &S3Sink,
    host: &str,
    path: &str,
    body: &[u8],
    now: chrono::DateTime<Utc>,
) -> Vec<(String, String)> {
    let request = crate::sigv4::Request {
        method: "PUT",
        host,
        path,
        query: &[],
        region: &s3.region,
        service: "s3",
    };
    let keys = crate::sigv4::Keys {
        access: &s3.access_key,
        secret: &s3.secret_key,
        token: s3.session_token.as_deref(),
    };
    crate::sigv4::headers(&request, &[], &crate::sigv4::sha256_hex(body), &keys, now)
}
