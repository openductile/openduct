// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: elasticsearch:` - each message a document, through the bulk API. A message's key is the document's id, so
//! a message written again replaces its document rather than adding a second one.
//!
//! ```yaml
//! to:
//!   elasticsearch:
//!     url: https://search.example.com:9200
//!     index: orders
//!     api_key: secret:ES_API_KEY              # or username: and password:
//! ```

use std::collections::BTreeMap;

use openduct_expr::Value;
use openduct_pipeline::Connector;
use reqwest::Client;

use crate::http::shown;
use crate::settings::{Limits, Settings};
use crate::{SinkOutcome, SinkRecord, web};

const SETTINGS: &[&str] = &["url", "index", "username", "password", "api_key", "action"];
/// The most one bulk request carries.
const BULK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
enum Auth {
    None,
    Basic(String, String),
    ApiKey(String),
}

#[derive(Debug)]
pub struct ElasticsearchSink {
    client: Client,
    url: String,
    index: String,
    auth: Auth,
    action: &'static str,
}

impl ElasticsearchSink {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let auth = match (s.text("api_key")?, s.text("username")?, s.text("password")?) {
            (Some(key), _, _) => Auth::ApiKey(key),
            (None, Some(user), Some(password)) => Auth::Basic(user, password),
            (None, Some(_), None) => return Err(format!("{}: is needed with username", s.path("password"))),
            _ => Auth::None,
        };
        let url = s.required("url")?.trim_end_matches('/').to_owned();
        s.reach_url(&s.path("url"), &url)?;
        Ok(Self {
            client: s.web_client()?,
            url,
            index: s.required("index")?,
            auth,
            action: s.choice("action", &["index", "create"])?,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!("{} index {}", shown(&self.url), self.index)
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        let mut rejected = Vec::new();
        let mut chunk: Vec<(SinkRecord, String)> = Vec::new();
        let mut size = 0;
        for record in records {
            let Some(document) = std::str::from_utf8(&record.value)
                .ok()
                .filter(|t| matches!(Value::parse_json(t), Ok(Value::Record(_))))
            else {
                rejected.push((
                    record,
                    "the value is not a JSON object, which is what a document is".to_owned(),
                ));
                continue;
            };
            let mut action = serde_json::Map::new();
            let mut target = serde_json::Map::new();
            target.insert("_index".into(), serde_json::Value::String(self.index.clone()));
            if let Some(key) = &record.key {
                target.insert("_id".into(), serde_json::Value::String(key.clone()));
            }
            action.insert(self.action.into(), serde_json::Value::Object(target));
            let lines = format!("{}\n{}\n", serde_json::Value::Object(action), document.trim());
            size += lines.len();
            chunk.push((record, lines));
            if size >= BULK_BYTES {
                rejected.extend(self.bulk(std::mem::take(&mut chunk)).await?);
                size = 0;
            }
        }
        if !chunk.is_empty() {
            rejected.extend(self.bulk(chunk).await?);
        }
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    /// One bulk request; the documents Elasticsearch refused, with why.
    async fn bulk(&self, chunk: Vec<(SinkRecord, String)>) -> Result<Vec<(SinkRecord, String)>, String> {
        let body: String = chunk.iter().map(|(_, lines)| lines.as_str()).collect();
        let url = format!("{}/_bulk", self.url);
        let answer = web::send(
            || {
                let request = self
                    .client
                    .post(&url)
                    .header("Content-Type", "application/x-ndjson")
                    .body(body.clone());
                match &self.auth {
                    Auth::None => request,
                    Auth::Basic(u, p) => request.basic_auth(u, Some(p)),
                    Auth::ApiKey(k) => request.header("Authorization", format!("ApiKey {k}")),
                }
            },
            "Elasticsearch",
        )
        .await?;
        let status = answer.status();
        let bytes = web::body(answer, "Elasticsearch").await?;
        if !status.is_success() {
            return Err(format!(
                "Elasticsearch answered {status}: {}",
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ));
        }
        let reply: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| "Elasticsearch did not answer with JSON".to_owned())?;
        if reply.get("errors").and_then(serde_json::Value::as_bool) != Some(true) {
            return Ok(Vec::new());
        }
        let items = reply
            .get("items")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut refused = Vec::new();
        for ((record, _), item) in chunk.into_iter().zip(items.iter()) {
            let result = item.as_object().and_then(|o| o.values().next());
            let failed = result
                .and_then(|r| r.get("status"))
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|s| s >= 300);
            if failed {
                let error = result.and_then(|r| r.get("error"));
                let kind = error
                    .and_then(|e| e.get("type"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("error");
                let why = error
                    .and_then(|e| e.get("reason"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("refused");
                refused.push((
                    record,
                    format!("Elasticsearch refused the document: {kind}: {why}"),
                ));
            }
        }
        Ok(refused)
    }
}
