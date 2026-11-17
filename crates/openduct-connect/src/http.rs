// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: http:` - ask a URL on a schedule and turn what it answers into messages, carrying on from a position.
//!
//! ```yaml
//! from:
//!   http:
//!     url: https://api.example.com/orders?updated_since={position}
//!     every: 30s
//!     headers: { Authorization: secret:API_TOKEN }
//!     records: value.data                      # the list of records in the answer
//!     position: max(records, r -> r.updated_at) # where to carry on from next time
//!     next: value.links.next                   # the next page, or null when there is none
//!     start: "2026-01-01T00:00:00Z"
//! ```

use std::time::{Duration, Instant};

use openduct_expr::{Compiled, Env, Value};
use openduct_pipeline::Connector;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{Client, Method, Url};

use crate::settings::{Limits, Settings};
use crate::{SourceBatch, SourceMessage, web};

const SETTINGS: &[&str] = &[
    "url",
    "method",
    "body",
    "headers",
    "every",
    "records",
    "position",
    "next",
    "start",
    "format",
    "max_pages",
];

#[derive(Debug)]
pub struct HttpSource {
    client: Client,
    url: String,
    method: Method,
    body: Option<String>,
    headers: Vec<(String, String)>,
    every: Duration,
    records: Option<Compiled>,
    position: Option<Compiled>,
    next: Option<Compiled>,
    start: Option<String>,
    text: bool,
    max_pages: u64,
    last_poll: Option<Instant>,
}

impl HttpSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let url = s.required("url")?;
        s.reach_url(&s.path("url"), &url)?;
        if !s.checking() {
            Url::parse(&url.replace("{position}", "x"))
                .map_err(|e| format!("{}: is not a URL: {e}", s.path("url")))?;
        }
        let method = match s.choice("method", &["GET", "POST"])? {
            "POST" => Method::POST,
            _ => Method::GET,
        };
        Ok(Self {
            client: s.web_client()?,
            url,
            method,
            body: s.text("body")?,
            headers: s.pairs("headers")?,
            every: s.duration("every", Duration::from_secs(60))?,
            records: s.expression("records")?,
            position: s.expression("position")?,
            next: s.expression("next")?,
            start: s.text("start")?,
            text: s.choice("format", &["json", "text"])? == "text",
            max_pages: s.number("max_pages", 100, 1..=10_000)?,
            last_poll: None,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!("{} {} every {:?}", self.method, shown(&self.url), self.every)
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        if let Some(last) = self.last_poll {
            tokio::time::sleep_until((last + self.every).into()).await;
        }
        self.last_poll = Some(Instant::now());
        let at = position
            .map(str::to_owned)
            .or_else(|| self.start.clone())
            .unwrap_or_default();
        let mut url = self.url.replace(
            "{position}",
            &utf8_percent_encode(&at, NON_ALPHANUMERIC).to_string(),
        );
        let body = self.body.as_ref().map(|b| b.replace("{position}", &at));
        let mut messages = Vec::new();
        let mut carried = position.map(str::to_owned);
        for _ in 0..self.max_pages {
            let where_ = shown(&url);
            let answer = web::send(
                || {
                    let mut request = self.client.request(self.method.clone(), &url);
                    for (name, value) in &self.headers {
                        request = request.header(name, value);
                    }
                    if let Some(b) = &body {
                        request = request.body(b.clone());
                    }
                    request
                },
                &where_,
            )
            .await?;
            if !answer.status().is_success() {
                return Err(format!("{where_} answered {}", answer.status()));
            }
            let bytes = web::body(answer, &where_).await?;
            if self.text {
                messages.push(SourceMessage::new(bytes));
                break;
            }
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| format!("{where_} answered with something that is not text"))?;
            let answer =
                Value::parse_json(text).map_err(|e| format!("{where_} did not answer with JSON: {e}"))?;
            let records = match &self.records {
                Some(f) => evaluate(f, &answer, &[], "records")?,
                None => answer.clone(),
            };
            let records = match records {
                Value::List(items) => items.to_vec(),
                Value::Null => Vec::new(),
                one => vec![one],
            };
            if let Some(f) = &self.position {
                let names = [("records".to_owned(), Value::list(records.clone()))];
                let p = evaluate(f, &answer, &names, "position")?;
                if !p.is_null() {
                    carried = Some(p.to_text());
                }
            }
            messages.extend(
                records
                    .into_iter()
                    .map(|r| SourceMessage::new(r.to_json().into_bytes())),
            );
            match self
                .next
                .as_ref()
                .map(|f| evaluate(f, &answer, &[], "next"))
                .transpose()?
            {
                Some(Value::Text(next)) if !next.is_empty() => url = absolute(&url, &next)?,
                _ => break,
            }
        }
        Ok(SourceBatch {
            messages,
            position: carried,
        })
    }
}

fn evaluate(
    expression: &Compiled,
    answer: &Value,
    names: &[(String, Value)],
    setting: &str,
) -> Result<Value, String> {
    expression
        .evaluate(&Env {
            value: answer,
            message: &Value::Null,
            names,
        })
        .map_err(|e| format!("from.http.{setting}: {e}"))
}

/// A next-page link may be relative to the page it came from.
fn absolute(base: &str, next: &str) -> Result<String, String> {
    let base = Url::parse(base).map_err(|e| format!("not a URL: {e}"))?;
    base.join(next)
        .map(String::from)
        .map_err(|e| format!("from.http.next: {next:?} is not a URL: {e}"))
}

/// A URL as messages show it: scheme, host and path - never the query, which may hold a key.
pub(crate) fn shown(url: &str) -> String {
    match Url::parse(url) {
        Ok(u) => format!(
            "{}://{}{}",
            u.scheme(),
            u.host_str().unwrap_or_default(),
            u.path()
        ),
        Err(_) => "the URL".to_owned(),
    }
}
