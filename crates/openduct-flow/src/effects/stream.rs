// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An HTTP request whose answer is handed over part by part as it comes - a model's answer, streamed to a chat - with
//! the same guards as every other request a run makes: http and https only, and no address on this machine's own
//! network unless the operator allows it (names that resolve to one are refused by the client itself).

use openduct_expr::Value;
use serde_json::{Value as Json, json};

use super::{Live, MAX_ANSWER, describe, literal_ip, private};
use crate::run::HttpRequest;

impl Live {
    /// Sends `r`; each part of a successful answer of events is handed to `part` as it arrives. Answers as
    /// [`Live::send`] does: `{status, headers, body}`, the body as the text that came.
    pub(super) async fn send_parts(
        &self,
        r: HttpRequest,
        mut part: crate::chat::Parts<'_>,
    ) -> Result<Value, String> {
        let mut url = reqwest::Url::parse(&r.url).map_err(|e| format!("{:?} is not a URL: {e}", r.url))?;
        if !r.query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(r.query.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("{:?}: only http and https addresses", r.url));
        }
        if !self.allow_private && url.host_str().and_then(literal_ip).is_some_and(private) {
            return Err(format!(
                "{} is on this machine's own network: start OpenDuct with --allow-private-network to let workflows \
                 reach it",
                url.host_str().unwrap_or_default()
            ));
        }
        let method = reqwest::Method::from_bytes(r.method.as_bytes()).map_err(|e| e.to_string())?;
        let mut request = self.http.request(method, url).timeout(r.timeout);
        for (name, value) in &r.headers {
            request = request.header(name, value);
        }
        if let Some((kind, bytes)) = r.body {
            request = request.header(reqwest::header::CONTENT_TYPE, kind).body(bytes);
        }
        let mut answer = request.send().await.map_err(|e| describe(&e))?;
        let status = answer.status().as_u16();
        let mut headers = serde_json::Map::new();
        for (name, value) in answer.headers() {
            if let Ok(v) = value.to_str() {
                headers.insert(name.as_str().to_owned(), Json::String(v.to_owned()));
            }
        }
        let kind = headers
            .get("content-type")
            .and_then(Json::as_str)
            .unwrap_or_default();
        let is_json = kind.contains("json");
        let events = (200..300).contains(&status) && kind.contains("event-stream");
        let mut body = Vec::new();
        while let Some(chunk) = answer.chunk().await.map_err(|e| describe(&e))? {
            if body.len() + chunk.len() > MAX_ANSWER {
                return Err(format!("an answer of more than {MAX_ANSWER} bytes"));
            }
            if events {
                part(&chunk);
            }
            body.extend_from_slice(&chunk);
        }
        let text = String::from_utf8_lossy(&body).into_owned();
        let body = if is_json {
            serde_json::from_str::<Json>(&text).unwrap_or(Json::String(text))
        } else {
            Json::String(text)
        };
        Ok(Value::from_json(
            &json!({ "status": status, "headers": headers, "body": body }),
        ))
    }
}
