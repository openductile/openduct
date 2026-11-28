// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The model calls retrieval makes, through the run's HTTP effect and an AI credential: vectors for texts, a batch at
//! a time in the standard embeddings form (the `embed` step's), and an ordering of passages by a rerank model - the
//! `POST <base_url>/rerank` form (`{model, query, documents, top_n}`, answered `{results: [{index,
//! relevance_score}]}`) that services offering reranking share. A service without one answers 404, and the step says
//! so.

use std::collections::BTreeMap;
use std::time::Instant;

use openduct_expr::{Env, Value};
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value as Json, json};

use super::{Embedder, MODEL_WAIT, as_json, millis, shown};
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

/// The most texts, and bytes of text, in one request for vectors.
const BATCH: usize = 64;
const BATCH_BYTES: usize = 256 * 1024;

/// Vectors made, or - in a test run - the request that would have made them, shown.
#[derive(Debug)]
pub(crate) enum Vectors {
    Made(Vec<Vec<f64>>),
    Shown(Value),
}

/// A list of numbers as a vector.
pub(crate) fn numbers(value: &Value) -> Option<Vec<f64>> {
    let Value::List(items) = value else { return None };
    items
        .iter()
        .map(|n| match n {
            Value::Number(d) => d.to_f64(),
            Value::BigNumber(raw) => raw.parse().ok(),
            _ => None,
        })
        .collect()
}

/// The model an embedder names, worked out; empty for its credential's own.
fn model_of(embedder: &Embedder, env: &Env<'_>) -> Result<String, String> {
    embedder
        .model
        .as_ref()
        .map_or(Ok(String::new()), |m| m.render(env))
        .map_err(|e| format!("embedding_model: {e}"))
}

/// The vectors for `texts`, in their order, and how long the model took.
pub(crate) async fn embed(
    effects: &dyn Effects,
    embedder: &Embedder,
    credential: &Credential,
    env: &Env<'_>,
    texts: &[String],
) -> Result<(Vectors, u64), String> {
    let model = model_of(embedder, env)?;
    let started = Instant::now();
    let mut out = Vec::with_capacity(texts.len());
    let mut rest = texts;
    while !rest.is_empty() {
        let mut take = 0;
        let mut bytes = 0;
        while let Some(t) = rest.get(take) {
            if take == BATCH || (take > 0 && bytes + t.len() > BATCH_BYTES) {
                break;
            }
            bytes += t.len();
            take += 1;
        }
        let (batch, after) = rest.split_at(take);
        rest = after;
        let mut fields = BTreeMap::new();
        if !model.trim().is_empty() {
            fields.insert("model".to_owned(), Value::text(model.as_str()));
        }
        let input = batch.iter().map(|t| Value::text(t.as_str())).collect();
        fields.insert("input".to_owned(), Value::list(input));
        let request = crate::apps::request("embed", credential, &fields)?;
        let asked = Instant::now();
        let answer = effects.http(request).await?;
        if shown(&answer) {
            return Ok((Vectors::Shown(answer), millis(started)));
        }
        let result = crate::apps::answer("embed", credential, &fields, answer, millis(asked))
            .map_err(|e| format!("{}: {e}", embedder.credential))?;
        let made = match &result {
            Value::Record(r) => match r.get("embeddings") {
                Some(Value::List(vectors)) => vectors.iter().map(numbers).collect::<Option<Vec<_>>>(),
                _ => None,
            },
            _ => None,
        }
        .ok_or("the model's answer holds no vectors")?;
        if made.len() != batch.len() {
            return Err(format!(
                "the model gave {} vectors for {} texts",
                made.len(),
                batch.len()
            ));
        }
        out.extend(made);
    }
    Ok((Vectors::Made(out), millis(started)))
}

/// The request that asks a rerank model to order `documents` by how well each answers `query`.
fn rerank_request(
    credential: &Credential,
    model: &str,
    query: &str,
    documents: &[String],
    top_n: usize,
) -> Result<HttpRequest, String> {
    // The credential's address and headers, as its other requests have them.
    let listing = crate::ai::models_request(credential)?;
    let base = listing
        .url
        .strip_suffix("/models")
        .unwrap_or(&listing.url)
        .to_owned();
    let body = json!({ "model": model, "query": query, "documents": documents, "top_n": top_n, "return_documents": false });
    Ok(HttpRequest {
        read: true,
        method: "POST".to_owned(),
        url: format!("{base}/rerank"),
        headers: listing.headers,
        query: Vec::new(),
        body: Some(("application/json".to_owned(), body.to_string().into_bytes())),
        timeout: MODEL_WAIT,
    })
}

/// The documents' places in the order the model put them, each with its relevance; `None` in a test run.
pub(crate) async fn rerank(
    effects: &dyn Effects,
    credential: &Credential,
    model: &str,
    query: &str,
    documents: &[String],
    top_n: usize,
) -> Result<Option<Vec<(usize, f64)>>, String> {
    if model.trim().is_empty() {
        return Err("rerank.model: the rerank model to ask".to_owned());
    }
    let answer = effects
        .http(rerank_request(credential, model, query, documents, top_n)?)
        .await?;
    if shown(&answer) {
        return Ok(None);
    }
    read_ranking(&as_json(&answer), documents.len()).map(Some)
}

/// A rerank answer (`{status, body}`) as places and relevances.
fn read_ranking(answer: &Json, count: usize) -> Result<Vec<(usize, f64)>, String> {
    let status = answer.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = answer.get("body").cloned().unwrap_or(Json::Null);
    if status >= 400 {
        let said = body
            .pointer("/error/message")
            .or_else(|| body.get("error"))
            .or_else(|| body.get("message"))
            .or_else(|| body.get("detail"))
            .map(|m| m.as_str().map_or_else(|| m.to_string(), str::to_owned))
            .unwrap_or_default();
        let said: String = said.chars().take(300).collect();
        let hint = if status == 404 {
            ": its service may not offer reranking"
        } else {
            ""
        };
        return Err(format!("the rerank model answered {status} {said}{hint}").replace("  ", " "));
    }
    let list = body
        .get("results")
        .or_else(|| body.get("data"))
        .and_then(Json::as_array)
        .ok_or("the rerank model's answer has no results")?;
    let mut seen = vec![false; count];
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        let index = item
            .get("index")
            .and_then(Json::as_u64)
            .and_then(|i| usize::try_from(i).ok())
            .filter(|i| *i < count)
            .ok_or("the rerank model named a passage it was not given")?;
        let score = item
            .get("relevance_score")
            .or_else(|| item.get("score"))
            .and_then(Json::as_f64)
            .ok_or("the rerank model gave a passage no relevance_score")?;
        if let Some(slot) = seen.get_mut(index)
            && !*slot
        {
            *slot = true;
            out.push((index, score));
        }
    }
    out.sort_by(|a, b| b.1.total_cmp(&a.1));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rerank_request_goes_to_the_credential_s_service_in_the_shared_form() {
        let credential = Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), "https://models.example.com/v1/".to_owned()),
                ("api_key".to_owned(), "k".to_owned()),
            ]
            .into(),
        };
        let docs = ["a".to_owned(), "b".to_owned()];
        let r = rerank_request(&credential, "ranker-1", "which?", &docs, 1).unwrap();
        assert_eq!(r.url, "https://models.example.com/v1/rerank");
        assert!(r.read, "a preview asks it: it changes nothing");
        assert!(
            r.headers
                .contains(&("authorization".to_owned(), "Bearer k".to_owned()))
        );
        let body: Json = serde_json::from_slice(&r.body.unwrap().1).unwrap();
        assert_eq!(
            body,
            json!({ "model": "ranker-1", "query": "which?", "documents": ["a", "b"], "top_n": 1, "return_documents": false })
        );
    }

    #[test]
    fn a_ranking_is_read_in_either_shape_and_a_bad_one_is_refused() {
        let ok = json!({ "status": 200, "body": { "results": [
            { "index": 1, "relevance_score": 0.9 }, { "index": 0, "relevance_score": 0.2 }, { "index": 1, "relevance_score": 0.1 }
        ] } });
        assert_eq!(read_ranking(&ok, 2).unwrap(), vec![(1, 0.9), (0, 0.2)]);
        let other = json!({ "status": 200, "body": { "data": [{ "index": 0, "relevance_score": 0.5 }] } });
        assert_eq!(read_ranking(&other, 1).unwrap(), vec![(0, 0.5)]);
        let outside =
            json!({ "status": 200, "body": { "results": [{ "index": 5, "relevance_score": 1.0 }] } });
        assert!(read_ranking(&outside, 2).unwrap_err().contains("not given"));
        let missing = json!({ "status": 404, "body": { "error": { "message": "Not found" } } });
        let e = read_ranking(&missing, 2).unwrap_err();
        assert_eq!(
            e,
            "the rerank model answered 404 Not found: its service may not offer reranking"
        );
    }

    #[test]
    fn numbers_read_as_a_vector_and_anything_else_does_not() {
        let v = Value::parse_json("[0.25, -1, 3]").unwrap();
        assert_eq!(numbers(&v).unwrap(), vec![0.25, -1.0, 3.0]);
        assert!(numbers(&Value::parse_json(r#"[1, "two"]"#).unwrap()).is_none());
        assert!(numbers(&Value::text("[1,2]")).is_none());
    }
}
