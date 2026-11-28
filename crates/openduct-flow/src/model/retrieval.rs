// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the retrieval steps (see [`crate::retrieval`]): `split_text`, `vector_store` and `ask_documents`. A
//! table's name is checked here, before any run, since it is the one thing written into their SQL.

use serde_json::{Map, Value as Json};

use super::{Kind, Reader, text};
use crate::retrieval::{
    Ask, Embedder, Given, MAX_CANDIDATES, MAX_DIMENSIONS, MAX_TOP_K, Rerank, Retrieval, Search, Split,
    SplitBy, Store, StoreAction, Target,
};

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

fn only(s: &Map<String, Json>, at: &str, what: &str, allowed: &[&str]) -> Result<(), String> {
    match s.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!(
            "{at}.{k}: not a setting of {what} ({})",
            allowed.join(", ")
        )),
        None => Ok(()),
    }
}

/// A whole number setting within `range`, written as a number or as its digits.
fn whole(s: &Map<String, Json>, name: &str, at: &str, least: u64, most: u64) -> Result<Option<u64>, String> {
    let Some(v) = s.get(name).filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    v.as_u64()
        .or_else(|| v.as_str().and_then(|t| t.trim().parse().ok()))
        .filter(|n| (least..=most).contains(n))
        .map(Some)
        .ok_or_else(|| format!("{at}.{name}: a whole number, {least} to {most}"))
}

fn decimal(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    least: f64,
    most: f64,
) -> Result<Option<f64>, String> {
    let Some(v) = s.get(name).filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    v.as_f64()
        .or_else(|| v.as_str().and_then(|t| t.trim().parse().ok()))
        .filter(|n| (least..=most).contains(n))
        .map(Some)
        .ok_or_else(|| format!("{at}.{name}: a number, {least} to {most}"))
}

fn flag(s: &Map<String, Json>, name: &str, at: &str, default: bool) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

/// A credential's name, when the setting is there.
fn name_of(s: &Map<String, Json>, name: &str) -> Option<String> {
    text(s.get(name))
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_owned)
}

// vector stores: the store, its credential, the table or collection, the distance, a namespace or an index.
const TABLE: &[&str] = crate::retrieval::vectors::read::SETTINGS;
const SEARCH: &[&str] = &["top_k", "filter", "min_score", "rerank"];

impl Reader {
    pub(super) fn retrieval(&self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        Ok(Kind::Retrieval(match kind {
            "split_text" => Retrieval::Split(self.split_text(body, at)?),
            "vector_store" => Retrieval::Store(self.vector_store(body, at)?),
            _ => Retrieval::Ask(Box::new(self.ask_documents(body, at)?)),
        }))
    }

    fn split_text(&self, body: &Json, at: &str) -> Result<Split, String> {
        let s = settings(body, at)?;
        only(
            s,
            at,
            "split_text",
            &[
                "text",
                "documents",
                "source",
                "metadata",
                "by",
                "size",
                "overlap",
                "format",
            ],
        )?;
        let (text, documents) = match (s.get("text"), s.get("documents")) {
            (Some(t), None) => (Some(self.template(t, &format!("{at}.text"))?), None),
            (None, Some(d)) => (None, Some(self.expression(d, &format!("{at}.documents"))?)),
            _ => {
                return Err(format!(
                    "{at}: text (one document's) or documents (a list of them), one of the two"
                ));
            }
        };
        let by = match text_or(s, "by", "paragraphs") {
            "characters" => SplitBy::Characters,
            "paragraphs" => SplitBy::Paragraphs,
            "sentences" => SplitBy::Sentences,
            "markdown" => SplitBy::Markdown,
            other => {
                return Err(format!(
                    "{at}.by: characters, paragraphs, sentences or markdown, not {other:?}"
                ));
            }
        };
        let size = whole(s, "size", at, 50, 20_000)?.unwrap_or(1_000);
        let overlap = whole(s, "overlap", at, 0, size / 2)
            .map_err(|_| format!("{at}.overlap: 0 to half the size ({})", size / 2))?
            .unwrap_or(0);
        let html = match text_or(s, "format", "text") {
            "text" => false,
            "html" => true,
            other => return Err(format!("{at}.format: text or html, not {other:?}")),
        };
        let metadata = match s.get("metadata") {
            None | Some(Json::Null) => None,
            Some(m @ Json::Object(_)) => Some(self.tree(m, &format!("{at}.metadata"))?),
            Some(_) => {
                return Err(format!(
                    "{at}.metadata: fields added to every piece, as {{team: sales}}"
                ));
            }
        };
        let (size, overlap) = (
            usize::try_from(size).unwrap_or(1_000),
            usize::try_from(overlap).unwrap_or(0),
        );
        Ok(Split {
            text,
            documents,
            source: s
                .get("source")
                .map(|v| self.template(v, &format!("{at}.source")))
                .transpose()?,
            metadata,
            by,
            size,
            overlap,
            html,
        })
    }

    // vector stores: a PostgreSQL table, a vector database's collection, or the run's memory - and whether its store
    // can express the step's filter.
    fn vector_table(&self, s: &Map<String, Json>, at: &str) -> Result<Target, String> {
        let target = crate::retrieval::vectors::read::target(s, at)?;
        crate::retrieval::vectors::read::filter(&target, s.get("filter"), &format!("{at}.filter"))?;
        Ok(target)
    }

    fn embedder(&self, s: &Map<String, Json>, at: &str) -> Result<Option<Embedder>, String> {
        let Some(credential) = name_of(s, "embed_with") else {
            if s.contains_key("embedding_model") {
                return Err(format!(
                    "{at}.embed_with: the AI credential whose model makes the vectors"
                ));
            }
            return Ok(None);
        };
        Ok(Some(Embedder {
            credential,
            model: s
                .get("embedding_model")
                .map(|m| self.template(m, &format!("{at}.embedding_model")))
                .transpose()?,
        }))
    }

    fn search(&self, s: &Map<String, Json>, at: &str, top_k: u64) -> Result<Search, String> {
        let top_k = whole(s, "top_k", at, 1, MAX_TOP_K as u64)?.unwrap_or(top_k);
        let filter = match s.get("filter") {
            None | Some(Json::Null) => None,
            Some(f @ Json::Object(_)) => Some(Given::Fields(self.tree(f, &format!("{at}.filter"))?)),
            Some(f) => Some(Given::Expression(self.expression(f, &format!("{at}.filter"))?)),
        };
        let rerank = match s.get("rerank") {
            None | Some(Json::Null) => None,
            Some(r) => {
                let place = format!("{at}.rerank");
                let r = settings(r, &place)?;
                only(r, &place, "rerank", &["model", "credential", "candidates"])?;
                // Four times the top-k by default, at least 20; never fewer than the top-k.
                let candidates = whole(r, "candidates", &place, top_k, MAX_CANDIDATES as u64)?
                    .unwrap_or_else(|| (top_k * 4).clamp(top_k.max(20), MAX_CANDIDATES as u64));
                let candidates = usize::try_from(candidates).unwrap_or(MAX_CANDIDATES);
                Some(Rerank {
                    credential: name_of(r, "credential"),
                    model: self.template(
                        r.get("model")
                            .ok_or_else(|| format!("{place}.model: the rerank model to ask"))?,
                        &format!("{place}.model"),
                    )?,
                    candidates,
                })
            }
        };
        let top_k = usize::try_from(top_k).unwrap_or(MAX_TOP_K);
        Ok(Search {
            query: None,
            vector: None,
            top_k,
            filter,
            min_score: decimal(s, "min_score", at, -1e9, 1e9)?,
            rerank,
        })
    }

    fn vector_store(&self, body: &Json, at: &str) -> Result<Store, String> {
        let s = settings(body, at)?;
        let action = text(s.get("action")).ok_or_else(|| format!("{at}.action: add, search or delete"))?;
        let own: &[&str] = match action {
            "add" => &["pieces", "embeddings", "create", "dimensions", "replace"],
            "search" => &["query", "vector", "top_k", "filter", "min_score", "rerank"],
            "delete" => &["source"],
            other => return Err(format!("{at}.action: add, search or delete, not {other:?}")),
        };
        let mut allowed: Vec<&str> = TABLE.to_vec();
        allowed.extend(["action", "embed_with", "embedding_model"]);
        allowed.extend(own);
        only(s, at, &format!("vector_store's {action}"), &allowed)?;
        let target = self.vector_table(s, at)?; // vector stores
        let embed = self.embedder(s, at)?;
        let expression = |name: &str, what: &str| {
            s.get(name)
                .ok_or_else(|| format!("{at}.{name}: {what}"))
                .and_then(|v| self.expression(v, &format!("{at}.{name}")))
        };
        let action = match action {
            "add" => StoreAction::Add {
                pieces: expression("pieces", "the pieces to add, as steps.split.pieces")?,
                embeddings: s
                    .get("embeddings")
                    .map(|v| self.expression(v, &format!("{at}.embeddings")))
                    .transpose()?,
                create: flag(s, "create", at, false)?,
                dimensions: whole(s, "dimensions", at, 1, u64::from(MAX_DIMENSIONS))?
                    .and_then(|d| u32::try_from(d).ok()),
                replace: flag(s, "replace", at, true)?,
            },
            "search" => {
                let mut search = self.search(s, at, 5)?;
                match (s.get("query"), s.get("vector")) {
                    (Some(q), None) => {
                        if embed.is_none() {
                            return Err(format!(
                                "{at}.embed_with: the AI credential whose model makes the query a vector"
                            ));
                        }
                        search.query = Some(self.template(q, &format!("{at}.query"))?);
                    }
                    (None, Some(v)) => {
                        if search.rerank.is_some() {
                            return Err(format!(
                                "{at}.rerank: a rerank model orders passages by a query: give query, not vector"
                            ));
                        }
                        search.vector = Some(self.expression(v, &format!("{at}.vector"))?);
                    }
                    _ => {
                        return Err(format!(
                            "{at}: query (a text to search for) or vector (one already made), one of the two"
                        ));
                    }
                }
                StoreAction::Search(search)
            }
            _ => StoreAction::Delete {
                sources: expression(
                    "source",
                    "the document (or a list of them) whose pieces to delete",
                )?,
            },
        };
        Ok(Store {
            target,
            embed,
            action,
        })
    }

    fn ask_documents(&self, body: &Json, at: &str) -> Result<Ask, String> {
        let s = settings(body, at)?;
        let mut allowed: Vec<&str> = TABLE.to_vec();
        allowed.extend(SEARCH);
        allowed.extend([
            "question",
            "embed_with",
            "embedding_model",
            "answer_with",
            "model",
            "instructions",
            "max_tokens",
            "temperature",
            "no_answer",
        ]);
        only(s, at, "ask_documents", &allowed)?;
        let template = |name: &str| {
            s.get(name)
                .map(|v| self.template(v, &format!("{at}.{name}")))
                .transpose()
        };
        Ok(Ask {
            target: self.vector_table(s, at)?, // vector stores
            embed: self.embedder(s, at)?.ok_or_else(|| {
                format!("{at}.embed_with: the AI credential whose model makes the question a vector")
            })?,
            question: template("question")?
                .ok_or_else(|| format!("{at}.question: the question to answer"))?,
            search: self.search(s, at, 4)?,
            answer_with: name_of(s, "answer_with"),
            model: template("model")?,
            instructions: template("instructions")?,
            max_tokens: whole(s, "max_tokens", at, 1, 100_000)?.and_then(|n| u32::try_from(n).ok()),
            temperature: decimal(s, "temperature", at, 0.0, 2.0)?,
            no_answer: template("no_answer")?,
        })
    }
}

fn text_or<'a>(s: &'a Map<String, Json>, name: &str, default: &'a str) -> &'a str {
    text(s.get(name)).map_or(default, str::trim)
}
