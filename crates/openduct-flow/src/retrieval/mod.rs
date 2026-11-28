// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Retrieval: documents cut into pieces, kept beside their vectors - in PostgreSQL with pgvector, a vector database
//! (Qdrant, Weaviate, Milvus, Chroma, Pinecone, Redis, MongoDB Atlas; see [`vectors`]), or the run's own memory - the
//! pieces nearest a question found again, and a model's answer written from them with every passage it was given
//! shown.
//!
//! - `split_text` cuts a text, or a list of documents, into pieces by characters, paragraphs, sentences or Markdown
//!   headings, within a size, with an overlap; each piece keeps its source and its place (see [`split`]).
//! - `vector_store` adds pieces to a table or a collection (made, with its indexes, when asked), searches it by
//!   similarity with metadata filters and a top-k, and deletes a document's pieces (see [`store`]). A PostgreSQL
//!   table's name is read when the workflow is; every value goes to the database as a parameter, never into the SQL.
//! - `ask_documents` embeds a question, searches, has a model answer from the passages found, and gives back the
//!   answer with each passage, its document and its score (see [`ask`]).
//!
//! Every request goes through the run's effects, so a test run sends nothing, a preview does the reads (a model's
//! vectors and answers, a search) and only shows the writes, and this machine's network stays closed unless allowed.

use std::time::Duration;

use openduct_expr::{Compiled, Env, Value};

use crate::model::Tree;
use crate::run::Effects;
use crate::template::Template;

mod api;
mod ask;
mod html;
mod pg; // vector stores
pub(crate) mod split;
mod sql;
mod store;
pub(crate) mod vectors; // vector stores

pub(crate) use sql::table_name;
pub(crate) use vectors::RunVectors; // vector stores
pub(crate) use vectors::Target; // vector stores

/// The most pieces one step adds, or one split makes.
pub(crate) const MAX_PIECES: usize = 5_000;
/// The most text one split reads, in bytes, all its documents together.
pub(crate) const MAX_TEXT: usize = 2 * 1024 * 1024;
/// The longest piece a store keeps, in bytes.
pub(crate) const MAX_PIECE: usize = 32 * 1024;
/// The most a search gives, and the most it looks at before reranking.
pub(crate) const MAX_TOP_K: usize = 50;
pub(crate) const MAX_CANDIDATES: usize = 200;
/// The most dimensions a vector has (pgvector's own limit), and the most its index takes.
pub(crate) const MAX_DIMENSIONS: u32 = 16_000;
pub(crate) const MAX_INDEXED: u32 = 2_000;
/// How long a model's service may take over one batch of vectors, or one reranking.
const MODEL_WAIT: Duration = Duration::from_secs(60);

/// A retrieval step.
#[derive(Debug, Clone)]
pub enum Retrieval {
    Split(Split),
    Store(Store),
    Ask(Box<Ask>),
}

impl Retrieval {
    /// The step's kind, as a workflow writes it.
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Split(_) => "split_text",
            Self::Store(_) => "vector_store",
            Self::Ask(_) => "ask_documents",
        }
    }
}

/// How a text is cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitBy {
    Characters,
    Paragraphs,
    Sentences,
    Markdown,
}

/// A `split_text` step.
#[derive(Debug, Clone)]
pub struct Split {
    /// One document's text, or else a list of documents (texts, or records with `text`, `source`, `metadata`).
    pub text: Option<Template>,
    pub documents: Option<Compiled>,
    /// Each document's name; with `documents`, worked out for each (`item`, `index`).
    pub source: Option<Template>,
    /// Fields added to every piece's metadata.
    pub metadata: Option<Tree>,
    pub by: SplitBy,
    /// The most characters a piece holds, and how many of one piece's last characters begin the next.
    pub size: usize,
    pub overlap: usize,
    /// The text is HTML: its tags are taken out first, its headings kept as Markdown's.
    pub html: bool,
}

/// How near two vectors are: the operator pgvector uses, and the index class for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    Cosine,
    L2,
    InnerProduct,
}

/// The table a store keeps its pieces in, on a PostgreSQL credential.
#[derive(Debug, Clone)]
pub(crate) struct Table {
    /// The table's name, checked: small letters, digits and `_`, with a schema before a `.` when given.
    pub(crate) schema: Option<String>,
    pub(crate) name: String,
    pub(crate) distance: Distance,
}

/// Which model makes the vectors: an AI credential, and its model when not the credential's own.
#[derive(Debug, Clone)]
pub struct Embedder {
    pub credential: String,
    pub model: Option<Template>,
}

/// A record given either as an expression (`{'team': value.team}`) or as fields with `{{ }}` in their texts.
#[derive(Debug, Clone)]
pub enum Given {
    Expression(Compiled),
    Fields(Tree),
}

impl Given {
    pub(crate) fn value(&self, env: &Env<'_>) -> Result<Value, String> {
        match self {
            Self::Expression(e) => e.evaluate(env),
            Self::Fields(t) => crate::run::tree(t, env),
        }
    }
}

/// A model that orders passages by how well each answers a question: an AI credential's `/rerank`.
#[derive(Debug, Clone)]
pub struct Rerank {
    /// The credential, when not the one that makes the vectors.
    pub credential: Option<String>,
    pub model: Template,
    /// How many of the nearest it is given to order.
    pub candidates: usize,
}

/// What a search looks for, and how many it keeps.
#[derive(Debug, Clone)]
pub struct Search {
    /// A text, made a vector by the store's embedder; or a vector already made.
    pub query: Option<Template>,
    pub vector: Option<Compiled>,
    pub top_k: usize,
    /// Metadata every piece found holds (a record: `{"team": "sales"}`).
    pub filter: Option<Given>,
    /// The lowest score a piece found may have.
    pub min_score: Option<f64>,
    pub rerank: Option<Rerank>,
}

/// What a `vector_store` step does.
#[derive(Debug, Clone)]
pub enum StoreAction {
    /// Pieces added, or replaced when their id is there already.
    Add {
        pieces: Compiled,
        /// Their vectors, in the pieces' order, when an earlier step made them.
        embeddings: Option<Compiled>,
        /// Make the table and its indexes when they are not there.
        create: bool,
        dimensions: Option<u32>,
        /// A document added again loses the pieces it no longer has.
        replace: bool,
    },
    Search(Search),
    /// Every piece of these documents (a source, or a list of them).
    Delete {
        sources: Compiled,
    },
}

/// A `vector_store` step.
#[derive(Debug, Clone)]
pub struct Store {
    /// Where its pieces are: a PostgreSQL table, a vector database's collection, or the run's memory.
    pub target: Target,
    pub embed: Option<Embedder>,
    pub action: StoreAction,
}

/// An `ask_documents` step.
#[derive(Debug, Clone)]
pub struct Ask {
    pub target: Target,
    pub embed: Embedder,
    pub question: Template,
    pub search: Search,
    /// The AI credential that writes the answer, when not the one that makes the vectors; its model.
    pub answer_with: Option<String>,
    pub model: Option<Template>,
    /// More to tell the model, beside the rule to answer from the passages only.
    pub instructions: Option<Template>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f64>,
    /// The answer when no passage is found: the model is not asked.
    pub no_answer: Option<Template>,
}

/// Runs a retrieval step; `held` is the run's own memory of pieces (`store: memory`).
pub(crate) async fn run(
    step: &Retrieval,
    effects: &dyn Effects,
    env: &Env<'_>,
    held: &RunVectors,
) -> Result<Value, String> {
    match step {
        Retrieval::Split(s) => split::run(s, env),
        Retrieval::Store(s) => store::run(s, effects, env, held).await,
        Retrieval::Ask(a) => ask::run(a, effects, env, held).await,
    }
}

/// A test run's (or a preview's) result: what was shown, the statements (PostgreSQL's), and the fields the step's
/// result has.
fn shown_result(answer: &Value, sql: &[String], fields: serde_json::Value) -> serde_json::Value {
    merged(as_json(answer), sql, fields)
}

/// What a store showed - the request, or the command - with the statements (when it has them) and the step's own
/// fields.
fn merged(shown: serde_json::Value, sql: &[String], fields: serde_json::Value) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    out.insert(
        "test".to_owned(),
        shown.get("test").cloned().unwrap_or(serde_json::Value::Null),
    );
    if !sql.is_empty() {
        out.insert("sql".to_owned(), serde_json::json!(sql));
    }
    if let serde_json::Value::Object(o) = shown {
        for (k, v) in o {
            // A statement shown is under `sql` already, its values with it in the statement's place.
            if !matches!(k.as_str(), "test" | "sql" | "database" | "params") {
                out.entry(k).or_insert(v);
            }
        }
    }
    if let serde_json::Value::Object(more) = fields {
        out.extend(more);
    }
    serde_json::Value::Object(out)
}

/// Whether an effect's answer is a test run's (or a preview's) showing of what it would have done.
fn shown(answer: &Value) -> bool {
    matches!(answer, Value::Record(r) if r.get("test").is_some())
}

/// A credential, refused when it is not of the kind a step takes.
fn credential_of(
    effects: &dyn Effects,
    name: &str,
    kind: &str,
    what: &str,
) -> Result<crate::apps::Credential, String> {
    let credential = effects.credential(name)?;
    if credential.kind != kind && credential.kind != "test" {
        return Err(format!(
            "{name} is a {} credential: {what} takes a {kind} one",
            credential.kind
        ));
    }
    Ok(credential)
}

fn as_json(value: &Value) -> serde_json::Value {
    serde_json::from_str(&value.to_json()).unwrap_or(serde_json::Value::Null)
}

fn millis(since: std::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}
