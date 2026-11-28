// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL with pgvector as a vector store: the statements of [`super::sql`], each run through the run's SQL
//! effect - a test run shows it, a preview runs what reads and shows what writes.

use openduct_expr::Value;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value as Json, json};

use super::sql::{rows_of, vector_text};
use super::vectors::{Got, Hit, Piece};
use super::{Table, as_json, shown, shown_result};
use crate::apps::Credential;
use crate::run::{Effects, SqlRequest};

/// How many pieces one statement writes.
const WRITE_BATCH: usize = 100;

/// A table of pieces on a PostgreSQL database.
pub(crate) struct Pg<'a> {
    pub(crate) effects: &'a dyn Effects,
    pub(crate) table: Table,
    pub(crate) db: Credential,
}

impl std::fmt::Debug for Pg<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pg")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

pub(crate) fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        Value::Record(r) => r.get(name),
        _ => None,
    }
}

pub(crate) fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(d) => d.to_f64(),
        Value::BigNumber(raw) => raw.parse().ok(),
        _ => None,
    }
}

fn rows(answer: &Value) -> Vec<Value> {
    match field(answer, "rows") {
        Some(Value::List(rows)) => rows.as_ref().clone(),
        _ => Vec::new(),
    }
}

fn command(answer: &Value) -> Option<String> {
    field(answer, "command").map(crate::template::as_text)
}

impl Pg<'_> {
    async fn sql(&self, sql: String, params: Vec<Option<String>>) -> Result<Value, String> {
        self.effects
            .sql(SqlRequest {
                credential: self.db.clone(),
                sql,
                params,
                read_only: false,
            })
            .await
    }

    /// The statements a search runs, as a test run shows them.
    pub(crate) fn search_sql(&self, filtered: bool) -> Vec<String> {
        vec![self.table.search(filtered)]
    }

    /// The statement that adds pieces, as a test run shows it.
    pub(crate) fn upsert_sql(&self) -> Vec<String> {
        vec![self.table.upsert()]
    }

    /// Makes the table and its indexes when they are not there; whether it made the table.
    pub(crate) async fn ensure(&self, dimensions: u32) -> Result<Got<bool>, String> {
        let table = &self.table;
        let (ask, names) = table.present();
        let answer = self
            .sql(ask.clone(), names.into_iter().map(Some).collect())
            .await?;
        let statements = table.create(dimensions);
        if shown(&answer) {
            let mut all = vec![ask];
            all.extend(statements);
            return Ok(Got::Shown(shown_result(&answer, &all, json!({}))));
        }
        let first = rows(&answer).into_iter().next().unwrap_or(Value::Null);
        let has = |name: &str| field(&first, name).is_some_and(|v| !v.is_null());
        let (has_table, has_index) = (has("kept_table"), has("kept_index"));
        let indexed = dimensions <= super::MAX_INDEXED;
        if has_table && (has_index || !indexed) {
            return Ok(Got::Done(false));
        }
        let mut extension_trouble = None;
        for (i, statement) in statements.into_iter().enumerate() {
            // The extension and the table only when the table is not there; its indexes whenever one is missing.
            if has_table && i < 2 {
                continue;
            }
            match self.sql(statement.clone(), Vec::new()).await {
                Ok(answer) if shown(&answer) => {
                    return Ok(Got::Shown(shown_result(&answer, &[statement], json!({}))));
                }
                Ok(_) => {}
                // Enabling pgvector may need rights a workflow's user lacks while a database owner has done it.
                Err(e) if i == 0 => extension_trouble = Some(e),
                Err(e) => {
                    let also = extension_trouble
                        .as_ref()
                        .map(|x| format!(" (pgvector could not be enabled: {x})"))
                        .unwrap_or_default();
                    return Err(format!("cannot make {}: {e}{also}", table.quoted()));
                }
            }
        }
        Ok(Got::Done(!has_table))
    }

    pub(crate) async fn upsert(&self, pieces: &[Piece]) -> Result<Got<u64>, String> {
        let mut added = 0;
        for batch in pieces.chunks(WRITE_BATCH) {
            let rows: Vec<Json> = batch
                .iter()
                .map(|p| {
                    Ok(json!({
                        "id": p.id, "source": p.source, "position": p.position, "content": p.text,
                        "metadata": p.metadata, "embedding": vector_text(&p.vector)?,
                    }))
                })
                .collect::<Result<_, String>>()?;
            let answer = self
                .sql(self.table.upsert(), vec![Some(Json::Array(rows).to_string())])
                .await?;
            if shown(&answer) {
                return Ok(Got::Shown(shown_result(&answer, &self.upsert_sql(), json!({}))));
            }
            added += rows_of(command(&answer).as_deref());
        }
        Ok(Got::Done(added))
    }

    pub(crate) async fn remove(
        &self,
        sources: &[&str],
        keep: Option<&[&str]>,
    ) -> Result<Got<Option<u64>>, String> {
        let (statement, params) = match keep {
            Some(ids) => (
                self.table.remove_stale(),
                vec![Some(json!(sources).to_string()), Some(json!(ids).to_string())],
            ),
            None => (
                self.table.delete_sources(),
                vec![Some(json!(sources).to_string())],
            ),
        };
        let answer = self.sql(statement.clone(), params).await?;
        // Removing what a document no longer has follows its pieces being written: a test run showed those already.
        if shown(&answer) && keep.is_none() {
            return Ok(Got::Shown(shown_result(&answer, &[statement], json!({}))));
        }
        Ok(Got::Done(Some(rows_of(command(&answer).as_deref()))))
    }

    pub(crate) async fn search(
        &self,
        vector: &[f64],
        k: usize,
        filter: Option<&Json>,
    ) -> Result<Got<Vec<Hit>>, String> {
        let statement = self.table.search(filter.is_some());
        let mut params = vec![Some(vector_text(vector)?)];
        params.extend(filter.map(|f| Some(f.to_string())));
        params.push(Some(k.to_string()));
        let answer = self.sql(statement.clone(), params).await?;
        if shown(&answer) {
            return Ok(Got::Shown(shown_result(&answer, &[statement], json!({}))));
        }
        Ok(Got::Done(
            rows(&answer)
                .iter()
                .map(|row| Hit {
                    id: field(row, "id").map(crate::template::as_text).unwrap_or_default(),
                    source: field(row, "source")
                        .map(crate::template::as_text)
                        .unwrap_or_default(),
                    position: field(row, "position").map_or(Json::Null, as_json),
                    text: field(row, "content")
                        .map(crate::template::as_text)
                        .unwrap_or_default(),
                    metadata: field(row, "metadata").map_or(Json::Null, as_json),
                    distance: number(field(row, "distance")).unwrap_or(f64::NAN),
                })
                .collect(),
        ))
    }
}
