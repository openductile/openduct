// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: salesforce:` - records written to one object through the sObject Collections API, 200 a call: upserted by
//! an external id (so writing a message again changes the same record), inserted, or updated by `Id`. A record
//! Salesforce refuses (a required field missing, a value too long, a duplicate) is set aside with Salesforce's
//! reason; the rest are written.

use std::collections::BTreeMap;

use reqwest::Method;

use super::session::{Session, error_words};
use crate::{SinkOutcome, SinkRecord};

/// The most records in one call.
const PER_CALL: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Upsert,
    Insert,
    Update,
}

#[derive(Debug)]
pub struct SalesforceSink {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) object: String,
    pub(super) mode: Mode,
    pub(super) external_id: Option<String>,
}

impl SalesforceSink {
    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        let mut rejected = Vec::new();
        let mut ready: Vec<(SinkRecord, serde_json::Value)> = Vec::new();
        for record in records {
            match self.body(&record) {
                Ok(fields) => ready.push((record, fields)),
                Err(why) => rejected.push((record, why)),
            }
        }
        for chunk in ready.chunks(PER_CALL) {
            let body = serde_json::json!({
                "allOrNone": false,
                "records": chunk.iter().map(|(_, f)| f.clone()).collect::<Vec<_>>(),
            });
            let (method, path) = match self.mode {
                Mode::Upsert => (
                    Method::PATCH,
                    self.session.data_path(&format!(
                        "composite/sobjects/{}/{}",
                        self.object,
                        self.external_id.as_deref().unwrap_or("Id")
                    )),
                ),
                Mode::Insert => (Method::POST, self.session.data_path("composite/sobjects")),
                Mode::Update => (Method::PATCH, self.session.data_path("composite/sobjects")),
            };
            let (status, answer) = self.session.call(method, &path, Some(&body)).await?;
            if !status.is_success() {
                return Err(format!(
                    "Salesforce refused writing to {}: {}",
                    self.object,
                    error_words(&answer)
                ));
            }
            let results: Vec<serde_json::Value> = serde_json::from_slice(&answer)
                .map_err(|_| "Salesforce answered the write with something else".to_owned())?;
            if results.len() != chunk.len() {
                return Err(format!(
                    "Salesforce answered for {} records of the {} written",
                    results.len(),
                    chunk.len()
                ));
            }
            for ((record, _), result) in chunk.iter().zip(results) {
                if result.get("success").and_then(serde_json::Value::as_bool) != Some(true) {
                    let errors = result
                        .get("errors")
                        .map(|e| error_words(e.to_string().as_bytes()))
                        .unwrap_or_default();
                    rejected.push((record.clone(), format!("Salesforce refused the record: {errors}")));
                }
            }
        }
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    /// The record as Salesforce takes it: the message's fields, with the object's type.
    fn body(&self, record: &SinkRecord) -> Result<serde_json::Value, String> {
        let value: serde_json::Value =
            serde_json::from_slice(&record.value).map_err(|_| "the message is not JSON".to_owned())?;
        let serde_json::Value::Object(mut fields) = value else {
            return Err("the message is not a JSON object of fields".to_owned());
        };
        match (self.mode, &self.external_id) {
            (Mode::Upsert, Some(id)) if !fields.contains_key(id) => {
                return Err(format!(
                    "the message has no {id}, the external id it is upserted by"
                ));
            }
            (Mode::Update, _) if !fields.contains_key("Id") => {
                return Err("the message has no Id, which an update needs".to_owned());
            }
            _ => {}
        }
        fields.insert(
            "attributes".to_owned(),
            serde_json::json!({ "type": self.object }),
        );
        Ok(serde_json::Value::Object(fields))
    }
}
