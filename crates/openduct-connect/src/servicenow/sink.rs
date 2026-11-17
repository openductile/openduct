// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `to: servicenow:` - each message written as a record of one table through the Table API: inserted, updated by
//! its `sys_id`, or upserted by a field of the pipeline's choosing (`correlation_id`, `number`), so writing a message
//! again changes the same record. A record ServiceNow refuses is set aside with ServiceNow's reason.

use std::collections::BTreeMap;

use reqwest::{Method, StatusCode};

use super::session::{Session, error_words};
use crate::{SinkOutcome, SinkRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Insert,
    Update,
    Upsert,
}

#[derive(Debug)]
pub struct ServiceNowSink {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) table: String,
    pub(super) mode: Mode,
    /// The field an upsert finds the record by.
    pub(super) match_on: Option<String>,
}

/// Whether ServiceNow refused this one record, so it is set aside, rather than the pipeline's settings, which stop
/// the pipeline. ServiceNow answers 403 both for a data policy or a business rule stopping one record and for a
/// user not allowed to write (an ACL), and tells them apart only in its words; a 403 that is not plainly the
/// record's stops the pipeline. A table that is not there, or still too busy after the waits, stops it too.
fn this_record(status: StatusCode, words: &str) -> bool {
    if !status.is_client_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || words.starts_with("Invalid table")
    {
        return false;
    }
    status != StatusCode::FORBIDDEN
        || words.contains("Data Policy Exception")
        || words.contains("aborted by Business Rule")
}

enum Written {
    Stored,
    Refused(String),
}

impl ServiceNowSink {
    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        let mut rejected = Vec::new();
        for record in records {
            match self.write(&record).await? {
                Written::Stored => {}
                Written::Refused(why) => rejected.push((record, why)),
            }
        }
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    async fn write(&mut self, record: &SinkRecord) -> Result<Written, String> {
        let Ok(serde_json::Value::Object(fields)) =
            serde_json::from_slice::<serde_json::Value>(&record.value)
        else {
            return Ok(Written::Refused(
                "the message is not a JSON object of fields".to_owned(),
            ));
        };
        let existing = match self.mode {
            Mode::Insert => None,
            Mode::Update => match fields.get("sys_id").and_then(|v| v.as_str()) {
                Some(id) if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) => {
                    Some(id.to_owned())
                }
                _ => {
                    return Ok(Written::Refused(
                        "the message has no sys_id, which an update needs".to_owned(),
                    ));
                }
            },
            Mode::Upsert => {
                let field = self.match_on.clone().unwrap_or_default();
                let value = match fields.get(&field) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(v @ (serde_json::Value::Number(_) | serde_json::Value::Bool(_))) => v.to_string(),
                    _ => {
                        return Ok(Written::Refused(format!(
                            "the message has no {field}, which it is matched by"
                        )));
                    }
                };
                if value.contains(['^', '\n', '\r']) || value.is_empty() {
                    return Ok(Written::Refused(format!(
                        "its {field} cannot be matched in a ServiceNow query"
                    )));
                }
                self.find(&field, &value).await?
            }
        };
        let body = serde_json::Value::Object(fields);
        let (method, path) = match &existing {
            Some(id) => (Method::PATCH, format!("/api/now/table/{}/{id}", self.table)),
            None => (Method::POST, format!("/api/now/table/{}", self.table)),
        };
        let (status, answer) = self
            .session
            .call(
                method,
                &format!("{path}?sysparm_exclude_reference_link=true"),
                Some(&body),
            )
            .await?;
        if status.is_success() {
            return Ok(Written::Stored);
        }
        let words = error_words(&answer);
        if this_record(status, &words) {
            return Ok(Written::Refused(format!(
                "ServiceNow refused the record: {words}"
            )));
        }
        Err(format!(
            "ServiceNow refused writing to {}: {words} ({status})",
            self.table
        ))
    }

    /// The `sys_id` of the record whose `field` is `value`, if there is one.
    async fn find(&mut self, field: &str, value: &str) -> Result<Option<String>, String> {
        let query = percent_encoding::utf8_percent_encode(
            &format!("{field}={value}"),
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        let path = format!(
            "/api/now/table/{}?sysparm_query={query}&sysparm_limit=1&sysparm_fields=sys_id",
            self.table
        );
        let (status, body) = self.session.call(Method::GET, &path, None).await?;
        if !status.is_success() {
            return Err(format!(
                "ServiceNow refused looking up {field} in {}: {}",
                self.table,
                error_words(&body)
            ));
        }
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "ServiceNow answered with something else".to_owned())?;
        Ok(v.get("result")
            .and_then(|r| r.as_array())
            .and_then(|r| r.first())
            .and_then(|r| r.get("sys_id"))
            .and_then(|id| id.as_str())
            .map(str::to_owned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refused_record_is_told_apart_from_settings_that_cannot_work() {
        let record = [
            (
                StatusCode::FORBIDDEN,
                "Operation Failed: Data Policy Exception: Short description is mandatory",
            ),
            (
                StatusCode::BAD_REQUEST,
                "Exception while reading request: The payload is not valid JSON",
            ),
            (
                StatusCode::NOT_FOUND,
                "No Record found: Record doesn't exist or ACL restricts the record retrieval",
            ),
        ];
        for (status, words) in record {
            assert!(this_record(status, words), "{words}");
        }
        assert!(this_record(
            StatusCode::FORBIDDEN,
            "Operation Failed: Operation against file 'incident' was aborted by Business Rule 'Check CI^9d38'"
        ));
        let settings = [
            (
                StatusCode::FORBIDDEN,
                "Operation Failed: ACL Exception Insert Failed due to security constraints",
            ),
            (
                StatusCode::FORBIDDEN,
                "User Not Authorized: Not permitted to create a record in incident",
            ),
            (StatusCode::BAD_REQUEST, "Invalid table incidnet"),
            (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded"),
        ];
        for (status, words) in settings {
            assert!(!this_record(status, words), "{words}");
        }
    }
}
