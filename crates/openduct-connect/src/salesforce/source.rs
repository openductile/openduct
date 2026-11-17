// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: salesforce:` - the records of one object (Account, Opportunity, a custom `Invoice__c`) as they are
//! created, changed and deleted, read on a schedule with SOQL through `queryAll` (which includes the recycle bin,
//! so deletions are seen). Records come in the order Salesforce last changed them, and the pipeline's position is
//! the time and id of the last record stored, so a restart neither skips nor repeats one.

use std::time::{Duration, Instant};

use reqwest::Method;

use super::session::{Session, error_words};
use crate::json::json_string;
use crate::{SourceBatch, SourceMessage};

/// The most records read in one call: Salesforce's own page size.
const PAGE: usize = 2_000;

#[derive(Debug)]
pub struct SalesforceSource {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) object: String,
    /// The fields to read; empty for every field the object has.
    pub(super) fields: Vec<String>,
    /// More of a SOQL `WHERE`, from the pipeline.
    pub(super) filter: Option<String>,
    pub(super) every: Duration,
    last_poll: Option<Instant>,
    /// The last call gave a whole page: read again at once.
    more: bool,
}

/// A position: the `SystemModstamp` and `Id` of the last record stored.
fn parse_position(p: &str) -> Option<(String, String)> {
    let (stamp, id) = p.split_once('|')?;
    let ok_stamp = stamp.len() == 20
        && stamp
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b':' | b'T' | b'Z'));
    let ok_id = (15..=18).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_alphanumeric());
    (ok_stamp && ok_id).then(|| (stamp.to_owned(), id.to_owned()))
}

/// `2026-11-15T09:46:40.000+0000` as SOQL takes a time: `2026-11-15T09:46:40Z` (Salesforce keeps whole seconds).
fn soql_time(stamp: &str) -> Option<String> {
    let base = stamp.get(..19)?;
    chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S").ok()?;
    Some(format!("{base}Z"))
}

impl SalesforceSource {
    pub(super) fn new(
        shown: String,
        session: Session,
        object: String,
        fields: Vec<String>,
        filter: Option<String>,
        every: Duration,
    ) -> Self {
        Self {
            shown,
            session,
            object,
            fields,
            filter,
            every,
            last_poll: None,
            more: false,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Sign in and learn the object's fields, so a mistake is found before the first poll.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let path = self
            .session
            .data_path(&format!("sobjects/{}/describe", self.object));
        let (status, body) = self.session.call(Method::GET, &path, None).await?;
        if !status.is_success() {
            return Err(format!(
                "Salesforce refused describing {}: {}",
                self.object,
                error_words(&body)
            ));
        }
        let described: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "Salesforce described the object in a way not known".to_owned())?;
        let all: Vec<(String, String)> = described
            .get("fields")
            .and_then(|f| f.as_array())
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|f| {
                        Some((
                            f.get("name")?.as_str()?.to_owned(),
                            f.get("type")?.as_str()?.to_owned(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        if self.fields.is_empty() {
            // Files' contents (base64) are read one record at a time only, so they are left out.
            self.fields = all
                .iter()
                .filter(|(_, t)| t != "base64")
                .map(|(n, _)| n.clone())
                .collect();
        } else if let Some(missing) = self
            .fields
            .iter()
            .find(|f| !all.iter().any(|(n, _)| n.eq_ignore_ascii_case(f)))
        {
            return Err(format!("Salesforce's {} has no field {missing}", self.object));
        }
        for needed in ["Id", "SystemModstamp", "IsDeleted"] {
            if !self.fields.iter().any(|f| f.eq_ignore_ascii_case(needed)) {
                self.fields.push(needed.to_owned());
            }
        }
        Ok(())
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        if let (Some(last), false) = (self.last_poll, self.more) {
            tokio::time::sleep_until((last + self.every).into()).await;
        }
        self.last_poll = Some(Instant::now());
        let after = position.and_then(parse_position);
        let mut conditions = Vec::new();
        if let Some((stamp, id)) = &after {
            let t = soql_time(stamp).ok_or("the saved position is not a time")?;
            conditions.push(format!(
                "(SystemModstamp > {t} OR (SystemModstamp = {t} AND Id > '{id}'))"
            ));
        }
        if let Some(f) = &self.filter {
            conditions.push(format!("({f})"));
        }
        let soql = format!(
            "SELECT {} FROM {}{} ORDER BY SystemModstamp, Id LIMIT {PAGE}",
            self.fields.join(", "),
            self.object,
            if conditions.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", conditions.join(" AND "))
            }
        );
        let query: String = url_encode(&soql);
        let path = format!("{}?q={query}", self.session.data_path("queryAll"));
        let (status, body) = self.session.call(Method::GET, &path, None).await?;
        if !status.is_success() {
            return Err(format!(
                "Salesforce refused the query of {}: {}",
                self.object,
                error_words(&body)
            ));
        }
        let answer: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "Salesforce answered the query with something else".to_owned())?;
        let records = answer
            .get("records")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        self.more = records.len() >= PAGE;
        let mut messages = Vec::with_capacity(records.len());
        let mut last = None;
        for mut record in records {
            if let Some(o) = record.as_object_mut() {
                o.remove("attributes");
            }
            let id = record
                .get("Id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let stamp = record
                .get("SystemModstamp")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let deleted = record
                .get("IsDeleted")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let (op, before, after) = if deleted {
                ("delete", record.to_string(), "null".to_owned())
            } else {
                ("upsert", "null".to_owned(), record.to_string())
            };
            let value = format!(
                "{{\"op\":{},\"table\":{},\"before\":{before},\"after\":{after}}}",
                json_string(op),
                json_string(&self.object)
            );
            messages.push(SourceMessage {
                key: Some(format!("{}/{id}", self.object).into_bytes()),
                value: Some(value.into_bytes()),
                headers: vec![
                    ("salesforce-object".to_owned(), self.object.clone().into_bytes()),
                    ("salesforce-op".to_owned(), op.as_bytes().to_vec()),
                ],
                ack: None,
            });
            if let Some(t) = stamp.get(..19) {
                last = Some(format!("{t}Z|{id}"));
            }
        }
        Ok(SourceBatch {
            messages,
            position: last,
        })
    }
}

fn url_encode(text: &str) -> String {
    percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn positions_are_a_time_and_an_id() {
        assert_eq!(
            parse_position("2026-11-15T09:46:40Z|001Hp00002AbCdEIAV"),
            Some(("2026-11-15T09:46:40Z".to_owned(), "001Hp00002AbCdEIAV".to_owned()))
        );
        // A position written by hand into SOQL must not be able to change the query.
        for bad in [
            "2026-11-15T09:46:40Z|001' OR Name != '",
            "x|001Hp00002AbCdEIAV",
            "2026-11-15T09:46:40Z",
        ] {
            assert_eq!(parse_position(bad), None, "{bad}");
        }
        assert_eq!(
            soql_time("2026-11-15T09:46:40.000+0000").unwrap(),
            "2026-11-15T09:46:40Z"
        );
        assert_eq!(soql_time("2026-11-15T09:46:40Z").unwrap(), "2026-11-15T09:46:40Z");
        assert_eq!(soql_time("yesterday"), None);
    }
}
