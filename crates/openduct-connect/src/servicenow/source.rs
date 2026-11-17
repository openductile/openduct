// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: servicenow:` - the records of one table (`incident`, `change_request`, a custom `u_asset`) as they are
//! created and updated, read on a schedule through the Table API, in the order they were last updated
//! (`sys_updated_on`, then `sys_id`). The position is the time and id of the last record stored, so a restart
//! neither skips nor repeats one.

use std::time::{Duration, Instant};

use reqwest::Method;

use super::session::{Session, error_words};
use crate::json::json_string;
use crate::{SourceBatch, SourceMessage};

/// The most records read in one call.
const PAGE: usize = 1_000;

#[derive(Debug)]
pub struct ServiceNowSource {
    pub(super) shown: String,
    pub(super) session: Session,
    pub(super) table: String,
    pub(super) fields: Vec<String>,
    /// More of the encoded query, from the pipeline (`active=true^priority=1`).
    pub(super) query: Option<String>,
    pub(super) every: Duration,
    last_poll: Option<Instant>,
    more: bool,
}

/// A position: `<sys_updated_on>|<sys_id>`, checked before it goes into a query.
fn parse_position(p: &str) -> Option<(String, String)> {
    let (time, id) = p.split_once('|')?;
    let ok_time = chrono::NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S").is_ok();
    let ok_id = id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit());
    (ok_time && ok_id).then(|| (time.to_owned(), id.to_owned()))
}

fn encode(text: &str) -> String {
    percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
}

impl ServiceNowSource {
    pub(super) fn new(
        shown: String,
        session: Session,
        table: String,
        fields: Vec<String>,
        query: Option<String>,
        every: Duration,
    ) -> Self {
        Self {
            shown,
            session,
            table,
            fields,
            query,
            every,
            last_poll: None,
            more: false,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Sign in and ask for one record, so a wrong table, field or password is found before the first poll.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let path = format!(
            "/api/now/table/{}?sysparm_limit=1&sysparm_fields=sys_id",
            self.table
        );
        let (status, body) = self.session.call(Method::GET, &path, None).await?;
        if !status.is_success() {
            return Err(format!(
                "ServiceNow refused reading {}: {}",
                self.table,
                error_words(&body)
            ));
        }
        Ok(())
    }

    /// The encoded query: after the position (later, or as late and a greater id), with the pipeline's own
    /// conditions in both halves, ordered as the position is.
    fn encoded_query(&self, after: Option<&(String, String)>) -> String {
        let own = self.query.as_deref().map(|q| format!("{q}^")).unwrap_or_default();
        let filter = match after {
            Some((t, id)) => format!("{own}sys_updated_on>{t}^NQ{own}sys_updated_on={t}^sys_id>{id}"),
            None => self.query.clone().unwrap_or_default(),
        };
        let order = "ORDERBYsys_updated_on^ORDERBYsys_id";
        if filter.is_empty() {
            order.to_owned()
        } else {
            format!("{filter}^{order}")
        }
    }

    pub(crate) async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        if let (Some(last), false) = (self.last_poll, self.more) {
            tokio::time::sleep_until((last + self.every).into()).await;
        }
        self.last_poll = Some(Instant::now());
        let after = position.and_then(parse_position);
        let mut path = format!(
            "/api/now/table/{}?sysparm_query={}&sysparm_limit={PAGE}&sysparm_display_value=false&sysparm_exclude_reference_link=true",
            self.table,
            encode(&self.encoded_query(after.as_ref()))
        );
        if !self.fields.is_empty() {
            path.push_str(&format!("&sysparm_fields={}", encode(&self.fields.join(","))));
        }
        let (status, body) = self.session.call(Method::GET, &path, None).await?;
        if !status.is_success() {
            return Err(format!(
                "ServiceNow refused reading {}: {}",
                self.table,
                error_words(&body)
            ));
        }
        let answer: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| "ServiceNow answered with something else".to_owned())?;
        let records = answer
            .get("result")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        self.more = records.len() >= PAGE;
        let mut messages = Vec::with_capacity(records.len());
        let mut last = None;
        for record in records {
            let id = record
                .get("sys_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let updated = record
                .get("sys_updated_on")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let value = format!(
                "{{\"op\":\"upsert\",\"table\":{},\"before\":null,\"after\":{record}}}",
                json_string(&self.table)
            );
            messages.push(SourceMessage {
                key: Some(format!("{}/{id}", self.table).into_bytes()),
                value: Some(value.into_bytes()),
                headers: vec![("servicenow-table".to_owned(), self.table.clone().into_bytes())],
                ack: None,
            });
            if parse_position(&format!("{updated}|{id}")).is_some() {
                last = Some(format!("{updated}|{id}"));
            }
        }
        Ok(SourceBatch {
            messages,
            position: last,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn positions_are_a_time_and_an_id_and_cannot_change_the_query() {
        let id = "9d385017c611228701d22104cc95c371";
        assert_eq!(
            parse_position(&format!("2026-11-15 09:46:40|{id}")).unwrap().1,
            id
        );
        for bad in [
            format!("2026-11-15 09:46:40|{id}^ORactive=false"),
            format!("2026-11-15T09:46:40Z|{id}"),
            "2026-11-15 09:46:40".to_owned(),
        ] {
            assert_eq!(parse_position(&bad), None, "{bad}");
        }
    }
}
