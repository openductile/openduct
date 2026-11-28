// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The feed trigger: a run for each new item of an RSS, Atom or JSON feed (`trigger: {feed: {url, every}}`). Its
//! polls are scheduled as an app's are - every node queues the times, the one member that holds the schedule
//! claims each - and each poll keeps what it has seen beside the runs it starts, in one batch, so neither a restart
//! nor a node's loss runs an item twice or passes one by. The first poll only notes what the feed holds: the
//! trigger starts from now. A feed that has not changed since (its ETag or Last-Modified) answers with nothing.
//! New items run oldest first, at most `max_items` a poll; the rest at the next.

use std::collections::HashSet;
use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Map, Value as Json, json};

use super::fetch::{Ask, Limits, get};
use super::{ACCEPT_FEED, Reach, USER_AGENT};
use crate::app_files::poll::{MOST_SEEN, Polled, Seen};
use crate::run::Effects;

/// How often a feed is asked, by default and at least; at most once a week.
const EVERY: Duration = Duration::from_secs(15 * 60);
const LEAST_EVERY: Duration = Duration::from_secs(60);
const MOST_EVERY: Duration = Duration::from_secs(7 * 86_400);
/// The most new items one poll runs, by default and at most.
const POLL_ITEMS: usize = 50;
const MOST_POLL_ITEMS: usize = 500;

/// A feed trigger, as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedTrigger {
    pub(crate) url: String,
    pub(crate) every: Duration,
    pub(crate) agent: Option<String>,
    pub(crate) max_items: usize,
}

impl FeedTrigger {
    /// How often the feed is asked.
    #[must_use]
    pub fn every(&self) -> Duration {
        self.every
    }

    /// The feed's address.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// `trigger: {feed: {url, every, user_agent, max_items}}`.
pub(crate) fn read(s: &Map<String, Json>) -> Result<crate::model::Trigger, String> {
    if let Some(k) = s
        .keys()
        .find(|k| !matches!(k.as_str(), "url" | "every" | "user_agent" | "max_items"))
    {
        return Err(format!(
            "feed.{k}: not a setting of feed (url, every, user_agent, max_items)"
        ));
    }
    let url = s
        .get("url")
        .and_then(Json::as_str)
        .ok_or("feed.url: the feed's address, as https://example.com/feed.xml")?;
    let url = super::fetch::checked(url, true).map_err(|e| format!("feed.url: {e}"))?;
    let every = match s.get("every").and_then(Json::as_str) {
        None => EVERY,
        Some(t) => crate::model::duration(t)
            .filter(|d| *d >= LEAST_EVERY && *d <= MOST_EVERY)
            .ok_or_else(|| {
                format!("feed.every: how often, as 15m or 1h - at least 1m, at most 7d (not {t:?})")
            })?,
    };
    let agent = match s.get("user_agent") {
        None | Some(Json::Null) => None,
        Some(Json::String(a)) if a.len() <= 300 && a.chars().all(|c| (' '..='~').contains(&c)) => {
            Some(a.trim().to_owned()).filter(|a| !a.is_empty())
        }
        Some(_) => return Err("feed.user_agent: at most 300 characters of ASCII".to_owned()),
    };
    let max_items = match s.get("max_items") {
        None | Some(Json::Null) => POLL_ITEMS,
        Some(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| (1..=MOST_POLL_ITEMS).contains(n))
            .ok_or_else(|| format!("feed.max_items: 1 to {MOST_POLL_ITEMS} new items a poll"))?,
    };
    Ok(crate::model::Trigger::Feed(Box::new(FeedTrigger {
        url: url.to_string(),
        every,
        agent,
        max_items,
    })))
}

/// One poll of the feed, after what the polls `before` saw (none: the first poll, which runs nothing).
pub(crate) async fn poll(
    effects: &dyn Effects,
    t: &FeedTrigger,
    before: Option<&Seen>,
) -> Result<Polled, String> {
    let Reach::Live(net) = effects.web() else {
        return Err("this node reaches no web: the feed cannot be read".to_owned());
    };
    let text_of = |k: &str| {
        before
            .and_then(|b| b.cursor.get(k))
            .and_then(Json::as_str)
            .map(str::to_owned)
    };
    let since = before.map(|_| (text_of("etag"), text_of("modified")));
    let agent = t.agent.as_deref().unwrap_or(USER_AGENT);
    let ask = Ask {
        url: &t.url,
        agent,
        headers: &[],
        accept: ACCEPT_FEED,
        limits: Limits {
            max_bytes: super::MOST_PAGE_BYTES,
            timeout: Duration::from_secs(60),
            max_redirects: super::REDIRECTS,
        },
        since,
    };
    let got = get(net, &ask).await?;
    if got.status == 304
        && let Some(b) = before
    {
        let mut seen = b.clone();
        seen.failing = None;
        return Ok(Polled {
            runs: Vec::new(),
            seen,
        });
    }
    let cursor = json!({ "etag": got.etag, "modified": got.modified });
    let answer = super::feed_answer(Some(net), &ask, got, super::feed::MOST_FEED_ITEMS).await?;
    let feed = json!({
        "title": answer.get("title"),
        "link": answer.get("link"),
        "url": answer.get("url"),
    });
    let items: Vec<Json> = answer
        .get("items")
        .and_then(Json::as_array)
        .cloned()
        .unwrap_or_default();
    let id_of = |i: &Json| i.get("id").and_then(Json::as_str).unwrap_or_default().to_owned();
    let Some(before) = before else {
        // The first poll: what is there now is seen, and nothing runs.
        let mut ids: Vec<(String, String)> = items.iter().rev().map(|i| (id_of(i), String::new())).collect();
        ids.dedup();
        return Ok(Polled {
            runs: Vec::new(),
            seen: Seen {
                cursor,
                items: ids,
                ..Seen::default()
            },
        });
    };
    let known: HashSet<&str> = before.items.iter().map(|(id, _)| id.as_str()).collect();
    // Feeds give their newest first: the new ones run oldest first.
    let mut fresh: Vec<&Json> = items
        .iter()
        .rev()
        .filter(|i| !id_of(i).is_empty() && !known.contains(id_of(i).as_str()))
        .collect();
    let mut taken_ids = HashSet::new();
    fresh.retain(|i| taken_ids.insert(id_of(i)));
    let more = fresh.len() > t.max_items;
    fresh.truncate(t.max_items);
    let mut seen = before.clone();
    // With new items left for the next poll, it asks for the whole feed again, not "only if changed".
    seen.cursor = if more { json!({}) } else { cursor };
    seen.failing = None;
    seen.pending = false;
    let mut runs = Vec::with_capacity(fresh.len());
    for item in fresh {
        let id = id_of(item);
        seen.items.push((id.clone(), String::new()));
        let mut value = item.clone();
        if let Some(record) = value.as_object_mut() {
            record.insert("feed".to_owned(), feed.clone());
        }
        runs.push((id, Value::from_json(&value)));
    }
    if seen.items.len() > MOST_SEEN {
        let extra = seen.items.len() - MOST_SEEN;
        seen.items.drain(..extra);
    }
    Ok(Polled { runs, seen })
}
