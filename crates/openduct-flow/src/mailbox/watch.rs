// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `mailbox` trigger: a run for each new message in a folder of any mailbox, over IMAP.
//!
//! Its polls are a schedule's times, as an app's or a folder's are: each queued by every node and claimed by the one
//! that reads the schedule's partition, which asks the server for the messages after the last one it dealt with and
//! writes each one's run together with what it has seen, in one batch - both kept, or neither (see
//! [`crate::runner`]). So a cluster asks once each time, and a restart neither runs a message twice nor passes one
//! by. What is seen is the folder's UIDVALIDITY and the highest UID dealt with: IMAP numbers a folder's messages in
//! the order they arrive, so the next poll asks for those above it. A folder rebuilt (its UIDVALIDITY changed) starts
//! again from now. The first poll notes where the folder is and runs nothing (unless `start: existing`).
//!
//! With `idle: true`, a server that offers IDLE (RFC 2177) is also listened to between polls, on one node, and a
//! message arriving asks for a poll at once ([`super::idle`]): the poll stays what runs messages, so the waiting adds
//! speed and nothing else.

use std::time::Duration;

use serde_json::{Map, Value as Json, json};

use super::imap::{self, Session};
use super::wire::{self, Arg};
use crate::app_files::poll::{Polled, Seen};
use crate::run::Effects;

/// How often a folder is asked, by default and at the least; how many messages one poll runs, by default and at most.
const EVERY: Duration = Duration::from_secs(300);
const LEAST: Duration = Duration::from_secs(60);
const POLL_ITEMS: i64 = 50;
const MOST_POLL_ITEMS: i64 = 500;
/// A new message larger than this is given its headers alone (read it whole with a mailbox step).
const LARGEST_READ: u64 = 4 * 1024 * 1024;
/// The most text and HTML a run is given of each message.
const RUN_TEXT: usize = 100 * 1024;

/// What starts a workflow: new messages in a folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailWatch {
    pub credential: String,
    pub folder: String,
    pub unseen: bool,
    pub from: Option<String>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub every: Duration,
    pub max_items: usize,
    /// The first poll runs the messages already there.
    pub existing: bool,
    /// The server is listened to between polls, where it offers IDLE.
    pub idle: bool,
    /// Each new message is read for its text and HTML (by default); otherwise a run gets its headers alone.
    pub read: bool,
}

const SETTINGS: &[&str] = &[
    "credential",
    "folder",
    "unseen",
    "from",
    "to",
    "subject",
    "every",
    "max_items",
    "start",
    "idle",
    "read",
];

/// A workflow's trigger, from a mailbox trigger's settings.
pub(crate) fn trigger(s: &Map<String, Json>) -> Result<crate::Trigger, String> {
    Ok(crate::Trigger::Mailbox(Box::new(parse(s)?)))
}

/// A mailbox trigger's settings, read.
pub(crate) fn parse(s: &Map<String, Json>) -> Result<MailWatch, String> {
    if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
        return Err(format!(
            "mailbox.{k}: not a setting of mailbox ({})",
            SETTINGS.join(", ")
        ));
    }
    let text = |name: &str| -> Result<Option<String>, String> {
        match s.get(name) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(t)) if t.contains("{{") => Err(format!(
                "mailbox.{name}: a trigger's settings are values: there is no event yet to work them out of"
            )),
            Some(Json::String(t)) if t.len() > 1000 => {
                Err(format!("mailbox.{name}: at most 1000 characters"))
            }
            Some(Json::String(t)) => {
                wire::safe(t, &format!("mailbox.{name}"))?;
                Ok(Some(t.trim().to_owned()).filter(|t| !t.is_empty()))
            }
            Some(_) => Err(format!("mailbox.{name}: text")),
        }
    };
    let flag = |name: &str, default: bool| match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("mailbox.{name}: true or false")),
    };
    let credential = text("credential")?.ok_or("mailbox.credential: the mailbox's imap credential")?;
    let every = match text("every")? {
        None => EVERY,
        Some(e) => crate::duration(&e)
            .filter(|d| *d >= LEAST && *d <= Duration::from_secs(86_400))
            .ok_or("mailbox.every: how often, as 5m or 1h - from 1m to 1d")?,
    };
    let max_items = match s.get("max_items") {
        None | Some(Json::Null) => POLL_ITEMS,
        Some(n) => n
            .as_i64()
            .filter(|n| (1..=MOST_POLL_ITEMS).contains(n))
            .ok_or_else(|| format!("mailbox.max_items: 1 to {MOST_POLL_ITEMS}"))?,
    };
    let existing = match text("start")?.as_deref() {
        None | Some("now") => false,
        Some("existing") => true,
        Some(other) => return Err(format!("mailbox.start: now or existing, not {other:?}")),
    };
    Ok(MailWatch {
        credential,
        folder: text("folder")?.unwrap_or_else(|| "INBOX".to_owned()),
        unseen: flag("unseen", false)?,
        from: text("from")?,
        to: text("to")?,
        subject: text("subject")?,
        every,
        max_items: usize::try_from(max_items).unwrap_or(50),
        existing,
        idle: flag("idle", false)?,
        read: flag("read", true)?,
    })
}

/// Where a folder's polls are: the folder as it was (its UIDVALIDITY) and the highest UID dealt with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursor {
    pub uidvalidity: u64,
    pub uid: u64,
}

impl Cursor {
    fn of(seen: Option<&Seen>) -> Option<Self> {
        let c = &seen?.cursor;
        Some(Self {
            uidvalidity: c.get("uidvalidity")?.as_u64()?,
            uid: c.get("uid")?.as_u64()?,
        })
    }

    fn json(self) -> Json {
        json!({ "uidvalidity": self.uidvalidity, "uid": self.uid })
    }
}

/// The search for the messages after `after` that meet the trigger's criteria.
fn criteria(w: &MailWatch, after: u64) -> Vec<Arg> {
    let mut args = vec![Arg::Raw(format!("UID {}:*", after + 1))];
    if w.unseen {
        args.push(Arg::Raw("UNSEEN".into()));
    }
    for (key, v) in [("FROM", &w.from), ("TO", &w.to), ("SUBJECT", &w.subject)] {
        if let Some(v) = v {
            args.push(Arg::Raw(key.into()));
            args.push(Arg::Text(v.clone()));
        }
    }
    args
}

/// Which UIDs to run, given those found and where the polls before got to: above it, oldest first, the most a poll
/// takes; and the cursor after them.
pub(crate) fn next(found: &[u64], after: u64, most: usize) -> (Vec<u64>, u64) {
    let mut fresh: Vec<u64> = found.iter().copied().filter(|u| *u > after).collect();
    fresh.sort_unstable();
    fresh.truncate(most);
    let to = fresh.last().copied().unwrap_or(after);
    (fresh, to)
}

/// One poll of the folder, through a real run's effects.
pub(crate) async fn poll(
    effects: &dyn Effects,
    w: &MailWatch,
    before: Option<&Seen>,
    slot: i64,
) -> Result<Polled, String> {
    let places = effects
        .storage()
        .allows(false)
        .map_err(|_| "this run reaches no mailbox".to_owned())?
        .clone();
    let credential = effects.credential(&w.credential)?;
    if credential.kind != "imap" {
        return Err(format!(
            "{} is a {} credential: a mailbox trigger takes an imap one",
            w.credential, credential.kind
        ));
    }
    let mut session = super::open(effects, &w.credential, &credential, &places).await?;
    let polled = poll_in(&mut session, w, before, slot).await;
    session.logout().await;
    polled
}

async fn poll_in(s: &mut Session, w: &MailWatch, before: Option<&Seen>, slot: i64) -> Result<Polled, String> {
    let selected = s.select(&w.folder, false).await?;
    let was = Cursor::of(before.filter(|b| !b.pending));
    let same_folder = was.is_some_and(|c| c.uidvalidity == selected.uidvalidity);
    let seen = |uid: u64| Seen {
        cursor: Cursor {
            uidvalidity: selected.uidvalidity,
            uid,
        }
        .json(),
        items: Vec::new(),
        slot,
        failing: None,
        pending: false,
    };
    if !same_folder && !w.existing {
        // From now: where the folder is noted, nothing run.
        let top = match selected.uidnext {
            Some(n) => n.saturating_sub(1),
            None => s
                .search(&[Arg::Raw("UID *".into())])
                .await?
                .last()
                .copied()
                .unwrap_or(0),
        };
        return Ok(Polled {
            runs: Vec::new(),
            seen: seen(top),
        });
    }
    let after = if same_folder { was.map_or(0, |c| c.uid) } else { 0 };
    let found = s.search(&criteria(w, after)).await?;
    let (fresh, to) = next(&found, after, w.max_items);
    let mut runs = Vec::with_capacity(fresh.len());
    let sizes = s.fetch(&fresh, super::LIST_FETCH).await?;
    for uid in &fresh {
        let Some(f) = sizes.iter().find(|f| f.uid() == Some(*uid)) else {
            continue;
        };
        let size = f.get("RFC822.SIZE").and_then(wire::Item::number).unwrap_or(0);
        let headers = super::mime::headers(f.body().unwrap_or_default());
        let mut event = super::summary(&w.folder, *uid, &headers, &f.flags(), size);
        if let Some(m) = event.as_object_mut() {
            m.insert("uidvalidity".to_owned(), json!(selected.uidvalidity));
        }
        if w.read {
            read_into(s, *uid, size, &mut event).await?;
        }
        runs.push((
            format!("{}:{uid}", selected.uidvalidity),
            openduct_expr::Value::from_json(&event),
        ));
    }
    Ok(Polled { runs, seen: seen(to) })
}

/// A new message's text, HTML and its attachments' names, for its run (the files themselves come from a read).
async fn read_into(s: &mut Session, uid: u64, size: u64, event: &mut Json) -> Result<(), String> {
    let Some(m) = event.as_object_mut() else {
        return Ok(());
    };
    if size > LARGEST_READ {
        m.insert("text".to_owned(), Json::String(String::new()));
        m.insert("html".to_owned(), Json::String(String::new()));
        m.insert("attachments".to_owned(), json!([]));
        m.insert("cut".to_owned(), Json::Bool(true));
        return Ok(());
    }
    let fetched = s.fetch(&[uid], "(UID BODY.PEEK[])").await?;
    let raw = fetched
        .iter()
        .find(|f| f.uid() == Some(uid))
        .and_then(imap::Fetched::body)
        .unwrap_or_default();
    let message = super::mime::parse(raw);
    let (text, a) = super::cut(message.text.unwrap_or_default(), RUN_TEXT);
    let (html, b) = super::cut(message.html.unwrap_or_default(), RUN_TEXT);
    let files: Vec<Json> = message
        .attachments
        .iter()
        .take(100)
        .map(|f| json!({ "name": f.name, "type": f.media_type, "size": f.bytes.len(), "inline": f.inline }))
        .collect();
    m.insert("text".to_owned(), Json::String(text));
    m.insert("html".to_owned(), Json::String(html));
    m.insert("attachments".to_owned(), Json::Array(files));
    if a || b {
        m.insert("cut".to_owned(), Json::Bool(true));
    }
    Ok(())
}

/// What a test run of a workflow this trigger starts is given, when it gives no input of its own.
pub fn sample(w: &MailWatch) -> Json {
    let mut m = super::sample_message(&w.folder, 1001);
    if let Some(o) = m.as_object_mut() {
        o.insert("uidvalidity".to_owned(), json!(1));
        if w.read {
            o.insert(
                "text".to_owned(),
                Json::String(
                    "Hello,\n\nPlease find invoice A-1001 attached: 120.00, due 2026-12-23.\n\nAnn\n".into(),
                ),
            );
            o.insert("html".to_owned(), Json::String(String::new()));
            o.insert(
                "attachments".to_owned(),
                json!([{ "name": "invoice-A-1001.csv", "type": "text/csv", "size": 43, "inline": false }]),
            );
        }
    }
    m
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn new_messages_are_those_above_the_cursor_oldest_first() {
        // `UID 10:*` answers the highest message even when there is none above 9: it is not taken again.
        assert_eq!(next(&[9], 9, 50), (vec![], 9));
        assert_eq!(next(&[12, 10, 11, 4], 9, 2), (vec![10, 11], 11));
        assert_eq!(next(&[], 0, 50), (vec![], 0));
    }

    #[test]
    fn settings_are_checked() {
        let read = |j: serde_json::Value| parse(j.as_object().unwrap());
        let w =
            read(json!({ "credential": "mail", "from": "billing@example.com", "every": "2m", "idle": true }))
                .unwrap();
        assert_eq!(w.folder, "INBOX");
        assert!(w.idle && w.read);
        for (bad, says) in [
            (json!({}), "credential"),
            (json!({ "credential": "m", "every": "10s" }), "from 1m"),
            (json!({ "credential": "m", "from": "{{ value.x }}" }), "values"),
            (
                json!({ "credential": "m", "subject": "x\r\nA1 DELETE INBOX" }),
                "line break",
            ),
            (json!({ "credential": "m", "max_items": 0 }), "1 to 500"),
            (json!({ "credential": "m", "start": "later" }), "now or existing"),
            (json!({ "credential": "m", "nope": 1 }), "not a setting"),
        ] {
            let err = read(bad.clone()).unwrap_err();
            assert!(err.contains(says), "{bad}: {err}");
        }
        let c = criteria(&w, 41);
        assert_eq!(c.first(), Some(&Arg::Raw("UID 42:*".into())));
        assert!(c.contains(&Arg::Text("billing@example.com".into())));
    }
}
