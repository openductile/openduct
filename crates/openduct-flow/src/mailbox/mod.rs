// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Email from any mailbox, over IMAP: a `mailbox` step that lists and searches a folder's messages, reads one - its
//! text, HTML and headers, its attachments as the run's files - marks one seen or flagged, moves one to another folder,
//! deletes one, or lists the folders; and a `mailbox` trigger that starts a run for each new message ([`watch`]).
//!
//! **Any mailbox.** An `imap` credential names the server, its port and security (TLS from the start by default), who
//! signs in, and how: a password, or an OAuth access token through XOAUTH2 for a provider that wants one - kept fresh,
//! when the credential also holds a refresh token, the provider's token address and the client's id, as the token
//! runs out or is refused. The client is OpenDuct's own ([`imap`]), over the TLS the storage steps use: a server is
//! reached on the internet's addresses only unless the installation allows its own network.
//!
//! **Runs that only look.** A test run reaches nothing: each action answers as it would, with a sample message. A
//! preview reads for real and shows what would change (marking, moving, deleting). Every input is bounded: how many
//! messages a list gives, how large a message read may be, how many attachments become files.

use std::time::Duration;

use openduct_expr::{Env, Record, Value};
use serde_json::{Value as Json, json};

use crate::apps::Credential;
use crate::files::RunFiles;
use crate::run::Effects;
use crate::template::Template;

pub mod idle;
mod imap;
mod mime;
pub mod watch;
mod wire;

pub use idle::idle;
pub use watch::MailWatch;

use imap::{Session, SignIn, field};
use wire::Arg;

/// How many messages a list gives, by default and at most.
pub(crate) const LIST_ITEMS: usize = 50;
pub(crate) const MOST_LIST_ITEMS: usize = 500;
/// How long one step's work with the server may take.
const STEP_TIME: Duration = Duration::from_secs(120);
/// The longest text and HTML a message read gives (the rest is cut, and said).
const LONGEST_TEXT: usize = 1024 * 1024;
/// How early a token is renewed before it runs out, in seconds.
const EARLY: u64 = 60;
/// The longest folder name a step gives.
const MOST_FOLDER: usize = 1000;

/// What a `mailbox` step does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    List,
    Read,
    Mark,
    Move,
    Delete,
    Folders,
}

impl Action {
    pub(crate) const ALL: &'static [&'static str] = &["list", "read", "mark", "move", "delete", "folders"];

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "list" => Self::List,
            "read" => Self::Read,
            "mark" => Self::Mark,
            "move" => Self::Move,
            "delete" => Self::Delete,
            "folders" => Self::Folders,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Read => "read",
            Self::Mark => "mark",
            Self::Move => "move",
            Self::Delete => "delete",
            Self::Folders => "folders",
        }
    }

    /// Whether it changes the mailbox: a preview shows it instead.
    pub(crate) fn writes(self) -> bool {
        matches!(self, Self::Mark | Self::Move | Self::Delete)
    }
}

/// Which messages a list (or the trigger) takes: each given criterion must hold.
#[derive(Debug, Clone, Default)]
pub struct Criteria {
    pub unseen: bool,
    pub flagged: bool,
    pub from: Option<Template>,
    pub to: Option<Template>,
    pub subject: Option<Template>,
    pub text: Option<Template>,
    /// Dates (`2026-11-01`), or times whose day counts.
    pub since: Option<Template>,
    pub before: Option<Template>,
}

/// A `mailbox` step, as read.
#[derive(Debug, Clone)]
pub struct MailboxStep {
    pub action: Action,
    pub credential: String,
    pub folder: Option<Template>,
    pub criteria: Criteria,
    pub limit: usize,
    /// The message a read, mark, move or delete is of: its UID in the folder.
    pub uid: Option<Template>,
    /// Marks: seen and flagged set (true), taken away (false), or left (none).
    pub seen: Option<bool>,
    pub flagged: Option<bool>,
    /// Where a move goes.
    pub to_folder: Option<Template>,
    /// A read gives the attachments as the run's files (by default), and the whole message as one when asked.
    pub files: bool,
    pub raw: bool,
}

/// The step's result.
pub(crate) async fn run(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &MailboxStep,
    env: &Env<'_>,
) -> Result<Value, String> {
    let render = |t: &Option<Template>, what: &str| -> Result<Option<String>, String> {
        t.as_ref()
            .map(|t| t.render(env).map_err(|e| format!("{what}: {e}")))
            .transpose()
            .map(|v| v.filter(|s| !s.trim().is_empty()))
    };
    let folder = render(&step.folder, "folder")?.unwrap_or_else(|| "INBOX".to_owned());
    wire::safe(&folder, "folder")?;
    if folder.len() > MOST_FOLDER {
        return Err(format!("folder: at most {MOST_FOLDER} characters"));
    }
    let uid = match (&step.uid, step.action) {
        (Some(_), _) => {
            let text = render(&step.uid, "uid")?.unwrap_or_default();
            Some(
                text.trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|u| *u > 0)
                    .ok_or_else(|| format!("uid: {text:?} is not a message's UID (a whole number)"))?,
            )
        }
        (None, Action::List | Action::Folders) => None,
        (None, a) => return Err(format!("uid: which message to {}", a.name())),
    };
    let to_folder = render(&step.to_folder, "to_folder")?;
    if let Some(t) = &to_folder {
        wire::safe(t, "to_folder")?;
        if t.len() > MOST_FOLDER {
            return Err(format!("to_folder: at most {MOST_FOLDER} characters"));
        }
    }
    let criteria = search_args(&step.criteria, env)?;
    let credential = effects.credential(&step.credential)?;
    if credential.kind == "test" {
        return sample(
            effects,
            files,
            step,
            &folder,
            uid,
            to_folder.as_deref(),
            "not sent",
        )
        .await;
    }
    if credential.kind != "imap" {
        return Err(format!(
            "{} is a {} credential: a mailbox step takes an imap one",
            step.credential, credential.kind
        ));
    }
    let places = match effects.storage().allows(step.action.writes()) {
        Ok(p) => p.clone(),
        Err(test) => return sample(effects, files, step, &folder, uid, to_folder.as_deref(), test).await,
    };
    let work = async {
        let mut session = open(effects, &step.credential, &credential, &places).await?;
        let done = act(
            &mut session,
            effects,
            files,
            step,
            &folder,
            uid,
            to_folder.as_deref(),
            &criteria,
        )
        .await;
        session.logout().await;
        done
    };
    match tokio::time::timeout(STEP_TIME, work).await {
        Ok(done) => done,
        Err(_) => Err(format!("the mailbox took more than {}s", STEP_TIME.as_secs())),
    }
}

/// A session signed in with the credential called `name`: with its password, or its OAuth token - renewed first
/// when it has run out, and once more when the server refuses it.
pub(crate) async fn open(
    effects: &dyn Effects,
    name: &str,
    c: &Credential,
    places: &crate::storage::Places,
) -> Result<Session, String> {
    let renewable = ["refresh_token", "token_url", "client_id"]
        .iter()
        .all(|f| field(c, f).is_some());
    let token = field(c, "access_token");
    if token.is_none() && !renewable {
        let password = c
            .fields
            .get("password")
            .filter(|p| !p.is_empty())
            .ok_or("the imap credential has neither a password nor an access token")?;
        return Session::open(c, places, &SignIn::Password(password.clone())).await;
    }
    let ends = field(c, "expires_at").and_then(|e| e.parse::<u64>().ok());
    let mut c = c.clone();
    if renewable && (token.is_none() || ends.is_some_and(|e| e <= now() + EARLY)) {
        c = renew(effects, name, &c).await?;
    }
    let token = field(&c, "access_token").ok_or("the imap credential has no access token")?;
    match Session::open(&c, places, &SignIn::Token(token)).await {
        Err(e) if renewable && e.contains("refused the OAuth token") => {
            let fresh = renew(effects, name, &c).await?;
            let token = field(&fresh, "access_token").unwrap_or_default();
            Session::open(&fresh, places, &SignIn::Token(token)).await
        }
        other => other,
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A fresh access token for the credential (the refresh grant of RFC 6749 6), kept in it.
async fn renew(effects: &dyn Effects, name: &str, c: &Credential) -> Result<Credential, String> {
    let url = field(c, "token_url").unwrap_or_default();
    if !url.starts_with("https://") {
        return Err("token_url: the provider's token address, starting https://".to_owned());
    }
    let mut form = reqwest::Url::parse("http://form/").map_err(|e| e.to_string())?;
    {
        let mut pairs = form.query_pairs_mut();
        pairs.append_pair("grant_type", "refresh_token");
        pairs.append_pair("refresh_token", &c.field("refresh_token"));
        pairs.append_pair("client_id", &c.field("client_id"));
        if let Some(secret) = field(c, "client_secret") {
            pairs.append_pair("client_secret", &secret);
        }
    }
    let request = crate::run::HttpRequest {
        read: true,
        method: "POST".to_owned(),
        url,
        headers: vec![("accept".to_owned(), "application/json".to_owned())],
        query: Vec::new(),
        body: Some((
            "application/x-www-form-urlencoded".to_owned(),
            form.query().unwrap_or_default().as_bytes().to_vec(),
        )),
        timeout: Duration::from_secs(30),
    };
    let answer = effects.http(request).await?;
    let tokens = crate::app_files::oauth::tokens(&answer, c).map_err(|e| format!("{name}: {e}"))?;
    effects.keep_credential(name, tokens.clone()).await?;
    let mut fresh = c.clone();
    fresh.fields.extend(tokens);
    Ok(fresh)
}

/// A date as IMAP's SEARCH writes it: `1-Nov-2026`.
fn imap_date(text: &str, what: &str) -> Result<String, String> {
    let day = text.trim().get(..10).unwrap_or_default();
    let date = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map_err(|_| format!("{what}: {text:?} is not a date, as 2026-11-01"))?;
    Ok(date.format("%-d-%b-%Y").to_string())
}

/// The criteria as `UID SEARCH` takes them, worked out.
fn search_args(c: &Criteria, env: &Env<'_>) -> Result<Vec<Arg>, String> {
    let mut args = Vec::new();
    if c.unseen {
        args.push(Arg::Raw("UNSEEN".into()));
    }
    if c.flagged {
        args.push(Arg::Raw("FLAGGED".into()));
    }
    for (key, t) in [
        ("FROM", &c.from),
        ("TO", &c.to),
        ("SUBJECT", &c.subject),
        ("TEXT", &c.text),
    ] {
        if let Some(t) = t {
            let v = t
                .render(env)
                .map_err(|e| format!("{}: {e}", key.to_ascii_lowercase()))?;
            if !v.trim().is_empty() {
                wire::safe(&v, &key.to_ascii_lowercase())?;
                if v.len() > 1000 {
                    return Err(format!("{}: at most 1000 characters", key.to_ascii_lowercase()));
                }
                args.push(Arg::Raw(key.into()));
                args.push(Arg::Text(v));
            }
        }
    }
    for (key, t) in [("SINCE", &c.since), ("BEFORE", &c.before)] {
        if let Some(t) = t {
            let v = t
                .render(env)
                .map_err(|e| format!("{}: {e}", key.to_ascii_lowercase()))?;
            if !v.trim().is_empty() {
                args.push(Arg::Raw(format!(
                    "{key} {}",
                    imap_date(&v, &key.to_ascii_lowercase())?
                )));
            }
        }
    }
    Ok(args)
}

/// The fields of a message a list gives, from its headers and what the server said of it.
pub(crate) fn summary(
    folder: &str,
    uid: u64,
    headers: &[(String, String)],
    flags: &[String],
    size: u64,
) -> Json {
    let get = |name: &str| headers.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());
    let people = |name: &str| -> Json {
        Json::Array(
            get(name)
                .map(mime::addresses)
                .unwrap_or_default()
                .into_iter()
                .take(100)
                .map(|(n, a)| json!({ "name": n, "address": a }))
                .collect(),
        )
    };
    let from = match people("from") {
        Json::Array(mut list) if !list.is_empty() => list.swap_remove(0),
        _ => Json::Null,
    };
    let has = |f: &str| flags.iter().any(|x| x.eq_ignore_ascii_case(f));
    json!({
        "uid": uid,
        "folder": folder,
        "subject": get("subject").unwrap_or_default(),
        "from": from,
        "to": people("to"),
        "cc": people("cc"),
        "reply_to": people("reply-to"),
        "date": get("date").and_then(mime::date),
        "message_id": get("message-id").map(|m| m.trim().to_owned()),
        "flags": flags,
        "seen": has("\\Seen"),
        "flagged": has("\\Flagged"),
        "size": size,
    })
}

/// The headers a list reads of each message.
pub(crate) const LIST_FETCH: &str =
    "(UID FLAGS RFC822.SIZE BODY.PEEK[HEADER.FIELDS (FROM TO CC REPLY-TO SUBJECT DATE MESSAGE-ID)])";

#[allow(clippy::too_many_arguments)]
async fn act(
    s: &mut Session,
    effects: &dyn Effects,
    files: &RunFiles,
    step: &MailboxStep,
    folder: &str,
    uid: Option<u64>,
    to_folder: Option<&str>,
    criteria: &[Arg],
) -> Result<Value, String> {
    if step.action == Action::Folders {
        let found = s.folders(1_000).await?;
        let list: Vec<Json> = found
            .into_iter()
            .map(|(name, attributes)| json!({ "name": name, "attributes": attributes }))
            .collect();
        return Ok(Value::from_json(&json!({ "count": list.len(), "folders": list })));
    }
    let selected = s.select(folder, step.action.writes()).await?;
    let Some(uid) = uid else {
        // A list: the newest `limit` of those that meet the criteria, newest first.
        let mut uids = s.search(criteria).await?;
        let total = uids.len();
        let keep_from = uids.len().saturating_sub(step.limit);
        uids.drain(..keep_from);
        let mut messages: Vec<Json> = s
            .fetch(&uids, LIST_FETCH)
            .await?
            .iter()
            .filter_map(|f| {
                let uid = f.uid()?;
                let size = f.get("RFC822.SIZE").and_then(wire::Item::number).unwrap_or(0);
                Some(summary(
                    folder,
                    uid,
                    &mime::headers(f.body().unwrap_or_default()),
                    &f.flags(),
                    size,
                ))
            })
            .collect();
        messages.sort_by_key(|m| std::cmp::Reverse(m.get("uid").and_then(Json::as_u64).unwrap_or(0)));
        return Ok(Value::from_json(&json!({
            "count": messages.len(), "total": total, "folder": folder, "uidvalidity": selected.uidvalidity,
            "messages": messages,
        })));
    };
    let exists = !s.search(&[Arg::Raw(format!("UID {uid}"))]).await?.is_empty();
    if !exists {
        return Err(format!(
            "{folder} has no message with UID {uid} (it may have been moved or deleted)"
        ));
    }
    match step.action {
        Action::Read => read(s, effects, files, step, folder, uid).await,
        Action::Mark => {
            let mut marked = json!({ "uid": uid, "folder": folder, "marked": true });
            for (flag, set, name) in [
                ("\\Seen", step.seen, "seen"),
                ("\\Flagged", step.flagged, "flagged"),
            ] {
                if let Some(on) = set {
                    s.store(&[uid], on, &[flag]).await?;
                    if let Some(m) = marked.as_object_mut() {
                        m.insert(name.to_owned(), Json::Bool(on));
                    }
                }
            }
            Ok(Value::from_json(&marked))
        }
        Action::Move => {
            let to = to_folder.ok_or("to_folder: the folder it goes to")?;
            s.move_to(&[uid], to).await?;
            Ok(Value::from_json(
                &json!({ "uid": uid, "folder": folder, "moved": true, "to_folder": to }),
            ))
        }
        Action::Delete => {
            let expunged = s.expunge(&[uid]).await?;
            Ok(Value::from_json(
                &json!({ "uid": uid, "folder": folder, "deleted": true, "expunged": expunged }),
            ))
        }
        Action::List | Action::Folders => Ok(Value::Null),
    }
}

/// Text cut to at most `most` bytes, on a character's edge; whether it was cut.
fn cut(mut text: String, most: usize) -> (String, bool) {
    if text.len() <= most {
        return (text, false);
    }
    let mut end = most;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    (text, true)
}

async fn read(
    s: &mut Session,
    effects: &dyn Effects,
    files: &RunFiles,
    step: &MailboxStep,
    folder: &str,
    uid: u64,
) -> Result<Value, String> {
    let sized = s.fetch(&[uid], "(UID RFC822.SIZE)").await?;
    let size = sized
        .first()
        .and_then(|f| f.get("RFC822.SIZE"))
        .and_then(wire::Item::number)
        .unwrap_or(0);
    if size > imap::MAX_MESSAGE as u64 {
        return Err(format!(
            "the message is {} MB: a read takes at most {} MB",
            size / (1024 * 1024),
            imap::MAX_MESSAGE / (1024 * 1024)
        ));
    }
    let fetched = s.fetch(&[uid], "(UID FLAGS RFC822.SIZE BODY.PEEK[])").await?;
    let f = fetched
        .iter()
        .find(|f| f.uid() == Some(uid))
        .ok_or("the server did not give the message")?;
    let raw = f.body().unwrap_or_default();
    let message = mime::parse(raw);
    let mut out = summary(folder, uid, &message.headers, &f.flags(), size);
    let (text, text_cut) = cut(message.text.clone().unwrap_or_default(), LONGEST_TEXT);
    let (html, html_cut) = cut(message.html.clone().unwrap_or_default(), LONGEST_TEXT);
    let mut headers = serde_json::Map::new();
    for (name, value) in message.headers.iter().take(200) {
        match headers.get_mut(name) {
            Some(Json::String(existing)) if existing.len() < 64 * 1024 => {
                existing.push('\n');
                existing.push_str(value);
            }
            Some(_) => {}
            None => {
                headers.insert(name.clone(), Json::String(value.clone()));
            }
        }
    }
    let mut attachments = Vec::new();
    for a in &message.attachments {
        let about =
            json!({ "name": a.name, "type": a.media_type, "size": a.bytes.len(), "inline": a.inline });
        if !step.files {
            attachments.push(about);
            continue;
        }
        match files.make(effects, &a.name, &a.media_type, a.bytes.clone()).await {
            Ok(file) => {
                let mut v: Json = serde_json::from_str(&file.to_json()).unwrap_or(Json::Null);
                if let Some(m) = v.as_object_mut() {
                    m.insert("inline".to_owned(), Json::Bool(a.inline));
                    if let Some(id) = &a.content_id {
                        m.insert("content_id".to_owned(), Json::String(id.clone()));
                    }
                }
                attachments.push(v);
            }
            // Past the run's limits: said, not kept.
            Err(why) => {
                let mut v = about;
                if let Some(m) = v.as_object_mut() {
                    m.insert("not_kept".to_owned(), Json::String(why));
                }
                attachments.push(v);
            }
        }
    }
    if let Some(m) = out.as_object_mut() {
        m.insert("text".to_owned(), Json::String(text));
        m.insert("html".to_owned(), Json::String(html));
        if text_cut || html_cut {
            m.insert("cut".to_owned(), Json::Bool(true));
        }
        m.insert("headers".to_owned(), Json::Object(headers));
        m.insert("attachments".to_owned(), Json::Array(attachments));
        if step.raw {
            let file = files
                .make(
                    effects,
                    &format!("message-{uid}.eml"),
                    "message/rfc822",
                    raw.to_vec(),
                )
                .await?;
            m.insert(
                "message".to_owned(),
                serde_json::from_str(&file.to_json()).unwrap_or(Json::Null),
            );
        }
    }
    Ok(Value::from_json(&out))
}

/// The message a test run (or the trigger's sample) answers with.
pub(crate) fn sample_message(folder: &str, uid: u64) -> Json {
    json!({
        "uid": uid,
        "folder": folder,
        "subject": "Invoice A-1001",
        "from": { "name": "Ann Example", "address": "ann@example.com" },
        "to": [{ "name": "Accounts", "address": "accounts@example.com" }],
        "cc": [],
        "reply_to": [],
        "date": "2026-11-23T09:00:00Z",
        "message_id": "<a1001@example.com>",
        "flags": [],
        "seen": false,
        "flagged": false,
        "size": 2048,
    })
}

const SAMPLE_TEXT: &str = "Hello,\n\nPlease find invoice A-1001 attached: 120.00, due 2026-12-23.\n\nAnn\n";
const SAMPLE_FILE: &str = "number,total,due\nA-1001,120.00,2026-12-23\n";

/// What a step would do, shaped as its result will be, saying it was not done (`test`).
#[allow(clippy::too_many_arguments)]
async fn sample(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &MailboxStep,
    folder: &str,
    uid: Option<u64>,
    to_folder: Option<&str>,
    test: &str,
) -> Result<Value, String> {
    let uid = uid.unwrap_or(1001);
    let mut out = match step.action {
        Action::Folders => json!({ "count": 3, "folders": [
            { "name": "INBOX", "attributes": [] },
            { "name": "Archive", "attributes": ["\\Archive"] },
            { "name": "Sent", "attributes": ["\\Sent"] },
        ] }),
        Action::List => json!({
            "count": 1, "total": 1, "folder": folder, "uidvalidity": 1, "messages": [sample_message(folder, uid)],
        }),
        Action::Read => {
            let mut m = sample_message(folder, uid);
            let file = if step.files {
                files
                    .make(
                        effects,
                        "invoice-A-1001.csv",
                        "text/csv",
                        SAMPLE_FILE.as_bytes().to_vec(),
                    )
                    .await
                    .map(|f| serde_json::from_str::<Json>(&f.to_json()).unwrap_or(Json::Null))?
            } else {
                json!({ "name": "invoice-A-1001.csv", "type": "text/csv", "size": SAMPLE_FILE.len() })
            };
            if let Some(o) = m.as_object_mut() {
                o.insert("text".to_owned(), Json::String(SAMPLE_TEXT.to_owned()));
                o.insert("html".to_owned(), Json::String(String::new()));
                o.insert(
                    "headers".to_owned(),
                    json!({ "subject": "Invoice A-1001", "from": "Ann Example <ann@example.com>" }),
                );
                o.insert("attachments".to_owned(), json!([file]));
            }
            m
        }
        Action::Mark => {
            let mut m = json!({ "uid": uid, "folder": folder, "marked": false });
            if let Some(o) = m.as_object_mut() {
                if let Some(seen) = step.seen {
                    o.insert("seen".to_owned(), Json::Bool(seen));
                }
                if let Some(flagged) = step.flagged {
                    o.insert("flagged".to_owned(), Json::Bool(flagged));
                }
            }
            m
        }
        Action::Move => json!({ "uid": uid, "folder": folder, "moved": false, "to_folder": to_folder }),
        Action::Delete => json!({ "uid": uid, "folder": folder, "deleted": false, "expunged": false }),
    };
    if let Some(o) = out.as_object_mut() {
        o.insert("test".to_owned(), Json::String(test.to_owned()));
        o.insert("action".to_owned(), Json::String(step.action.name().to_owned()));
    }
    let mut record = Record::new();
    if let Value::Record(r) = Value::from_json(&out) {
        record = (*r).clone();
    }
    Ok(Value::record(record))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn dates_go_as_imap_writes_them() {
        assert_eq!(imap_date("2026-11-01", "since").unwrap(), "1-Nov-2026");
        assert_eq!(imap_date("2026-11-23T10:00:00Z", "since").unwrap(), "23-Nov-2026");
        assert!(
            imap_date("yesterday", "since")
                .unwrap_err()
                .contains("not a date")
        );
        assert_eq!(
            cut("été".into(), 4),
            ("ét".into(), true),
            "cut on a character's edge"
        );
    }

    #[test]
    fn a_summary_reads_the_headers() {
        let headers = vec![
            ("from".to_owned(), "Ann <ann@example.com>".to_owned()),
            ("subject".to_owned(), "Hi".to_owned()),
            ("date".to_owned(), "Mon, 23 Nov 2026 10:00:00 +0000".to_owned()),
        ];
        let s = summary("INBOX", 7, &headers, &["\\Seen".to_owned()], 99);
        assert_eq!(s["from"]["address"], "ann@example.com");
        assert_eq!(s["seen"], true);
        assert_eq!(s["flagged"], false);
        assert_eq!(s["date"], "2026-11-23T10:00:00Z");
    }
}
