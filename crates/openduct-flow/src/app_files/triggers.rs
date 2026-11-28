// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app's triggers (`triggers:` in its definition): what starts a workflow from the app. A trigger either polls -
//! asks the app every so often for a list (new emails, rows, files, messages) and starts a run for each item not seen
//! before - or takes the app's own webhook deliveries, each checked against the signature the app puts on it. Both
//! make their requests as actions do: an action of the app's, named, or a request of the trigger's own written the
//! same way. Read and checked when the app is loaded, so a mistake is found with where it is.

use std::collections::BTreeMap;
use std::time::Duration;

use openduct_expr::Compiled;
use serde::Deserialize;
use serde_json::{Map, Value as Json};

use super::definition::{Action, Input};
use crate::model::Tree;

/// The most triggers an app has.
const MOST_TRIGGERS: usize = 20;
/// How often a poll asks, unless the trigger or the workflow says; and the least and most it may be.
pub const POLL_EVERY: Duration = Duration::from_secs(300);
pub const POLL_LEAST: Duration = Duration::from_secs(60);
pub const POLL_MOST: Duration = Duration::from_secs(86_400);
/// How old a signed delivery may be, unless the trigger says; and the most it may say.
const TOLERANCE: Duration = Duration::from_secs(300);
const MOST_TOLERANCE: Duration = Duration::from_secs(3600);
/// The most requests registering (or unregistering) a webhook makes.
const MOST_REGISTER: usize = 3;
/// Names a trigger's requests read in `input` beside its settings: no input may take them.
pub(crate) const RESERVED: &[&str] = &[
    "cursor",
    "now",
    "item",
    "index",
    "address",
    "secret",
    "registered",
    "previous",
    "workflow",
    "credential",
    "when",
    "every",
    "max_items",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerFile {
    label: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    inputs: Vec<Json>,
    #[serde(default)]
    poll: Option<Map<String, Json>>,
    #[serde(default)]
    webhook: Option<HookFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookFile {
    #[serde(default)]
    signature: Option<SignatureFile>,
    secret: SecretNames,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    only: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    events: Option<String>,
    #[serde(default)]
    ping: Option<String>,
    #[serde(default)]
    challenge: Option<ChallengeFile>,
    #[serde(default)]
    confirm: Option<ConfirmFile>,
    #[serde(default)]
    register: Vec<Map<String, Json>>,
    #[serde(default)]
    keep: BTreeMap<String, String>,
    #[serde(default)]
    unregister: Vec<Map<String, Json>>,
    #[serde(default)]
    setup: String,
    // messaging: what a delivery that runs is answered with; an address check answered with what it asks back;
    // requests that keep a registration that runs out, and how often.
    #[serde(default)]
    answer: Option<String>,
    #[serde(default)]
    echo: Option<String>,
    #[serde(default)]
    renew: Vec<Map<String, Json>>,
    #[serde(default)]
    renew_every: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SecretNames {
    One(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignatureFile {
    #[serde(default)] // messaging: a secret carried in the body comes in no header
    header: String,
    #[serde(default)]
    algorithm: Option<String>,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    pairs: Option<PairsFile>,
    #[serde(default)]
    time_header: Option<String>,
    #[serde(default)]
    time_unit: Option<String>,
    #[serde(default)]
    signed: Option<String>,
    #[serde(default)]
    tolerance: Option<String>,
    /// messaging: where in the body the secret is, for `algorithm: body_token`.
    #[serde(default)]
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PairsFile {
    signature: String,
    #[serde(default)]
    time: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChallengeFile {
    when: String,
    answer: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmFile {
    token: String,
    field: String,
    answer: String,
}

/// A trigger of an app's: what it is called and asks for, and how it starts runs.
#[derive(Debug)]
pub struct TriggerDef {
    pub label: String,
    pub description: String,
    /// What a workflow's trigger gives it (a form's id, a channel): plain values, not expressions.
    pub inputs: Vec<Input>,
    pub how: How,
}

#[derive(Debug)]
pub enum How {
    Poll(Poll),
    Webhook(Box<Hook>),
}

/// A request a trigger makes: an action of the app's, or one of its own, and what its inputs are.
#[derive(Debug)]
pub struct Request {
    pub uses: Uses,
    /// The action's inputs, each a template over `input` (the trigger's settings and the names beside them); none:
    /// `input` itself.
    pub input: Vec<(String, Tree)>,
}

#[derive(Debug)]
pub enum Uses {
    Action(String),
    Own(Box<Action>),
}

/// A trigger that asks the app every so often.
#[derive(Debug)]
pub struct Poll {
    /// The list asked for: its items are what may start runs.
    pub list: Request,
    /// What tells one item from another (over `item` and `index`).
    pub id: Compiled,
    /// What tells an item that changed from one that did not; none: an item seen once is never new again.
    pub version: Option<Compiled>,
    /// Where the next poll starts (over `items`, the items dealt with, oldest first, and `cursor`, the one before).
    pub cursor: Option<Compiled>,
    /// The app lists the newest first: they are taken oldest first, and listing stops at a page with one seen.
    pub newest_first: bool,
    /// The first item names the fields of the rest (a sheet's header row): each item after it is a record of them.
    pub header_row: bool,
    /// A request for each new item, whose answer is what runs (an email read in full).
    pub each: Option<Request>,
    /// What a run gets (over `item`); none: the item.
    pub event: Option<Compiled>,
    /// How often it asks, unless the workflow says.
    pub every: Duration,
}

/// A trigger the app calls: its deliveries, checked.
#[derive(Debug)]
pub struct Hook {
    pub signature: Signature,
    /// The credential's fields that may hold the signing secret, in order: one kept on registering first.
    pub secret: Vec<String>,
    /// What tells one delivery (or event) from another: one seen again is refused.
    pub id: Option<Compiled>,
    /// Which deliveries start runs: the rest are answered and not run.
    pub only: Option<Compiled>,
    /// What a run gets (over `value`, `headers`, `query`, `input`); none: the delivery's body.
    pub event: Option<Compiled>,
    /// A list of events in one delivery: each runs (`item` is one).
    pub events: Option<Compiled>,
    /// A request that only checks the address (sent unsigned): answered, nothing run.
    pub ping: Option<Compiled>,
    /// A signed request that asks for something back to prove the address is ours: when, and the answer.
    pub challenge: Option<(Compiled, Compiled)>,
    /// A GET the app sends to check the address: the token it gives, the credential field it must equal, the answer.
    pub confirm: Option<Confirm>,
    /// Requests that register the address with the app, when the workflow is turned on.
    pub register: Vec<Request>,
    /// What registering gave that is kept, sealed, in the credential for this workflow (over `value`, `input`).
    pub keep: Vec<(String, Compiled)>,
    /// Requests that take the address away again, when the workflow is turned off.
    pub unregister: Vec<Request>,
    /// What to do in the app's own pages when it cannot be registered from here.
    pub setup: String,
    /// messaging: what a delivery that starts runs is answered with (over `value`, `headers`, `query`, `input`):
    /// Discord's deferred reply. None: OpenDuct's own answer.
    pub answer: Option<Compiled>,
    /// messaging: an unsigned request that checks the address and asks for something back (Microsoft Graph's
    /// `validationToken`): what it asks, answered as text - even while the workflow is being turned on.
    pub echo: Option<Compiled>,
    /// messaging: requests that keep a registration that runs out (Graph's subscriptions), made every `renew_every`
    /// once in the cluster; the address is registered again when they fail.
    pub renew: Vec<Request>,
    pub renew_every: Option<Duration>,
}

#[derive(Debug)]
pub struct Confirm {
    pub token: Compiled,
    pub field: String,
    pub answer: Compiled,
}

/// How a delivery is signed.
#[derive(Debug)]
pub struct Signature {
    /// The header the signature comes in (in small letters).
    pub header: String,
    pub algorithm: Algorithm,
    pub base64: bool,
    /// What comes before the signature in its header (`sha256=`).
    pub prefix: String,
    /// The header holds names and values (`t=1700000000,v1=...`): the name of the signature's, and of the time's.
    pub pairs: Option<(String, Option<String>)>,
    /// A header the time it was signed comes in.
    pub time_header: Option<String>,
    pub time_unit: TimeUnit,
    /// What is signed, in order.
    pub signed: Vec<Signed>,
    /// How old a delivery may be, when it carries a time.
    pub tolerance: Duration,
    /// messaging: for `body_token`, what in the body must equal the secret (text, or a list of texts, each).
    pub token: Option<Compiled>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    HmacSha256,
    HmacSha1,
    /// The header is the secret itself (Telegram's).
    Token,
    /// messaging: an Ed25519 signature (RFC 8032), checked with the public key the secret field holds, in hex
    /// (Discord's).
    Ed25519,
    /// messaging: the body carries the secret itself, where `token` says (Microsoft Graph's `clientState`).
    BodyToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Seconds,
    Milliseconds,
    /// RFC 3339 text.
    Text,
}

/// One part of what is signed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signed {
    Text(String),
    Body,
    Time,
    /// The whole address the delivery was sent to.
    Address,
    Method,
}

impl TriggerDef {
    /// The workflow's settings for it as its expressions read them: every input it declares there, null when not
    /// given (so `input.x == null` asks whether it was).
    pub fn settings_of(&self, given: &Json) -> Json {
        let mut out = given.as_object().cloned().unwrap_or_default();
        for i in &self.inputs {
            out.entry(i.name.clone()).or_insert(Json::Null);
        }
        Json::Object(out)
    }

    /// Whether it polls.
    pub fn polls(&self) -> Option<&Poll> {
        match &self.how {
            How::Poll(p) => Some(p),
            How::Webhook(_) => None,
        }
    }

    /// Whether the app calls it.
    pub fn hook(&self) -> Option<&Hook> {
        match &self.how {
            How::Webhook(h) => Some(h.as_ref()),
            How::Poll(_) => None,
        }
    }
}

impl Request {
    /// The action it makes: the app's own, or its own.
    pub fn action<'a>(&'a self, actions: &'a [(String, Action)]) -> Option<&'a Action> {
        match &self.uses {
            Uses::Own(a) => Some(a),
            Uses::Action(name) => actions.iter().find(|(n, _)| n == name).map(|(_, a)| a),
        }
    }
}

fn expression(source: &str, at: &str) -> Result<Compiled, String> {
    openduct_expr::compile("cel", source).map_err(|e| format!("{at}: {e}"))
}

fn optional(source: Option<&String>, at: &str) -> Result<Option<Compiled>, String> {
    source.map(|s| expression(s, at)).transpose()
}

fn duration_of(text: &str, at: &str, least: Duration, most: Duration) -> Result<Duration, String> {
    crate::model::duration(text)
        .filter(|d| *d >= least && *d <= most)
        .ok_or_else(|| format!("{at}: {text:?} is not a time from {least:?} to {most:?}, as 5m or 1h"))
}

/// Reads an app's triggers: `files` as the definition has them, `at` where (for messages).
pub(super) fn read(
    files: &BTreeMap<String, Json>,
    at: &dyn Fn(&str) -> String,
    sign_ins: &[super::definition::SignIn],
    actions: &[(String, Action)],
) -> Result<Vec<(String, TriggerDef)>, String> {
    if files.len() > MOST_TRIGGERS {
        return Err(at(&format!("triggers: at most {MOST_TRIGGERS}")));
    }
    let ids: Vec<String> = sign_ins.iter().map(|s| s.id.clone()).collect();
    let mut out = Vec::new();
    for (name, json) in files {
        let here = at(&format!("triggers.{name}"));
        if !super::definition::named(name) {
            return Err(format!(
                "{here}: a trigger's name is lowercase letters, digits and '_'"
            ));
        }
        let file: TriggerFile = serde_json::from_value(json.clone()).map_err(|e| format!("{here}: {e}"))?;
        let inputs = super::definition::inputs_of(&file.inputs, &here)?;
        if let Some(i) = inputs.iter().find(|i| RESERVED.contains(&i.name.as_str())) {
            return Err(format!(
                "{here}.inputs: {} is a name of the trigger's own",
                i.name
            ));
        }
        let how = match (file.poll, file.webhook) {
            (Some(p), None) => How::Poll(poll(&p, &format!("{here}.poll"), &file.label, &ids, actions)?),
            (None, Some(h)) => How::Webhook(Box::new(hook(
                h,
                &format!("{here}.webhook"),
                &file.label,
                sign_ins,
                actions,
            )?)),
            _ => return Err(format!("{here}: either poll or webhook")),
        };
        out.push((
            name.clone(),
            TriggerDef {
                label: file.label,
                description: file.description,
                inputs,
                how,
            },
        ));
    }
    Ok(out)
}

/// A request of a trigger's: `{action: <name>, input: {...}}`, or a request written as an action is (`method`,
/// `path`, `query`, `send` ...) with an `input` of its own if it likes.
fn request(
    json: &Map<String, Json>,
    at: &str,
    label: &str,
    sign_ins: &[String],
    actions: &[(String, Action)],
) -> Result<Request, String> {
    let input = match json.get("input") {
        None | Some(Json::Null) => Vec::new(),
        Some(Json::Object(fields)) => fields
            .iter()
            .map(|(k, v)| Ok((k.clone(), super::definition::tree(v, &format!("{at}.input.{k}"))?)))
            .collect::<Result<_, String>>()?,
        Some(_) => return Err(format!("{at}.input: names and their templates")),
    };
    let uses = match json.get("action") {
        Some(name) => {
            let name = name
                .as_str()
                .ok_or_else(|| format!("{at}.action: an action's name"))?;
            if let Some(k) = json.keys().find(|k| *k != "action" && *k != "input") {
                return Err(format!(
                    "{at}.{k}: a request naming an action has only action and input"
                ));
            }
            if !actions.iter().any(|(n, _)| n == name) {
                return Err(format!("{at}.action: the app has no action {name}"));
            }
            Uses::Action(name.to_owned())
        }
        None => {
            let mut own = json.clone();
            own.remove("input");
            own.entry("label")
                .or_insert_with(|| Json::String(label.to_owned()));
            Uses::Own(Box::new(super::definition::action_of(
                &Json::Object(own),
                at,
                sign_ins,
            )?))
        }
    };
    Ok(Request { uses, input })
}

const POLL_FIELDS: &[&str] = &[
    "id",
    "version",
    "cursor",
    "newest_first",
    "header_row",
    "each",
    "event",
    "every",
];

fn poll(
    json: &Map<String, Json>,
    at: &str,
    label: &str,
    sign_ins: &[String],
    actions: &[(String, Action)],
) -> Result<Poll, String> {
    let text = |k: &str| -> Result<Option<String>, String> {
        match json.get(k) {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(format!("{at}.{k}: an expression")),
        }
    };
    let flag = |k: &str| json.get(k).and_then(Json::as_bool).unwrap_or(false);
    let listing: Map<String, Json> = json
        .iter()
        .filter(|(k, _)| !POLL_FIELDS.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let list = request(&listing, at, label, sign_ins, actions)?;
    if list.action(actions).is_none_or(|a| a.items.is_none()) {
        return Err(format!(
            "{at}: polls a list: an action with items (the records of a page)"
        ));
    }
    let id = text("id")?.ok_or_else(|| format!("{at}.id: what tells one item from another, as item.id"))?;
    let each = match json.get("each") {
        None | Some(Json::Null) => None,
        Some(Json::Object(e)) => {
            let r = request(e, &format!("{at}.each"), label, sign_ins, actions)?;
            if r.action(actions).is_some_and(|a| a.items.is_some()) {
                return Err(format!("{at}.each: reads one item, not a list"));
            }
            Some(r)
        }
        Some(_) => return Err(format!("{at}.each: a request for each new item")),
    };
    let (newest_first, header_row) = (flag("newest_first"), flag("header_row"));
    if newest_first && header_row {
        return Err(format!(
            "{at}: a list under a header row is not listed newest first"
        ));
    }
    let every = match text("every")? {
        Some(e) => duration_of(&e, &format!("{at}.every"), POLL_LEAST, POLL_MOST)?,
        None => POLL_EVERY,
    };
    Ok(Poll {
        list,
        id: expression(&id, &format!("{at}.id"))?,
        version: optional(text("version")?.as_ref(), &format!("{at}.version"))?,
        cursor: optional(text("cursor")?.as_ref(), &format!("{at}.cursor"))?,
        newest_first,
        header_row,
        each,
        event: optional(text("event")?.as_ref(), &format!("{at}.event"))?,
        every,
    })
}

fn hook(
    h: HookFile,
    at: &str,
    label: &str,
    sign_ins: &[super::definition::SignIn],
    actions: &[(String, Action)],
) -> Result<Hook, String> {
    let ids: Vec<String> = sign_ins.iter().map(|s| s.id.clone()).collect();
    let requests = |list: &[Map<String, Json>], what: &str| -> Result<Vec<Request>, String> {
        if list.len() > MOST_REGISTER {
            return Err(format!("{at}.{what}: at most {MOST_REGISTER} requests"));
        }
        list.iter()
            .enumerate()
            .map(|(i, r)| request(r, &format!("{at}.{what}[{i}]"), label, &ids, actions))
            .collect()
    };
    let register = requests(&h.register, "register")?;
    let unregister = requests(&h.unregister, "unregister")?;
    let renew = requests(&h.renew, "renew")?; // messaging
    let renew_every = match (&h.renew_every, renew.is_empty()) {
        (Some(e), false) => Some(duration_of(
            e,
            &format!("{at}.renew_every"),
            Duration::from_secs(60),
            Duration::from_secs(7 * 86_400),
        )?),
        (None, false) => return Err(format!("{at}.renew_every: how often renew is made, as 20m")),
        (Some(_), true) => return Err(format!("{at}.renew_every: there is no renew")),
        (None, true) => None,
    };
    if !renew.is_empty() && register.is_empty() {
        return Err(format!(
            "{at}.renew: renews what register made: there is no register"
        ));
    }
    if register.is_empty() && !h.keep.is_empty() {
        return Err(format!("{at}.keep: what registering gives: there is no register"));
    }
    let keep: Vec<(String, Compiled)> = h
        .keep
        .iter()
        .map(|(k, e)| {
            if !super::definition::named(k) {
                return Err(format!("{at}.keep: {k:?} is lowercase letters, digits and '_'"));
            }
            Ok((k.clone(), expression(e, &format!("{at}.keep.{k}"))?))
        })
        .collect::<Result<_, String>>()?;
    let secret = match h.secret {
        SecretNames::One(s) => vec![s],
        SecretNames::Many(s) => s,
    };
    if secret.is_empty() {
        return Err(format!(
            "{at}.secret: the credential field the signing secret is in"
        ));
    }
    for s in &secret {
        let declared = sign_ins.iter().any(|si| si.fields.iter().any(|f| &f.name == s));
        if !declared && !keep.iter().any(|(k, _)| k == s) {
            return Err(format!(
                "{at}.secret: {s} is neither a field of a sign-in nor kept on registering"
            ));
        }
    }
    let signature = h.signature.ok_or_else(|| {
        format!("{at}.signature: how the app signs a delivery (an unsigned one is refused)")
    })?;
    if h.event.is_some() && h.events.is_some() {
        return Err(format!("{at}: either event or events"));
    }
    let confirm = match h.confirm {
        Some(c) => {
            if !sign_ins
                .iter()
                .any(|si| si.fields.iter().any(|f| f.name == c.field))
            {
                return Err(format!("{at}.confirm.field: no sign-in has a field {}", c.field));
            }
            Some(Confirm {
                token: expression(&c.token, &format!("{at}.confirm.token"))?,
                field: c.field,
                answer: expression(&c.answer, &format!("{at}.confirm.answer"))?,
            })
        }
        None => None,
    };
    Ok(Hook {
        signature: signature_of(signature, &format!("{at}.signature"))?,
        secret,
        id: optional(h.id.as_ref(), &format!("{at}.id"))?,
        only: optional(h.only.as_ref(), &format!("{at}.only"))?,
        event: optional(h.event.as_ref(), &format!("{at}.event"))?,
        events: optional(h.events.as_ref(), &format!("{at}.events"))?,
        ping: optional(h.ping.as_ref(), &format!("{at}.ping"))?,
        challenge: match h.challenge {
            Some(c) => Some((
                expression(&c.when, &format!("{at}.challenge.when"))?,
                expression(&c.answer, &format!("{at}.challenge.answer"))?,
            )),
            None => None,
        },
        confirm,
        register,
        keep,
        unregister,
        setup: h.setup,
        answer: optional(h.answer.as_ref(), &format!("{at}.answer"))?, // messaging
        echo: optional(h.echo.as_ref(), &format!("{at}.echo"))?,       // messaging
        renew,                                                         // messaging
        renew_every,                                                   // messaging
    })
}

fn header_name(name: &str, at: &str) -> Result<String, String> {
    let lower = name.trim().to_ascii_lowercase();
    if lower.is_empty() || lower.len() > 100 || !lower.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(format!("{at}: {name:?} is not a header's name"));
    }
    Ok(lower)
}

fn signature_of(s: SignatureFile, at: &str) -> Result<Signature, String> {
    let algorithm = match s.algorithm.as_deref() {
        None | Some("hmac_sha256") => Algorithm::HmacSha256,
        Some("hmac_sha1") => Algorithm::HmacSha1,
        Some("token") => Algorithm::Token,
        Some("ed25519") => Algorithm::Ed25519,      // messaging
        Some("body_token") => Algorithm::BodyToken, // messaging
        Some(other) => {
            return Err(format!(
                "{at}.algorithm: hmac_sha256, hmac_sha1, ed25519, token or body_token, not {other:?}"
            ));
        }
    };
    // messaging: a secret in the body is found by an expression, and comes in no header.
    let token = optional(s.token.as_ref(), &format!("{at}.token"))?;
    if (algorithm == Algorithm::BodyToken) != token.is_some() {
        return Err(format!(
            "{at}.token: where the body carries the secret, for body_token (and only for it)"
        ));
    }
    if algorithm == Algorithm::BodyToken {
        if !s.header.is_empty() || s.pairs.is_some() || s.time_header.is_some() || s.signed.is_some() {
            return Err(format!("{at}: a body_token signature is the token alone"));
        }
        return Ok(Signature {
            header: String::new(),
            algorithm,
            base64: false,
            prefix: String::new(),
            pairs: None,
            time_header: None,
            time_unit: TimeUnit::Seconds,
            signed: Vec::new(),
            tolerance: TOLERANCE,
            token,
        });
    }
    let base64 = match s.encoding.as_deref() {
        None | Some("hex") => false,
        Some("base64") => true,
        Some(other) => return Err(format!("{at}.encoding: hex or base64, not {other:?}")),
    };
    let time_unit = match s.time_unit.as_deref() {
        None | Some("seconds") => TimeUnit::Seconds,
        Some("milliseconds") => TimeUnit::Milliseconds,
        Some("text") => TimeUnit::Text,
        Some(other) => {
            return Err(format!(
                "{at}.time_unit: seconds, milliseconds or text, not {other:?}"
            ));
        }
    };
    let pairs = s.pairs.map(|p| (p.signature, p.time));
    let has_time = s.time_header.is_some() || pairs.as_ref().is_some_and(|(_, t)| t.is_some());
    let signed = signed_parts(s.signed.as_deref().unwrap_or("{body}"), &format!("{at}.signed"))?;
    if signed.contains(&Signed::Time) && !has_time {
        return Err(format!("{at}.signed: names {{time}}, and no header gives one"));
    }
    if algorithm == Algorithm::Token && (pairs.is_some() || has_time || s.signed.is_some()) {
        return Err(format!("{at}: a token signature is the header alone"));
    }
    Ok(Signature {
        header: header_name(&s.header, &format!("{at}.header"))?,
        algorithm,
        base64,
        prefix: s.prefix,
        pairs,
        time_header: s
            .time_header
            .map(|t| header_name(&t, &format!("{at}.time_header")))
            .transpose()?,
        time_unit,
        signed,
        tolerance: match s.tolerance {
            Some(t) => duration_of(
                &t,
                &format!("{at}.tolerance"),
                Duration::from_secs(1),
                MOST_TOLERANCE,
            )?,
            None => TOLERANCE,
        },
        token: None, // messaging
    })
}

/// `{time}.{body}` as its parts: text, and the delivery's body, time, address and method.
fn signed_parts(source: &str, at: &str) -> Result<Vec<Signed>, String> {
    let mut parts = Vec::new();
    let mut rest = source;
    while let Some(open) = rest.find('{') {
        if open > 0 {
            parts.push(Signed::Text(rest.get(..open).unwrap_or_default().to_owned()));
        }
        let after = rest.get(open + 1..).unwrap_or_default();
        let close = after
            .find('}')
            .ok_or_else(|| format!("{at}: a {{ with no }} after it"))?;
        parts.push(match after.get(..close).unwrap_or_default() {
            "body" => Signed::Body,
            "time" => Signed::Time,
            "address" => Signed::Address,
            "method" => Signed::Method,
            other => return Err(format!("{at}: {{{other}}} is not body, time, address or method")),
        });
        rest = after.get(close + 1..).unwrap_or_default();
    }
    if !rest.is_empty() {
        parts.push(Signed::Text(rest.to_owned()));
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn what_is_signed_reads_into_its_parts() {
        assert_eq!(
            signed_parts("v0:{time}:{body}", "s").unwrap(),
            [
                Signed::Text("v0:".into()),
                Signed::Time,
                Signed::Text(":".into()),
                Signed::Body
            ]
        );
        assert_eq!(
            signed_parts("{address}{body}", "s").unwrap(),
            [Signed::Address, Signed::Body]
        );
        assert!(signed_parts("{bodyy}", "s").unwrap_err().contains("not body"));
        assert!(signed_parts("{body", "s").unwrap_err().contains("no }"));
    }
}
