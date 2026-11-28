// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A delivery an app sends to a trigger's address, checked before anything runs: signed with the secret the
//! credential holds (an unsigned one, one whose signature does not match, and one older than the trigger allows are
//! refused), then read - a challenge answered, a delivery of another kind passed, each event of it given to a run.
//! A delivery seen again is refused by [`Replays`]. Registering the address with the app, where its API allows, is
//! here too: the requests the trigger says, and what they give kept, sealed, in the credential for that workflow.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use base64::Engine as _;
use openduct_expr::{Env, Value};
use ring::hmac;
use serde_json::Value as Json;

use super::definition::AppDef;
use super::poll::{inputs_for, names_of, send};
use super::triggers::{Algorithm, Hook, Signed, TimeUnit, TriggerDef};
use crate::apps::Credential;
use crate::run::Effects;

/// The most events one delivery starts.
pub const MOST_EVENTS: usize = 100;
/// How many deliveries a node remembers, and for how long, to refuse one sent again.
const REMEMBERED: usize = 20_000;
const REMEMBERED_FOR: Duration = Duration::from_secs(24 * 3600);

/// A delivery, as it came.
#[derive(Debug, Clone, Copy)]
pub struct Delivery<'a> {
    pub method: &'a str,
    /// The whole address it was sent to: what an app that signs its address signed.
    pub address: &'a str,
    /// Its headers, names in small letters.
    pub headers: &'a [(String, String)],
    /// What follows `?`, as sent.
    pub query: &'a str,
    pub content_type: &'a str,
    pub body: &'a [u8],
}

/// What a delivery comes to.
#[derive(Debug)]
pub enum Verdict {
    /// Runs to start: each event's id (when the trigger tells them apart) and value.
    Run(Vec<(Option<String>, Value)>),
    /// The answer the app asked for (a challenge to prove the address).
    Answer(Json),
    /// Answered, and nothing run: why.
    Pass(&'static str),
}

/// Why a delivery was refused: the status it is answered with, and the reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub status: u16,
    pub reason: String,
}

fn refused(status: u16, reason: impl Into<String>) -> Refused {
    Refused {
        status,
        reason: reason.into(),
    }
}

/// The name a field kept on registering has in the credential: its own, for that workflow alone.
pub fn kept_name(field: &str, workflow: &str) -> String {
    format!("{field}@{workflow}")
}

/// The signing secret of `hook` for `workflow`: one kept on registering first, then one the credential was given.
pub fn secret_of(hook: &Hook, credential: &Credential, workflow: &str) -> Option<String> {
    hook.secret.iter().find_map(|name| {
        [kept_name(name, workflow), name.clone()]
            .iter()
            .find_map(|k| credential.fields.get(k).filter(|v| !v.trim().is_empty()).cloned())
    })
}

/// What was kept on registering `hook` for `workflow`, by its own names.
pub fn registered(hook: &Hook, credential: &Credential, workflow: &str) -> BTreeMap<String, String> {
    hook.keep
        .iter()
        .filter_map(|(k, _)| {
            credential
                .fields
                .get(&kept_name(k, workflow))
                .filter(|v| !v.is_empty())
                .map(|v| (k.clone(), v.clone()))
        })
        .collect()
}

fn header<'a>(d: &Delivery<'a>, name: &str) -> Option<&'a str> {
    d.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

fn form(text: &str) -> serde_json::Map<String, Json> {
    reqwest::Url::parse(&format!("http://form/?{text}"))
        .map(|u| {
            u.query_pairs()
                .map(|(k, v)| (k.into_owned(), Json::String(v.into_owned())))
                .collect()
        })
        .unwrap_or_default()
}

/// The delivery's body as its expressions read it: JSON, a form's fields, or text.
fn body_value(d: &Delivery<'_>) -> Value {
    if let Ok(json) = serde_json::from_slice::<Json>(d.body) {
        return Value::from_json(&json);
    }
    let text = String::from_utf8_lossy(d.body);
    if d.content_type.starts_with("application/x-www-form-urlencoded") {
        return Value::from_json(&Json::Object(form(&text)));
    }
    Value::text(text.as_ref())
}

fn names(d: &Delivery<'_>, settings: &Json) -> Vec<(String, Value)> {
    let headers: serde_json::Map<String, Json> = d
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), Json::String(v.clone())))
        .collect();
    vec![
        ("headers".to_owned(), Value::from_json(&Json::Object(headers))),
        ("query".to_owned(), Value::from_json(&Json::Object(form(d.query)))),
        ("input".to_owned(), Value::from_json(settings)),
    ]
}

fn truth(e: &openduct_expr::Compiled, env: &Env<'_>, what: &str) -> Result<bool, Refused> {
    match e.evaluate(env) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(Value::Null) => Ok(false),
        Ok(other) => Err(refused(
            500,
            format!("{what}: gave {}, not true or false", other.kind()),
        )),
        Err(e) => Err(refused(500, format!("{what}: {e}"))),
    }
}

/// When a delivery was signed, in milliseconds since 1970.
fn time_of(text: &str, unit: TimeUnit) -> Option<i64> {
    let text = text.trim();
    match unit {
        TimeUnit::Seconds => text.parse::<i64>().ok()?.checked_mul(1000),
        TimeUnit::Milliseconds => text.parse::<i64>().ok(),
        TimeUnit::Text => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|t| t.timestamp_millis()),
    }
}

/// Equal, in a time that does not depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn decode(text: &str, base64: bool) -> Option<Vec<u8>> {
    let text = text.trim();
    if base64 {
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .ok()
            .or_else(|| base64::engine::general_purpose::URL_SAFE.decode(text).ok())
    } else {
        if !text.len().is_multiple_of(2) || text.len() > 512 {
            return None;
        }
        (0..text.len())
            .step_by(2)
            .map(|i| text.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
            .collect()
    }
}

/// Checks a delivery's signature with `secret`: refused when there is none, it does not match, or (for a signature
/// that carries its time) it is older, or later, than the trigger allows.
fn signed(hook: &Hook, secret: &str, d: &Delivery<'_>, now_ms: i64) -> Result<(), Refused> {
    let s = &hook.signature;
    let given = header(d, &s.header)
        .filter(|g| !g.trim().is_empty())
        .ok_or_else(|| {
            refused(
                401,
                format!("the delivery is not signed: it has no {} header", s.header),
            )
        })?;
    if s.algorithm == Algorithm::Token {
        return if same(given.trim().as_bytes(), secret.as_bytes()) {
            Ok(())
        } else {
            Err(refused(401, "the delivery's token is wrong"))
        };
    }
    let mut signatures: Vec<&str> = Vec::new();
    let mut time: Option<&str> = s.time_header.as_deref().and_then(|h| header(d, h));
    match &s.pairs {
        Some((signature, time_name)) => {
            for pair in given.split(',').take(20) {
                let Some((k, v)) = pair.trim().split_once('=') else {
                    continue;
                };
                if k == signature {
                    signatures.push(v);
                } else if time_name.as_deref() == Some(k) {
                    time = Some(v);
                }
            }
        }
        None => signatures.extend(given.trim().strip_prefix(s.prefix.as_str())),
    }
    if signatures.is_empty() {
        return Err(refused(
            401,
            format!("the {} header holds no signature", s.header),
        ));
    }
    let carries_time = s.time_header.is_some() || s.pairs.as_ref().is_some_and(|(_, t)| t.is_some());
    if carries_time {
        let at = time
            .and_then(|t| time_of(t, s.time_unit))
            .ok_or_else(|| refused(401, "the delivery does not say when it was signed"))?;
        let tolerance = i64::try_from(s.tolerance.as_millis()).unwrap_or(i64::MAX);
        if (now_ms - at).abs() > tolerance {
            return Err(refused(
                401,
                "the delivery was signed too long ago (or is not yet due): it is not run",
            ));
        }
    }
    let mut message: Vec<u8> = Vec::with_capacity(d.body.len() + 64);
    for part in &s.signed {
        match part {
            Signed::Text(t) => message.extend_from_slice(t.as_bytes()),
            Signed::Body => message.extend_from_slice(d.body),
            Signed::Time => message.extend_from_slice(time.unwrap_or_default().trim().as_bytes()),
            Signed::Address => message.extend_from_slice(d.address.as_bytes()),
            Signed::Method => message.extend_from_slice(d.method.as_bytes()),
        }
    }
    // messaging: Discord signs with Ed25519; the secret field holds its public key, in hex.
    if s.algorithm == Algorithm::Ed25519 {
        return ed25519(secret, &message, &signatures, s.base64);
    }
    let algorithm = match s.algorithm {
        Algorithm::HmacSha1 => hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
        _ => hmac::HMAC_SHA256,
    };
    let key = hmac::Key::new(algorithm, secret.as_bytes());
    let matches = signatures
        .iter()
        .filter_map(|g| decode(g, s.base64))
        .any(|tag| hmac::verify(&key, &message, &tag).is_ok());
    if matches {
        Ok(())
    } else {
        Err(refused(
            401,
            "the delivery's signature does not match: it was not sent by the app, or the secret is another",
        ))
    }
}

/// What a delivery to `trigger`'s address comes to: `secret` is its signing secret ([`secret_of`]), `settings` the
/// workflow's for the trigger, `now_ms` the time.
pub fn check(
    trigger: &TriggerDef,
    secret: Option<&str>,
    settings: &Json,
    d: &Delivery<'_>,
    now_ms: i64,
) -> Result<Verdict, Refused> {
    let hook = trigger
        .hook()
        .ok_or_else(|| refused(404, "this trigger takes no deliveries"))?;
    let value = body_value(d);
    let names = names(d, &trigger.settings_of(settings));
    let env = Env {
        value: &value,
        message: &Value::Null,
        names: &names,
    };
    // messaging: a check of the address that asks for something back (Graph's validationToken), answered with it.
    if let Some(echoed) = echo(trigger, settings, d) {
        return echoed.map(|text| Verdict::Answer(Json::String(text)));
    }
    // A request that only checks the address comes unsigned: it is answered, and nothing runs.
    if let Some(ping) = &hook.ping
        && header(d, &hook.signature.header).is_none()
        && truth(ping, &env, "ping")?
    {
        return Ok(Verdict::Pass("a check of the address"));
    }
    let secret = secret.ok_or_else(|| {
        refused(
            401,
            "the trigger has no signing secret yet: turn the workflow off and on to register it, or give the \
             credential the app's signing secret",
        )
    })?;
    // messaging: a secret carried in the body (Graph's clientState) is checked where the body says.
    match &hook.signature.token {
        Some(token) if hook.signature.algorithm == Algorithm::BodyToken => body_token(token, secret, &env)?,
        _ => signed(hook, secret, d, now_ms)?,
    }
    if let Some((when, answer)) = &hook.challenge
        && truth(when, &env, "challenge.when")?
    {
        let a = answer
            .evaluate(&env)
            .map_err(|e| refused(500, format!("challenge.answer: {e}")))?;
        return Ok(Verdict::Answer(
            serde_json::from_str(&a.to_json()).unwrap_or(Json::Null),
        ));
    }
    if let Some(only) = &hook.only
        && !truth(only, &env, "only")?
    {
        return Ok(Verdict::Pass("a delivery of another kind than this trigger's"));
    }
    let events: Vec<Value> = match (&hook.events, &hook.event) {
        (Some(list), _) => match list
            .evaluate(&env)
            .map_err(|e| refused(500, format!("events: {e}")))?
        {
            Value::List(items) => items.iter().take(MOST_EVENTS).cloned().collect(),
            Value::Null => Vec::new(),
            other => vec![other],
        },
        (None, Some(one)) => vec![
            one.evaluate(&env)
                .map_err(|e| refused(500, format!("event: {e}")))?,
        ],
        (None, None) => vec![value.clone()],
    };
    if events.is_empty() {
        return Ok(Verdict::Pass("a delivery with no events in it"));
    }
    let mut runs = Vec::with_capacity(events.len());
    for event in events {
        let id = match &hook.id {
            Some(id) => {
                let mut each = names.clone();
                each.push(("item".to_owned(), event.clone()));
                let env = Env {
                    value: &value,
                    message: &Value::Null,
                    names: &each,
                };
                let v = id.evaluate(&env).map_err(|e| refused(500, format!("id: {e}")))?;
                Some(crate::template::as_text(&v)).filter(|t| !t.trim().is_empty())
            }
            None => None,
        };
        runs.push((id, event));
    }
    Ok(Verdict::Run(runs))
}

// messaging
/// An Ed25519 signature (RFC 8032) of `message` by the key `public_key` (hex): one of `signatures` must be it.
fn ed25519(public_key: &str, message: &[u8], signatures: &[&str], base64: bool) -> Result<(), Refused> {
    let key = decode(public_key, false)
        .filter(|k| k.len() == 32)
        .ok_or_else(|| refused(500, "the credential's public key is not 64 hexadecimal digits"))?;
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, key);
    let matches = signatures
        .iter()
        .filter_map(|g| decode(g, base64))
        .any(|sig| key.verify(message, &sig).is_ok());
    if matches {
        Ok(())
    } else {
        Err(refused(
            401,
            "the delivery's signature does not match: it was not sent by the app, or the public key is another",
        ))
    }
}

// messaging
/// A secret the body carries (each of a list, when it is one): it must be the trigger's.
fn body_token(token: &openduct_expr::Compiled, secret: &str, env: &Env<'_>) -> Result<(), Refused> {
    let given = token.evaluate(env).map_err(|_| {
        refused(
            401,
            "the delivery does not carry the secret where the app puts it",
        )
    })?;
    let texts: Vec<String> = match &given {
        Value::List(items) => items
            .iter()
            .take(MOST_EVENTS)
            .map(crate::template::as_text)
            .collect(),
        Value::Null => Vec::new(),
        one => vec![crate::template::as_text(one)],
    };
    if texts.is_empty() || texts.iter().any(|t| t.is_empty()) {
        return Err(refused(
            401,
            "the delivery does not carry the secret where the app puts it",
        ));
    }
    if texts.iter().all(|t| same(t.as_bytes(), secret.as_bytes())) {
        Ok(())
    } else {
        Err(refused(
            401,
            "the delivery carries another secret: it was not sent for this workflow's registration",
        ))
    }
}

// messaging
/// The answer to an unsigned request that checks the address and asks for something back (Microsoft Graph's
/// `?validationToken=`): what the trigger's `echo` gives, as text - none when it gives nothing, and the request is a
/// delivery like any other. Answered whether or not the workflow is on (an app checks the address while it is being
/// turned on), bounded, and refused when it holds what a web page would read as markup.
pub fn echo(trigger: &TriggerDef, settings: &Json, d: &Delivery<'_>) -> Option<Result<String, Refused>> {
    let hook = trigger.hook()?;
    let expression = hook.echo.as_ref()?;
    let value = body_value(d);
    let names = names(d, &trigger.settings_of(settings));
    let env = Env {
        value: &value,
        message: &Value::Null,
        names: &names,
    };
    let text = match expression.evaluate(&env) {
        Ok(Value::Null) | Err(_) => return None,
        Ok(v) => crate::template::as_text(&v),
    };
    if text.is_empty() {
        return None;
    }
    let plain = text.len() <= 2048
        && !text
            .chars()
            .any(|c| c.is_control() || matches!(c, '<' | '>' | '"' | '\'' | '&'));
    Some(if plain {
        Ok(text)
    } else {
        Err(refused(
            400,
            "the address check asks back for what is not a plain token",
        ))
    })
}

// messaging
/// What a delivery that starts runs is answered with, when the trigger says (Discord's deferred reply): JSON, or
/// text. None: OpenDuct's own answer.
pub fn answer_for(trigger: &TriggerDef, settings: &Json, d: &Delivery<'_>) -> Result<Option<Json>, Refused> {
    let Some(expression) = trigger.hook().and_then(|h| h.answer.as_ref()) else {
        return Ok(None);
    };
    let value = body_value(d);
    let names = names(d, &trigger.settings_of(settings));
    let env = Env {
        value: &value,
        message: &Value::Null,
        names: &names,
    };
    let a = expression
        .evaluate(&env)
        .map_err(|e| refused(500, format!("answer: {e}")))?;
    Ok(Some(serde_json::from_str(&a.to_json()).unwrap_or(Json::Null)))
}

/// The answer to a GET an app sends to check the address (Meta's): its challenge, when the token it gives is the
/// credential's.
pub fn confirm(trigger: &TriggerDef, credential: &Credential, query: &str) -> Result<String, Refused> {
    let hook = trigger
        .hook()
        .ok_or_else(|| refused(404, "this trigger takes no deliveries"))?;
    let c = hook
        .confirm
        .as_ref()
        .ok_or_else(|| refused(405, "this trigger takes deliveries as POST"))?;
    let names = vec![("query".to_owned(), Value::from_json(&Json::Object(form(query))))];
    let env = Env {
        value: &Value::Null,
        message: &Value::Null,
        names: &names,
    };
    let given = c
        .token
        .evaluate(&env)
        .map(|v| crate::template::as_text(&v))
        .unwrap_or_default();
    let wanted = credential.fields.get(&c.field).cloned().unwrap_or_default();
    if wanted.is_empty() || !same(given.as_bytes(), wanted.as_bytes()) {
        return Err(refused(
            403,
            "the address was checked with another token than the credential's",
        ));
    }
    c.answer
        .evaluate(&env)
        .map(|v| crate::template::as_text(&v).chars().take(1000).collect())
        .map_err(|e| refused(400, format!("confirm.answer: {e}")))
}

/// The deliveries a node has taken lately, so one sent again is refused: at most [`REMEMBERED`], each for a day.
#[derive(Debug, Default)]
pub struct Replays {
    seen: Mutex<Remembered>,
}

/// The keys remembered, and when each came, oldest first.
type Remembered = (HashSet<String>, VecDeque<(i64, String)>);

impl Replays {
    /// Whether `key` comes for the first time (and is remembered now).
    pub fn first_time(&self, key: &str, now_ms: i64) -> bool {
        let Ok(mut held) = self.seen.lock() else {
            return true;
        };
        let (set, order) = &mut *held;
        let horizon = now_ms - i64::try_from(REMEMBERED_FOR.as_millis()).unwrap_or(i64::MAX);
        while order.front().is_some_and(|(at, _)| *at < horizon) || order.len() >= REMEMBERED {
            if let Some((_, old)) = order.pop_front() {
                set.remove(&old);
            }
        }
        if !set.insert(key.to_owned()) {
            return false;
        }
        order.push_back((now_ms, key.to_owned()));
        true
    }
}

/// A random secret, for an app that is told the secret it signs with.
fn new_secret() -> Result<String, String> {
    let mut bytes = [0u8; 24];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
        .map_err(|_| "no randomness for a webhook's secret".to_owned())?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// Registers `trigger`'s address with the app, for `workflow`, through `effects` with the credential called `name`:
/// what is kept, by the names kept under (already kept in the credential). A trigger that registers nothing
/// keeps nothing.
pub async fn register(
    effects: &dyn Effects,
    app: &AppDef,
    trigger: &TriggerDef,
    settings: &Json,
    name: &str,
    workflow: &str,
    address: &str,
) -> Result<BTreeMap<String, String>, String> {
    register_at(
        effects,
        app,
        trigger,
        settings,
        &Target {
            name,
            workflow,
            address,
            secret: None,
        },
        crate::runner::now_ms(),
    )
    .await
}

/// Who a registration is for: the credential's name, the workflow, its address. // messaging
#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    pub name: &'a str,
    pub workflow: &'a str,
    pub address: &'a str,
    /// The secret a registration is told (a recording's); none: a random one.
    pub secret: Option<&'a str>,
}

/// [`register`], its requests reading `input.now` as `now_ms` says (a recording's time). // messaging
pub async fn register_at(
    effects: &dyn Effects,
    app: &AppDef,
    trigger: &TriggerDef,
    settings: &Json,
    target: &Target<'_>,
    now_ms: i64,
) -> Result<BTreeMap<String, String>, String> {
    let Target {
        name,
        workflow,
        address,
        ..
    } = *target;
    let hook = trigger.hook().ok_or("this trigger takes no deliveries")?;
    let settings = &trigger.settings_of(settings);
    if hook.register.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut credential = effects.credential(name)?;
    let secret = match target.secret {
        Some(s) => s.to_owned(),
        None => new_secret()?,
    };
    let last = requests(
        effects,
        app,
        hook,
        &hook.register,
        settings,
        target,
        &secret,
        &mut credential,
        now_ms,
    )
    .await?;
    let names = names_of(
        settings,
        &[
            ("address", Value::text(address)),
            ("secret", Value::text(secret.as_str())),
            ("workflow", Value::text(workflow)),
        ],
    );
    let env_names = vec![
        ("input".to_owned(), names),
        ("secret".to_owned(), Value::text(secret.as_str())),
    ];
    let env = Env {
        value: &last,
        message: &Value::Null,
        names: &env_names,
    };
    let mut kept = BTreeMap::new();
    for (k, e) in &hook.keep {
        let v = e.evaluate(&env).map_err(|e| format!("keep.{k}: {e}"))?;
        let text = crate::template::as_text(&v);
        if text.trim().is_empty() {
            return Err(format!("{} gave no {k} on registering the address", app.name));
        }
        kept.insert(kept_name(k, workflow), text);
    }
    if !kept.is_empty() {
        effects.keep_credential(name, kept.clone()).await?;
    }
    Ok(kept)
}

/// Takes `trigger`'s address away from the app again, for `workflow`; what was kept for it is emptied.
pub async fn unregister(
    effects: &dyn Effects,
    app: &AppDef,
    trigger: &TriggerDef,
    settings: &Json,
    name: &str,
    workflow: &str,
    address: &str,
) -> Result<(), String> {
    let hook = trigger.hook().ok_or("this trigger takes no deliveries")?;
    let settings = &trigger.settings_of(settings);
    let mut credential = effects.credential(name)?;
    if hook.unregister.is_empty() || registered(hook, &credential, workflow).is_empty() {
        return Ok(());
    }
    requests(
        effects,
        app,
        hook,
        &hook.unregister,
        settings,
        &Target {
            name,
            workflow,
            address,
            secret: None,
        },
        "",
        &mut credential,
        crate::runner::now_ms(),
    )
    .await?;
    let emptied: BTreeMap<String, String> = hook
        .keep
        .iter()
        .map(|(k, _)| (kept_name(k, workflow), String::new()))
        .collect();
    effects.keep_credential(name, emptied).await?;
    Ok(())
}

/// Sends `list` in order, each reading what the one before gave as `previous`: what the last one gave.
#[allow(clippy::too_many_arguments)]
async fn requests(
    effects: &dyn Effects,
    app: &AppDef,
    hook: &Hook,
    list: &[super::triggers::Request],
    settings: &Json,
    target: &Target<'_>,
    secret: &str,
    credential: &mut Credential,
    now_ms: i64,
) -> Result<Value, String> {
    let Target {
        name,
        workflow,
        address,
        ..
    } = *target;
    let kept: serde_json::Map<String, Json> = registered(hook, credential, workflow)
        .into_iter()
        .map(|(k, v)| (k, Json::String(v)))
        .collect();
    let mut previous = Value::Null;
    for request in list {
        let action = request
            .action(&app.actions)
            .ok_or_else(|| format!("{} has no such action", app.name))?;
        let names = names_of(
            settings,
            &[
                ("address", Value::text(address)),
                ("secret", Value::text(secret)),
                ("workflow", Value::text(workflow)),
                ("registered", Value::from_json(&Json::Object(kept.clone()))),
                ("previous", previous.clone()),
                ("now", Value::text(crate::runner::iso(now_ms).as_str())), // messaging
            ],
        );
        let inputs = inputs_for(request, &names)?;
        let answer = send(effects, app, name, credential, &|c| {
            super::request(app, action, c, &inputs, &Value::Null).map(Some)
        })
        .await?
        .unwrap_or(Value::Null);
        previous = super::answer(app, action, credential, &answer, &Value::Null)?.result;
    }
    Ok(previous)
}

// messaging
/// Whether a workflow's trigger is an app's webhook whose registration runs out, and is renewed on a schedule.
pub fn renews(trigger: &crate::Trigger) -> bool {
    every_renewal(trigger).is_some()
}

/// How often a workflow's app webhook registration is renewed, when it is.
pub fn every_renewal(trigger: &crate::Trigger) -> Option<Duration> {
    let crate::Trigger::App(t) = trigger else {
        return None;
    };
    t.definition()?.1.hook()?.renew_every
}

/// Keeps `trigger`'s registration for `workflow` going: its `renew` requests, at `now_ms`. When they fail - the app
/// has let it go (a node was down past its end) - the address it was registered at is registered again, when the
/// trigger keeps that address; otherwise the failure is said, for the workflow to be turned off and on.
pub async fn renew(
    effects: &dyn Effects,
    app: &AppDef,
    trigger: &TriggerDef,
    settings: &Json,
    name: &str,
    workflow: &str,
) -> Result<(), String> {
    renew_at(
        effects,
        app,
        trigger,
        settings,
        name,
        workflow,
        crate::runner::now_ms(),
    )
    .await
}

/// [`renew`], its requests reading `input.now` as `now_ms` says.
pub async fn renew_at(
    effects: &dyn Effects,
    app: &AppDef,
    trigger: &TriggerDef,
    settings: &Json,
    name: &str,
    workflow: &str,
    now_ms: i64,
) -> Result<(), String> {
    let hook = trigger.hook().ok_or("this trigger takes no deliveries")?;
    if hook.renew.is_empty() {
        return Ok(());
    }
    let settings = &trigger.settings_of(settings);
    let mut credential = effects.credential(name)?;
    let kept = registered(hook, &credential, workflow);
    let address = kept.get("address").cloned().unwrap_or_default();
    let target = Target {
        name,
        workflow,
        address: &address,
        secret: None,
    };
    let renewed = if kept.is_empty() {
        Err(format!("{} has no registration for this workflow", app.name))
    } else {
        requests(
            effects,
            app,
            hook,
            &hook.renew,
            settings,
            &target,
            "",
            &mut credential,
            now_ms,
        )
        .await
        .map(|_| ())
    };
    match renewed {
        Ok(()) => Ok(()),
        Err(why) if !address.is_empty() => {
            tracing::info!(
                workflow,
                error = why,
                "a registration could not be renewed: registering it again"
            );
            let _ = unregister(effects, app, trigger, settings, name, workflow, &address).await;
            register_at(effects, app, trigger, settings, &target, now_ms)
                .await
                .map(|_| ())
                .map_err(|e| {
                    format!(
                        "{} could not be renewed ({why}), nor registered again: {e}",
                        app.name
                    )
                })
        }
        Err(why) => Err(format!(
            "{} could not be renewed: {why} - turn the workflow off and on to register it again",
            app.name
        )),
    }
}
