// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Running an app's action: its request, from the credential and the step's inputs (and, for a list, the page
//! asked for), and what its answer means - the step's result, the records of a page and where the next one is, or
//! why the app refused.

use std::time::Duration;

use base64::Engine as _;
use openduct_expr::{Env, Value};
use serde_json::{Value as Json, json};

use super::definition::{Action, AppDef, BodyKind, SignIn, host_matches};
use crate::apps::Credential;
use crate::model::Tree;
use crate::run::HttpRequest;

/// How long one request of an app's may take.
const TIMEOUT: Duration = Duration::from_secs(30);
/// The longest refusal shown from an app's answer.
const SHOWN: usize = 300;

/// The names an app's templates and expressions read.
struct Names {
    input: Value,
    credential: Value,
    page: Value,
}

impl Names {
    fn env(&self) -> Vec<(String, Value)> {
        vec![
            ("input".to_owned(), self.input.clone()),
            ("credential".to_owned(), self.credential.clone()),
            ("page".to_owned(), self.page.clone()),
        ]
    }
}

/// A credential's fields as its sign-in declares them. A secret one it does not hold (a test run's, which reads no
/// secret) is •••; another one left out is null.
fn credential_value(sign_in: &SignIn, credential: &Credential) -> Value {
    let fields: serde_json::Map<String, Json> = sign_in
        .fields
        .iter()
        .filter_map(|f| {
            let v = match credential.fields.get(&f.name) {
                Some(v) => Some(v.clone()),
                None if f.secret => Some(credential.field(&f.name)),
                None => None,
            };
            v.map(|v| (f.name.clone(), Json::String(v)))
        })
        .collect();
    Value::from_json(&Json::Object(fields))
}

/// The sign-in a credential is of: its own kind's, or (a test run's stand-in) the app's first.
fn sign_in_of<'a>(app: &'a AppDef, credential: &Credential) -> Result<&'a SignIn, String> {
    if credential.kind == "test" {
        return app
            .sign_ins
            .first()
            .ok_or_else(|| "the app has no sign-in".to_owned());
    }
    app.sign_in(&credential.kind).ok_or_else(|| {
        let kinds: Vec<&str> = app.sign_ins.iter().map(|s| s.id.as_str()).collect();
        format!(
            "a {} credential is not one of {}'s: it takes {}",
            credential.kind,
            app.name,
            kinds.join(" or ")
        )
    })
}

fn render(t: &crate::template::Template, names: &[(String, Value)]) -> Result<String, String> {
    let null = Value::Null;
    t.render(&Env {
        value: &null,
        message: &null,
        names,
    })
}

fn tree_value(t: &Tree, names: &[(String, Value)]) -> Result<Value, String> {
    let null = Value::Null;
    crate::run::tree(
        t,
        &Env {
            value: &null,
            message: &null,
            names,
        },
    )
}

/// The text a value is in a query or a form: itself for text, JSON for anything else, nothing for null.
fn plain(v: &Json) -> String {
    match v {
        Json::Null => String::new(),
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The request for `action` of `app`, with `credential` and the step's `inputs` (a record); `page` is what the
/// previous page's `next` gave (null for the first): a record of query parameters, or the next page's whole address.
pub fn request(
    app: &AppDef,
    action: &Action,
    credential: &Credential,
    inputs: &Value,
    page: &Value,
) -> Result<HttpRequest, String> {
    let sign_in = sign_in_of(app, credential)?;
    // An input the action needs that came empty (a value the event did not have) is not sent as nothing - an
    // address such as /forms//responses - but said.
    for input in action.inputs.iter().filter(|i| i.required) {
        let given = match inputs {
            Value::Record(r) => r.get(&input.name),
            _ => None,
        };
        let empty = match given {
            None | Some(Value::Null) => true,
            Some(Value::Text(t)) => t.trim().is_empty(),
            Some(_) => false,
        };
        if empty {
            return Err(format!(
                "{}'s {} needs {} ({}): it came empty",
                app.name, action.label, input.name, input.label
            ));
        }
    }
    let names = Names {
        input: inputs.clone(),
        credential: credential_value(sign_in, credential),
        page: page.clone(),
    }
    .env();
    let way = action.by.iter().find(|(id, _)| *id == sign_in.id).map(|(_, o)| o);
    // Where it goes: a webhook's own address, a base the credential gives (a self-hosted copy), or the app's.
    let (base, from_credential) = match (&sign_in.url_field, &app.base_url_field) {
        (Some(field), _) => (credential.field(field), true),
        (None, Some(field)) if credential.fields.get(field).is_some_and(|b| !b.trim().is_empty()) => {
            (credential.field(field).trim_end_matches('/').to_owned(), true)
        }
        _ => {
            let base = sign_in.base.as_ref().unwrap_or(&app.base);
            (render(base, &names)?.trim_end_matches('/').to_owned(), false)
        }
    };
    let url = match (way.and_then(|w| w.url.as_ref()), page) {
        (_, Value::Text(next)) => next.to_string(),
        (Some(u), _) => render(u, &names)?,
        (None, _) => {
            // A webhook's address is the whole of it: the action's path is for the app's own address.
            let path = match (way.and_then(|w| w.path.as_ref()), &sign_in.url_field) {
                (Some(p), _) => render(p, &names)?,
                (None, Some(_)) => String::new(),
                (None, None) => render(&action.path, &names)?,
            };
            format!("{base}{path}")
        }
    };
    // The app's own addresses (and a next page's) go only to its hosts. One the credential gives is its owner's,
    // and is used as it is (in a test run, where secrets read •••, it is not even an address).
    if !from_credential || matches!(page, Value::Text(_)) {
        let parsed =
            reqwest::Url::parse(&url).map_err(|e| format!("{}: {url:?} is not an address: {e}", app.name))?;
        let host = parsed.host_str().unwrap_or_default().to_owned();
        if !app.hosts.iter().any(|h| host_matches(h, &host)) {
            return Err(format!("{}: {host} is not one of the app's hosts", app.name));
        }
    }
    let mut query: Vec<(String, String)> = Vec::new();
    // A next page the service gave as a whole address already says what it asks: the action's query is not added
    // again (a sign-in's, a key, still is).
    let whole = matches!(page, Value::Text(_));
    let asked = if whole { &[][..] } else { action.query.as_slice() };
    for (k, t) in sign_in.query.iter() {
        let v = render(t, &names)?;
        if !v.is_empty() {
            query.push((k.clone(), v));
        }
    }
    let signed = query.len();
    for (k, t) in asked {
        let v = render(t, &names)?;
        if !v.is_empty() {
            query.push((k.clone(), v));
        }
    }
    // A next page's values take the place of the action's own of the same name (Xero's page=1 becomes page=2), never
    // of the sign-in's.
    if let Value::Record(params) = page
        && !action.page_in_body
    {
        for (k, v) in params.iter() {
            let v: Json = serde_json::from_str(&v.to_json()).unwrap_or(Json::Null);
            let mut at = 0;
            query.retain(|(q, _)| {
                at += 1;
                at <= signed || q != k
            });
            query.push((k.clone(), plain(&v)));
        }
    }
    let mut headers: Vec<(String, String)> = Vec::new();
    let extra = way.map_or(&[][..], |w| w.headers.as_slice());
    for (k, t) in sign_in
        .headers
        .iter()
        .chain(action.headers.iter())
        .chain(extra.iter())
    {
        let v = render(t, &names)?;
        if !v.is_empty() {
            headers.push((k.to_ascii_lowercase(), v));
        }
    }
    if let Some((user, password)) = &sign_in.basic {
        let pair = format!("{}:{}", render(user, &names)?, render(password, &names)?);
        headers.push((
            "authorization".to_owned(),
            format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(pair)),
        ));
    }
    let send = match (way.and_then(|w| w.send.as_ref()), &action.send) {
        (Some(s), _) | (None, Some(s)) => Some(tree_value(s, &names)?),
        (None, None) if !matches!(action.method.as_str(), "GET" | "DELETE") => Some(inputs.clone()),
        (None, None) => None,
    };
    let body = match (action.body, send) {
        (BodyKind::None, _) | (_, None) => None,
        (BodyKind::Json, Some(v)) => {
            let mut json: Json = serde_json::from_str(&v.to_json()).unwrap_or(Json::Null);
            // A next page's values in the body, for a service that takes them there.
            if let (true, Value::Record(params), Json::Object(fields)) =
                (action.page_in_body, page, &mut json)
            {
                for (k, v) in params.iter() {
                    fields.insert(
                        k.clone(),
                        serde_json::from_str(&v.to_json()).unwrap_or(Json::Null),
                    );
                }
            }
            without_nulls(&mut json);
            Some(("application/json".to_owned(), json.to_string().into_bytes()))
        }
        (BodyKind::Form, Some(v)) => {
            let json: Json = serde_json::from_str(&v.to_json()).unwrap_or(Json::Null);
            let mut form = reqwest::Url::parse("http://form/").map_err(|e| e.to_string())?;
            if let Json::Object(fields) = json {
                let mut pairs = form.query_pairs_mut();
                for (k, v) in &fields {
                    if !v.is_null() {
                        pairs.append_pair(k, &plain(v));
                    }
                }
            }
            Some((
                "application/x-www-form-urlencoded".to_owned(),
                form.query().unwrap_or_default().as_bytes().to_vec(),
            ))
        }
    };
    Ok(HttpRequest {
        read: !action.write,
        method: action.method.clone(),
        url,
        headers,
        query,
        body,
        timeout: TIMEOUT,
    })
}

/// What an answer of `action` gave: the step's result, or for a list action the page's records and what the next
/// page is (null for none).
#[derive(Debug)]
pub struct Answer {
    pub result: Value,
    pub items: Vec<Value>,
    pub next: Value,
}

/// A field left empty (an optional input not given) left out, at any depth, not sent as null; a record left empty by
/// that is left out too.
fn without_nulls(json: &mut Json) {
    match json {
        Json::Object(fields) => {
            for v in fields.values_mut() {
                without_nulls(v);
            }
            fields.retain(|_, v| !v.is_null() && !matches!(v, Json::Object(o) if o.is_empty()));
        }
        Json::Array(items) => items.iter_mut().for_each(without_nulls),
        _ => {}
    }
}

fn shown(b: &Json) -> String {
    plain(b).chars().take(SHOWN).collect()
}

/// A Link header's addresses by their rel (RFC 8288): `<https://x/orders?page_info=b>; rel="next"` gives
/// `{next: "https://x/orders?page_info=b"}`.
pub(crate) fn links_of(header: Option<&str>) -> Json {
    let mut out = serde_json::Map::new();
    let mut rest = header.unwrap_or("");
    while let Some(open) = rest.find('<') {
        let Some(close) = rest[open..].find('>') else {
            break;
        };
        let url = &rest[open + 1..open + close];
        let after = &rest[open + close + 1..];
        let end = after.find('<').unwrap_or(after.len());
        for param in after[..end].split(';') {
            let param = param.trim().trim_end_matches(',').trim();
            if param.len() > 4 && param[..4].eq_ignore_ascii_case("rel=") {
                for rel in param[4..].trim_matches('"').split_whitespace() {
                    out.entry(rel.to_ascii_lowercase())
                        .or_insert_with(|| Json::String(url.to_owned()));
                }
            }
        }
        rest = &after[end..];
    }
    Json::Object(out)
}

/// Reads an answer (`{status, headers, body}`) to the request for `page` (what `next` gave; null for the first):
/// what it gave, or why the app refused.
pub fn answer(
    app: &AppDef,
    action: &Action,
    credential: &Credential,
    answer: &Value,
    page: &Value,
) -> Result<Answer, String> {
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = json.get("body").cloned().unwrap_or(Json::Null);
    let value = Value::from_json(&body);
    let way = sign_in_of(app, credential)
        .ok()
        .and_then(|s| action.by.iter().find(|(id, _)| *id == s.id).map(|(_, o)| o));
    // `page` lets a service that says nothing of a next page be read page by page (WooCommerce: page 2 while a page
    // comes back full).
    // Its headers by their names in lowercase, and the addresses its Link header gives by their rel (Shopify's next
    // page is only there).
    let headers: serde_json::Map<String, Json> = json
        .get("headers")
        .and_then(Json::as_object)
        .map(|h| {
            h.iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
                .collect()
        })
        .unwrap_or_default();
    let links = links_of(headers.get("link").and_then(Json::as_str));
    let names = vec![
        ("status".to_owned(), Value::from_json(&json!(status))),
        ("page".to_owned(), page.clone()),
        ("headers".to_owned(), Value::from_json(&Json::Object(headers))),
        ("links".to_owned(), Value::from_json(&links)),
    ];
    let env = Env {
        value: &value,
        message: &Value::Null,
        names: &names,
    };
    let error = way
        .and_then(|w| w.error.as_ref())
        .or(action.error.as_ref())
        .or(app.error.as_ref());
    let said = error
        .and_then(|e| e.evaluate(&env).ok())
        .map(|v| crate::template::as_text(&v))
        .filter(|t| !t.trim().is_empty());
    if status >= 400 {
        let reason = said.unwrap_or_else(|| {
            body.pointer("/error/message")
                .or_else(|| body.get("message"))
                .or_else(|| body.get("description"))
                .or_else(|| body.get("error"))
                .map_or_else(|| shown(&body), shown)
        });
        return Err(format!(
            "{} answered {status}: {}",
            app.name,
            reason.chars().take(SHOWN).collect::<String>()
        ));
    }
    if let Some(why) = said {
        return Err(format!(
            "{} refused it: {}",
            app.name,
            why.chars().take(SHOWN).collect::<String>()
        ));
    }
    if let Some(items) = &action.items {
        let list = match items.evaluate(&env)? {
            Value::List(l) => l.to_vec(),
            Value::Null => Vec::new(),
            other => vec![other],
        };
        let next = match &action.next {
            Some(n) => n.evaluate(&env)?,
            None => Value::Null,
        };
        return Ok(Answer {
            result: Value::Null,
            items: list,
            next,
        });
    }
    let result = match way.and_then(|w| w.result.as_ref()).or(action.result.as_ref()) {
        Some(r) => r.evaluate(&env)?,
        None => value,
    };
    Ok(Answer {
        result,
        items: Vec::new(),
        next: Value::Null,
    })
}
