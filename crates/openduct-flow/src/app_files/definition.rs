// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app's definition (`apps/<id>/app.yaml`), read and checked: every name, host, template and expression - so a
//! mistake is found when the app is loaded, with where it is, not when a workflow first runs it.

use std::collections::BTreeMap;

use openduct_expr::Compiled;
use serde::Deserialize;
use serde_json::Value as Json;

use crate::model::Tree;
use crate::template::Template;

/// The language an app's templates and expressions are written in.
const LANGUAGE: &str = "cel";
/// The most pages a list action follows unless it says.
const PAGES: u32 = 10;
/// The most pages any list action may follow.
pub(super) const MOST_PAGES: u32 = 100;
/// How long one request of an action may take unless it says, in seconds.
const WAIT: u64 = 30;
/// The longest any action's request may take, in seconds (a service that answers only when its work is done).
const MOST_WAIT: u64 = 300;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppFile {
    app: String,
    name: String,
    #[serde(default)]
    category: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    docs: Option<String>,
    #[serde(default)]
    hosts: Vec<String>,
    #[serde(default)]
    base_url: String,
    #[serde(default)]
    base_url_field: Option<String>,
    sign_in: Vec<Json>,
    #[serde(default)]
    default_action: Option<String>,
    actions: BTreeMap<String, Json>,
    #[serde(default)]
    error: Option<String>,
    // G3 app triggers
    #[serde(default)]
    triggers: BTreeMap<String, Json>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignInFile {
    id: String,
    label: String,
    #[serde(default)]
    fields: Vec<FieldFile>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    query: BTreeMap<String, String>,
    #[serde(default)]
    basic: Option<BasicFile>,
    #[serde(default)]
    url_field: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    oauth2: Option<OAuth2File>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldFile {
    name: String,
    label: String,
    #[serde(default)]
    secret: bool,
    #[serde(default)]
    optional: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BasicFile {
    username: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OAuth2File {
    #[serde(default)]
    grant: Option<String>,
    #[serde(default)]
    authorize: Option<String>,
    token: String,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    extra: BTreeMap<String, String>,
    #[serde(default)]
    client_auth: Option<String>,
    #[serde(default)]
    callback_fields: BTreeMap<String, String>,
    #[serde(default)]
    after_connect: Option<AfterConnectFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AfterConnectFile {
    url: String,
    keep: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActionFile {
    label: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    write: bool,
    #[serde(default)] // app generator: an action that calls an MCP tool names no method
    method: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    inputs: Vec<InputFile>,
    #[serde(default)]
    send: Option<Json>,
    #[serde(default)]
    query: BTreeMap<String, String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    items: Option<String>,
    #[serde(default)]
    next: Option<String>,
    #[serde(default)]
    max_pages: Option<u32>,
    #[serde(default)]
    page_in: Option<String>,
    #[serde(default)]
    timeout: Option<u64>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    by: BTreeMap<String, Json>,
    // app generator: what the step's result holds, and the tool of the app's MCP server it calls instead.
    #[serde(default)]
    output: Option<serde_json::Map<String, Json>>,
    #[serde(default)]
    mcp: Option<String>,
    /// messaging: requests made after this one, each reading what the one before gave (`input.previous`).
    #[serde(default)]
    then: Vec<Json>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    name: String,
    label: String,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    description: String,
    #[serde(default)]
    kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OverrideFile {
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    send: Option<Json>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// An app: what it is, how a credential of it signs in, and what it can do.
#[derive(Debug)]
pub struct AppDef {
    pub id: String,
    pub name: String,
    pub category: String,
    pub description: String,
    pub docs: Option<String>,
    /// The only hosts its own addresses may name (`*.zendesk.com` for any of its subdomains).
    pub hosts: Vec<String>,
    pub base_url: String,
    /// `base_url` as a template: it may name the credential's fields (`https://{{ credential.shop }}.myshopify.com`).
    pub base: Template,
    /// A credential field that, given, replaces `base_url` (a self-hosted copy, or a stand-in in tests).
    pub base_url_field: Option<String>,
    pub sign_ins: Vec<SignIn>,
    pub default_action: Option<String>,
    pub actions: Vec<(String, Action)>,
    pub error: Option<Compiled>,
    /// What starts a workflow from the app: polls and webhooks (see [`super::triggers`]). // G3 app triggers
    pub triggers: Vec<(String, super::triggers::TriggerDef)>,
    /// Where it was read from, for messages.
    pub source: String,
}

/// One way a credential of the app signs in.
#[derive(Debug)]
pub struct SignIn {
    /// The credential's kind.
    pub id: String,
    pub label: String,
    pub fields: Vec<Field>,
    pub headers: Vec<(String, Template)>,
    pub query: Vec<(String, Template)>,
    pub basic: Option<(Template, Template)>,
    /// A field that is the whole address requests go to (a webhook's URL).
    pub url_field: Option<String>,
    /// Its own base address, in place of the app's (a sandbox, say).
    pub base: Option<Template>,
    base_source: Option<String>,
    pub oauth2: Option<OAuth2>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub label: String,
    pub secret: bool,
    pub optional: bool,
}

/// How an OAuth 2.0 sign-in gets its token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grant {
    /// A person signs in at the vendor (Connect in the studio), through the install's own registration with it; the
    /// token is refreshed as it runs out.
    AuthorizationCode,
    /// The credential's own client id and secret are traded for a token (PayPal and the like).
    ClientCredentials,
}

#[derive(Debug)]
pub struct OAuth2 {
    pub grant: Grant,
    /// Where a person signs in (authorization code).
    pub authorize: String,
    pub token: String,
    /// A token address that names the credential's fields (a store's own: Shopify, Zendesk), and the hosts it may
    /// name once filled in - the app's. Client credentials only.
    pub(crate) token_named: Option<(Template, Vec<String>)>,
    pub scopes: Vec<String>,
    pub extra: Vec<(String, String)>,
    /// How the client id and secret go to the token address: in the form (`body`) or as HTTP basic (`basic`).
    pub client_basic: bool,
    /// Values the vendor's callback gives beside the code, kept in the credential: (field, the callback's name).
    pub callback_fields: Vec<(String, String)>,
    /// A request made once connected (with the new token), and what of its answer is kept: (field, expression).
    pub after_connect: Option<(String, Vec<(String, Compiled)>)>,
}

/// The fields a sign-in by OAuth keeps its token in, managed by OpenDuct.
pub const TOKEN_FIELDS: &[&str] = &["access_token", "refresh_token", "expires_at"];

/// Whether `url` may be one of an app's own addresses: https, or http to this machine (a stand-in in tests).
pub(crate) fn secure_address(url: &reqwest::Url) -> bool {
    url.scheme() == "https"
        || (url.scheme() == "http"
            && url.host_str().is_some_and(|h| {
                h == "localhost" || crate::effects::literal_ip(h).is_some_and(|ip| ip.is_loopback())
            }))
}

/// What a request's body is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    Json,
    Form,
    None,
    /// messaging: the bytes of `send` as they are - text, or a run's file.
    Raw,
}

/// Something the app can do: one request (or a list of them, page after page).
#[derive(Debug)]
pub struct Action {
    pub label: String,
    pub description: String,
    /// It changes something: a preview shows it and does not send it.
    pub write: bool,
    pub method: String,
    pub path: Template,
    pub body: BodyKind,
    pub inputs: Vec<Input>,
    pub send: Option<Tree>,
    pub query: Vec<(String, Template)>,
    pub headers: Vec<(String, Template)>,
    pub result: Option<Compiled>,
    pub items: Option<Compiled>,
    pub next: Option<Compiled>,
    pub max_pages: u32,
    /// Where a next page's values go: the query (most services), or the JSON body (Notion).
    pub page_in_body: bool,
    /// How long one request may take: 30 seconds, or what the action says (a service that answers when its work is
    /// done, such as Apify's run-and-wait).
    pub timeout: std::time::Duration,
    pub error: Option<Compiled>,
    /// What differs for a credential of one sign-in or another.
    pub by: Vec<(String, Override)>,
    /// What the step's result holds - for a list, each record - field by field, with its kind. // app generator
    pub output: Vec<(String, String)>,
    /// The tool of the app's MCP server the action calls, in place of a request. // app generator
    pub mcp: Option<String>,
    /// messaging: requests made after this one, in order, each reading the step's inputs and `input.previous` (what
    /// the request before gave): an upload that takes three (Slack's). The last one's result is the step's.
    pub then: Vec<Action>,
}

#[derive(Debug)]
pub struct Input {
    pub name: String,
    pub label: String,
    pub required: bool,
    pub description: String,
    pub kind: String,
}

/// What an action does differently for one sign-in.
#[derive(Debug, Default)]
pub struct Override {
    pub url: Option<Template>,
    pub path: Option<Template>,
    pub send: Option<Tree>,
    pub headers: Vec<(String, Template)>,
    pub result: Option<Compiled>,
    pub error: Option<Compiled>,
}

pub(super) fn named(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.starts_with(|c: char| c.is_ascii_lowercase())
}

fn template(source: &str, at: &str) -> Result<Template, String> {
    Template::compile(source, LANGUAGE).map_err(|e| format!("{at}: {e}"))
}

fn expression(source: Option<&String>, at: &str) -> Result<Option<Compiled>, String> {
    source
        .map(|s| openduct_expr::compile(LANGUAGE, s).map_err(|e| format!("{at}: {e}")))
        .transpose()
}

fn templates(map: &BTreeMap<String, String>, at: &str) -> Result<Vec<(String, Template)>, String> {
    map.iter()
        .map(|(k, v)| Ok((k.clone(), template(v, &format!("{at}.{k}"))?)))
        .collect()
}

pub(super) fn tree(source: &Json, at: &str) -> Result<Tree, String> {
    Ok(match source {
        Json::String(s) => Tree::Template(template(s, at)?),
        Json::Array(items) => Tree::List(
            items
                .iter()
                .enumerate()
                .map(|(i, v)| tree(v, &format!("{at}[{i}]")))
                .collect::<Result<_, _>>()?,
        ),
        Json::Object(fields) => Tree::Record(
            fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), tree(v, &format!("{at}.{k}"))?)))
                .collect::<Result<_, String>>()?,
        ),
        other => Tree::Literal(other.clone()),
    })
}

/// Whether `host` is one `pattern` names: exactly, or (`*.example.com`) any subdomain of it.
pub(crate) fn host_matches(pattern: &str, host: &str) -> bool {
    let (pattern, host) = (pattern.to_ascii_lowercase(), host.to_ascii_lowercase());
    match pattern.strip_prefix("*.") {
        Some(domain) => host.ends_with(&format!(".{domain}")),
        None => pattern == host,
    }
}

fn parse<T: serde::de::DeserializeOwned>(json: &Json, at: &str) -> Result<T, String> {
    serde_json::from_value(json.clone()).map_err(|e| format!("{at}: {e}"))
}

impl AppDef {
    /// Reads and checks an app's definition (YAML); `source` is where it came from, said in every message.
    pub fn parse(text: &str, source: &str) -> Result<Self, String> {
        let json = crate::yaml::to_json(text).map_err(|e| format!("{source}: {e}"))?;
        let at = |rest: &str| format!("{source}: {rest}");
        let file: AppFile = parse(&json, source)?;
        if !named(&file.app) {
            return Err(at(&format!(
                "app: {:?} is a name of lowercase letters, digits and '_', at most 40",
                file.app
            )));
        }
        if file.name.trim().is_empty() {
            return Err(at("name: the app's name, as people know it"));
        }
        let mut sign_ins = Vec::new();
        for (i, s) in file.sign_in.iter().enumerate() {
            let here = format!("sign_in[{i}]");
            let s: SignInFile = parse(s, &at(&here))?;
            sign_ins.push(sign_in(s, &at(&here))?);
        }
        if sign_ins.is_empty() {
            return Err(at("sign_in: how a credential of the app signs in"));
        }
        for s in &mut sign_ins {
            if let Some((_, hosts)) = s.oauth2.as_mut().and_then(|o| o.token_named.as_mut()) {
                if file.hosts.is_empty() {
                    return Err(at(&format!(
                        "hosts: sign_in {}'s token address names the credential's fields, so is checked against them",
                        s.id
                    )));
                }
                hosts.clone_from(&file.hosts);
            }
        }
        let mut seen = Vec::new();
        for s in &sign_ins {
            if seen.contains(&s.id) {
                return Err(at(&format!("sign_in: {} is named twice", s.id)));
            }
            seen.push(s.id.clone());
        }
        // A webhook's address, or a self-hosted service's (WooCommerce: the store's own), is the credential's alone.
        let by_url = sign_ins.iter().all(|s| s.url_field.is_some())
            || (file.base_url.is_empty() && file.base_url_field.is_some());
        if !by_url {
            if file.hosts.is_empty() {
                return Err(at("hosts: the hosts its addresses name, such as api.example.com"));
            }
            check_base(&file.base_url, &file.hosts, &at("base_url"))?;
            for s in &sign_ins {
                if let Some(b) = &s.base_source {
                    check_base(b, &file.hosts, &at(&format!("sign_in {}: base_url", s.id)))?;
                }
            }
        }
        if let Some(field) = &file.base_url_field
            && !sign_ins.iter().any(|s| s.fields.iter().any(|f| &f.name == field))
        {
            return Err(at(&format!("base_url_field: no sign-in has a field {field}")));
        }
        let ids: Vec<String> = sign_ins.iter().map(|s| s.id.clone()).collect();
        let mut actions = Vec::new();
        for (name, a) in &file.actions {
            let here = at(&format!("actions.{name}"));
            if !named(name) {
                return Err(format!(
                    "{here}: an action's name is lowercase letters, digits and '_'"
                ));
            }
            let a: ActionFile = parse(a, &here)?;
            actions.push((name.clone(), action(a, &here, &ids)?));
        }
        if actions.is_empty() {
            return Err(at("actions: what the app can do"));
        }
        let triggers = super::triggers::read(&file.triggers, &at, &sign_ins, &actions)?; // G3 app triggers
        super::mcp_action::check_app(&sign_ins, &actions, &at)?; // app generator
        if let Some(d) = &file.default_action
            && !actions.iter().any(|(n, _)| n == d)
        {
            return Err(at(&format!("default_action: there is no action {d}")));
        }
        Ok(Self {
            id: file.app,
            name: file.name,
            category: file.category,
            description: file.description,
            docs: file.docs,
            hosts: file.hosts,
            base: template(file.base_url.trim_end_matches('/'), &at("base_url"))?,
            base_url: file.base_url.trim_end_matches('/').to_owned(),
            base_url_field: file.base_url_field,
            sign_ins,
            default_action: file.default_action,
            actions,
            error: expression(file.error.as_ref(), &at("error"))?,
            triggers,
            source: source.to_owned(),
        })
    }

    /// The action called `name`, or the app's default when there is none.
    pub fn action(&self, name: Option<&str>) -> Option<(&str, &Action)> {
        let name = name.or(self.default_action.as_deref())?;
        self.actions
            .iter()
            .find(|(n, _)| n == name)
            .map(|(n, a)| (n.as_str(), a))
    }

    /// Its sign-in for a credential of `kind`.
    pub fn sign_in(&self, kind: &str) -> Option<&SignIn> {
        self.sign_ins.iter().find(|s| s.id == kind)
    }

    /// The trigger called `name`, or its only one when there is no name. // G3 app triggers
    pub fn trigger(&self, name: Option<&str>) -> Option<(&str, &super::triggers::TriggerDef)> {
        match name {
            Some(name) => self.triggers.iter().find(|(n, _)| n == name),
            None if self.triggers.len() == 1 => self.triggers.first(),
            None => None,
        }
        .map(|(n, t)| (n.as_str(), t))
    }
}

/// A base address: https (or http to this machine) on the app's hosts. One with `{{ }}` in it (a credential's field)
/// is checked for its form here and for its host as each request is made.
fn check_base(base: &str, hosts: &[String], at: &str) -> Result<(), String> {
    if base.contains("{{") {
        if !(base.starts_with("https://")
            || base.starts_with("http://localhost")
            || base.starts_with("http://127.0.0.1"))
        {
            return Err(format!("{at}: is an https:// address"));
        }
        return Ok(());
    }
    let parsed = reqwest::Url::parse(base).map_err(|e| format!("{at}: {base:?} is not an address: {e}"))?;
    if !secure_address(&parsed) {
        return Err(format!("{at}: is an https:// address"));
    }
    let host = parsed.host_str().unwrap_or_default();
    if !hosts.iter().any(|h| host_matches(h, host)) {
        return Err(format!("{at}: {host} is not among its hosts"));
    }
    Ok(())
}

fn sign_in(s: SignInFile, at: &str) -> Result<SignIn, String> {
    if !named(&s.id) {
        return Err(format!(
            "{at}.id: a credential kind's name is lowercase letters, digits and '_'"
        ));
    }
    let mut fields = Vec::new();
    if s.oauth2
        .as_ref()
        .is_some_and(|o| o.grant.as_deref() != Some("client_credentials"))
        && !s.fields.is_empty()
    {
        return Err(format!(
            "{at}.fields: a sign-in where a person signs in at the vendor has no fields: its token is OpenDuct's to keep"
        ));
    }
    for f in s.fields {
        if !named(&f.name) {
            return Err(format!(
                "{at}.fields: {:?} is lowercase letters, digits and '_'",
                f.name
            ));
        }
        fields.push(Field {
            name: f.name,
            label: f.label,
            secret: f.secret,
            optional: f.optional,
        });
    }
    if let Some(u) = &s.url_field
        && !fields.iter().any(|f| &f.name == u)
    {
        return Err(format!("{at}.url_field: there is no field {u}"));
    }
    let oauth2 = match s.oauth2 {
        Some(o) => {
            let grant = match o.grant.as_deref() {
                None | Some("authorization_code") => Grant::AuthorizationCode,
                Some("client_credentials") => Grant::ClientCredentials,
                Some(other) => {
                    return Err(format!(
                        "{at}.oauth2.grant: authorization_code or client_credentials, not {other:?}"
                    ));
                }
            };
            let authorize = match (grant, o.authorize) {
                (Grant::AuthorizationCode, Some(a)) => a,
                (Grant::AuthorizationCode, None) => {
                    return Err(format!(
                        "{at}.oauth2.authorize: where a person signs in at the vendor"
                    ));
                }
                (Grant::ClientCredentials, _) => String::new(),
            };
            // A token address naming the credential's fields is filled in, and checked against the app's hosts, as a
            // token is asked for: only the credential's own client id and secret are sent to it.
            let token_named = if o.token.contains("{{") {
                if grant != Grant::ClientCredentials {
                    return Err(format!(
                        "{at}.oauth2.token: only a client_credentials sign-in's token address may name the credential's fields"
                    ));
                }
                if !o.token.starts_with("https://") {
                    return Err(format!("{at}.oauth2.token: is an https:// address"));
                }
                Some((template(&o.token, &format!("{at}.oauth2.token"))?, Vec::new()))
            } else {
                None
            };
            for (name, url) in [("authorize", &authorize), ("token", &o.token)] {
                if url.is_empty() || (name == "token" && token_named.is_some()) {
                    continue;
                }
                let parsed = reqwest::Url::parse(url).map_err(|e| format!("{at}.oauth2.{name}: {e}"))?;
                if !secure_address(&parsed) {
                    return Err(format!("{at}.oauth2.{name}: is an https:// address"));
                }
            }
            if grant == Grant::ClientCredentials {
                for needed in ["client_id", "client_secret"] {
                    if !fields.iter().any(|f: &Field| f.name == needed) {
                        return Err(format!(
                            "{at}.fields: a client_credentials sign-in has a {needed} field"
                        ));
                    }
                }
            }
            let client_basic = match o.client_auth.as_deref() {
                None | Some("body") => false,
                Some("basic") => true,
                Some(other) => return Err(format!("{at}.oauth2.client_auth: body or basic, not {other:?}")),
            };
            let after_connect = match o.after_connect {
                Some(a) => {
                    let parsed = reqwest::Url::parse(&a.url)
                        .map_err(|e| format!("{at}.oauth2.after_connect.url: {e}"))?;
                    if !secure_address(&parsed) {
                        return Err(format!("{at}.oauth2.after_connect.url: is an https:// address"));
                    }
                    let keep = a
                        .keep
                        .iter()
                        .map(|(k, e)| {
                            expression(Some(e), &format!("{at}.oauth2.after_connect.keep.{k}"))
                                .map(|c| (k.clone(), c))
                        })
                        .collect::<Result<Vec<_>, String>>()?
                        .into_iter()
                        .filter_map(|(k, c)| c.map(|c| (k, c)))
                        .collect();
                    Some((a.url, keep))
                }
                None => None,
            };
            Some(OAuth2 {
                grant,
                authorize,
                token: o.token,
                token_named,
                scopes: o.scopes,
                extra: o.extra.into_iter().collect(),
                client_basic,
                callback_fields: o.callback_fields.into_iter().collect(),
                after_connect,
            })
        }
        None => None,
    };
    // What the vendor's callback, and a request once connected, give are kept beside the token.
    if let Some(o) = &oauth2 {
        let extra = o.callback_fields.iter().map(|(k, _)| k.clone()).chain(
            o.after_connect
                .iter()
                .flat_map(|(_, keep)| keep.iter().map(|(k, _)| k.clone())),
        );
        for name in extra.collect::<Vec<_>>() {
            if !named(&name) {
                return Err(format!(
                    "{at}.oauth2: {name:?} is lowercase letters, digits and '_'"
                ));
            }
            if !fields.iter().any(|f| f.name == name) {
                fields.push(Field {
                    label: name.replace('_', " "),
                    name,
                    secret: false,
                    optional: true,
                });
            }
        }
    }
    // A token the vendor gives is kept in fields of OpenDuct's own: secret, and never asked of a person.
    if oauth2.is_some() {
        for name in TOKEN_FIELDS {
            if !fields.iter().any(|f| f.name == *name) {
                fields.push(Field {
                    name: (*name).to_owned(),
                    label: (*name).replace('_', " "),
                    secret: *name != "expires_at",
                    optional: true,
                });
            }
        }
    }
    Ok(SignIn {
        id: s.id,
        label: s.label,
        fields,
        headers: templates(&s.headers, &format!("{at}.headers"))?,
        query: templates(&s.query, &format!("{at}.query"))?,
        basic: s
            .basic
            .map(|b| {
                Ok::<_, String>((
                    template(&b.username, &format!("{at}.basic.username"))?,
                    template(&b.password, &format!("{at}.basic.password"))?,
                ))
            })
            .transpose()?,
        url_field: s.url_field,
        base: s
            .base_url
            .as_deref()
            .map(|b| template(b.trim_end_matches('/'), &format!("{at}.base_url")))
            .transpose()?,
        base_source: s.base_url,
        oauth2,
    })
}

/// An action's (or a trigger's) inputs as written, checked: their names and kinds, none twice.
pub(super) fn inputs_of(files: &[Json], at: &str) -> Result<Vec<Input>, String> {
    let files: Vec<InputFile> = files
        .iter()
        .enumerate()
        .map(|(i, f)| parse(f, &format!("{at}.inputs[{i}]")))
        .collect::<Result<_, _>>()?;
    inputs_from(files, at)
}

/// An action written as JSON (a trigger's own request), read as the app's actions are. // G3 app triggers
pub(super) fn action_of(json: &Json, at: &str, sign_ins: &[String]) -> Result<Action, String> {
    action(parse(json, at)?, at, sign_ins)
}

fn inputs_from(files: Vec<InputFile>, at: &str) -> Result<Vec<Input>, String> {
    let mut inputs: Vec<Input> = Vec::new();
    for i in files {
        if !named(&i.name) {
            return Err(format!(
                "{at}.inputs: {:?} is lowercase letters, digits and '_'",
                i.name
            ));
        }
        if i.name == "credential" || i.name == "action" {
            return Err(format!("{at}.inputs: {} is the step's own setting", i.name));
        }
        if inputs.iter().any(|x| x.name == i.name) {
            return Err(format!("{at}.inputs: {} is named twice", i.name));
        }
        let kind = i.kind.unwrap_or_else(|| "text".to_owned());
        if !matches!(kind.as_str(), "text" | "number" | "boolean" | "list" | "record") {
            return Err(format!(
                "{at}.inputs.{}: kind is text, number, boolean, list or record",
                i.name
            ));
        }
        inputs.push(Input {
            name: i.name,
            label: i.label,
            required: i.required,
            description: i.description,
            kind,
        });
    }
    Ok(inputs)
}

fn action(a: ActionFile, at: &str, sign_ins: &[String]) -> Result<Action, String> {
    let method = super::mcp_action::method_of(a.mcp.as_deref(), &a.method, at)?; // app generator
    if !matches!(method.as_str(), "GET" | "POST" | "PUT" | "PATCH" | "DELETE") {
        return Err(format!("{at}.method: GET, POST, PUT, PATCH or DELETE"));
    }
    let body = match a.body.as_deref() {
        None | Some("json") => BodyKind::Json,
        Some("form") => BodyKind::Form,
        Some("none") => BodyKind::None,
        Some("raw") => BodyKind::Raw, // messaging
        Some(other) => return Err(format!("{at}.body: json, form, raw or none, not {other:?}")),
    };
    let then = super::follow_ups::read(&a.then, at, &a.label, a.write, sign_ins)?; // messaging
    if !then.is_empty() && a.items.is_some() {
        return Err(format!(
            "{at}.then: a list's pages are not followed by other requests"
        ));
    }
    let inputs = inputs_from(a.inputs, at)?;
    let max_pages = a.max_pages.unwrap_or(PAGES);
    if max_pages == 0 || max_pages > MOST_PAGES {
        return Err(format!("{at}.max_pages: 1 to {MOST_PAGES}"));
    }
    let page_in_body = match a.page_in.as_deref() {
        None | Some("query") => false,
        Some("body") => true,
        Some(other) => return Err(format!("{at}.page_in: query or body, not {other:?}")),
    };
    if a.next.is_some() && a.items.is_none() {
        return Err(format!("{at}.next: pages need items, the records of each"));
    }
    let timeout = a.timeout.unwrap_or(WAIT);
    if timeout == 0 || timeout > MOST_WAIT {
        return Err(format!("{at}.timeout: 1 to {MOST_WAIT} seconds"));
    }
    let mut by = Vec::new();
    for (id, o) in &a.by {
        if !sign_ins.contains(id) {
            return Err(format!("{at}.by.{id}: there is no sign-in {id}"));
        }
        let here = format!("{at}.by.{id}");
        let o: OverrideFile = parse(o, &here)?;
        by.push((
            id.clone(),
            Override {
                url: o
                    .url
                    .as_deref()
                    .map(|u| template(u, &format!("{here}.url")))
                    .transpose()?,
                path: o
                    .path
                    .as_deref()
                    .map(|p| template(p, &format!("{here}.path")))
                    .transpose()?,
                send: o
                    .send
                    .as_ref()
                    .map(|s| tree(s, &format!("{here}.send")))
                    .transpose()?,
                headers: templates(&o.headers, &format!("{here}.headers"))?,
                result: expression(o.result.as_ref(), &format!("{here}.result"))?,
                error: expression(o.error.as_ref(), &format!("{here}.error"))?,
            },
        ));
    }
    Ok(Action {
        label: a.label,
        description: a.description,
        write: a.write,
        method,
        path: template(&a.path, &format!("{at}.path"))?,
        body,
        inputs,
        send: a
            .send
            .as_ref()
            .map(|s| tree(s, &format!("{at}.send")))
            .transpose()?,
        query: templates(&a.query, &format!("{at}.query"))?,
        headers: templates(&a.headers, &format!("{at}.headers"))?,
        result: expression(a.result.as_ref(), &format!("{at}.result"))?,
        items: expression(a.items.as_ref(), &format!("{at}.items"))?,
        next: expression(a.next.as_ref(), &format!("{at}.next"))?,
        max_pages,
        page_in_body,
        timeout: std::time::Duration::from_secs(timeout),
        error: expression(a.error.as_ref(), &format!("{at}.error"))?,
        by,
        output: super::quality::output_of(a.output.as_ref(), at)?, // app generator
        mcp: a.mcp,
        then, // messaging
    })
}
