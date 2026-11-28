// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! How an app made from a description signs in: each of its security schemes as a sign-in - an API key in a
//! header, the query or a cookie; a bearer token; a user name and password; OAuth 2.0's authorization code (a person
//! signs in at the vendor) or client credentials. Other schemes (OpenID Connect, implicit and password grants,
//! mutual TLS, digest) are listed as not done. API keys and tokens come first: the simplest to set up.

use base64::Engine as _;
use serde_json::{Map, Value as Json, json};

use super::rank::cut;
use super::spec::{Api, Operation};

/// The most scopes a sign-in asks for.
const MAX_SCOPES: usize = 30;

/// How an app signs in when its description does not say (given when it is made).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignInChoice {
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// A user name and password.
    Basic,
    /// A key in the header of this name.
    Header(String),
    /// A key in the query parameter of this name.
    Query(String),
}

impl std::str::FromStr for SignInChoice {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let name_ok = |n: &str| {
            !n.is_empty()
                && n.len() <= 64
                && n.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        };
        match s.split_once(':') {
            None if s == "bearer" => Ok(Self::Bearer),
            None if s == "basic" => Ok(Self::Basic),
            Some(("header", n)) if name_ok(n) => Ok(Self::Header(n.to_owned())),
            Some(("query", n)) if name_ok(n) => Ok(Self::Query(n.to_owned())),
            _ => Err(format!(
                "{s:?}: bearer, basic, header:<its name> or query:<its name>"
            )),
        }
    }
}

/// A sign-in made for an app: as its definition writes it, and how a recorded test signs in with it.
#[derive(Debug, Clone)]
pub(super) struct Made {
    pub(super) id: String,
    pub(super) label: String,
    /// How it signs in, in plain words (as the quality report says).
    pub(super) kind: &'static str,
    /// The `sign_in` entry, as JSON.
    pub(super) entry: Json,
    /// A recorded test's credential: its fields and values.
    pub(super) credential: Vec<(String, String)>,
    /// What a request carries for it: a header (its name in lowercase) or a query parameter, and the value.
    pub(super) header: Option<(String, String)>,
    pub(super) query: Option<(String, String)>,
    /// How to get it, for the README.
    pub(super) readme: String,
    /// Its place among the sign-ins: the simplest first.
    order: u8,
}

/// The app's sign-ins, and what of its description's could not be one.
#[derive(Debug, Default)]
pub(super) struct SignIns {
    pub(super) made: Vec<Made>,
    pub(super) not_done: Vec<String>,
}

fn field(name: &str, label: &str, secret: bool) -> Json {
    let mut f = json!({ "name": name, "label": label });
    if secret && let Some(o) = f.as_object_mut() {
        o.insert("secret".to_owned(), json!(true));
    }
    f
}

/// A key, from a header, the query or a cookie.
fn key(place: &str, name: &str, vendor: &str) -> Made {
    let label = if name.eq_ignore_ascii_case("authorization") {
        format!("The whole Authorization value, as {vendor}'s API reference writes it")
    } else {
        "The API key".to_owned()
    };
    let mut entry = json!({ "label": "An API key", "fields": [field("api_key", &label, true)] });
    let (way, header, query) = match place {
        "query" => (
            ("query", json!({ name: "{{ credential.api_key }}" })),
            None,
            Some((name.to_owned(), "test-key".to_owned())),
        ),
        "cookie" => (
            (
                "headers",
                json!({ "Cookie": format!("{name}={{{{ credential.api_key }}}}") }),
            ),
            Some(("cookie".to_owned(), format!("{name}=test-key"))),
            None,
        ),
        _ => (
            ("headers", json!({ name: "{{ credential.api_key }}" })),
            Some((name.to_ascii_lowercase(), "test-key".to_owned())),
            None,
        ),
    };
    if let Some(o) = entry.as_object_mut() {
        o.insert(way.0.to_owned(), way.1);
    }
    Made {
        id: String::new(),
        label: "An API key".to_owned(),
        kind: if place == "query" {
            "a key in the address"
        } else {
            "a key in a header"
        },
        entry,
        credential: vec![("api_key".to_owned(), "test-key".to_owned())],
        header,
        query,
        readme: format!(
            "an API key from {vendor}'s pages for developers; it goes in the {} `{name}`",
            match place {
                "query" => "query parameter",
                "cookie" => "cookie",
                _ => "header",
            }
        ),
        order: 0,
    }
}

fn bearer(vendor: &str) -> Made {
    Made {
        id: String::new(),
        label: "An access token".to_owned(),
        kind: "a bearer token",
        entry: json!({
            "label": "An access token",
            "fields": [field("token", "The access token (or API key) it sends as a bearer token", true)],
            "headers": { "Authorization": "Bearer {{ credential.token }}" },
        }),
        credential: vec![("token".to_owned(), "test-token".to_owned())],
        header: Some(("authorization".to_owned(), "Bearer test-token".to_owned())),
        query: None,
        readme: format!(
            "an access token or API key from {vendor}'s pages for developers, sent as `Authorization: Bearer`"
        ),
        order: 0,
    }
}

fn basic(vendor: &str) -> Made {
    let encoded = base64::engine::general_purpose::STANDARD.encode("ann:test-password");
    Made {
        id: String::new(),
        label: "A user name and password".to_owned(),
        kind: "a user name and password",
        entry: json!({
            "label": "A user name and password",
            "fields": [field("username", "User name", false), field("password", "Password", true)],
            "basic": { "username": "{{ credential.username }}", "password": "{{ credential.password }}" },
        }),
        credential: vec![
            ("username".to_owned(), "ann".to_owned()),
            ("password".to_owned(), "test-password".to_owned()),
        ],
        header: Some(("authorization".to_owned(), format!("Basic {encoded}"))),
        query: None,
        readme: format!("a user name and password of {vendor} (HTTP basic)"),
        order: 1,
    }
}

/// An absolute https address for an OAuth URL the description gives, relative ones taken from `base`.
fn oauth_url(url: &str, base: Option<&str>) -> Option<String> {
    if url.starts_with("https://") {
        return Some(url.to_owned());
    }
    let base = reqwest::Url::parse(base?).ok()?;
    let joined = base.join(url).ok()?;
    (joined.scheme() == "https").then(|| joined.to_string())
}

/// Every scope the app's actions need of a scheme, in the order they are first named.
fn scopes_of(api: &Api<'_>, chosen: &[&Operation<'_>], scheme: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let all = chosen
        .iter()
        .flat_map(|op| op.security.as_ref().unwrap_or(&api.security).iter());
    for alternative in all {
        for (name, scopes) in alternative {
            if name == scheme {
                for s in scopes {
                    if !out.contains(s) && out.len() < MAX_SCOPES {
                        out.push(s.clone());
                    }
                }
            }
        }
    }
    out
}

/// The sign-ins of one OAuth 2.0 scheme: one for each flow it has that an app can take.
fn oauth(api: &Api<'_>, name: &str, scheme: &Json, scopes: &[String], vendor: &str, out: &mut SignIns) {
    let base = api.servers.first().map(String::as_str);
    let flows = scheme
        .get("flows")
        .and_then(Json::as_object)
        .cloned()
        .unwrap_or_default();
    for (flow, f) in &flows {
        let token = f
            .get("tokenUrl")
            .and_then(Json::as_str)
            .and_then(|u| oauth_url(u, base));
        match flow.as_str() {
            "authorizationCode" => {
                let authorize = f
                    .get("authorizationUrl")
                    .and_then(Json::as_str)
                    .and_then(|u| oauth_url(u, base));
                let (Some(authorize), Some(token)) = (authorize, token) else {
                    out.not_done.push(format!(
                        "the sign-in {name}: its OAuth addresses are not https:// addresses"
                    ));
                    continue;
                };
                out.made.push(Made {
                    id: String::new(),
                    label: format!("Sign in with {vendor}"),
                    kind: "a sign-in at the vendor (OAuth 2.0)",
                    entry: json!({
                        "label": format!("Sign in with {vendor}"),
                        "oauth2": { "authorize": authorize, "token": token, "scopes": scopes },
                        "headers": { "Authorization": "Bearer {{ credential.access_token }}" },
                    }),
                    credential: vec![("access_token".to_owned(), "test-access-token".to_owned())],
                    header: Some(("authorization".to_owned(), "Bearer test-access-token".to_owned())),
                    query: None,
                    readme: oauth_readme(vendor, api.docs.as_deref(), scopes),
                    order: 3,
                });
            }
            "clientCredentials" => {
                let Some(token) = token else {
                    out.not_done.push(format!(
                        "the sign-in {name}: its token address is not an https:// address"
                    ));
                    continue;
                };
                out.made.push(Made {
                    id: String::new(),
                    label: "A client ID and secret".to_owned(),
                    kind: "a client ID and secret (OAuth 2.0)",
                    entry: json!({
                        "label": "A client ID and secret",
                        "fields": [field("client_id", "Client ID", false), field("client_secret", "Client secret", true)],
                        "oauth2": { "grant": "client_credentials", "token": token },
                        "headers": { "Authorization": "Bearer {{ credential.access_token }}" },
                    }),
                    credential: vec![
                        ("client_id".to_owned(), "test-client".to_owned()),
                        ("client_secret".to_owned(), "test-secret".to_owned()),
                        ("access_token".to_owned(), "test-access-token".to_owned()),
                    ],
                    header: Some(("authorization".to_owned(), "Bearer test-access-token".to_owned())),
                    query: None,
                    readme: format!(
                        "a client ID and secret of an app made in {vendor}'s pages for developers, allowed the client \
                         credentials grant; OpenDuct trades them for a token as it needs one"
                    ),
                    order: 2,
                });
            }
            other => out.not_done.push(format!(
                "the sign-in {name}: OAuth 2.0's {other} flow (an app takes the authorization code and client credentials flows)"
            )),
        }
    }
}

fn oauth_readme(vendor: &str, docs: Option<&str>, scopes: &[String]) -> String {
    let where_ = docs.map_or_else(
        || format!("{vendor}'s pages for developers"),
        |d| format!("{vendor}'s pages for developers ({d})"),
    );
    let scopes = if scopes.is_empty() {
        "the permissions the actions need".to_owned()
    } else {
        format!(
            "the scopes {}",
            scopes
                .iter()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    format!(
        "a sign-in at {vendor}. Once for the installation, its admin registers an app (an OAuth client) in \
         {where_}, gives it the callback address shown under **Settings → App sign-ins** \
         (`https://<your studio>/oauth/callback`), allows {scopes}, and enters its client ID and secret there. Then \
         each workspace **Connects** its account under Credentials"
    )
}

/// The app's sign-ins: from its description's security schemes - those its actions name, or all of them - with
/// `extra` (given when it was made) first. Each gets its id: the app's for the first, the app's with what it is
/// for the others.
pub(super) fn sign_ins(
    api: &Api<'_>,
    chosen: &[&Operation<'_>],
    app_id: &str,
    vendor: &str,
    extra: Option<&SignInChoice>,
) -> SignIns {
    let mut out = SignIns::default();
    if let Some(e) = extra {
        out.made.push(match e {
            SignInChoice::Bearer => bearer(vendor),
            SignInChoice::Basic => basic(vendor),
            SignInChoice::Header(n) => key("header", n, vendor),
            SignInChoice::Query(n) => key("query", n, vendor),
        });
    }
    for (name, scheme) in &api.schemes {
        let kind = scheme.get("type").and_then(Json::as_str).unwrap_or_default();
        match kind {
            "apiKey" => {
                let place = scheme.get("in").and_then(Json::as_str).unwrap_or("header");
                let Some(header) = scheme
                    .get("name")
                    .and_then(Json::as_str)
                    .filter(|n| !n.is_empty())
                else {
                    out.not_done
                        .push(format!("the sign-in {name}: it names no header or parameter"));
                    continue;
                };
                out.made.push(key(place, header, vendor));
            }
            "http" => match scheme
                .get("scheme")
                .and_then(Json::as_str)
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("bearer") => out.made.push(bearer(vendor)),
                Some("basic") => out.made.push(basic(vendor)),
                other => out.not_done.push(format!(
                    "the sign-in {name}: HTTP {} (an app takes bearer and basic)",
                    other.unwrap_or("of no scheme")
                )),
            },
            "oauth2" => {
                let scopes = scopes_of(api, chosen, name);
                oauth(api, name, scheme, &scopes, vendor, &mut out);
            }
            "openIdConnect" => out.not_done.push(format!(
                "the sign-in {name}: OpenID Connect (give its OAuth 2.0 addresses by hand)"
            )),
            "mutualTLS" => out
                .not_done
                .push(format!("the sign-in {name}: mutual TLS (a client certificate)")),
            other => out
                .not_done
                .push(format!("the sign-in {name}: of the kind {other:?}")),
        }
    }
    // The same sign-in twice (two schemes for one key) once.
    let mut seen: Vec<Json> = Vec::new();
    out.made.retain(|m| {
        let fresh = !seen.contains(&m.entry);
        seen.push(m.entry.clone());
        fresh
    });
    out.made.sort_by_key(|m| m.order);
    let mut ids: Vec<String> = Vec::new();
    for (i, m) in out.made.iter_mut().enumerate() {
        let suffix = match m.kind {
            "a bearer token" => "token",
            "a user name and password" => "basic",
            "a client ID and secret (OAuth 2.0)" => "client",
            "a sign-in at the vendor (OAuth 2.0)" => "oauth",
            _ => "key",
        };
        let mut id = if i == 0 {
            cut(app_id)
        } else {
            let room = super::rank::MAX_NAME - suffix.len() - 1;
            format!(
                "{}_{suffix}",
                app_id.get(..app_id.len().min(room)).unwrap_or(app_id)
            )
        };
        let mut n = 2;
        while ids.contains(&id) {
            id = format!(
                "{}_{n}",
                id.get(..id.len().min(super::rank::MAX_NAME - 3)).unwrap_or(&id)
            );
            n += 1;
        }
        ids.push(id.clone());
        m.id = id.clone();
        let mut entry = Map::new();
        entry.insert("id".to_owned(), json!(id));
        if let Some(fields) = m.entry.as_object() {
            entry.extend(fields.clone());
        }
        m.entry = Json::Object(entry);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    #[test]
    fn each_scheme_becomes_a_sign_in_the_simplest_first() {
        let doc = json!({
            "openapi": "3.0.3", "servers": [{ "url": "https://api.shop.example/v1" }], "paths": {
                "/orders": { "get": { "security": [{ "code": ["orders:read"] }], "responses": {} } },
            },
            "components": { "securitySchemes": {
                "code": { "type": "oauth2", "flows": {
                    "authorizationCode": { "authorizationUrl": "https://shop.example/oauth/authorize", "tokenUrl": "/oauth/token",
                                           "scopes": { "orders:read": "", "orders:write": "" } },
                    "implicit": { "authorizationUrl": "https://shop.example/oauth/authorize", "scopes": {} },
                    "clientCredentials": { "tokenUrl": "https://shop.example/oauth/token", "scopes": {} },
                } },
                "key": { "type": "apiKey", "in": "header", "name": "X-Shop-Key" },
                "query_key": { "type": "apiKey", "in": "query", "name": "api_key" },
                "token": { "type": "http", "scheme": "Bearer" },
                "user": { "type": "http", "scheme": "basic" },
                "oidc": { "type": "openIdConnect", "openIdConnectUrl": "https://shop.example/.well-known" },
                "digest": { "type": "http", "scheme": "digest" },
            } },
        });
        let api = super::super::spec::read(&doc, None).unwrap();
        let ops: Vec<&Operation<'_>> = api.operations.iter().collect();
        let s = sign_ins(&api, &ops, "shop", "Shop", None);
        let ids: Vec<&str> = s.made.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "shop",
                "shop_key",
                "shop_token",
                "shop_basic",
                "shop_client",
                "shop_oauth"
            ]
        );
        assert_eq!(
            s.made[0].entry["headers"]["X-Shop-Key"],
            json!("{{ credential.api_key }}")
        );
        assert_eq!(
            s.made[1].entry["query"]["api_key"],
            json!("{{ credential.api_key }}")
        );
        assert_eq!(
            s.made[3].header.as_ref().unwrap().1,
            "Basic YW5uOnRlc3QtcGFzc3dvcmQ="
        );
        let oauth = &s.made[5].entry["oauth2"];
        // Its token address is taken from the server; its scopes are those the actions need.
        assert_eq!(oauth["token"], json!("https://api.shop.example/oauth/token"));
        assert_eq!(oauth["scopes"], json!(["orders:read"]));
        assert!(s.made[5].readme.contains("/oauth/callback"));
        assert_eq!(s.not_done.len(), 3, "{:?}", s.not_done);
        assert!(s.not_done.iter().any(|n| n.contains("implicit")));
        assert!(s.not_done.iter().any(|n| n.contains("OpenID")));
        assert!(s.not_done.iter().any(|n| n.contains("digest")));
    }

    #[test]
    fn a_sign_in_given_by_hand_comes_first() {
        let doc = json!({ "swagger": "2.0", "host": "api.x.example", "paths": {} });
        let api = super::super::spec::read(&doc, None).unwrap();
        let none = sign_ins(&api, &[], "x", "X", None);
        assert!(none.made.is_empty());
        let given: SignInChoice = "header:X-Api-Key".parse().unwrap();
        let s = sign_ins(&api, &[], "x", "X", Some(&given));
        assert_eq!(s.made[0].id, "x");
        assert_eq!(
            s.made[0].header,
            Some(("x-api-key".to_owned(), "test-key".to_owned()))
        );
        assert!("header:".parse::<SignInChoice>().is_err());
        assert!("cookie:a".parse::<SignInChoice>().is_err());
        assert_eq!("basic".parse::<SignInChoice>().unwrap(), SignInChoice::Basic);
    }
}
