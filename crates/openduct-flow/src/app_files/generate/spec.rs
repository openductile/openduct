// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An API description - OpenAPI 3.0 or 3.1, or Swagger 2.0 - read into one shape: where the API is, how it signs in,
//! and each operation's parameters, body and answer. A `$ref` is followed within the description; one into another
//! file is not (said, and left out).

use std::borrow::Cow;

use serde_json::{Map, Value as Json, json};

/// The most `$ref`s followed one after another.
const MAX_HOPS: usize = 32;
/// The most operations read from a description.
pub(super) const MAX_OPERATIONS: usize = 5_000;
/// The methods an operation may have that an app can make.
const METHODS: &[&str] = &["get", "post", "put", "patch", "delete"];

static NOTHING: Json = Json::Null;

/// What security an operation needs: alternatives, each a list of (scheme, scopes).
pub(super) type Security = Vec<Vec<(String, Vec<String>)>>;

/// Which description language it is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Dialect {
    Swagger2,
    OpenApi30,
    OpenApi31,
}

/// Where a parameter goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Place {
    Path,
    Query,
    Header,
    Cookie,
}

#[derive(Debug)]
pub(super) struct Param<'a> {
    pub(super) name: String,
    pub(super) place: Place,
    pub(super) required: bool,
    pub(super) description: String,
    pub(super) schema: Cow<'a, Json>,
}

/// What a request's body is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BodyKind {
    Json,
    Form,
    /// A file upload, or anything else multipart.
    Multipart,
    Xml,
    Other(String),
}

#[derive(Debug)]
pub(super) struct Body<'a> {
    pub(super) kind: BodyKind,
    pub(super) required: bool,
    pub(super) schema: Cow<'a, Json>,
}

/// An operation's answer when it succeeds.
#[derive(Debug)]
pub(super) struct Answer<'a> {
    pub(super) status: u16,
    /// Its JSON body's schema; none for an answer with no body, or one that is not JSON.
    pub(super) schema: Option<Cow<'a, Json>>,
    /// It says it sends a `Link` header.
    pub(super) link: bool,
    /// It has a body that is not JSON.
    pub(super) not_json: Option<String>,
}

#[derive(Debug)]
pub(super) struct Operation<'a> {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) id: String,
    pub(super) summary: String,
    pub(super) description: String,
    pub(super) tags: Vec<String>,
    pub(super) deprecated: bool,
    pub(super) params: Vec<Param<'a>>,
    pub(super) body: Option<Body<'a>>,
    pub(super) answer: Option<Answer<'a>>,
    /// The security it needs, when it says (else the description's own): alternatives, each a list of
    /// (scheme, scopes).
    pub(super) security: Option<Security>,
    pub(super) callbacks: bool,
    /// What of it could not be read (a `$ref` into another file).
    pub(super) unread: Vec<String>,
}

/// A description, read.
#[derive(Debug)]
pub(super) struct Api<'a> {
    pub(super) root: &'a Json,
    pub(super) dialect: Dialect,
    pub(super) title: String,
    pub(super) description: String,
    pub(super) version: String,
    pub(super) docs: Option<String>,
    /// Its servers' addresses, worked out: absolute, variables at their defaults.
    pub(super) servers: Vec<String>,
    /// What was assumed about the servers (a variable's default).
    pub(super) server_notes: Vec<String>,
    /// Its security schemes by name, as OpenAPI 3 writes them (Swagger 2.0's are rewritten so).
    pub(super) schemes: Vec<(String, Json)>,
    pub(super) security: Security,
    pub(super) operations: Vec<Operation<'a>>,
    /// Its webhooks (OpenAPI 3.1), which an app does not take.
    pub(super) webhooks: usize,
    /// Operations left unread beyond the most read.
    pub(super) cut: usize,
}

/// The node a `$ref` points at, followed until it is not a `$ref`: within the description only.
pub(super) fn resolve<'a>(root: &'a Json, node: &'a Json) -> Result<&'a Json, String> {
    let mut at = node;
    for _ in 0..MAX_HOPS {
        let Some(target) = at.get("$ref").and_then(Json::as_str) else {
            return Ok(at);
        };
        let Some(pointer) = target.strip_prefix('#') else {
            return Err(format!("{target} is in another file"));
        };
        let pointer = percent_decoded(pointer);
        at = root
            .pointer(&pointer)
            .ok_or_else(|| format!("{target} points at nothing"))?;
    }
    Err(format!("$refs point at $refs more than {MAX_HOPS} times"))
}

/// A `$ref`'s pointer with its %XX escapes undone (`%7B` for `{`).
fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes.get(i) == Some(&b'%')
            && let Some(hex) = text.get(i + 1..i + 3)
            && let Ok(b) = u8::from_str_radix(hex, 16)
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.extend(bytes.get(i));
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn text(node: &Json, name: &str) -> String {
    node.get(name)
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// A security requirement list: alternatives, each a list of (scheme, scopes).
fn requirements(node: Option<&Json>) -> Option<Security> {
    let list = node?.as_array()?;
    Some(
        list.iter()
            .filter_map(Json::as_object)
            .map(|alternative| {
                alternative
                    .iter()
                    .map(|(scheme, scopes)| {
                        let scopes = scopes
                            .as_array()
                            .map(|s| s.iter().filter_map(Json::as_str).map(str::to_owned).collect())
                            .unwrap_or_default();
                        (scheme.clone(), scopes)
                    })
                    .collect()
            })
            .collect(),
    )
}

/// Reads a description. `origin` is where it was fetched from (`https://host`), for servers it names relatively.
pub(super) fn read<'a>(root: &'a Json, origin: Option<&str>) -> Result<Api<'a>, String> {
    let dialect = if let Some(v) = root.get("openapi").and_then(Json::as_str) {
        if v.starts_with("3.0") {
            Dialect::OpenApi30
        } else if v.starts_with("3.1") {
            Dialect::OpenApi31
        } else {
            return Err(format!(
                "OpenAPI {v}: OpenDuct reads OpenAPI 3.0 and 3.1, and Swagger 2.0"
            ));
        }
    } else if root.get("swagger").and_then(Json::as_str) == Some("2.0") {
        Dialect::Swagger2
    } else {
        return Err(
            "this is not an OpenAPI 3.0 or 3.1 description, nor a Swagger 2.0 one (it names neither `openapi` nor `swagger`)"
                .to_owned(),
        );
    };
    let info = root.get("info").unwrap_or(&NOTHING);
    let docs = root
        .pointer("/externalDocs/url")
        .or_else(|| info.pointer("/contact/url"))
        .and_then(Json::as_str)
        .filter(|u| u.starts_with("https://"))
        .map(str::to_owned);
    let (servers, server_notes) = servers(root, dialect, origin);
    let schemes = schemes(root, dialect);
    let security = requirements(root.get("security")).unwrap_or_default();
    let mut operations = Vec::new();
    let mut cut = 0;
    if let Some(paths) = root.get("paths").and_then(Json::as_object) {
        for (path, item) in paths {
            let Ok(item) = resolve(root, item) else {
                continue;
            };
            for method in METHODS {
                let Some(op) = item.get(*method) else {
                    continue;
                };
                if operations.len() >= MAX_OPERATIONS {
                    cut += 1;
                    continue;
                }
                operations.push(operation(root, dialect, path, method, item, op));
            }
        }
    }
    let webhooks = root.get("webhooks").and_then(Json::as_object).map_or(0, Map::len);
    Ok(Api {
        root,
        dialect,
        title: text(info, "title"),
        description: text(info, "description"),
        version: text(info, "version"),
        docs,
        servers,
        server_notes,
        schemes,
        security,
        operations,
        webhooks,
        cut,
    })
}

/// The servers' addresses, worked out, and what was assumed to work them out.
fn servers(root: &Json, dialect: Dialect, origin: Option<&str>) -> (Vec<String>, Vec<String>) {
    let mut out = Vec::new();
    let mut notes = Vec::new();
    let absolute = |url: String| -> Option<String> {
        if url.starts_with("https://") || url.starts_with("http://") {
            return Some(url);
        }
        // A server named from where the description is: `/v1` or `//api.example.com/v1`.
        let origin = origin?;
        if let Some(rest) = url.strip_prefix("//") {
            return Some(format!("https://{rest}"));
        }
        Some(format!(
            "{}/{}",
            origin.trim_end_matches('/'),
            url.trim_start_matches('/')
        ))
    };
    if dialect == Dialect::Swagger2 {
        let schemes: Vec<&str> = root
            .get("schemes")
            .and_then(Json::as_array)
            .map(|s| s.iter().filter_map(Json::as_str).collect())
            .unwrap_or_default();
        let scheme = if schemes.is_empty() || schemes.contains(&"https") {
            "https"
        } else {
            "http"
        };
        let base = text(root, "basePath");
        let url = match root.get("host").and_then(Json::as_str) {
            Some(host) => Some(format!("{scheme}://{host}{base}")),
            None => absolute(if base.is_empty() { "/".to_owned() } else { base }),
        };
        out.extend(url);
        return (out, notes);
    }
    for server in root.get("servers").and_then(Json::as_array).into_iter().flatten() {
        let mut url = text(server, "url");
        if let Some(vars) = server.get("variables").and_then(Json::as_object) {
            for (name, v) in vars {
                let default = v
                    .get("default")
                    .map(|d| match d {
                        Json::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                if url.contains(&format!("{{{name}}}")) {
                    notes.push(format!(
                        "the server's {name} is taken as {default:?}, its default"
                    ));
                    url = url.replace(&format!("{{{name}}}"), &default);
                }
            }
        }
        if url.contains('{') {
            continue;
        }
        out.extend(absolute(url));
    }
    if out.is_empty()
        && root.get("servers").is_none()
        && let Some(u) = absolute("/".to_owned())
    {
        // OpenAPI 3: no servers means the description's own place.
        out.push(u);
    }
    (out, notes)
}

/// The security schemes, as OpenAPI 3 writes them.
fn schemes(root: &Json, dialect: Dialect) -> Vec<(String, Json)> {
    let declared = match dialect {
        Dialect::Swagger2 => root.get("securityDefinitions"),
        _ => root.pointer("/components/securitySchemes"),
    };
    let Some(declared) = declared.and_then(Json::as_object) else {
        return Vec::new();
    };
    declared
        .iter()
        .filter_map(|(name, s)| {
            let s = resolve(root, s).ok()?;
            let scheme = if dialect == Dialect::Swagger2 {
                swagger_scheme(s)
            } else {
                s.clone()
            };
            Some((name.clone(), scheme))
        })
        .collect()
}

/// A Swagger 2.0 security definition, as OpenAPI 3 writes it.
fn swagger_scheme(s: &Json) -> Json {
    match s.get("type").and_then(Json::as_str) {
        Some("basic") => json!({ "type": "http", "scheme": "basic", "description": s.get("description") }),
        Some("oauth2") => {
            let flow = match s.get("flow").and_then(Json::as_str) {
                Some("accessCode") => "authorizationCode",
                Some("application") => "clientCredentials",
                Some(other) => other,
                None => "",
            };
            json!({
                "type": "oauth2",
                "flows": { flow: {
                    "authorizationUrl": s.get("authorizationUrl"), "tokenUrl": s.get("tokenUrl"),
                    "scopes": s.get("scopes").cloned().unwrap_or_else(|| json!({})),
                } },
            })
        }
        _ => s.clone(),
    }
}

fn place(name: &str) -> Option<Place> {
    match name {
        "path" => Some(Place::Path),
        "query" => Some(Place::Query),
        "header" => Some(Place::Header),
        "cookie" => Some(Place::Cookie),
        _ => None,
    }
}

/// The media type of a body to take, of those an operation offers: JSON first, then a form.
fn media(content: &Map<String, Json>) -> Option<(BodyKind, &Json)> {
    let kind = |t: &str| {
        let t = t
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if t == "application/json" || t.ends_with("+json") || t == "*/*" || t == "application/*" {
            BodyKind::Json
        } else if t == "application/x-www-form-urlencoded" {
            BodyKind::Form
        } else if t.starts_with("multipart/") || t == "application/octet-stream" {
            BodyKind::Multipart
        } else if t.ends_with("/xml") || t.ends_with("+xml") {
            BodyKind::Xml
        } else {
            BodyKind::Other(t)
        }
    };
    let rank = |k: &BodyKind| match k {
        BodyKind::Json => 0,
        BodyKind::Form => 1,
        BodyKind::Multipart => 2,
        BodyKind::Xml => 3,
        BodyKind::Other(_) => 4,
    };
    content
        .iter()
        .map(|(t, m)| (kind(t), m))
        .min_by_key(|(k, _)| rank(k))
}

fn operation<'a>(
    root: &'a Json,
    dialect: Dialect,
    path: &str,
    method: &str,
    item: &'a Json,
    op: &'a Json,
) -> Operation<'a> {
    let mut unread = Vec::new();
    let mut params: Vec<Param<'a>> = Vec::new();
    let mut body: Option<Body<'a>> = None;
    let mut form = Map::new();
    let mut form_required = Vec::new();
    let mut multipart = false;
    // The path's parameters first; the operation's own take the place of one of the same name and place.
    let listed = item
        .get("parameters")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .chain(
            op.get("parameters")
                .and_then(Json::as_array)
                .into_iter()
                .flatten(),
        );
    for p in listed {
        let p = match resolve(root, p) {
            Ok(p) => p,
            Err(e) => {
                unread.push(format!("a parameter ({e})"));
                continue;
            }
        };
        let name = text(p, "name");
        let at = text(p, "in");
        let required = p.get("required").and_then(Json::as_bool).unwrap_or(false);
        let description = text(p, "description");
        if dialect == Dialect::Swagger2 && at == "body" {
            body = Some(Body {
                kind: BodyKind::Json,
                required,
                schema: Cow::Borrowed(p.get("schema").unwrap_or(&NOTHING)),
            });
            continue;
        }
        if dialect == Dialect::Swagger2 && at == "formData" {
            multipart |= p.get("type").and_then(Json::as_str) == Some("file");
            let mut schema = p.clone();
            if let Some(fields) = schema.as_object_mut() {
                for k in ["name", "in", "required"] {
                    fields.remove(k);
                }
            }
            form.insert(name.clone(), schema);
            if required {
                form_required.push(Json::String(name));
            }
            continue;
        }
        let Some(place) = place(&at) else {
            continue;
        };
        let schema = match (dialect, p.get("schema")) {
            // Swagger 2.0 writes a parameter's type on the parameter itself.
            (Dialect::Swagger2, _) => Cow::Borrowed(p),
            (_, Some(s)) => Cow::Borrowed(s),
            (_, None) => match p.get("content").and_then(Json::as_object).and_then(media) {
                Some((_, m)) => Cow::Borrowed(m.get("schema").unwrap_or(&NOTHING)),
                None => Cow::Borrowed(&NOTHING),
            },
        };
        params.retain(|q| !(q.name == name && q.place == place));
        params.push(Param {
            name,
            place,
            required: required || place == Place::Path,
            description,
            schema,
        });
    }
    if !form.is_empty() {
        let consumes_multipart = op
            .get("consumes")
            .or_else(|| root.get("consumes"))
            .and_then(Json::as_array)
            .is_some_and(|c| {
                c.iter()
                    .filter_map(Json::as_str)
                    .all(|t| t.starts_with("multipart/"))
            });
        body = Some(Body {
            kind: if multipart || consumes_multipart {
                BodyKind::Multipart
            } else {
                BodyKind::Form
            },
            required: !form_required.is_empty(),
            schema: Cow::Owned(json!({ "type": "object", "properties": form, "required": form_required })),
        });
    }
    if dialect == Dialect::Swagger2 {
        // A JSON body unless the operation takes XML alone.
        if let Some(b) = &mut body
            && b.kind == BodyKind::Json
        {
            let consumes: Vec<&str> = op
                .get("consumes")
                .or_else(|| root.get("consumes"))
                .and_then(Json::as_array)
                .map(|c| c.iter().filter_map(Json::as_str).collect())
                .unwrap_or_default();
            if !consumes.is_empty() && consumes.iter().all(|t| t.contains("xml")) {
                b.kind = BodyKind::Xml;
            }
        }
    } else if let Some(rb) = op.get("requestBody") {
        match resolve(root, rb) {
            Ok(rb) => {
                let required = rb.get("required").and_then(Json::as_bool).unwrap_or(false);
                if let Some((kind, m)) = rb.get("content").and_then(Json::as_object).and_then(media) {
                    body = Some(Body {
                        kind,
                        required,
                        schema: Cow::Borrowed(m.get("schema").unwrap_or(&NOTHING)),
                    });
                }
            }
            Err(e) => unread.push(format!("the request's body ({e})")),
        }
    }
    let answer = answer(root, dialect, op, &mut unread);
    Operation {
        method: method.to_ascii_uppercase(),
        path: path.to_owned(),
        id: text(op, "operationId"),
        summary: text(op, "summary"),
        description: text(op, "description"),
        tags: op
            .get("tags")
            .and_then(Json::as_array)
            .map(|t| t.iter().filter_map(Json::as_str).map(str::to_owned).collect())
            .unwrap_or_default(),
        deprecated: op.get("deprecated").and_then(Json::as_bool).unwrap_or(false),
        params,
        body,
        answer,
        security: requirements(op.get("security")),
        callbacks: op
            .get("callbacks")
            .and_then(Json::as_object)
            .is_some_and(|c| !c.is_empty()),
        unread,
    }
}

/// The answer an operation gives when it succeeds: its first 2xx.
fn answer<'a>(
    root: &'a Json,
    dialect: Dialect,
    op: &'a Json,
    unread: &mut Vec<String>,
) -> Option<Answer<'a>> {
    let responses = op.get("responses")?.as_object()?;
    let mut codes: Vec<(&String, &Json)> = responses
        .iter()
        .filter(|(code, _)| code.starts_with('2'))
        .collect();
    codes.sort_by_key(|(code, _)| (*code).clone());
    let (code, response) = codes.first()?;
    let status = code.parse::<u16>().unwrap_or(200);
    let response = match resolve(root, response) {
        Ok(r) => r,
        Err(e) => {
            unread.push(format!("the answer ({e})"));
            return Some(Answer {
                status,
                schema: None,
                link: false,
                not_json: None,
            });
        }
    };
    let link = response
        .get("headers")
        .and_then(Json::as_object)
        .is_some_and(|h| h.keys().any(|k| k.eq_ignore_ascii_case("link")));
    let (schema, not_json) = if dialect == Dialect::Swagger2 {
        let produces: Vec<&str> = op
            .get("produces")
            .or_else(|| root.get("produces"))
            .and_then(Json::as_array)
            .map(|c| c.iter().filter_map(Json::as_str).collect())
            .unwrap_or_default();
        let json = produces.is_empty() || produces.iter().any(|t| t.contains("json"));
        match response.get("schema") {
            Some(s) if json => (Some(Cow::Borrowed(s)), None),
            Some(_) => (None, produces.first().map(|t| (*t).to_owned())),
            None => (None, None),
        }
    } else {
        match response.get("content").and_then(Json::as_object).and_then(media) {
            Some((BodyKind::Json, m)) => (Some(Cow::Borrowed(m.get("schema").unwrap_or(&NOTHING))), None),
            Some((other, _)) => (None, Some(format!("{other:?}").to_ascii_lowercase())),
            None => (None, None),
        }
    };
    Some(Answer {
        status,
        schema,
        link,
        not_json,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    #[test]
    fn swagger_2_and_openapi_3_read_to_the_same_shape() {
        let swagger = json!({
            "swagger": "2.0", "info": { "title": "Shop", "version": "1" },
            "host": "api.shop.example", "basePath": "/v1", "schemes": ["https"],
            "securityDefinitions": {
                "key": { "type": "apiKey", "in": "header", "name": "X-Key" },
                "code": { "type": "oauth2", "flow": "accessCode", "authorizationUrl": "https://shop.example/auth",
                          "tokenUrl": "https://shop.example/token", "scopes": { "orders": "Orders" } },
            },
            "parameters": { "Limit": { "name": "limit", "in": "query", "type": "integer" } },
            "paths": { "/orders/{id}": {
                "parameters": [{ "name": "id", "in": "path", "type": "string" }],
                "get": { "operationId": "getOrder", "parameters": [{ "$ref": "#/parameters/Limit" }],
                         "responses": { "200": { "schema": { "$ref": "#/definitions/Order" } } } },
                "post": { "parameters": [{ "name": "note", "in": "formData", "type": "string", "required": true }],
                          "responses": { "201": { "description": "ok" } } },
            } },
            "definitions": { "Order": { "type": "object", "properties": { "id": { "type": "string" } } } },
        });
        let api = read(&swagger, None).unwrap();
        assert_eq!(api.dialect, Dialect::Swagger2);
        assert_eq!(api.servers, ["https://api.shop.example/v1"]);
        assert_eq!(
            api.schemes[1].1["flows"]["authorizationCode"]["tokenUrl"],
            json!("https://shop.example/token")
        );
        let get = &api.operations[0];
        assert_eq!((get.method.as_str(), get.path.as_str()), ("GET", "/orders/{id}"));
        assert_eq!(get.params.len(), 2);
        assert!(get.params[0].required, "a path parameter is required");
        assert_eq!(get.params[1].schema["type"], json!("integer"));
        let answer = get.answer.as_ref().unwrap();
        let schema = resolve(&swagger, answer.schema.as_ref().unwrap()).unwrap();
        assert_eq!(schema["properties"]["id"]["type"], json!("string"));
        let post = &api.operations[1];
        let body = post.body.as_ref().unwrap();
        assert_eq!(body.kind, BodyKind::Form);
        assert_eq!(body.schema["required"], json!(["note"]));

        let openapi = json!({
            "openapi": "3.1.0", "info": { "title": "Shop", "version": "1" },
            "servers": [{ "url": "https://{region}.shop.example/v1", "variables": { "region": { "default": "eu" } } }],
            "paths": { "/orders": { "post": {
                "requestBody": { "content": { "application/xml": {}, "application/json": { "schema": { "type": "object" } } } },
                "responses": { "201": { "content": { "application/json": { "schema": { "type": "object" } } },
                                        "headers": { "Link": { "schema": { "type": "string" } } } } },
            } } },
            "webhooks": { "orderPaid": {} },
        });
        let api = read(&openapi, None).unwrap();
        assert_eq!(api.servers, ["https://eu.shop.example/v1"]);
        assert!(api.server_notes[0].contains("region"));
        let post = &api.operations[0];
        assert_eq!(post.body.as_ref().unwrap().kind, BodyKind::Json);
        assert!(post.answer.as_ref().unwrap().link);
        assert_eq!(api.webhooks, 1);
    }

    #[test]
    fn refs_are_followed_within_the_description_only() {
        let root = json!({ "a": { "$ref": "#/b" }, "b": { "$ref": "#/c~1d" }, "c/d": 1, "x": { "$ref": "other.yaml#/x" },
                           "loop": { "$ref": "#/loop" } });
        assert_eq!(resolve(&root, &root["a"]).unwrap(), &json!(1));
        assert!(resolve(&root, &root["x"]).unwrap_err().contains("another file"));
        assert!(resolve(&root, &root["loop"]).unwrap_err().contains("more than"));
        assert!(
            read(&json!({ "openapi": "2.5" }), None)
                .unwrap_err()
                .contains("3.0 and 3.1")
        );
        assert!(
            read(&json!({ "info": {} }), None)
                .unwrap_err()
                .contains("neither")
        );
        // A server named relatively is where the description came from.
        let relative = json!({ "openapi": "3.0.3", "servers": [{ "url": "/api/v2" }], "paths": {} });
        assert_eq!(
            read(&relative, Some("https://docs.shop.example"))
                .unwrap()
                .servers,
            ["https://docs.shop.example/api/v2"]
        );
    }
}
