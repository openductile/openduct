// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One operation made into an action - its inputs from its parameters and body (named plainly), what it sends, its
//! output, its pages - and its recorded test, worked out from the description alone: what the request must be, and
//! an answer that fits the answer's schema.

use serde_json::{Map, Value as Json, json};

use super::auth::Made as SignIn;
use super::pages::{self, Listing, Paging, cel};
use super::rank::{Candidate, MAX_NAME, Verb, cut, sentence, snake, words};
use super::schema::{Purpose, kind, merged, properties, sample};
use super::spec::{Api, BodyKind, Operation, Place};

/// The most inputs an action is given.
const MAX_INPUTS: usize = 40;
/// The most fields an output names.
const MAX_OUTPUT: usize = crate::app_files::quality::MAX_OUTPUT;
/// Headers a request carries of itself, or a sign-in: never inputs.
const OWN_HEADERS: &[&str] = &[
    "accept",
    "accept-encoding",
    "authorization",
    "content-length",
    "content-type",
    "cookie",
    "host",
    "user-agent",
];

/// What an action is made with: the description, the API's address and origin, and the app's first sign-in (its
/// recordings sign in with it).
pub(super) struct Context<'x, 'a> {
    pub(super) api: &'x Api<'a>,
    pub(super) base: &'x str,
    pub(super) origin: &'x str,
    pub(super) sign_in: &'x SignIn,
}

/// An action as made.
#[derive(Debug)]
pub(super) struct Built {
    pub(super) name: String,
    pub(super) label: String,
    pub(super) description: String,
    pub(super) definition: Json,
    pub(super) recording: Json,
    pub(super) notes: Vec<String>,
    pub(super) read: bool,
}

#[derive(Debug)]
struct In {
    name: String,
    label: String,
    description: String,
    required: bool,
    kind: &'static str,
    schema: Json,
    /// The vendor's own name for it.
    original: String,
}

/// Where a body's values come from: an input, a value of its own, or a record of them.
#[derive(Debug)]
enum Slot {
    Input(usize),
    Fixed(Json),
    Record(Vec<(String, Slot)>),
}

/// The inputs of an action, named once each.
#[derive(Debug, Default)]
struct Inputs {
    list: Vec<In>,
}

impl Inputs {
    /// Adds an input; its index, or none when the action has as many as it may.
    fn add(
        &mut self,
        original: &str,
        description: &str,
        required: bool,
        schema: &Json,
        root: &Json,
    ) -> Option<usize> {
        if self.list.len() >= MAX_INPUTS {
            return None;
        }
        let mut name = cut(&snake(original));
        if name.is_empty() {
            name = "value".to_owned();
        }
        if matches!(name.as_str(), "credential" | "action") {
            name = format!("{name}_field");
        }
        let base = name
            .get(..name.len().min(MAX_NAME - 3))
            .unwrap_or(&name)
            .to_owned();
        let mut n = 2;
        while self.list.iter().any(|i| i.name == name) {
            name = format!("{base}_{n}");
            n += 1;
        }
        let label = sentence(description, 80).trim_end_matches('.').trim().to_owned();
        let pointer = label.contains("://") || label.to_ascii_lowercase().starts_with("for more information");
        let label = if label.is_empty() || pointer {
            words(&name)
        } else {
            label
        };
        let long = sentence(description, 300);
        let description = if long.len() > label.len() + 10 {
            long
        } else {
            String::new()
        };
        self.list.push(In {
            name,
            label,
            description,
            required,
            kind: kind(root, schema),
            schema: schema.clone(),
            original: original.to_owned(),
        });
        Some(self.list.len() - 1)
    }
}

/// `{{ input.<name> }}`.
fn template(name: &str) -> String {
    format!("{{{{ {} }}}}", cel("input", &[name.to_owned()]))
}

/// A value as a query or a path carries it: text as it is, anything else as JSON.
fn as_text(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Null => String::new(),
        other => other.to_string(),
    }
}

/// A single value a field must have (`const`, or an enum of one): sent as it is, not asked for.
fn fixed(root: &Json, schema: &Json) -> Option<Json> {
    let s = merged(root, schema);
    if let Some(c) = s.get("const") {
        return Some(c.clone());
    }
    match s.get("enum").and_then(Json::as_array).map(Vec::as_slice) {
        Some([only]) => Some(only.clone()),
        _ => None,
    }
}

/// A JSON body's fields as inputs: a record of one record (`{data: {...}}`) and JSON:API's `attributes` are opened,
/// so their fields are inputs of their own.
fn body_slots(
    root: &Json,
    schema: &Json,
    required: bool,
    inputs: &mut Inputs,
    notes: &mut Vec<String>,
    depth: usize,
) -> Slot {
    let props = properties(root, schema, Purpose::Request);
    let mut out = Vec::new();
    let single_wrapper = props.len() == 1;
    for (name, p, needed) in props {
        let needed = needed && required;
        if let Some(v) = fixed(root, &p) {
            out.push((name, Slot::Fixed(v)));
            continue;
        }
        let opens = depth < 3
            && kind(root, &p) == "record"
            && !properties(root, &p, Purpose::Request).is_empty()
            && (single_wrapper || name == "attributes");
        if opens {
            out.push((name, body_slots(root, &p, needed, inputs, notes, depth + 1)));
            continue;
        }
        let description = merged(root, &p)
            .get("description")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned();
        match inputs.add(&name, &description, needed, &p, root) {
            Some(i) => out.push((name, Slot::Input(i))),
            None => notes.push(format!(
                "the body's field {name} (an action takes at most {MAX_INPUTS} inputs)"
            )),
        }
    }
    Slot::Record(out)
}

/// What `send` writes for a slot.
fn send_of(slot: &Slot, inputs: &Inputs) -> Json {
    match slot {
        Slot::Input(i) => json!(inputs.list.get(*i).map(|x| template(&x.name)).unwrap_or_default()),
        Slot::Fixed(v) => v.clone(),
        Slot::Record(fields) => Json::Object(
            fields
                .iter()
                .map(|(k, s)| (k.clone(), send_of(s, inputs)))
                .collect(),
        ),
    }
}

/// What the request's body holds for a slot, given the recording's inputs: those not given left out, and records
/// left empty by that.
fn sent_of(slot: &Slot, inputs: &Inputs, given: &Map<String, Json>) -> Json {
    match slot {
        Slot::Input(i) => inputs
            .list
            .get(*i)
            .and_then(|x| given.get(&x.name))
            .cloned()
            .unwrap_or(Json::Null),
        Slot::Fixed(v) => v.clone(),
        Slot::Record(fields) => {
            let mut out = Map::new();
            for (k, s) in fields {
                let v = sent_of(s, inputs, given);
                let empty = v.is_null() || v.as_object().is_some_and(Map::is_empty);
                if !empty {
                    out.insert(k.clone(), v);
                }
            }
            Json::Object(out)
        }
    }
}

/// A body as the engine sends it: fields left empty left out at any depth, and records left empty by that.
fn pruned(v: Json) -> Json {
    match v {
        Json::Object(fields) => Json::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, pruned(v)))
                .filter(|(_, v)| !v.is_null() && !v.as_object().is_some_and(Map::is_empty))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.into_iter().map(pruned).collect()),
        other => other,
    }
}

/// The fields an answer's record holds, with their kinds.
fn output_of(root: &Json, schema: &Json) -> Vec<(String, String)> {
    properties(root, schema, Purpose::Answer)
        .into_iter()
        .take(MAX_OUTPUT)
        .filter(|(name, _, _)| !name.is_empty() && name.len() <= 100)
        .map(|(name, p, _)| (name, kind(root, &p).to_owned()))
        .collect()
}

fn object(pairs: Vec<(&str, Json)>) -> Json {
    Json::Object(
        pairs
            .into_iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| (k.to_owned(), v))
            .collect(),
    )
}

/// Makes the action for operation `op` (ranked as `candidate`).
pub(super) fn action(cx: &Context<'_, '_>, op: &Operation<'_>, candidate: &Candidate) -> Built {
    let root = cx.api.root;
    let mut notes: Vec<String> = op
        .unread
        .iter()
        .map(|u| format!("{}: {u}", candidate.name))
        .collect();
    let listing = (candidate.verb == Verb::List)
        .then(|| pages::listing(cx.api, op))
        .flatten();
    let controlled = listing.as_ref().map(|l| l.controlled.clone()).unwrap_or_default();
    let mut inputs = Inputs::default();
    let mut path = op.path.clone();
    let mut path_inputs: Vec<(String, usize)> = Vec::new();
    let mut query: Vec<(String, Json)> = Vec::new();
    let mut query_inputs: Vec<(String, usize)> = Vec::new();
    let mut headers: Vec<(String, Json)> = Vec::new();
    let mut header_inputs: Vec<(String, usize)> = Vec::new();
    // Sent as they are: a required parameter's one value.
    let mut fixed_headers: Vec<(String, Json)> = Vec::new();
    let mut fixed_query: Vec<(String, Json)> = Vec::new();
    // The path's parameters in the order the path names them.
    let mut in_path: Vec<&super::spec::Param<'_>> =
        op.params.iter().filter(|p| p.place == Place::Path).collect();
    in_path.sort_by_key(|p| op.path.find(&format!("{{{}}}", p.name)).unwrap_or(usize::MAX));
    for p in in_path {
        if let Some(i) = inputs.add(&p.name, &p.description, true, &p.schema, root) {
            let name = inputs.list.get(i).map(|x| x.name.clone()).unwrap_or_default();
            path = path.replace(&format!("{{{}}}", p.name), &template(&name));
            path_inputs.push((p.name.clone(), i));
        }
    }
    for p in op.params.iter().filter(|p| p.place != Place::Path) {
        if controlled.contains(&p.name) {
            continue;
        }
        let own = OWN_HEADERS.contains(&p.name.to_ascii_lowercase().as_str());
        if p.required
            && matches!(p.place, Place::Header | Place::Query)
            && !own
            && let Some(v) =
                fixed(root, &p.schema).or_else(|| merged(root, &p.schema).get("default").cloned())
            && !v.is_null()
        {
            let entry = (p.name.clone(), json!(as_text(&v)));
            if p.place == Place::Header {
                fixed_headers.push(entry.clone());
                headers.push(entry);
            } else {
                fixed_query.push(entry.clone());
                query.push(entry);
            }
            continue;
        }
        match p.place {
            Place::Cookie => notes.push(format!("{}: the cookie {}", candidate.name, p.name)),
            Place::Header if OWN_HEADERS.contains(&p.name.to_ascii_lowercase().as_str()) => {}
            Place::Header => {
                if let Some(i) = inputs.add(&p.name, &p.description, p.required, &p.schema, root) {
                    let name = inputs.list.get(i).map(|x| x.name.clone()).unwrap_or_default();
                    headers.push((p.name.clone(), json!(template(&name))));
                    header_inputs.push((p.name.clone(), i));
                }
            }
            Place::Query => {
                let s = merged(root, &p.schema);
                let repeated = kind(root, &s) == "list"
                    && cx.api.dialect != super::spec::Dialect::Swagger2
                    && s.get("explode").and_then(Json::as_bool) != Some(false);
                if repeated && !p.required {
                    notes.push(format!(
                        "{}: the list {} in the query (sent once for each of its values)",
                        candidate.name, p.name
                    ));
                    continue;
                }
                if let Some(i) = inputs.add(&p.name, &p.description, p.required, &p.schema, root) {
                    if let Some(x) = inputs.list.get_mut(i)
                        && x.kind == "list"
                    {
                        // A list in the query goes as one value, its items with commas.
                        x.kind = "text";
                        x.label = format!("{}, with commas", x.label);
                    }
                    let name = inputs.list.get(i).map(|x| x.name.clone()).unwrap_or_default();
                    query.push((p.name.clone(), json!(template(&name))));
                    query_inputs.push((p.name.clone(), i));
                }
            }
            Place::Path => {}
        }
    }
    if let Some((name, n)) = listing.as_ref().and_then(|l| l.size.clone()) {
        query.push((name, json!(n.to_string())));
    }
    // The body.
    let mut body_kind: Option<&str> = None;
    let mut slot: Option<Slot> = None;
    match &op.body {
        Some(b) if matches!(b.kind, BodyKind::Json | BodyKind::Form) => {
            let s = merged(root, &b.schema);
            let record = !properties(root, &s, Purpose::Request).is_empty();
            if b.kind == BodyKind::Form {
                body_kind = Some("form");
            }
            if record {
                let made = body_slots(
                    root,
                    &s,
                    b.required || op.method != "PATCH",
                    &mut inputs,
                    &mut notes,
                    0,
                );
                if b.kind == BodyKind::Form
                    && let Slot::Record(fields) = &made
                {
                    for (name, f) in fields {
                        if let Slot::Record(_) = f {
                            notes.push(format!("{}: the form's nested field {name}", candidate.name));
                        }
                    }
                }
                slot = Some(match made {
                    Slot::Record(fields) if b.kind == BodyKind::Form => Slot::Record(
                        fields
                            .into_iter()
                            .filter(|(_, f)| !matches!(f, Slot::Record(_)))
                            .collect(),
                    ),
                    other => other,
                });
            } else if let Some(i) = inputs.add(
                "body",
                "The whole body of the request, as the API reference describes it",
                b.required,
                &s,
                root,
            ) {
                if s.get("oneOf").is_some() || s.get("anyOf").is_some() {
                    notes.push(format!(
                        "{}: its body is one of several shapes: it is given whole, as the input body",
                        candidate.name
                    ));
                }
                slot = Some(Slot::Input(i));
            }
        }
        Some(b) => notes.push(format!("{}: a body of {:?}", candidate.name, b.kind)),
        None if !matches!(op.method.as_str(), "GET" | "DELETE") => body_kind = Some("none"),
        None => {}
    }
    if op.callbacks {
        notes.push(format!(
            "{}: the callbacks it makes to an address of yours",
            candidate.name
        ));
    }
    // What it gives.
    let answer_schema = op
        .answer
        .as_ref()
        .and_then(|a| a.schema.as_ref())
        .map(|s| merged(root, s));
    if let Some(t) = op.answer.as_ref().and_then(|a| a.not_json.as_ref()) {
        notes.push(format!(
            "{}: its answer is {t}, not JSON: the result is its text",
            candidate.name
        ));
    }
    let output = match (&listing, &answer_schema) {
        (Some(l), _) => output_of(root, &l.item),
        (None, Some(s)) => output_of(root, s),
        (None, None) => Vec::new(),
    };
    let description = {
        let d = sentence(&op.description, 300);
        let d = if d.is_empty() && op.summary.chars().count() > candidate.label.chars().count() + 3 {
            sentence(&op.summary, 300)
        } else {
            d
        };
        if d.trim_end_matches('.') == candidate.label {
            String::new()
        } else {
            d
        }
    };
    let write = op.method != "GET";
    let definition = object(vec![
        ("label", json!(candidate.label)),
        (
            "description",
            if description.is_empty() {
                Json::Null
            } else {
                json!(description)
            },
        ),
        ("write", if write { json!(true) } else { Json::Null }),
        ("method", json!(op.method)),
        ("path", json!(path)),
        ("body", body_kind.map_or(Json::Null, |b| json!(b))),
        (
            "inputs",
            if inputs.list.is_empty() {
                Json::Null
            } else {
                Json::Array(inputs.list.iter().map(input_entry).collect())
            },
        ),
        (
            "query",
            if query.is_empty() {
                Json::Null
            } else {
                Json::Object(query.into_iter().collect())
            },
        ),
        (
            "headers",
            if headers.is_empty() {
                Json::Null
            } else {
                Json::Object(headers.into_iter().collect())
            },
        ),
        ("send", slot.as_ref().map_or(Json::Null, |s| send_of(s, &inputs))),
        (
            "items",
            listing
                .as_ref()
                .map_or(Json::Null, |l| json!(cel("value", &l.items))),
        ),
        (
            "next",
            listing
                .as_ref()
                .and_then(|l| pages::next(l, cx.origin))
                .map_or(Json::Null, Json::String),
        ),
        (
            "output",
            if output.is_empty() {
                Json::Null
            } else {
                Json::Object(output.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
            },
        ),
    ]);
    let parts = Parts {
        path_inputs: &path_inputs,
        query_inputs: &query_inputs,
        header_inputs: &header_inputs,
        fixed_headers: &fixed_headers,
        fixed_query: &fixed_query,
        slot: slot.as_ref(),
        form: body_kind == Some("form"),
    };
    let recording = recording(
        cx,
        op,
        candidate,
        &inputs,
        &parts,
        listing.as_ref(),
        answer_schema.as_ref(),
    );
    Built {
        name: candidate.name.clone(),
        label: candidate.label.clone(),
        description,
        definition,
        recording,
        notes,
        read: !write,
    }
}

fn input_entry(i: &In) -> Json {
    object(vec![
        ("name", json!(i.name)),
        ("label", json!(i.label)),
        ("required", if i.required { json!(true) } else { Json::Null }),
        (
            "kind",
            if i.kind == "text" {
                Json::Null
            } else {
                json!(i.kind)
            },
        ),
        (
            "description",
            if i.description.is_empty() {
                Json::Null
            } else {
                json!(i.description)
            },
        ),
    ])
}

/// Where an action's inputs go: the path, the query, headers, the body.
struct Parts<'p> {
    path_inputs: &'p [(String, usize)],
    query_inputs: &'p [(String, usize)],
    header_inputs: &'p [(String, usize)],
    fixed_headers: &'p [(String, Json)],
    fixed_query: &'p [(String, Json)],
    slot: Option<&'p Slot>,
    form: bool,
}

/// The recorded test of an action: its required inputs given values that fit, the request that must follow from
/// them, and an answer that fits - two pages for a list whose pages are followed.
fn recording(
    cx: &Context<'_, '_>,
    op: &Operation<'_>,
    candidate: &Candidate,
    inputs: &Inputs,
    parts: &Parts<'_>,
    listing: Option<&Listing>,
    answer_schema: Option<&Json>,
) -> Json {
    let root = cx.api.root;
    let mut given = Map::new();
    for i in inputs.list.iter().filter(|i| i.required) {
        let mut v = sample(root, &i.schema, &i.original, Purpose::Request);
        if v.is_null() || (i.kind == "text" && !v.is_string()) {
            v = json!(as_text(&v))
                .as_str()
                .filter(|s| !s.is_empty())
                .map_or(json!("example"), |s| json!(s));
        }
        given.insert(i.name.clone(), v);
    }
    let value_of = |i: usize| -> String {
        inputs
            .list
            .get(i)
            .and_then(|x| given.get(&x.name))
            .map(as_text)
            .unwrap_or_default()
    };
    let mut url_path = op.path.clone();
    for (name, i) in parts.path_inputs {
        url_path = url_path.replace(&format!("{{{name}}}"), &value_of(*i));
    }
    let url = format!("{}{url_path}", cx.base);
    let mut query = Map::new();
    if let Some((k, v)) = &cx.sign_in.query {
        query.insert(k.clone(), json!(v));
    }
    for (name, i) in parts.query_inputs {
        let v = value_of(*i);
        if !v.is_empty() {
            query.insert(name.clone(), json!(v));
        }
    }
    for (name, v) in parts.fixed_query {
        query.insert(name.clone(), v.clone());
    }
    if let Some((name, n)) = listing.and_then(|l| l.size.clone()) {
        query.insert(name, json!(n.to_string()));
    }
    let mut headers = Map::new();
    if let Some((k, v)) = &cx.sign_in.header {
        headers.insert(k.clone(), json!(v));
    }
    for (name, i) in parts.header_inputs {
        let v = value_of(*i);
        if !v.is_empty() {
            headers.insert(name.to_ascii_lowercase(), json!(v));
        }
    }
    for (name, v) in parts.fixed_headers {
        headers.insert(name.to_ascii_lowercase(), v.clone());
    }
    let body = parts.slot.map(|s| {
        let b = sent_of(s, inputs, &given);
        match (parts.form, b) {
            // A form is recorded as its names and values, all text.
            (true, Json::Object(fields)) => Json::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| match v {
                        Json::Array(items) => {
                            (k, Json::Array(items.iter().map(|x| json!(as_text(x))).collect()))
                        }
                        other => (k, json!(as_text(&other))),
                    })
                    .collect(),
            ),
            (_, b) => pruned(b),
        }
    });
    let sent = |url: &str, query: &Map<String, Json>| {
        object(vec![
            ("method", json!(op.method)),
            ("url", json!(url)),
            ("query", Json::Object(query.clone())),
            (
                "headers",
                if headers.is_empty() {
                    Json::Null
                } else {
                    Json::Object(headers.clone())
                },
            ),
            ("body", body.clone().unwrap_or(Json::Null)),
        ])
    };
    let status = op.answer.as_ref().map_or(200, |a| a.status);
    let whole = answer_schema.map(|s| sample(root, s, "", Purpose::Answer));
    let credential: Map<String, Json> = cx
        .sign_in
        .credential
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    let mut exchanges = Vec::new();
    let result = match listing {
        None => {
            let body = whole.unwrap_or(Json::Null);
            exchanges.push(json!({ "sent": sent(&url, &query), "answer": object(vec![("status", json!(status)), ("body", body.clone())]) }));
            body
        }
        Some(l) => {
            let first = sample(root, &l.item, "", Purpose::Answer);
            let mut second = first.clone();
            if let Some(o) = second.as_object_mut()
                && o.contains_key("id")
            {
                o.insert("id".to_owned(), json!("id_2"));
            }
            let around = whole.unwrap_or(Json::Null);
            let last_page = |records: Vec<Json>| {
                let mut b = pages::page_body(l, &around, records);
                match &l.paging {
                    Paging::Token { at, .. } | Paging::Address(at) => pages::put(&mut b, at, Json::Null),
                    Paging::LastId { more, .. } => pages::put(&mut b, more, json!(false)),
                    _ => {}
                }
                b
            };
            let more_url = format!("{url}?page=2");
            let (two, next_query, next_url, first_body, first_headers): (
                bool,
                Map<String, Json>,
                String,
                Json,
                Json,
            ) = match &l.paging {
                Paging::One => (
                    false,
                    query.clone(),
                    url.clone(),
                    last_page(vec![first.clone()]),
                    Json::Null,
                ),
                Paging::Link => (
                    true,
                    Map::new(),
                    more_url.clone(),
                    last_page(vec![first.clone()]),
                    json!({ "Link": format!("<{more_url}>; rel=\"next\"") }),
                ),
                Paging::Address(at) => {
                    let mut b = last_page(vec![first.clone()]);
                    pages::put(&mut b, at, json!(more_url));
                    (true, Map::new(), more_url.clone(), b, Json::Null)
                }
                Paging::Token { param, at } => {
                    let mut b = last_page(vec![first.clone()]);
                    pages::put(&mut b, at, json!("token_2"));
                    let mut q = query.clone();
                    q.insert(param.clone(), json!("token_2"));
                    (true, q, url.clone(), b, Json::Null)
                }
                Paging::LastId { param, more } => {
                    let mut b = last_page(vec![first.clone()]);
                    pages::put(&mut b, more, json!(true));
                    let mut q = query.clone();
                    q.insert(
                        param.clone(),
                        json!(first.get("id").map(as_text).unwrap_or_default()),
                    );
                    (true, q, url.clone(), b, Json::Null)
                }
                Paging::Number { param, first: f } if l.size.is_none() => {
                    let mut q = query.clone();
                    q.insert(param.clone(), json!((f + 1).to_string()));
                    (true, q, url.clone(), last_page(vec![first.clone()]), Json::Null)
                }
                Paging::Offset { param } if l.size.is_none() => {
                    let mut q = query.clone();
                    q.insert(param.clone(), json!("1"));
                    (true, q, url.clone(), last_page(vec![first.clone()]), Json::Null)
                }
                Paging::Number { .. } | Paging::Offset { .. } => (
                    false,
                    query.clone(),
                    url.clone(),
                    last_page(vec![first.clone()]),
                    Json::Null,
                ),
            };
            exchanges.push(json!({
                "sent": sent(&url, &query),
                "answer": object(vec![("status", json!(status)), ("headers", first_headers), ("body", first_body)]),
            }));
            let mut records = vec![first.clone()];
            if two {
                // A page asked for by its address carries its own query.
                let second_sent = if matches!(l.paging, Paging::Link | Paging::Address(_)) {
                    object(vec![("url", json!(next_url))])
                } else {
                    sent(&next_url, &next_query)
                };
                let numbered = matches!(l.paging, Paging::Number { .. } | Paging::Offset { .. });
                let page_two = if numbered { vec![] } else { vec![second.clone()] };
                if !numbered {
                    records.push(second);
                }
                exchanges.push(json!({
                    "sent": second_sent,
                    "answer": object(vec![("status", json!(status)), ("body", last_page(page_two))]),
                }));
            }
            Json::Array(records)
        }
    };
    object(vec![
        (
            "description",
            json!(format!(
                "{}, made from the API description's schemas",
                candidate.label
            )),
        ),
        ("action", json!(candidate.name)),
        ("credential", Json::Object(credential)),
        (
            "input",
            if given.is_empty() {
                Json::Null
            } else {
                Json::Object(given)
            },
        ),
        ("exchanges", Json::Array(exchanges)),
        ("result", result),
    ])
}

/// A recording of a refusal, from an action's own: the same request, answered 404.
pub(super) fn refusal(built: &Built, vendor: &str) -> Json {
    let mut r = built.recording.clone();
    if let Some(o) = r.as_object_mut() {
        o.insert(
            "description".to_owned(),
            json!(format!(
                "{vendor} refuses {}: the step fails in its words",
                built.name
            )),
        );
        o.insert(
            "exchanges".to_owned(),
            json!([{ "answer": { "status": 404, "body": { "message": "Not found" } } }]),
        );
        o.remove("result");
        o.insert(
            "error".to_owned(),
            json!(format!("{vendor} answered 404: Not found")),
        );
    }
    r
}
