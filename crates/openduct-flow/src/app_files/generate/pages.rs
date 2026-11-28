// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A list action's records and pages, as its description shows them: where the records are in the answer, and how
//! the next page is asked for - the `Link` header; a next page's address in the answer; a cursor (a token the answer
//! gives, or the last record's id while the answer says there is more); a page number; an offset. What is not
//! shown is not guessed: such a list reads one page.

use serde_json::{Value as Json, json};

use super::schema::{field_named, kind, list_in, merged};
use super::spec::{Api, Operation, Place};

/// The largest page an action asks for.
const PAGE: u64 = 100;

/// The query parameters that carry a cursor.
const CURSOR_PARAMS: &[&str] = &[
    "cursor",
    "page_token",
    "next_token",
    "marker",
    "after",
    "starting_after",
    "page[cursor]",
    "page[after]",
    "continuation",
    "continuation_token",
    "start_cursor",
    "next_cursor",
    "pagination_token",
    "next_page_token",
    "page_cursor",
    "offset_token",
];
/// The fields of an answer that hold the next page's cursor.
const CURSOR_FIELDS: &[&str] = &[
    "next_cursor",
    "next_page_token",
    "next_token",
    "next_marker",
    "end_cursor",
    "continuation",
    "continuation_token",
    "next_page_cursor",
    "cursor",
    "after",
    "next_after",
];
/// The fields of an answer that hold the next page's address.
const NEXT_FIELDS: &[&str] = &[
    "next",
    "next_url",
    "next_page_url",
    "next_page_uri",
    "next_link",
    "nextlink",
    "@odata.nextlink",
    "next_href",
    "next_page",
];
const PAGE_PARAMS: &[&str] = &["page", "page_number", "pageno", "page[number]"];
const OFFSET_PARAMS: &[&str] = &["offset", "skip", "start", "start_index", "page[offset]"];
const SIZE_PARAMS: &[&str] = &[
    "per_page",
    "page_size",
    "limit",
    "count",
    "size",
    "page[size]",
    "page[limit]",
    "max_results",
    "results_per_page",
    "top",
    "take",
    "max",
];

/// How a list's next page is asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Paging {
    /// One page only: the description shows no way to the next.
    One,
    /// The `Link` header's `next`.
    Link,
    /// An address in the answer (at this path).
    Address(Vec<String>),
    /// A token in the answer (at this path), sent back as the parameter.
    Token { param: String, at: Vec<String> },
    /// The last record's `id`, sent as the parameter while the answer's flag (at this path) says there is more.
    LastId { param: String, more: Vec<String> },
    /// A page number, counted from `first`.
    Number { param: String, first: i64 },
    /// An offset into the records.
    Offset { param: String },
}

/// A list action: where its records are, each record's schema, how its pages go, and the parameters the engine
/// sets itself (with the page size it asks for).
#[derive(Debug, Clone)]
pub(super) struct Listing {
    pub(super) items: Vec<String>,
    pub(super) item: Json,
    pub(super) paging: Paging,
    /// The page size parameter and the size asked for, when the action sets one.
    pub(super) size: Option<(String, u64)>,
    /// Parameters the engine sets: not inputs.
    pub(super) controlled: Vec<String>,
}

fn squash(s: &str) -> String {
    s.to_ascii_lowercase().replace(['_', '-'], "")
}

fn named<'o, 'a>(op: &'o Operation<'a>, names: &[&str]) -> Option<&'o super::spec::Param<'a>> {
    let wanted: Vec<String> = names.iter().map(|n| squash(n)).collect();
    op.params
        .iter()
        .filter(|p| p.place == Place::Query)
        .find(|p| wanted.contains(&squash(&p.name)))
}

/// An expression reading `path` from `base`: `value.data`, or `value['@odata.nextLink']` for a name that is not a
/// plain word.
pub(super) fn cel(base: &str, path: &[String]) -> String {
    const RESERVED: &[&str] = &[
        "as",
        "break",
        "const",
        "continue",
        "else",
        "false",
        "for",
        "function",
        "if",
        "import",
        "in",
        "let",
        "loop",
        "package",
        "namespace",
        "null",
        "return",
        "true",
        "var",
        "void",
        "while",
    ];
    let mut out = base.to_owned();
    for p in path {
        let plain = p.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !RESERVED.contains(&p.as_str());
        if plain {
            out.push('.');
            out.push_str(p);
        } else {
            out.push_str(&format!("['{}']", p.replace('\\', "\\\\").replace('\'', "\\'")));
        }
    }
    out
}

/// A text in single quotes, for an expression.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The page size an action asks for: the most the parameter allows up to 100, else its default.
fn size_of(root: &Json, p: &super::spec::Param<'_>) -> Option<u64> {
    let s = merged(root, &p.schema);
    match (
        s.get("maximum").and_then(Json::as_u64),
        s.get("default").and_then(Json::as_u64),
    ) {
        (Some(max), _) => Some(max.clamp(1, PAGE)),
        (None, Some(d)) if d > 0 => Some(d.min(PAGE)),
        _ => None,
    }
}

/// The listing of a list operation (one whose answer holds a list), if it is one.
pub(super) fn listing(api: &Api<'_>, op: &Operation<'_>) -> Option<Listing> {
    let root = api.root;
    let answer = op.answer.as_ref()?;
    let schema = answer.schema.as_ref()?;
    let (items, item) = list_in(root, schema)?;
    let mut controlled = Vec::new();
    let size_param = named(op, SIZE_PARAMS);
    let size = size_param.and_then(|p| size_of(root, p).map(|n| (p.name.clone(), n)));
    let text_field = |names: &[&str]| -> Option<Vec<String>> {
        let (path, s) = field_named(root, schema, names, 3)?;
        if kind(root, &s) == "record" {
            // `_links: {next: {href}}`.
            let (inner, s) = field_named(root, &s, &["href"], 1)?;
            return (kind(root, &s) == "text").then(|| [path, inner].concat());
        }
        (kind(root, &s) == "text").then_some(path)
    };
    let paging = if answer.link {
        Paging::Link
    } else if let Some(cursor) = named(op, CURSOR_PARAMS) {
        let mut fields: Vec<&str> = vec![cursor.name.as_str()];
        fields.extend_from_slice(CURSOR_FIELDS);
        let more = field_named(
            root,
            schema,
            &["has_more", "more", "has_next", "has_next_page"],
            2,
        )
        .filter(|(_, s)| kind(root, s) == "boolean");
        let has_id = super::schema::properties(root, &item, super::schema::Purpose::Answer)
            .iter()
            .any(|(n, _, _)| n == "id");
        match (text_field(&fields), more) {
            (Some(at), _) => Paging::Token {
                param: cursor.name.clone(),
                at,
            },
            (None, Some((more, _))) if has_id => Paging::LastId {
                param: cursor.name.clone(),
                more,
            },
            _ => match text_field(NEXT_FIELDS) {
                Some(at) => Paging::Address(at),
                None => Paging::One,
            },
        }
    } else if let Some(at) = text_field(NEXT_FIELDS) {
        Paging::Address(at)
    } else if let Some(page) = named(op, PAGE_PARAMS) {
        let s = merged(root, &page.schema);
        let first = s
            .get("minimum")
            .or_else(|| s.get("default"))
            .and_then(Json::as_i64)
            .filter(|n| *n == 0)
            .unwrap_or(1);
        Paging::Number {
            param: page.name.clone(),
            first,
        }
    } else if let Some(offset) = named(op, OFFSET_PARAMS) {
        Paging::Offset {
            param: offset.name.clone(),
        }
    } else {
        Paging::One
    };
    match &paging {
        Paging::Token { param, .. }
        | Paging::LastId { param, .. }
        | Paging::Number { param, .. }
        | Paging::Offset { param } => controlled.push(param.clone()),
        Paging::One | Paging::Link | Paging::Address(_) => {}
    }
    if paging != Paging::One
        && let Some(cursor) = named(op, CURSOR_PARAMS)
        && !controlled.contains(&cursor.name)
    {
        controlled.push(cursor.name.clone());
    }
    // A page size is set when the pages are followed; for a list of one page, it stays the caller's to choose.
    let size = if paging == Paging::One { None } else { size };
    if let Some((p, _)) = &size {
        controlled.push(p.clone());
    }
    Some(Listing {
        items,
        item,
        paging,
        size,
        controlled,
    })
}

/// The action's `next`: how the page after the one just read is asked for, from its answer; `origin` is the API's
/// own (`https://api.example.com`), for an address the answer gives without it.
pub(super) fn next(listing: &Listing, origin: &str) -> Option<String> {
    let items = cel("value", &listing.items);
    let stop = match &listing.size {
        Some((_, n)) => format!("size({items}) < {n}"),
        None => format!("size({items}) == 0"),
    };
    Some(match &listing.paging {
        Paging::One => return None,
        Paging::Link => "links.next".to_owned(),
        Paging::Address(at) => {
            let e = cel("value", at);
            format!(
                "{e} == null || {e} == '' ? null : ({e}.startsWith('http') ? {e} : {} + {e})",
                quoted(origin)
            )
        }
        Paging::Token { param, at } => {
            let e = cel("value", at);
            format!("{e} == null || {e} == '' ? null : {{{}: {e}}}", quoted(param))
        }
        Paging::LastId { param, more } => {
            let flag = cel("value", more);
            format!(
                "{flag} == true && size({items}) > 0 ? {{{}: {items}[size({items}) - 1].id}} : null",
                quoted(param)
            )
        }
        Paging::Number { param, first } => format!(
            "{stop} ? null : {{{q}: (page == null ? {first} : int(page[{q}])) + 1}}",
            q = quoted(param)
        ),
        Paging::Offset { param } => format!(
            "{stop} ? null : {{{q}: (page == null ? 0 : int(page[{q}])) + size({items})}}",
            q = quoted(param)
        ),
    })
}

/// A sample answer body holding `records` where the listing keeps them, inside `around` (a sample of the whole
/// answer).
pub(super) fn page_body(listing: &Listing, around: &Json, records: Vec<Json>) -> Json {
    if listing.items.is_empty() {
        return Json::Array(records);
    }
    let mut body = if around.is_object() {
        around.clone()
    } else {
        json!({})
    };
    put(&mut body, &listing.items, Json::Array(records));
    body
}

/// Sets the value at `path` in a record, making the records on the way.
pub(super) fn put(into: &mut Json, path: &[String], value: Json) {
    let Some((last, rest)) = path.split_last() else {
        *into = value;
        return;
    };
    let mut at = into;
    for p in rest {
        if !at.get(p).is_some_and(Json::is_object)
            && let Some(o) = at.as_object_mut()
        {
            o.insert(p.clone(), json!({}));
        }
        let Some(next) = at.get_mut(p) else {
            return;
        };
        at = next;
    }
    if let Some(o) = at.as_object_mut() {
        o.insert(last.clone(), value);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    fn listing_of(params: Json, schema: Json, link: bool) -> Listing {
        let mut answer = json!({ "content": { "application/json": { "schema": schema } } });
        if link {
            answer["headers"] = json!({ "Link": {} });
        }
        let doc = json!({ "openapi": "3.0.0", "servers": [{ "url": "https://api.x.example" }], "paths": {
            "/things": { "get": { "parameters": params, "responses": { "200": answer } } } } });
        let api = super::super::spec::read(&doc, None).unwrap();
        let l = listing(&api, &api.operations[0]).unwrap();
        // The listing holds no reference to the description.
        l.clone()
    }

    fn q(name: &str, schema: Json) -> Json {
        json!({ "name": name, "in": "query", "schema": schema })
    }

    #[test]
    fn each_way_to_the_next_page_is_found() {
        let thing = json!({ "type": "object", "properties": { "id": { "type": "string" } } });
        let list = json!({ "type": "array", "items": thing });
        // The Link header.
        let l = listing_of(
            json!([q("per_page", json!({ "type": "integer", "maximum": 250 }))]),
            list.clone(),
            true,
        );
        assert_eq!(l.paging, Paging::Link);
        assert_eq!(l.size, Some(("per_page".to_owned(), 100)));
        assert_eq!(next(&l, "https://api.x.example").unwrap(), "links.next");
        // A cursor the answer gives back, deep in it.
        let paged = json!({ "type": "object", "properties": {
            "data": list, "meta": { "type": "object", "properties": { "next_cursor": { "type": "string" } } } } });
        let l = listing_of(
            json!([
                q("cursor", json!({ "type": "string" })),
                q("limit", json!({ "type": "integer", "default": 20 }))
            ]),
            paged,
            false,
        );
        assert_eq!(l.items, ["data"]);
        assert_eq!(
            l.paging,
            Paging::Token {
                param: "cursor".to_owned(),
                at: vec!["meta".to_owned(), "next_cursor".to_owned()]
            }
        );
        assert_eq!(l.controlled, ["cursor", "limit"]);
        assert_eq!(
            next(&l, "").unwrap(),
            "value.meta.next_cursor == null || value.meta.next_cursor == '' ? null : {'cursor': value.meta.next_cursor}"
        );
        // The last record's id while the answer says there is more.
        let more =
            json!({ "type": "object", "properties": { "has_more": { "type": "boolean" }, "data": list } });
        let l = listing_of(
            json!([q("starting_after", json!({ "type": "string" }))]),
            more,
            false,
        );
        assert_eq!(
            l.paging,
            Paging::LastId {
                param: "starting_after".to_owned(),
                more: vec!["has_more".to_owned()]
            }
        );
        assert!(
            next(&l, "")
                .unwrap()
                .contains("value.data[size(value.data) - 1].id")
        );
        // An address in the answer.
        let odata = json!({ "type": "object", "properties": { "value": list, "@odata.nextLink": { "type": "string" } } });
        let l = listing_of(json!([]), odata, false);
        assert_eq!(l.items, ["value"]);
        assert!(
            next(&l, "https://api.x.example")
                .unwrap()
                .contains("value['@odata.nextLink']")
        );
        // A page number, and an offset.
        let l = listing_of(
            json!([
                q("page", json!({ "type": "integer" })),
                q("page_size", json!({ "type": "integer", "maximum": 50 }))
            ]),
            list.clone(),
            false,
        );
        assert_eq!(
            l.paging,
            Paging::Number {
                param: "page".to_owned(),
                first: 1
            }
        );
        assert_eq!(
            next(&l, "").unwrap(),
            "size(value) < 50 ? null : {'page': (page == null ? 1 : int(page['page'])) + 1}"
        );
        let l = listing_of(
            json!([q("offset", json!({ "type": "integer" }))]),
            list.clone(),
            false,
        );
        assert_eq!(
            next(&l, "").unwrap(),
            "size(value) == 0 ? null : {'offset': (page == null ? 0 : int(page['offset'])) + size(value)}"
        );
        // Nothing shown: one page, and the size parameter left to the caller.
        let l = listing_of(
            json!([q("limit", json!({ "type": "integer", "maximum": 10 }))]),
            list,
            false,
        );
        assert_eq!(
            (l.paging.clone(), l.size.clone(), l.controlled.len()),
            (Paging::One, None, 0)
        );
        assert!(next(&l, "").is_none());
    }

    #[test]
    fn records_go_where_the_answer_keeps_them() {
        let l = Listing {
            items: vec!["a".to_owned(), "b".to_owned()],
            item: json!({}),
            paging: Paging::One,
            size: None,
            controlled: vec![],
        };
        assert_eq!(
            page_body(&l, &json!({ "x": 1 }), vec![json!(1)]),
            json!({ "x": 1, "a": { "b": [1] } })
        );
        assert_eq!(
            cel("value", &["in".to_owned(), "x-y".to_owned(), "ok".to_owned()]),
            "value['in']['x-y'].ok"
        );
    }
}
