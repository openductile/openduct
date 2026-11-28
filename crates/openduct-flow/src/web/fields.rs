// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A `select` step's fields: each a CSS selector, giving the first element it matches (or, with `all`, every one) as
//! its text, an attribute (`href`, `src` and their kin at their full addresses), Markdown or HTML - or, with fields of
//! its own, a record of those picked out inside each element it matches: the cards of a list, each its name and
//! price. Nothing matched is null (or an empty list).

use serde_json::{Map, Value as Json};

use super::page::dom::Data;
use super::page::{Base, Budget, Page, Selector, inline_text};

/// The most fields at one level, and how deep fields hold fields.
pub(crate) const MAX_FIELDS: usize = 50;
pub(crate) const MAX_FIELD_DEPTH: usize = 3;
/// The most elements one field gives with `all`.
pub(crate) const MAX_ALL: usize = 10_000;

/// What a field gives of an element.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    Text,
    Markdown,
    Html,
}

/// One field.
#[derive(Debug, Clone)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) selector: Selector,
    pub(crate) attribute: Option<String>,
    pub(crate) all: bool,
    pub(crate) shape: Shape,
    pub(crate) fields: Vec<Field>,
}

/// Attributes that hold an address, given whole.
const ADDRESSES: &[&str] = &["href", "src", "action", "poster", "data-src", "cite", "data-href"];

const VOID: &[&str] = &[
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track",
    "wbr",
];

fn escape(text: &str, attribute: bool, out: &mut String) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' if !attribute => out.push_str("&lt;"),
            '>' if !attribute => out.push_str("&gt;"),
            '"' if attribute => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
}

/// An element as HTML, with what it holds.
fn html_of(page: &Page, id: usize, out: &mut String) {
    let mut ends: Vec<(usize, String)> = Vec::new();
    for i in page.within(id) {
        while let Some((end, name)) = ends.last()
            && *end <= i
        {
            out.push_str(&format!("</{name}>"));
            ends.pop();
        }
        match page.nodes.get(i).map(|n| &n.data) {
            Some(Data::Text(t)) => {
                let raw = page
                    .parent(i)
                    .and_then(|p| page.name(p))
                    .is_some_and(|n| matches!(n, "script" | "style"));
                if raw {
                    out.push_str(t);
                } else {
                    escape(t, false, out);
                }
            }
            Some(Data::Element { name, attributes }) => {
                out.push('<');
                out.push_str(name);
                for (k, v) in attributes {
                    out.push(' ');
                    out.push_str(k);
                    out.push_str("=\"");
                    escape(v, true, out);
                    out.push('"');
                }
                out.push('>');
                if !VOID.contains(&name.as_str()) {
                    ends.push((page.within(i).end, name.clone()));
                }
            }
            _ => {}
        }
    }
    while let Some((_, name)) = ends.pop() {
        out.push_str(&format!("</{name}>"));
    }
}

fn value_of(page: &Page, base: &Base, field: &Field, id: usize, budget: &mut Budget) -> Result<Json, String> {
    if !field.fields.is_empty() {
        return pick(page, base, &field.fields, id, budget).map(Json::Object);
    }
    if let Some(attribute) = &field.attribute {
        let value = page.attr(id, attribute).map(str::trim);
        return Ok(match value {
            Some(v) if ADDRESSES.contains(&attribute.as_str()) => {
                base.link(v).map_or(Json::Null, Json::String)
            }
            Some(v) => Json::String(v.to_owned()),
            None => Json::Null,
        });
    }
    Ok(Json::String(match field.shape {
        Shape::Text => inline_text(page, id, base),
        Shape::Markdown => {
            let hidden = vec![false; page.nodes.len()];
            super::page::write_markdown(page, id, &hidden, base)
        }
        Shape::Html => {
            let mut out = String::new();
            html_of(page, id, &mut out);
            out
        }
    }))
}

/// The fields picked out of the elements inside `scope`.
pub(crate) fn pick(
    page: &Page,
    base: &Base,
    fields: &[Field],
    scope: usize,
    budget: &mut Budget,
) -> Result<Map<String, Json>, String> {
    let mut out = Map::new();
    for field in fields {
        let found = field
            .selector
            .select(page, scope, if field.all { MAX_ALL } else { 1 }, budget)
            .map_err(|e| format!("{}: {e}", field.name))?;
        let value = if field.all {
            let mut list = Vec::with_capacity(found.len());
            for id in found {
                list.push(value_of(page, base, field, id, budget)?);
            }
            Json::Array(list)
        } else {
            match found.first() {
                Some(id) => value_of(page, base, field, *id, budget)?,
                None => Json::Null,
            }
        };
        out.insert(field.name.clone(), value);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn field(name: &str, selector: &str) -> Field {
        Field {
            name: name.to_owned(),
            selector: Selector::parse(selector).unwrap(),
            attribute: None,
            all: false,
            shape: Shape::Text,
            fields: Vec::new(),
        }
    }

    #[test]
    fn fields_are_picked_as_text_attributes_lists_and_records() {
        let html = r#"<h1>Shop <small>new</small></h1><ul><li class=item><a href="/cap">Cap</a> <b class=price>24</b></li>
            <li class=item><a href="tote">Tote</a><b class=price>12</b></li></ul><p>A <i>"quoted"</i> &amp; done</p>"#;
        let page = Page::parse(html, false);
        let base = Base::new("https://shop.example/c/".parse().unwrap());
        let mut links = field("links", "li a");
        links.attribute = Some("href".to_owned());
        links.all = true;
        let mut items = field("items", ".item");
        items.all = true;
        items.fields = vec![field("name", "a"), field("price", ".price"), field("none", "em")];
        let mut html_field = field("html", "p");
        html_field.shape = Shape::Html;
        let mut md = field("md", "p");
        md.shape = Shape::Markdown;
        let picked = pick(
            &page,
            &base,
            &[
                field("title", "h1"),
                links,
                items,
                html_field,
                md,
                field("missing", "table"),
            ],
            0,
            &mut Budget(100_000),
        )
        .unwrap();
        assert_eq!(
            Json::Object(picked),
            json!({
                "title": "Shop new",
                "links": ["https://shop.example/cap", "https://shop.example/c/tote"],
                "items": [{"name": "Cap", "price": "24", "none": null}, {"name": "Tote", "price": "12", "none": null}],
                "html": "<p>A <i>\"quoted\"</i> &amp; done</p>",
                "md": "A *\"quoted\"* & done",
                "missing": null,
            })
        );
    }
}
