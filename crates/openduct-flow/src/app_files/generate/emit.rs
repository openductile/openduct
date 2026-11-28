// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app made from JSON, written as the YAML of an app's folder: records in blocks, short values on one line, every
//! text that YAML could read as something else in double quotes (JSON's escapes, which YAML reads alike).

use serde_json::Value as Json;

/// The widest line a value is kept on.
const WIDE: usize = 110;

/// Whether a text may be written without quotes: a plain word YAML reads as that text and nothing else.
fn bare(text: &str) -> bool {
    const WORDS: &[&str] = &["true", "false", "null", "yes", "no", "on", "off", "y", "n", "~"];
    !text.is_empty()
        && text.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '/' | '-'))
        && !WORDS.contains(&text.to_ascii_lowercase().as_str())
}

/// A text as YAML: bare, or in double quotes.
pub(super) fn text(t: &str) -> String {
    if bare(t) {
        return t.to_owned();
    }
    serde_json::to_string(t)
        .unwrap_or_else(|_| "\"\"".to_owned())
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .replace('\u{85}', "\\u0085")
}

/// A value on one line.
pub(super) fn flow(v: &Json) -> String {
    match v {
        Json::String(s) => text(s),
        Json::Array(items) => format!("[{}]", items.iter().map(flow).collect::<Vec<_>>().join(", ")),
        Json::Object(fields) if fields.is_empty() => "{}".to_owned(),
        Json::Object(fields) => format!(
            "{{ {} }}",
            fields
                .iter()
                .map(|(k, v)| format!("{}: {}", text(k), flow(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => other.to_string(),
    }
}

/// A record's fields as YAML at `indent`.
pub(super) fn block(out: &mut String, fields: &serde_json::Map<String, Json>, indent: usize) {
    let pad = " ".repeat(indent);
    for (k, v) in fields {
        let key = text(k);
        let one = flow(v);
        let fits = pad.len() + key.len() + 2 + one.len() <= WIDE;
        match v {
            Json::Object(inner) if !fits && !inner.is_empty() => {
                out.push_str(&format!("{pad}{key}:\n"));
                block(out, inner, indent + 2);
            }
            Json::Array(items) if !fits && !items.is_empty() => {
                out.push_str(&format!("{pad}{key}:\n"));
                for item in items {
                    item_line(out, item, indent + 2);
                }
            }
            _ => out.push_str(&format!("{pad}{key}: {one}\n")),
        }
    }
}

/// One item of a list, at `indent`: on its line, or as a record in a block.
fn item_line(out: &mut String, item: &Json, indent: usize) {
    let pad = " ".repeat(indent);
    let one = flow(item);
    match item {
        Json::Object(fields) if pad.len() + 2 + one.len() > WIDE && !fields.is_empty() => {
            let mut inner = String::new();
            block(&mut inner, fields, indent + 2);
            // The record's first line carries the dash.
            let lead = " ".repeat(indent + 2);
            let rest = inner.strip_prefix(&lead).unwrap_or(&inner);
            out.push_str(&format!("{pad}- {rest}"));
        }
        _ => out.push_str(&format!("{pad}- {one}\n")),
    }
}

/// A record as a YAML file, `comment` lines first.
pub(super) fn file(value: &Json, comment: &[String]) -> String {
    let mut out = String::new();
    for line in comment {
        out.push_str(&format!("# {}\n", line.replace(['\n', '\r'], " ")));
    }
    if let Json::Object(fields) = value {
        block(&mut out, fields, 0);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    #[test]
    fn what_is_written_reads_back_the_same() {
        let v = json!({
            "app": "shop", "name": "Shop: the store", "hosts": ["api.shop.example"],
            "yes": "no", "n": 3, "t": true, "quote": "it's \"here\"\n and there #1", "empty": {},
            "actions": { "list_orders": {
                "label": "List orders", "method": "GET", "path": "/orders/{{ input.id }}",
                "next": "value.more ? {'after': value.data[size(value.data) - 1].id} : null",
                "inputs": [{ "name": "id", "label": "The order's id: a long label that goes on and on, to make it wrap well past the line", "required": true }],
            } },
            "deep": { "a": { "b": { "c": [1, { "d": "e" }] } } },
        });
        let yaml = file(&v, &["made for a test".to_owned()]);
        assert!(yaml.starts_with("# made for a test\n"));
        assert!(yaml.contains("\"yes\": \"no\""), "{yaml}");
        let back = crate::yaml::to_json(&yaml).unwrap();
        assert_eq!(back, v, "{yaml}");
    }
}
