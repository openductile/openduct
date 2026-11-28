// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow's JSON written as YAML a person reads and a review shows line by line: settings in blocks, scripts
//! and long texts as literal blocks, anything YAML could read as something else in double quotes. What is written
//! is read back before it is given; should it not read back as the same, the workflow is written as JSON (which YAML
//! reads too) instead.

use std::fmt::Write as _;

use serde_json::Value;

/// `value` as a YAML document.
#[must_use]
pub fn to_yaml(value: &Value) -> String {
    let mut out = String::new();
    block(&mut out, value, 0);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    match openduct_flow::yaml_to_json(&out) {
        Ok(back) if back == *value => out,
        _ => {
            let mut json = String::new();
            flow(&mut json, value);
            json.push('\n');
            json
        }
    }
}

fn pad(out: &mut String, indent: usize) {
    out.extend(std::iter::repeat_n(' ', indent));
}

/// A value at the start of a line, its lines at `indent`.
fn block(out: &mut String, value: &Value, indent: usize) {
    match value {
        Value::Object(fields) if !fields.is_empty() => {
            for (k, v) in fields {
                pad(out, indent);
                out.push_str(&key(k));
                out.push(':');
                after_key(out, v, indent);
            }
        }
        Value::Array(items) if !items.is_empty() => {
            for item in items {
                pad(out, indent);
                out.push('-');
                match item {
                    Value::Object(f) if !f.is_empty() => {
                        // The item's settings start on the dash's line, the rest under the first.
                        let mut inner = String::new();
                        block(&mut inner, item, indent + 2);
                        out.push(' ');
                        out.push_str(inner.get(indent + 2..).unwrap_or_default());
                    }
                    Value::Array(a) if !a.is_empty() => {
                        out.push('\n');
                        block(out, item, indent + 2);
                    }
                    _ => {
                        out.push(' ');
                        scalar_after(out, item, indent + 2);
                    }
                }
            }
        }
        _ => {
            pad(out, indent);
            scalar_after(out, value, indent);
        }
    }
}

/// What follows `key:`.
fn after_key(out: &mut String, value: &Value, indent: usize) {
    match value {
        Value::Object(f) if !f.is_empty() => {
            out.push('\n');
            block(out, value, indent + 2);
        }
        Value::Array(a) if !a.is_empty() => {
            out.push('\n');
            block(out, value, indent + 2);
        }
        _ => {
            out.push(' ');
            scalar_after(out, value, indent + 2);
        }
    }
}

/// A scalar (or an empty list or map) ending its line; a literal block's lines at `indent`.
fn scalar_after(out: &mut String, value: &Value, indent: usize) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) if literal_block(s) => {
            let trailing = s.len() - s.trim_end_matches('\n').len();
            out.push_str(match trailing {
                0 => "|-",
                1 => "|",
                _ => "|+",
            });
            out.push('\n');
            let body = s.strip_suffix('\n').unwrap_or(s);
            for line in body.split('\n') {
                if !line.is_empty() {
                    pad(out, indent);
                    out.push_str(line);
                }
                out.push('\n');
            }
            return;
        }
        Value::String(s) if plain(s) => out.push_str(s),
        Value::String(s) => quoted(out, s),
        Value::Array(_) => out.push_str("[]"),
        Value::Object(_) => out.push_str("{}"),
    }
    out.push('\n');
}

/// A key as written: plain when it is a word, quoted otherwise.
fn key(k: &str) -> String {
    let word = k
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && k.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !special(k);
    if word || k == super::ON {
        k.to_owned()
    } else {
        let mut out = String::new();
        quoted(&mut out, k);
        out
    }
}

/// Words some YAML readers take for true, false or nothing.
fn special(s: &str) -> bool {
    matches!(
        s.to_ascii_lowercase().as_str(),
        "true" | "false" | "null" | "~" | "yes" | "no" | "on" | "off" | "y" | "n"
    )
}

/// Whether a text is written as it is: words, numbers in words, paths, and nothing YAML reads otherwise.
fn plain(s: &str) -> bool {
    let Some(first) = s.chars().next() else {
        return false;
    };
    (first.is_alphanumeric() || first == '/')
        && s.chars().all(|c| {
            c.is_alphanumeric()
                || matches!(
                    c,
                    ' ' | '_' | '-' | '.' | '/' | ',' | '(' | ')' | '+' | '=' | '\''
                )
        })
        && !s.ends_with(' ')
        && !s.contains("  ")
        && !special(s)
        && s.parse::<f64>().is_err()
        && !looks_numeric(s)
}

/// A text YAML might read as a number: digits with signs, dots, underscores, `0x`, `0o`, `e`.
fn looks_numeric(s: &str) -> bool {
    let t = s.trim_start_matches(['+', '-']);
    t.starts_with(|c: char| c.is_ascii_digit() || c == '.')
        && t.chars()
            .all(|c| c.is_ascii_hexdigit() || matches!(c, '.' | '_' | 'x' | 'o' | 'e' | 'E' | '+' | '-'))
}

/// Whether a text of several lines is written as a literal block: only printable characters, no line beginning
/// with a space at its start, no carriage returns or tabs.
fn literal_block(s: &str) -> bool {
    s.contains('\n')
        && !s.starts_with(' ')
        && !s.starts_with('\n')
        && s.chars().all(|c| c == '\n' || (printable(c) && c != '\t'))
        && s.split('\n').all(|line| line == line.trim_end())
}

fn printable(c: char) -> bool {
    matches!(c, ' '..='~' | '\u{a0}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..)
        && c != '\u{feff}'
}

/// A text in double quotes, with every character YAML might not print escaped.
fn quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if printable(c) => out.push(c),
            c if u32::from(c) <= 0xffff => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => {
                let _ = write!(out, "\\U{:08x}", u32::from(c));
            }
        }
    }
    out.push('"');
}

/// `value` in JSON's form, which YAML reads as its flow style.
fn flow(out: &mut String, value: &Value) {
    match value {
        Value::Object(fields) => {
            out.push('{');
            for (i, (k, v)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                quoted(out, k);
                out.push_str(": ");
                flow(out, v);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                flow(out, v);
            }
            out.push(']');
        }
        Value::String(s) => quoted(out, s),
        other => out.push_str(&other.to_string()),
    }
}
