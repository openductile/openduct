// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A page's content read for its text: the strings each text operator shows, in the font set at the time, with a
//! new line where the text moves down and a space where it moves along - and the text of the forms it draws.

use std::collections::HashMap;

use super::Document;
use super::fonts::{Font, font};
use super::lex::{Lexer, Object, white};

/// How deep forms are drawn inside forms.
const MAX_FORMS: usize = 8;
/// A move along a line, in thousandths of the font's size, that is a space between words.
const WORD_GAP: f64 = 200.0;

/// Reads pages' content, keeping the fonts it has read.
#[derive(Debug, Default)]
pub(super) struct Reader {
    fonts: HashMap<u32, Font>,
}

fn newline(out: &mut String) {
    while out.ends_with(' ') {
        out.pop();
    }
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
}

fn space(out: &mut String) {
    if !out.is_empty() && !out.ends_with([' ', '\n']) {
        out.push(' ');
    }
}

impl Reader {
    /// The font a resource name sets, read once per font.
    fn font_of(&mut self, doc: &mut Document<'_>, resources: &Object, name: &[u8]) -> Font {
        let fonts = doc.resolve(resources.get(b"Font").unwrap_or(&Object::Null));
        let Some(reference) = fonts.get(name).cloned() else {
            return Font::plain();
        };
        if let Object::Ref(n) = reference
            && let Some(f) = self.fonts.get(&n)
        {
            return f.clone();
        }
        let dict = doc.resolve(&reference);
        let mut fields = Vec::new();
        if let Object::Dict(given) = &dict {
            for (key, value) in given {
                let value = match key.as_slice() {
                    // Kept a reference, so its stream can be read.
                    b"ToUnicode" => value.clone(),
                    b"Encoding" => match doc.resolve(value) {
                        Object::Dict(inner) => {
                            Object::Dict(inner.iter().map(|(k, v)| (k.clone(), doc.resolve(v))).collect())
                        }
                        other => other,
                    },
                    _ => doc.resolve(value),
                };
                fields.push((key.clone(), value));
            }
        }
        let found = font(&Object::Dict(fields), &mut |o: &Object| doc.stream(o));
        if let Object::Ref(n) = reference {
            self.fonts.insert(n, found.clone());
        }
        found
    }

    /// The text a content stream shows, added to `out`.
    pub(super) fn run(
        &mut self,
        doc: &mut Document<'_>,
        bytes: &[u8],
        resources: &Object,
        out: &mut String,
        depth: usize,
    ) {
        if depth > MAX_FORMS {
            return;
        }
        let mut lexer = Lexer::new(bytes, 0, false);
        let mut operands: Vec<Object> = Vec::new();
        let mut current = Font::plain();
        let mut line_y: Option<f64> = None;
        while let Some(object) = lexer.object() {
            if out.len() > super::MAX_TEXT {
                return;
            }
            let Object::Word(op) = object else {
                operands.push(object);
                continue;
            };
            let number = |i: usize| operands.get(i).and_then(Object::number).unwrap_or(0.0);
            match op.as_slice() {
                b"Tf" => {
                    if let Some(name) = operands.first().and_then(Object::name) {
                        current = self.font_of(doc, resources, name);
                    }
                }
                b"Td" | b"TD" => {
                    let (x, y) = (number(0), number(1));
                    if y.abs() > 0.01 {
                        newline(out);
                    } else if x > 0.0 {
                        space(out);
                    }
                    line_y = Some(line_y.unwrap_or(0.0) + y);
                }
                b"Tm" => {
                    let y = number(5);
                    match line_y {
                        Some(old) if (old - y).abs() > 0.5 => newline(out),
                        Some(_) => space(out),
                        None => {}
                    }
                    line_y = Some(y);
                }
                b"T*" => newline(out),
                b"Tj" => {
                    if let Some(Object::String(s)) = operands.first() {
                        current.text(s, out);
                    }
                }
                b"'" | b"\"" => {
                    newline(out);
                    if let Some(Object::String(s)) = operands.last() {
                        current.text(s, out);
                    }
                }
                b"TJ" => {
                    if let Some(Object::Array(parts)) = operands.first() {
                        for part in parts {
                            match part {
                                Object::String(s) => current.text(s, out),
                                Object::Number(n) if *n < -WORD_GAP => space(out),
                                _ => {}
                            }
                        }
                    }
                }
                b"Do" => {
                    if let Some(name) = operands.first().and_then(Object::name) {
                        self.form(doc, resources, name, out, depth);
                    }
                }
                b"BI" => skip_image(&mut lexer),
                _ => {}
            }
            operands.clear();
        }
    }

    /// A form drawn on the page (`Do`): its own content read, with its own resources (or the page's).
    fn form(
        &mut self,
        doc: &mut Document<'_>,
        resources: &Object,
        name: &[u8],
        out: &mut String,
        depth: usize,
    ) {
        let objects = doc.resolve(resources.get(b"XObject").unwrap_or(&Object::Null));
        let Some(reference) = objects.get(name).cloned() else {
            return;
        };
        let dict = doc.resolve(&reference);
        if dict.get(b"Subtype").and_then(Object::name) != Some(b"Form") {
            return;
        }
        let Some(bytes) = doc.stream(&reference) else {
            return;
        };
        let own = dict
            .get(b"Resources")
            .map_or_else(|| resources.clone(), |r| doc.resolve(r));
        newline(out);
        self.run(doc, &bytes, &own, out, depth + 1);
    }
}

/// An inline image (`BI ... ID <bytes> EI`) passed over: its bytes are not content.
fn skip_image(lexer: &mut Lexer<'_>) {
    let bytes = lexer.bytes;
    let Some(id) = memchr::memmem::find(bytes.get(lexer.at..).unwrap_or_default(), b"ID") else {
        lexer.at = bytes.len();
        return;
    };
    let mut at = lexer.at + id + 2;
    loop {
        let Some(found) = memchr::memmem::find(bytes.get(at..).unwrap_or_default(), b"EI") else {
            lexer.at = bytes.len();
            return;
        };
        let end = at + found;
        let before = end.checked_sub(1).and_then(|i| bytes.get(i));
        let after = bytes.get(end + 2);
        if before.is_some_and(|b| white(*b)) && after.is_none_or(|b| white(*b)) {
            lexer.at = end + 2;
            return;
        }
        at = end + 2;
    }
}
