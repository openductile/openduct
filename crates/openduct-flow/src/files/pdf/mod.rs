// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A PDF's text, page by page: its objects found where they are written (and in object streams), its pages walked
//! from its catalog, and each page's content read for the text it shows, in each font's own codes.
//!
//! What it does not read: text drawn as pictures (a scan has none to give), and a protected (encrypted) PDF, which it
//! refuses. Everything is bounded - objects, pages, nesting, and what its streams unpack to in all.

use std::collections::{HashMap, HashSet};

mod content;
mod filters;
mod fonts;
mod lex;

use lex::{Lexer, Object, white};

/// The most objects a PDF is read with.
const MAX_OBJECTS: usize = 200_000;
/// The most pages read.
const MAX_PAGES: usize = 2_000;
/// The most its streams unpack to, in all.
const MAX_UNPACKED: usize = 64 * 1024 * 1024;
/// The most text it gives.
pub(super) const MAX_TEXT: usize = 4 * 1024 * 1024;

/// An object as the file holds it, and where its stream's bytes are.
#[derive(Debug, Clone)]
struct Held {
    object: Object,
    stream: Option<(usize, usize)>,
}

#[derive(Debug)]
struct Document<'a> {
    bytes: &'a [u8],
    objects: HashMap<u32, Held>,
    /// How many more bytes its streams may unpack to.
    budget: usize,
    /// Its trailers (and cross-reference streams): where its catalog is, and whether it is protected.
    trailers: Vec<Object>,
}

const BROKEN: &str = "not a PDF, or a damaged one";

/// Where an object's number starts, reading back from its `obj`: `12 0 obj`.
fn object_number(bytes: &[u8], obj: usize) -> Option<u32> {
    let mut i = obj;
    let back = |i: &mut usize, digits: bool| -> usize {
        let end = *i;
        while *i > 0
            && bytes
                .get(*i - 1)
                .is_some_and(|b| if digits { b.is_ascii_digit() } else { white(*b) })
        {
            *i -= 1;
        }
        end - *i
    };
    if back(&mut i, false) == 0 || back(&mut i, true) == 0 || back(&mut i, false) == 0 {
        return None;
    }
    let end = i;
    if back(&mut i, true) == 0 {
        return None;
    }
    if i > 0
        && bytes
            .get(i - 1)
            .is_some_and(|b| !white(*b) && !b"<>[]()/%}".contains(b))
    {
        return None;
    }
    std::str::from_utf8(bytes.get(i..end)?).ok()?.parse().ok()
}

impl<'a> Document<'a> {
    fn open(bytes: &'a [u8]) -> Result<Self, String> {
        if memchr::memmem::find(bytes.get(..1024).unwrap_or(bytes), b"%PDF").is_none() {
            return Err(BROKEN.to_owned());
        }
        let mut doc = Self {
            bytes,
            objects: HashMap::new(),
            budget: MAX_UNPACKED,
            trailers: Vec::new(),
        };
        for at in memchr::memmem::find_iter(bytes, b"obj") {
            if bytes
                .get(at + 3)
                .is_some_and(|b| !white(*b) && !b"<[(/%".contains(b))
            {
                continue;
            }
            let Some(number) = object_number(bytes, at) else {
                continue;
            };
            let mut lexer = Lexer::new(bytes, at + 3, true);
            let Some(object) = lexer.object() else { continue };
            lexer.skip_space();
            let stream = if bytes.get(lexer.at..lexer.at + 6) == Some(b"stream") {
                let mut start = lexer.at + 6;
                if bytes.get(start) == Some(&b'\r') {
                    start += 1;
                }
                if bytes.get(start) == Some(&b'\n') {
                    start += 1;
                }
                Some(start)
            } else {
                None
            };
            if object.get(b"Type").and_then(Object::name) == Some(b"XRef") {
                doc.trailers.push(object.clone());
            }
            if doc.objects.len() >= MAX_OBJECTS {
                return Err(format!("a PDF of more than {MAX_OBJECTS} objects"));
            }
            doc.objects.insert(
                number,
                Held {
                    object,
                    stream: stream.map(|s| (s, 0)),
                },
            );
        }
        for at in memchr::memmem::find_iter(bytes, b"trailer") {
            if let Some(t) = Lexer::new(bytes, at + 7, true).object() {
                doc.trailers.push(t);
            }
        }
        // Each stream's end: its length (perhaps another object), checked against `endstream`.
        let lengths: Vec<(u32, usize, Option<Object>)> = doc
            .objects
            .iter()
            .filter_map(|(n, h)| h.stream.map(|(s, _)| (*n, s, h.object.get(b"Length").cloned())))
            .collect();
        for (number, start, length) in lengths {
            let length = length.map(|l| doc.resolve(&l)).and_then(|l| l.number());
            let end = length
                .filter(|l| *l >= 0.0)
                .map(|l| start.saturating_add(usize::try_from(l as u64).unwrap_or(usize::MAX)))
                .filter(|end| {
                    let mut after = *end;
                    while bytes.get(after).is_some_and(|b| white(*b)) {
                        after += 1;
                    }
                    bytes.get(after..after + 9) == Some(b"endstream")
                })
                .or_else(|| {
                    let found = memchr::memmem::find(bytes.get(start..)?, b"endstream")?;
                    let mut end = start + found;
                    while end > start && bytes.get(end - 1).is_some_and(|b| *b == b'\n' || *b == b'\r') {
                        end -= 1;
                    }
                    Some(end)
                });
            if let (Some(end), Some(held)) = (end, doc.objects.get_mut(&number)) {
                held.stream = Some((start, end));
            }
        }
        doc.object_streams();
        if doc.trailers.iter().any(|t| t.get(b"Encrypt").is_some()) {
            return Err("the PDF is protected (encrypted): its text cannot be read here".to_owned());
        }
        Ok(doc)
    }

    /// The objects kept in object streams, beside those written where they are.
    fn object_streams(&mut self) {
        let streams: Vec<u32> = self
            .objects
            .iter()
            .filter(|(_, h)| h.object.get(b"Type").and_then(Object::name) == Some(b"ObjStm"))
            .map(|(n, _)| *n)
            .collect();
        for number in streams {
            let Some(bytes) = self.stream_of(number) else {
                continue;
            };
            let Some(held) = self.objects.get(&number) else {
                continue;
            };
            let count = held.object.get(b"N").and_then(Object::number).unwrap_or(0.0);
            let first = held.object.get(b"First").and_then(Object::number).unwrap_or(0.0);
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // counts and offsets
            let (count, first) = (count.clamp(0.0, 100_000.0) as usize, first.max(0.0) as usize);
            let mut header = Lexer::new(&bytes, 0, false);
            let mut found = Vec::new();
            for _ in 0..count {
                let (Some(Object::Number(n)), Some(Object::Number(at))) = (header.object(), header.object())
                else {
                    break;
                };
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                // an object's number and place
                found.push((n as u32, at.max(0.0) as usize));
            }
            for (n, at) in found {
                if self.objects.contains_key(&n) || self.objects.len() >= MAX_OBJECTS {
                    continue;
                }
                if let Some(object) = Lexer::new(&bytes, first.saturating_add(at), true).object() {
                    self.objects.insert(n, Held { object, stream: None });
                }
            }
        }
    }

    /// An object, its reference followed (a few times at most: a reference to a reference is rare).
    fn resolve(&self, object: &Object) -> Object {
        let mut current = object.clone();
        for _ in 0..8 {
            match current {
                Object::Ref(n) => {
                    current = self.objects.get(&n).map_or(Object::Null, |h| h.object.clone());
                }
                other => return other,
            }
        }
        Object::Null
    }

    /// The bytes of object `number`'s stream, its filters undone; `None` when it has none, or they cannot be.
    fn stream_of(&mut self, number: u32) -> Option<Vec<u8>> {
        let held = self.objects.get(&number)?;
        let (start, end) = held.stream?;
        let raw = self.bytes.get(start..end.max(start))?;
        let dict = held.object.clone();
        filters::decode(raw, &dict, &mut self.budget).ok()
    }

    /// The bytes of the stream an object refers to.
    fn stream(&mut self, object: &Object) -> Option<Vec<u8>> {
        match object {
            Object::Ref(n) => self.stream_of(*n),
            _ => None,
        }
    }

    /// Each page's dictionary, in order, with the resources it inherits.
    fn pages(&self) -> Vec<(Object, Object)> {
        let root = self
            .trailers
            .iter()
            .rev()
            .find_map(|t| t.get(b"Root").cloned())
            .map(|r| self.resolve(&r))
            .filter(|r| r.get(b"Pages").is_some())
            .or_else(|| {
                self.objects
                    .values()
                    .find(|h| h.object.get(b"Type").and_then(Object::name) == Some(b"Catalog"))
                    .map(|h| h.object.clone())
            });
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        if let Some(pages) = root.as_ref().and_then(|r| r.get(b"Pages")) {
            self.walk(pages, &Object::Null, 0, &mut seen, &mut out);
        }
        out
    }

    fn walk(
        &self,
        node: &Object,
        inherited: &Object,
        depth: usize,
        seen: &mut HashSet<u32>,
        out: &mut Vec<(Object, Object)>,
    ) {
        if depth > 32 || out.len() >= MAX_PAGES {
            return;
        }
        if let Object::Ref(n) = node
            && !seen.insert(*n)
        {
            return;
        }
        let dict = self.resolve(node);
        let resources = dict
            .get(b"Resources")
            .map_or_else(|| inherited.clone(), |r| self.resolve(r));
        match dict.get(b"Kids").map(|k| self.resolve(k)) {
            Some(Object::Array(kids)) => {
                for kid in &kids {
                    self.walk(kid, &resources, depth + 1, seen, out);
                }
            }
            _ if dict.get(b"Contents").is_some()
                || dict.get(b"Type").and_then(Object::name) == Some(b"Page") =>
            {
                out.push((dict, resources));
            }
            _ => {}
        }
    }
}

/// A PDF's text, page after page (a blank line between pages), and how many pages it has.
pub(super) fn text(bytes: &[u8]) -> Result<(String, usize), String> {
    let mut doc = Document::open(bytes)?;
    let pages = doc.pages();
    if pages.is_empty() {
        return Err("the PDF has no pages to read".to_owned());
    }
    let mut reader = content::Reader::default();
    let mut out = String::new();
    for (page, resources) in &pages {
        let contents = match page.get(b"Contents") {
            Some(Object::Array(list)) => list.clone(),
            Some(Object::Ref(n)) => match doc.resolve(&Object::Ref(*n)) {
                Object::Array(list) => list,
                _ => vec![Object::Ref(*n)],
            },
            _ => Vec::new(),
        };
        let mut bytes = Vec::new();
        for part in &contents {
            if let Some(b) = doc.stream(part) {
                bytes.extend_from_slice(&b);
                bytes.push(b'\n');
            }
        }
        let mut page_text = String::new();
        reader.run(&mut doc, &bytes, resources, &mut page_text, 0);
        let page_text: Vec<&str> = page_text.lines().map(str::trim).collect();
        let page_text = page_text.join("\n");
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(page_text.trim());
        if out.len() > MAX_TEXT {
            return Err(format!("a PDF of more than {MAX_TEXT} bytes of text"));
        }
    }
    Ok((out, pages.len()))
}

#[cfg(test)]
mod tests;
