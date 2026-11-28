// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A small XML reader, enough for a spreadsheet's parts: elements (their names without a prefix), attributes and
//! text, entities decoded; comments, declarations and processing instructions passed over. And text escaped for
//! writing.

/// What the reader finds next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Event<'a> {
    /// An element opened, with its attributes; `empty` when it closes at once (`<c/>`).
    Open {
        name: &'a str,
        attributes: Vec<(&'a str, String)>,
        empty: bool,
    },
    Close(&'a str),
    Text(String),
}

/// Reads XML text event by event.
#[derive(Debug)]
pub(super) struct Reader<'a> {
    text: &'a str,
    at: usize,
}

/// A name without its prefix: `x:row` is `row`.
fn local(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, n)| n)
}

/// Text with its entities decoded: `&amp;`, `&lt;`, `&#65;`, `&#x41;`...
pub(super) fn decode(text: &str) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(rest.get(..at).unwrap_or_default());
        rest = rest.get(at..).unwrap_or_default();
        let Some(end) = rest.find(';').filter(|e| *e <= 12) else {
            out.push('&');
            rest = rest.get(1..).unwrap_or_default();
            continue;
        };
        let name = rest.get(1..end).unwrap_or_default();
        let found = match name {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            n => n
                .strip_prefix("#x")
                .or_else(|| n.strip_prefix("#X"))
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| n.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match found {
            Some(c) => {
                out.push(c);
                rest = rest.get(end + 1..).unwrap_or_default();
            }
            None => {
                out.push('&');
                rest = rest.get(1..).unwrap_or_default();
            }
        }
    }
    out.push_str(rest);
    out
}

/// Text as XML writes it, in an element or an attribute.
pub(super) fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            // Characters XML 1.0 does not allow are left out.
            c if (c as u32) < 0x20 && !matches!(c, '\t' | '\n' | '\r') => {}
            '\u{fffe}' | '\u{ffff}' => {}
            c => out.push(c),
        }
    }
    out
}

fn attributes(text: &str) -> Vec<(&str, String)> {
    let mut out = Vec::new();
    let mut rest = text.trim_start();
    while let Some(eq) = rest.find('=') {
        let name = rest.get(..eq).unwrap_or_default().trim();
        let after = rest.get(eq + 1..).unwrap_or_default().trim_start();
        let Some(quote) = after.chars().next().filter(|c| *c == '"' || *c == '\'') else {
            break;
        };
        let inner = after.get(1..).unwrap_or_default();
        let Some(end) = inner.find(quote) else { break };
        out.push((local(name), decode(inner.get(..end).unwrap_or_default())));
        rest = inner.get(end + 1..).unwrap_or_default().trim_start();
    }
    out
}

impl<'a> Reader<'a> {
    pub(super) fn new(text: &'a str) -> Self {
        Self {
            text: text.strip_prefix('\u{feff}').unwrap_or(text),
            at: 0,
        }
    }

    fn rest(&self) -> &'a str {
        self.text.get(self.at..).unwrap_or_default()
    }

    /// The next event; `None` at the end. Markup that never ends is the end.
    pub(super) fn next_event(&mut self) -> Option<Event<'a>> {
        loop {
            let rest = self.rest();
            if rest.is_empty() {
                return None;
            }
            if !rest.starts_with('<') {
                let end = rest.find('<').unwrap_or(rest.len());
                self.at += end;
                return Some(Event::Text(decode(rest.get(..end).unwrap_or_default())));
            }
            if let Some(after) = rest.strip_prefix("<![CDATA[") {
                let end = after.find("]]>")?;
                self.at += 9 + end + 3;
                return Some(Event::Text(after.get(..end).unwrap_or_default().to_owned()));
            }
            let skip = [("<!--", "-->"), ("<?", "?>"), ("<!", ">")]
                .iter()
                .find(|(open, _)| rest.starts_with(open));
            if let Some((_, close)) = skip {
                let end = rest.find(close)?;
                self.at += end + close.len();
                continue;
            }
            let end = rest.find('>')?;
            let inside = rest.get(1..end).unwrap_or_default();
            self.at += end + 1;
            if let Some(name) = inside.strip_prefix('/') {
                return Some(Event::Close(local(name.trim())));
            }
            let (inside, empty) = match inside.strip_suffix('/') {
                Some(i) => (i, true),
                None => (inside, false),
            };
            let split = inside.find(char::is_whitespace).unwrap_or(inside.len());
            let name = local(inside.get(..split).unwrap_or_default());
            return Some(Event::Open {
                name,
                attributes: attributes(inside.get(split..).unwrap_or_default()),
                empty,
            });
        }
    }
}

/// An attribute's value, by its name (without a prefix).
pub(super) fn attribute<'v>(attributes: &'v [(&str, String)], name: &str) -> Option<&'v str> {
    attributes
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_reads_elements_attributes_and_text() {
        let text = "<?xml version=\"1.0\"?><!-- a note --><x:row r='2' spans=\"1:3\"><c t=\"s\"><v>4</v></c>\
                    <c/><t>a &amp; b &#x263A; &#65; &bogus; <![CDATA[<raw>]]></t></x:row>";
        let mut reader = Reader::new(text);
        let mut events = Vec::new();
        while let Some(e) = reader.next_event() {
            events.push(e);
        }
        assert_eq!(
            events[0],
            Event::Open {
                name: "row",
                attributes: vec![("r", "2".into()), ("spans", "1:3".into())],
                empty: false
            }
        );
        assert!(events.contains(&Event::Open {
            name: "c",
            attributes: vec![],
            empty: true
        }));
        assert!(events.contains(&Event::Text("a & b \u{263a} A &bogus; ".into())));
        assert!(events.contains(&Event::Text("<raw>".into())));
        assert_eq!(events.last(), Some(&Event::Close("row")));
        assert_eq!(escape("a<b & \"c\"\u{1}"), "a&lt;b &amp; &quot;c&quot;");
        let mut cut = Reader::new("<a><b");
        assert!(cut.next_event().is_some());
        assert_eq!(cut.next_event(), None);
    }
}
