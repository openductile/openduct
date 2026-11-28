// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Markup read token by token: tags with their attributes, and the text between them, references read. HTML's rules
//! where they matter to what a page shows: names in small letters, a script's or a style's content running to its own
//! closing tag (`if (a < b)` is not a tag), a title's and a text area's content read as text, comments and the doctype
//! passed over, a tag the page never closes dropped. XML (a feed, a sitemap) keeps names as written, its own empty
//! tags (`<link/>`) and CDATA sections as text. Linear in the text's length; nothing is bounded here but by the text.

use std::borrow::Cow;

use super::entities::decode;

/// The most attributes kept of one tag: the rest are passed over.
const MAX_ATTRIBUTES: usize = 128;

/// A start tag's attributes, whether it closes itself, and where it ends.
type Tag = (Vec<(String, String)>, bool, usize);

/// One token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Token<'a> {
    /// A tag that opens an element; `closed` when written `<x/>`.
    Start {
        name: String,
        attributes: Vec<(String, String)>,
        closed: bool,
    },
    End(String),
    Text(Cow<'a, str>),
}

/// How an element's content is read in HTML, until its own closing tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Raw {
    /// As it is: a script, a style.
    Plain,
    /// As text with its references read: a title, a text area.
    Text,
}

fn raw_of(name: &str) -> Option<Raw> {
    match name {
        "script" | "style" | "xmp" | "iframe" | "noembed" | "noframes" | "noscript" => Some(Raw::Plain),
        "title" | "textarea" => Some(Raw::Text),
        _ => None,
    }
}

/// Reads tokens from markup.
#[derive(Debug)]
pub(crate) struct Tokens<'a> {
    text: &'a str,
    at: usize,
    xml: bool,
    /// The element whose content is read as it is, until its closing tag.
    raw: Option<(String, Raw)>,
}

/// Where `</name` (any case) next starts, from `from`, followed by what ends a tag's name.
fn closing(text: &str, from: usize, name: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut at = from;
    loop {
        let found = memchr::memmem::find(bytes.get(at..)?, b"</")?;
        let pos = at + found;
        let end = pos + 2 + name.len();
        let named = bytes
            .get(pos + 2..end)
            .is_some_and(|n| n.eq_ignore_ascii_case(name.as_bytes()));
        let ended = bytes
            .get(end)
            .is_none_or(|b| b.is_ascii_whitespace() || *b == b'>' || *b == b'/');
        if named && ended {
            return Some(pos);
        }
        at = pos + 2;
    }
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c')
}

impl<'a> Tokens<'a> {
    pub(crate) fn new(text: &'a str, xml: bool) -> Self {
        Self {
            text: text.strip_prefix('\u{feff}').unwrap_or(text),
            at: 0,
            xml,
            raw: None,
        }
    }

    fn rest(&self) -> &'a str {
        self.text.get(self.at..).unwrap_or_default()
    }

    fn name(&self, name: &str) -> String {
        if self.xml {
            name.to_owned()
        } else {
            name.to_ascii_lowercase()
        }
    }

    /// The content of a script, a style, a title ...: up to its closing tag, which comes next.
    fn raw_text(&mut self, name: &str, raw: Raw) -> Option<Token<'a>> {
        let end = closing(self.text, self.at, name).unwrap_or(self.text.len());
        let inside = self.text.get(self.at..end).unwrap_or_default();
        self.at = end;
        if inside.is_empty() {
            return None;
        }
        Some(Token::Text(match raw {
            Raw::Plain => Cow::Borrowed(inside),
            Raw::Text => Cow::Owned(decode(inside, false)),
        }))
    }

    /// Passes over what starts at `self.at` up to and with `until`; to the end when it never comes.
    fn pass(&mut self, from: usize, until: &str) {
        let start = self.at + from;
        self.at = self
            .text
            .get(start..)
            .and_then(|r| r.find(until))
            .map_or(self.text.len(), |e| start + e + until.len());
    }

    /// A tag's attributes, from `at` (just after its name): the attributes, whether it closes itself, and where it
    /// ends (just after its `>`). `None` when the text ends inside it.
    fn attributes(&self, mut at: usize) -> Option<Tag> {
        let bytes = self.text.as_bytes();
        let mut out: Vec<(String, String)> = Vec::new();
        loop {
            while bytes.get(at).copied().is_some_and(is_space) {
                at += 1;
            }
            match bytes.get(at)? {
                b'>' => return Some((out, false, at + 1)),
                b'/' if bytes.get(at + 1) == Some(&b'>') => return Some((out, true, at + 2)),
                b'/' => {
                    at += 1;
                    continue;
                }
                _ => {}
            }
            let start = at;
            at += 1;
            while bytes
                .get(at)
                .is_some_and(|b| !is_space(*b) && !matches!(b, b'/' | b'>' | b'='))
            {
                at += 1;
            }
            let name = self.text.get(start..at).unwrap_or_default();
            while bytes.get(at).copied().is_some_and(is_space) {
                at += 1;
            }
            let mut value = String::new();
            if bytes.get(at) == Some(&b'=') {
                at += 1;
                while bytes.get(at).copied().is_some_and(is_space) {
                    at += 1;
                }
                let raw = match bytes.get(at)? {
                    q @ (b'"' | b'\'') => {
                        let from = at + 1;
                        let end = memchr::memchr(*q, bytes.get(from..)?).map(|e| from + e)?;
                        at = end + 1;
                        self.text.get(from..end).unwrap_or_default()
                    }
                    _ => {
                        let from = at;
                        while bytes.get(at).is_some_and(|b| !is_space(*b) && *b != b'>') {
                            at += 1;
                        }
                        self.text.get(from..at).unwrap_or_default()
                    }
                };
                value = decode(raw, true);
            }
            let name = self.name(name);
            if out.len() < MAX_ATTRIBUTES && !out.iter().any(|(n, _)| *n == name) {
                out.push((name, value));
            }
        }
    }

    /// The next token; `None` at the end.
    pub(crate) fn next_token(&mut self) -> Option<Token<'a>> {
        if let Some((name, raw)) = self.raw.take()
            && let Some(text) = self.raw_text(&name, raw)
        {
            return Some(text);
        }
        loop {
            let rest = self.rest();
            if rest.is_empty() {
                return None;
            }
            let bytes = rest.as_bytes();
            if bytes.first() != Some(&b'<') {
                let end = memchr::memchr(b'<', bytes).unwrap_or(rest.len());
                self.at += end;
                let text = rest.get(..end).unwrap_or_default();
                return Some(Token::Text(if text.contains('&') {
                    Cow::Owned(decode(text, false))
                } else {
                    Cow::Borrowed(text)
                }));
            }
            match bytes.get(1) {
                Some(b'!') => {
                    if rest.starts_with("<!--") {
                        self.pass(4, "-->");
                    } else if let Some(inside) = rest.strip_prefix("<![CDATA[") {
                        let end = inside.find("]]>").unwrap_or(inside.len());
                        self.at += 9 + end + 3;
                        self.at = self.at.min(self.text.len());
                        let text = inside.get(..end).unwrap_or_default();
                        if !text.is_empty() {
                            return Some(Token::Text(Cow::Borrowed(text)));
                        }
                    } else {
                        self.pass(2, ">");
                    }
                }
                Some(b'?') => self.pass(2, if self.xml { "?>" } else { ">" }),
                Some(b'/') => {
                    let bytes = rest.as_bytes();
                    if !bytes.get(2).is_some_and(u8::is_ascii_alphabetic) {
                        // `</>` and `</ 3>`: not a tag a page means.
                        self.pass(2, ">");
                        continue;
                    }
                    let len = bytes
                        .iter()
                        .skip(2)
                        .take_while(|b| !is_space(**b) && **b != b'>' && **b != b'/')
                        .count();
                    let name = self.name(rest.get(2..2 + len).unwrap_or_default());
                    self.pass(2 + len, ">");
                    return Some(Token::End(name));
                }
                Some(b) if b.is_ascii_alphabetic() => {
                    let len = bytes
                        .iter()
                        .skip(1)
                        .take_while(|b| !is_space(**b) && **b != b'>' && **b != b'/')
                        .count();
                    let name = self.name(rest.get(1..1 + len).unwrap_or_default());
                    let Some((attributes, closed, end)) = self.attributes(self.at + 1 + len) else {
                        // A tag the text ends inside: dropped, as a browser drops it.
                        self.at = self.text.len();
                        return None;
                    };
                    self.at = end;
                    if !self.xml
                        && !closed
                        && let Some(raw) = raw_of(&name)
                    {
                        self.raw = Some((name.clone(), raw));
                    }
                    return Some(Token::Start {
                        name,
                        attributes,
                        closed,
                    });
                }
                _ => {
                    // A `<` that opens nothing: `a < b`, `<3`.
                    let end = memchr::memchr(b'<', bytes.get(1..).unwrap_or_default())
                        .map_or(rest.len(), |e| e + 1);
                    self.at += end;
                    return Some(Token::Text(Cow::Owned(decode(
                        rest.get(..end).unwrap_or_default(),
                        false,
                    ))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(text: &str, xml: bool) -> Vec<Token<'_>> {
        let mut t = Tokens::new(text, xml);
        let mut out = Vec::new();
        while let Some(token) = t.next_token() {
            out.push(token);
        }
        out
    }

    fn start(name: &str, attributes: &[(&str, &str)], closed: bool) -> Token<'static> {
        Token::Start {
            name: name.to_owned(),
            attributes: attributes
                .iter()
                .map(|(n, v)| ((*n).to_owned(), (*v).to_owned()))
                .collect(),
            closed,
        }
    }

    #[test]
    fn tags_attributes_and_text_are_read_as_a_browser_reads_them() {
        let tokens = all(
            "<!DOCTYPE html><!-- note --><A HREF=\"/x?a=1&amp;b=2\" data-x=one class='c d' hidden>Fish &amp; \
             chips</a><br/><img src=p.png alt=\"a > b\"><p>a < b <3</p>",
            false,
        );
        assert_eq!(
            tokens,
            vec![
                start(
                    "a",
                    &[
                        ("href", "/x?a=1&b=2"),
                        ("data-x", "one"),
                        ("class", "c d"),
                        ("hidden", "")
                    ],
                    false
                ),
                Token::Text("Fish & chips".into()),
                Token::End("a".into()),
                start("br", &[], true),
                start("img", &[("src", "p.png"), ("alt", "a > b")], false),
                start("p", &[], false),
                Token::Text("a ".into()),
                Token::Text("< b ".into()),
                Token::Text("<3".into()),
                Token::End("p".into()),
            ]
        );
    }

    #[test]
    fn a_scripts_content_is_not_markup_and_a_title_is_text() {
        let tokens = all(
            "<script>if (a < b && c > d) { x = '</div>' }</SCRIPT ><title>A &amp; B <i>not</i></title>",
            false,
        );
        assert_eq!(
            tokens,
            vec![
                start("script", &[], false),
                Token::Text("if (a < b && c > d) { x = '</div>' }".into()),
                Token::End("script".into()),
                start("title", &[], false),
                Token::Text("A & B <i>not</i>".into()),
                Token::End("title".into()),
            ]
        );
        assert_eq!(
            all("<style>p{}", false),
            vec![start("style", &[], false), Token::Text("p{}".into())]
        );
    }

    #[test]
    fn xml_keeps_names_empty_tags_and_cdata() {
        let tokens = all(
            "<?xml version=\"1.0\"?><rss><item><Title><![CDATA[<b>Hi</b>]]></Title><atom:link href=\"u\"/></item></rss>",
            true,
        );
        assert_eq!(
            tokens,
            vec![
                start("rss", &[], false),
                start("item", &[], false),
                start("Title", &[], false),
                Token::Text("<b>Hi</b>".into()),
                Token::End("Title".into()),
                start("atom:link", &[("href", "u")], true),
                Token::End("item".into()),
                Token::End("rss".into()),
            ]
        );
    }

    #[test]
    fn broken_markup_ends_without_a_panic() {
        for text in [
            "<a href=\"never",
            "<a",
            "</",
            "<!--",
            "<![CDATA[x",
            "<p class=",
            "<",
            "&",
            "<x y='1' z",
            "</a",
            "<script>x",
            "<a b=c d=\"",
        ] {
            let _ = all(text, false);
            let _ = all(text, true);
        }
        assert_eq!(all("ok <a href=\"x", false), vec![Token::Text("ok ".into())]);
    }
}
