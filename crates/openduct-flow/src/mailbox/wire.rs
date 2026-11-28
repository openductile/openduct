// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! IMAP's words on the wire (RFC 9051, and RFC 3501 which most servers still speak): a server's answer read into
//! its parts - atoms, quoted strings, literals and parenthesised lists - and what a command sends written so the
//! server reads each argument as one: an atom, a quoted string, or a literal for text a quoted string cannot hold.
//! Folder names outside ASCII are in IMAP's modified UTF-7 (RFC 3501 5.1.3).
//!
//! Everything a server sends is bounded before it is held: a line, a literal, the whole answer, how deep its lists
//! go and how many parts it has.

use base64::Engine as _;

/// How deep a server's lists may go, and how many parts one answer may have.
pub(crate) const MAX_DEPTH: usize = 24;
pub(crate) const MAX_PARTS: usize = 400_000;

/// One part of an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item {
    /// An atom or a number: `FLAGS`, `\Seen`, `42`, `BODY[HEADER.FIELDS (FROM)]`.
    Atom(String),
    /// A quoted string or a literal, as bytes.
    Text(Vec<u8>),
    /// `( ... )`.
    List(Vec<Item>),
    Nil,
}

impl Item {
    /// The atom, in capitals: what a name is compared as.
    pub(crate) fn name(&self) -> Option<String> {
        match self {
            Self::Atom(a) => Some(a.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// The text of an atom, a string or a literal; none for NIL or a list.
    pub(crate) fn text(&self) -> Option<String> {
        match self {
            Self::Atom(a) => Some(a.clone()),
            Self::Text(b) => Some(String::from_utf8_lossy(b).into_owned()),
            Self::List(_) | Self::Nil => None,
        }
    }

    pub(crate) fn number(&self) -> Option<u64> {
        match self {
            Self::Atom(a) => a.parse().ok(),
            _ => None,
        }
    }

    pub(crate) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Text(b) => Some(b),
            _ => None,
        }
    }
}

/// An answer as read: its text, each literal in it left as `{n}`, and the literals' bytes in order.
#[derive(Debug, Default)]
pub(crate) struct Raw {
    pub text: Vec<u8>,
    pub literals: Vec<Vec<u8>>,
}

/// The number of bytes a line ending in a literal's `{n}` (or `{n+}`) says follow it.
pub(crate) fn literal_size(line: &[u8]) -> Option<usize> {
    let line = line.strip_suffix(b"}")?;
    let open = line.iter().rposition(|b| *b == b'{')?;
    let digits = line.get(open + 1..)?;
    let digits = digits.strip_suffix(b"+").unwrap_or(digits);
    if digits.is_empty() || digits.len() > 12 || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// Reads the parts of an answer's text, from `at`: its words up to the end, lists read whole.
pub(crate) fn parse(raw: &Raw, at: usize) -> Result<Vec<Item>, String> {
    let mut p = Parser {
        text: &raw.text,
        at,
        literals: raw.literals.iter(),
        parts: 0,
    };
    p.items(0, false)
}

struct Parser<'a> {
    text: &'a [u8],
    at: usize,
    literals: std::slice::Iter<'a, Vec<u8>>,
    parts: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.get(self.at).copied()
    }

    fn items(&mut self, depth: usize, inside: bool) -> Result<Vec<Item>, String> {
        if depth > MAX_DEPTH {
            return Err("the server's answer is nested too deep".to_owned());
        }
        let mut out = Vec::new();
        loop {
            while self.peek() == Some(b' ') {
                self.at += 1;
            }
            let Some(b) = self.peek() else {
                return if inside {
                    Err("the server's answer has a list that does not end".to_owned())
                } else {
                    Ok(out)
                };
            };
            self.parts += 1;
            if self.parts > MAX_PARTS {
                return Err("the server's answer has too many parts".to_owned());
            }
            match b {
                b')' if inside => {
                    self.at += 1;
                    return Ok(out);
                }
                b'(' => {
                    self.at += 1;
                    out.push(Item::List(self.items(depth + 1, true)?));
                }
                b'"' => out.push(Item::Text(self.quoted()?)),
                b'{' => out.push(Item::Text(self.literal()?)),
                _ => {
                    let atom = self.atom();
                    if atom.is_empty() {
                        // A stray `)` outside a list, or a byte no atom holds: skipped.
                        self.at += 1;
                        continue;
                    }
                    out.push(if atom.eq_ignore_ascii_case("NIL") {
                        Item::Nil
                    } else {
                        Item::Atom(atom)
                    });
                }
            }
        }
    }

    fn quoted(&mut self) -> Result<Vec<u8>, String> {
        self.at += 1;
        let mut out = Vec::new();
        while let Some(b) = self.peek() {
            self.at += 1;
            match b {
                b'"' => return Ok(out),
                b'\\' => {
                    if let Some(next) = self.peek() {
                        out.push(next);
                        self.at += 1;
                    }
                }
                _ => out.push(b),
            }
        }
        Err("the server's answer has a quoted string that does not end".to_owned())
    }

    fn literal(&mut self) -> Result<Vec<u8>, String> {
        while let Some(b) = self.peek() {
            self.at += 1;
            if b == b'}' {
                break;
            }
        }
        self.literals
            .next()
            .cloned()
            .ok_or_else(|| "the server's answer names a literal it did not send".to_owned())
    }

    /// An atom: up to a space or a bracket that ends it; a `[...]` inside it (a section, `BODY[HEADER]`) is read whole.
    fn atom(&mut self) -> String {
        let start = self.at;
        let mut square = 0usize;
        while let Some(b) = self.peek() {
            match b {
                b'[' => square += 1,
                b']' if square > 0 => square -= 1,
                b' ' | b'(' | b')' if square == 0 => break,
                b'"' | b'{' if square == 0 => break,
                _ => {}
            }
            self.at += 1;
        }
        String::from_utf8_lossy(self.text.get(start..self.at).unwrap_or_default()).into_owned()
    }
}

/// An argument of a command, as it goes on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Arg {
    /// Sent as it is: a command's own words, numbers, a set of UIDs, `(\Seen)`.
    Raw(String),
    /// Text the server reads as one string: quoted, or a literal when quotes cannot hold it.
    Text(String),
}

/// How a text argument goes: `None` for a quoted string, or the bytes of a literal.
pub(crate) fn quoted(text: &str) -> Option<String> {
    let plain = text.bytes().all(|b| (0x20..0x7f).contains(&b));
    plain.then(|| {
        let mut out = String::with_capacity(text.len() + 2);
        out.push('"');
        for c in text.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
        out
    })
}

/// A folder's name as IMAP writes it: modified UTF-7 for what is outside printable ASCII, `&` as `&-`.
pub(crate) fn folder_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut wide: Vec<u16> = Vec::new();
    let flush = |wide: &mut Vec<u16>, out: &mut String| {
        if wide.is_empty() {
            return;
        }
        let bytes: Vec<u8> = wide.iter().flat_map(|u| u.to_be_bytes()).collect();
        let encoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .encode(bytes)
            .replace('/', ",");
        out.push('&');
        out.push_str(&encoded);
        out.push('-');
        wide.clear();
    };
    for c in name.chars() {
        if (' '..='~').contains(&c) {
            flush(&mut wide, &mut out);
            if c == '&' {
                out.push_str("&-");
            } else {
                out.push(c);
            }
        } else {
            let mut units = [0u16; 2];
            wide.extend_from_slice(c.encode_utf16(&mut units));
        }
    }
    flush(&mut wide, &mut out);
    out
}

/// A folder's name as people read it, from IMAP's modified UTF-7; a name that is not that is kept as it is.
pub(crate) fn folder_text(wire: &str) -> String {
    let mut out = String::with_capacity(wire.len());
    let mut rest = wire;
    while let Some(open) = rest.find('&') {
        out.push_str(rest.get(..open).unwrap_or_default());
        let after = rest.get(open + 1..).unwrap_or_default();
        let Some(close) = after.find('-') else {
            out.push_str(rest.get(open..).unwrap_or_default());
            return out;
        };
        let inner = after.get(..close).unwrap_or_default();
        if inner.is_empty() {
            out.push('&');
        } else {
            let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(inner.replace(',', "/"))
                .ok()
                .filter(|b| b.len() % 2 == 0)
                .map(|b| {
                    let units: Vec<u16> = b
                        .chunks(2)
                        .map(|p| {
                            u16::from_be_bytes([
                                p.first().copied().unwrap_or(0),
                                p.get(1).copied().unwrap_or(0),
                            ])
                        })
                        .collect();
                    String::from_utf16_lossy(&units)
                });
            match decoded {
                Some(text) => out.push_str(&text),
                None => {
                    out.push('&');
                    out.push_str(inner);
                    out.push('-');
                }
            }
        }
        rest = after.get(close + 1..).unwrap_or_default();
    }
    out.push_str(rest);
    out
}

/// Text that goes into a command: no line breaks or NUL, which would end it or start another.
pub(crate) fn safe(text: &str, what: &str) -> Result<(), String> {
    if text.contains(['\r', '\n', '\0']) {
        Err(format!("{what} holds a line break"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]
mod tests {
    use super::*;

    fn raw(text: &str, literals: &[&str]) -> Raw {
        Raw {
            text: text.as_bytes().to_vec(),
            literals: literals.iter().map(|l| l.as_bytes().to_vec()).collect(),
        }
    }

    #[test]
    fn an_answer_reads_into_its_parts() {
        let r = raw(
            "* 12 FETCH (UID 34 FLAGS (\\Seen \\Flagged) INTERNALDATE \"17-Jul-1996 02:44:25 -0700\" \
             BODY[HEADER.FIELDS (FROM SUBJECT)] {9} NIL)",
            &["Subject: x"],
        );
        let items = parse(&r, 2).unwrap();
        assert_eq!(items[0], Item::Atom("12".into()));
        assert_eq!(items[1].name().as_deref(), Some("FETCH"));
        let Item::List(fetch) = &items[2] else { panic!() };
        assert_eq!(fetch[0].name().as_deref(), Some("UID"));
        assert_eq!(fetch[1].number(), Some(34));
        assert_eq!(
            fetch[3],
            Item::List(vec![Item::Atom("\\Seen".into()), Item::Atom("\\Flagged".into())])
        );
        assert_eq!(fetch[5].text().as_deref(), Some("17-Jul-1996 02:44:25 -0700"));
        assert_eq!(
            fetch[6].name().as_deref(),
            Some("BODY[HEADER.FIELDS (FROM SUBJECT)]")
        );
        assert_eq!(fetch[7].bytes(), Some(&b"Subject: x"[..]));
        assert_eq!(fetch[8], Item::Nil);
    }

    #[test]
    fn hostile_answers_are_refused_not_followed() {
        let deep = format!("{}{}", "(".repeat(100), ")".repeat(100));
        assert!(parse(&raw(&deep, &[]), 0).unwrap_err().contains("nested"));
        assert!(parse(&raw("(a b", &[]), 0).unwrap_err().contains("does not end"));
        assert!(
            parse(&raw("\"open", &[]), 0)
                .unwrap_err()
                .contains("does not end")
        );
        assert!(parse(&raw("{5}", &[]), 0).unwrap_err().contains("did not send"));
        assert_eq!(literal_size(b"* 1 FETCH (BODY[] {42}"), Some(42));
        assert_eq!(literal_size(b"A {7+}"), Some(7));
        assert_eq!(literal_size(b"A {}"), None);
        assert_eq!(literal_size(b"A {99999999999999}"), None);
        assert_eq!(literal_size(b"A OK done"), None);
    }

    #[test]
    fn text_is_quoted_or_sent_as_a_literal() {
        assert_eq!(quoted("a \"b\" \\c").as_deref(), Some("\"a \\\"b\\\" \\\\c\""));
        assert_eq!(quoted("caf\u{e9}"), None);
        assert!(safe("x\r\nA2 DELETE INBOX", "it").is_err());
    }

    #[test]
    fn folder_names_in_modified_utf7() {
        // RFC 3501's own example: ~peter/mail/台北/日本語
        let name = "~peter/mail/\u{53f0}\u{5317}/\u{65e5}\u{672c}\u{8a9e}";
        assert_eq!(folder_name(name), "~peter/mail/&U,BTFw-/&ZeVnLIqe-");
        assert_eq!(folder_text("~peter/mail/&U,BTFw-/&ZeVnLIqe-"), name);
        assert_eq!(folder_name("A&B"), "A&-B");
        assert_eq!(folder_text("A&-B"), "A&B");
        assert_eq!(folder_text("Entw&APw-rfe"), "Entw\u{fc}rfe");
        assert_eq!(folder_text("odd&zz"), "odd&zz");
    }
}
