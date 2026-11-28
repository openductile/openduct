// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A PDF's objects as its bytes write them: numbers, names, strings, arrays, dictionaries, references, and (in a
//! page's content) operators. Nesting is bounded, and bytes that are not an object end the reading of one.

/// The deepest arrays and dictionaries nest.
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Object {
    Null,
    Bool(bool),
    Number(f64),
    Name(Vec<u8>),
    String(Vec<u8>),
    Array(Vec<Object>),
    Dict(Vec<(Vec<u8>, Object)>),
    Ref(u32),
    /// An operator of a page's content (`Tj`, `BT`...), or a keyword (`obj`, `stream`).
    Word(Vec<u8>),
}

impl Object {
    pub(super) fn get(&self, key: &[u8]) -> Option<&Object> {
        match self {
            Self::Dict(fields) => fields.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub(super) fn number(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub(super) fn name(&self) -> Option<&[u8]> {
        match self {
            Self::Name(n) => Some(n),
            _ => None,
        }
    }
}

pub(super) fn white(b: u8) -> bool {
    matches!(b, 0 | b'\t' | b'\n' | 0x0c | b'\r' | b' ')
}

fn delimiter(b: u8) -> bool {
    matches!(
        b,
        b'(' | b')' | b'<' | b'>' | b'[' | b']' | b'{' | b'}' | b'/' | b'%'
    )
}

/// Reads objects from bytes, from a place.
#[derive(Debug)]
pub(super) struct Lexer<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) at: usize,
    /// Whether `1 0 R` is a reference (a file's objects) or three things (a page's content).
    refs: bool,
}

impl<'a> Lexer<'a> {
    pub(super) fn new(bytes: &'a [u8], at: usize, refs: bool) -> Self {
        Self { bytes, at, refs }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    pub(super) fn skip_space(&mut self) {
        while let Some(b) = self.peek() {
            if white(b) {
                self.at += 1;
            } else if b == b'%' {
                while self.peek().is_some_and(|b| b != b'\n' && b != b'\r') {
                    self.at += 1;
                }
            } else {
                break;
            }
        }
    }

    fn regular(&mut self) -> &'a [u8] {
        let start = self.at;
        while self.peek().is_some_and(|b| !white(b) && !delimiter(b)) {
            self.at += 1;
        }
        self.bytes.get(start..self.at).unwrap_or_default()
    }

    /// The next object; `None` at the end or at bytes that are not one.
    pub(super) fn object(&mut self) -> Option<Object> {
        self.object_at(0)
    }

    fn object_at(&mut self, depth: usize) -> Option<Object> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.skip_space();
        let b = self.peek()?;
        Some(match b {
            b'/' => {
                self.at += 1;
                Object::Name(unescape_name(self.regular()))
            }
            b'(' => {
                self.at += 1;
                Object::String(self.literal())
            }
            b'<' if self.bytes.get(self.at + 1) == Some(&b'<') => {
                self.at += 2;
                let mut fields = Vec::new();
                loop {
                    self.skip_space();
                    match self.peek()? {
                        b'>' => {
                            self.at += 2;
                            break;
                        }
                        b'/' => {
                            self.at += 1;
                            let key = unescape_name(self.regular());
                            let value = self.object_at(depth + 1)?;
                            fields.push((key, value));
                        }
                        _ => {
                            // A broken entry: passed over.
                            self.object_at(depth + 1)?;
                        }
                    }
                }
                Object::Dict(fields)
            }
            b'<' => {
                self.at += 1;
                Object::String(self.hex())
            }
            b'[' => {
                self.at += 1;
                let mut items = Vec::new();
                loop {
                    self.skip_space();
                    if self.peek()? == b']' {
                        self.at += 1;
                        break;
                    }
                    items.push(self.object_at(depth + 1)?);
                }
                Object::Array(items)
            }
            b')' | b'>' | b']' | b'{' | b'}' => {
                self.at += 1;
                Object::Word(vec![b])
            }
            _ => {
                let word = self.regular();
                if word.is_empty() {
                    self.at += 1;
                    return Some(Object::Word(vec![b]));
                }
                match word {
                    b"true" => Object::Bool(true),
                    b"false" => Object::Bool(false),
                    b"null" => Object::Null,
                    w => match number(w) {
                        Some(n) => self.maybe_ref(n),
                        None => Object::Word(w.to_vec()),
                    },
                }
            }
        })
    }

    /// `n` - or, followed by a generation and `R`, a reference to object `n`.
    fn maybe_ref(&mut self, n: f64) -> Object {
        if !self.refs || n.fract() != 0.0 || !(0.0..4e9).contains(&n) {
            return Object::Number(n);
        }
        let back = self.at;
        self.skip_space();
        let generation = self.regular();
        if !generation.is_empty() && generation.iter().all(u8::is_ascii_digit) {
            self.skip_space();
            if self.peek() == Some(b'R')
                && self
                    .bytes
                    .get(self.at + 1)
                    .is_none_or(|b| white(*b) || delimiter(*b))
            {
                self.at += 1;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                // checked to be a whole number
                return Object::Ref(n as u32);
            }
        }
        self.at = back;
        Object::Number(n)
    }

    fn literal(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        let mut depth = 1usize;
        while let Some(b) = self.peek() {
            self.at += 1;
            match b {
                b'(' => {
                    depth += 1;
                    out.push(b);
                }
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                    out.push(b);
                }
                b'\\' => {
                    let Some(e) = self.peek() else { break };
                    self.at += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'\r' => {
                            if self.peek() == Some(b'\n') {
                                self.at += 1;
                            }
                        }
                        b'\n' => {}
                        b'0'..=b'7' => {
                            let mut v = u32::from(e - b'0');
                            for _ in 0..2 {
                                match self.peek() {
                                    Some(d @ b'0'..=b'7') => {
                                        v = v * 8 + u32::from(d - b'0');
                                        self.at += 1;
                                    }
                                    _ => break,
                                }
                            }
                            out.push(u8::try_from(v & 0xff).unwrap_or(0));
                        }
                        other => out.push(other),
                    }
                }
                other => out.push(other),
            }
        }
        out
    }

    fn hex(&mut self) -> Vec<u8> {
        let mut digits = Vec::new();
        while let Some(b) = self.peek() {
            self.at += 1;
            if b == b'>' {
                break;
            }
            if let Some(d) = char::from(b).to_digit(16) {
                digits.push(u8::try_from(d).unwrap_or(0));
            }
        }
        if digits.len() % 2 == 1 {
            digits.push(0);
        }
        digits
            .chunks(2)
            .map(|p| (p.first().copied().unwrap_or(0) << 4) | p.get(1).copied().unwrap_or(0))
            .collect()
    }
}

fn number(word: &[u8]) -> Option<f64> {
    if word.is_empty()
        || !word
            .iter()
            .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.'))
    {
        return None;
    }
    if !word.iter().any(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(word).ok()?.parse().ok()
}

fn unescape_name(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while let Some(&b) = raw.get(i) {
        if b == b'#'
            && let Some(v) = raw
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn objects_read_as_written() {
        let text = b"<< /Type /Page /Kids [3 0 R 4 0 R] /N 12.5 /S (a\\(b\\) \\101\\n) /H <48656C6C6F> /X#20y true >>";
        let mut lexer = Lexer::new(text, 0, true);
        let dict = lexer.object().unwrap();
        assert_eq!(dict.get(b"Type"), Some(&Object::Name(b"Page".to_vec())));
        assert_eq!(
            dict.get(b"Kids"),
            Some(&Object::Array(vec![Object::Ref(3), Object::Ref(4)]))
        );
        assert_eq!(dict.get(b"N").and_then(Object::number), Some(12.5));
        assert_eq!(dict.get(b"S"), Some(&Object::String(b"a(b) A\n".to_vec())));
        assert_eq!(dict.get(b"H"), Some(&Object::String(b"Hello".to_vec())));
        assert_eq!(dict.get(b"X y"), Some(&Object::Bool(true)));
        let mut content = Lexer::new(b"1 0 0 1 72 700 Tm (Hi) Tj", 0, false);
        let mut seen = Vec::new();
        while let Some(o) = content.object() {
            seen.push(o);
        }
        assert_eq!(seen.len(), 9);
        assert_eq!(seen[6], Object::Word(b"Tm".to_vec()));
        let deep = "[".repeat(1000);
        assert!(Lexer::new(deep.as_bytes(), 0, true).object().is_none());
        assert!(Lexer::new(b"<< /A (never ends", 0, true).object().is_none());
    }
}
