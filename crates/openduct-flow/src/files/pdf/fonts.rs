// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a PDF font's codes mean as text: its ToUnicode map when it has one, else its encoding - WinAnsi, MacRoman,
//! Standard, with its differences named by glyph. A composite font without a map gives no text, rather than wrong text.

use std::collections::HashMap;

use super::lex::{Lexer, Object};

/// The most codes a font's map holds.
const MAX_CODES: usize = 100_000;

/// A font's codes as text.
#[derive(Debug, Default, Clone)]
pub(super) struct Font {
    /// Codes are two bytes (a composite font).
    pub(super) wide: bool,
    map: HashMap<u32, String>,
    /// Codes that no map names, read through the encoding: `None` for a composite font.
    simple: Option<[char; 256]>,
}

impl Font {
    /// A font the page does not describe: its codes read as WinAnsi.
    pub(super) fn plain() -> Self {
        Self {
            simple: Some(win_ansi()),
            ..Self::default()
        }
    }

    /// The text of a string shown in this font.
    pub(super) fn text(&self, bytes: &[u8], out: &mut String) {
        if self.wide {
            for pair in bytes.chunks(2) {
                let code = (u32::from(pair.first().copied().unwrap_or(0)) << 8)
                    | u32::from(pair.get(1).copied().unwrap_or(0));
                if let Some(t) = self.map.get(&code) {
                    out.push_str(t);
                }
            }
            return;
        }
        for &b in bytes {
            match (self.map.get(&u32::from(b)), &self.simple) {
                (Some(t), _) => out.push_str(t),
                (None, Some(table)) => {
                    let c = table.get(usize::from(b)).copied().unwrap_or('\0');
                    if c != '\0' {
                        out.push(c);
                    }
                }
                (None, None) => {}
            }
        }
    }
}

/// The Windows code page most PDF fonts use: Latin-1, with its 0x80-0x9F filled in.
fn win_ansi() -> [char; 256] {
    const HIGH: [char; 32] = [
        '€', '\0', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '\0', 'Ž', '\0', '\0', '‘', '’',
        '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '\0', 'ž', 'Ÿ',
    ];
    let mut t = ['\0'; 256];
    for (i, c) in t.iter_mut().enumerate() {
        *c = match i {
            0x20..=0x7e | 0xa0..=0xff => char::from(u8::try_from(i).unwrap_or(0)),
            0x80..=0x9f => HIGH.get(i - 0x80).copied().unwrap_or('\0'),
            _ => '\0',
        };
    }
    t
}

fn mac_roman() -> [char; 256] {
    const HIGH: &str = "ÄÅÇÉÑÖÜáàâäãåçéèêëíìîïñóòôöõúùûü†°¢£§•¶ß®©™´¨≠ÆØ∞±≤≥¥µ∂∑∏π∫ªºΩæø¿¡¬√ƒ≈∆«»… ÀÃÕŒœ–—“”‘’÷◊ÿŸ⁄€‹›ﬁﬂ‡·‚„‰ÂÊÁËÈÍÎÏÌÓÔ\u{f8ff}ÒÚÛÙıˆ˜¯˘˙˚¸˝˛ˇ";
    let mut t = win_ansi();
    for (slot, c) in t.iter_mut().skip(0x80).zip(HIGH.chars()) {
        *slot = c;
    }
    t
}

/// A glyph's name as its character: the names fonts' differences use most, and `uniXXXX`.
fn glyph(name: &[u8]) -> Option<char> {
    let name = std::str::from_utf8(name).ok()?;
    if name.chars().count() == 1 {
        return name.chars().next().filter(char::is_ascii_alphabetic);
    }
    if let Some(c) = name
        .strip_prefix("uni")
        .or_else(|| name.strip_prefix('u'))
        .filter(|h| (4..=6).contains(&h.len()) && h.bytes().all(|b| b.is_ascii_hexdigit()))
        .and_then(|h| u32::from_str_radix(h, 16).ok())
        .and_then(char::from_u32)
    {
        return Some(c);
    }
    const NAMES: &[(&str, char)] = &[
        ("space", ' '),
        ("exclam", '!'),
        ("quotedbl", '"'),
        ("numbersign", '#'),
        ("dollar", '$'),
        ("percent", '%'),
        ("ampersand", '&'),
        ("quotesingle", '\''),
        ("parenleft", '('),
        ("parenright", ')'),
        ("asterisk", '*'),
        ("plus", '+'),
        ("comma", ','),
        ("hyphen", '-'),
        ("period", '.'),
        ("slash", '/'),
        ("zero", '0'),
        ("one", '1'),
        ("two", '2'),
        ("three", '3'),
        ("four", '4'),
        ("five", '5'),
        ("six", '6'),
        ("seven", '7'),
        ("eight", '8'),
        ("nine", '9'),
        ("colon", ':'),
        ("semicolon", ';'),
        ("less", '<'),
        ("equal", '='),
        ("greater", '>'),
        ("question", '?'),
        ("at", '@'),
        ("bracketleft", '['),
        ("backslash", '\\'),
        ("bracketright", ']'),
        ("asciicircum", '^'),
        ("underscore", '_'),
        ("grave", '`'),
        ("braceleft", '{'),
        ("bar", '|'),
        ("braceright", '}'),
        ("asciitilde", '~'),
        ("quoteleft", '‘'),
        ("quoteright", '’'),
        ("quotedblleft", '“'),
        ("quotedblright", '”'),
        ("endash", '–'),
        ("emdash", '—'),
        ("bullet", '•'),
        ("ellipsis", '…'),
        ("Euro", '€'),
        ("trademark", '™'),
        ("copyright", '©'),
        ("registered", '®'),
        ("degree", '°'),
        ("sterling", '£'),
        ("yen", '¥'),
        ("cent", '¢'),
        ("section", '§'),
        ("paragraph", '¶'),
        ("minus", '−'),
        ("multiply", '×'),
        ("divide", '÷'),
        ("fi", 'ﬁ'),
        ("fl", 'ﬂ'),
        ("germandbls", 'ß'),
        ("nbspace", ' '),
        ("periodcentered", '·'),
        ("guillemotleft", '«'),
        ("guillemotright", '»'),
        ("exclamdown", '¡'),
        ("questiondown", '¿'),
        ("ae", 'æ'),
        ("AE", 'Æ'),
        ("oslash", 'ø'),
        ("Oslash", 'Ø'),
        ("oe", 'œ'),
        ("OE", 'Œ'),
        ("dotlessi", 'ı'),
    ];
    if let Some((_, c)) = NAMES.iter().find(|(n, _)| *n == name) {
        return Some(*c);
    }
    // A letter and its accent, as Latin-1 has them: eacute, Udieresis, ntilde...
    const ACCENTED: &[(&str, &str, &str)] = &[
        ("grave", "AEIOUaeiou", "ÀÈÌÒÙàèìòù"),
        ("acute", "AEIOUYaeiouy", "ÁÉÍÓÚÝáéíóúý"),
        ("circumflex", "AEIOUaeiou", "ÂÊÎÔÛâêîôû"),
        ("dieresis", "AEIOUaeiouy", "ÄËÏÖÜäëïöüÿ"),
        ("tilde", "ANOano", "ÃÑÕãñõ"),
        ("cedilla", "Cc", "Çç"),
        ("ring", "Aa", "Åå"),
        ("caron", "SZsz", "ŠŽšž"),
    ];
    let mut chars = name.chars();
    let base = chars.next()?;
    let accent = chars.as_str();
    let (_, bases, marked) = ACCENTED.iter().find(|(a, _, _)| *a == accent)?;
    let at = bases.chars().position(|b| b == base)?;
    marked.chars().nth(at)
}

/// A UTF-16BE string's text.
fn utf16(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks(2)
        .map(|p| {
            (u16::from(p.first().copied().unwrap_or(0)) << 8) | u16::from(p.get(1).copied().unwrap_or(0))
        })
        .collect();
    String::from_utf16_lossy(&units)
}

fn code(bytes: &[u8]) -> u32 {
    bytes.iter().take(4).fold(0u32, |c, b| (c << 8) | u32::from(*b))
}

/// A ToUnicode map: its codes' text, and whether its codes are two bytes.
fn to_unicode(cmap: &[u8], font: &mut Font) {
    let mut lexer = Lexer::new(cmap, 0, false);
    let mut operands: Vec<Object> = Vec::new();
    let mut section: &[u8] = b"";
    while let Some(o) = lexer.object() {
        if font.map.len() > MAX_CODES {
            break;
        }
        let Object::Word(word) = &o else {
            operands.push(o);
            continue;
        };
        match word.as_slice() {
            b"begincodespacerange" | b"beginbfchar" | b"beginbfrange" => {
                section = match word.as_slice() {
                    b"begincodespacerange" => b"space",
                    b"beginbfchar" => b"char",
                    _ => b"range",
                };
                operands.clear();
            }
            b"endcodespacerange" => {
                if let Some(Object::String(low)) = operands.first() {
                    font.wide = low.len() >= 2;
                }
                operands.clear();
            }
            b"endbfchar" => {
                for pair in operands.chunks(2) {
                    if let [Object::String(from), Object::String(to)] = pair {
                        font.map.insert(code(from), utf16(to));
                    }
                }
                operands.clear();
            }
            b"endbfrange" => {
                for triple in operands.chunks(3) {
                    let [Object::String(low), Object::String(high), target] = triple else {
                        continue;
                    };
                    let (low, high) = (code(low), code(high));
                    if high < low || high - low > 65_535 {
                        continue;
                    }
                    for (i, c) in (low..=high).enumerate() {
                        let text = match target {
                            Object::String(start) => {
                                let mut units: Vec<u8> = start.clone();
                                // The last character counts up through the range.
                                let n = u32::try_from(i).unwrap_or(0);
                                if let (Some(last), Some(before)) = (units.pop(), units.pop()) {
                                    let v = ((u32::from(before) << 8) | u32::from(last)).saturating_add(n);
                                    units.push(u8::try_from((v >> 8) & 0xff).unwrap_or(0));
                                    units.push(u8::try_from(v & 0xff).unwrap_or(0));
                                }
                                utf16(&units)
                            }
                            Object::Array(list) => match list.get(i) {
                                Some(Object::String(s)) => utf16(s),
                                _ => continue,
                            },
                            _ => continue,
                        };
                        font.map.insert(c, text);
                        if font.map.len() > MAX_CODES {
                            break;
                        }
                    }
                }
                operands.clear();
            }
            _ => {
                if section.is_empty() {
                    operands.clear();
                }
            }
        }
    }
}

/// How a font's codes read as text, from its dictionary (its references followed, but for ToUnicode's); `stream`
/// reads a stream's bytes.
pub(super) fn font(dict: &Object, stream: &mut dyn FnMut(&Object) -> Option<Vec<u8>>) -> Font {
    let mut out = Font::default();
    let composite = dict.get(b"Subtype").and_then(Object::name) == Some(b"Type0");
    out.wide = composite;
    if !composite {
        let encoding = dict.get(b"Encoding").cloned().unwrap_or(Object::Null);
        let base = match encoding
            .name()
            .or_else(|| encoding.get(b"BaseEncoding").and_then(Object::name))
        {
            Some(b"MacRomanEncoding") => mac_roman(),
            _ => win_ansi(),
        };
        let mut table = base;
        if let Some(Object::Array(differences)) = encoding.get(b"Differences") {
            let mut at = 0usize;
            for item in differences {
                match item {
                    Object::Number(n) => {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // a code, 0 to 255
                        let c = n.clamp(0.0, 255.0) as usize;
                        at = c;
                    }
                    Object::Name(name) => {
                        if let (Some(slot), Some(c)) = (table.get_mut(at), glyph(name)) {
                            *slot = c;
                        }
                        at += 1;
                    }
                    _ => {}
                }
            }
        }
        out.simple = Some(table);
    }
    if let Some(cmap) = dict.get(b"ToUnicode").and_then(stream) {
        to_unicode(&cmap, &mut out);
        out.wide = composite || out.wide;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_font_reads_its_map_its_encoding_and_its_differences() {
        let cmap = b"/CIDInit /ProcSet findresource begin 1 begincodespacerange <0000> <FFFF> endcodespacerange \
                     2 beginbfchar <0003> <0020> <0011> <00480065> endbfchar 1 beginbfrange <0020> <0022> <0041> \
                     endbfrange 1 beginbfrange <0030> <0031> [<0078> <0079>] endbfrange end";
        let mut f = Font::default();
        to_unicode(cmap, &mut f);
        assert!(f.wide);
        let mut out = String::new();
        f.text(&[0, 0x11, 0, 3, 0, 0x20, 0, 0x22, 0, 0x31, 0, 0x99], &mut out);
        assert_eq!(out, "He ACy");

        let dict = Object::Dict(vec![
            (b"Subtype".to_vec(), Object::Name(b"TrueType".to_vec())),
            (
                b"Encoding".to_vec(),
                Object::Dict(vec![(
                    b"Differences".to_vec(),
                    Object::Array(vec![
                        Object::Number(65.0),
                        Object::Name(b"eacute".to_vec()),
                        Object::Name(b"uni263A".to_vec()),
                    ]),
                )]),
            ),
        ]);
        let simple = font(&dict, &mut |_| None);
        let mut out = String::new();
        simple.text(b"\x93AB z\x80", &mut out);
        assert_eq!(out, "“é☺ z€");
        assert_eq!(glyph(b"Udieresis"), Some('Ü'));
        assert_eq!(glyph(b"nonsense"), None);
    }
}
