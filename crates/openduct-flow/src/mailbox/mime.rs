// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A message as mail is written (RFC 5322), its parts as MIME says (RFC 2045 to 2047, 2231): its headers, unfolded
//! and with encoded words read (`=?UTF-8?B?...?=`); its text and its HTML; and its attachments, each decoded from
//! Base64 or quoted-printable. A message from outside is hostile: parts go at most [`MAX_DEPTH`] deep and are at most
//! [`MAX_PARTS`], text in a character set this does not know is read as UTF-8, and nothing in it is ever run.

use base64::Engine as _;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};

/// How deep parts may go in parts, and how many a message may have.
const MAX_DEPTH: usize = 10;
const MAX_PARTS: usize = 200;
/// The most header lines read, and the longest value kept.
const MAX_HEADERS: usize = 1_000;
const MAX_HEADER: usize = 64 * 1024;

/// A message, read.
#[derive(Debug, Default)]
pub(crate) struct Message {
    /// Its headers, names in small letters, values with encoded words read, in order.
    pub headers: Vec<(String, String)>,
    pub text: Option<String>,
    pub html: Option<String>,
    pub attachments: Vec<Attachment>,
}

/// A part that is a file: an attachment, or a picture the HTML shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attachment {
    pub name: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
    /// Shown in the message (an inline picture) rather than attached.
    pub inline: bool,
    pub content_id: Option<String>,
}

impl Message {
    /// The first header of this name (in small letters).
    #[cfg(test)]
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// The headers and the body of a part.
fn split(entity: &[u8]) -> (&[u8], &[u8]) {
    let find = |needle: &[u8]| memchr::memmem::find(entity, needle);
    match (find(b"\r\n\r\n"), find(b"\n\n")) {
        (Some(a), Some(b)) if b < a => (
            entity.get(..b).unwrap_or_default(),
            entity.get(b + 2..).unwrap_or_default(),
        ),
        (Some(a), _) => (
            entity.get(..a).unwrap_or_default(),
            entity.get(a + 4..).unwrap_or_default(),
        ),
        (None, Some(b)) => (
            entity.get(..b).unwrap_or_default(),
            entity.get(b + 2..).unwrap_or_default(),
        ),
        (None, None) => (entity, &[]),
    }
}

/// Header lines, unfolded: names in small letters, values as they came (bytes outside ASCII read as UTF-8).
fn header_lines(block: &[u8]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in block.split(|b| *b == b'\n').take(MAX_HEADERS * 4) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let text = String::from_utf8_lossy(line);
        if text.starts_with([' ', '\t']) {
            if let Some((_, value)) = out.last_mut()
                && value.len() < MAX_HEADER
            {
                value.push_str(&text);
            }
            continue;
        }
        if out.len() >= MAX_HEADERS {
            break;
        }
        if let Some((name, value)) = text.split_once(':') {
            let name = name.trim();
            if !name.is_empty() && name.bytes().all(|b| b.is_ascii_graphic()) {
                out.push((name.to_ascii_lowercase(), value.trim_start().to_owned()));
            }
        }
    }
    for (_, v) in &mut out {
        if v.len() > MAX_HEADER {
            let mut end = MAX_HEADER;
            while !v.is_char_boundary(end) {
                end -= 1;
            }
            v.truncate(end);
        }
        *v = v.trim_end().to_owned();
    }
    out
}

/// Bytes in a character set, as text: UTF-8 (and ASCII), ISO-8859-1 and Windows-1252; any other is read as UTF-8.
pub(crate) fn decode_charset(bytes: &[u8], charset: &str) -> String {
    const CP1252: [char; 32] = [
        '\u{20ac}', '\u{81}', '\u{201a}', '\u{192}', '\u{201e}', '\u{2026}', '\u{2020}', '\u{2021}',
        '\u{2c6}', '\u{2030}', '\u{160}', '\u{2039}', '\u{152}', '\u{8d}', '\u{17d}', '\u{8f}', '\u{90}',
        '\u{2018}', '\u{2019}', '\u{201c}', '\u{201d}', '\u{2022}', '\u{2013}', '\u{2014}', '\u{2dc}',
        '\u{2122}', '\u{161}', '\u{203a}', '\u{153}', '\u{9d}', '\u{17e}', '\u{178}',
    ];
    match charset.trim().trim_matches('"').to_ascii_lowercase().as_str() {
        "iso-8859-1" | "latin1" | "latin-1" | "iso8859-1" | "l1" => {
            bytes.iter().map(|b| char::from(*b)).collect()
        }
        "windows-1252" | "cp1252" | "x-cp1252" => bytes
            .iter()
            .map(|b| match b {
                0x80..=0x9f => CP1252.get(usize::from(b - 0x80)).copied().unwrap_or('\u{fffd}'),
                _ => char::from(*b),
            })
            .collect(),
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn lenient() -> GeneralPurpose {
    GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true),
    )
}

/// Base64 as mail carries it: lines, and spaces, left out.
pub(crate) fn base64_bytes(text: &[u8]) -> Vec<u8> {
    let clean: Vec<u8> = text
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    lenient().decode(&clean).unwrap_or_else(|_| {
        // A part cut short: what decodes of it.
        let whole = clean.len() / 4 * 4;
        lenient()
            .decode(clean.get(..whole).unwrap_or_default())
            .unwrap_or_default()
    })
}

/// Quoted-printable (RFC 2045 6.7): `=41` a byte, `=` at a line's end no break; `underscores` for encoded words.
fn quoted_printable(text: &[u8], underscores: bool) -> Vec<u8> {
    let hex = |b: u8| char::from(b).to_digit(16);
    let mut out = Vec::with_capacity(text.len());
    let mut i = 0;
    while let Some(&b) = text.get(i) {
        match b {
            b'=' => match (text.get(i + 1).copied(), text.get(i + 2).copied()) {
                (Some(b'\r'), Some(b'\n')) => i += 3,
                (Some(b'\n'), _) => i += 2,
                (Some(h), Some(l)) if hex(h).is_some() && hex(l).is_some() => {
                    let v = hex(h).unwrap_or(0) * 16 + hex(l).unwrap_or(0);
                    out.push(u8::try_from(v).unwrap_or(b'?'));
                    i += 3;
                }
                _ => {
                    out.push(b);
                    i += 1;
                }
            },
            b'_' if underscores => {
                out.push(b' ');
                i += 1;
            }
            _ => {
                out.push(b);
                i += 1;
            }
        }
    }
    out
}

/// A header's value with its encoded words (RFC 2047) read: `=?UTF-8?B?SGk=?= there` is `Hi there`.
pub(crate) fn decode_words(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    let mut last_was_word = false;
    while let Some(start) = rest.find("=?") {
        let before = rest.get(..start).unwrap_or_default();
        let after = rest.get(start + 2..).unwrap_or_default();
        let word = (|| {
            let (charset, after) = after.split_once('?')?;
            let (encoding, after) = after.split_once('?')?;
            let end = after.find("?=")?;
            let text = after.get(..end)?;
            let bytes = match encoding.to_ascii_lowercase().as_str() {
                "b" => base64_bytes(text.as_bytes()),
                "q" => quoted_printable(text.as_bytes(), true),
                _ => return None,
            };
            let charset = charset.split('*').next().unwrap_or(charset);
            Some((decode_charset(&bytes, charset), after.get(end + 2..)?))
        })();
        match word {
            Some((decoded, remaining)) => {
                if !(last_was_word && before.trim().is_empty()) {
                    out.push_str(before);
                }
                out.push_str(&decoded);
                rest = remaining;
                last_was_word = true;
            }
            None => {
                out.push_str(rest.get(..start + 2).unwrap_or_default());
                rest = after;
                last_was_word = false;
            }
        }
    }
    out.push_str(rest);
    out
}

/// A header with parameters (`text/plain; charset="utf-8"`, `attachment; filename*=UTF-8''r%C3%A9.pdf`): its value
/// in small letters, and its parameters by name, RFC 2231's continuations and character sets read.
fn with_params(value: &str) -> (String, Vec<(String, String)>) {
    let mut parts = Vec::new();
    let (mut current, mut quoted) = (String::new(), false);
    for c in value.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                current.push(c);
            }
            ';' if !quoted => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    parts.push(current);
    let mut iter = parts.into_iter();
    let main = iter.next().unwrap_or_default().trim().to_ascii_lowercase();
    // name -> (sections in order, whether encoded)
    let mut raw: Vec<(String, u32, bool, String)> = Vec::new();
    for p in iter.take(50) {
        let Some((name, v)) = p.split_once('=') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let v = v.trim();
        let v = v
            .strip_prefix('"')
            .and_then(|x| x.strip_suffix('"'))
            .unwrap_or(v)
            .replace("\\\"", "\"");
        let (base, encoded) = match name.strip_suffix('*') {
            Some(b) => (b.to_owned(), true),
            None => (name, false),
        };
        let (base, section) = match base.rsplit_once('*') {
            Some((b, n)) if n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty() => {
                (b.to_owned(), n.parse().unwrap_or(0))
            }
            _ => (base, 0),
        };
        raw.push((base, section, encoded, v));
    }
    raw.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut out: Vec<(String, String)> = Vec::new();
    let mut charset = String::new();
    for (name, section, encoded, v) in raw {
        let mut text = v;
        if encoded {
            if section == 0
                && let Some((cs, rest)) = text.split_once('\'')
                && let Some((_, value)) = rest.split_once('\'')
            {
                charset = cs.to_owned();
                text = value.to_owned();
            }
            text = decode_charset(&percent(&text), &charset);
        }
        match out.last_mut() {
            Some((n, existing)) if *n == name && section > 0 => existing.push_str(&text),
            _ => out.push((name, decode_words(&text))),
        }
    }
    (main, out)
}

fn percent(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b, hex) {
            (b'%', Some(v)) => {
                out.push(v);
                i += 3;
            }
            _ => {
                out.push(b);
                i += 1;
            }
        }
    }
    out
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
}

/// A file name a part may keep: no folders, no control characters, at most 200 characters.
fn clean_name(name: &str) -> String {
    let last = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let clean: String = last.chars().filter(|c| !c.is_control()).take(200).collect();
    let clean = clean.trim().trim_start_matches('.').to_owned();
    if clean.is_empty() {
        "attachment".to_owned()
    } else {
        clean
    }
}

/// The parts of a multipart body, between its boundaries.
fn parts<'a>(body: &'a [u8], boundary: &str) -> Vec<&'a [u8]> {
    let delimiter = format!("--{boundary}");
    let finder = memchr::memmem::Finder::new(delimiter.as_bytes());
    let mut starts: Vec<(usize, usize)> = Vec::new();
    for at in finder.find_iter(body) {
        let line_start = at == 0 || body.get(at - 1) == Some(&b'\n');
        if !line_start {
            continue;
        }
        let after = at + delimiter.len();
        let closing = body.get(after..after + 2) == Some(b"--");
        starts.push((at, after));
        if closing || starts.len() > MAX_PARTS {
            break;
        }
    }
    let mut out = Vec::new();
    for pair in starts.windows(2) {
        let (Some(&(_, after)), Some(&(next, _))) = (pair.first(), pair.get(1)) else {
            continue;
        };
        let begin = body
            .get(after..)
            .and_then(|b| b.iter().position(|x| *x == b'\n'))
            .map_or(after, |n| after + n + 1);
        let mut end = next;
        if end > begin && body.get(end - 1) == Some(&b'\n') {
            end -= 1;
            if end > begin && body.get(end - 1) == Some(&b'\r') {
                end -= 1;
            }
        }
        if let Some(part) = body.get(begin..end.max(begin)) {
            out.push(part);
        }
    }
    out
}

fn extension(media_type: &str) -> &'static str {
    match media_type {
        "application/pdf" => "pdf",
        "text/csv" => "csv",
        "text/plain" => "txt",
        "text/html" => "html",
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "application/json" => "json",
        "message/rfc822" => "eml",
        _ => "bin",
    }
}

/// Reads a whole message.
pub(crate) fn parse(raw: &[u8]) -> Message {
    let mut message = Message::default();
    let (head, _) = split(raw);
    message.headers = header_lines(head)
        .into_iter()
        .map(|(n, v)| {
            let decoded = decode_words(&v);
            (n, decoded)
        })
        .collect();
    let mut count = 0;
    walk(raw, 0, &mut message, &mut count);
    message
}

/// The headers of a message (or of the header section alone), read.
pub(crate) fn headers(raw: &[u8]) -> Vec<(String, String)> {
    let (head, _) = split(raw);
    header_lines(head)
        .into_iter()
        .map(|(n, v)| (n, decode_words(&v)))
        .collect()
}

fn walk(entity: &[u8], depth: usize, out: &mut Message, count: &mut usize) {
    *count += 1;
    if *count > MAX_PARTS {
        return;
    }
    let (head, body) = split(entity);
    let fields = header_lines(head);
    let get = |name: &str| fields.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());
    let (media_type, params) = with_params(get("content-type").unwrap_or("text/plain"));
    let media_type = if media_type.contains('/') {
        media_type
    } else {
        "text/plain".to_owned()
    };
    if media_type.starts_with("multipart/") {
        if let Some(boundary) = param(&params, "boundary").filter(|b| !b.is_empty() && b.len() <= 200)
            && depth < MAX_DEPTH
        {
            for part in parts(body, boundary) {
                walk(part, depth + 1, out, count);
            }
        }
        return;
    }
    let (disposition, dparams) = with_params(get("content-disposition").unwrap_or(""));
    let filename = param(&dparams, "filename")
        .or_else(|| param(&params, "name"))
        .map(clean_name);
    let encoding = get("content-transfer-encoding")
        .unwrap_or("7bit")
        .trim()
        .to_ascii_lowercase();
    let decoded = match encoding.as_str() {
        "base64" => base64_bytes(body),
        "quoted-printable" => quoted_printable(body, false),
        _ => body.to_vec(),
    };
    let attached = disposition == "attachment";
    let charset = param(&params, "charset").unwrap_or("utf-8");
    if !attached && filename.is_none() && media_type == "text/plain" && out.text.is_none() {
        out.text = Some(decode_charset(&decoded, charset));
        return;
    }
    if !attached && filename.is_none() && media_type == "text/html" && out.html.is_none() {
        out.html = Some(decode_charset(&decoded, charset));
        return;
    }
    let content_id =
        get("content-id").map(|c| c.trim().trim_start_matches('<').trim_end_matches('>').to_owned());
    let name =
        filename.unwrap_or_else(|| format!("part-{}.{}", out.attachments.len() + 1, extension(&media_type)));
    out.attachments.push(Attachment {
        name,
        media_type: media_type.clone(),
        bytes: decoded,
        inline: disposition == "inline" || (!attached && content_id.is_some()),
        content_id,
    });
}

/// The addresses in a header (`Ann <ann@example.com>, "Lee, B" <b@example.com>`): each one's name and address.
pub(crate) fn addresses(value: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let (mut current, mut quoted, mut angled) = (String::new(), false, false);
    let mut items = Vec::new();
    for c in value.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                current.push(c);
            }
            '<' if !quoted => {
                angled = true;
                current.push(c);
            }
            '>' if !quoted => {
                angled = false;
                current.push(c);
            }
            ',' | ';' if !quoted && !angled => items.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    items.push(current);
    for item in items.into_iter().take(500) {
        // A group's name (`Team: a@x, b@y;`) is not an address.
        let item = match item.split_once(':') {
            Some((_, rest)) if !item.contains('<') || item.find(':') < item.find('<') => rest.to_owned(),
            _ => item,
        };
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (name, address) = match (item.rfind('<'), item.rfind('>')) {
            (Some(open), Some(close)) if close > open => (
                item.get(..open)
                    .unwrap_or_default()
                    .trim()
                    .trim_matches('"')
                    .replace("\\\"", "\""),
                item.get(open + 1..close).unwrap_or_default().trim().to_owned(),
            ),
            _ => (String::new(), item.trim_matches('"').to_owned()),
        };
        if address.contains('@') {
            out.push((decode_words(&name).trim().to_owned(), address));
        }
    }
    out
}

/// A message's Date header as RFC 3339 in UTC, when it can be read.
pub(crate) fn date(value: &str) -> Option<String> {
    let without_comment = match value.find('(') {
        Some(at) => value.get(..at).unwrap_or(value),
        None => value,
    };
    chrono::DateTime::parse_from_rfc2822(without_comment.trim())
        .ok()
        .map(|t| {
            t.with_timezone(&chrono::Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    const MESSAGE: &str = "From: =?UTF-8?Q?Ren=C3=A9e?= <renee@example.com>\r\n\
To: \"Shop, Accounts\" <accounts@example.com>, bob@example.com\r\n\
Subject: =?UTF-8?B?SW52b2ljZQ==?= =?UTF-8?B?IE5vdmVtYmVy?=\r\n\x202026\r\n\
Date: Mon, 23 Nov 2026 10:00:00 +0100 (CET)\r\n\
MIME-Version: 1.0\r\n\
Content-Type: multipart/mixed; boundary=\"outer\"\r\n\
\r\n\
preamble\r\n\
--outer\r\n\
Content-Type: multipart/alternative; boundary=inner\r\n\
\r\n\
--inner\r\n\
Content-Type: text/plain; charset=iso-8859-1\r\n\
Content-Transfer-Encoding: quoted-printable\r\n\
\r\n\
Total: 12 =E9t=\r\n\
e\r\n\
--inner\r\n\
Content-Type: text/html; charset=utf-8\r\n\
\r\n\
<p>Total: 12</p>\r\n\
--inner--\r\n\
--outer\r\n\
Content-Type: application/pdf; name=\"ignored.pdf\"\r\n\
Content-Disposition: attachment; filename*=UTF-8''r%C3%A9cu%20N%C2%B01.pdf\r\n\
Content-Transfer-Encoding: base64\r\n\
\r\n\
JVBERi0x\r\n\
LjQK\r\n\
--outer--\r\n\
epilogue\r\n";

    #[test]
    fn a_message_reads_into_its_text_html_and_files() {
        let m = parse(MESSAGE.as_bytes());
        assert_eq!(m.header("subject"), Some("Invoice November 2026"));
        assert_eq!(m.header("from"), Some("Renée <renee@example.com>"));
        assert_eq!(m.text.as_deref(), Some("Total: 12 \u{e9}te"));
        assert_eq!(m.html.as_deref(), Some("<p>Total: 12</p>"));
        assert_eq!(m.attachments.len(), 1);
        let a = &m.attachments[0];
        assert_eq!(a.name, "r\u{e9}cu N\u{b0}1.pdf");
        assert_eq!(a.media_type, "application/pdf");
        assert_eq!(a.bytes, b"%PDF-1.4\n");
        assert!(!a.inline);
        assert_eq!(
            date(m.header("date").unwrap()).as_deref(),
            Some("2026-11-23T09:00:00Z")
        );
        let to = addresses(m.header("to").unwrap());
        assert_eq!(
            to,
            [
                ("Shop, Accounts".to_owned(), "accounts@example.com".to_owned()),
                (String::new(), "bob@example.com".to_owned())
            ]
        );
    }

    #[test]
    fn hostile_messages_are_bounded() {
        // Parts in parts far deeper than allowed, and a part with no end.
        let mut deep = String::new();
        for i in 0..50 {
            deep.push_str(&format!(
                "Content-Type: multipart/mixed; boundary=b{i}\r\n\r\n--b{i}\r\n"
            ));
        }
        deep.push_str("Content-Type: text/plain\r\n\r\nbottom\r\n");
        let m = parse(deep.as_bytes());
        assert!(m.text.is_none(), "too deep to be read");
        let many: String = (0..1000)
            .map(|_| "--x\r\nContent-Type: image/png\r\n\r\nAA\r\n")
            .collect();
        let m = parse(format!("Content-Type: multipart/mixed; boundary=x\r\n\r\n{many}").as_bytes());
        assert!(m.attachments.len() <= MAX_PARTS);
        assert_eq!(clean_name("../../etc/passwd"), "passwd");
        assert_eq!(clean_name("..."), "attachment");
        assert_eq!(decode_words("=?x?Z?bad?= plain"), "=?x?Z?bad?= plain");
        assert_eq!(
            decode_charset(b"\x93hi\x94", "windows-1252"),
            "\u{201c}hi\u{201d}"
        );
        assert!(base64_bytes(b"not base64 !!").len() < 16);
    }
}
