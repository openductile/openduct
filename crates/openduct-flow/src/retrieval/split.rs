// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Cutting text into pieces a model can embed and a search can find: by characters (cut at the best place before the
//! size - a blank line, a line's end, a sentence's end, a space), by paragraphs or sentences (whole ones packed into
//! pieces up to the size), or by Markdown headings (no piece crosses a heading, and each knows the headings it is
//! under). Sizes are in characters. An overlap begins each piece with the end of the one before: by characters, the
//! last characters (from a word's start); otherwise the last whole paragraphs or sentences that fit in it.

use openduct_expr::{Env, Record, Value};
use rust_decimal::Decimal;

use super::{MAX_PIECES, MAX_TEXT, Split, SplitBy};

/// The most documents one split reads.
const MAX_DOCUMENTS: usize = 1_000;
/// The longest name of a document.
const MAX_SOURCE: usize = 1_000;

/// Where a piece is in its text, in characters, and the headings it is under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Piece {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) headings: Vec<String>,
}

type Span = (usize, usize);

fn at(chars: &[char], i: usize) -> char {
    chars.get(i).copied().unwrap_or('\0')
}

fn text_of(chars: &[char], (start, end): Span) -> String {
    chars
        .get(start..end)
        .map(|c| c.iter().collect())
        .unwrap_or_default()
}

/// A span without the white space at its ends.
fn trim(chars: &[char], (mut start, mut end): Span) -> Span {
    while start < end && at(chars, start).is_whitespace() {
        start += 1;
    }
    while end > start && at(chars, end - 1).is_whitespace() {
        end -= 1;
    }
    (start, end)
}

/// The lines between `from` and `to`, each without its line end.
fn lines(chars: &[char], (from, to): Span) -> Vec<Span> {
    let mut out = Vec::new();
    let mut start = from;
    for i in from..to {
        if at(chars, i) == '\n' {
            out.push((start, i));
            start = i + 1;
        }
    }
    out.push((start, to));
    out
}

fn blank(chars: &[char], (start, end): Span) -> bool {
    (start..end).all(|i| at(chars, i).is_whitespace())
}

/// The paragraphs of a span: lines between blank ones.
fn paragraphs(chars: &[char], span: Span) -> Vec<Span> {
    let mut out = Vec::new();
    let mut current: Option<Span> = None;
    for line in lines(chars, span) {
        if blank(chars, line) {
            out.extend(current.take());
        } else {
            current = Some(current.map_or(line, |(start, _)| (start, line.1)));
        }
    }
    out.extend(current);
    out.into_iter().map(|p| trim(chars, p)).collect()
}

fn ends_sentence(c: char) -> bool {
    matches!(c, '.' | '!' | '?' | '\u{3002}' | '\u{FF01}' | '\u{FF1F}')
}

/// The sentences of a span: each to its `.`, `!` or `?` (and any closing quote or bracket) before white space.
fn sentences(chars: &[char], span: Span) -> Vec<Span> {
    let mut out = Vec::new();
    let (mut start, mut i) = (span.0, span.0);
    while i < span.1 {
        let c = at(chars, i);
        if ends_sentence(c) {
            let mut end = i + 1;
            while end < span.1
                && (ends_sentence(at(chars, end))
                    || matches!(
                        at(chars, end),
                        '"' | '\'' | ')' | ']' | '\u{201D}' | '\u{2019}' | '\u{BB}'
                    ))
            {
                end += 1;
            }
            // Full-width marks end a sentence with no space after them.
            if end >= span.1 || at(chars, end).is_whitespace() || !c.is_ascii() {
                out.push(trim(chars, (start, end)));
                start = end;
                i = end;
                continue;
            }
        }
        i += 1;
    }
    out.push(trim(chars, (start, span.1)));
    out.retain(|(s, e)| s < e);
    out
}

/// A Markdown heading line (`## Title`, up to three spaces before it): its level and its title.
fn heading(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim_start_matches(' ');
    if line.len() - trimmed.len() > 3 {
        return None;
    }
    let level = trimmed.bytes().take_while(|b| *b == b'#').count();
    let rest = trimmed.get(level..)?;
    if !(1..=6).contains(&level) || !(rest.is_empty() || rest.starts_with([' ', '\t'])) {
        return None;
    }
    Some((level, rest.trim().trim_end_matches('#').trim().to_owned()))
}

/// The sections of a Markdown text, each from its heading to the next (code blocks left alone), with the headings
/// it is under.
fn sections(chars: &[char]) -> Vec<(Span, Vec<String>)> {
    let mut out = Vec::new();
    let mut path: Vec<(usize, String)> = Vec::new();
    let (mut start, mut fenced) = (0, false);
    let titles = |path: &[(usize, String)]| path.iter().map(|(_, t)| t.clone()).collect::<Vec<_>>();
    for line in lines(chars, (0, chars.len())) {
        let text = text_of(chars, line);
        let bare = text.trim_start();
        if bare.starts_with("```") || bare.starts_with("~~~") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            continue;
        }
        if let Some((level, title)) = heading(&text) {
            if !blank(chars, (start, line.0)) {
                out.push(((start, line.0), titles(&path)));
            }
            path.retain(|(l, _)| *l < level);
            path.push((level, title));
            start = line.0;
        }
    }
    if !blank(chars, (start, chars.len())) {
        out.push(((start, chars.len()), titles(&path)));
    }
    out
}

/// Where to end a piece that begins at `start` and may run to `hard`: the last blank line, line end, sentence end
/// or space in its second half, in that order of choice; else `hard`.
fn boundary(chars: &[char], start: usize, hard: usize) -> usize {
    let least = start + (hard - start) / 2 + 1;
    let rules: [&dyn Fn(usize) -> bool; 4] = [
        &|i| at(chars, i - 1) == '\n' && at(chars, i.saturating_sub(2)) == '\n',
        &|i| at(chars, i - 1) == '\n',
        &|i| {
            (at(chars, i - 1).is_whitespace() && ends_sentence(at(chars, i.saturating_sub(2))))
                || (ends_sentence(at(chars, i - 1)) && !at(chars, i - 1).is_ascii())
        },
        &|i| at(chars, i - 1).is_whitespace(),
    ];
    for rule in rules {
        if let Some(i) = (least..=hard).rev().find(|i| rule(*i)) {
            return i;
        }
    }
    hard
}

/// A span cut by characters into pieces of at most `size`, each after the first beginning `overlap` characters
/// before the one before ended (at a word's start).
fn cut(chars: &[char], span: Span, size: usize, overlap: usize) -> Vec<Span> {
    let mut out = Vec::new();
    let mut start = trim(chars, span).0;
    while start < span.1 {
        if span.1 - start <= size {
            out.push(trim(chars, (start, span.1)));
            break;
        }
        let end = boundary(chars, start, start + size);
        out.push(trim(chars, (start, end)));
        let mut next = end.saturating_sub(overlap).max(start + 1);
        if overlap > 0
            && !at(chars, next - 1).is_whitespace()
            && let Some(space) = (next..end).find(|i| at(chars, *i).is_whitespace())
        {
            next = space + 1;
        }
        start = trim(chars, (next, span.1)).0;
    }
    out.retain(|(s, e)| s < e);
    out
}

/// Whole units (paragraphs, sentences) packed into pieces of at most `size`; a unit longer than that cut by
/// characters. With an overlap, a piece begins with the last units of the one before that fit in it.
fn pack(chars: &[char], units: &[Span], size: usize, overlap: usize) -> Vec<Span> {
    let mut out = Vec::new();
    let mut current: Vec<Span> = Vec::new();
    for &unit in units {
        if unit.1 - unit.0 > size {
            if let (Some(first), Some(last)) = (current.first(), current.last()) {
                out.push((first.0, last.1));
            }
            current.clear();
            out.extend(cut(chars, unit, size, overlap));
            continue;
        }
        if let (Some(first), Some(last)) = (current.first().copied(), current.last().copied())
            && unit.1 - first.0 > size
        {
            out.push((first.0, last.1));
            let kept = current
                .iter()
                .rev()
                .take_while(|u| last.1 - u.0 <= overlap && unit.1 - u.0 <= size)
                .count();
            current.drain(..current.len() - kept);
        }
        current.push(unit);
    }
    if let (Some(first), Some(last)) = (current.first(), current.last()) {
        out.push((first.0, last.1));
    }
    out
}

/// The pieces of a text.
pub(crate) fn pieces(chars: &[char], by: SplitBy, size: usize, overlap: usize) -> Vec<Piece> {
    let whole = (0, chars.len());
    let plain = |spans: Vec<Span>| {
        spans
            .into_iter()
            .map(|(start, end)| Piece {
                start,
                end,
                headings: Vec::new(),
            })
            .collect()
    };
    match by {
        SplitBy::Characters => plain(cut(chars, whole, size, overlap)),
        SplitBy::Paragraphs => plain(pack(chars, &paragraphs(chars, whole), size, overlap)),
        SplitBy::Sentences => {
            let units: Vec<Span> = paragraphs(chars, whole)
                .into_iter()
                .flat_map(|p| sentences(chars, p))
                .collect();
            plain(pack(chars, &units, size, overlap))
        }
        SplitBy::Markdown => sections(chars)
            .into_iter()
            .flat_map(|(span, headings)| {
                pack(chars, &paragraphs(chars, span), size, overlap)
                    .into_iter()
                    .map(move |(start, end)| Piece {
                        start,
                        end,
                        headings: headings.clone(),
                    })
            })
            .collect(),
    }
}

/// One document to split: its text, its name, its metadata.
struct Document {
    text: String,
    source: String,
    metadata: Record,
}

fn record_of(value: Value, what: &str) -> Result<Record, String> {
    match value {
        Value::Null => Ok(Record::new()),
        Value::Record(r) => Ok(r.as_ref().clone()),
        other => Err(format!("{what}: a record of fields, not {}", other.describe())),
    }
}

/// The documents a step splits: its one text, or each of its list.
fn documents(s: &Split, env: &Env<'_>) -> Result<Vec<Document>, String> {
    let source = |env: &Env<'_>, default: String| -> Result<String, String> {
        let named = match &s.source {
            Some(t) => t.render(env).map_err(|e| format!("source: {e}"))?,
            None => String::new(),
        };
        let named = if named.trim().is_empty() { default } else { named };
        if named.len() > MAX_SOURCE {
            return Err(format!("source: a name of at most {MAX_SOURCE} characters"));
        }
        Ok(named)
    };
    let metadata = |env: &Env<'_>| -> Result<Record, String> {
        match &s.metadata {
            Some(t) => record_of(crate::run::tree(t, env)?, "metadata"),
            None => Ok(Record::new()),
        }
    };
    if let Some(text) = &s.text {
        return Ok(vec![Document {
            text: text.render(env).map_err(|e| format!("text: {e}"))?,
            source: source(env, "document".to_owned())?,
            metadata: metadata(env)?,
        }]);
    }
    let list = match s.documents.as_ref().map(|d| d.evaluate(env)).transpose()? {
        Some(Value::List(items)) => items,
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(other) => {
            return Err(format!("documents: a list, not {}", other.describe()));
        }
    };
    if list.len() > MAX_DOCUMENTS {
        return Err(format!(
            "documents: {} of them, at most {MAX_DOCUMENTS}",
            list.len()
        ));
    }
    let mut out = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        let mut names = env.names.to_vec();
        names.push(("item".to_owned(), item.clone()));
        names.push(("index".to_owned(), Value::number(Decimal::from(i))));
        let inner = Env {
            value: env.value,
            message: env.message,
            names: &names,
        };
        let at = format!("documents[{i}]");
        let (text, own_source, mut own) = match item {
            Value::Text(t) => (t.to_string(), None, Record::new()),
            Value::Record(r) => {
                let text = r
                    .get("text")
                    .or_else(|| r.get("content"))
                    .filter(|t| !t.is_null())
                    .ok_or_else(|| format!("{at}: a record with its text in text"))?;
                (
                    crate::template::as_text(text),
                    r.get("source").map(crate::template::as_text),
                    record_of(
                        r.get("metadata").cloned().unwrap_or(Value::Null),
                        &format!("{at}.metadata"),
                    )?,
                )
            }
            other => return Err(format!("{at}: a text or a record, not {}", other.describe())),
        };
        let default = own_source
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("document-{}", i + 1));
        own.extend(metadata(&inner)?);
        out.push(Document {
            text,
            source: source(&inner, default)?,
            metadata: own,
        });
    }
    Ok(out)
}

/// A `split_text` step's result: `{pieces, count, documents}`, each piece `{id, source, position, text, start, end,
/// metadata}`.
pub(crate) fn run(s: &Split, env: &Env<'_>) -> Result<Value, String> {
    let documents = documents(s, env)?;
    let mut total = 0;
    let mut out = Vec::new();
    for document in &documents {
        total += document.text.len();
        if total > MAX_TEXT {
            return Err(format!(
                "more than {} MiB of text: split fewer documents at a time",
                MAX_TEXT / 1024 / 1024
            ));
        }
        let text = if s.html {
            super::html::to_text(&document.text)
        } else {
            document.text.clone()
        };
        let chars: Vec<char> = text.chars().collect();
        for (position, piece) in pieces(&chars, s.by, s.size, s.overlap).into_iter().enumerate() {
            if out.len() >= MAX_PIECES {
                return Err(format!(
                    "more than {MAX_PIECES} pieces: a larger size, or fewer documents at a time"
                ));
            }
            let mut metadata = document.metadata.clone();
            if !piece.headings.is_empty() {
                let headings = piece.headings.iter().map(|h| Value::text(h.as_str())).collect();
                metadata.insert("headings".to_owned(), Value::list(headings));
            }
            let number = |n: usize| Value::number(Decimal::from(n));
            let mut fields = Record::new();
            fields.insert(
                "id".to_owned(),
                Value::text(format!("{}#{position}", document.source)),
            );
            fields.insert("source".to_owned(), Value::text(document.source.as_str()));
            fields.insert("position".to_owned(), number(position));
            fields.insert(
                "text".to_owned(),
                Value::text(text_of(&chars, (piece.start, piece.end))),
            );
            fields.insert("start".to_owned(), number(piece.start));
            fields.insert("end".to_owned(), number(piece.end));
            fields.insert("metadata".to_owned(), Value::record(metadata));
            out.push(Value::record(fields));
        }
    }
    let mut result = Record::new();
    result.insert("count".to_owned(), Value::number(Decimal::from(out.len())));
    result.insert(
        "documents".to_owned(),
        Value::number(Decimal::from(documents.len())),
    );
    result.insert("pieces".to_owned(), Value::list(out));
    Ok(Value::record(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(text: &str, by: SplitBy, size: usize, overlap: usize) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        pieces(&chars, by, size, overlap)
            .into_iter()
            .map(|p| text_of(&chars, (p.start, p.end)))
            .collect()
    }

    #[test]
    fn characters_are_cut_at_the_best_place_before_the_size() {
        let text = "Hello there my friend. How are you today? Fine.";
        assert_eq!(
            split(text, SplitBy::Characters, 30, 0),
            ["Hello there my friend.", "How are you today? Fine."]
        );
        // Without a sentence's end in its second half, a piece ends at a space.
        let words = "one two three four five six seven eight nine ten eleven twelve";
        let got = split(words, SplitBy::Characters, 20, 0);
        assert!(got.iter().all(|p| p.chars().count() <= 20), "{got:?}");
        assert_eq!(got.join(" "), words, "no word cut or lost");
        // A word longer than a piece is cut where it must be.
        assert_eq!(
            split(&"x".repeat(25), SplitBy::Characters, 10, 0),
            ["x".repeat(10), "x".repeat(10), "x".repeat(5)]
        );
    }

    #[test]
    fn an_overlap_begins_each_piece_with_the_last_words_of_the_one_before() {
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu";
        let got = split(text, SplitBy::Characters, 30, 12);
        assert!(got.len() >= 3, "{got:?}");
        for pair in got.windows(2) {
            let last_word = pair[0].split(' ').next_back().unwrap();
            assert!(pair[1].contains(last_word), "{pair:?}");
            assert!(!pair[1].starts_with(|c: char| c.is_whitespace()));
        }
        let joined: Vec<&str> = got.iter().flat_map(|p| p.split(' ')).collect();
        for word in text.split(' ') {
            assert!(joined.contains(&word), "{word} lost: {got:?}");
        }
    }

    #[test]
    fn paragraphs_are_kept_whole_and_packed_up_to_the_size() {
        let text = "First paragraph.\n\nSecond one here.\n\n\nThird, a little longer than the rest.";
        assert_eq!(
            split(text, SplitBy::Paragraphs, 40, 0),
            [
                "First paragraph.\n\nSecond one here.",
                "Third, a little longer than the rest."
            ]
        );
        // With an overlap, the last paragraph that fits begins the next piece.
        assert_eq!(
            split(text, SplitBy::Paragraphs, 60, 20),
            [
                "First paragraph.\n\nSecond one here.",
                "Second one here.\n\n\nThird, a little longer than the rest."
            ]
        );
        // One longer than a piece is cut by characters.
        let long = format!("Short.\n\n{}", "word ".repeat(30));
        let got = split(&long, SplitBy::Paragraphs, 40, 0);
        assert_eq!(got[0], "Short.");
        assert!(got[1..].iter().all(|p| p.chars().count() <= 40), "{got:?}");
    }

    #[test]
    fn sentences_end_at_their_marks_not_inside_numbers() {
        let chars: Vec<char> = "Pi is 3.14 or so. Is it? \"Yes!\" she said. 終わり。次"
            .chars()
            .collect();
        let got: Vec<String> = sentences(&chars, (0, chars.len()))
            .into_iter()
            .map(|s| text_of(&chars, s))
            .collect();
        assert_eq!(
            got,
            [
                "Pi is 3.14 or so.",
                "Is it?",
                "\"Yes!\"",
                "she said.",
                "終わり。",
                "次"
            ]
        );
        let packed = split("One. Two. Three. Four.", SplitBy::Sentences, 10, 0);
        assert_eq!(packed, ["One. Two.", "Three.", "Four."]);
    }

    #[test]
    fn markdown_pieces_never_cross_a_heading_and_know_the_headings_they_are_under() {
        let text = "Intro text.\n\n# Install\n\nGet it.\n\n## On Linux\n\nUse the package.\n\n```\n# not a heading\n```\n\n# Use\nRun it.";
        let chars: Vec<char> = text.chars().collect();
        let got: Vec<(String, Vec<String>)> = pieces(&chars, SplitBy::Markdown, 500, 0)
            .into_iter()
            .map(|p| (text_of(&chars, (p.start, p.end)), p.headings))
            .collect();
        assert_eq!(got.len(), 4, "{got:?}");
        assert_eq!(got[0], ("Intro text.".to_owned(), vec![]));
        assert_eq!(
            got[1],
            ("# Install\n\nGet it.".to_owned(), vec!["Install".to_owned()])
        );
        assert_eq!(got[2].1, ["Install", "On Linux"]);
        assert!(
            got[2].0.contains("# not a heading"),
            "a code block is not a heading: {got:?}"
        );
        assert_eq!(got[3], ("# Use\nRun it.".to_owned(), vec!["Use".to_owned()]));
        assert_eq!(heading("#hashtag"), None);
        assert_eq!(heading("    # indented code"), None);
        assert_eq!(heading("### Three ###"), Some((3, "Three".to_owned())));
    }

    #[test]
    fn nothing_but_white_space_makes_no_piece() {
        for by in [
            SplitBy::Characters,
            SplitBy::Paragraphs,
            SplitBy::Sentences,
            SplitBy::Markdown,
        ] {
            assert!(split(" \n\n \t ", by, 100, 10).is_empty(), "{by:?}");
            assert!(split("", by, 100, 10).is_empty(), "{by:?}");
        }
    }

    #[test]
    fn every_way_of_cutting_ends_on_random_text_within_its_size() {
        // A small generator, so the test needs nothing more: every piece within the size, and pieces always move on.
        let mut seed = 7u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = ['a', 'b', ' ', '\n', '.', '#', '!', 'é', '\t', '?'];
        for _ in 0..200 {
            let len = usize::try_from(next() % 400).unwrap();
            let text: String = (0..len)
                .map(|_| alphabet[usize::try_from(next() % 10).unwrap()])
                .collect();
            let size = 20 + usize::try_from(next() % 60).unwrap();
            let overlap = usize::try_from(next()).unwrap() % (size / 2 + 1);
            for by in [
                SplitBy::Characters,
                SplitBy::Paragraphs,
                SplitBy::Sentences,
                SplitBy::Markdown,
            ] {
                for piece in split(&text, by, size, overlap) {
                    assert!(piece.chars().count() <= size, "{by:?} {size}: {piece:?}");
                    assert!(!piece.trim().is_empty());
                }
            }
        }
    }
}
