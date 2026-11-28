// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A web page read as a person reads it, in OpenDuct's own code: its markup built into a tree as a browser builds it
//! ([`dom`]), the part a person reads found ([`read`]) and written as Markdown or text ([`write`]), what it says of
//! itself and its links and pictures ([`meta`]), and elements picked out by CSS selectors ([`select`]). Not a browser:
//! a page that builds itself with scripts gives what its HTML holds.

pub(crate) mod dom;
mod entities;
pub(crate) mod meta;
mod read;
pub(crate) mod select;
mod tokens;
mod write;

use reqwest::Url;

pub(crate) use dom::Page;
pub(crate) use meta::{Base, Meta};
pub(crate) use read::Part;
pub(crate) use select::{BUDGET, Budget, Selector};
pub(crate) use write::inline_text;

/// How a page is read.
#[derive(Debug, Clone)]
pub(crate) struct Options {
    pub(crate) part: Part,
    pub(crate) markdown: bool,
    pub(crate) links: bool,
    pub(crate) images: bool,
    /// The most characters of text given; the rest is cut off.
    pub(crate) max_chars: Option<usize>,
}

/// A page, read.
#[derive(Debug, Clone)]
pub(crate) struct Read {
    pub(crate) meta: Meta,
    pub(crate) text: String,
    /// Its text was cut at the most characters asked for, or its markup at the most nodes read.
    pub(crate) cut: bool,
    pub(crate) links: Vec<(String, String)>,
    pub(crate) images: Vec<(String, String)>,
}

/// `text` up to `most` characters, cut at a word's end when one is near; whether it was cut.
pub(crate) fn cut_text(text: &mut String, most: usize) -> bool {
    let Some((at, _)) = text.char_indices().nth(most) else {
        return false;
    };
    let near = text
        .get(..at)
        .and_then(|t| t.rfind(char::is_whitespace))
        .filter(|w| at - w < 200)
        .unwrap_or(at);
    text.truncate(near);
    let kept = text.trim_end().len();
    text.truncate(kept);
    true
}

/// `page`, at `url`, read as `options` say.
pub(crate) fn read(page: &Page, url: &Url, options: &Options, budget: &mut Budget) -> Result<Read, String> {
    let base = Base::of(page, url);
    let meta = meta::meta(page, &base);
    let content = read::content(page, &options.part, budget)?;
    let mut text = write::write(page, &content.parts, &content.hidden, &base, options.markdown);
    let mut cut = page.cut;
    if let Some(most) = options.max_chars {
        cut |= cut_text(&mut text, most);
    }
    let never = read::never(page);
    let hidden = |id: usize| never.get(id).copied().unwrap_or(true);
    Ok(Read {
        meta,
        text,
        cut,
        links: if options.links {
            meta::links(page, &base, &hidden)
        } else {
            Vec::new()
        },
        images: if options.images {
            meta::images(page, &base, &hidden)
        } else {
            Vec::new()
        },
    })
}

/// The links of `page` at `url` to other pages, and whether it asks that they not be followed: for a crawl.
pub(crate) fn links_of(page: &Page, url: &Url) -> (Vec<String>, bool) {
    let base = Base::of(page, url);
    let never = read::never(page);
    let links = meta::links(page, &base, &|id| never.get(id).copied().unwrap_or(true));
    (
        links.into_iter().map(|(u, _)| u).collect(),
        meta::meta(page, &base).nofollow,
    )
}

/// One element of `page` written as Markdown.
pub(crate) fn write_markdown(page: &Page, id: usize, hidden: &[bool], base: &Base) -> String {
    write::write(page, &[id], hidden, base, true)
}

/// A page's bytes as text, in its character set.
pub(crate) fn decode(bytes: &[u8], content_type: &str) -> String {
    meta::decode(bytes, content_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_cut_at_a_words_end() {
        let mut t = "one two three".to_owned();
        assert!(cut_text(&mut t, 9));
        assert_eq!(t, "one two");
        let mut t = "short".to_owned();
        assert!(!cut_text(&mut t, 5));
        let mut t = "ééééé".to_owned();
        assert!(cut_text(&mut t, 2));
        assert_eq!(t, "éé");
    }
}
