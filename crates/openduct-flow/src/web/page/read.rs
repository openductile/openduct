// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The part of a page a person reads: what is never shown is left out wherever it is (scripts, styles, templates,
//! frames, forms' fields, what is marked hidden), and for the main part (the default) also what surrounds the content
//! on most sites - navigation, menus, sidebars, banners, footers, cookie notices, share bars, comments - by the
//! elements and roles HTML gives them and the names sites give them. The main part is the page's one `<article>`, or
//! its `<main>`, or, for a page that marks neither, the block whose paragraphs hold the most text (scored as reading
//! tools score them: by length and commas, less what is links), with the blocks beside it that read as part of it.
//! Inside it, clusters of links (a list of related posts, tags) are left out too. A page whose main part cannot be
//! told gives all it shows.

use super::dom::Page;
use super::select::{Budget, Selector};

/// Which part of a page is read.
#[derive(Debug, Clone)]
pub(crate) enum Part {
    /// The main content (the default).
    Main,
    /// All a person sees, navigation and footers too.
    All,
    /// What these elements hold.
    Within(Selector),
}

/// What is written: the elements, in order, and what is left out inside them.
#[derive(Debug)]
pub(crate) struct Content {
    pub(crate) parts: Vec<usize>,
    pub(crate) hidden: Vec<bool>,
}

/// Elements never shown as text.
const NEVER_SHOWN: &[&str] = &[
    "script", "style", "noscript", "template", "svg", "canvas", "iframe", "object", "embed", "video",
    "audio", "map", "head", "select", "option", "button", "input", "textarea", "datalist", "frameset",
    "frame", "title", "meta", "link", "base",
];
/// Elements around a page's content, not part of it.
const AROUND: &[&str] = &["nav", "aside", "menu", "footer", "dialog"];
/// Roles of what is around a page's content.
const AROUND_ROLES: &[&str] = &[
    "navigation",
    "banner",
    "contentinfo",
    "complementary",
    "search",
    "menu",
    "menubar",
    "toolbar",
    "dialog",
    "alertdialog",
    "tooltip",
];
/// Words in the names (class, id) sites give what is around their content.
const UNLIKELY: &[&str] = &[
    "advert",
    "agegate",
    "banner",
    "breadcrumb",
    "comment",
    "consent",
    "cookie",
    "disqus",
    "footer",
    "gdpr",
    "masthead",
    "menu",
    "modal",
    "navbar",
    "newsletter",
    "pager",
    "pagination",
    "popup",
    "promo",
    "related",
    "share",
    "sharing",
    "sidebar",
    "skip-link",
    "social",
    "sponsor",
    "subscribe",
    "toolbar",
    "widget",
];
/// Words in the names of what holds a page's content.
const LIKELY: &[&str] = &[
    "article", "body", "content", "entry", "main", "post", "story", "text", "blog", "page",
];
/// Words in names that score a block down.
const NEGATIVE: &[&str] = &[
    "hidden", "banner", "comment", "contact", "foot", "footnote", "masthead", "media", "meta", "promo",
    "related", "scroll", "share", "shoutbox", "sidebar", "sponsor", "shopping", "tags", "tool", "widget",
    "ad-",
];
/// Blocks that hold others: a paragraph-like element has none of them.
const HOLDERS: &[&str] = &[
    "div",
    "section",
    "article",
    "main",
    "p",
    "ul",
    "ol",
    "table",
    "blockquote",
    "pre",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "dl",
    "figure",
    "form",
    "header",
    "footer",
    "nav",
    "aside",
];

/// The visible text's measures inside each node.
struct Measures {
    chars: Vec<u32>,
    link_chars: Vec<u32>,
    commas: Vec<u32>,
    links: Vec<u32>,
}

fn names_of(page: &Page, id: usize) -> String {
    let class = page.attr(id, "class").unwrap_or_default();
    let ident = page.attr(id, "id").unwrap_or_default();
    format!("{class} {ident}").to_ascii_lowercase()
}

fn weight(page: &Page, id: usize) -> f64 {
    let names = names_of(page, id);
    let mut w = 0.0;
    if LIKELY.iter().any(|w| names.contains(w)) {
        w += 25.0;
    }
    if NEGATIVE.iter().any(|w| names.contains(w)) {
        w -= 25.0;
    }
    w
}

/// Whether an element is never shown: its kind, or what it says of itself.
fn never_shown(page: &Page, id: usize) -> bool {
    let Some(name) = page.name(id) else {
        return false;
    };
    if NEVER_SHOWN.contains(&name) || (name == "dialog" && page.attr(id, "open").is_none()) {
        return true;
    }
    if page.attr(id, "hidden").is_some() || page.attr(id, "aria-hidden") == Some("true") {
        return true;
    }
    page.attr(id, "style").is_some_and(|s| {
        let s: String = s
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        s.contains("display:none") || s.contains("visibility:hidden")
    })
}

/// Whether an element is around a page's content rather than part of it.
fn around(page: &Page, id: usize) -> bool {
    let Some(name) = page.name(id) else {
        return false;
    };
    if matches!(name, "html" | "body" | "main" | "article") || page.attr(id, "role") == Some("main") {
        return false;
    }
    if AROUND.contains(&name) {
        return true;
    }
    if name == "header"
        && !page
            .ancestors(id)
            .any(|a| page.is(a, "article") || page.is(a, "main"))
    {
        return true;
    }
    if page.attr(id, "role").is_some_and(|r| AROUND_ROLES.contains(&r)) {
        return true;
    }
    let names = names_of(page, id);
    UNLIKELY.iter().any(|w| names.contains(w)) && !LIKELY.iter().any(|w| names.contains(w))
}

/// Each node left out: by its kind or its ancestors'; for the main part, also what is around the content.
fn hidden(page: &Page, main: bool) -> Vec<bool> {
    let mut out = vec![false; page.nodes.len()];
    for id in 1..page.nodes.len() {
        let above = page.parent(id).and_then(|p| out.get(p).copied()).unwrap_or(false);
        let own = never_shown(page, id) || (main && around(page, id));
        if let Some(h) = out.get_mut(id) {
            *h = above || own;
        }
    }
    out
}

fn measure(page: &Page, hidden: &[bool]) -> Measures {
    let n = page.nodes.len();
    let mut m = Measures {
        chars: vec![0; n],
        link_chars: vec![0; n],
        commas: vec![0; n],
        links: vec![0; n],
    };
    // Children come after their parents: counted from the end, each node's totals are whole when added up.
    for id in (1..n).rev() {
        if hidden.get(id).copied().unwrap_or(true) {
            continue;
        }
        if let Some(t) = page.text_node(id) {
            let chars = t.chars().filter(|c| !c.is_whitespace()).count();
            let commas = t.chars().filter(|c| matches!(c, ',' | '，' | '、')).count();
            if let Some(c) = m.chars.get_mut(id) {
                *c = u32::try_from(chars).unwrap_or(u32::MAX);
            }
            if let Some(c) = m.commas.get_mut(id) {
                *c = u32::try_from(commas).unwrap_or(u32::MAX);
            }
        }
        if page.is(id, "a") {
            let all = m.chars.get(id).copied().unwrap_or(0);
            if let Some(l) = m.link_chars.get_mut(id) {
                *l = all;
            }
            if let Some(n) = m.links.get_mut(id) {
                *n = n.saturating_add(1);
            }
        }
        if let Some(parent) = page.parent(id) {
            for column in [&mut m.chars, &mut m.link_chars, &mut m.commas, &mut m.links] {
                let own = column.get(id).copied().unwrap_or(0);
                if let Some(p) = column.get_mut(parent) {
                    *p = p.saturating_add(own);
                }
            }
        }
    }
    m
}

impl Measures {
    fn chars(&self, id: usize) -> f64 {
        f64::from(self.chars.get(id).copied().unwrap_or(0))
    }

    fn link_density(&self, id: usize) -> f64 {
        let all = self.chars(id);
        if all == 0.0 {
            return 0.0;
        }
        f64::from(self.link_chars.get(id).copied().unwrap_or(0)) / all
    }

    fn commas(&self, id: usize) -> f64 {
        f64::from(self.commas.get(id).copied().unwrap_or(0))
    }
}

fn base_score(page: &Page, id: usize) -> f64 {
    let tag = match page.name(id).unwrap_or_default() {
        "div" | "article" | "main" | "section" => 5.0,
        "pre" | "td" | "blockquote" => 3.0,
        "address" | "ol" | "ul" | "dl" | "dd" | "dt" | "li" | "form" => -3.0,
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "th" => -5.0,
        _ => 0.0,
    };
    tag + weight(page, id)
}

/// The block whose paragraphs hold the most text, and the blocks beside it that read as part of it.
fn scored(page: &Page, hidden: &[bool], m: &Measures) -> Option<Vec<usize>> {
    let mut scores: std::collections::HashMap<usize, f64> = std::collections::HashMap::new();
    for id in 1..page.nodes.len() {
        if hidden.get(id).copied().unwrap_or(true) {
            continue;
        }
        let Some(name) = page.name(id) else { continue };
        let paragraph = matches!(name, "p" | "pre" | "td" | "blockquote")
            || (matches!(name, "div" | "section")
                && !page
                    .elements(id)
                    .any(|c| HOLDERS.contains(&page.name(c).unwrap_or_default())));
        let len = m.chars(id);
        if !paragraph || len < 25.0 {
            continue;
        }
        let own = 1.0 + m.commas(id) + (len / 100.0).floor().min(3.0);
        for (level, a) in page.ancestors(id).take(3).enumerate() {
            if page.is(a, "html") || hidden.get(a).copied().unwrap_or(true) {
                continue;
            }
            let divide = match level {
                0 => 1.0,
                1 => 2.0,
                l => (l as f64) * 3.0,
            };
            *scores.entry(a).or_insert_with(|| base_score(page, a)) += own / divide;
        }
    }
    let finals: std::collections::HashMap<usize, f64> = scores
        .iter()
        .map(|(id, s)| (*id, s * (1.0 - m.link_density(*id))))
        .collect();
    let (top, top_score) = finals
        .iter()
        .map(|(id, s)| (*id, *s))
        .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))?;
    if m.chars(top) < 140.0 {
        return None;
    }
    let Some(parent) = page
        .parent(top)
        .filter(|p| !page.is(*p, "body") && page.name(*p).is_some())
    else {
        return Some(vec![top]);
    };
    let threshold = (top_score * 0.2).max(10.0);
    let score_of = |id: usize| finals.get(&id).copied().unwrap_or(0.0);
    let mut parts = Vec::new();
    for s in page.elements(parent) {
        if hidden.get(s).copied().unwrap_or(true) {
            continue;
        }
        let len = m.chars(s);
        let density = m.link_density(s);
        let beside = s == top
            || score_of(s) >= threshold
            || (page.is(s, "p") && len > 80.0 && density < 0.25)
            || (page.is(s, "p")
                && len > 0.0
                && density == 0.0
                && page.text_of(s, &|_| false).ends_with(['.', '!', '?']));
        if beside {
            parts.push(s);
        }
    }
    Some(parts)
}

/// The visible elements called `name` with enough text to be a page's content.
fn substantial(page: &Page, hidden: &[bool], m: &Measures, test: &dyn Fn(usize) -> bool) -> Vec<usize> {
    (1..page.nodes.len())
        .filter(|id| test(*id) && !hidden.get(*id).copied().unwrap_or(true) && m.chars(*id) >= 140.0)
        .collect()
}

/// Inside the main part, blocks that are clusters of links (related posts, tags, a share bar) are left out too.
fn clear_clusters(page: &Page, parts: &[usize], hidden: &mut [bool], m: &Measures) {
    for part in parts {
        for id in page.within(*part).skip(1) {
            let Some(name) = page.name(id) else { continue };
            if !matches!(name, "ul" | "ol" | "div" | "section" | "table" | "p" | "dl") {
                continue;
            }
            let links = m.links.get(id).copied().unwrap_or(0);
            let cluster = links >= 3 && m.link_density(id) > 0.5 && m.commas(id) < 10.0;
            let scored_down = weight(page, id) < 0.0 && m.commas(id) < 10.0;
            if (cluster || scored_down)
                && let Some(h) = hidden.get_mut(id)
            {
                *h = true;
            }
        }
    }
    // What is inside a block left out is left out with it.
    for id in 1..page.nodes.len() {
        let above = page
            .parent(id)
            .and_then(|p| hidden.get(p).copied())
            .unwrap_or(false);
        if above && let Some(h) = hidden.get_mut(id) {
            *h = true;
        }
    }
}

/// What is never shown, wherever it is on `page`: what a page's links and pictures are taken from.
pub(crate) fn never(page: &Page) -> Vec<bool> {
    hidden(page, false)
}

/// What of `page` is written, for `part`.
pub(crate) fn content(page: &Page, part: &Part, budget: &mut Budget) -> Result<Content, String> {
    let body = page.first("body").unwrap_or(0);
    match part {
        Part::All => Ok(Content {
            parts: vec![body],
            hidden: hidden(page, false),
        }),
        Part::Within(selector) => {
            let found = selector.select(page, 0, 100, budget)?;
            // An element inside another found is written with it, once.
            let parts: Vec<usize> = found
                .iter()
                .copied()
                .filter(|id| !page.ancestors(*id).any(|a| found.contains(&a)))
                .collect();
            if parts.is_empty() {
                return Err("within: nothing on the page matches it".to_owned());
            }
            Ok(Content {
                parts,
                hidden: hidden(page, false),
            })
        }
        Part::Main => {
            let mut hide = hidden(page, true);
            let m = measure(page, &hide);
            let articles = substantial(page, &hide, &m, &|id| page.is(id, "article"));
            let mains = substantial(page, &hide, &m, &|id| {
                page.is(id, "main") || page.attr(id, "role") == Some("main")
            });
            let parts = match (articles.as_slice(), mains.first()) {
                ([one], _) => vec![*one],
                (_, Some(main)) => vec![*main],
                _ => scored(page, &hide, &m).unwrap_or_else(|| vec![body]),
            };
            clear_clusters(page, &parts, &mut hide, &m);
            let mut parts = parts;
            // The page's heading, when the content is under it rather than inside it.
            let has_h1 = parts.iter().any(|p| page.within(*p).any(|i| page.is(i, "h1")));
            if !has_h1
                && let Some(first) = parts.first().copied()
                && let Some(h1) = page
                    .all("h1")
                    .find(|h| *h < first && !hide.get(*h).copied().unwrap_or(true))
            {
                parts.insert(0, h1);
            }
            Ok(Content { parts, hidden: hide })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shown(html: &str, part: Part) -> String {
        let page = Page::parse(html, false);
        let c = content(&page, &part, &mut Budget(1_000_000)).unwrap();
        c.parts
            .iter()
            .map(|p| page.text_of(*p, &|i| c.hidden.get(i).copied().unwrap_or(true)))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    const LONG: &str = "This paragraph is long enough to be what a person came to read, with commas, clauses, and \
                        more words than any menu would ever hold, so it scores well.";

    #[test]
    fn the_one_article_is_the_main_part_without_its_footer() {
        let html = format!(
            "<body><nav><a href=/>Home</a></nav><article><header><h1>Title</h1></header><p>{LONG}</p>\
             <footer>Tags: x</footer></article><aside>Popular</aside><footer>Footer</footer></body>"
        );
        assert_eq!(shown(&html, Part::Main), format!("Title {LONG}"));
        assert!(shown(&html, Part::All).contains("Home"));
    }

    #[test]
    fn a_page_of_divs_is_scored_and_what_is_around_it_left_out() {
        let html = format!(
            "<body><div class=top><a href=/a>A</a> <a href=/b>B</a> <a href=/c>C</a></div><h1>The story</h1>\
             <div id=story><p>{LONG}</p><p>{LONG}</p><ul class=related><li><a href=/1>One related post</a></li>\
             <li><a href=/2>Another related post</a></li><li><a href=/3>A third</a></li></ul></div>\
             <div class=cookie-banner>We use cookies, and you agree.</div><div class=links><a href=/x>X</a> <a \
             href=/y>Y</a> <a href=/z>Z</a></div></body>"
        );
        assert_eq!(shown(&html, Part::Main), format!("The story | {LONG} {LONG}"));
    }

    #[test]
    fn hidden_things_are_never_shown_and_within_picks_a_part() {
        let html = "<body><p>Shown</p><p hidden>No</p><p style='display: none'>No</p><div aria-hidden=true>No</div>\
                    <script>no()</script><template><p>No</p></template><div id=x><p>Here</p></div></body>";
        assert_eq!(shown(html, Part::All), "Shown Here");
        assert_eq!(shown(html, Part::Within(Selector::parse("#x").unwrap())), "Here");
        let page = Page::parse(html, false);
        let e = content(
            &page,
            &Part::Within(Selector::parse("#none").unwrap()),
            &mut Budget(1000),
        )
        .unwrap_err();
        assert!(e.contains("nothing on the page"));
        // A page too short to tell its main part gives all it shows, but what is around it.
        assert_eq!(shown("<body><nav>Menu</nav><p>Hi</p></body>", Part::Main), "Hi");
    }
}
