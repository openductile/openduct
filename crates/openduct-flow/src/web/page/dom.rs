// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A page as a tree of elements and text, built as a browser builds it from markup that is often not well formed:
//! elements that never hold others (`<br>`, `<img>`) stay empty; a paragraph, a list item, a table cell or row ends
//! where the next begins or a block starts (`<p>one<p>two`); a closing tag with nothing open to close is passed over,
//! and one that closes an element further out closes those inside it too. Nodes are kept in the order they come, so
//! their numbers are the document's order. Bounded: at most [`MAX_NODES`] nodes and [`MAX_DEPTH`] levels, beyond
//! which what follows is laid in at the deepest level, or left out.

use super::tokens::{Token, Tokens};

/// The most nodes a page is read into; what follows is left out.
pub(crate) const MAX_NODES: usize = 400_000;
/// The deepest elements nest; deeper ones are laid in beside the deepest.
pub(crate) const MAX_DEPTH: usize = 256;

/// Elements that never hold others.
const VOID: &[&str] = &[
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track",
    "wbr", "keygen", "basefont", "frame",
];
/// Elements whose start ends an open paragraph.
const ENDS_P: &[&str] = &[
    "address",
    "article",
    "aside",
    "blockquote",
    "center",
    "details",
    "dialog",
    "dir",
    "div",
    "dl",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hgroup",
    "hr",
    "main",
    "menu",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "summary",
    "table",
    "ul",
    "li",
    "dd",
    "dt",
    "listing",
    "xmp",
];
/// Where a search for an open element to close stops, for elements that are not part of a table.
const SCOPE: &[&str] = &[
    "td", "th", "table", "caption", "template", "html", "object", "marquee", "applet",
];
/// Where the search for an open paragraph to end stops.
const P_SCOPE: &[&str] = &[
    "td", "th", "table", "caption", "template", "html", "object", "marquee", "applet", "button",
];

/// One node of a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Data {
    /// The page itself, the top of the tree.
    Root,
    Element {
        name: String,
        attributes: Vec<(String, String)>,
    },
    Text(String),
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub(crate) parent: Option<usize>,
    pub(crate) children: Vec<usize>,
    pub(crate) data: Data,
}

/// A page's tree.
#[derive(Debug, Clone)]
pub(crate) struct Page {
    pub(crate) nodes: Vec<Node>,
    /// Some of the page was left out: it had more than [`MAX_NODES`] nodes.
    pub(crate) cut: bool,
}

fn heading(name: &str) -> bool {
    matches!(name, "h1" | "h2" | "h3" | "h4" | "h5" | "h6")
}

struct Builder {
    page: Page,
    /// The open elements, outermost first; the root at the bottom.
    open: Vec<usize>,
    xml: bool,
    /// The page's <html>, <head> and <body>, once made: a second one opens nothing.
    once: [Option<usize>; 3],
}

impl Builder {
    fn current(&self) -> usize {
        self.open.last().copied().unwrap_or(0)
    }

    fn name_of(&self, id: usize) -> &str {
        match self.page.nodes.get(id).map(|n| &n.data) {
            Some(Data::Element { name, .. }) => name,
            _ => "",
        }
    }

    fn add(&mut self, data: Data) -> Option<usize> {
        if self.page.nodes.len() >= MAX_NODES {
            self.page.cut = true;
            return None;
        }
        let parent = self.current();
        let id = self.page.nodes.len();
        self.page.nodes.push(Node {
            parent: Some(parent),
            children: Vec::new(),
            data,
        });
        if let Some(p) = self.page.nodes.get_mut(parent) {
            p.children.push(id);
        }
        Some(id)
    }

    fn text(&mut self, text: &str) {
        let parent = self.current();
        let last = self
            .page
            .nodes
            .get(parent)
            .and_then(|p| p.children.last().copied());
        if let Some(last) = last
            && let Some(Node {
                data: Data::Text(t), ..
            }) = self.page.nodes.get_mut(last)
        {
            t.push_str(text);
            return;
        }
        self.add(Data::Text(text.to_owned()));
    }

    /// Where on the open elements the nearest `names` is, looking no further out than `stop`.
    fn open_at(&self, names: &[&str], stop: &[&str]) -> Option<usize> {
        for (i, id) in self.open.iter().enumerate().rev() {
            let name = self.name_of(*id);
            if names.contains(&name) {
                return Some(i);
            }
            if stop.contains(&name) {
                return None;
            }
        }
        None
    }

    fn close_from(&mut self, at: usize) {
        self.open.truncate(at.max(1));
    }

    /// What a start tag ends before it opens, as HTML says.
    fn ends_before(&mut self, name: &str) {
        if ENDS_P.contains(&name)
            && let Some(at) = self.open_at(&["p"], P_SCOPE)
        {
            self.close_from(at);
        }
        let (names, stop): (&[&str], &[&str]) = match name {
            "li" => (&["li"], &["ul", "ol", "menu", "table", "td", "th", "body"]),
            "dt" | "dd" => (&["dt", "dd"], &["dl", "table", "td", "th", "body"]),
            "tr" => (&["tr"], &["table"]),
            "td" | "th" => (&["td", "th"], &["tr", "table"]),
            "thead" | "tbody" | "tfoot" => (&["thead", "tbody", "tfoot"], &["table"]),
            "option" => (&["option"], &["select", "datalist", "optgroup"]),
            "optgroup" => (&["optgroup", "option"], &["select"]),
            "a" => (&["a"], SCOPE),
            "button" => (&["button"], SCOPE),
            _ => (&[], &[]),
        };
        if !names.is_empty()
            && let Some(at) = self.open_at(names, stop)
        {
            self.close_from(at);
        }
        if heading(name) && heading(self.name_of(self.current())) {
            self.open.pop();
        }
    }

    fn start(&mut self, name: String, attributes: Vec<(String, String)>, closed: bool) {
        if !self.xml {
            // A second <html> or <body> adds its attributes to the first; it opens nothing.
            let once = ["html", "head", "body"].iter().position(|n| *n == name);
            if let Some(slot) = once
                && let Some(id) = self.once.get(slot).copied().flatten()
            {
                if let Some(Node {
                    data: Data::Element { attributes: have, .. },
                    ..
                }) = self.page.nodes.get_mut(id)
                {
                    for (k, v) in attributes {
                        if !have.iter().any(|(h, _)| *h == k) {
                            have.push((k, v));
                        }
                    }
                }
                return;
            }
            if let Some(slot) = once
                && let Some(kept) = self.once.get_mut(slot)
            {
                *kept = Some(self.page.nodes.len());
            }
            self.ends_before(&name);
        }
        let empty = if self.xml {
            closed
        } else {
            VOID.contains(&name.as_str())
                || (closed
                    && (matches!(name.as_str(), "svg" | "math")
                        || self
                            .open
                            .iter()
                            .any(|id| matches!(self.name_of(*id), "svg" | "math"))))
        };
        let Some(id) = self.add(Data::Element { name, attributes }) else {
            return;
        };
        if !empty && self.open.len() <= MAX_DEPTH {
            self.open.push(id);
        }
    }

    fn end(&mut self, name: &str) {
        if !self.xml && name == "br" {
            self.start("br".to_owned(), Vec::new(), false);
            return;
        }
        if !self.xml && matches!(name, "body" | "html") {
            // What follows a page's end still belongs to its body, as a browser has it.
            return;
        }
        let table = matches!(
            name,
            "td" | "th" | "tr" | "tbody" | "thead" | "tfoot" | "table" | "caption"
        );
        let stop: &[&str] = if self.xml || table { &[] } else { SCOPE };
        let names: &[&str] = if !self.xml && heading(name) {
            &["h1", "h2", "h3", "h4", "h5", "h6"]
        } else {
            &[name]
        };
        if let Some(at) = self.open_at(names, stop) {
            self.close_from(at);
        }
    }
}

impl Page {
    /// A page read from HTML (`xml` false) or XML.
    pub(crate) fn parse(text: &str, xml: bool) -> Self {
        let mut b = Builder {
            page: Page {
                nodes: vec![Node {
                    parent: None,
                    children: Vec::new(),
                    data: Data::Root,
                }],
                cut: false,
            },
            open: vec![0],
            xml,
            once: [None; 3],
        };
        let mut tokens = Tokens::new(text, xml);
        while let Some(token) = tokens.next_token() {
            match token {
                Token::Start {
                    name,
                    attributes,
                    closed,
                } => b.start(name, attributes, closed),
                Token::End(name) => b.end(&name),
                Token::Text(t) => b.text(&t),
            }
            if b.page.cut {
                break;
            }
        }
        b.page
    }

    /// An element's name (in small letters, for HTML); `None` for text and the root.
    pub(crate) fn name(&self, id: usize) -> Option<&str> {
        match &self.nodes.get(id)?.data {
            Data::Element { name, .. } => Some(name),
            _ => None,
        }
    }

    pub(crate) fn is(&self, id: usize, name: &str) -> bool {
        self.name(id) == Some(name)
    }

    /// An element's attribute.
    pub(crate) fn attr(&self, id: usize, name: &str) -> Option<&str> {
        match &self.nodes.get(id)?.data {
            Data::Element { attributes, .. } => attributes
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str()),
            _ => None,
        }
    }

    pub(crate) fn text_node(&self, id: usize) -> Option<&str> {
        match &self.nodes.get(id)?.data {
            Data::Text(t) => Some(t),
            _ => None,
        }
    }

    pub(crate) fn children(&self, id: usize) -> &[usize] {
        self.nodes.get(id).map_or(&[], |n| n.children.as_slice())
    }

    pub(crate) fn parent(&self, id: usize) -> Option<usize> {
        self.nodes.get(id)?.parent
    }

    /// The element children of `id`.
    pub(crate) fn elements(&self, id: usize) -> impl Iterator<Item = usize> + '_ {
        self.children(id)
            .iter()
            .copied()
            .filter(|c| self.name(*c).is_some())
    }

    /// `id` and every node inside it, in the document's order (their numbers run on from `id`'s).
    pub(crate) fn within(&self, id: usize) -> std::ops::Range<usize> {
        let mut end = id;
        let mut at = id;
        // The last node inside `id` is the last child's last ... last child.
        while let Some(last) = self.children(at).last() {
            end = *last;
            at = *last;
        }
        id..(end + 1).min(self.nodes.len())
    }

    /// The first element called `name`, in the document's order.
    pub(crate) fn first(&self, name: &str) -> Option<usize> {
        (0..self.nodes.len()).find(|i| self.is(*i, name))
    }

    /// Every element called `name`, in the document's order.
    pub(crate) fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = usize> + 'a {
        (0..self.nodes.len()).filter(move |i| self.is(*i, name))
    }

    /// The text inside `id`, its runs of white space made one space, trimmed; `skip` leaves elements out.
    pub(crate) fn text_of(&self, id: usize, skip: &dyn Fn(usize) -> bool) -> String {
        let mut out = String::new();
        // Where the blocks entered end: a block's end, like its start, parts the words on either side.
        let mut ends: Vec<usize> = Vec::new();
        let mut apart = false;
        let mut range = self.within(id);
        while let Some(i) = range.next() {
            while ends.last().is_some_and(|e| *e <= i) {
                ends.pop();
                apart = true;
            }
            if i != id && skip(i) {
                // Past everything inside it.
                range = self.within(i).end..range.end;
                continue;
            }
            match self.nodes.get(i).map(|n| &n.data) {
                Some(Data::Text(t)) => {
                    if apart && !out.is_empty() && !out.ends_with(' ') {
                        out.push(' ');
                    }
                    apart = false;
                    push_words(&mut out, t);
                }
                Some(Data::Element { name, .. }) if is_block(name) || name == "br" => {
                    apart = true;
                    ends.push(self.within(i).end);
                }
                _ => {}
            }
        }
        out.trim().to_owned()
    }

    /// The elements `id` is inside, nearest first.
    pub(crate) fn ancestors(&self, id: usize) -> impl Iterator<Item = usize> + '_ {
        std::iter::successors(self.parent(id), |p| self.parent(*p)).filter(|p| self.name(*p).is_some())
    }
}

/// Whether an element stands on lines of its own.
pub(crate) fn is_block(name: &str) -> bool {
    ENDS_P.contains(&name)
        || matches!(
            name,
            "html"
                | "body"
                | "tr"
                | "td"
                | "th"
                | "tbody"
                | "thead"
                | "tfoot"
                | "caption"
                | "option"
                | "title"
        )
}

/// Text added to `out`, its runs of white space one space.
pub(crate) fn push_words(out: &mut String, text: &str) {
    if text.starts_with(char::is_whitespace) && !out.is_empty() && !out.ends_with(' ') {
        out.push(' ');
    }
    let mut first = true;
    for word in text.split_whitespace() {
        if !first {
            out.push(' ');
        }
        out.push_str(word);
        first = false;
    }
    if text.ends_with(char::is_whitespace) && !first && !out.ends_with(' ') {
        out.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tree as nested names, for a test to compare: `p(t,b(t))`.
    fn shape(page: &Page, id: usize) -> String {
        let inside: Vec<String> = page.children(id).iter().map(|c| shape(page, *c)).collect();
        let name = match &page.nodes[id].data {
            Data::Root => "#",
            Data::Element { name, .. } => name,
            Data::Text(_) => return "t".to_owned(),
        };
        if inside.is_empty() {
            name.to_owned()
        } else {
            format!("{name}({})", inside.join(","))
        }
    }

    #[test]
    fn markup_that_is_not_well_formed_is_built_as_a_browser_builds_it() {
        let page = Page::parse(
            "<html><body><p>one<p>two<div>three</div><ul><li>a<li>b<ul><li>c</ul><li>d</ul>\
             <table><tr><td>1<td>2<tr><td>3</table></b><img src=x><br></br><h1>x<h2>y</h2></body></html>after",
            false,
        );
        assert_eq!(
            shape(&page, 0),
            "#(html(body(p(t),p(t),div(t),ul(li(t),li(t,ul(li(t))),li(t)),table(tr(td(t),td(t)),tr(td(t))),img,br,br,\
             h1(t),h2(t),t)))"
        );
    }

    #[test]
    fn a_closing_tag_closes_what_is_inside_it_but_not_past_a_table_cell() {
        let page = Page::parse(
            "<div><span><b>x</div>y<table><tr><td><div>in</span></div>z</td></tr></table>",
            false,
        );
        assert_eq!(shape(&page, 0), "#(div(span(b(t))),t,table(tr(td(div(t),t))))");
        let td = page.first("td").unwrap();
        assert_eq!(page.text_of(td, &|_| false), "in z");
    }

    #[test]
    fn xml_is_built_as_written_and_limits_hold() {
        let page = Page::parse(
            "<feed><entry><link href='a'/><title>T</title></entry></feed>",
            true,
        );
        assert_eq!(shape(&page, 0), "#(feed(entry(link,title(t))))");
        let deep = "<div>".repeat(MAX_DEPTH * 2) + "x";
        let page = Page::parse(&deep, false);
        assert!(page.nodes.len() > MAX_DEPTH * 2);
        let wide = "<i></i>".repeat(MAX_NODES + 10);
        let page = Page::parse(&wide, false);
        assert!(page.cut);
        assert_eq!(page.nodes.len(), MAX_NODES);
    }

    #[test]
    fn a_nodes_range_and_text_are_found() {
        let page = Page::parse("<p>a <b>b</b><script>no</script></p><p>c</p>", false);
        let p = page.first("p").unwrap();
        assert_eq!(page.within(p).len(), 6);
        assert_eq!(page.text_of(p, &|i| page.is(i, "script")), "a b");
        assert_eq!(
            page.ancestors(page.first("b").unwrap()).collect::<Vec<_>>(),
            vec![p]
        );
    }
}
