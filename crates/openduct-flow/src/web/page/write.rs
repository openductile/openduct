// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The part of a page that is read, written as Markdown or as plain text, in the page's order: headings (`## How`
//! in Markdown, a line of their own in text), paragraphs apart, lists as `- ` and `1. ` (nested ones indented),
//! quotes as `> `, preformatted text and code as they are (fenced in Markdown), data tables as rows of cells (a
//! Markdown table, or cells apart by tabs), and - in Markdown - links, pictures, bold, italics and code marked as
//! Markdown marks them, links and pictures at their full addresses. A table used to lay a page out is written as
//! the blocks it holds.

use super::dom::{Data, Page, is_block, push_words};
use super::meta::Base;

struct Writer<'a> {
    page: &'a Page,
    hidden: &'a [bool],
    base: &'a Base,
    markdown: bool,
    out: String,
    /// The paragraph being made.
    line: String,
    /// What each line of a paragraph begins with, outermost first: `> ` for a quote, spaces for a list item.
    prefix: Vec<String>,
    /// A list item's mark, for the first line of its first paragraph.
    mark: Option<String>,
    /// The lists open, each with the number of its next item (`None` for a list of bullets).
    lists: Vec<Option<u64>>,
    /// Inside an element written inline (a link's text, a cell): blocks are only apart by a space.
    inline: usize,
    /// The last block written was a list item's first paragraph.
    after_item: bool,
}

/// `text` without `]` and `[` breaking a link's text.
fn link_text(text: &str) -> String {
    text.replace('[', "\\[").replace(']', "\\]").replace('\n', " ")
}

/// An address as Markdown reads it inside `( )`.
fn link_target(url: &str) -> String {
    url.replace('(', "%28").replace(')', "%29").replace(' ', "%20")
}

impl Writer<'_> {
    fn skipped(&self, id: usize) -> bool {
        self.hidden.get(id).copied().unwrap_or(true)
    }

    /// Text added to the paragraph, apart from what is before it by a space when either side has one.
    fn put(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if self.line.ends_with(' ') && text.starts_with(' ') {
            self.line.push_str(text.trim_start_matches(' '));
        } else {
            self.line.push_str(text);
        }
    }

    fn apart(&mut self) {
        if !self.line.is_empty() && !self.line.ends_with([' ', '\n']) {
            self.line.push(' ');
        }
    }

    /// The prefix of a paragraph's lines: its first's (with a list item's mark) and the rest's.
    fn prefixes(&mut self) -> (String, String) {
        let rest: String = self.prefix.concat();
        let first = match self.mark.take() {
            Some(mark) => {
                let outer: String = self
                    .prefix
                    .get(..self.prefix.len().saturating_sub(1))
                    .unwrap_or_default()
                    .concat();
                format!("{outer}{mark}")
            }
            None => rest.clone(),
        };
        (first, rest)
    }

    /// Writes a block: its lines, each begun as the context says; apart from the one before by a blank line, or a
    /// line's end between items of a list.
    fn block(&mut self, text: &str, keep_spaces: bool) {
        let text = if keep_spaces {
            text.trim_matches('\n')
        } else {
            text.trim()
        };
        if text.is_empty() {
            return;
        }
        let item = self.mark.is_some();
        let (first, rest) = self.prefixes();
        if !self.out.is_empty() {
            self.out
                .push_str(if item && self.after_item { "\n" } else { "\n\n" });
        }
        for (i, line) in text.split('\n').enumerate() {
            if i > 0 {
                self.out.push('\n');
            }
            let line = if keep_spaces { line.trim_end() } else { line.trim() };
            self.out.push_str(if i == 0 { &first } else { &rest });
            self.out.push_str(line);
        }
        self.after_item = item || (self.after_item && !self.lists.is_empty());
    }

    /// The paragraph so far, written.
    fn flush(&mut self) {
        if self.inline > 0 {
            self.apart();
            return;
        }
        let line = std::mem::take(&mut self.line);
        self.block(&line, false);
    }

    fn children(&mut self, id: usize) {
        for c in self.page.children(id).to_vec() {
            self.node(c);
        }
    }

    fn node(&mut self, id: usize) {
        if self.skipped(id) {
            return;
        }
        match self.page.nodes.get(id).map(|n| &n.data) {
            Some(Data::Text(t)) => {
                if t.starts_with(char::is_whitespace) {
                    self.apart();
                }
                let mut words = String::new();
                push_words(&mut words, t.trim_start());
                self.line.push_str(&words);
            }
            Some(Data::Element { name, .. }) => {
                let name = name.clone();
                self.element(id, &name);
            }
            Some(Data::Root) => self.children(id),
            None => {}
        }
    }

    /// An element's content written inline, taken out of the paragraph: its words, and whether a space came before
    /// and after them.
    fn inline_of(&mut self, id: usize) -> (String, bool, bool) {
        let start = self.line.len();
        self.inline += 1;
        self.children(id);
        self.inline -= 1;
        let made = self.line.split_off(start);
        let before = made.starts_with([' ', '\n']) || (start > 0 && self.line.ends_with(' '));
        let after = made.ends_with([' ', '\n']);
        (
            made.split_whitespace().collect::<Vec<_>>().join(" "),
            before,
            after,
        )
    }

    /// `core` put in the paragraph, with the spaces that were around it.
    fn put_spaced(&mut self, core: &str, before: bool, after: bool) {
        if core.is_empty() {
            if before || after {
                self.apart();
            }
            return;
        }
        if before {
            self.apart();
        }
        self.put(core);
        if after {
            self.line.push(' ');
        }
    }

    /// Bold, italics, code: the content between its marks, the spaces around it outside them.
    fn marked(&mut self, id: usize, mark: &str) {
        let (core, before, after) = self.inline_of(id);
        let shown = if self.markdown && !core.is_empty() {
            format!("{mark}{core}{mark}")
        } else {
            core
        };
        self.put_spaced(&shown, before, after);
    }

    fn element(&mut self, id: usize, name: &str) {
        match name {
            "br" => {
                while self.line.ends_with(' ') {
                    self.line.pop();
                }
                self.line.push(if self.inline > 0 { ' ' } else { '\n' });
            }
            "hr" => {
                self.flush();
                if self.markdown && self.inline == 0 {
                    self.block("---", false);
                }
            }
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                self.flush();
                let (text, _, _) = self.inline_of(id);
                if self.inline > 0 {
                    self.put(&text);
                    self.apart();
                } else if !text.is_empty() {
                    let level = name.get(1..).and_then(|l| l.parse::<usize>().ok()).unwrap_or(1);
                    let heading = if self.markdown {
                        format!("{} {text}", "#".repeat(level))
                    } else {
                        text
                    };
                    self.block(&heading, false);
                    self.after_item = false;
                }
            }
            "ul" | "ol" | "menu" => {
                self.flush();
                let start = self
                    .page
                    .attr(id, "start")
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .unwrap_or(1);
                self.lists.push((name == "ol").then_some(start));
                self.children(id);
                self.flush();
                self.lists.pop();
                if self.lists.is_empty() {
                    self.after_item = false;
                }
            }
            "li" if self.inline == 0 => {
                self.flush();
                let mark = match self.lists.last_mut() {
                    Some(Some(n)) => {
                        let mark = format!("{n}. ");
                        *n += 1;
                        mark
                    }
                    _ => "- ".to_owned(),
                };
                self.prefix.push(" ".repeat(mark.len()));
                self.mark = Some(mark);
                self.children(id);
                self.flush();
                self.mark = None;
                self.prefix.pop();
            }
            "blockquote" if self.inline == 0 => {
                self.flush();
                self.prefix.push(if self.markdown { "> " } else { "" }.to_owned());
                self.after_item = false;
                self.children(id);
                self.flush();
                self.prefix.pop();
            }
            "pre" if self.inline == 0 => self.pre(id),
            "table" if self.inline == 0 => self.table(id),
            "a" => self.link(id),
            "img" => self.image(id),
            "strong" | "b" => self.marked(id, "**"),
            "em" | "i" | "cite" => self.marked(id, "*"),
            "code" | "kbd" | "samp" | "tt" => self.marked(id, "`"),
            "del" | "s" | "strike" => self.marked(id, "~~"),
            "dt" | "figcaption" | "caption" | "summary" => {
                self.flush();
                self.children(id);
                self.flush();
            }
            "td" | "th" => {
                self.apart();
                self.children(id);
                self.apart();
            }
            _ if is_block(name) => {
                self.flush();
                self.children(id);
                self.flush();
            }
            _ => self.children(id),
        }
    }

    fn link(&mut self, id: usize) {
        let (text, before, after) = self.inline_of(id);
        let target = self
            .page
            .attr(id, "href")
            .and_then(|h| self.base.link(h))
            .filter(|t| t.starts_with("http") || t.starts_with("mailto:") || t.starts_with("tel:"));
        let shown = match target {
            Some(t) if self.markdown && !text.is_empty() => {
                format!("[{}]({})", link_text(&text), link_target(&t))
            }
            _ => text,
        };
        self.put_spaced(&shown, before, after);
    }

    fn image(&mut self, id: usize) {
        if !self.markdown {
            return;
        }
        let src = self
            .page
            .attr(id, "src")
            .filter(|s| !s.trim().is_empty() && !s.trim_start().starts_with("data:"))
            .or_else(|| self.page.attr(id, "data-src"))
            .and_then(|s| self.base.link(s));
        if let Some(src) = src {
            let alt = self.page.attr(id, "alt").unwrap_or_default().trim().to_owned();
            self.apart();
            self.put(&format!("![{}]({})", link_text(&alt), link_target(&src)));
            self.apart();
        }
    }

    fn pre(&mut self, id: usize) {
        self.flush();
        let mut raw = String::new();
        for i in self.page.within(id).skip(1) {
            if self.skipped(i) {
                continue;
            }
            if let Some(t) = self.page.text_node(i) {
                raw.push_str(t);
            } else if self.page.is(i, "br") {
                raw.push('\n');
            }
        }
        let raw = raw.trim_matches('\n');
        if raw.trim().is_empty() {
            return;
        }
        if self.markdown {
            let language = self
                .page
                .within(id)
                .filter_map(|i| self.page.attr(i, "class"))
                .flat_map(str::split_whitespace)
                .find_map(|c| c.strip_prefix("language-").or_else(|| c.strip_prefix("lang-")))
                .unwrap_or_default()
                .to_owned();
            let fence = if raw.contains("```") { "~~~" } else { "```" };
            self.block(&format!("{fence}{language}\n{raw}\n{fence}"), true);
        } else {
            self.block(raw, true);
        }
        self.after_item = false;
    }

    /// The rows of a table, each its cells (not those of a table inside it), and whether it is laid out as data.
    fn rows(&self, table: usize) -> (Vec<Vec<usize>>, bool) {
        let mut rows = Vec::new();
        let mut layout = false;
        let mut stack: Vec<usize> = self.page.elements(table).collect();
        stack.reverse();
        while let Some(id) = stack.pop() {
            if self.skipped(id) {
                continue;
            }
            match self.page.name(id).unwrap_or_default() {
                "thead" | "tbody" | "tfoot" => {
                    let mut inner: Vec<usize> = self.page.elements(id).collect();
                    inner.reverse();
                    stack.extend(inner);
                }
                "tr" => {
                    let cells: Vec<usize> = self
                        .page
                        .elements(id)
                        .filter(|c| matches!(self.page.name(*c), Some("td" | "th")) && !self.skipped(*c))
                        .collect();
                    layout |= cells.iter().any(|c| {
                        self.page.within(*c).skip(1).any(|i| {
                            matches!(
                                self.page.name(i),
                                Some("p" | "div" | "ul" | "ol" | "table" | "h1" | "h2" | "h3" | "section")
                            )
                        })
                    });
                    rows.push(cells);
                }
                _ => {}
            }
        }
        let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
        (rows, !layout && columns > 1)
    }

    fn table(&mut self, id: usize) {
        self.flush();
        let (rows, data) = self.rows(id);
        if !data {
            // A table that lays a page out: its cells' blocks, in order.
            for row in rows {
                for cell in row {
                    self.children(cell);
                    self.flush();
                }
            }
            return;
        }
        let mut lines: Vec<Vec<String>> = Vec::new();
        for row in &rows {
            let cells: Vec<String> = row
                .iter()
                .map(|c| {
                    let (text, _, _) = self.inline_of(*c);
                    if self.markdown {
                        text.replace('|', "\\|")
                    } else {
                        text.replace('\t', " ")
                    }
                })
                .collect();
            if cells.iter().any(|c| !c.is_empty()) {
                lines.push(cells);
            }
        }
        let columns = lines.iter().map(Vec::len).max().unwrap_or(0);
        if lines.is_empty() {
            return;
        }
        let mut text = String::new();
        for (i, cells) in lines.iter().enumerate() {
            let mut cells = cells.clone();
            cells.resize(columns, String::new());
            if self.markdown {
                text.push_str(&format!("| {} |\n", cells.join(" | ")));
                if i == 0 {
                    text.push_str(&format!("|{}\n", " --- |".repeat(columns)));
                }
            } else {
                text.push_str(&cells.join("\t"));
                text.push('\n');
            }
        }
        self.block(&text, true);
        self.after_item = false;
    }
}

/// `parts` of `page` written, as Markdown or plain text; `hidden` says what is left out.
pub(crate) fn write(page: &Page, parts: &[usize], hidden: &[bool], base: &Base, markdown: bool) -> String {
    let mut w = Writer {
        page,
        hidden,
        base,
        markdown,
        out: String::new(),
        line: String::new(),
        prefix: Vec::new(),
        mark: None,
        lists: Vec::new(),
        inline: 0,
        after_item: false,
    };
    for part in parts {
        w.node(*part);
        w.flush();
    }
    w.out
}

/// One element written inline as text (a field picked out of a page): its words, its line breaks kept.
pub(crate) fn inline_text(page: &Page, id: usize, base: &Base) -> String {
    let hidden = vec![false; page.nodes.len()];
    let text = write(page, &[id], &hidden, base, false);
    text.split("\n\n").collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md(html: &str) -> String {
        let page = Page::parse(html, false);
        let base = Base::new("https://shop.example/a/page".parse().unwrap());
        let hidden = vec![false; page.nodes.len()];
        write(&page, &[0], &hidden, &base, true)
    }

    fn text(html: &str) -> String {
        let page = Page::parse(html, false);
        let base = Base::new("https://shop.example/a/page".parse().unwrap());
        let hidden = vec![false; page.nodes.len()];
        write(&page, &[0], &hidden, &base, false)
    }

    #[test]
    fn a_page_is_written_as_markdown_in_its_order() {
        let html = "<h1>Returns</h1><p>Send it <b>back </b>within <em>14</em> days, <a href=\"/help#x\">see \
                    help</a>.<br>New line.</p><ul><li>Pack it<ul><li>in a box</li></ul></li><li>Post it</li></ul>\
                    <ol start=3><li>Three</li><li>Four</li></ol><blockquote><p>Quoted</p></blockquote>\
                    <pre><code class=\"language-sh\">curl -s\n  x</code></pre><p><img src=\"/i.png\" alt=\"A cap\"> \
                    <code>x|y</code></p><table><tr><th>Item</th><th>Price</th></tr><tr><td>Cap</td><td>24|5</td></tr>\
                    </table><hr><p>End</p>";
        assert_eq!(
            md(html),
            "# Returns\n\nSend it **back** within *14* days, [see help](https://shop.example/help#x).\nNew line.\n\n\
             - Pack it\n  - in a box\n- Post it\n\n3. Three\n4. Four\n\n> Quoted\n\n```sh\ncurl -s\n  x\n```\n\n\
             ![A cap](https://shop.example/i.png) `x|y`\n\n| Item | Price |\n| --- | --- |\n| Cap | 24\\|5 |\n\n---\n\nEnd"
        );
    }

    #[test]
    fn plain_text_has_no_marks_but_its_lists_and_cells() {
        let html = "<h2>How</h2><p>A <b>bold</b> <a href=/x>link</a>.</p><ul><li>One</li><li>Two</li></ul>\
                    <table><tr><td>a</td><td>b</td></tr></table><blockquote>Said</blockquote>";
        assert_eq!(text(html), "How\n\nA bold link.\n\n- One\n- Two\n\na\tb\n\nSaid");
    }

    #[test]
    fn a_layout_table_and_odd_markup_read_well() {
        let html = "<table><tr><td><h1>Site</h1><p>Welcome to it.</p></td></tr><tr><td><p>More.</p></td></tr></table>\
                    <a href=\"/card\"><div><h3>Card</h3><p>Its text</p></div></a><p><b> </b>x<i></i></p>\
                    <a href=\"javascript:void(0)\">Menu</a><pre>  </pre>";
        assert_eq!(
            md(html),
            "# Site\n\nWelcome to it.\n\nMore.\n\n[Card Its text](https://shop.example/card)\n\nx\n\nMenu"
        );
    }
}
