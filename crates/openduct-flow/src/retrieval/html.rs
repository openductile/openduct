// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A web page as text, for splitting: its tags taken out, what is not read (scripts, styles, the head) left out, its
//! blocks on lines of their own, its headings written as Markdown's (`## Title`) so a split by headings finds them,
//! its list items as `- `, and its character references read (`&amp;`, `&#233;`). Not a browser: a page that builds
//! itself with scripts gives what its HTML holds.

/// Elements whose content is not text a reader sees.
const SKIPPED: &[&str] = &[
    "script", "style", "noscript", "template", "head", "svg", "iframe", "object",
];
/// Elements that stand on lines of their own.
const BLOCKS: &[&str] = &[
    "p",
    "div",
    "section",
    "article",
    "header",
    "footer",
    "main",
    "aside",
    "nav",
    "blockquote",
    "table",
    "ul",
    "ol",
    "dl",
    "dt",
    "dd",
    "form",
    "figure",
    "figcaption",
    "hr",
    "address",
    "details",
    "summary",
];

/// The text of an HTML page.
pub(crate) fn to_text(html: &str) -> String {
    // ASCII-only lowering keeps every byte where it was, so a place found in one is the same place in the other.
    let lower = html.to_ascii_lowercase();
    let mut out = String::with_capacity(html.len() / 2);
    let mut pre = 0usize;
    let mut at = 0;
    while at < html.len() {
        let rest = html.get(at..).unwrap_or_default();
        let Some(open) = rest.find('<') else {
            words(rest, pre > 0, &mut out);
            break;
        };
        words(rest.get(..open).unwrap_or_default(), pre > 0, &mut out);
        let tag = at + open;
        let from_tag = lower.get(tag..).unwrap_or_default();
        if from_tag.starts_with("<!--") {
            at = from_tag.find("-->").map_or(html.len(), |e| tag + e + 3);
            continue;
        }
        let Some(close) = from_tag.find('>') else {
            words(html.get(tag..).unwrap_or_default(), pre > 0, &mut out);
            break;
        };
        let inside = from_tag.get(1..close).unwrap_or_default();
        let closing = inside.starts_with('/');
        let name: String = inside
            .trim_start_matches('/')
            .chars()
            .take_while(char::is_ascii_alphanumeric)
            .collect();
        if inside.starts_with('!') {
            // `<!doctype html>`: not part of the text.
            at = tag + close + 1;
            continue;
        }
        if !name.starts_with(|c: char| c.is_ascii_alphabetic()) {
            // `x < y`, `<3`: a sign in the text, not a tag.
            out.push('<');
            at = tag + 1;
            continue;
        }
        at = tag + close + 1;
        if !closing && SKIPPED.contains(&name.as_str()) && !inside.ends_with('/') {
            let end = format!("</{name}");
            at = match lower.get(at..).and_then(|r| r.find(&end)) {
                Some(e) => {
                    let from = at + e;
                    lower
                        .get(from..)
                        .and_then(|r| r.find('>'))
                        .map_or(html.len(), |g| from + g + 1)
                }
                None => html.len(),
            };
            continue;
        }
        let level = match name.as_bytes() {
            [b'h', d @ b'1'..=b'6'] => usize::from(d - b'0'),
            _ => 0,
        };
        match name.as_str() {
            "br" => out.push('\n'),
            "li" if !closing => out.push_str("\n- "),
            "pre" => {
                pre = if closing { pre.saturating_sub(1) } else { pre + 1 };
                out.push_str("\n\n");
            }
            "tr" if closing => out.push('\n'),
            "td" | "th" if closing => out.push(' '),
            _ if level > 0 => {
                out.push_str("\n\n");
                if !closing {
                    out.push_str(&"#".repeat(level));
                    out.push(' ');
                }
            }
            n if BLOCKS.contains(&n) => out.push_str("\n\n"),
            _ => {}
        }
    }
    tidy(&out)
}

/// Text between tags, its character references read and - outside `<pre>` - its runs of white space made one space.
fn words(text: &str, keep_spaces: bool, out: &mut String) {
    let mut rest = text;
    while let Some(c) = rest.chars().next() {
        let (c, used) = if c == '&' {
            reference(rest).unwrap_or(('&', 1))
        } else {
            (c, c.len_utf8())
        };
        rest = rest.get(used..).unwrap_or_default();
        if !keep_spaces && c.is_whitespace() {
            if !out.ends_with(|e: char| e.is_whitespace()) && !out.is_empty() {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
    }
}

/// The character a reference at the start of `text` stands for, and how many bytes it took.
fn reference(text: &str) -> Option<(char, usize)> {
    let end = text.get(..12.min(text.len()))?.find(';')?;
    let name = text.get(1..end)?;
    let c = match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" | "#39" => '\'',
        "nbsp" => ' ',
        _ => {
            let number = name.strip_prefix('#')?;
            let code = match number.strip_prefix(['x', 'X']) {
                Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                None => number.parse().ok()?,
            };
            char::from_u32(code).filter(|c| *c != '\0')?
        }
    };
    Some((c, end + 1))
}

/// Lines without the spaces at their ends, and no more than one blank line between paragraphs.
fn tidy(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut blank = 0;
    for line in text.lines() {
        let line = line.trim_end();
        let line = line
            .strip_prefix(' ')
            .filter(|l| !l.starts_with(' '))
            .unwrap_or(line);
        if line.is_empty() {
            blank += 1;
            continue;
        }
        if !out.is_empty() {
            out.push_str(if blank > 0 { "\n\n" } else { "\n" });
        }
        blank = 0;
        out.push_str(line);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_reads_as_its_text_with_its_headings_as_markdown() {
        let page = r#"<!doctype html><html><head><title>T</title><style>p{color:red}</style></head>
            <body><h1>Returns &amp; refunds</h1><p>Send it   back
            within <b>14 days</b>.</p><script>alert("<p>no</p>")</script><!-- a note -->
            <h2>How</h2><ul><li>Pack it</li><li>Post it</li></ul><p>Caf&#233; &lt;3 &#x2603; a &lt b</p></body></html>"#;
        assert_eq!(
            to_text(page),
            "# Returns & refunds\n\nSend it back within 14 days.\n\n## How\n\n- Pack it\n- Post it\n\nCafé <3 ☃ a &lt b"
        );
    }

    #[test]
    fn preformatted_text_keeps_its_spaces_and_broken_html_still_reads() {
        assert_eq!(to_text("<pre>a  b\n  c</pre>"), "a  b\n  c");
        assert_eq!(to_text("x < y and <b>bold"), "x < y and bold");
        assert_eq!(to_text("<p>open <script>never closed"), "open");
        assert_eq!(to_text("tail <a href='x'"), "tail <a href='x'");
        assert_eq!(to_text(""), "");
    }
}
