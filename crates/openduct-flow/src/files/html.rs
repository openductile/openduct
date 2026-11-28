// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A web page as text: what a reader sees - paragraphs and headings on lines of their own, list items as `- `, table
//! cells apart, entities decoded - without scripts, styles or markup. And its title.

/// Elements whose content is not text a reader sees.
const HIDDEN: &[&str] = &[
    "script", "style", "noscript", "template", "svg", "iframe", "object",
];
/// Elements that start a line of their own.
const BLOCKS: &[&str] = &[
    "p",
    "div",
    "br",
    "tr",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "table",
    "ul",
    "ol",
    "section",
    "article",
    "header",
    "footer",
    "nav",
    "aside",
    "main",
    "blockquote",
    "pre",
    "hr",
    "form",
    "fieldset",
    "address",
    "figure",
    "dl",
    "dt",
    "dd",
    "title",
    "body",
    "html",
];

/// A named entity's character: the ones pages use most.
fn entity(name: &str) -> Option<char> {
    Some(match name {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => ' ',
        "copy" => '©',
        "reg" => '®',
        "trade" => '™',
        "hellip" => '…',
        "mdash" => '—',
        "ndash" => '–',
        "lsquo" => '‘',
        "rsquo" => '’',
        "ldquo" => '“',
        "rdquo" => '”',
        "bull" => '•',
        "middot" => '·',
        "euro" => '€',
        "pound" => '£',
        "yen" => '¥',
        "cent" => '¢',
        "deg" => '°',
        "times" => '×',
        "divide" => '÷',
        "laquo" => '«',
        "raquo" => '»',
        "sect" => '§',
        "para" => '¶',
        _ => return None,
    })
}

/// Text with its character references decoded.
fn decode(text: &str, out: &mut String) {
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(rest.get(..at).unwrap_or_default());
        rest = rest.get(at..).unwrap_or_default();
        let end = rest.find(';').filter(|e| *e <= 10);
        let name = end.and_then(|e| rest.get(1..e)).unwrap_or_default();
        let found = name
            .strip_prefix("#x")
            .or_else(|| name.strip_prefix("#X"))
            .and_then(|h| u32::from_str_radix(h, 16).ok())
            .or_else(|| name.strip_prefix('#').and_then(|d| d.parse().ok()))
            .and_then(char::from_u32)
            .or_else(|| entity(name));
        match (found, end) {
            (Some(c), Some(e)) => {
                out.push(c);
                rest = rest.get(e + 1..).unwrap_or_default();
            }
            _ => {
                out.push('&');
                rest = rest.get(1..).unwrap_or_default();
            }
        }
    }
    out.push_str(rest);
}

/// The page's text, and its title (empty when it has none).
pub(super) fn to_text(html: &str) -> (String, String) {
    let mut out = String::new();
    let mut title = String::new();
    let (mut hidden, mut in_title, mut in_pre) = (0usize, false, 0usize);
    let mut rest = html;
    while !rest.is_empty() {
        let Some(open) = rest.find('<') else {
            if hidden == 0 {
                decode(rest, &mut out);
            }
            break;
        };
        let before = rest.get(..open).unwrap_or_default();
        if in_title {
            decode(before, &mut title);
        } else if hidden == 0 {
            if in_pre > 0 {
                decode(before, &mut out);
            } else {
                let mut decoded = String::new();
                decode(
                    &before.split_whitespace().collect::<Vec<_>>().join(" "),
                    &mut decoded,
                );
                if before.starts_with(char::is_whitespace)
                    && !out.ends_with([' ', '\n', '\t'])
                    && !out.is_empty()
                {
                    out.push(' ');
                }
                out.push_str(&decoded);
                if before.ends_with(char::is_whitespace) && !decoded.is_empty() {
                    out.push(' ');
                }
            }
        }
        rest = rest.get(open..).unwrap_or_default();
        if let Some(after) = rest.strip_prefix("<!--") {
            rest = after
                .find("-->")
                .and_then(|e| after.get(e + 3..))
                .unwrap_or_default();
            continue;
        }
        let Some(close) = rest.find('>') else { break };
        let tag = rest.get(1..close).unwrap_or_default();
        rest = rest.get(close + 1..).unwrap_or_default();
        let closing = tag.starts_with('/');
        let name: String = tag
            .trim_start_matches('/')
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase();
        if name == "title" {
            in_title = !closing;
            continue;
        }
        if HIDDEN.contains(&name.as_str()) {
            if closing {
                hidden = hidden.saturating_sub(1);
            } else if !tag.ends_with('/') {
                hidden += 1;
                // A script's or a style's content is not markup: it runs to its closing tag.
                if matches!(name.as_str(), "script" | "style") {
                    let end = format!("</{name}");
                    let at = rest.to_ascii_lowercase().find(&end).unwrap_or(rest.len());
                    rest = rest.get(at..).unwrap_or_default();
                }
            }
            continue;
        }
        if hidden > 0 {
            continue;
        }
        if name == "pre" {
            in_pre = if closing {
                in_pre.saturating_sub(1)
            } else {
                in_pre + 1
            };
        }
        if BLOCKS.contains(&name.as_str()) || name == "li" {
            while out.ends_with(' ') {
                out.pop();
            }
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            if name == "li" && !closing {
                out.push_str("- ");
            }
        } else if matches!(name.as_str(), "td" | "th") && !closing && !out.ends_with('\n') && !out.is_empty()
        {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push('\t');
        }
    }
    let lines: Vec<&str> = out.lines().map(str::trim_end).collect();
    let mut text = String::new();
    let mut blank = 0;
    for line in lines {
        if line.trim().is_empty() {
            blank += 1;
            if blank > 1 {
                continue;
            }
        } else {
            blank = 0;
        }
        text.push_str(line);
        text.push('\n');
    }
    (
        text.trim().to_owned(),
        title.split_whitespace().collect::<Vec<_>>().join(" "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_reads_as_its_reader_sees_it() {
        let page = "<!doctype html><html><head><title> Order\n 2041 </title><style>p { color: red }</style>\
                    <script>if (a < b) { alert('<p>no</p>') }</script></head><body>\
                    <h1>Thanks,&nbsp;Ada!</h1><p>Your order of <b>3 items</b> &amp; a gift is on its way.</p>\
                    <!-- not shown --><ul><li>Cap</li><li>Tote &#x2014; blue</li></ul>\
                    <table><tr><th>Item</th><th>Price</th></tr><tr><td>Cap</td><td>&euro;24</td></tr></table>\
                    <pre>  kept\n  as is</pre><p>Bye &unknown; now</p></body></html>";
        let (text, title) = to_text(page);
        assert_eq!(title, "Order 2041");
        assert_eq!(
            text,
            "Thanks, Ada!\nYour order of 3 items & a gift is on its way.\n- Cap\n- Tote — blue\nItem\tPrice\n\
             Cap\t€24\n  kept\n  as is\nBye &unknown; now"
        );
        assert_eq!(to_text("plain <b>text").0, "plain text");
        assert_eq!(to_text("<p>cut short <a href=").0, "cut short");
    }
}
