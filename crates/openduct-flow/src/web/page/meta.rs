// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What a page says of itself - its title, description, language, canonical address, site, author, date, picture,
//! the feeds it offers and whether it asks not to be followed - and its links and pictures, at their full addresses
//! (read against the page's address, or its `<base>`). And a page's bytes read as text in the character set it is in.

use reqwest::Url;

use super::dom::Page;

/// The most links and pictures a page gives.
pub(crate) const MAX_LINKS: usize = 1_000;
pub(crate) const MAX_IMAGES: usize = 500;
/// The longest address kept.
const MAX_ADDRESS: usize = 4_096;

/// The address relative ones are read against.
#[derive(Debug, Clone)]
pub(crate) struct Base(Url);

impl Base {
    pub(crate) fn new(url: Url) -> Self {
        Self(url)
    }

    /// The page's own address and its `<base href>`, when it has one.
    pub(crate) fn of(page: &Page, url: &Url) -> Self {
        let given = page
            .first("base")
            .and_then(|b| page.attr(b, "href"))
            .and_then(|h| url.join(h.trim()).ok())
            .filter(|u| matches!(u.scheme(), "http" | "https"));
        Self(given.unwrap_or_else(|| url.clone()))
    }

    /// A link's full address: `None` for a script's (`javascript:`), a data address, or what is not an address.
    pub(crate) fn link(&self, href: &str) -> Option<String> {
        let href = href.trim();
        if href.is_empty() || href.len() > MAX_ADDRESS {
            return None;
        }
        let lower = href
            .get(..11.min(href.len()))
            .unwrap_or_default()
            .to_ascii_lowercase();
        if lower.starts_with("javascript:") || lower.starts_with("data:") || lower.starts_with("vbscript:") {
            return None;
        }
        self.0.join(href).ok().map(String::from)
    }

    /// A page's full address to fetch: http or https, its `#fragment` left off.
    pub(crate) fn page(&self, href: &str) -> Option<String> {
        let mut url = Url::parse(&self.link(href)?).ok()?;
        if !matches!(url.scheme(), "http" | "https") {
            return None;
        }
        url.set_fragment(None);
        Some(String::from(url))
    }
}

/// What a page says of itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Meta {
    pub(crate) title: String,
    pub(crate) description: Option<String>,
    pub(crate) language: Option<String>,
    pub(crate) canonical: Option<String>,
    pub(crate) site_name: Option<String>,
    pub(crate) author: Option<String>,
    pub(crate) published: Option<String>,
    pub(crate) image: Option<String>,
    /// The feeds it offers (`<link rel="alternate" type="application/rss+xml">`).
    pub(crate) feeds: Vec<String>,
    /// It asks that its links not be followed (`<meta name="robots" content="nofollow">`).
    pub(crate) nofollow: bool,
}

fn words(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The content of the first `<meta>` whose `name` or `property` is one of `names`.
fn meta_of(page: &Page, names: &[&str]) -> Option<String> {
    for wanted in names {
        for id in page.all("meta") {
            let named = page
                .attr(id, "name")
                .or_else(|| page.attr(id, "property"))
                .or_else(|| page.attr(id, "itemprop"))
                .unwrap_or_default();
            if named.eq_ignore_ascii_case(wanted)
                && let Some(content) = page.attr(id, "content").map(words).filter(|c| !c.is_empty())
            {
                return Some(content);
            }
        }
    }
    None
}

/// What `page` (at `base`) says of itself.
pub(crate) fn meta(page: &Page, base: &Base) -> Meta {
    let title = page
        .first("title")
        .map(|t| page.text_of(t, &|_| false))
        .filter(|t| !t.is_empty())
        .or_else(|| meta_of(page, &["og:title", "twitter:title"]))
        .or_else(|| page.first("h1").map(|h| page.text_of(h, &|_| false)))
        .unwrap_or_default();
    let language = page
        .first("html")
        .and_then(|h| page.attr(h, "lang").or_else(|| page.attr(h, "xml:lang")))
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            page.all("meta")
                .find(|m| {
                    page.attr(*m, "http-equiv")
                        .is_some_and(|h| h.eq_ignore_ascii_case("content-language"))
                })
                .and_then(|m| page.attr(m, "content"))
                .map(|c| c.trim().to_owned())
        });
    let link_rel = |rel: &str| {
        page.all("link").find(|l| {
            page.attr(*l, "rel")
                .is_some_and(|r| r.split_whitespace().any(|w| w.eq_ignore_ascii_case(rel)))
        })
    };
    let canonical = link_rel("canonical")
        .and_then(|l| page.attr(l, "href"))
        .and_then(|h| base.page(h))
        .or_else(|| meta_of(page, &["og:url"]).and_then(|u| base.page(&u)));
    let feeds = page
        .all("link")
        .filter(|l| {
            page.attr(*l, "rel")
                .is_some_and(|r| r.split_whitespace().any(|w| w.eq_ignore_ascii_case("alternate")))
                && page.attr(*l, "type").is_some_and(|t| {
                    let t = t.to_ascii_lowercase();
                    t.contains("rss") || t.contains("atom") || t.contains("feed+json")
                })
        })
        .filter_map(|l| page.attr(l, "href").and_then(|h| base.page(h)))
        .take(10)
        .collect();
    let nofollow = page.all("meta").any(|m| {
        page.attr(m, "name")
            .is_some_and(|n| n.eq_ignore_ascii_case("robots"))
            && page.attr(m, "content").is_some_and(|c| {
                c.to_ascii_lowercase()
                    .split([',', ' '])
                    .any(|w| w == "nofollow" || w == "none")
            })
    });
    Meta {
        title,
        description: meta_of(page, &["description", "og:description", "twitter:description"]),
        language,
        canonical,
        site_name: meta_of(page, &["og:site_name", "application-name"]),
        author: meta_of(page, &["author", "article:author", "twitter:creator"]),
        published: meta_of(
            page,
            &[
                "article:published_time",
                "date",
                "dc.date",
                "datePublished",
                "pubdate",
            ],
        ),
        image: meta_of(page, &["og:image", "twitter:image"]).and_then(|i| base.link(&i)),
        feeds,
        nofollow,
    }
}

/// The page's links to other pages: each address once (without its `#fragment`), with its text, in order.
pub(crate) fn links(page: &Page, base: &Base, hidden: &dyn Fn(usize) -> bool) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for id in page.all("a").chain(page.all("area")) {
        if out.len() >= MAX_LINKS {
            break;
        }
        if hidden(id) {
            continue;
        }
        let Some(url) = page.attr(id, "href").and_then(|h| base.page(h)) else {
            continue;
        };
        if seen.insert(url.clone()) {
            let text = page.text_of(id, &|_| false);
            let text = if text.is_empty() {
                page.attr(id, "title")
                    .or_else(|| page.attr(id, "aria-label"))
                    .unwrap_or_default()
                    .to_owned()
            } else {
                text
            };
            out.push((url, text));
        }
    }
    out
}

/// The page's pictures: each address once, with what it says it shows (`alt`).
pub(crate) fn images(page: &Page, base: &Base, hidden: &dyn Fn(usize) -> bool) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for id in page.all("img") {
        if out.len() >= MAX_IMAGES {
            break;
        }
        if hidden(id) {
            continue;
        }
        let src = page
            .attr(id, "src")
            .filter(|s| !s.trim().is_empty() && !s.trim_start().starts_with("data:"))
            .or_else(|| page.attr(id, "data-src"))
            .and_then(|s| base.link(s))
            .filter(|s| s.starts_with("http"));
        if let Some(src) = src
            && seen.insert(src.clone())
        {
            out.push((src, words(page.attr(id, "alt").unwrap_or_default())));
        }
    }
    out
}

/// The character set markup says it is in: `<meta charset>`, `<meta http-equiv="Content-Type">` or an XML
/// declaration, in its first bytes.
fn declared(bytes: &[u8]) -> Option<String> {
    let head =
        String::from_utf8_lossy(bytes.get(..2048.min(bytes.len())).unwrap_or_default()).to_ascii_lowercase();
    let after = |key: &str| -> Option<String> {
        let at = head.find(key)? + key.len();
        let rest = head.get(at..)?.trim_start_matches(['=', ' ', '"', '\'']);
        let label: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | ':' | '.'))
            .collect();
        (!label.is_empty()).then_some(label)
    };
    if head.trim_start().starts_with("<?xml") {
        let decl = head.get(..head.find("?>").unwrap_or(head.len()))?;
        let at = decl.find("encoding")? + "encoding".len();
        let label: String = decl
            .get(at..)?
            .trim_start_matches(['=', ' ', '"', '\''])
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
            .collect();
        return (!label.is_empty()).then_some(label);
    }
    after("charset")
}

/// A page's bytes as text: in the character set its answer names, or its markup declares, or UTF-8; a byte order
/// mark wins over both. What cannot be read is shown as U+FFFD.
pub(crate) fn decode(bytes: &[u8], content_type: &str) -> String {
    let named = content_type
        .split(';')
        .map(str::trim)
        .filter(|p| p.get(..8).is_some_and(|k| k.eq_ignore_ascii_case("charset=")))
        .filter_map(|p| p.get(8..))
        .map(|c| c.trim_matches(['"', '\'']).to_owned())
        .next();
    let encoding = named
        .or_else(|| declared(bytes))
        .and_then(|label| encoding_rs::Encoding::for_label(label.as_bytes()))
        .unwrap_or(encoding_rs::UTF_8);
    // A page that says it is UTF-16 in its markup cannot be: its markup would not have been readable.
    let encoding = if encoding == encoding_rs::UTF_16LE || encoding == encoding_rs::UTF_16BE {
        encoding_rs::UTF_8
    } else {
        encoding
    };
    let (text, _, _) = encoding.decode(bytes);
    text.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"<html lang="en-GB"><head><title> Caps &amp; hats
        </title><base href="/shop/"><meta name="description" content=" Warm caps. "><meta property="og:site_name"
        content="Hat Co"><link rel="canonical" href="caps?page=1#top"><meta property="og:image" content="/i/og.png">
        <link rel="alternate" type="application/rss+xml" href="/feed.xml"><meta name="robots" content="index, nofollow">
        <meta name="author" content="Ada"></head><body><a href="cap">Cap</a> <a href="cap#more">again</a>
        <a href="javascript:go()">Go</a><a href="mailto:a@b.example">Mail</a><a href="https://other.example/x" title="X"></a>
        <img src="cap.png" alt=" A  cap "><img src="data:image/png;base64,AAAA" data-src="lazy.png"><img src="cap.png"></body></html>"#;

    #[test]
    fn a_page_says_what_it_is_and_its_addresses_are_whole() {
        let page = Page::parse(PAGE, false);
        let base = Base::of(&page, &"https://hats.example/a/b".parse().unwrap());
        let m = meta(&page, &base);
        assert_eq!(m.title, "Caps & hats");
        assert_eq!(m.description.as_deref(), Some("Warm caps."));
        assert_eq!(m.language.as_deref(), Some("en-GB"));
        assert_eq!(
            m.canonical.as_deref(),
            Some("https://hats.example/shop/caps?page=1")
        );
        assert_eq!(m.site_name.as_deref(), Some("Hat Co"));
        assert_eq!(m.author.as_deref(), Some("Ada"));
        assert_eq!(m.image.as_deref(), Some("https://hats.example/i/og.png"));
        assert_eq!(m.feeds, ["https://hats.example/feed.xml"]);
        assert!(m.nofollow);
        let links = links(&page, &base, &|_| false);
        assert_eq!(
            links,
            [
                ("https://hats.example/shop/cap".to_owned(), "Cap".to_owned()),
                ("https://other.example/x".to_owned(), "X".to_owned()),
            ]
        );
        let images = images(&page, &base, &|_| false);
        assert_eq!(
            images,
            [
                ("https://hats.example/shop/cap.png".to_owned(), "A cap".to_owned()),
                ("https://hats.example/shop/lazy.png".to_owned(), String::new()),
            ]
        );
    }

    #[test]
    fn bytes_are_read_in_the_character_set_they_are_in() {
        assert_eq!(decode("café".as_bytes(), "text/html"), "café");
        assert_eq!(decode(b"caf\xe9", "text/html; charset=ISO-8859-1"), "café");
        assert_eq!(
            decode(b"<meta charset=\"windows-1252\">\x93hi\x94", ""),
            "<meta charset=\"windows-1252\">\u{201c}hi\u{201d}"
        );
        assert_eq!(
            decode(b"<?xml version='1.0' encoding='iso-8859-1'?><a>\xe9</a>", ""),
            "<?xml version='1.0' encoding='iso-8859-1'?><a>é</a>"
        );
        assert_eq!(decode(b"\xef\xbb\xbfhi", "text/html; charset=windows-1252"), "hi");
        assert_eq!(decode(b"bad \xff", ""), "bad \u{fffd}");
    }
}
