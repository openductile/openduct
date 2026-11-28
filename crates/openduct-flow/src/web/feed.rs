// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A feed's items: RSS 2.0, RSS 1.0 (RDF), Atom and JSON Feed, read into one shape - each item's id, title, link (at
//! its full address), date (as an ISO time, UTC), summary and content (as plain text, the HTML they often hold read
//! as a page is), author, categories and enclosure (a podcast's file, say). An item without an id is known by its
//! link, or by its title and date. Bounded: the items given, and each text's length.

use reqwest::Url;
use serde_json::{Value as Json, json};

use super::page::{Base, Options, Page, Part, cut_text};

/// The most items a feed gives, by default and at most.
pub(crate) const FEED_ITEMS: usize = 50;
pub(crate) const MOST_FEED_ITEMS: usize = 1_000;
/// The longest summary and content of an item, in characters.
const SUMMARY: usize = 2_000;
const CONTENT: usize = 20_000;

/// One item of a feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    pub(crate) date: Option<String>,
    pub(crate) summary: String,
    pub(crate) content: Option<String>,
    pub(crate) author: Option<String>,
    pub(crate) categories: Vec<String>,
    pub(crate) enclosure: Option<(String, String)>,
}

impl Item {
    pub(crate) fn value(&self) -> Json {
        json!({
            "id": self.id,
            "title": self.title,
            "link": self.link,
            "date": self.date,
            "summary": self.summary,
            "content": self.content,
            "author": self.author,
            "categories": self.categories,
            "enclosure": self.enclosure.as_ref().map(|(url, kind)| json!({ "url": url, "type": kind })),
        })
    }
}

/// A feed: what it says of itself, and its items in the order it gives them (newest first, as feeds mostly are).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Feed {
    pub(crate) title: String,
    pub(crate) link: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) items: Vec<Item>,
}

/// A time a feed gives, as an ISO time in UTC; as written when it cannot be read.
pub(crate) fn date(text: &str) -> Option<String> {
    let t = text.trim();
    if t.is_empty() {
        return None;
    }
    let ms = chrono::DateTime::parse_from_rfc3339(t)
        .or_else(|_| chrono::DateTime::parse_from_rfc2822(t))
        .or_else(|_| chrono::DateTime::parse_from_rfc2822(t.split_once(", ").map_or(t, |(_, rest)| rest)))
        .map(|d| d.timestamp_millis())
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S"))
                .ok()
                .map(|d| d.and_utc().timestamp_millis())
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|d| d.and_utc().timestamp_millis())
        });
    Some(ms.map_or_else(|| t.chars().take(100).collect(), crate::runner::iso))
}

/// Text that may hold HTML, as plain text, at most `most` characters.
fn plain(raw: &str, most: usize) -> String {
    let mut text = if raw.contains('<') && raw.contains('>') {
        let page = Page::parse(raw, false);
        let options = Options {
            part: Part::All,
            markdown: false,
            links: false,
            images: false,
            max_chars: None,
        };
        let base = "https://feed.invalid/".parse::<Url>().ok();
        match base {
            Some(base) => super::page::read(&page, &base, &options, &mut super::page::Budget(1_000_000))
                .map(|r| r.text)
                .unwrap_or_default(),
            None => String::new(),
        }
    } else {
        raw.split_whitespace().collect::<Vec<_>>().join(" ")
    };
    cut_text(&mut text, most);
    text
}

/// The text inside an element, as it is (a CDATA section's HTML too).
fn raw(page: &Page, id: usize) -> String {
    let mut out = String::new();
    for i in page.within(id) {
        if let Some(t) = page.text_node(i) {
            out.push_str(t);
        }
    }
    out
}

fn named(page: &Page, id: usize, names: &[&str]) -> bool {
    page.name(id)
        .is_some_and(|n| names.iter().any(|w| n.eq_ignore_ascii_case(w)))
}

/// The first child of `id` with one of `names` (written with their usual prefixes).
fn child(page: &Page, id: usize, names: &[&str]) -> Option<usize> {
    page.elements(id).find(|c| named(page, *c, names))
}

fn child_text(page: &Page, id: usize, names: &[&str]) -> Option<String> {
    child(page, id, names)
        .map(|c| page.text_of(c, &|_| false))
        .filter(|t| !t.is_empty())
}

/// An item's link: an RSS `<link>`'s text, or an Atom `<link>`'s address (the one for people, not its enclosure).
fn link_of(page: &Page, id: usize, base: &Base) -> Option<String> {
    let links: Vec<usize> = page.elements(id).filter(|c| named(page, *c, &["link"])).collect();
    for l in &links {
        let text = page.text_of(*l, &|_| false);
        if !text.is_empty() {
            return base.page(&text);
        }
    }
    links
        .iter()
        .find(|l| {
            page.attr(**l, "rel")
                .is_none_or(|r| r.eq_ignore_ascii_case("alternate"))
        })
        .and_then(|l| page.attr(*l, "href"))
        .and_then(|h| base.page(h))
}

fn item_of(page: &Page, id: usize, base: &Base, atom: bool) -> Item {
    let title = child(page, id, &["title"])
        .map(|t| plain(&raw(page, t), 1_000))
        .unwrap_or_default();
    let link = link_of(page, id, base);
    let date = child_text(
        page,
        id,
        &["pubDate", "published", "updated", "dc:date", "issued", "modified"],
    )
    .and_then(|d| date(&d));
    let content = child(
        page,
        id,
        &[
            "content:encoded",
            if atom { "content" } else { "content:encoded" },
        ],
    )
    .map(|c| plain(&raw(page, c), CONTENT))
    .filter(|c| !c.is_empty());
    let summary = child(page, id, &["description", "summary", "dc:description"])
        .map(|s| plain(&raw(page, s), SUMMARY))
        .filter(|s| !s.is_empty())
        .or_else(|| content.as_ref().map(|c| plain(c, SUMMARY)))
        .unwrap_or_default();
    let author = child(page, id, &["author", "dc:creator", "creator"]).and_then(|a| {
        child_text(page, a, &["name"])
            .or_else(|| Some(page.text_of(a, &|_| false)))
            .filter(|t| !t.is_empty())
    });
    let categories = page
        .elements(id)
        .filter(|c| named(page, *c, &["category", "dc:subject"]))
        .filter_map(|c| {
            page.attr(c, "term")
                .map(str::to_owned)
                .or_else(|| Some(page.text_of(c, &|_| false)))
                .filter(|t| !t.is_empty())
        })
        .take(20)
        .collect();
    let enclosure = page
        .elements(id)
        .find(|c| {
            named(page, *c, &["enclosure"])
                || (named(page, *c, &["link"]) && page.attr(*c, "rel") == Some("enclosure"))
        })
        .and_then(|e| {
            let url = page.attr(e, "url").or_else(|| page.attr(e, "href"))?;
            Some((
                base.link(url)?,
                page.attr(e, "type").unwrap_or_default().to_owned(),
            ))
        });
    let id_text = child_text(page, id, &["guid", "id"])
        .or_else(|| page.attr(id, "rdf:about").map(str::to_owned))
        .or_else(|| link.clone())
        .unwrap_or_else(|| {
            let digest = ring::digest::digest(
                &ring::digest::SHA256,
                format!("{title}\n{}", date.clone().unwrap_or_default()).as_bytes(),
            );
            digest
                .as_ref()
                .iter()
                .take(12)
                .map(|b| format!("{b:02x}"))
                .collect()
        });
    Item {
        id: id_text.chars().take(500).collect(),
        title,
        link,
        date,
        summary,
        content,
        author,
        categories,
        enclosure,
    }
}

fn json_feed(text: &str, base: &Base, most: usize) -> Result<Feed, String> {
    let j: Json =
        serde_json::from_str(text).map_err(|e| format!("not a feed: its JSON cannot be read ({e})"))?;
    if !j
        .get("version")
        .and_then(Json::as_str)
        .is_some_and(|v| v.contains("jsonfeed"))
    {
        return Err("not a feed: JSON without a JSON Feed version".to_owned());
    }
    let s = |v: &Json, k: &str| v.get(k).and_then(Json::as_str).map(str::to_owned);
    let items =
        j.get("items")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
            .take(most)
            .map(|i| {
                let link = s(i, "url").and_then(|u| base.page(&u));
                let content = s(i, "content_html")
                    .or_else(|| s(i, "content_text"))
                    .map(|c| plain(&c, CONTENT));
                Item {
                    id: s(i, "id").or_else(|| link.clone()).unwrap_or_default(),
                    title: s(i, "title").map(|t| plain(&t, 1_000)).unwrap_or_default(),
                    date: s(i, "date_published")
                        .or_else(|| s(i, "date_modified"))
                        .and_then(|d| date(&d)),
                    summary: s(i, "summary")
                        .map(|t| plain(&t, SUMMARY))
                        .or_else(|| content.as_ref().map(|c| plain(c, SUMMARY)))
                        .unwrap_or_default(),
                    content,
                    author: i
                        .get("authors")
                        .and_then(|a| a.get(0))
                        .or_else(|| i.get("author"))
                        .and_then(|a| s(a, "name")),
                    categories: i
                        .get("tags")
                        .and_then(Json::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Json::as_str)
                        .take(20)
                        .map(str::to_owned)
                        .collect(),
                    enclosure: i.get("attachments").and_then(|a| a.get(0)).and_then(|a| {
                        Some((base.link(&s(a, "url")?)?, s(a, "mime_type").unwrap_or_default()))
                    }),
                    link,
                }
            })
            .collect();
    Ok(Feed {
        title: s(&j, "title").unwrap_or_default(),
        link: s(&j, "home_page_url").and_then(|u| base.page(&u)),
        description: s(&j, "description"),
        items,
    })
}

/// A feed's text, at `url`: its items, at most `most`.
pub(crate) fn parse(text: &str, url: &Url, most: usize) -> Result<Feed, String> {
    let base = Base::new(url.clone());
    let trimmed = text.trim_start_matches('\u{feff}').trim_start();
    if trimmed.starts_with('{') {
        return json_feed(trimmed, &base, most);
    }
    let page = Page::parse(trimmed, true);
    let root = page
        .elements(0)
        .next()
        .ok_or("not a feed: no RSS, Atom or JSON Feed in it")?;
    let kind = page.name(root).unwrap_or_default().to_ascii_lowercase();
    let (head, items, atom): (Option<usize>, Vec<usize>, bool) = match kind.as_str() {
        "rss" => {
            let channel = child(&page, root, &["channel"]);
            let items = channel.map(|c| page.elements(c).filter(|i| named(&page, *i, &["item"])).collect());
            (channel, items.unwrap_or_default(), false)
        }
        "rdf:rdf" | "rdf" => {
            let channel = child(&page, root, &["channel"]);
            let items = page
                .elements(root)
                .chain(channel.into_iter().flat_map(|c| page.elements(c)))
                .filter(|i| named(&page, *i, &["item"]))
                .collect();
            (channel, items, false)
        }
        "feed" => (
            Some(root),
            page.elements(root)
                .filter(|i| named(&page, *i, &["entry"]))
                .collect(),
            true,
        ),
        "html" => return Err("not a feed: a web page (fetch it, or give the feed's own address)".to_owned()),
        other => {
            return Err(format!(
                "not a feed: its top element is <{other}>, not RSS or Atom"
            ));
        }
    };
    let (title, link, description) = match head {
        Some(h) => (
            child(&page, h, &["title"])
                .map(|t| plain(&raw(&page, t), 1_000))
                .unwrap_or_default(),
            link_of(&page, h, &base),
            child(&page, h, &["description", "subtitle"]).map(|d| plain(&raw(&page, d), SUMMARY)),
        ),
        None => (String::new(), None, None),
    };
    Ok(Feed {
        title,
        link,
        description,
        items: items
            .into_iter()
            .take(most)
            .map(|i| item_of(&page, i, &base, atom))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_are_read_in_the_forms_feeds_write_them() {
        assert_eq!(
            date("Fri, 27 Nov 2026 09:30:00 GMT").unwrap(),
            crate::runner::iso(1_795_771_800_000)
        );
        assert_eq!(
            date("27 Nov 2026 09:30:00 +0000").unwrap(),
            crate::runner::iso(1_795_771_800_000)
        );
        assert_eq!(
            date("2026-11-27T10:30:00+01:00").unwrap(),
            crate::runner::iso(1_795_771_800_000)
        );
        assert_eq!(date("2026-11-27").unwrap(), crate::runner::iso(1_795_737_600_000));
        assert_eq!(date("last Tuesday").unwrap(), "last Tuesday");
        assert_eq!(date("  "), None);
    }

    #[test]
    fn a_json_feed_is_read() {
        let text = r#"{"version": "https://jsonfeed.org/version/1.1", "title": "Notes", "items": [
            {"id": "1", "url": "/n/1", "title": "One", "content_html": "<p>Hello <b>there</b></p>",
             "date_published": "2026-11-27T09:30:00Z", "authors": [{"name": "Ada"}], "tags": ["a"]}]}"#;
        let feed = parse(text, &"https://notes.example/feed.json".parse().unwrap(), 10).unwrap();
        assert_eq!(feed.title, "Notes");
        let item = &feed.items[0];
        assert_eq!(item.link.as_deref(), Some("https://notes.example/n/1"));
        assert_eq!(item.summary, "Hello there");
        assert_eq!(item.author.as_deref(), Some("Ada"));
        assert!(parse("{\"a\":1}", &"https://x.example/".parse().unwrap(), 10).is_err());
        assert!(
            parse(
                "<html><body>x</body></html>",
                &"https://x.example/".parse().unwrap(),
                10
            )
            .unwrap_err()
            .contains("a web page")
        );
    }
}
