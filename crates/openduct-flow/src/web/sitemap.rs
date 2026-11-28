// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A site's sitemap: the addresses it lists, with when each last changed, how often it changes and its priority.
//! Given a site (`https://example.com/`), the sitemaps its robots.txt names are read, or its `/sitemap.xml`; given a
//! sitemap's own address, that one. A sitemap index's sitemaps are read in turn (at most [`MAX_SITEMAPS`], two
//! levels deep); gzipped sitemaps are unpacked and text sitemaps (one address a line) read too. Bounded: the
//! addresses given, the sitemaps read, and each one's bytes.

use std::collections::{HashSet, VecDeque};
use std::io::Read as _;

use reqwest::Url;

use super::fetch::{Ask, Limits, Net, get};
use super::page::Page;

/// The most sitemaps one step reads.
pub(crate) const MAX_SITEMAPS: usize = 50;
/// The most bytes of one sitemap, unpacked (the sitemap protocol's own limit).
const MAX_SITEMAP_BYTES: usize = 50 * 1024 * 1024;

/// An address a sitemap lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) url: String,
    pub(crate) modified: Option<String>,
    pub(crate) changes: Option<String>,
    pub(crate) priority: Option<String>,
}

/// What a sitemap (or an index of them) holds.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Listed {
    pub(crate) entries: Vec<Entry>,
    pub(crate) sitemaps: Vec<String>,
}

/// The text of an element's child called `name` (by its name without a prefix).
fn child_text(page: &Page, id: usize, name: &str) -> Option<String> {
    page.elements(id)
        .find(|c| page.name(*c).is_some_and(|n| local(n).eq_ignore_ascii_case(name)))
        .map(|c| page.text_of(c, &|_| false))
        .filter(|t| !t.is_empty())
}

/// A name without its prefix: `image:loc` is `loc`.
pub(crate) fn local(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, n)| n)
}

/// What one sitemap's text holds: the addresses it lists, or - for an index - the sitemaps it names.
pub(crate) fn parse(text: &str) -> (Vec<Entry>, Vec<String>) {
    let trimmed = text.trim_start_matches('\u{feff}').trim_start();
    if !trimmed.starts_with('<') {
        // A text sitemap: one address a line.
        let entries = trimmed
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("http://") || l.starts_with("https://"))
            .map(|l| Entry {
                url: l.to_owned(),
                modified: None,
                changes: None,
                priority: None,
            })
            .collect();
        return (entries, Vec::new());
    }
    let page = Page::parse(text, true);
    let (mut entries, mut sitemaps) = (Vec::new(), Vec::new());
    for id in 0..page.nodes.len() {
        let Some(name) = page.name(id).map(local) else {
            continue;
        };
        let parent = page
            .parent(id)
            .and_then(|p| page.name(p))
            .map(local)
            .unwrap_or_default();
        match (name, parent) {
            ("url", "urlset") => {
                if let Some(url) = child_text(&page, id, "loc") {
                    entries.push(Entry {
                        url,
                        modified: child_text(&page, id, "lastmod"),
                        changes: child_text(&page, id, "changefreq"),
                        priority: child_text(&page, id, "priority"),
                    });
                }
            }
            ("sitemap", "sitemapindex") => {
                if let Some(url) = child_text(&page, id, "loc") {
                    sitemaps.push(url);
                }
            }
            _ => {}
        }
    }
    (entries, sitemaps)
}

/// A body, unpacked when it is gzipped (by its first bytes), at most [`MAX_SITEMAP_BYTES`].
fn unpacked(body: Vec<u8>) -> Result<Vec<u8>, String> {
    if body.get(..2) != Some(&[0x1f, 0x8b]) {
        return Ok(body);
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(body.as_slice())
        .take(u64::try_from(MAX_SITEMAP_BYTES).unwrap_or(u64::MAX) + 1)
        .read_to_end(&mut out)
        .map_err(|e| format!("a gzipped sitemap cannot be read: {e}"))?;
    if out.len() > MAX_SITEMAP_BYTES {
        return Err(format!(
            "a sitemap of more than {MAX_SITEMAP_BYTES} bytes unpacked"
        ));
    }
    Ok(out)
}

/// Whether an address names a sitemap itself rather than a site.
fn is_sitemap(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".xml") || path.ends_with(".xml.gz") || path.ends_with(".txt") || path.contains("sitemap")
}

/// The sitemaps to start from, for `url`: the address itself, the ones its site's robots.txt names, or the site's
/// `/sitemap.xml`.
pub(crate) async fn starts(net: &Net, url: &Url, agent: &str) -> Vec<String> {
    if is_sitemap(url) {
        return vec![url.to_string()];
    }
    let origin = url.origin().ascii_serialization();
    let named: Vec<String> = super::robots::fetch(net, &origin, agent)
        .await
        .sitemaps
        .into_iter()
        .filter(|s| Url::parse(s).is_ok_and(|s| matches!(s.scheme(), "http" | "https")))
        .collect();
    if named.is_empty() {
        vec![format!("{origin}/sitemap.xml")]
    } else {
        named
    }
}

/// The addresses the sitemaps from `starts` list, at most `most`. A step's `headers` go to the first sitemap's site
/// alone.
pub(crate) async fn read(
    net: &Net,
    starts: Vec<String>,
    agent: &str,
    headers: &[(String, String)],
    limits: Limits,
    most: usize,
) -> Result<Listed, String> {
    let site = starts.first().and_then(|s| Url::parse(s).ok());
    let mut queue: VecDeque<(String, usize)> = starts.into_iter().map(|s| (s, 0)).collect();
    let mut seen: HashSet<String> = HashSet::new();
    let mut listed = Listed::default();
    let mut first_error = None;
    while let Some((at, level)) = queue.pop_front() {
        if listed.sitemaps.len() >= MAX_SITEMAPS || listed.entries.len() >= most {
            break;
        }
        if !seen.insert(at.clone()) {
            continue;
        }
        let own_site = site
            .as_ref()
            .zip(Url::parse(&at).ok())
            .is_some_and(|(a, b)| super::fetch::same_origin(a, &b));
        let ask = Ask {
            url: &at,
            agent,
            headers: if own_site { headers } else { &[] },
            accept: "application/xml, text/xml, text/plain;q=0.8, */*;q=0.5",
            limits: Limits {
                max_bytes: limits.max_bytes.clamp(1024 * 1024, MAX_SITEMAP_BYTES),
                ..limits
            },
            since: None,
        };
        let got = match get(net, &ask).await {
            Ok(g) if (200..300).contains(&g.status) => g,
            Ok(g) => {
                first_error.get_or_insert_with(|| format!("{at} answered {}", g.status));
                continue;
            }
            Err(e) => {
                first_error.get_or_insert(e);
                continue;
            }
        };
        listed.sitemaps.push(got.url.to_string());
        let text = super::page::decode(&unpacked(got.body)?, &got.content_type);
        let (entries, more) = parse(&text);
        for e in entries {
            if listed.entries.len() >= most {
                break;
            }
            listed.entries.push(e);
        }
        if level < 2 {
            queue.extend(more.into_iter().map(|s| (s, level + 1)));
        }
    }
    if listed.sitemaps.is_empty() {
        return Err(first_error.unwrap_or_else(|| "no sitemap found".to_owned()));
    }
    Ok(listed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sitemap_and_an_index_are_read() {
        let set = "<?xml version=\"1.0\"?><urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\
                   <url><loc> https://example.com/a </loc><lastmod>2026-11-01</lastmod><changefreq>weekly</changefreq>\
                   <priority>0.8</priority></url><url><loc>https://example.com/b?x=1&amp;y=2</loc></url><url></url></urlset>";
        let (entries, more) = parse(set);
        assert!(more.is_empty());
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].url, "https://example.com/a");
        assert_eq!(entries[0].modified.as_deref(), Some("2026-11-01"));
        assert_eq!(entries[0].priority.as_deref(), Some("0.8"));
        assert_eq!(entries[1].url, "https://example.com/b?x=1&y=2");
        let index = "<sitemapindex><sitemap><loc>https://example.com/s1.xml</loc></sitemap>\
                     <sitemap><loc>https://example.com/s2.xml.gz</loc></sitemap></sitemapindex>";
        assert_eq!(
            parse(index).1,
            ["https://example.com/s1.xml", "https://example.com/s2.xml.gz"]
        );
        let text = "https://example.com/1\n\nnot an address\nhttps://example.com/2\n";
        assert_eq!(parse(text).0.len(), 2);
        assert!(is_sitemap(
            &"https://example.com/sitemap_index.xml".parse().unwrap()
        ));
        assert!(!is_sitemap(&"https://example.com/".parse().unwrap()));
    }

    #[test]
    fn a_gzipped_sitemap_is_unpacked() {
        use std::io::Write as _;
        let mut packer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        packer.write_all(b"<urlset/>").unwrap();
        let packed = packer.finish().unwrap();
        assert_eq!(unpacked(packed).unwrap(), b"<urlset/>");
        assert_eq!(unpacked(b"plain".to_vec()).unwrap(), b"plain");
        assert!(unpacked(vec![0x1f, 0x8b, 1, 2]).is_err());
    }
}
