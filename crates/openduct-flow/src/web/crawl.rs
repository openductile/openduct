// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A crawl: pages read from a start address outward, nearest first - its links followed on the same site (`www.`
//! or not), or those matching the step's patterns - to a depth and a number of pages, and within a time. Politely:
//! the site's robots.txt is honoured unless the step says not to (its rules for this crawler, and its
//! `Crawl-delay`), one page at a time, at least `rate` apart on one host, and a page that asks not to be followed
//! (`<meta name="robots" content="nofollow">`) is read but its links are not. The site's sitemap can add the pages
//! it lists. Files that are not pages (PDFs, pictures, archives ...) are not fetched. Every page is given as a fetch
//! gives it; each one passed by is listed with why.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use reqwest::Url;
use serde_json::{Value as Json, json};

use super::fetch::{Ask, Limits, Net, get};
use super::page::{Budget, Page, links_of};
use super::robots::Robots;

/// The most pages a crawl reads, by default and at most; how deep it goes, by default and at most.
pub(crate) const CRAWL_PAGES: usize = 10;
pub(crate) const MOST_CRAWL_PAGES: usize = 200;
pub(crate) const CRAWL_DEPTH: u32 = 1;
pub(crate) const MOST_CRAWL_DEPTH: u32 = 5;
/// The least and default time between two pages of one host; how long a crawl takes, by default and at most.
pub(crate) const LEAST_RATE: Duration = Duration::from_millis(100);
pub(crate) const CRAWL_RATE: Duration = Duration::from_secs(1);
pub(crate) const CRAWL_TIME: Duration = Duration::from_secs(120);
pub(crate) const MOST_CRAWL_TIME: Duration = Duration::from_secs(600);
/// The most characters of text a crawl gives, its pages together: it stops there.
const MOST_TEXT: usize = 8 * 1024 * 1024;
/// The most addresses a crawl keeps in mind, seen or waiting.
const MOST_SEEN: usize = 20_000;
/// The most pages passed by that are listed.
const MOST_SKIPPED: usize = 200;
/// Endings of addresses that are not pages.
const NOT_PAGES: &[&str] = &[
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp", ".tif", ".tiff", ".zip", ".gz",
    ".tgz", ".rar", ".7z", ".tar", ".mp3", ".mp4", ".m4a", ".mov", ".avi", ".webm", ".wav", ".ogg", ".css",
    ".js", ".mjs", ".woff", ".woff2", ".ttf", ".otf", ".eot", ".csv", ".xls", ".xlsx", ".doc", ".docx",
    ".ppt", ".pptx", ".exe", ".dmg", ".iso", ".apk", ".msi", ".bin", ".json", ".xml", ".rss", ".atom",
];

/// What a crawl does.
#[derive(Debug, Clone)]
pub(crate) struct Crawl {
    pub(crate) depth: u32,
    pub(crate) max_pages: usize,
    /// Patterns of the addresses followed (`*` any characters); none: the start's site.
    pub(crate) include: Vec<String>,
    pub(crate) exclude: Vec<String>,
    pub(crate) sitemap: bool,
    pub(crate) robots: bool,
    pub(crate) rate: Duration,
    pub(crate) time: Duration,
}

/// What each page of a crawl is fetched and read with.
pub(crate) struct Reading<'a> {
    pub(crate) agent: &'a str,
    pub(crate) limits: Limits,
    /// A page as a fetch gives it.
    pub(crate) value: &'a (dyn Fn(&super::fetch::Got, &Page, &mut Budget) -> Result<Json, String> + Sync),
}

/// Whether `text` matches `pattern`, where `*` stands for any characters.
pub(crate) fn glob(pattern: &str, text: &str) -> bool {
    // With `*` the only wildcard, the pieces between them are found in order, each as early as it can be: linear in
    // the text for each piece, however the pattern is written.
    let pieces: Vec<&str> = pattern.split('*').collect();
    let [first, middle @ .., last] = pieces.as_slice() else {
        return pattern == text;
    };
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    for piece in middle {
        match rest.find(piece) {
            Some(at) => rest = rest.get(at + piece.len()..).unwrap_or_default(),
            None => return false,
        }
    }
    rest.ends_with(last)
}

/// A site's name: its host, without `www.`.
fn site_of(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    host.strip_prefix("www.").map(str::to_owned).unwrap_or(host)
}

fn is_page(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    !NOT_PAGES.iter().any(|e| path.ends_with(e))
}

fn follows(url: &Url, site: &str, c: &Crawl) -> bool {
    let text = url.as_str();
    if !matches!(url.scheme(), "http" | "https") || !is_page(url) || c.exclude.iter().any(|p| glob(p, text)) {
        return false;
    }
    if c.include.is_empty() {
        site_of(url) == site
    } else {
        c.include.iter().any(|p| glob(p, text))
    }
}

struct Crawler<'a> {
    net: &'a Net,
    c: &'a Crawl,
    reading: &'a Reading<'a>,
    deadline: Instant,
    site: String,
    robots: HashMap<String, Robots>,
    last: HashMap<String, Instant>,
    skipped: Vec<Json>,
}

impl Crawler<'_> {
    fn skip(&mut self, url: &str, why: &str) {
        if self.skipped.len() < MOST_SKIPPED {
            self.skipped.push(json!({ "url": url, "why": why }));
        }
    }

    /// Waits until a request to `url`'s host is polite again; false when the crawl's time runs out first.
    async fn wait_for(&mut self, url: &Url) -> bool {
        let host = url.host_str().unwrap_or_default().to_owned();
        let delay = self
            .robots
            .get(&url.origin().ascii_serialization())
            .and_then(|r| r.delay)
            .filter(|_| self.c.robots)
            .map_or(self.c.rate, |d| self.c.rate.max(Duration::from_secs_f64(d)));
        if let Some(last) = self.last.get(&host) {
            let at = *last + delay;
            if at >= self.deadline {
                return false;
            }
            tokio::time::sleep_until(at.into()).await;
        }
        self.last.insert(host, Instant::now());
        true
    }

    /// Whether the site's robots.txt lets this crawler read `url`.
    async fn allowed(&mut self, url: &Url) -> bool {
        if !self.c.robots {
            return true;
        }
        let origin = url.origin().ascii_serialization();
        if !self.robots.contains_key(&origin) {
            // Read once a site, just before its first page, as crawlers do.
            let rules = super::robots::fetch(self.net, &origin, self.reading.agent).await;
            self.robots.insert(origin.clone(), rules);
        }
        let path = match url.query() {
            Some(q) => format!("{}?{q}", url.path()),
            None => url.path().to_owned(),
        };
        self.robots.get(&origin).is_some_and(|r| r.allows(&path))
    }
}

/// The crawl from `start`.
pub(crate) async fn crawl(net: &Net, start: &Url, c: &Crawl, reading: &Reading<'_>) -> Result<Json, String> {
    let started = Instant::now();
    let mut crawler = Crawler {
        net,
        c,
        reading,
        deadline: started + c.time,
        site: site_of(start),
        robots: HashMap::new(),
        last: HashMap::new(),
        skipped: Vec::new(),
    };
    let mut queue: VecDeque<(String, u32)> = VecDeque::from([(start.to_string(), 0)]);
    let mut seen: HashSet<String> = HashSet::from([start.to_string()]);
    if c.sitemap {
        let starts = super::sitemap::starts(net, start, reading.agent).await;
        let most = (c.max_pages * 5).min(MOST_SEEN);
        if let Ok(listed) = super::sitemap::read(net, starts, reading.agent, &[], reading.limits, most).await
        {
            for entry in listed.entries {
                if let Ok(url) = Url::parse(&entry.url)
                    && follows(&url, &crawler.site, c)
                    && seen.insert(url.to_string())
                {
                    queue.push_back((url.to_string(), 1));
                }
            }
        }
    }
    let mut pages = Vec::new();
    let mut read: HashSet<String> = HashSet::new();
    let mut text_size = 0usize;
    let mut stopped: Option<&str> = None;
    let mut budget = Budget(super::page::BUDGET);
    while let Some((at, depth)) = queue.pop_front() {
        if pages.len() >= c.max_pages {
            stopped = Some("max_pages");
            break;
        }
        if Instant::now() >= crawler.deadline {
            stopped = Some("time");
            break;
        }
        let Ok(url) = Url::parse(&at) else { continue };
        if !crawler.allowed(&url).await {
            crawler.skip(&at, "robots.txt does not allow it");
            continue;
        }
        if !crawler.wait_for(&url).await {
            stopped = Some("time");
            break;
        }
        let ask = Ask {
            url: &at,
            agent: reading.agent,
            headers: &[],
            accept: super::ACCEPT_PAGE,
            limits: reading.limits,
            since: None,
        };
        let left = crawler.deadline.saturating_duration_since(Instant::now());
        let got = match tokio::time::timeout(left, get(net, &ask)).await {
            Ok(Ok(g)) => g,
            Ok(Err(e)) => {
                crawler.skip(&at, &e);
                continue;
            }
            Err(_) => {
                stopped = Some("time");
                break;
            }
        };
        if got.status >= 400 {
            crawler.skip(&at, &format!("answered {}", got.status));
            continue;
        }
        if !got.is_html() {
            crawler.skip(&at, &format!("not a web page ({})", got.media_type()));
            continue;
        }
        if !read.insert(got.url.to_string()) {
            continue;
        }
        let text = super::page::decode(&got.body, &got.content_type);
        let page = Page::parse(&text, false);
        let mut value = match (reading.value)(&got, &page, &mut budget) {
            Ok(v) => v,
            Err(e) => {
                crawler.skip(&at, &e);
                continue;
            }
        };
        if let Some(record) = value.as_object_mut() {
            record.insert("depth".to_owned(), json!(depth));
            text_size += record.get("text").and_then(Json::as_str).map_or(0, str::len);
        }
        pages.push(value);
        if text_size > MOST_TEXT {
            stopped = Some("size");
            break;
        }
        let (links, nofollow) = links_of(&page, &got.url);
        if depth >= c.depth || (nofollow && c.robots) {
            continue;
        }
        for link in links {
            if seen.len() >= MOST_SEEN {
                break;
            }
            if let Ok(next) = Url::parse(&link)
                && follows(&next, &crawler.site, c)
                && seen.insert(link.clone())
            {
                queue.push_back((link, depth + 1));
            }
        }
    }
    Ok(json!({
        "url": start.as_str(),
        "count": pages.len(),
        "pages": pages,
        "skipped": crawler.skipped,
        "stopped": stopped,
        "ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_and_the_site_say_what_is_followed() {
        assert!(glob("https://example.com/docs/*", "https://example.com/docs/a/b"));
        assert!(glob("*/blog/*", "https://x.example/blog/1"));
        assert!(!glob("*/blog/*", "https://x.example/news/1"));
        assert!(glob("*.html", "https://x.example/a.html"));
        assert!(glob("a*b*c", "aXXbYYc"));
        assert!(!glob("a*b*c", "aXXbYY"));
        assert!(!glob("a*a", "a"));
        assert!(glob("exact", "exact") && !glob("exact", "exactly"));
        // A pattern made to be slow is not: each piece is looked for once.
        let slow = "*a".repeat(250);
        assert!(!glob(&slow, &format!("{}b", "a".repeat(4_000))));
        let c = Crawl {
            depth: 1,
            max_pages: 10,
            include: Vec::new(),
            exclude: vec!["*/login*".to_owned()],
            sitemap: false,
            robots: true,
            rate: CRAWL_RATE,
            time: CRAWL_TIME,
        };
        let site = site_of(&"https://www.example.com/".parse().unwrap());
        assert_eq!(site, "example.com");
        let yes = |u: &str| follows(&u.parse().unwrap(), &site, &c);
        assert!(yes("https://example.com/about"));
        assert!(yes("http://www.example.com/team"));
        assert!(!yes("https://other.example/"));
        assert!(!yes("https://example.com/login?next=/"));
        assert!(!yes("https://example.com/report.PDF"));
        assert!(!yes("mailto:a@example.com"));
    }
}
