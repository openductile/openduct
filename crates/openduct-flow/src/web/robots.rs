// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A site's robots.txt, read as RFC 9309 says: the group of rules for this crawler's name (the first word of its
//! User-Agent, `OpenDuct` unless a step sets another), or else the group for `*`; the rule whose path matches the
//! most characters wins (`*` stands for any characters, `$` for the end), an `Allow` over a `Disallow` of the same
//! length. A robots.txt that is not there (4xx) allows everything; one that cannot be read (5xx, no answer) allows
//! nothing, for now. Its `Crawl-delay` and `Sitemap` lines are kept. At most [`MAX_ROBOTS`] bytes are read.

use super::fetch::{Ask, Limits, Net, get};

/// The most bytes of a robots.txt read, as RFC 9309 asks crawlers to read at least.
pub(crate) const MAX_ROBOTS: usize = 512 * 1024;
/// The longest a site's `Crawl-delay` makes a crawl wait between pages.
const MOST_DELAY: f64 = 30.0;

/// The rules for one crawler.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Robots {
    /// Each rule: allowed or not, and its path.
    rules: Vec<(bool, String)>,
    /// Nothing may be fetched: the robots.txt could not be read.
    closed: bool,
    pub(crate) delay: Option<f64>,
    pub(crate) sitemaps: Vec<String>,
}

/// The product name a robots.txt knows a crawler by: the first word of its User-Agent, before any `/`.
pub(crate) fn token(agent: &str) -> String {
    agent
        .split(|c: char| c == '/' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

/// A group of a robots.txt: the crawlers it is for, its rules (allowed or not, and a path) and its delay.
#[derive(Default)]
struct Group {
    agents: Vec<String>,
    rules: Vec<(bool, String)>,
    delay: Option<f64>,
}

impl Robots {
    /// What a robots.txt says to the crawler called `name`.
    pub(crate) fn parse(text: &str, name: &str) -> Self {
        let name = name.to_ascii_lowercase();
        let mut groups: Vec<Group> = Vec::new();
        let mut sitemaps = Vec::new();
        let mut in_agents = false;
        for line in text.lines().take(100_000) {
            let line = line.split('#').next().unwrap_or_default().trim();
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let (key, value) = (key.trim().to_ascii_lowercase(), value.trim());
            match key.as_str() {
                "user-agent" => {
                    if !in_agents {
                        groups.push(Group::default());
                    }
                    in_agents = true;
                    if let Some(g) = groups.last_mut() {
                        g.agents.push(token(value));
                    }
                }
                "allow" | "disallow" => {
                    in_agents = false;
                    if let Some(g) = groups.last_mut()
                        && !value.is_empty()
                        && g.rules.len() < 10_000
                    {
                        g.rules.push((key == "allow", value.to_owned()));
                    }
                }
                "crawl-delay" => {
                    in_agents = false;
                    if let Some(g) = groups.last_mut() {
                        g.delay = value.parse::<f64>().ok().filter(|d| d.is_finite() && *d >= 0.0);
                    }
                }
                "sitemap" if sitemaps.len() < 50 && !value.is_empty() => sitemaps.push(value.to_owned()),
                _ => {}
            }
        }
        let mine: Vec<&Group> = groups.iter().filter(|g| g.agents.contains(&name)).collect();
        let chosen = if mine.is_empty() {
            groups
                .iter()
                .filter(|g| g.agents.iter().any(|a| a == "*"))
                .collect()
        } else {
            mine
        };
        Self {
            rules: chosen.iter().flat_map(|g| g.rules.iter().cloned()).collect(),
            closed: false,
            delay: chosen.iter().find_map(|g| g.delay).map(|d| d.min(MOST_DELAY)),
            sitemaps,
        }
    }

    /// Rules that allow nothing: what a robots.txt that cannot be read means.
    pub(crate) fn closed() -> Self {
        Self {
            closed: true,
            ..Self::default()
        }
    }

    /// Whether the path and query of an address (`/a/b?c=1`) may be fetched.
    pub(crate) fn allows(&self, path: &str) -> bool {
        if self.closed {
            return false;
        }
        if path == "/robots.txt" {
            return true;
        }
        let mut best: Option<(usize, bool)> = None;
        for (allow, pattern) in &self.rules {
            if matches(pattern, path) {
                let len = pattern.len();
                let better = match best {
                    None => true,
                    Some((l, a)) => len > l || (len == l && *allow && !a),
                };
                if better {
                    best = Some((len, *allow));
                }
            }
        }
        best.is_none_or(|(_, allow)| allow)
    }
}

/// Whether a rule's path matches `path` from its start: `*` any characters, a final `$` its end.
fn matches(pattern: &str, path: &str) -> bool {
    let (pattern, anchored) = match pattern.strip_suffix('$') {
        Some(p) => (p, true),
        None => (pattern, false),
    };
    let pieces: Vec<&str> = pattern.split('*').collect();
    let mut at = 0;
    for (i, piece) in pieces.iter().enumerate() {
        let rest = path.get(at..).unwrap_or_default();
        if i == 0 {
            if !rest.starts_with(piece) {
                return false;
            }
            at += piece.len();
            continue;
        }
        if i == pieces.len() - 1 && anchored {
            return rest.len() >= piece.len() && rest.ends_with(piece);
        }
        match rest.find(piece) {
            Some(found) => at += found + piece.len(),
            None => return false,
        }
    }
    !anchored || at == path.len() || pattern.ends_with('*')
}

/// The robots.txt of the site of `origin` (`https://example.com`), for the crawler called `name`.
pub(crate) async fn fetch(net: &Net, origin: &str, agent: &str) -> Robots {
    let url = format!("{origin}/robots.txt");
    let ask = Ask {
        url: &url,
        agent,
        headers: &[],
        accept: "text/plain, */*;q=0.5",
        limits: Limits {
            max_bytes: MAX_ROBOTS,
            timeout: std::time::Duration::from_secs(20),
            max_redirects: 5,
        },
        since: None,
    };
    match get(net, &ask).await {
        Ok(got) if (200..300).contains(&got.status) => {
            Robots::parse(&String::from_utf8_lossy(&got.body), &token(agent))
        }
        Ok(got) if (400..500).contains(&got.status) => Robots::default(),
        // A page larger than the limit is read up to it, as the RFC allows.
        Err(e) if e.contains("larger than") => Robots::default(),
        _ => Robots::closed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "# a comment\nUser-agent: *\nDisallow: /private\nAllow: /private/open\nCrawl-delay: 2\n\n\
                        User-agent: OtherBot\nUser-agent: openduct\nDisallow: /search\nDisallow: /*.pdf$\n\
                        Allow: /search/help\nDisallow: /tmp*/x\n\nSitemap: https://example.com/sitemap.xml\n";

    #[test]
    fn the_group_for_this_crawler_wins_and_the_longest_rule_decides() {
        let mine = Robots::parse(TEXT, &token("OpenDuct/0.1 (+https://example.com)"));
        assert!(!mine.allows("/search?q=1"));
        assert!(mine.allows("/search/help"));
        assert!(!mine.allows("/files/a.pdf"));
        assert!(mine.allows("/files/a.pdf?x=1"));
        assert!(!mine.allows("/tmp2/x/y"));
        assert!(mine.allows("/private"), "the * group is not this crawler's");
        assert_eq!(mine.delay, None);
        assert_eq!(mine.sitemaps, ["https://example.com/sitemap.xml"]);
        let other = Robots::parse(TEXT, "somebot");
        assert!(!other.allows("/private/x"));
        assert!(other.allows("/private/open/x"));
        assert!(other.allows("/search"));
        assert_eq!(other.delay, Some(2.0));
        assert!(other.allows("/robots.txt"));
        assert!(Robots::default().allows("/anything"));
        assert!(!Robots::closed().allows("/"));
        let all = Robots::parse("User-agent: *\nDisallow: /\n", "openduct");
        assert!(!all.allows("/"));
        assert!(Robots::parse("User-agent: *\nDisallow:\n", "openduct").allows("/a"));
    }
}
