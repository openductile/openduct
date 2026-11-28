// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The web step: what people mean by "fetch this page" and "scrape this site", in OpenDuct's own code.
//!
//! - `fetch`: one page - its readable text as Markdown or plain text (in reading order, headings kept, navigation,
//!   scripts and styles left out), its title, description, language, canonical address, links and pictures, and its
//!   HTML when asked (as text, or a run's file).
//! - `select`: fields picked out of a page by CSS selectors, as text, an attribute, Markdown or HTML, one or all.
//! - `crawl`: pages from a start address outward, on the same site or matching patterns, to a depth and a number of
//!   pages; robots.txt honoured, one host asked at a polite rate, the sitemap used when asked.
//! - `sitemap`: the addresses a site's sitemap (or sitemap index) lists.
//! - `feed`: an RSS, Atom or JSON feed's items. A trigger runs a workflow for each new item ([`trigger`]).
//!
//! **Bounded.** Each page's bytes, the time a fetch takes, the redirects followed, the pages and time of a crawl, the
//! items given: each has a default and a hard limit. **Guarded.** Only http and https, and never this machine's own
//! network unless the installation allows it - checked at every address and every redirect ([`fetch`]). Each request
//! names OpenDuct as its User-Agent unless the step names another. **Runs that only look.** A test run fetches
//! nothing: it answers with a sample page read as a real one is, so the steps after it see the same shape. A preview
//! fetches for real: a fetch only reads. Every action answers, so the step is an agent's tool as it stands.

mod crawl;
mod feed;
mod fetch;
mod fields;
mod page;
mod robots;
mod sample;
pub(crate) mod settings;
mod sitemap;
pub(crate) mod trigger;

use std::time::Duration;

use openduct_expr::{Compiled, Env, Value};
use serde_json::{Value as Json, json};

use crate::files::RunFiles;
use crate::run::Effects;
use crate::template::Template;

pub use fetch::Net;
pub use trigger::FeedTrigger;

/// The User-Agent a web step sends, unless it names another.
pub const USER_AGENT: &str = concat!(
    "OpenDuct/",
    env!("CARGO_PKG_VERSION"),
    " (+",
    env!("CARGO_PKG_REPOSITORY"),
    ")"
);

/// What a page is asked for as.
pub(crate) const ACCEPT_PAGE: &str = "text/html,application/xhtml+xml;q=0.9,text/plain;q=0.8,*/*;q=0.5";
const ACCEPT_FEED: &str = "application/rss+xml, application/atom+xml, application/feed+json, application/xml;q=0.9, \
                           text/xml;q=0.9, */*;q=0.5";

/// The most bytes of a page, by default and at most.
pub(crate) const PAGE_BYTES: usize = 5 * 1024 * 1024;
pub(crate) const MOST_PAGE_BYTES: usize = crate::files::MAX_FILE;
/// How long a fetch may take, by default and at most.
pub(crate) const PAGE_TIME: Duration = Duration::from_secs(30);
pub(crate) const MOST_PAGE_TIME: Duration = Duration::from_secs(120);
/// The redirects followed, by default and at most.
pub(crate) const REDIRECTS: usize = 5;
pub(crate) const MOST_REDIRECTS: usize = 10;
/// The addresses a sitemap gives, by default and at most.
pub(crate) const SITEMAP_URLS: usize = 1_000;
pub(crate) const MOST_SITEMAP_URLS: usize = 50_000;
/// The most characters of text a page gives, when a step cuts it.
pub(crate) const MOST_CHARS: usize = 1_000_000;

/// What a web step does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Fetch,
    Select,
    Crawl,
    Sitemap,
    Feed,
}

impl Action {
    pub(crate) const ALL: &'static [&'static str] = &["fetch", "select", "crawl", "sitemap", "feed"];

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "fetch" => Self::Fetch,
            "select" => Self::Select,
            "crawl" => Self::Crawl,
            "sitemap" => Self::Sitemap,
            "feed" => Self::Feed,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::Select => "select",
            Self::Crawl => "crawl",
            Self::Sitemap => "sitemap",
            Self::Feed => "feed",
        }
    }
}

/// A page's HTML given beside its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HtmlOut {
    No,
    Text,
    File,
}

/// A `web` step, as read.
#[derive(Debug, Clone)]
pub struct WebStep {
    pub(crate) action: Action,
    /// The address to fetch; with `from`, the address the page's links are read against.
    pub(crate) url: Option<Template>,
    /// A page (or a feed) the run already has: text, or a run's file.
    pub(crate) from: Option<Compiled>,
    pub(crate) markdown: bool,
    pub(crate) part: page::Part,
    pub(crate) links: bool,
    pub(crate) images: bool,
    pub(crate) max_chars: Option<usize>,
    pub(crate) html: HtmlOut,
    pub(crate) agent: Option<Template>,
    pub(crate) headers: Vec<(String, Template)>,
    pub(crate) limits: fetch::Limits,
    pub(crate) fail_on_status: bool,
    pub(crate) fields: Vec<fields::Field>,
    pub(crate) crawl: Option<crawl::Crawl>,
    /// The most addresses (sitemap) or items (feed) given.
    pub(crate) max_items: usize,
}

/// How a run's web steps reach the web.
#[derive(Debug, Clone, Copy)]
pub enum Reach<'a> {
    /// A test run: nothing is fetched; a sample page stands in.
    Shown,
    /// A real run or a preview.
    Live(&'a Net),
}

/// What reads as a page's address and type when it came from the run, not from the web.
const GIVEN: &str = "https://page.invalid/";

/// A step's User-Agent: its own, worked out, or OpenDuct's.
fn agent_of(step: &WebStep, env: &Env<'_>) -> Result<String, String> {
    let Some(t) = &step.agent else {
        return Ok(USER_AGENT.to_owned());
    };
    let agent = t.render(env).map_err(|e| format!("user_agent: {e}"))?;
    let agent = agent.trim();
    if agent.is_empty() {
        return Ok(USER_AGENT.to_owned());
    }
    if agent.len() > 300 || !agent.chars().all(|c| (' '..='~').contains(&c)) {
        return Err("user_agent: at most 300 characters, letters, digits and signs of ASCII".to_owned());
    }
    Ok(agent.to_owned())
}

fn headers_of(step: &WebStep, env: &Env<'_>) -> Result<Vec<(String, String)>, String> {
    step.headers
        .iter()
        .map(|(name, t)| {
            let value = t.render(env).map_err(|e| format!("headers.{name}: {e}"))?;
            if value.chars().any(|c| c.is_control()) || value.len() > 8_192 {
                return Err(format!("headers.{name}: one line of at most 8 KiB"));
            }
            Ok((name.clone(), value))
        })
        .collect()
}

/// A page's value, as fetch gives it.
pub(crate) fn page_value(
    step: &WebStep,
    got: &fetch::Got,
    page: &page::Page,
    budget: &mut page::Budget,
) -> Result<Json, String> {
    let options = page::Options {
        part: step.part.clone(),
        markdown: step.markdown,
        links: step.links,
        images: step.images,
        max_chars: step.max_chars,
    };
    let read = page::read(page, &got.url, &options, budget)?;
    let m = &read.meta;
    let mut out = json!({
        "url": got.url.as_str(),
        "status": got.status,
        "type": got.media_type(),
        "title": m.title,
        "description": m.description,
        "language": m.language,
        "canonical": m.canonical,
        "site_name": m.site_name,
        "author": m.author,
        "published": m.published,
        "image": m.image,
        "feeds": m.feeds,
        "format": if step.markdown { "markdown" } else { "text" },
        "text": read.text,
        "cut": read.cut,
        "bytes": got.body.len(),
        "redirects": got.redirects,
        "ms": got.ms,
    });
    if let Some(record) = out.as_object_mut() {
        if step.links {
            let links: Vec<Json> = read
                .links
                .iter()
                .map(|(u, t)| json!({ "url": u, "text": t }))
                .collect();
            record.insert("links".to_owned(), Json::Array(links));
        }
        if step.images {
            let images: Vec<Json> = read
                .images
                .iter()
                .map(|(u, a)| json!({ "url": u, "alt": a }))
                .collect();
            record.insert("images".to_owned(), Json::Array(images));
        }
        if step.html == HtmlOut::Text {
            record.insert(
                "html".to_owned(),
                Json::String(page::decode(&got.body, &got.content_type)),
            );
        }
    }
    Ok(out)
}

/// A text answer (not HTML) as fetch gives a page: its text, as it is.
fn text_value(step: &WebStep, got: &fetch::Got) -> Json {
    let mut text = page::decode(&got.body, &got.content_type);
    let cut = step.max_chars.is_some_and(|most| page::cut_text(&mut text, most));
    json!({
        "url": got.url.as_str(), "status": got.status, "type": got.media_type(), "title": "",
        "format": "text", "text": text, "cut": cut, "links": [], "images": [], "bytes": got.body.len(),
        "redirects": got.redirects, "ms": got.ms,
    })
}

/// The answer of one fetched page, for `fetch` and `select`.
async fn page_answer(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &WebStep,
    got: &fetch::Got,
) -> Result<Json, String> {
    if step.fail_on_status && got.status >= 400 {
        return Err(format!("{} answered {}", got.url, got.status));
    }
    let media = got.media_type();
    if !got.is_html() {
        if step.action == Action::Fetch && (media.starts_with("text/") || media.is_empty()) {
            return Ok(text_value(step, got));
        }
        let what = if media.is_empty() {
            "not text".to_owned()
        } else {
            media
        };
        if ["xml", "rss", "atom", "feed+json"]
            .iter()
            .any(|k| what.contains(k))
        {
            return Err(format!(
                "{} is {what}, not a web page: read a feed with action: feed, a sitemap with action: sitemap",
                got.url
            ));
        }
        return Err(format!(
            "{} is {what}, not a web page: fetch it with an http step (its answer is then a file) and read it with \
             extract_file",
            got.url
        ));
    }
    let text = page::decode(&got.body, &got.content_type);
    let tree = page::Page::parse(&text, false);
    let mut budget = page::Budget(page::BUDGET);
    let mut out = match step.action {
        Action::Select => {
            let base = page::Base::of(&tree, &got.url);
            let picked = fields::pick(&tree, &base, &step.fields, 0, &mut budget)?;
            let title = page::meta::meta(&tree, &base).title;
            json!({ "url": got.url.as_str(), "status": got.status, "title": title, "fields": picked })
        }
        _ => page_value(step, got, &tree, &mut budget)?,
    };
    if step.html == HtmlOut::File
        && let Some(record) = out.as_object_mut()
    {
        let name = got
            .url
            .path_segments()
            .and_then(|mut s| s.next_back())
            .filter(|s| !s.is_empty())
            .map_or_else(
                || "page.html".to_owned(),
                |s| format!("{}.html", s.trim_end_matches(".html")),
            );
        let file = files.make(effects, &name, "text/html", got.body.clone()).await?;
        record.insert(
            "file".to_owned(),
            serde_json::from_str(&file.to_json()).unwrap_or(Json::Null),
        );
    }
    Ok(out)
}

/// A feed's answer: its items; a web page that offers a feed has it read instead.
async fn feed_answer(
    net: Option<&Net>,
    ask: &fetch::Ask<'_>,
    got: fetch::Got,
    most: usize,
) -> Result<Json, String> {
    if got.status >= 400 {
        return Err(format!("{} answered {}", got.url, got.status));
    }
    let mut got = got;
    if got.is_html()
        && let Some(net) = net
    {
        let text = page::decode(&got.body, &got.content_type);
        let tree = page::Page::parse(&text, false);
        let offered = page::meta::meta(&tree, &page::Base::of(&tree, &got.url)).feeds;
        let Some(feed) = offered.first() else {
            return Err(format!("{} is a web page that offers no feed", got.url));
        };
        got = fetch::get(
            net,
            &fetch::Ask {
                url: feed,
                ..ask.clone()
            },
        )
        .await?;
        if got.status >= 400 {
            return Err(format!("{} answered {}", got.url, got.status));
        }
    }
    let text = page::decode(&got.body, &got.content_type);
    let feed = feed::parse(&text, &got.url, most)?;
    let items: Vec<Json> = feed.items.iter().map(feed::Item::value).collect();
    Ok(json!({
        "url": got.url.as_str(),
        "title": feed.title,
        "link": feed.link,
        "description": feed.description,
        "count": items.len(),
        "items": items,
    }))
}

/// The step, for real: each action against the web.
async fn live(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &WebStep,
    net: &Net,
    url: &str,
    env: &Env<'_>,
) -> Result<Json, String> {
    let agent = agent_of(step, env)?;
    let headers = headers_of(step, env)?;
    let ask = fetch::Ask {
        url,
        agent: &agent,
        headers: &headers,
        accept: if step.action == Action::Feed {
            ACCEPT_FEED
        } else {
            ACCEPT_PAGE
        },
        limits: step.limits,
        since: None,
    };
    match step.action {
        Action::Fetch | Action::Select => {
            let got = fetch::get(net, &ask).await?;
            page_answer(effects, files, step, &got).await
        }
        Action::Feed => {
            let got = fetch::get(net, &ask).await?;
            feed_answer(Some(net), &ask, got, step.max_items).await
        }
        Action::Sitemap => {
            let start = fetch::checked(url, true)?;
            let starts = sitemap::starts(net, &start, &agent).await;
            let listed = sitemap::read(net, starts, &agent, &headers, step.limits, step.max_items).await?;
            Ok(sitemap_value(url, &listed))
        }
        Action::Crawl => {
            let start = fetch::checked(url, true)?;
            let c = step.crawl.as_ref().ok_or("crawl: no settings")?;
            let value = |got: &fetch::Got, tree: &page::Page, budget: &mut page::Budget| {
                page_value(step, got, tree, budget)
            };
            let reading = crawl::Reading {
                agent: &agent,
                limits: step.limits,
                value: &value,
            };
            crawl::crawl(net, &start, c, &reading).await
        }
    }
}

fn sitemap_value(url: &str, listed: &sitemap::Listed) -> Json {
    let urls: Vec<Json> = listed
        .entries
        .iter()
        .map(
            |e| json!({ "url": e.url, "modified": e.modified, "changes": e.changes, "priority": e.priority }),
        )
        .collect();
    json!({ "url": url, "count": urls.len(), "urls": urls, "sitemaps": listed.sitemaps })
}

/// A page the run already has (`from`), read with no fetch.
async fn given(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &WebStep,
    env: &Env<'_>,
) -> Result<Json, String> {
    let from = step.from.as_ref().ok_or("from: the page")?;
    let file = crate::files::steps::file_of(effects, files, from, env)
        .await
        .map_err(|e| format!("from: {e}"))?;
    let address = match &step.url {
        Some(t) => t.render(env).map_err(|e| format!("url: {e}"))?,
        None => GIVEN.to_owned(),
    };
    let url = fetch::checked(&address, true)?;
    let media = if file.media_type.contains("html") || file.media_type == "text/plain" {
        "text/html".to_owned()
    } else {
        file.media_type.clone()
    };
    let got = fetch::Got {
        url,
        status: 200,
        content_type: media,
        body: file.bytes.to_vec(),
        redirects: 0,
        etag: None,
        modified: None,
        ms: 0,
    };
    if got.body.len() > MOST_PAGE_BYTES {
        return Err(format!("from: a page of more than {MOST_PAGE_BYTES} bytes"));
    }
    match step.action {
        Action::Feed => {
            let ask = fetch::Ask {
                url: GIVEN,
                agent: USER_AGENT,
                headers: &[],
                accept: ACCEPT_FEED,
                limits: step.limits,
                since: None,
            };
            feed_answer(None, &ask, got, step.max_items).await
        }
        _ => {
            let mut got = got;
            if !got.content_type.contains("html") {
                got.content_type = "text/html".to_owned();
            }
            page_answer(effects, files, step, &got).await
        }
    }
}

/// The step's result.
pub(crate) async fn run(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &WebStep,
    env: &Env<'_>,
) -> Result<Value, String> {
    let answer = if step.from.is_some() {
        given(effects, files, step, env).await?
    } else {
        let url = step
            .url
            .as_ref()
            .ok_or("url: the page's address")?
            .render(env)
            .map_err(|e| format!("url: {e}"))?;
        match effects.web() {
            Reach::Live(net) => live(effects, files, step, net, url.trim(), env).await?,
            Reach::Shown => sample::answer(effects, files, step, url.trim()).await?,
        }
    };
    Ok(Value::from_json(&answer))
}

/// An agent's tool that runs a web step: the address is the model's to give, unless the step gives it.
pub(crate) fn tool_defaults(body: &mut Json, parameters: &mut Option<Json>) {
    let Some(b) = body.as_object_mut() else { return };
    if b.contains_key("url") || b.contains_key("from") {
        return;
    }
    b.insert("url".to_owned(), json!("{{ args.url }}"));
    let action = b.get("action").and_then(Json::as_str).unwrap_or("fetch");
    let said = match action {
        "crawl" => "The address to start the crawl from, whole: https://...",
        "feed" => "The feed's address (or a site's page that offers one), whole: https://...",
        "sitemap" => "The site (https://example.com/) or its sitemap's address",
        _ => "The page's address, whole: https://...",
    };
    parameters.get_or_insert_with(|| {
        json!({
            "type": "object",
            "properties": { "url": { "type": "string", "description": said } },
            "required": ["url"],
        })
    });
}
