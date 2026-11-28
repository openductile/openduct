// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A `web` step's settings, read and checked when the workflow is: only the settings its action takes, every number
//! within its limits, every selector and pattern read, each mistake said with where it is.

use std::time::Duration;

use openduct_expr::Compiled;
use serde_json::{Map, Value as Json};

use super::crawl::{
    CRAWL_DEPTH, CRAWL_PAGES, CRAWL_RATE, CRAWL_TIME, Crawl, LEAST_RATE, MOST_CRAWL_DEPTH, MOST_CRAWL_PAGES,
    MOST_CRAWL_TIME,
};
use super::feed::{FEED_ITEMS, MOST_FEED_ITEMS};
use super::fetch::Limits;
use super::fields::{Field, MAX_FIELD_DEPTH, MAX_FIELDS, Shape};
use super::page::{Part, Selector};
use super::{
    Action, HtmlOut, MOST_CHARS, MOST_PAGE_BYTES, MOST_PAGE_TIME, MOST_REDIRECTS, MOST_SITEMAP_URLS,
    PAGE_BYTES, PAGE_TIME, REDIRECTS, SITEMAP_URLS, WebStep,
};
use crate::template::Template;

type Templates<'a> = &'a dyn Fn(&Json, &str) -> Result<Template, String>;
type Expressions<'a> = &'a dyn Fn(&Json, &str) -> Result<Compiled, String>;

/// The most headers a step sends, and patterns a crawl has.
const MAX_HEADERS: usize = 20;
const MAX_PATTERNS: usize = 20;
/// Headers a step may not set: the client sets them.
const OWN_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "upgrade",
    "te",
];

fn text(v: Option<&Json>) -> Option<&str> {
    v.and_then(Json::as_str)
}

fn flag(s: &Map<String, Json>, name: &str, at: &str, default: bool) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

fn count(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    default: u64,
    (least, most): (u64, u64),
) -> Result<u64, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(n) => n
            .as_u64()
            .filter(|n| (least..=most).contains(n))
            .ok_or_else(|| format!("{at}.{name}: a whole number, {least} to {most}")),
    }
}

fn size(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    default: usize,
    range: (usize, usize),
) -> Result<usize, String> {
    let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let n = count(s, name, at, wide(default), (wide(range.0), wide(range.1)))?;
    Ok(usize::try_from(n).unwrap_or(default))
}

fn wait(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    default: Duration,
    range: (Duration, Duration),
) -> Result<Duration, String> {
    let Some(given) = text(s.get(name)) else {
        if s.get(name).is_some_and(|v| !v.is_null()) {
            return Err(format!("{at}.{name}: how long, as 500ms, 30s or 2m"));
        }
        return Ok(default);
    };
    crate::model::duration(given)
        .filter(|d| *d >= range.0 && *d <= range.1)
        .ok_or_else(|| {
            format!(
                "{at}.{name}: how long, as 500ms, 30s or 2m - from {}ms to {}s",
                range.0.as_millis(),
                range.1.as_secs()
            )
        })
}

/// A size: bytes, or `512KB`, `2MB` (`KiB`, `MiB` too).
fn bytes(s: &Map<String, Json>, at: &str) -> Result<usize, String> {
    let wrong = || format!("{at}.max_bytes: a size, as 2MB or 500KB - at most 10MB");
    let n = match s.get("max_bytes") {
        None | Some(Json::Null) => return Ok(PAGE_BYTES),
        Some(Json::Number(n)) => n.as_u64().ok_or_else(wrong)?,
        Some(Json::String(t)) => {
            let t = t.trim().to_ascii_lowercase();
            let digits: String = t.chars().take_while(char::is_ascii_digit).collect();
            let number: u64 = digits.parse().map_err(|_| wrong())?;
            let unit = match t.get(digits.len()..).unwrap_or_default().trim() {
                "" | "b" => 1,
                "kb" | "kib" | "k" => 1024,
                "mb" | "mib" | "m" => 1024 * 1024,
                _ => return Err(wrong()),
            };
            number.checked_mul(unit).ok_or_else(wrong)?
        }
        Some(_) => return Err(wrong()),
    };
    usize::try_from(n)
        .ok()
        .filter(|n| (1024..=MOST_PAGE_BYTES).contains(n))
        .ok_or_else(wrong)
}

fn patterns(s: &Map<String, Json>, name: &str, at: &str) -> Result<Vec<String>, String> {
    let list = match s.get(name) {
        None | Some(Json::Null) => return Ok(Vec::new()),
        Some(Json::String(p)) => vec![p.clone()],
        Some(Json::Array(items)) => items
            .iter()
            .map(|p| {
                p.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| format!("{at}.{name}: addresses, with * for any characters"))
            })
            .collect::<Result<_, _>>()?,
        Some(_) => {
            return Err(format!(
                "{at}.{name}: a list of addresses, with * for any characters"
            ));
        }
    };
    if list.len() > MAX_PATTERNS || list.iter().any(|p| p.trim().is_empty() || p.len() > 500) {
        return Err(format!(
            "{at}.{name}: at most {MAX_PATTERNS} patterns of up to 500 characters"
        ));
    }
    Ok(list.into_iter().map(|p| p.trim().to_owned()).collect())
}

/// A step's fields to pick out: each a selector, or a record of `selector`, `attribute`, `all`, `as`, `fields`.
fn fields(v: Option<&Json>, at: &str, depth: usize) -> Result<Vec<Field>, String> {
    let Some(Json::Object(given)) = v else {
        return Err(format!(
            "{at}: the fields to pick out, each a CSS selector - as {{title: h1, price: .price}}"
        ));
    };
    if given.is_empty() || given.len() > MAX_FIELDS {
        return Err(format!("{at}: 1 to {MAX_FIELDS} fields"));
    }
    if depth > MAX_FIELD_DEPTH {
        return Err(format!("{at}: fields hold fields at most {MAX_FIELD_DEPTH} deep"));
    }
    let mut out = Vec::with_capacity(given.len());
    for (name, spec) in given {
        let place = format!("{at}.{name}");
        if name.is_empty() || name.len() > 64 || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(format!("{place}: a field's name is letters, digits and '_'"));
        }
        let field = match spec {
            Json::String(selector) => Field {
                name: name.clone(),
                selector: Selector::parse(selector).map_err(|e| format!("{place}: {e}"))?,
                attribute: None,
                all: false,
                shape: Shape::Text,
                fields: Vec::new(),
            },
            Json::Object(s) => {
                if let Some(k) = s
                    .keys()
                    .find(|k| !matches!(k.as_str(), "selector" | "attribute" | "all" | "as" | "fields"))
                {
                    return Err(format!(
                        "{place}.{k}: not a setting of a field (selector, attribute, all, as, fields)"
                    ));
                }
                let selector = text(s.get("selector"))
                    .ok_or_else(|| format!("{place}.selector: the CSS selector, as .price"))?;
                let attribute = text(s.get("attribute")).map(|a| a.trim().to_ascii_lowercase());
                if attribute.as_ref().is_some_and(|a| a.is_empty() || a.len() > 100) {
                    return Err(format!("{place}.attribute: an attribute's name, as href"));
                }
                let shape = match text(s.get("as")) {
                    None | Some("text") => Shape::Text,
                    Some("markdown") => Shape::Markdown,
                    Some("html") => Shape::Html,
                    Some(other) => return Err(format!("{place}.as: text, markdown or html, not {other:?}")),
                };
                let inner = match s.get("fields") {
                    None | Some(Json::Null) => Vec::new(),
                    v => fields(v, &format!("{place}.fields"), depth + 1)?,
                };
                if !inner.is_empty() && (attribute.is_some() || s.contains_key("as")) {
                    return Err(format!("{place}: fields, or an attribute or as - not both"));
                }
                Field {
                    name: name.clone(),
                    selector: Selector::parse(selector).map_err(|e| format!("{place}.selector: {e}"))?,
                    attribute,
                    all: flag(s, "all", &place, false)?,
                    shape,
                    fields: inner,
                }
            }
            _ => {
                return Err(format!(
                    "{place}: a CSS selector, or {{selector, attribute, all, as, fields}}"
                ));
            }
        };
        out.push(field);
    }
    Ok(out)
}

/// The settings an action takes, beside those every action has.
fn settings_of(action: Action) -> &'static [&'static str] {
    match action {
        Action::Fetch => &[
            "format",
            "content",
            "within",
            "links",
            "images",
            "max_chars",
            "html",
            "from",
            "headers",
            "fail_on_status",
        ],
        Action::Select => &["fields", "html", "from", "headers", "fail_on_status"],
        Action::Crawl => &[
            "format",
            "content",
            "within",
            "links",
            "images",
            "max_chars",
            "html",
            "depth",
            "max_pages",
            "include",
            "exclude",
            "sitemap",
            "robots",
            "rate",
            "time",
        ],
        Action::Sitemap => &["max_urls", "headers"],
        Action::Feed => &["max_items", "from", "headers"],
    }
}

/// A `web` step's settings, read.
pub(crate) fn parse(
    body: &Json,
    at: &str,
    template: Templates<'_>,
    expression: Expressions<'_>,
) -> Result<WebStep, String> {
    let s = match body {
        Json::String(_) => {
            let mut m = Map::new();
            m.insert("url".to_owned(), body.clone());
            m
        }
        Json::Object(m) => m.clone(),
        _ => {
            return Err(format!(
                "{at}: a set of settings, as {{action: fetch, url: https://...}}"
            ));
        }
    };
    let named = text(s.get("action")).unwrap_or("fetch");
    let action = Action::parse(named)
        .ok_or_else(|| format!("{at}.action: {} - not {named:?}", Action::ALL.join(", ")))?;
    let mut allowed: Vec<&str> = vec![
        "action",
        "url",
        "user_agent",
        "timeout",
        "max_bytes",
        "max_redirects",
    ];
    allowed.extend_from_slice(settings_of(action));
    if let Some(k) = s.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(format!(
            "{at}.{k}: not a setting of web's {} ({})",
            action.name(),
            allowed.join(", ")
        ));
    }
    let url = s
        .get("url")
        .map(|u| template(u, &format!("{at}.url")))
        .transpose()?;
    let from = s
        .get("from")
        .map(|f| expression(f, &format!("{at}.from")))
        .transpose()?;
    if url.is_none() && from.is_none() {
        return Err(format!(
            "{at}.url: the address, as https://example.com/page{}",
            if allowed.contains(&"from") {
                " (or from: a page the run already has)"
            } else {
                ""
            }
        ));
    }
    // An address written out is checked now; one worked out per run, when it runs.
    if let Some(Json::String(u)) = s.get("url")
        && !u.contains("{{")
    {
        super::fetch::checked(u, true).map_err(|e| format!("{at}.url: {e}"))?;
    }
    let markdown = match text(s.get("format")) {
        None | Some("markdown") => true,
        Some("text") => false,
        Some(other) => return Err(format!("{at}.format: markdown or text, not {other:?}")),
    };
    let part = match (text(s.get("content")), text(s.get("within"))) {
        (Some(_), Some(_)) => return Err(format!("{at}: content or within, not both")),
        (_, Some(w)) => Part::Within(Selector::parse(w).map_err(|e| format!("{at}.within: {e}"))?),
        (None | Some("main"), None) => Part::Main,
        (Some("all"), None) => Part::All,
        (Some(other), None) => return Err(format!("{at}.content: main or all, not {other:?}")),
    };
    let html = match s.get("html") {
        None | Some(Json::Null | Json::Bool(false)) => HtmlOut::No,
        Some(Json::Bool(true)) => HtmlOut::Text,
        Some(Json::String(f)) if f == "file" && action != Action::Crawl => HtmlOut::File,
        Some(_) => {
            return Err(format!(
                "{at}.html: true (the page's HTML as text){}",
                if action == Action::Crawl {
                    ""
                } else {
                    ", or file (as a run's file)"
                }
            ));
        }
    };
    let mut headers = Vec::new();
    match s.get("headers") {
        None | Some(Json::Null) => {}
        Some(Json::Object(h)) if h.len() <= MAX_HEADERS => {
            for (name, value) in h {
                let ok = !name.is_empty()
                    && name.len() <= 100
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
                if !ok || OWN_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                    return Err(format!("{at}.headers.{name}: not a header a step sets"));
                }
                headers.push((name.clone(), template(value, &format!("{at}.headers.{name}"))?));
            }
        }
        Some(_) => {
            return Err(format!(
                "{at}.headers: at most {MAX_HEADERS} names and their text"
            ));
        }
    }
    let max_chars = match s.get("max_chars") {
        None | Some(Json::Null) => None,
        Some(n) => Some(
            n.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| (100..=MOST_CHARS).contains(n))
                .ok_or_else(|| format!("{at}.max_chars: a whole number, 100 to {MOST_CHARS}"))?,
        ),
    };
    let crawling = action == Action::Crawl;
    let crawl = crawling
        .then(|| -> Result<Crawl, String> {
            Ok(Crawl {
                depth: u32::try_from(count(
                    &s,
                    "depth",
                    at,
                    u64::from(CRAWL_DEPTH),
                    (0, u64::from(MOST_CRAWL_DEPTH)),
                )?)
                .unwrap_or(CRAWL_DEPTH),
                max_pages: size(&s, "max_pages", at, CRAWL_PAGES, (1, MOST_CRAWL_PAGES))?,
                include: patterns(&s, "include", at)?,
                exclude: patterns(&s, "exclude", at)?,
                sitemap: flag(&s, "sitemap", at, false)?,
                robots: flag(&s, "robots", at, true)?,
                rate: wait(&s, "rate", at, CRAWL_RATE, (LEAST_RATE, Duration::from_secs(60)))?,
                time: wait(
                    &s,
                    "time",
                    at,
                    CRAWL_TIME,
                    (Duration::from_secs(1), MOST_CRAWL_TIME),
                )?,
            })
        })
        .transpose()?;
    let max_items = match action {
        Action::Sitemap => size(&s, "max_urls", at, SITEMAP_URLS, (1, MOST_SITEMAP_URLS))?,
        Action::Feed => size(&s, "max_items", at, FEED_ITEMS, (1, MOST_FEED_ITEMS))?,
        _ => 0,
    };
    Ok(WebStep {
        action,
        url,
        from,
        markdown,
        part,
        links: flag(&s, "links", at, !crawling)?,
        images: flag(&s, "images", at, !crawling)?,
        max_chars,
        html,
        agent: s
            .get("user_agent")
            .map(|a| template(a, &format!("{at}.user_agent")))
            .transpose()?,
        headers,
        limits: Limits {
            max_bytes: bytes(&s, at)?,
            timeout: wait(
                &s,
                "timeout",
                at,
                PAGE_TIME,
                (Duration::from_secs(1), MOST_PAGE_TIME),
            )?,
            max_redirects: size(&s, "max_redirects", at, REDIRECTS, (0, MOST_REDIRECTS))?,
        },
        fail_on_status: flag(&s, "fail_on_status", at, true)?,
        fields: if action == Action::Select {
            fields(s.get("fields"), &format!("{at}.fields"), 1)?
        } else {
            Vec::new()
        },
        crawl,
        max_items,
    })
}
