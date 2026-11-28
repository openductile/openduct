// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A test run's web step: nothing is fetched. A sample page of OpenDuct's own stands in for the address asked - read
//! by the same code a real page is, at that address - so the steps after it see the shape a real run gives them: a
//! page's fields, a crawl's pages, a sitemap's addresses, a feed's items. Each answer says it is a sample.

use reqwest::Url;
use serde_json::{Value as Json, json};

use super::{Action, WebStep, fetch, page_answer, sitemap_value};
use crate::files::RunFiles;
use crate::run::Effects;

/// What a test run's answer says of itself.
pub(crate) const SAMPLE: &str = "sample page, nothing fetched";

const PAGE: &str = "<!doctype html><html lang=\"en\"><head><title>A sample page</title>\
<meta name=\"description\" content=\"What a test run reads in place of the page it would fetch.\">\
<link rel=\"alternate\" type=\"application/rss+xml\" href=\"/feed.xml\"></head><body>\
<nav><a href=\"/\">Home</a> <a href=\"/about\">About</a></nav><main><h1>A sample page</h1>\
<p>A test run fetches nothing: this page stands in for the one asked for. A preview, or a run, reads the page \
itself.</p><h2>A section</h2><p>Its text, with <a href=\"/more\">a link</a> and a picture.</p>\
<img src=\"/picture.png\" alt=\"A picture\"></main><footer>Its footer</footer></body></html>";

const FEED: &str = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>A sample feed</title>\
<link>/</link><item><title>The newer item</title><link>/items/2</link><guid>sample-2</guid>\
<pubDate>Fri, 27 Nov 2026 09:00:00 GMT</pubDate><description>What a test run reads in place of the feed.\
</description></item><item><title>The older item</title><link>/items/1</link><guid>sample-1</guid>\
<pubDate>Thu, 26 Nov 2026 09:00:00 GMT</pubDate><description>Another sample item.</description></item>\
</channel></rss>";

fn marked(mut answer: Json) -> Json {
    if let Some(record) = answer.as_object_mut() {
        record.insert("test".to_owned(), Json::String(SAMPLE.to_owned()));
    }
    answer
}

/// The sample page, as fetched from `url`.
fn got(url: Url) -> fetch::Got {
    fetch::Got {
        url,
        status: 200,
        content_type: "text/html; charset=utf-8".to_owned(),
        body: PAGE.as_bytes().to_vec(),
        redirects: 0,
        etag: None,
        modified: None,
        ms: 0,
    }
}

/// A test run's answer for `step` at `url`.
pub(crate) async fn answer(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &WebStep,
    url: &str,
) -> Result<Json, String> {
    // The address is checked as a real run checks it, but for this machine's network, which a real run checks.
    let at = fetch::checked(url, true)?;
    let origin = at.origin().ascii_serialization();
    Ok(marked(match step.action {
        Action::Fetch | Action::Select => page_answer(effects, files, step, &got(at)).await?,
        Action::Crawl => {
            let page = super::page::Page::parse(PAGE, false);
            let mut value = super::page_value(
                step,
                &got(at.clone()),
                &page,
                &mut super::page::Budget(super::page::BUDGET),
            )?;
            if let Some(record) = value.as_object_mut() {
                record.insert("depth".to_owned(), json!(0));
            }
            json!({ "url": at.as_str(), "count": 1, "pages": [value], "skipped": [], "stopped": null, "ms": 0 })
        }
        Action::Sitemap => {
            let listed = super::sitemap::Listed {
                entries: ["/", "/about"]
                    .iter()
                    .map(|p| super::sitemap::Entry {
                        url: format!("{origin}{p}"),
                        modified: Some("2026-11-27".to_owned()),
                        changes: None,
                        priority: None,
                    })
                    .collect(),
                sitemaps: vec![format!("{origin}/sitemap.xml")],
            };
            sitemap_value(at.as_str(), &listed)
        }
        Action::Feed => {
            let feed = super::feed::parse(FEED, &at, step.max_items)?;
            let items: Vec<Json> = feed.items.iter().map(super::feed::Item::value).collect();
            json!({
                "url": at.as_str(), "title": feed.title, "link": feed.link, "description": feed.description,
                "count": items.len(), "items": items,
            })
        }
    }))
}
