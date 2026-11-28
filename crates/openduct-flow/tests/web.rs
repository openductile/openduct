// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The web step against stand-in sites on this machine (tests/web_support): a journal post and an old-style page read
//! as their articles, fields picked by selectors, a crawl kept out by robots.txt, to its depth, its pages and a
//! polite rate, a sitemap index through robots.txt, RSS and Atom feeds, redirects followed and counted - and refused
//! when they lead to this machine's own network - limits on bytes and time, character sets and gzip, a page the run
//! already has, test runs and previews, and the step as an agent's tool. Every site is local; nothing reaches the
//! internet. The sites are reached with the private-network setting on, as only a test may; the guard's own test
//! turns it off and reaches the site by a name pinned to it.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod web_support;

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_expr::Value;
use openduct_flow::web::{Net, Reach};
use openduct_flow::{Credential, DryRun, Effects, Event, HttpRequest, Preview, Workflow, run};
use serde_json::{Value as Json, json};
use web_support::{Answer, Site};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// A run's effects that reach the web through `net`, with a scripted model for an agent's turns.
struct Web {
    net: Net,
    answers: Mutex<VecDeque<Json>>,
    asked: Mutex<Vec<Json>>,
}

impl Web {
    fn new(net: Net) -> Self {
        Self {
            net,
            answers: Mutex::new(VecDeque::new()),
            asked: Mutex::new(Vec::new()),
        }
    }
}

impl Effects for Web {
    fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let body: Json = r
                .body
                .as_ref()
                .and_then(|(_, b)| serde_json::from_slice(b).ok())
                .unwrap_or(Json::Null);
            self.asked.lock().unwrap().push(body);
            let answer = self.answers.lock().unwrap().pop_front().ok_or("no model here")?;
            Ok(Value::from_json(
                &json!({ "status": 200, "headers": {}, "body": answer }),
            ))
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no topics here".to_owned()) })
    }

    fn credential(&self, _: &str) -> Result<Credential, String> {
        let fields: BTreeMap<String, String> = [
            ("base_url".to_owned(), "https://models.example.com/v1".to_owned()),
            ("api_key".to_owned(), "sk-test".to_owned()),
            ("model".to_owned(), "small-1".to_owned()),
        ]
        .into();
        Ok(Credential {
            kind: "ai".to_owned(),
            fields,
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path}")) })
    }

    fn web(&self) -> Reach<'_> {
        Reach::Live(&self.net)
    }
}

/// The shop's site: its pages, robots.txt, sitemaps and feeds, and pages made to test limits and redirects.
fn shop() -> Site {
    Site::start(|asked| {
        let host = asked.host.as_str();
        let wants_gzip = asked
            .headers
            .iter()
            .any(|(k, v)| k == "accept-encoding" && v.contains("gzip"));
        match asked.path.as_str() {
            "/" => Answer::html(&web_support::home()),
            "/products/cap" => Answer::html(&web_support::product("Waxed cap", "£24.00")),
            "/products/tote" => Answer::html(&web_support::product("Canvas tote", "£18.50")),
            "/products/mug" => Answer::html(&web_support::product("Stoneware mug", "£12.00")),
            "/blog/how-we-wax-a-cap" => Answer::html(&web_support::post()),
            "/news" => Answer::html(&web_support::old_news()),
            "/robots.txt" => Answer::of(200, "text/plain", web_support::robots(host).as_bytes()),
            "/sitemap_index.xml" => Answer::of(
                200,
                "application/xml",
                web_support::sitemap_index(host).as_bytes(),
            ),
            "/sitemap-pages.xml" => Answer::of(
                200,
                "application/xml",
                web_support::sitemap_pages(host).as_bytes(),
            ),
            "/sitemap-products.xml.gz" => Answer::of(
                200,
                "application/x-gzip",
                &web_support::gzip(web_support::sitemap_products(host).as_bytes()),
            ),
            "/feed.xml" => Answer::of(200, "application/rss+xml", web_support::rss(host, &[]).as_bytes()),
            "/atom.xml" => Answer::of(200, "application/atom+xml", web_support::atom(host).as_bytes()),
            "/redirect" => Answer::redirect("/blog/how-we-wax-a-cap"),
            "/loop" => Answer::redirect("/loop"),
            "/to-private" => Answer::redirect(&format!("http://127.0.0.1:{}/secret", port_of(host))),
            "/to-localhost" => Answer::redirect(&format!("http://localhost:{}/secret", port_of(host))),
            "/big" => Answer::html(&format!("<p>{}</p>", "x".repeat(300 * 1024))),
            "/slow" => {
                std::thread::sleep(Duration::from_secs(3));
                Answer::html("<p>late</p>")
            }
            "/latin1" => Answer::of(
                200,
                "text/html; charset=ISO-8859-1",
                b"<title>Caf\xe9</title><p>Cr\xe8me br\xfbl\xe9e</p>",
            ),
            "/gz" if wants_gzip => {
                let mut a = Answer::html("");
                a.body = web_support::gzip(web_support::post().as_bytes());
                a.headers.push(("Content-Encoding".to_owned(), "gzip".to_owned()));
                a
            }
            "/report.pdf" => Answer::of(200, "application/pdf", b"%PDF-1.4"),
            "/missing" => Answer::not_found(),
            path if path.starts_with("/admin") || path.starts_with("/private") => {
                Answer::html("<p>Staff only</p>")
            }
            path if path.starts_with("/reviews/") => {
                Answer::html("<h1>Reviews</h1><p>Five stars, every one.</p>")
            }
            path => Answer::html(&web_support::shop_page(
                path,
                &format!("<main><h1>{path}</h1><p>A page.</p></main>"),
            )),
        }
    })
}

fn port_of(host: &str) -> &str {
    host.rsplit_once(':').map_or("80", |(_, p)| p)
}

fn block<T>(f: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

/// The run of a workflow whose one step is `step` (YAML, indented under `steps:`), as its history keeps it.
fn go_with(step: &str, effects: &dyn Effects, value: Json) -> Json {
    let yaml = format!("name: Web\ntrigger: manual\nsteps:\n{step}");
    let workflow = Workflow::parse(&yaml).unwrap_or_else(|e| panic!("{e}\n{yaml}"));
    let event = Event {
        value: Value::from_json(&value),
        details: Value::Null,
    };
    block(run(&workflow, &event, effects)).to_json()
}

fn go(step: &str, effects: &dyn Effects) -> Json {
    go_with(step, effects, json!({}))
}

/// The first step's result, the run having gone through.
fn result(record: &Json) -> Json {
    assert_eq!(record["status"], "done", "{record:#}");
    record["steps"][0]["result"].clone()
}

fn live() -> Web {
    Web::new(Net::new(true))
}

#[test]
fn a_journal_post_reads_as_its_article_in_markdown_and_text() {
    let site = shop();
    let url = site.url("/blog/how-we-wax-a-cap");
    let r = result(&go(&format!("  - web: {{ url: \"{url}\" }}\n"), &live()));
    let text = r["text"].as_str().unwrap();
    for wanted in [
        "# How we wax a cap",
        "By Ada Fern, 20 November 2026",
        "Waxing a cap takes an afternoon",
        "## What you need",
        "- A cotton cap, washed and dry\n- Natural wax, about 50 g\n  - beeswax works too\n- A hair dryer",
        "1. Warm the wax until it is soft.\n2. Rub it in, panel by panel.",
        "> Thin coats, many times, beat one thick coat.",
        "Our dryer runs at `60 °C`",
        "```text\npanel 1: 4 min\npanel 2: 4 min\n```",
        "| Cloth | Coats |\n| --- | --- |\n| Cotton | 3 |",
        "[Write to us](http://127.0.0.1:",
        "[a guide to waxes](https://wax.example/guide)",
    ] {
        assert!(text.contains(wanted), "{wanted:?} not in:\n{text}");
    }
    for unwanted in [
        "Skip to content",
        "We use cookies",
        "Journal",
        "Search",
        "Popular",
        "3 comments",
        "Great post",
        "You may also like",
        "All rights reserved",
        "Share",
        "Tags:",
        "never shown",
        "color: green",
        "dataLayer",
    ] {
        assert!(!text.contains(unwanted), "{unwanted:?} in:\n{text}");
    }
    assert_eq!(r["title"], "How we wax a cap | Fern & Thread");
    assert_eq!(
        r["description"],
        "Hand-made caps, totes and mugs from a small workshop."
    );
    assert_eq!(r["language"], "en");
    assert_eq!(r["site_name"], "Fern & Thread");
    assert_eq!(r["status"], 200);
    assert_eq!(r["format"], "markdown");
    assert_eq!(r["feeds"], json!([site.url("/feed.xml")]));
    let links: Vec<&str> = r["links"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["url"].as_str().unwrap())
        .collect();
    assert!(links.contains(&site.url("/about").as_str()), "{links:?}");
    assert!(links.contains(&"https://wax.example/guide"));
    assert!(
        links.iter().all(|l| l.starts_with("http") && !l.contains('#')),
        "{links:?}"
    );
    // The request named OpenDuct.
    let agent = &site.requests()[0].agent;
    assert!(agent.starts_with("OpenDuct/"), "{agent}");

    // As plain text: no Markdown's marks; and with its own User-Agent, and its text cut short.
    let r = result(&go(
        &format!(
            "  - web: {{ url: \"{url}\", format: text, user_agent: \"FernBot/2.0\", max_chars: 200, links: false }}\n"
        ),
        &live(),
    ));
    let text = r["text"].as_str().unwrap();
    assert!(text.starts_with("How we wax a cap\n\nBy Ada Fern"), "{text}");
    assert!(!text.contains('#') && !text.contains("**"));
    assert!(text.chars().count() <= 200);
    assert_eq!(r["cut"], true);
    assert!(r.get("links").is_none());
    assert_eq!(site.requests()[1].agent, "FernBot/2.0");
}

#[test]
fn an_old_style_page_is_read_by_where_its_text_is() {
    let site = shop();
    let r = result(&go(
        &format!("  - web: {{ url: \"{}\", format: text }}\n", site.url("/news")),
        &live(),
    ));
    let text = r["text"].as_str().unwrap();
    assert!(text.starts_with("The kiln is fixed"), "{text}");
    assert!(text.contains("orders from October will ship this week"));
    assert!(text.contains("New glazes, in green and in sand"));
    for unwanted in ["Archive", "Friends", "Mill Lane", "Contact"] {
        assert!(!text.contains(unwanted), "{unwanted:?} in:\n{text}");
    }
    // All of it, when asked; or what one part holds.
    let all = result(&go(
        &format!(
            "  - web: {{ url: \"{}\", content: all, format: text }}\n",
            site.url("/news")
        ),
        &live(),
    ));
    assert!(all["text"].as_str().unwrap().contains("Mill Lane"));
    let within = result(&go(
        &format!(
            "  - web: {{ url: \"{}\", within: \"td.menu\", format: text }}\n",
            site.url("/news")
        ),
        &live(),
    ));
    assert_eq!(within["text"], "Archive\nLinks\nFriends");
}

#[test]
fn select_picks_fields_out_of_a_page() {
    let site = shop();
    let step = format!(
        r#"  - name: shop
    web:
      action: select
      url: "{}"
      fields:
        heading: h1
        products:
          selector: .card
          all: true
          fields:
            name: .name
            price: .price
            link: {{ selector: a, attribute: href }}
            picture: {{ selector: img, attribute: src }}
        skus: {{ selector: "[data-sku]", attribute: data-sku, all: true }}
        on_sale: "li.sale .name"
        intro: {{ selector: ".hero p", as: markdown }}
        nothing: table
"#,
        site.url("/")
    );
    let r = result(&go(&step, &live()));
    let f = &r["fields"];
    assert_eq!(f["heading"], "Made by hand, made to last");
    assert_eq!(f["products"].as_array().unwrap().len(), 3);
    assert_eq!(
        f["products"][0],
        json!({ "name": "Waxed cap", "price": "£24.00", "link": site.url("/products/cap"), "picture": site.url("/img/cap.jpg") })
    );
    assert_eq!(f["skus"], json!(["CAP-01", "TOTE-02", "MUG-03"]));
    assert_eq!(f["on_sale"], "Canvas tote");
    assert!(
        f["intro"]
            .as_str()
            .unwrap()
            .starts_with("Every cap, tote and mug")
    );
    assert_eq!(f["nothing"], Json::Null);
    assert_eq!(r["title"], "Home | Fern & Thread");

    // A wrong selector is found when the workflow is read, and said plainly.
    let wrong = Workflow::parse("name: W\nsteps:\n  - web: { action: select, url: \"https://x.example/\", fields: { a: \"li:hover\" } }\n")
        .unwrap_err();
    assert!(
        wrong.contains("fields.a") && wrong.contains("not a pseudo-class"),
        "{wrong}"
    );
}

#[test]
fn robots_txt_keeps_a_crawl_out_until_it_is_turned_off() {
    let site = shop();
    let step = format!(
        "  - web: {{ action: crawl, url: \"{}\", depth: 1, max_pages: 20, rate: 100ms }}\n",
        site.url("/")
    );
    let r = result(&go(&step, &live()));
    let pages: Vec<&str> = r["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["url"].as_str().unwrap())
        .collect();
    assert!(pages.contains(&site.url("/products/cap").as_str()), "{pages:?}");
    assert!(pages.contains(&site.url("/about").as_str()));
    let paths = site.paths();
    assert_eq!(
        paths.iter().filter(|p| *p == "/robots.txt").count(),
        1,
        "{paths:?}"
    );
    assert!(
        !paths.iter().any(|p| p.starts_with("/admin")),
        "robots.txt keeps it out: {paths:?}"
    );
    assert!(
        !paths.iter().any(|p| p.ends_with(".pdf")),
        "a PDF is not a page: {paths:?}"
    );
    let skipped = r["skipped"].as_array().unwrap();
    assert!(
        skipped
            .iter()
            .any(|s| s["url"] == site.url("/admin/orders")
                && s["why"].as_str().unwrap().contains("robots.txt")),
        "{skipped:?}"
    );
    assert!(site.requests().iter().all(|a| a.agent.starts_with("OpenDuct/")));
    // Each page as fetch gives it, with its depth; a crawl gives no links unless asked.
    let first = &r["pages"][0];
    assert_eq!(first["depth"], 0);
    assert_eq!(first["title"], "Home | Fern & Thread");
    assert!(first.get("links").is_none());

    // Turned off, the crawl reads what robots.txt keeps out.
    let site = shop();
    let step = format!(
        "  - web: {{ action: crawl, url: \"{}\", robots: false, max_pages: 20, rate: 100ms }}\n",
        site.url("/")
    );
    result(&go(&step, &live()));
    let paths = site.paths();
    assert!(paths.iter().any(|p| p == "/admin/orders"), "{paths:?}");
    assert!(!paths.iter().any(|p| p == "/robots.txt"));
}

#[test]
fn a_crawl_keeps_to_its_depth_its_pages_its_patterns_and_a_polite_rate() {
    let site = shop();
    let crawl = |extra: &str| {
        let step = format!(
            "  - web: {{ action: crawl, url: \"{}\", rate: 100ms{extra} }}\n",
            site.url("/")
        );
        result(&go(&step, &live()))
    };
    let r = crawl(", depth: 0");
    assert_eq!(r["count"], 1);
    let r = crawl(", depth: 2, max_pages: 50");
    let urls: Vec<String> = r["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["url"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        urls.contains(&site.url("/reviews/waxed-cap")),
        "two links from home: {urls:?}"
    );
    let depth_of = |u: &str| {
        r["pages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["url"] == u)
            .unwrap()["depth"]
            .clone()
    };
    assert_eq!(depth_of(&site.url("/reviews/waxed-cap")), 2);
    assert_eq!(depth_of(&site.url("/products/cap")), 1);
    let r = crawl(", depth: 2, max_pages: 3");
    assert_eq!(r["count"], 3);
    assert_eq!(r["stopped"], "max_pages");
    let r = crawl(", depth: 2, max_pages: 50, include: [\"*/products/*\"]");
    let urls: Vec<&str> = r["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["url"].as_str().unwrap())
        .collect();
    assert_eq!(
        urls.len(),
        5,
        "the start, the shop's own page and the three products: {urls:?}"
    );
    assert!(urls.iter().skip(1).all(|u| u.contains("/products/")));
    let r = crawl(", depth: 1, exclude: [\"*/products/*\"], max_pages: 50");
    assert!(
        r["pages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| !p["url"].as_str().unwrap().contains("/products/"))
    );

    // One host at the rate asked: the pages' requests at least that far apart.
    let site = shop();
    let step = format!(
        "  - web: {{ action: crawl, url: \"{}\", rate: 300ms, max_pages: 4 }}\n",
        site.url("/")
    );
    let r = result(&go(&step, &live()));
    assert_eq!(r["count"], 4);
    let times: Vec<_> = site
        .requests()
        .iter()
        .filter(|a| a.path != "/robots.txt")
        .map(|a| a.at)
        .collect();
    for pair in times.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(290),
            "{:?}",
            pair[1] - pair[0]
        );
    }
    // The crawl's own time limit stops it.
    let step = format!(
        "  - web: {{ action: crawl, url: \"{}\", rate: 2s, time: 1s, max_pages: 50 }}\n",
        site.url("/")
    );
    let r = result(&go(&step, &live()));
    assert_eq!(r["stopped"], "time");
    assert_eq!(r["count"], 1);
}

#[test]
fn a_sitemap_index_is_read_through_robots_txt_and_feeds_a_crawl() {
    let site = shop();
    let r = result(&go(
        &format!("  - web: {{ action: sitemap, url: \"{}\" }}\n", site.url("/")),
        &live(),
    ));
    let urls: Vec<&str> = r["urls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["url"].as_str().unwrap())
        .collect();
    assert_eq!(urls.len(), 6, "{urls:?}");
    assert!(
        urls.contains(&site.url("/products/mug").as_str()),
        "from the gzipped sitemap: {urls:?}"
    );
    assert_eq!(r["urls"][0]["modified"], "2026-11-20");
    assert_eq!(r["urls"][0]["changes"], "daily");
    assert_eq!(r["sitemaps"].as_array().unwrap().len(), 3);
    let r = result(&go(
        &format!(
            "  - web: {{ action: sitemap, url: \"{}\", max_urls: 2 }}\n",
            site.url("/sitemap_index.xml")
        ),
        &live(),
    ));
    assert_eq!(r["count"], 2);

    // A crawl that starts from the sitemap, and follows no links.
    let site = shop();
    let step = format!(
        "  - web: {{ action: crawl, url: \"{}\", sitemap: true, depth: 1, max_pages: 50, rate: 100ms }}\n",
        site.url("/news")
    );
    let r = result(&go(&step, &live()));
    let urls: Vec<&str> = r["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["url"].as_str().unwrap())
        .collect();
    assert!(
        urls.contains(&site.url("/blog/how-we-wax-a-cap").as_str()),
        "{urls:?}"
    );
    assert!(urls.contains(&site.url("/products/tote").as_str()));
}

#[test]
fn rss_and_atom_feeds_are_read_and_a_page_leads_to_its_feed() {
    let site = shop();
    let r = result(&go(
        &format!(
            "  - web: {{ action: feed, url: \"{}\" }}\n",
            site.url("/feed.xml")
        ),
        &live(),
    ));
    assert_eq!(r["title"], "Fern & Thread Journal");
    assert_eq!(r["link"], site.url("/blog/"));
    assert_eq!(r["count"], 2);
    let first = &r["items"][0];
    assert_eq!(first["title"], "How we wax a cap");
    assert_eq!(first["id"], site.url("/blog/how-we-wax-a-cap"));
    assert_eq!(first["link"], site.url("/blog/how-we-wax-a-cap"));
    assert_eq!(first["date"], "2026-11-20T09:00:00Z");
    assert_eq!(first["summary"], "Waxing a cap takes an afternoon.");
    assert_eq!(
        first["content"],
        "Waxing a cap takes an afternoon, a pot of wax and patience.\n\n- Warm it"
    );
    assert_eq!(first["author"], "Ada Fern");
    assert_eq!(first["categories"], json!(["care", "caps"]));
    let second = &r["items"][1];
    assert_eq!(second["id"], site.url("/blog/a"), "no guid: known by its link");
    assert_eq!(second["summary"], "Handles, glazes & sizes.");
    assert_eq!(
        second["enclosure"],
        json!({ "url": site.url("/audio/mugs.mp3"), "type": "audio/mpeg" })
    );

    let r = result(&go(
        &format!(
            "  - web: {{ action: feed, url: \"{}\" }}\n",
            site.url("/atom.xml")
        ),
        &live(),
    ));
    let entry = &r["items"][0];
    assert_eq!(entry["title"], "How we wax a cap");
    assert_eq!(entry["id"], "tag:fern.example,2026:wax");
    assert_eq!(entry["link"], site.url("/blog/how-we-wax-a-cap"));
    assert_eq!(entry["date"], "2026-11-20T09:00:00Z");
    assert_eq!(entry["summary"], "Waxing a cap takes an afternoon.");
    assert_eq!(entry["content"], "Waxing takes patience.");
    assert_eq!(entry["author"], "Ada Fern");
    assert_eq!(entry["enclosure"]["url"], site.url("/img/cap.jpg"));

    // A shop's page that offers a feed: its feed is read.
    let r = result(&go(
        &format!(
            "  - web: {{ action: feed, url: \"{}\", max_items: 1 }}\n",
            site.url("/")
        ),
        &live(),
    ));
    assert_eq!(r["url"], site.url("/feed.xml"));
    assert_eq!(r["count"], 1);
}

#[test]
fn redirects_are_followed_counted_and_never_to_this_machine() {
    let site = shop();
    let port = site.address.port();
    // The guard on, as an installation has it: the stand-in site reached by a name pinned to it.
    let guarded = Web::new(Net::new(false).pin("shop.example", site.address));
    let url = |path: &str| format!("http://shop.example:{port}{path}");
    let r = result(&go(
        &format!("  - web: {{ url: \"{}\" }}\n", url("/redirect")),
        &guarded,
    ));
    assert_eq!(r["url"], url("/blog/how-we-wax-a-cap"));
    assert_eq!(r["redirects"], 1);
    for (path, says) in [("/to-private", "own network"), ("/to-localhost", "own network")] {
        let record = go(&format!("  - web: {{ url: \"{}\" }}\n", url(path)), &guarded);
        assert_eq!(record["status"], "failed");
        assert!(
            record["error"].as_str().unwrap().contains(says),
            "{path}: {record}"
        );
    }
    assert!(!site.paths().iter().any(|p| p == "/secret"), "{:?}", site.paths());
    let record = go(
        &format!("  - web: {{ url: \"{}\", max_redirects: 2 }}\n", url("/loop")),
        &guarded,
    );
    assert!(
        record["error"]
            .as_str()
            .unwrap()
            .contains("more than 2 redirects"),
        "{record}"
    );
    let record = go(&format!("  - web: {{ url: \"{}\" }}\n", site.url("/")), &guarded);
    assert!(
        record["error"]
            .as_str()
            .unwrap()
            .contains("--allow-private-network"),
        "{record}"
    );
    // A crawl and a feed are held to the same guard.
    let record = go(
        &format!("  - web: {{ action: feed, url: \"{}\" }}\n", url("/to-private")),
        &guarded,
    );
    assert!(
        record["error"].as_str().unwrap().contains("own network"),
        "{record}"
    );
}

#[test]
fn limits_on_bytes_time_and_what_is_not_a_page_hold() {
    let site = shop();
    let record = go(
        &format!("  - web: {{ url: \"{}\", max_bytes: 100KB }}\n", site.url("/big")),
        &live(),
    );
    assert!(
        record["error"]
            .as_str()
            .unwrap()
            .contains("larger than 102400 bytes"),
        "{record}"
    );
    let record = go(
        &format!("  - web: {{ url: \"{}\", timeout: 1s }}\n", site.url("/slow")),
        &live(),
    );
    assert!(
        record["error"].as_str().unwrap().contains("no answer in 1s"),
        "{record}"
    );
    let record = go(
        &format!("  - web: {{ url: \"{}\" }}\n", site.url("/report.pdf")),
        &live(),
    );
    assert!(
        record["error"]
            .as_str()
            .unwrap()
            .contains("application/pdf, not a web page"),
        "{record}"
    );
    let record = go(
        &format!("  - web: {{ url: \"{}\" }}\n", site.url("/missing")),
        &live(),
    );
    assert!(
        record["error"].as_str().unwrap().contains("answered 404"),
        "{record}"
    );
    let r = result(&go(
        &format!(
            "  - web: {{ url: \"{}\", fail_on_status: false }}\n",
            site.url("/missing")
        ),
        &live(),
    ));
    assert_eq!(r["status"], 404);
    for (wrong, says) in [
        ("{ url: \"ftp://x.example/\" }", "only http and https"),
        ("{ url: \"https://x.example/\", max_bytes: 50MB }", "at most 10MB"),
        ("{ url: \"https://x.example/\", timeout: 10m }", "timeout"),
        (
            "{ action: crawl, url: \"https://x.example/\", max_pages: 1000 }",
            "1 to 200",
        ),
        (
            "{ action: crawl, url: \"https://x.example/\", depth: 9 }",
            "0 to 5",
        ),
        (
            "{ action: crawl, url: \"https://x.example/\", html: file }",
            "html: true",
        ),
        (
            "{ action: crawl, url: \"https://x.example/\", headers: { a: b } }",
            "not a setting of web's crawl",
        ),
        (
            "{ action: grab, url: \"https://x.example/\" }",
            "fetch, select, crawl, sitemap, feed",
        ),
        (
            "{ url: \"https://x.example/\", headers: { Host: x } }",
            "not a header a step sets",
        ),
        (
            "{ action: select, url: \"https://x.example/\" }",
            "the fields to pick out",
        ),
        (
            "{ format: html, url: \"https://x.example/\" }",
            "markdown or text",
        ),
    ] {
        let e = Workflow::parse(&format!("name: W\nsteps:\n  - web: {wrong}\n")).unwrap_err();
        assert!(e.contains(says), "{wrong}: {e}");
    }
}

#[test]
fn character_sets_and_gzip_are_read() {
    let site = shop();
    let r = result(&go(
        &format!("  - web: {{ url: \"{}\", format: text }}\n", site.url("/latin1")),
        &live(),
    ));
    assert_eq!(r["title"], "Café");
    assert_eq!(r["text"], "Crème brûlée");
    let r = result(&go(
        &format!("  - web: {{ url: \"{}\" }}\n", site.url("/gz")),
        &live(),
    ));
    assert!(r["text"].as_str().unwrap().starts_with("# How we wax a cap"));
}

#[test]
fn a_page_the_run_has_is_read_and_a_page_is_kept_as_a_file() {
    let html = web_support::post();
    let step = "  - web: { action: select, from: value.html, url: \"https://fern.example/blog/x\", fields: { title: h1, link: { selector: \".entry-content a\", attribute: href } } }\n";
    let r = result(&go_with(step, &DryRun::default(), json!({ "html": html })));
    assert_eq!(r["fields"]["title"], "How we wax a cap");
    assert_eq!(r["fields"]["link"], "https://fern.example/contact");
    assert!(
        r.get("test").is_none(),
        "a page the run has is read for real, even in a test run"
    );

    let site = shop();
    let r = result(&go(
        &format!(
            "  - web: {{ url: \"{}\", html: file, links: false, images: false }}\n",
            site.url("/blog/how-we-wax-a-cap")
        ),
        &live(),
    ));
    assert_eq!(r["file"]["name"], "how-we-wax-a-cap.html");
    assert_eq!(r["file"]["type"], "text/html");
    assert_eq!(r["file"]["size"], web_support::post().len());
    let r = result(&go(
        &format!("  - web: {{ url: \"{}\", html: true }}\n", site.url("/news")),
        &live(),
    ));
    assert!(r["html"].as_str().unwrap().contains("<td class=\"menu\">"));
}

#[test]
fn a_test_run_fetches_nothing_and_a_preview_does() {
    let site = shop();
    let page = format!("  - web: {{ url: \"{}\" }}\n", site.url("/blog/how-we-wax-a-cap"));
    let r = result(&go(&page, &DryRun::default()));
    assert_eq!(r["test"], "sample page, nothing fetched");
    assert_eq!(r["title"], "A sample page");
    assert_eq!(r["url"], site.url("/blog/how-we-wax-a-cap"));
    assert!(r["text"].as_str().unwrap().starts_with("# A sample page"));
    for action in ["crawl", "sitemap", "feed"] {
        let r = result(&go(
            &format!("  - web: {{ action: {action}, url: \"{}\" }}\n", site.url("/")),
            &DryRun::default(),
        ));
        assert_eq!(r["test"], "sample page, nothing fetched", "{action}");
    }
    let feed = result(&go(
        &format!(
            "  - web: {{ action: feed, url: \"{}\" }}\n",
            site.url("/feed.xml")
        ),
        &DryRun::default(),
    ));
    assert_eq!(feed["items"][0]["title"], "The newer item");
    assert!(
        site.paths().is_empty(),
        "a test run sent nothing: {:?}",
        site.paths()
    );

    let preview = Preview::new(Arc::new(live()));
    let r = result(&go(&page, &preview));
    assert_eq!(r["title"], "How we wax a cap | Fern & Thread");
    assert_eq!(site.paths(), ["/blog/how-we-wax-a-cap"]);
}

#[test]
fn the_step_is_an_agents_tool_and_the_model_gives_the_address() {
    let site = shop();
    let workflow = r#"  - name: helper
    agent:
      credential: model
      input: "{{ value.ask }}"
      tools:
        - name: read_page
          description: Reads one web page and gives its main text as Markdown, its title and its links.
          web: { action: fetch, max_chars: 4000, links: false, images: false }
"#;
    let effects = live();
    let call = json!({
        "model": "small-1",
        "choices": [{ "finish_reason": "tool_calls", "message": { "role": "assistant", "content": null, "tool_calls": [
            { "id": "c1", "type": "function", "function": { "name": "read_page",
              "arguments": json!({ "url": site.url("/blog/how-we-wax-a-cap") }).to_string() } }
        ] } }],
        "usage": { "prompt_tokens": 10, "completion_tokens": 5 },
    });
    let done = json!({
        "model": "small-1",
        "choices": [{ "finish_reason": "stop", "message": { "role": "assistant", "content": "It takes an afternoon." } }],
        "usage": { "prompt_tokens": 10, "completion_tokens": 5 },
    });
    effects.answers.lock().unwrap().extend([call, done]);
    let record = go_with(
        workflow,
        &effects,
        json!({ "ask": "How long does waxing a cap take?" }),
    );
    let r = result(&record);
    assert_eq!(r["text"], "It takes an afternoon.");
    let asked = effects.asked.lock().unwrap().clone();
    let tools = asked[0]["tools"].to_string();
    assert!(
        tools.contains("read_page") && tools.contains("\"url\""),
        "the model is asked for the address: {tools}"
    );
    let second = asked[1].to_string();
    assert!(
        second.contains("How we wax a cap"),
        "the page went back to the model"
    );
    assert_eq!(site.paths(), ["/blog/how-we-wax-a-cap"]);
}
