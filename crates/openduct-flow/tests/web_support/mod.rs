// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Stand-in web sites on 127.0.0.1 for the web step's tests: a small HTTP/1.1 server whose answers a test gives (by
//! path, with the host asked), every request kept with its time and User-Agent. And the pages of a made-up shop and
//! its blog, written for these tests to look like the pages sites serve: navigation, banners, sidebars, comments,
//! cookie notices and footers around what a person reads.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A request the site was sent.
#[derive(Debug, Clone)]
pub(crate) struct Asked {
    pub(crate) path: String,
    pub(crate) host: String,
    pub(crate) agent: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) at: Instant,
}

/// What the site answers.
#[derive(Debug, Clone)]
pub(crate) struct Answer {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl Answer {
    pub(crate) fn html(body: &str) -> Self {
        Self::of(200, "text/html; charset=utf-8", body.as_bytes())
    }

    pub(crate) fn of(status: u16, kind: &str, body: &[u8]) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".to_owned(), kind.to_owned())],
            body: body.to_vec(),
        }
    }

    pub(crate) fn redirect(to: &str) -> Self {
        Self {
            status: 302,
            headers: vec![("Location".to_owned(), to.to_owned())],
            body: Vec::new(),
        }
    }

    pub(crate) fn not_found() -> Self {
        Self::of(404, "text/plain", b"not found")
    }
}

type Pages = dyn Fn(&Asked) -> Answer + Send + Sync;

/// A stand-in site, serving until the test ends.
pub(crate) struct Site {
    pub(crate) address: SocketAddr,
    pub(crate) asked: Arc<Mutex<Vec<Asked>>>,
}

impl Site {
    /// A site whose answers `pages` gives.
    pub(crate) fn start(pages: impl Fn(&Asked) -> Answer + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let asked = Arc::new(Mutex::new(Vec::new()));
        let pages: Arc<Pages> = Arc::new(pages);
        let kept = Arc::clone(&asked);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (pages, kept) = (Arc::clone(&pages), Arc::clone(&kept));
                std::thread::spawn(move || serve(stream, &pages, &kept));
            }
        });
        Self { address, asked }
    }

    /// The address of `path` on this site.
    pub(crate) fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.address)
    }

    /// The paths asked for, in order.
    pub(crate) fn paths(&self) -> Vec<String> {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .map(|a| a.path.clone())
            .collect()
    }

    pub(crate) fn requests(&self) -> Vec<Asked> {
        self.asked.lock().unwrap().clone()
    }
}

fn serve(stream: TcpStream, pages: &Arc<Pages>, kept: &Arc<Mutex<Vec<Asked>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).is_err() || h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_owned()));
        }
    }
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let asked = Asked {
        path,
        host: get("host"),
        agent: get("user-agent"),
        headers: headers.clone(),
        at: Instant::now(),
    };
    kept.lock().unwrap().push(asked.clone());
    let answer = pages(&asked);
    let mut out = stream;
    let mut head = format!(
        "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n",
        answer.status,
        answer.body.len()
    );
    for (k, v) in &answer.headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let _ = out.write_all(head.as_bytes());
    let _ = out.write_all(&answer.body);
    let _ = out.flush();
    let mut rest = Vec::new();
    let _ = reader.read_to_end(&mut rest);
}

/// Bytes gzipped.
pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut packer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    packer.write_all(bytes).unwrap();
    packer.finish().unwrap()
}

/// The shop's navigation, header, banner and footer, around `main`.
pub(crate) fn shop_page(title: &str, main: &str) -> String {
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title} | Fern &amp; Thread</title>
  <meta name="description" content="Hand-made caps, totes and mugs from a small workshop.">
  <meta property="og:site_name" content="Fern &amp; Thread">
  <link rel="stylesheet" href="/static/site.css">
  <link rel="alternate" type="application/rss+xml" title="Journal" href="/feed.xml">
  <script>window.dataLayer = [];  if (a < b) {{ document.write("<p>never shown</p>"); }}</script>
  <style>.price {{ color: green }}</style>
</head>
<body class="page">
  <a class="skip-link" href="#main">Skip to content</a>
  <div class="cookie-banner" role="dialog">We use cookies to run this shop. <button>Accept</button></div>
  <header class="site-header">
    <a href="/" class="logo">Fern &amp; Thread</a>
    <nav aria-label="Main"><ul><li><a href="/">Home</a></li><li><a href="/products/">Shop</a></li>
      <li><a href="/blog/">Journal</a></li><li><a href="/about">About</a></li></ul></nav>
    <form class="search" action="/search"><input name="q" placeholder="Search"><button>Go</button></form>
  </header>
  {main}
  <footer class="site-footer">
    <p>&copy; 2026 Fern &amp; Thread. All rights reserved.</p>
    <ul><li><a href="/privacy">Privacy</a></li><li><a href="/terms">Terms</a></li></ul>
  </footer>
  <script src="/static/app.js"></script>
</body>
</html>"##
    )
}

/// The shop's home page: its products, a link to its admin pages (robots.txt keeps crawlers out) and a catalogue PDF.
pub(crate) fn home() -> String {
    shop_page(
        "Home",
        r#"<main id="main">
    <section class="hero"><h1>Made by hand, made to last</h1>
      <p>Every cap, tote and mug is cut, sewn or thrown in our workshop, a few at a time, from materials we can name.</p></section>
    <section class="products"><h2>This season</h2>
      <ul class="grid">
        <li class="card" data-sku="CAP-01"><a href="/products/cap"><img src="/img/cap.jpg" alt="A green waxed cap"><h3 class="name">Waxed cap</h3></a><span class="price">£24.00</span></li>
        <li class="card sale" data-sku="TOTE-02"><a href="/products/tote"><img src="/img/tote.jpg" alt="A canvas tote"><h3 class="name">Canvas tote</h3></a><span class="price">£18.50</span></li>
        <li class="card" data-sku="MUG-03"><a href="/products/mug"><img src="/img/mug.jpg" alt="A stoneware mug"><h3 class="name">Stoneware mug</h3></a><span class="price">£12.00</span></li>
      </ul>
    </section>
    <p><a href="/files/catalogue.pdf">The catalogue (PDF)</a> · <a href="/admin/orders">Staff</a> · <a href="https://elsewhere.example/partner">A partner</a></p>
  </main>"#,
    )
}

/// A product page, linking to its reviews (only reachable two links from home).
pub(crate) fn product(name: &str, price: &str) -> String {
    shop_page(
        name,
        &format!(
            r#"<main id="main"><nav class="breadcrumb"><a href="/">Home</a> / <a href="/products/">Shop</a></nav>
    <article class="product"><h1>{name}</h1><p class="price">{price}</p>
    <p>Cut from one piece of cloth, waxed by hand, and finished with a brass buckle that will outlive the rest of it.</p>
    <p><a href="/reviews/{slug}">Read the reviews</a></p></article></main>"#,
            slug = name.to_ascii_lowercase().replace(' ', "-")
        ),
    )
}

/// A journal post: the article a person reads, among share buttons, a sidebar, comments and related posts.
pub(crate) fn post() -> String {
    shop_page(
        "How we wax a cap",
        r#"<div class="layout">
    <main id="main">
      <article class="post h-entry">
        <header class="entry-header"><h1 class="entry-title">How we wax a cap</h1>
          <p class="byline">By Ada Fern, <time datetime="2026-11-20">20 November 2026</time></p></header>
        <div class="share-buttons"><a href="https://social.example/share?u=1">Share</a> <a href="https://chat.example/s">Post</a> <a href="mailto:?subject=x">Mail</a></div>
        <div class="entry-content">
          <p>Waxing a cap takes an afternoon, a pot of wax and more patience than you would think. Here is how we do it, step by step, so yours lasts as long as ours.</p>
          <h2>What you need</h2>
          <ul><li>A cotton cap, washed and dry</li><li>Natural wax, about 50 g<ul><li>beeswax works too</li></ul></li><li>A hair dryer</li></ul>
          <h2>The steps</h2>
          <ol><li>Warm the wax until it is soft.</li><li>Rub it in, panel by panel.</li><li>Melt it into the cloth with the dryer.</li></ol>
          <blockquote><p>Thin coats, many times, beat one thick coat.</p></blockquote>
          <p>Our dryer runs at <code>60 °C</code>; the times we keep look like this:</p>
          <pre><code class="language-text">panel 1: 4 min
panel 2: 4 min</code></pre>
          <table><thead><tr><th>Cloth</th><th>Coats</th></tr></thead><tbody><tr><td>Cotton</td><td>3</td></tr><tr><td>Linen</td><td>4</td></tr></tbody></table>
          <p>Questions? <a href="/contact">Write to us</a>, and see <a href="https://wax.example/guide">a guide to waxes</a>.</p>
        </div>
        <footer class="entry-footer">Tags: <a href="/tag/care">care</a>, <a href="/tag/caps">caps</a></footer>
      </article>
      <section class="related-posts"><h2>You may also like</h2><ul><li><a href="/blog/a">Choosing a mug</a></li><li><a href="/blog/b">Totes for the market</a></li><li><a href="/blog/c">A year of caps</a></li></ul></section>
      <section id="comments" class="comments"><h2>3 comments</h2><p>Great post, thanks!</p></section>
    </main>
    <aside class="sidebar"><h2>Popular</h2><p>The canvas tote is back in stock, for a while.</p></aside>
  </div>"#,
    )
}

/// A news page of old-style markup: no article or main, only a block of paragraphs among others.
pub(crate) fn old_news() -> String {
    r#"<html><head><title>Workshop news</title></head><body>
<div id="top"><a href="/">Home</a> | <a href="/news">News</a> | <a href="/contact">Contact</a></div>
<table width="100%"><tr><td class="menu"><a href="/a">Archive</a><br><a href="/b">Links</a><br><a href="/c">Friends</a></td>
<td><div id="story"><font size="5"><b>The kiln is fixed</b></font>
<p>After three weeks without it, the kiln is mended, tested, and back to firing mugs, so orders from October will ship this week.</p>
<p>We thank everyone who waited, wrote, and asked after it, and we are sorry that it took as long as it did.</p>
<p>New glazes, in green and in sand, follow in December, once the first firings come out as we hope they will.</p>
</div></td></tr></table>
<div class="footer">Workshop, 1 Mill Lane. <a href="/privacy">Privacy</a></div></body></html>"#
        .to_owned()
}

/// The shop's robots.txt: its admin pages closed to every crawler, and its private pages to OpenDuct's.
pub(crate) fn robots(host: &str) -> String {
    format!(
        "# Fern & Thread\nUser-agent: *\nDisallow: /admin/\n\nUser-agent: OpenDuct\nDisallow: /admin/\nDisallow: \
         /private\n\nSitemap: http://{host}/sitemap_index.xml\n"
    )
}

/// The shop's sitemap index, and its two sitemaps (the second gzipped).
pub(crate) fn sitemap_index(host: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>http://{host}/sitemap-pages.xml</loc><lastmod>2026-11-20</lastmod></sitemap>
  <sitemap><loc>http://{host}/sitemap-products.xml.gz</loc></sitemap>
</sitemapindex>"#
    )
}

pub(crate) fn sitemap_pages(host: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://{host}/</loc><lastmod>2026-11-20</lastmod><changefreq>daily</changefreq><priority>1.0</priority></url>
  <url><loc>http://{host}/about</loc></url>
  <url><loc>http://{host}/blog/how-we-wax-a-cap</loc><lastmod>2026-11-20T09:00:00+00:00</lastmod></url>
</urlset>"#
    )
}

pub(crate) fn sitemap_products(host: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://{host}/products/cap</loc></url><url><loc>http://{host}/products/tote</loc></url>
  <url><loc>http://{host}/products/mug</loc></url>
</urlset>"#
    )
}

/// The journal's RSS feed, items newest first; `extra` adds items before the rest (the newest).
pub(crate) fn rss(host: &str, extra: &[(&str, &str)]) -> String {
    let mut items = String::new();
    for (id, title) in extra {
        items.push_str(&format!(
            "<item><title>{title}</title><link>http://{host}/blog/{id}</link><guid isPermaLink=\"false\">{id}</guid>\
             <pubDate>Fri, 27 Nov 2026 12:00:00 GMT</pubDate><description>{title}, in short.</description></item>"
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
  <title>Fern &amp; Thread Journal</title>
  <link>http://{host}/blog/</link>
  <atom:link href="http://{host}/feed.xml" rel="self" type="application/rss+xml"/>
  <description>Notes from the workshop</description>
  {items}
  <item>
    <title>How we wax a cap</title>
    <link>http://{host}/blog/how-we-wax-a-cap</link>
    <guid>http://{host}/blog/how-we-wax-a-cap</guid>
    <pubDate>Fri, 20 Nov 2026 09:00:00 +0000</pubDate>
    <dc:creator>Ada Fern</dc:creator>
    <category>care</category><category>caps</category>
    <description><![CDATA[<p>Waxing a cap takes an <b>afternoon</b>.</p>]]></description>
    <content:encoded><![CDATA[<p>Waxing a cap takes an afternoon, a pot of wax and patience.</p><ul><li>Warm it</li></ul>]]></content:encoded>
  </item>
  <item>
    <title>Choosing a mug</title>
    <link>http://{host}/blog/a</link>
    <pubDate>Tue, 10 Nov 2026 08:00:00 GMT</pubDate>
    <description>Handles, glazes &amp; sizes.</description>
    <enclosure url="http://{host}/audio/mugs.mp3" length="1234" type="audio/mpeg"/>
  </item>
</channel>
</rss>"#
    )
}

/// The journal as an Atom feed.
pub(crate) fn atom(host: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title type="text">Fern &amp; Thread Journal</title>
  <link href="http://{host}/blog/"/>
  <link rel="self" href="http://{host}/atom.xml"/>
  <updated>2026-11-20T09:00:00Z</updated>
  <id>urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6</id>
  <entry>
    <title type="html">How we wax a &lt;em&gt;cap&lt;/em&gt;</title>
    <link rel="alternate" type="text/html" href="/blog/how-we-wax-a-cap"/>
    <link rel="enclosure" type="image/jpeg" href="/img/cap.jpg"/>
    <id>tag:fern.example,2026:wax</id>
    <published>2026-11-20T10:00:00+01:00</published>
    <updated>2026-11-21T09:00:00Z</updated>
    <author><name>Ada Fern</name></author>
    <category term="care"/>
    <summary>Waxing a cap takes an afternoon.</summary>
    <content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p>Waxing takes <b>patience</b>.</p></div></content>
  </entry>
</feed>"#
    )
}
