// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The studio's page: the files `npm run build` writes to `ui/dist`, built into the program (see `build.rs`). A
//! program built without them still runs, and its page says how to build them.

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Response, StatusCode};

use crate::http::Answer;

// `FILES`: (path, bytes) for every file of `ui/dist`, made by build.rs.
include!(concat!(env!("OUT_DIR"), "/assets.rs"));

const NOT_BUILT: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>OpenDuct</title>
<meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="font-family: system-ui, sans-serif; max-width: 40rem; margin: 4rem auto; padding: 0 1rem; line-height: 1.5">
<h1>OpenDuct is running</h1>
<p>This program was built without the studio's page. To build it in, run <code>npm ci &amp;&amp; npm run build</code>
in the <code>ui</code> folder, then build OpenDuct again. The API answers at <code>/api/status</code>.</p>
</body></html>"#;

pub(crate) fn built() -> bool {
    !FILES.is_empty()
}

pub(crate) fn serve(path: &str) -> Answer {
    if !built() {
        return file(
            StatusCode::OK,
            "text/html; charset=utf-8",
            Bytes::from_static(NOT_BUILT.as_bytes()),
            false,
        );
    }
    let wanted = if path == "/" { "/index.html" } else { path };
    if let Some((_, bytes)) = FILES.iter().find(|(p, _)| *p == wanted) {
        // Vite names what it builds in assets/ by their content: they never change, so they are kept for a year.
        let lasting = wanted.starts_with("/assets/");
        return file(
            StatusCode::OK,
            content_type(wanted),
            Bytes::from_static(bytes),
            lasting,
        );
    }
    // A page of the app (`/workflows/7`): the app itself, which shows it. A missing file is a 404.
    let last = wanted.rsplit('/').next().unwrap_or_default();
    match FILES.iter().find(|(p, _)| *p == "/index.html") {
        Some((_, bytes)) if !last.contains('.') => file(
            StatusCode::OK,
            "text/html; charset=utf-8",
            Bytes::from_static(bytes),
            false,
        ),
        _ => crate::http::problem(StatusCode::NOT_FOUND, "no such file"),
    }
}

fn file(status: StatusCode, content_type: &'static str, body: Bytes, lasting: bool) -> Answer {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    let cache = if lasting {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    response
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" | "map" => "application/json",
        "woff2" => "font/woff2",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
