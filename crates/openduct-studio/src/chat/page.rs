// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The chat's page, made here from the trigger's words and colours: every word escaped, every colour checked to be
//! `#rrggbb` when the workflow was read, the script and the style from this address and nowhere else, and a policy
//! that lets only the studio and the sites the trigger names show it in a frame.

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Response, StatusCode};
use openduct_flow::chat::Chat;

use super::{Found, boxed};
use crate::http::Streamed;

const SCRIPT: &str = include_str!("chat.js");
const EMBED: &str = include_str!("embed.js");
const STYLE: &str = include_str!("chat.css");

/// Text as HTML shows it, in an element or an attribute.
pub(super) fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Who may show the page in a frame: the studio, and the sites the trigger names.
fn policy(chat: &Chat) -> String {
    let mut ancestors = vec!["'self'".to_owned()];
    ancestors.extend(chat.origins.iter().cloned());
    format!(
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; \
         base-uri 'none'; form-action 'none'; frame-ancestors {}",
        ancestors.join(" ")
    )
}

fn answer(
    status: StatusCode,
    kind: &'static str,
    cache: &'static str,
    body: String,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(kind));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    response
}

/// The page: open, or closed while the workflow is off.
pub(super) fn page(found: &Found, hook: &str, embedded: bool) -> Streamed {
    let chat = &found.chat;
    let t = &chat.texts;
    let base = format!("/chat/{}", escape(hook));
    let (status, main) = if found.record.active {
        (
            StatusCode::OK,
            format!(
                r#"<main id="chat" data-endpoint="{base}/messages" data-hook="{hook}" data-password="{password}" data-max="{max}" data-error="{error}" data-embed="{embed}">
<header><h1>{title}</h1><button id="new" type="button" title="Start a new conversation">New</button>{close}</header>
<ol id="messages" aria-live="polite"><li class="bot welcome">{welcome}</li></ol>
<form id="gate" hidden><label for="password">Password</label><input id="password" type="password" autocomplete="current-password" required><button type="submit">Open</button><p id="gate-note" role="alert"></p></form>
<form id="ask" autocomplete="off"><label class="hidden" for="text">{placeholder}</label><textarea id="text" rows="1" maxlength="{max}" placeholder="{placeholder}" required></textarea><button type="submit">{send}</button></form>
<noscript><p class="note">This chat needs JavaScript.</p></noscript>
</main>"#,
                hook = escape(hook),
                password = u8::from(chat.password.is_some()),
                max = chat.max_message,
                error = escape(&t.error),
                embed = u8::from(embedded),
                title = escape(&t.title),
                close = if embedded {
                    r#"<button id="close" type="button" aria-label="Close" title="Close">×</button>"#
                } else {
                    ""
                },
                welcome = escape(&t.welcome),
                placeholder = escape(&t.placeholder),
                send = escape(&t.send),
            ),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                r#"<main id="closed"><header><h1>{}</h1></header><p class="note">This chat is closed for now.</p></main>"#,
                escape(&t.title)
            ),
        )
    };
    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>{title}</title>
<link rel="stylesheet" href="{base}/chat.css">
<script src="{base}/chat.js" defer></script>
</head>
<body{class}>
{main}
</body>
</html>
"#,
        title = escape(&t.title),
        class = if embedded { r#" class="embedded""# } else { "" },
    );
    let mut response = answer(status, "text/html; charset=utf-8", "no-store", html);
    if let Ok(value) = HeaderValue::from_str(&policy(chat)) {
        response.headers_mut().insert("content-security-policy", value);
    }
    boxed(response)
}

/// The page's script: the same for every chat.
pub(super) fn script() -> Streamed {
    boxed(answer(
        StatusCode::OK,
        "text/javascript; charset=utf-8",
        "public, max-age=300",
        SCRIPT.to_owned(),
    ))
}

/// Whether text on this colour reads better dark than light.
fn light(hex: &str) -> bool {
    let digits: Vec<u32> = hex
        .trim_start_matches('#')
        .chars()
        .filter_map(|c| c.to_digit(16))
        .collect();
    let (r, g, b) = match digits.as_slice() {
        [r, g, b] => (r * 17, g * 17, b * 17),
        [r1, r2, g1, g2, b1, b2] => (r1 * 16 + r2, g1 * 16 + g2, b1 * 16 + b2),
        _ => return true,
    };
    r * 299 + g * 587 + b * 114 > 150_000
}

/// The page's style, in the trigger's colours.
pub(super) fn style(chat: &Chat) -> Streamed {
    let theme = &chat.theme;
    let on_accent = if light(&theme.accent) {
        "#111111"
    } else {
        "#ffffff"
    };
    let css = format!(
        ":root {{ --accent: {}; --on-accent: {on_accent}; --background: {}; --text: {}; }}\n{STYLE}",
        theme.accent, theme.background, theme.text
    );
    boxed(answer(StatusCode::OK, "text/css; charset=utf-8", "no-cache", css))
}

/// The script a site puts on its pages: a button that opens the chat in a frame.
pub(super) fn embed(chat: &Chat) -> Streamed {
    let config = serde_json::json!({
        "title": chat.texts.title, "accent": chat.theme.accent, "background": chat.theme.background,
        "onAccent": if light(&chat.theme.accent) { "#111111" } else { "#ffffff" },
    })
    .to_string()
    // Safe wherever a script's text is put, even inside an HTML page.
    .replace('<', "\\u003c")
    .replace('>', "\\u003e");
    let mut response = answer(
        StatusCode::OK,
        "text/javascript; charset=utf-8",
        "public, max-age=300",
        EMBED.replace("__CONFIG__", &config),
    );
    response.headers_mut().insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("cross-origin"),
    );
    boxed(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_are_escaped_and_the_frame_policy_names_only_the_chat_s_sites() {
        assert_eq!(
            escape(r#"<b a="1">'&'</b>"#),
            "&lt;b a=&quot;1&quot;&gt;&#39;&amp;&#39;&lt;/b&gt;"
        );
        let mut chat = Chat::default();
        assert!(policy(&chat).ends_with("frame-ancestors 'self'"));
        chat.origins = vec!["https://shop.example.com".to_owned()];
        assert!(policy(&chat).ends_with("frame-ancestors 'self' https://shop.example.com"));
        assert!(policy(&chat).contains("script-src 'self';"));
        assert!(light("#ffffff") && light("#fde047") && !light("#0f766e") && !light("#000"));
    }
}
