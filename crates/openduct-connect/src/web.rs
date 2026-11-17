// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The HTTP client the connectors share: TLS through rustls, bounded answers, and retries for the failures that
//! pass (a dropped connection, 429, 5xx) with growing waits between them.

use std::time::Duration;

use reqwest::{Client, RequestBuilder, Response, StatusCode};

/// The largest answer read from a remote system.
pub(crate) const MAX_ANSWER: usize = 64 * 1024 * 1024;
pub(crate) const ATTEMPTS: u32 = 6;

/// A client reaching only the internet unless `private_network` (see `crate::reach`).
pub(crate) fn client(private_network: bool) -> Result<Client, String> {
    // rustls needs a cryptography provider chosen once per process; a second call finds it chosen already.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .user_agent(concat!("openduct/", env!("CARGO_PKG_VERSION")));
    crate::reach::guard(builder, private_network, "pipelines")
        .build()
        .map_err(|e| format!("cannot set up HTTP: {e}"))
}

/// Whether trying again later may succeed.
pub(crate) fn passing(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Send what `make` builds, again after a wait while the failure is one that passes. The last answer (whatever
/// its status) or error is returned.
pub(crate) async fn send(make: impl Fn() -> RequestBuilder, what: &str) -> Result<Response, String> {
    let mut wait = Duration::from_millis(500);
    let mut last = String::new();
    for attempt in 1..=ATTEMPTS {
        match make().send().await {
            Ok(answer) if !passing(answer.status()) || attempt == ATTEMPTS => return Ok(answer),
            Ok(answer) => last = format!("{what} answered {}", answer.status()),
            Err(e) if refusal(&e).is_some() => {
                return Err(format!("{what} could not be reached: {}", short(&e)));
            }
            Err(e) => last = format!("{what} could not be reached: {}", short(&e)),
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(10));
    }
    Err(last)
}

/// An answer's body, refused past [`MAX_ANSWER`].
pub(crate) async fn body(mut answer: Response, what: &str) -> Result<Vec<u8>, String> {
    if answer.content_length().is_some_and(|n| n > MAX_ANSWER as u64) {
        return Err(format!("{what} answered with more than 64 MiB"));
    }
    let mut out = Vec::new();
    while let Some(chunk) = answer
        .chunk()
        .await
        .map_err(|e| format!("{what}: {}", short(&e)))?
    {
        out.extend_from_slice(&chunk);
        if out.len() > MAX_ANSWER {
            return Err(format!("{what} answered with more than 64 MiB"));
        }
    }
    Ok(out)
}

/// The refusal of this machine's own network (see `crate::reach`) among an error's causes, as it was said: it says
/// what to do, and trying again will not help. It names the host alone, never the URL.
fn refusal(e: &reqwest::Error) -> Option<String> {
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        let text = s.to_string();
        if text.contains("--allow-private-network") {
            return Some(text);
        }
        source = s.source();
    }
    None
}

/// An error without the chain of causes reqwest adds, which can include the full URL and its query - but for a
/// refusal of this machine's network, said as it is.
pub(crate) fn short(e: &reqwest::Error) -> String {
    if let Some(why) = refusal(e) {
        return why;
    }
    let kind = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "connection refused or unreachable"
    } else if e.is_body() || e.is_decode() {
        "the answer was cut short"
    } else {
        "the request failed"
    };
    kind.to_owned()
}
