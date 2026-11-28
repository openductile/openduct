// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Fetching one address for a web step, with its limits: the bytes read (a larger page is refused, compressed or
//! not), the time it may take, and the redirects followed - each followed here, one by one, so every address on the
//! way is checked. Only http and https; never an address on this machine's own network unless the installation
//! allows it (`--allow-private-network`): a name is checked as it is resolved, an address as it is read, and both
//! again at every redirect. Headers a step gives go to the address it names and its own site only; a redirect to
//! another site gets none of them.

use std::io::Read as _;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use reqwest::Url;
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH,
    LAST_MODIFIED, LOCATION, USER_AGENT,
};

/// The longest address a step fetches.
const MAX_URL: usize = 4_096;

/// How a web step reaches the web: its client, and whether this machine's own network may be reached.
#[derive(Debug)]
pub struct Net {
    allow_private: bool,
    pins: Vec<(String, SocketAddr)>,
    client: OnceLock<Result<reqwest::Client, String>>,
}

impl Net {
    /// The web as an installation reaches it: `allow_private` lets steps reach this machine's own network.
    #[must_use]
    pub fn new(allow_private: bool) -> Self {
        Self {
            allow_private,
            pins: Vec::new(),
            client: OnceLock::new(),
        }
    }

    /// The same, with the name `host` answered by `address` rather than looked up: a stand-in site, for tests. It
    /// is not a way past the guard: every other name and address on the way is checked as before.
    #[must_use]
    pub fn pin(mut self, host: &str, address: SocketAddr) -> Self {
        self.pins.push((host.to_ascii_lowercase(), address));
        self.client = OnceLock::new();
        self
    }

    /// The client, made the first time a step reaches the web, as the effects' own HTTP client is made with them.
    fn client(&self) -> Result<&reqwest::Client, String> {
        self.client
            .get_or_init(|| build(self.allow_private, &self.pins))
            .as_ref()
            .map_err(Clone::clone)
    }
}

fn build(allow_private: bool, pins: &[(String, SocketAddr)]) -> Result<reqwest::Client, String> {
    // rustls needs a cryptography provider chosen once per process; a second call finds it chosen.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder = reqwest::Client::builder().connect_timeout(Duration::from_secs(10));
    // Names that resolve to this machine's network refused as they are resolved, as the HTTP step's are; redirects
    // are followed here, one by one, so each is checked and counted.
    let mut builder = openduct_connect::reach::guard(builder, allow_private, "workflows")
        .redirect(reqwest::redirect::Policy::none());
    for (host, address) in pins {
        builder = builder.resolve(host, *address);
    }
    builder
        .build()
        .map_err(|e| format!("cannot set up the web client: {e}"))
}

/// How much one fetch may take.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_bytes: usize,
    pub(crate) timeout: Duration,
    pub(crate) max_redirects: usize,
}

/// One address to fetch.
#[derive(Debug, Clone)]
pub(crate) struct Ask<'a> {
    pub(crate) url: &'a str,
    pub(crate) agent: &'a str,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) accept: &'static str,
    pub(crate) limits: Limits,
    /// What the last answer was (its ETag and Last-Modified): an unchanged page answers 304 with nothing.
    pub(crate) since: Option<(Option<String>, Option<String>)>,
}

/// An answer.
#[derive(Debug, Clone)]
pub(crate) struct Got {
    /// The address that answered, after the redirects.
    pub(crate) url: Url,
    pub(crate) status: u16,
    pub(crate) content_type: String,
    pub(crate) body: Vec<u8>,
    pub(crate) redirects: usize,
    pub(crate) etag: Option<String>,
    pub(crate) modified: Option<String>,
    pub(crate) ms: u64,
}

impl Got {
    /// Whether it is a web page (HTML), by what it says it is or, when it says nothing, by how it starts.
    pub(crate) fn is_html(&self) -> bool {
        let t = self.content_type.to_ascii_lowercase();
        if t.contains("html") {
            return true;
        }
        if !t.is_empty() && !t.starts_with("text/plain") && !t.contains("octet-stream") {
            return false;
        }
        let start = String::from_utf8_lossy(self.body.get(..512.min(self.body.len())).unwrap_or_default())
            .trim_start()
            .to_ascii_lowercase();
        start.starts_with("<!doctype html") || start.starts_with("<html") || start.contains("<body")
    }

    /// The media type, without its parameters.
    pub(crate) fn media_type(&self) -> String {
        self.content_type
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    }
}

/// `text` as an address a step may fetch: http or https, with a host, without a user name or password in it, and -
/// unless allowed - not an address of this machine's own network.
pub(crate) fn checked(text: &str, allow_private: bool) -> Result<Url, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("no address to fetch".to_owned());
    }
    if text.len() > MAX_URL {
        return Err(format!("an address of more than {MAX_URL} characters"));
    }
    let url = Url::parse(text).map_err(|e| {
        format!("{text:?} is not an address ({e}): write it whole, as https://example.com/page")
    })?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("{text:?}: only http and https addresses"));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(format!("{text:?} has no host"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("an address with a user name or password in it: give the page's own address".to_owned());
    }
    if !allow_private && let Some(refused) = openduct_connect::reach::url_refused(&url, "workflows") {
        return Err(refused);
    }
    Ok(url)
}

/// Whether two addresses are of one site: the same scheme, host and port.
pub(crate) fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

/// A failed request in plain words, with what lies under it (a name refused as this machine's own, say).
fn describe(e: &reqwest::Error, url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    let mut text = String::new();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        let said = s.to_string();
        if !text.contains(&said) {
            if !text.is_empty() {
                text.push_str(": ");
            }
            text.push_str(&said);
        }
        source = s.source();
    }
    if let Some(at) = text.find("own network") {
        // From the start of what says so: "localhost is on this machine's own network: start OpenDuct with ...".
        let start = text.get(..at).and_then(|t| t.rfind(": ")).map_or(0, |s| s + 2);
        return text.get(start..).unwrap_or(&text).to_owned();
    }
    if e.is_timeout() {
        return format!("{host} gave no answer in time");
    }
    if e.is_connect() {
        return format!("cannot reach {host}: {text}");
    }
    if text.is_empty() {
        e.to_string()
    } else {
        format!("{host}: {text}")
    }
}

fn header(answer: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    answer
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// The answer's body, at most `most` bytes once unpacked.
async fn body_of(mut answer: reqwest::Response, most: usize) -> Result<Vec<u8>, String> {
    let too_large = || format!("the page is larger than {most} bytes (max_bytes)");
    if answer
        .content_length()
        .is_some_and(|n| n > u64::try_from(most).unwrap_or(u64::MAX))
    {
        return Err(too_large());
    }
    let packed = header(&answer, CONTENT_ENCODING).map(|e| e.trim().to_ascii_lowercase());
    let mut body = Vec::new();
    while let Some(chunk) = answer
        .chunk()
        .await
        .map_err(|e| format!("the page was cut short: {e}"))?
    {
        if body.len() + chunk.len() > most {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    match packed.as_deref() {
        Some("gzip" | "x-gzip") => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(body.as_slice())
                .take(u64::try_from(most).unwrap_or(u64::MAX).saturating_add(1))
                .read_to_end(&mut out)
                .map_err(|e| format!("the page's gzip cannot be read: {e}"))?;
            if out.len() > most {
                return Err(too_large());
            }
            Ok(out)
        }
        _ => Ok(body),
    }
}

async fn fetch(net: &Net, ask: &Ask<'_>) -> Result<Got, String> {
    let client = net.client()?;
    let first = checked(ask.url, net.allow_private)?;
    let mut url = first.clone();
    let mut redirects = 0;
    loop {
        let mut request = client
            .get(url.clone())
            .header(USER_AGENT, ask.agent)
            .header(ACCEPT, ask.accept)
            .header(ACCEPT_ENCODING, "gzip");
        if same_origin(&url, &first) {
            for (name, value) in ask.headers {
                request = request.header(name.as_str(), value.as_str());
            }
            if let Some((etag, modified)) = &ask.since {
                if let Some(e) = etag {
                    request = request.header(IF_NONE_MATCH, e.as_str());
                }
                if let Some(m) = modified {
                    request = request.header(IF_MODIFIED_SINCE, m.as_str());
                }
            }
        }
        let answer = request.send().await.map_err(|e| describe(&e, &url))?;
        let status = answer.status().as_u16();
        if matches!(status, 301 | 302 | 303 | 307 | 308)
            && let Some(location) = header(&answer, LOCATION)
        {
            if redirects >= ask.limits.max_redirects {
                return Err(format!(
                    "more than {} redirects (max_redirects), the last to {location}",
                    ask.limits.max_redirects
                ));
            }
            let next = url
                .join(location.trim())
                .map_err(|_| format!("a redirect to {location:?}, which is not an address"))?;
            url =
                checked(next.as_str(), net.allow_private).map_err(|e| format!("a redirect refused: {e}"))?;
            redirects += 1;
            continue;
        }
        let content_type = header(&answer, CONTENT_TYPE).unwrap_or_default();
        let etag = header(&answer, ETAG);
        let modified = header(&answer, LAST_MODIFIED);
        let body = if status == 304 {
            Vec::new()
        } else {
            body_of(answer, ask.limits.max_bytes).await?
        };
        return Ok(Got {
            url,
            status,
            content_type,
            body,
            redirects,
            etag,
            modified,
            ms: 0,
        });
    }
}

/// Fetches one address, within its limits.
pub(crate) async fn get(net: &Net, ask: &Ask<'_>) -> Result<Got, String> {
    let started = Instant::now();
    match tokio::time::timeout(ask.limits.timeout, fetch(net, ask)).await {
        Ok(got) => got.map(|mut g| {
            g.ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            g
        }),
        Err(_) => Err(format!(
            "{} gave no answer in {}s (timeout)",
            Url::parse(ask.url)
                .ok()
                .and_then(|u| u.host_str().map(str::to_owned))
                .unwrap_or_default(),
            ask.limits.timeout.as_secs()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_whole_web_addresses_off_this_machine_are_fetched() {
        assert!(checked("https://example.com/a", false).is_ok());
        for (text, says) in [
            ("", "no address"),
            ("example.com", "not an address"),
            ("ftp://example.com/x", "only http and https"),
            ("file:///etc/passwd", "only http and https"),
            ("http://127.0.0.1:8080/", "own network"),
            ("http://[::1]/", "own network"),
            ("http://169.254.169.254/latest/meta-data/", "own network"),
            ("https://ada:secret@example.com/", "user name or password"),
        ] {
            let e = checked(text, false).unwrap_err();
            assert!(e.contains(says), "{text}: {e}");
        }
        assert!(checked("http://127.0.0.1:8080/", true).is_ok());
        assert!(checked(&format!("https://example.com/{}", "a".repeat(MAX_URL)), false).is_err());
        let (a, b): (Url, Url) = (
            "https://a.example/x".parse().unwrap(),
            "https://a.example:443/y".parse().unwrap(),
        );
        assert!(same_origin(&a, &b));
        assert!(!same_origin(&a, &"http://a.example/x".parse().unwrap()));
    }
}
