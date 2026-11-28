// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The command line as a client of a running studio's API, signed in with an API token: `--studio <url>`, and the
//! token from `--token-file` (a file only its owner may read, its first line the token) or `OPENDUCT_TOKEN`. The
//! token goes over HTTPS only, unless the studio is on this machine or `--allow-http` says otherwise on purpose.

use std::path::PathBuf;
use std::time::Duration;

use openduct_flow::{Effects, HttpRequest};
use serde_json::Value;

/// How long one request to the studio may take: a push of many workflows is saved before it is answered.
const TIME: Duration = Duration::from_secs(120);

/// Where the studio is, and how to sign in to it.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct StudioArgs {
    /// The running studio's address, as https://flows.example.com.
    #[arg(long, env = "OPENDUCT_STUDIO", value_name = "URL")]
    pub studio: String,
    /// A file whose first line is the API token (only its owner may read it); otherwise OPENDUCT_TOKEN.
    #[arg(long, env = "OPENDUCT_TOKEN_FILE", value_name = "FILE")]
    pub token_file: Option<PathBuf>,
    /// Send the token over plain HTTP to a studio beyond this machine (inside a private network).
    #[arg(long)]
    pub allow_http: bool,
    /// Print one JSON document on standard output, for CI, instead of lines for people.
    #[arg(long)]
    pub json: bool,
}

/// What the studio refused, or why it could not be asked.
#[derive(Debug)]
pub(crate) struct Failure {
    /// The HTTP status; 0 when there was no answer.
    pub status: u16,
    pub body: Value,
    pub message: String,
}

/// A client of the studio's API.
pub(crate) struct Client {
    base: String,
    token: String,
    http: openduct_flow::Live,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

/// Whether `host` (`name[:port]`) is this machine: `localhost` or a loopback address.
fn local(host: &str) -> bool {
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or_default(),
        None => host.split(':').next().unwrap_or_default(),
    };
    name == "localhost" || name.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

impl Client {
    pub(crate) fn new(args: &StudioArgs) -> Result<Self, String> {
        let base = args.studio.trim().trim_end_matches('/').to_owned();
        let Some((scheme, rest)) = base.split_once("://") else {
            return Err(format!(
                "--studio: {base:?} is not an address, as https://flows.example.com"
            ));
        };
        let host = rest.split('/').next().unwrap_or_default();
        let plain_local = scheme == "http" && local(host);
        if !(scheme == "https" || plain_local || (scheme == "http" && args.allow_http)) {
            return Err(format!(
                "--studio: {base} is not HTTPS: the token would travel in clear (--allow-http to send it anyway)"
            ));
        }
        let token = match &args.token_file {
            Some(path) => crate::metrics_http::read_token_of("API", path)?.to_string(),
            None => std::env::var("OPENDUCT_TOKEN").map_err(|_| {
                "no API token: set OPENDUCT_TOKEN, or give --token-file (an admin makes one in the studio's Settings, \
                 API tokens)"
                    .to_owned()
            })?,
        };
        let token = token.trim().to_owned();
        if !token.starts_with("odapi_") {
            return Err("the API token is not one the studio made (it begins with odapi_)".to_owned());
        }
        // The client of a workflow's HTTP step, allowed to reach this machine: the studio may be on it.
        let http = openduct_flow::Live::new("127.0.0.1:9", true)?;
        Ok(Self { base, token, http })
    }

    pub(crate) async fn get(&self, path: &str) -> Result<Value, Failure> {
        self.send("GET", path, None).await
    }

    pub(crate) async fn post(&self, path: &str, body: &Value) -> Result<Value, Failure> {
        self.send("POST", path, Some(body)).await
    }

    async fn send(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, Failure> {
        let request = HttpRequest {
            read: method == "GET",
            method: method.to_owned(),
            url: format!("{}{path}", self.base),
            headers: vec![
                ("authorization".to_owned(), format!("Bearer {}", self.token)),
                ("accept".to_owned(), "application/json".to_owned()),
            ],
            query: Vec::new(),
            body: body.map(|b| ("application/json".to_owned(), b.to_string().into_bytes())),
            timeout: TIME,
        };
        let raw = self.http.http_bytes(request).await.map_err(|e| Failure {
            status: 0,
            body: Value::Null,
            message: format!("cannot reach the studio at {}: {e}", self.base),
        })?;
        let meta: Value = serde_json::from_str(&raw.answer.to_json()).unwrap_or(Value::Null);
        let status = meta
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|s| u16::try_from(s).ok())
            .unwrap_or(0);
        let bytes = raw.body.unwrap_or_default();
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            return Ok(body);
        }
        let said = body
            .get("error")
            .and_then(Value::as_str)
            .map_or_else(|| format!("the studio answered {status}"), str::to_owned);
        Err(Failure {
            status,
            body,
            message: said,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(studio: &str, allow_http: bool) -> StudioArgs {
        StudioArgs {
            studio: studio.to_owned(),
            token_file: None,
            allow_http,
            json: false,
        }
    }

    #[test]
    fn the_token_goes_over_https_unless_the_studio_is_here() {
        let refused = Client::new(&args("http://flows.example.com", false)).unwrap_err();
        assert!(refused.contains("not HTTPS"), "{refused}");
        assert!(
            Client::new(&args("flows.example.com", false))
                .unwrap_err()
                .contains("not an address")
        );
        assert!(local("127.0.0.1:5480") && local("localhost") && local("[::1]:5480"));
        assert!(!local("flows.example.com") && !local("127.evil.example:80"));
    }
}
