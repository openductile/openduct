// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Where a connector's secrets come from. A setting written `secret:NAME` (or `secret:NAME.field`) names a secret:
//! in the studio, the workspace's credential of that name; on the command line, a line of the secrets file given with
//! `--secrets-file`, which only its owner may read. Secrets are never kept in the environment, so `env:` is refused;
//! `file:/path` reads a file - on the command line only, as a workspace's pipeline reads nothing from the server's
//! disk.

use crate::{Connector, Pipeline, Setting};

/// What names a secret.
pub const SECRET: &str = "secret:";

/// Every text value of a connector's settings, wherever it sits (in a list, a map), with where it is.
pub fn values(connector: &Connector) -> Vec<(String, &str)> {
    fn walk<'a>(at: String, setting: &'a Setting, out: &mut Vec<(String, &'a str)>) {
        match setting {
            Setting::Text(t) => out.push((at, t)),
            Setting::List(items) => {
                for (i, s) in items.iter().enumerate() {
                    walk(format!("{at}[{i}]"), s, out);
                }
            }
            Setting::Map(items) => {
                for (k, s) in items {
                    walk(format!("{at}.{k}"), s, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    for (name, setting) in &connector.settings {
        walk(format!("{}.{name}", connector.path), setting, &mut out);
    }
    out
}

/// The secrets a pipeline names (`secret:NAME` or `secret:NAME.field`), each once.
pub fn named(pipeline: &Pipeline) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for connector in pipeline.source.iter().chain(pipeline.sink.iter()) {
        for (_, value) in values(connector) {
            if let Some(name) = value.strip_prefix(SECRET) {
                let name = name.trim().to_owned();
                if !out.contains(&name) {
                    out.push(name);
                }
            }
        }
    }
    out
}

/// Why a value may not be used where it is: `env:` anywhere; `file:` in a workspace's pipeline (`files` false).
pub fn refused(at: &str, value: &str, files: bool) -> Option<String> {
    if let Some(var) = value.strip_prefix("env:") {
        return Some(format!(
            "{at}: env:{} - secrets are not read from the environment: {}, and write secret:NAME",
            var.trim(),
            if files {
                "put it in a secrets file (--secrets-file, readable only by you)"
            } else {
                "add it in Credentials"
            }
        ));
    }
    if !files && value.starts_with("file:") {
        return Some(format!(
            "{at}: a workspace's pipeline reads no file on the server: add it in Credentials, and write secret:NAME"
        ));
    }
    None
}

/// The first value of a pipeline that may not be used where it runs (see [`refused`]).
pub fn first_refused(pipeline: &Pipeline, files: bool) -> Option<String> {
    pipeline
        .source
        .iter()
        .chain(pipeline.sink.iter())
        .flat_map(values)
        .find_map(|(at, value)| refused(&at, value, files))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_named_and_the_environment_is_refused() {
        let p = Pipeline::parse(
            "name: x
from:
  http:
    url: https://shop.example/orders
    headers: { Authorization: secret:shop-token, X-Other: secret:shop.key }
to:
  topic: orders
",
        )
        .unwrap();
        assert_eq!(named(&p), ["shop-token", "shop.key"]);
        assert!(first_refused(&p, false).is_none());
        let env = Pipeline::parse(
            "name: x\nfrom:\n  http:\n    url: https://a.example\n    headers: { Authorization: env:TOKEN }\nto:\n  topic: t\n",
        )
        .unwrap();
        let why = first_refused(&env, true).unwrap();
        assert!(
            why.contains("from.http.headers.Authorization") && why.contains("secrets file"),
            "{why}"
        );
        assert!(first_refused(&env, false).unwrap().contains("Credentials"));
        let file = Pipeline::parse(
            "name: x\nfrom:\n  http:\n    url: https://a.example\n    headers: { Authorization: file:/etc/passwd }\nto:\n  topic: t\n",
        )
        .unwrap();
        assert!(first_refused(&file, true).is_none());
        assert!(first_refused(&file, false).unwrap().contains("reads no file"));
    }
}
