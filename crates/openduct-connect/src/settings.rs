// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A connector's settings, read and checked. A value written `secret:NAME` (or `secret:NAME.field`) is a named secret:
//! in the studio, the workspace's credential; on the command line, a line of the owner's secrets file. So secrets
//! never sit in a pipeline file. `file:/path` reads a file, on the command line only; `env:` is refused, as secrets
//! are not kept in the environment (see `openduct_pipeline::secrets`). No message ever shows a value.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openduct_expr::Compiled;
use openduct_pipeline::{Connector, Setting};

/// Where a pipeline's secrets come from: looked up now, to run, or only checked for their form (`pipeline check`,
/// the studio's check as a pipeline is written). `files` is whether `file:` values may be read (the command line,
/// not the studio).
#[derive(Clone)]
pub enum Secrets {
    Check {
        files: bool,
    },
    Resolve {
        named: Arc<BTreeMap<String, String>>,
        files: bool,
    },
}

impl Secrets {
    /// The secrets of a run: `named` by `secret:` (NAME, or NAME.field), `files` if `file:` may be read.
    #[must_use]
    pub fn resolve(named: BTreeMap<String, String>, files: bool) -> Self {
        Self::Resolve {
            named: Arc::new(named),
            files,
        }
    }

    /// Whether they are looked up (a run), not only checked.
    #[must_use]
    pub fn resolving(&self) -> bool {
        matches!(self, Self::Resolve { .. })
    }

    fn files(&self) -> bool {
        match self {
            Self::Check { files } | Self::Resolve { files, .. } => *files,
        }
    }
}

/// What a pipeline's connectors may use where it runs: its secrets, and whether they may reach this machine's own
/// network (see [`crate::reach`]) - in the studio as `--allow-private-network` says, on the command line always.
#[derive(Clone, Debug)]
pub struct Limits {
    pub secrets: Secrets,
    pub private_network: bool,
}

impl Limits {
    #[must_use]
    pub fn new(secrets: Secrets, private_network: bool) -> Self {
        Self {
            secrets,
            private_network,
        }
    }

    /// Only checking a pipeline's form (`pipeline check`): nothing looked up, nothing reached.
    #[must_use]
    pub fn check(files: bool) -> Self {
        Self::new(Secrets::Check { files }, true)
    }
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Their names at most: never a value.
        match self {
            Self::Check { files } => f.debug_struct("Check").field("files", files).finish(),
            Self::Resolve { named, files } => f
                .debug_struct("Resolve")
                .field("named", &named.keys().collect::<Vec<_>>())
                .field("files", files)
                .finish(),
        }
    }
}

/// A named group of names and values.
pub(crate) type Section = (String, Vec<(String, String)>);

#[derive(Debug)]
pub(crate) struct Settings<'a> {
    path: &'a str,
    items: &'a [(String, Setting)],
    secrets: Secrets,
    private_network: bool,
    /// The language of expressions among them: the pipeline's.
    language: &'a str,
}

impl<'a> Settings<'a> {
    /// The connector's settings, refusing any name it does not know.
    pub(crate) fn new(connector: &'a Connector, allowed: &[&str], limits: Limits) -> Result<Self, String> {
        let Limits {
            secrets,
            private_network,
        } = limits;
        for (name, _) in &connector.settings {
            if !allowed.contains(&name.as_str()) {
                let near = allowed.iter().find(|a| close(a, name));
                return Err(match near {
                    Some(n) => format!(
                        "{}: {name:?} is not a setting (did you mean {n:?}?)",
                        connector.path
                    ),
                    None => format!(
                        "{}: {name:?} is not a setting: the settings are {}",
                        connector.path,
                        allowed.join(", ")
                    ),
                });
            }
        }
        Ok(Self {
            path: &connector.path,
            language: &connector.language,
            items: &connector.settings,
            secrets,
            private_network,
        })
    }

    /// A web client for the connector: reaching only the internet unless this machine's network is allowed.
    pub(crate) fn web_client(&self) -> Result<reqwest::Client, String> {
        crate::web::client(self.private_network)
    }

    /// Whether the connector may reach this machine's own network.
    pub(crate) fn private_network(&self) -> bool {
        self.private_network
    }

    /// Refuses `host` when it is on this machine's own network and that is not allowed (it is looked up now). Not
    /// when only checking a pipeline's form.
    pub(crate) fn reach(&self, at: &str, host: &str, port: u16) -> Result<(), String> {
        if self.private_network || self.checking() {
            return Ok(());
        }
        crate::reach::host_refused(host, port, "pipelines").map_or(Ok(()), |why| Err(format!("{at}: {why}")))
    }

    /// Refuses a web address naming this machine's own network outright (`http://10.0.0.5/`), as names are checked
    /// when they are resolved but an address is not. `{position}` in it is read as a word.
    pub(crate) fn reach_url(&self, at: &str, url: &str) -> Result<(), String> {
        if self.private_network || self.checking() {
            return Ok(());
        }
        let Ok(parsed) = reqwest::Url::parse(&url.replace("{position}", "x")) else {
            return Ok(());
        };
        crate::reach::url_refused(&parsed, "pipelines").map_or(Ok(()), |why| Err(format!("{at}: {why}")))
    }

    /// As [`Settings::reach`], for `host:port` (`[v6]:port`), `port` when it gives none.
    pub(crate) fn reach_address(&self, at: &str, address: &str, port: u16) -> Result<(), String> {
        let (host, port) = match address.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(':') && p.parse::<u16>().is_ok() => (h, p.parse().unwrap_or(port)),
            _ => (address, port),
        };
        self.reach(at, host, port)
    }

    /// Whether secrets are only checked for their form, and so read as empty.
    pub(crate) fn checking(&self) -> bool {
        !self.secrets.resolving()
    }

    pub(crate) fn path(&self, name: &str) -> String {
        format!("{}.{name}", self.path)
    }

    fn get(&self, name: &str) -> Option<&'a Setting> {
        self.items.iter().find(|(n, _)| n == name).map(|(_, s)| s)
    }

    fn resolve(&self, at: &str, raw: &str) -> Result<String, String> {
        if let Some(why) = openduct_pipeline::secrets::refused(at, raw, self.secrets.files()) {
            return Err(why);
        }
        if let Some(name) = raw.strip_prefix(openduct_pipeline::secrets::SECRET) {
            let name = name.trim();
            return match &self.secrets {
                Secrets::Check { .. } => Ok(String::new()),
                Secrets::Resolve { named, files } => named.get(name).cloned().ok_or_else(|| {
                    if *files {
                        format!("{at}: no secret {name} in the secrets file: add a line {name}=...")
                    } else {
                        format!("{at}: no credential {name} in this workspace: add it in Credentials")
                    }
                }),
            };
        }
        if let Some(file) = raw.strip_prefix("file:") {
            return match &self.secrets {
                Secrets::Check { .. } => Ok(String::new()),
                Secrets::Resolve { .. } => std::fs::read_to_string(file.trim())
                    .map(|s| s.trim().to_owned())
                    .map_err(|e| format!("{at}: cannot read {}: {e}", file.trim())),
            };
        }
        Ok(raw.to_owned())
    }

    pub(crate) fn text(&self, name: &str) -> Result<Option<String>, String> {
        match self.get(name) {
            None => Ok(None),
            Some(Setting::Text(raw)) => self.resolve(&self.path(name), raw).map(Some),
            Some(_) => Err(format!("{}: is a single value", self.path(name))),
        }
    }

    pub(crate) fn required(&self, name: &str) -> Result<String, String> {
        self.text(name)?
            .ok_or_else(|| format!("{}: is required", self.path(name)))
    }

    pub(crate) fn duration(&self, name: &str, default: Duration) -> Result<Duration, String> {
        match self.text(name)? {
            None => Ok(default),
            Some(text) => parse_duration(&text).ok_or_else(|| {
                format!(
                    "{}: {text:?} is not a duration: write 500ms, 30s, 5m, 1h or 1d",
                    self.path(name)
                )
            }),
        }
    }

    pub(crate) fn number(
        &self,
        name: &str,
        default: u64,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<u64, String> {
        let Some(text) = self.text(name)? else {
            return Ok(default);
        };
        text.trim()
            .parse::<u64>()
            .ok()
            .filter(|n| range.contains(n))
            .ok_or_else(|| {
                format!(
                    "{}: is a whole number from {} to {}",
                    self.path(name),
                    range.start(),
                    range.end()
                )
            })
    }

    pub(crate) fn boolean(&self, name: &str, default: bool) -> Result<bool, String> {
        match self.text(name)?.as_deref().map(str::trim) {
            None => Ok(default),
            Some("true") => Ok(true),
            Some("false") => Ok(false),
            Some(_) => Err(format!("{}: is true or false", self.path(name))),
        }
    }

    pub(crate) fn choice(&self, name: &str, choices: &[&'static str]) -> Result<&'static str, String> {
        let Some(given) = self.text(name)? else {
            return Ok(choices.first().copied().unwrap_or_default());
        };
        choices
            .iter()
            .copied()
            .find(|c| c.eq_ignore_ascii_case(given.trim()))
            .ok_or_else(|| {
                format!(
                    "{}: is one of {}, not {given:?}",
                    self.path(name),
                    choices.join(", ")
                )
            })
    }

    /// Names and values (`headers: {Authorization: secret:shop-token}`), each value resolved.
    pub(crate) fn pairs(&self, name: &str) -> Result<Vec<(String, String)>, String> {
        match self.get(name) {
            None => Ok(Vec::new()),
            Some(Setting::Map(items)) => items
                .iter()
                .map(|(k, v)| match v {
                    Setting::Text(raw) => self
                        .resolve(&format!("{}.{k}", self.path(name)), raw)
                        .map(|v| (k.clone(), v)),
                    _ => Err(format!("{}.{k}: is a single value", self.path(name))),
                })
                .collect(),
            Some(_) => Err(format!("{}: is a set of names and values", self.path(name))),
        }
    }

    /// Named groups of names and values (`read: {temperature: {table: holding, address: 100}}`), each value
    /// resolved.
    pub(crate) fn sections(&self, name: &str) -> Result<Vec<Section>, String> {
        let at = self.path(name);
        match self.get(name) {
            None => Ok(Vec::new()),
            Some(Setting::Map(items)) => items
                .iter()
                .map(|(k, v)| match v {
                    Setting::Map(inner) => inner
                        .iter()
                        .map(|(ik, iv)| match iv {
                            Setting::Text(raw) => self
                                .resolve(&format!("{at}.{k}.{ik}"), raw)
                                .map(|v| (ik.clone(), v)),
                            _ => Err(format!("{at}.{k}.{ik}: is a single value")),
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map(|inner| (k.clone(), inner)),
                    _ => Err(format!("{at}.{k}: is a set of names and values")),
                })
                .collect(),
            Some(_) => Err(format!("{at}: is a set of names, each with its own settings")),
        }
    }

    pub(crate) fn list(&self, name: &str) -> Result<Vec<String>, String> {
        match self.get(name) {
            None => Ok(Vec::new()),
            Some(Setting::List(items)) => items
                .iter()
                .map(|v| match v {
                    Setting::Text(t) => Ok(t.clone()),
                    _ => Err(format!("{}: is a list of single values", self.path(name))),
                })
                .collect(),
            Some(Setting::Text(t)) => Ok(vec![t.clone()]),
            Some(Setting::Map(_)) => Err(format!("{}: is a list", self.path(name))),
        }
    }

    /// An expression, in the pipeline's language.
    pub(crate) fn expression(&self, name: &str) -> Result<Option<Compiled>, String> {
        match self.get(name) {
            None => Ok(None),
            Some(Setting::Text(source)) => openduct_expr::compile(self.language, source)
                .map(Some)
                .map_err(|e| format!("{}: {e}", self.path(name))),
            Some(_) => Err(format!("{}: is an expression", self.path(name))),
        }
    }
}

fn close(a: &str, b: &str) -> bool {
    a.starts_with(b.get(..3).unwrap_or(b))
        || b.starts_with(a)
        || a.replace('_', "") == b.replace(['_', '-'], "")
}

/// `500ms`, `30s`, `5m`, `1h`, `1d`.
pub(crate) fn parse_duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    let n: u64 = digits.parse().ok()?;
    let unit = text.get(digits.len()..)?.trim();
    let millis = match unit {
        "ms" => n,
        "s" | "" => n.checked_mul(1_000)?,
        "m" => n.checked_mul(60_000)?,
        "h" => n.checked_mul(3_600_000)?,
        "d" => n.checked_mul(86_400_000)?,
        _ => return None,
    };
    Some(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_read_as_people_write_them() {
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86_400)));
        assert_eq!(parse_duration("soon"), None);
    }

    fn connector(token: &str) -> Connector {
        Connector {
            language: "cel".to_owned(),
            kind: "http".into(),
            path: "from.http".into(),
            settings: vec![("token".into(), Setting::Text(token.into()))],
        }
    }

    #[test]
    fn secrets_are_named_never_from_the_environment_and_never_shown() {
        let named = BTreeMap::from([("shop".to_owned(), "s3cr3t".to_owned())]);
        let c = connector("secret:shop");
        let s = Settings::new(
            &c,
            &["token"],
            Limits::new(Secrets::resolve(named.clone(), false), true),
        )
        .unwrap();
        assert_eq!(s.text("token").unwrap().as_deref(), Some("s3cr3t"));
        // A name the workspace (or the file) does not have: said where to add it.
        let missing = connector("secret:other");
        let s = Settings::new(
            &missing,
            &["token"],
            Limits::new(Secrets::resolve(named.clone(), false), true),
        )
        .unwrap();
        assert_eq!(
            s.text("token").unwrap_err(),
            "from.http.token: no credential other in this workspace: add it in Credentials"
        );
        let s = Settings::new(
            &missing,
            &["token"],
            Limits::new(Secrets::resolve(named.clone(), true), true),
        )
        .unwrap();
        assert!(s.text("token").unwrap_err().contains("add a line other=..."));
        // The environment is never read, even when checking.
        let env = connector("env:HOME");
        for secrets in [
            Secrets::Check { files: true },
            Secrets::resolve(named.clone(), true),
        ] {
            let s = Settings::new(&env, &["token"], Limits::new(secrets, true)).unwrap();
            assert!(
                s.text("token")
                    .unwrap_err()
                    .contains("not read from the environment")
            );
        }
        // A file: on the command line only.
        let file = connector("file:/etc/hosts");
        let studio = Settings::new(
            &file,
            &["token"],
            Limits::new(Secrets::resolve(named.clone(), false), true),
        )
        .unwrap();
        assert!(
            studio
                .text("token")
                .unwrap_err()
                .contains("reads no file on the server")
        );
        // A secret never shows in what is printed of them.
        assert!(!format!("{:?}", Secrets::resolve(named, false)).contains("s3cr3t"));
        let checking =
            Settings::new(&c, &["token"], Limits::new(Secrets::Check { files: false }, true)).unwrap();
        assert!(checking.text("token").is_ok());
        let wrong = Connector {
            settings: vec![("tokn".into(), Setting::Text("x".into()))],
            ..c
        };
        assert!(
            Settings::new(
                &wrong,
                &["token"],
                Limits::new(Secrets::Check { files: true }, true)
            )
            .unwrap_err()
            .contains("did you mean \"token\"")
        );
    }
}
