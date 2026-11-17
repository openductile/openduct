// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct app try --send`: one action run against the real service exactly as a workflow step runs it - a token
//! fetched or renewed as its sign-in needs, every page of a list - with each request shown, its secrets as •••. A
//! token the vendor renews is kept in the credential file, as a workflow keeps it in its credential: vendors that
//! give a new refresh token each time take the old one no more.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;

use openduct_expr::Value;
use openduct_flow::app_files::AppDef;
use openduct_flow::{Credential, Effects, Event, HttpRequest, Live, Outcome, Workflow};
use serde_json::json;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// The fields that hold a token or a secret, whatever the sign-in calls secret.
const ALWAYS_SECRET: [&str; 3] = ["access_token", "refresh_token", "client_secret"];

struct Tried {
    live: Live,
    kind: String,
    fields: Mutex<BTreeMap<String, String>>,
    file: Option<PathBuf>,
    registration: Option<Credential>,
    secrets: Mutex<Vec<String>>,
}

/// `text` with each secret as •••, as it is and as a form or query encodes it.
fn masked(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|s| s.len() >= 4)
        .fold(text.to_owned(), |t, s| {
            t.replace(s.as_str(), "•••").replace(&percent(s), "•••")
        })
}

fn percent(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// A value as a secrets file holds it: in quotes when it would not read back as it is.
fn quoted(value: &str) -> String {
    let plain = !value.is_empty()
        && value.trim() == value
        && !value.starts_with('"')
        && !value.chars().any(|c| c == '\n' || c == '\r');
    if plain {
        return value.to_owned();
    }
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

impl Tried {
    fn show(&self, r: &HttpRequest) {
        let secrets = self.secrets.lock().map(|s| s.clone()).unwrap_or_default();
        println!("{} {}", r.method, masked(&r.url, &secrets));
        for (k, v) in &r.query {
            println!("  ?{k}={}", masked(v, &secrets));
        }
        for (k, v) in &r.headers {
            // A basic sign-in's header is its secret, encoded.
            let v = if k.eq_ignore_ascii_case("authorization") {
                "•••".to_owned()
            } else {
                masked(v, &secrets)
            };
            println!("  {k}: {v}");
        }
        if let Some((kind, body)) = &r.body {
            println!("  ({kind}) {}", masked(&String::from_utf8_lossy(body), &secrets));
        }
    }

    /// Writes `fields` into the credential file: each line of a name given changed, the others as they were, the
    /// new names added; the file kept its owner's alone.
    fn write_back(&self, path: &Path, fields: &BTreeMap<String, String>) -> Result<(), String> {
        let text =
            std::fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let mut left: BTreeMap<&str, &str> = fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let mut out = String::new();
        for line in text.lines() {
            let name = line.split_once('=').map(|(n, _)| n.trim()).unwrap_or("");
            match left.remove(name) {
                Some(value) if !line.trim_start().starts_with('#') => {
                    out.push_str(&format!("{name}={}\n", quoted(value)));
                }
                _ => {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
        for (name, value) in left {
            out.push_str(&format!("{name}={}\n", quoted(value)));
        }
        let new = path.with_extension("new");
        {
            use std::io::Write as _;
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut f = options
                .open(&new)
                .map_err(|e| format!("cannot write {}: {e}", new.display()))?;
            f.write_all(out.as_bytes())
                .map_err(|e| format!("cannot write {}: {e}", new.display()))?;
            f.sync_all()
                .map_err(|e| format!("cannot write {}: {e}", new.display()))?;
        }
        std::fs::rename(&new, path).map_err(|e| format!("cannot replace {}: {e}", path.display()))
    }
}

impl Effects for Tried {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            self.show(&request);
            let answer = self.live.http(request).await?;
            if let Value::Record(r) = &answer
                && let Some(status) = r.get("status")
            {
                println!("  → {}", openduct_flow::as_text(status));
            }
            Ok(answer)
        })
    }

    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("trying an action writes to no topic".to_owned()) })
    }

    fn credential(&self, _: &str) -> Result<Credential, String> {
        let fields = self
            .fields
            .lock()
            .map_err(|_| "the credential cannot be read now")?
            .clone();
        Ok(Credential {
            kind: self.kind.clone(),
            fields,
        })
    }

    fn registration(&self, _: &str) -> Option<Credential> {
        self.registration.clone()
    }

    fn keep_credential<'a>(&'a self, _: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        Box::pin(async move {
            if let Ok(mut secrets) = self.secrets.lock() {
                secrets.extend(
                    fields
                        .iter()
                        .filter(|(k, _)| ALWAYS_SECRET.contains(&k.as_str()))
                        .map(|(_, v)| v.clone()),
                );
            }
            if let Ok(mut held) = self.fields.lock() {
                held.extend(fields.clone());
            }
            match &self.file {
                Some(path) => {
                    self.write_back(path, &fields)?;
                    println!("(the new token is kept in {})", path.display());
                }
                None => println!("(a new token: there is no credential file to keep it in)"),
            }
            Ok(Value::Null)
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("trying an action reads no lookup file ({path})")) })
    }
}

/// What a `try --send` is given.
pub(crate) struct Given<'a> {
    pub app: &'static AppDef,
    pub action: &'a str,
    pub kind: String,
    pub fields: BTreeMap<String, String>,
    pub credential_file: Option<&'a Path>,
    pub registration_file: Option<&'a Path>,
    pub inputs: serde_json::Map<String, serde_json::Value>,
}

/// Runs the action as a one-step workflow, through the run's own code; prints its result.
pub(crate) async fn send(given: Given<'_>) -> Result<(), String> {
    let registration = match given.registration_file {
        Some(path) => Some(Credential {
            kind: "oauth_registration".to_owned(),
            fields: crate::secrets_file::read(path)?,
        }),
        None => None,
    };
    let sign_in = given
        .app
        .sign_in(&given.kind)
        .ok_or_else(|| format!("{} has no sign-in {}", given.app.name, given.kind))?;
    let mut secrets: Vec<String> = given
        .fields
        .iter()
        .filter(|(k, _)| {
            ALWAYS_SECRET.contains(&k.as_str()) || sign_in.fields.iter().any(|f| f.secret && f.name == **k)
        })
        .map(|(_, v)| v.clone())
        .collect();
    if let Some(r) = &registration {
        secrets.extend(r.fields.get("client_secret").cloned());
    }
    let mut body = given.inputs;
    body.insert("action".to_owned(), json!(given.action));
    // Named in what a refusal says as its file is.
    let named = given.credential_file.and_then(|p| p.file_name()).map_or_else(
        || "the credential".to_owned(),
        |n| n.to_string_lossy().into_owned(),
    );
    body.insert("credential".to_owned(), json!(named));
    let workflow = Workflow::from_json(
        &json!({ "name": "try", "trigger": "webhook", "steps": [ { "name": "try", given.app.id.as_str(): body } ] }),
        None,
    )?;
    let tried = Tried {
        // Run by whoever runs it, from this machine: it may reach this machine's own network.
        live: Live::new("127.0.0.1:9092", true)?,
        kind: given.kind,
        fields: Mutex::new(given.fields),
        file: given.credential_file.map(Path::to_path_buf),
        registration,
        secrets: Mutex::new(secrets),
    };
    let event = Event {
        value: Value::Null,
        details: Value::Null,
    };
    let record = openduct_flow::run(&workflow, &event, &tried).await;
    match record.outcome {
        Outcome::Failed { error, .. } => Err(error),
        _ => {
            let result = record
                .steps
                .last()
                .map(|s| s.result.to_json())
                .unwrap_or_default();
            println!("{result}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn a_secret_is_hidden_as_it_is_and_as_a_form_encodes_it() {
        let secrets = vec!["1//0gAb+c=".to_owned()];
        assert_eq!(
            masked("refresh_token=1%2F%2F0gAb%2Bc%3D&x=1", &secrets),
            "refresh_token=•••&x=1"
        );
        assert_eq!(masked("token 1//0gAb+c=", &secrets), "token •••");
    }

    #[test]
    fn a_kept_token_reads_back_as_it_was_written() {
        let dir = std::env::temp_dir().join(format!("openduct-try-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cred.env");
        std::fs::write(&path, "# a comment\naccess_token=old\nrealm_id=42\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let tried = Tried {
            live: Live::new("127.0.0.1:9092", true).unwrap(),
            kind: "x".to_owned(),
            fields: Mutex::new(BTreeMap::new()),
            file: Some(path.clone()),
            registration: None,
            secrets: Mutex::new(Vec::new()),
        };
        let fields: BTreeMap<String, String> = [
            ("access_token".to_owned(), "new \"one\"".to_owned()),
            ("expires_at".to_owned(), "1800000000".to_owned()),
        ]
        .into();
        tried.write_back(&path, &fields).unwrap();
        let back = crate::secrets_file::read(&path).unwrap();
        assert_eq!(back.get("access_token").map(String::as_str), Some("new \"one\""));
        assert_eq!(back.get("realm_id").map(String::as_str), Some("42"));
        assert_eq!(back.get("expires_at").map(String::as_str), Some("1800000000"));
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .starts_with("# a comment\n")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
