// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What reaches outside a run: [`Effects`] and the requests steps make through them - an HTTP call, a message to a
//! topic, SQL, an e-mail, a script - and a test run's effects, which send nothing and say what they would have sent.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use openduct_expr::Value;
use serde_json::{Value as Json, json};

use crate::apps::Credential;

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// How a run reaches outside.
pub trait Effects: Send + Sync {
    /// Sends an HTTP request; answers `{status, headers, body}`.
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a>;
    /// Writes a message to a topic; answers `{topic, partition, offset}`.
    fn write<'a>(&'a self, topic: &'a str, key: Option<String>, value: String) -> Pending<'a>;
    /// The credential called `name`.
    fn credential(&self, name: &str) -> Result<Credential, String>;
    /// The install's own registration with app `app`'s vendor (OAuth client id and secret), if it has one.
    fn registration(&self, app: &str) -> Option<Credential> {
        let _ = app;
        None
    }
    /// Keeps new fields of the credential called `name` (a refreshed token), merged into what it holds.
    fn keep_credential<'a>(
        &'a self,
        name: &'a str,
        fields: std::collections::BTreeMap<String, String>,
    ) -> Pending<'a> {
        let _ = (name, fields);
        Box::pin(async { Err("this run keeps no credential".to_owned()) })
    }
    /// The rows of the lookup file at `path`.
    fn file<'a>(&'a self, path: &'a str) -> Pending<'a>;
    /// Sends an e-mail; answers `{sent, to, server}`.
    fn email<'a>(&'a self, request: EmailRequest) -> Pending<'a> {
        let _ = request;
        Box::pin(async { Err("this run sends no e-mail".to_owned()) })
    }
    /// Runs a script; answers `{result, logs}`.
    fn script<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        let _ = request;
        Box::pin(async { Err("this run runs no scripts".to_owned()) })
    }
    /// Runs a `postgres` step's SQL; answers `{rows, count, columns, command}`.
    fn sql<'a>(&'a self, request: SqlRequest) -> Pending<'a> {
        let _ = request;
        Box::pin(async { Err("this run reaches no database".to_owned()) })
    }
    /// The definition of another workflow of the same workspace, for a run_workflow step.
    fn workflow<'a>(&'a self, id: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("there is no workflow {id} this run can reach")) })
    }
    /// Waits, for a `wait` step: a test run does not.
    fn sleep<'a>(&'a self, wait: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(wait))
    }
}

/// What a `postgres` step runs.
#[derive(Debug, Clone)]
pub struct SqlRequest {
    /// The database's credential, its secrets in it.
    pub credential: Credential,
    pub sql: String,
    /// The values for $1, $2 ..., each as text (the database reads it as its place's type), or null.
    pub params: Vec<Option<String>>,
    /// Run in a read-only transaction, rolled back: a preview's, so only what reads is done.
    pub read_only: bool,
}

/// What an `email` step sends.
#[derive(Debug, Clone)]
pub struct EmailRequest {
    /// The mail server's credential, its secrets in it.
    pub credential: Credential,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub reply_to: Option<String>,
    pub subject: String,
    pub text: Option<String>,
    pub html: Option<String>,
}

/// What a `javascript` or `python` step runs.
#[derive(Debug, Clone)]
pub struct ScriptRequest {
    pub language: &'static str,
    pub code: String,
    /// `value`, `steps`, `trigger`, and a loop's `item` and `index`, as JSON.
    pub names: serde_json::Map<String, Json>,
    pub time: Duration,
}

/// What effects answer when SQL run to read only would write: a preview shows it instead.
pub(crate) const WRITES: &str = "this SQL writes, and this run only reads";

/// An HTTP request as a step makes it.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    /// Whether it only reads (a GET, a model's answer): a preview sends it; a write it only shows.
    pub read: bool,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub query: Vec<(String, String)>,
    pub body: Option<(String, Vec<u8>)>,
    pub timeout: Duration,
}

/// Effects that send nothing and say what they would have sent: a test run's. It reads no secret: it has the
/// credentials with their secret fields as ••• (so a step builds the request it would, to the address it would).
#[derive(Debug, Default, Clone)]
pub struct DryRun {
    /// The credentials, masked ([`Credential::masked`]), by name.
    pub credentials: std::collections::BTreeMap<String, Credential>,
    /// The workspace's lookup files: reading one sends nothing, so a test run reads them.
    pub files: Option<std::sync::Arc<dyn crate::Files>>,
    /// Where scripts run: a script only computes, so a test run runs it.
    pub scripts: Option<std::sync::Arc<dyn crate::Scripts>>,
    /// The other workflows a run_workflow step runs - in a test run, as a test run too.
    pub workflows: Option<std::sync::Arc<dyn crate::Workflows>>,
}

impl Effects for DryRun {
    fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            let body = r.body.map(|(kind, bytes)| {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                let shown = if kind.contains("json") {
                    serde_json::from_str::<Json>(&text).unwrap_or(Json::String(text))
                } else {
                    Json::String(text)
                };
                json!({ "type": kind, "content": shown })
            });
            let headers: serde_json::Map<String, Json> =
                r.headers.into_iter().map(|(k, v)| (k, Json::String(v))).collect();
            let query: serde_json::Map<String, Json> =
                r.query.into_iter().map(|(k, v)| (k, Json::String(v))).collect();
            Ok(Value::from_json(&json!({
                "test": "not sent",
                "request": { "method": r.method, "url": r.url, "headers": headers, "query": query, "body": body },
            })))
        })
    }

    fn write<'a>(&'a self, topic: &'a str, key: Option<String>, value: String) -> Pending<'a> {
        Box::pin(async move {
            let shown = serde_json::from_str::<Json>(&value).unwrap_or(Json::String(value));
            Ok(Value::from_json(
                &json!({ "test": "not written", "topic": topic, "key": key, "value": shown }),
            ))
        })
    }

    fn sleep<'a>(&'a self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }

    fn sql<'a>(&'a self, r: SqlRequest) -> Pending<'a> {
        Box::pin(async move { Ok(sql_shown("not run", &r)) })
    }

    fn script<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        match &self.scripts {
            Some(scripts) => scripts.run(request),
            None => Box::pin(async { Err("this test run runs no scripts".to_owned()) }),
        }
    }

    fn email<'a>(&'a self, request: EmailRequest) -> Pending<'a> {
        Box::pin(async move {
            crate::email::check(&request)?;
            Ok(crate::email::shown("not sent", &request))
        })
    }

    fn workflow<'a>(&'a self, id: &'a str) -> Pending<'a> {
        match &self.workflows {
            Some(w) => w.definition(id),
            None => {
                Box::pin(async move { Err(format!("there is no workflow {id} this test run can reach")) })
            }
        }
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        match &self.files {
            Some(files) => files.rows(path),
            None => {
                Box::pin(async move { Err(format!("there is no file {path} in a test run without files")) })
            }
        }
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        Ok(self.credentials.get(name).map_or_else(
            || Credential {
                kind: "test".to_owned(),
                fields: std::collections::BTreeMap::new(),
            },
            Credential::masked,
        ))
    }
}

/// A `postgres` step's SQL as a test run or a preview shows it, not run.
pub(crate) fn sql_shown(test: &str, r: &SqlRequest) -> Value {
    Value::from_json(&json!({
        "test": test,
        "database": r.credential.fields.get("url"),
        "sql": r.sql,
        "params": r.params,
    }))
}
