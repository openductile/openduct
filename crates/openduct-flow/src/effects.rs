// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The effects of a real run: HTTP calls, and messages written to the broker's topics.
//!
//! An HTTP step cannot reach this machine's own network unless the operator allows it: an address on loopback, a
//! private range, link-local (where clouds answer with their machines' credentials) and the like is refused when a
//! name resolves to it, when a URL names it, and when a redirect leads to it. A workflow written by one client of an
//! agency must not be able to read what sits beside the studio.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openduct_broker::client::{Outgoing, Routed, records};
use openduct_expr::Value;
use serde_json::{Value as Json, json};
use tokio::sync::Mutex;

use openduct_connect::postgres::sql::{Database, Refusal, query};

use crate::apps::Credential;
use crate::run::{Effects, HttpRequest, SqlRequest, WRITES};

/// The largest answer an HTTP step reads.
pub const MAX_ANSWER: usize = 10 * 1024 * 1024;
type Pending<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Where a run's credentials come from: the studio's, kept encrypted.
pub trait Secrets: Send + Sync + std::fmt::Debug {
    /// The credential called `name`, its secrets in it.
    fn get(&self, name: &str) -> Option<Credential>;
    /// The install's own registration with app `app`'s vendor (OAuth client id and secret), if an admin gave one.
    fn registration(&self, app: &str) -> Option<Credential> {
        let _ = app;
        None
    }
    /// Keeps new fields of the credential called `name` (a refreshed token), merged into what it holds.
    fn keep<'a>(&'a self, name: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        let _ = (name, fields);
        Box::pin(async { Err("credentials cannot be changed here".to_owned()) })
    }
}

/// Where a run's lookup files come from: its workspace's.
pub trait Files: Send + Sync + std::fmt::Debug {
    /// The rows of the file at `path` (a list of records).
    fn rows<'a>(&'a self, path: &'a str) -> Pending<'a>;
}

/// Where a run finds the other workflows it may run (run_workflow): its workspace's, and no other's.
pub trait Workflows: Send + Sync + std::fmt::Debug {
    /// The definition of workflow `id`, as JSON.
    fn definition<'a>(&'a self, id: &'a str) -> Pending<'a>;
}

/// Where a run's scripts run: each in a process of its own, with limits (the studio's).
pub trait Scripts: Send + Sync + std::fmt::Debug {
    /// Runs a script; answers `{result, logs}`, or why it failed with what it logged before.
    fn run<'a>(&'a self, request: crate::run::ScriptRequest) -> Pending<'a>;
}

/// Effects that reach outside.
#[derive(Debug)]
pub struct Live {
    http: reqwest::Client,
    allow_private: bool,
    broker: String,
    client: Mutex<Option<Routed>>,
    secrets: Option<Arc<dyn Secrets>>,
    files: Option<Arc<dyn Files>>,
    scripts: Option<Arc<dyn Scripts>>,
    workflows: Option<Arc<dyn Workflows>>,
    /// What the topics a run names are named on the broker: a workspace's own, kept apart from every other's.
    topic_prefix: String,
    /// The node's connections, shared: a topic step borrows one instead of keeping its own.
    shared: Option<Arc<openduct_broker::client::Shared>>,
}

/// The name on the broker of a workspace's topic `name`: its topics kept apart from every other workspace's, so two
/// workspaces may each have an `orders` of their own.
pub fn workspace_topic(workspace: &str, name: &str) -> String {
    format!("{}{name}", workspace_prefix(workspace))
}

/// What every topic of a workspace's is named on the broker beginning with.
pub fn workspace_prefix(workspace: &str) -> String {
    format!("w.{workspace}.")
}

impl Live {
    /// Effects that write to the broker at `broker`; `allow_private` lets HTTP steps reach this machine's network.
    pub fn new(broker: &str, allow_private: bool) -> Result<Self, String> {
        // rustls needs a cryptography provider chosen once per process; a second call finds it chosen already.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .user_agent(concat!("openduct/", env!("CARGO_PKG_VERSION")));
        // The same rules as pipelines' connectors: names, addresses and redirects to this machine's network refused.
        let builder = openduct_connect::reach::guard(builder, allow_private, "workflows");
        Ok(Self {
            http: builder.build().map_err(|e| format!("cannot set up HTTP: {e}"))?,
            allow_private,
            broker: broker.to_owned(),
            client: Mutex::new(None),
            secrets: None,
            files: None,
            scripts: None,
            workflows: None,
            topic_prefix: String::new(),
            shared: None,
        })
    }

    /// These effects, with their runs' lookup files read from `files`.
    #[must_use]
    pub fn with_files(mut self, files: Arc<dyn Files>) -> Self {
        self.files = Some(files);
        self
    }

    /// These effects, writing through the node's shared connections.
    #[must_use]
    pub fn with_shared(mut self, shared: Arc<openduct_broker::client::Shared>) -> Self {
        self.shared = Some(shared);
        self
    }

    /// These effects, writing to a workspace's own topics: `orders` is `w.<workspace>.orders` on the broker.
    #[must_use]
    pub fn in_workspace(mut self, workspace: &str) -> Self {
        self.topic_prefix = workspace_prefix(workspace);
        self
    }

    /// These effects, with the other workflows their runs may run found in `workflows`.
    #[must_use]
    pub fn with_workflows(mut self, workflows: Arc<dyn Workflows>) -> Self {
        self.workflows = Some(workflows);
        self
    }

    /// These effects, with their runs' scripts run by `scripts`.
    #[must_use]
    pub fn with_scripts(mut self, scripts: Arc<dyn Scripts>) -> Self {
        self.scripts = Some(scripts);
        self
    }

    /// These effects, with their runs' credentials read from `secrets`.
    #[must_use]
    pub fn with_secrets(mut self, secrets: Arc<dyn Secrets>) -> Self {
        self.secrets = Some(secrets);
        self
    }

    async fn send(&self, r: HttpRequest) -> Result<Value, String> {
        let mut url = reqwest::Url::parse(&r.url).map_err(|e| format!("{:?} is not a URL: {e}", r.url))?;
        if !r.query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(r.query.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("{:?}: only http and https addresses", r.url));
        }
        if !self.allow_private && url.host_str().and_then(literal_ip).is_some_and(private) {
            return Err(format!(
                "{} is on this machine's own network: start OpenDuct with --allow-private-network to let workflows \
                 reach it",
                url.host_str().unwrap_or_default()
            ));
        }
        let method = reqwest::Method::from_bytes(r.method.as_bytes()).map_err(|e| e.to_string())?;
        let mut request = self.http.request(method, url).timeout(r.timeout);
        for (name, value) in &r.headers {
            request = request.header(name, value);
        }
        if let Some((kind, bytes)) = r.body {
            request = request.header(reqwest::header::CONTENT_TYPE, kind).body(bytes);
        }
        let mut answer = request.send().await.map_err(|e| describe(&e))?;
        let status = answer.status().as_u16();
        let mut headers = serde_json::Map::new();
        for (name, value) in answer.headers() {
            if let Ok(v) = value.to_str() {
                headers.insert(name.as_str().to_owned(), Json::String(v.to_owned()));
            }
        }
        let is_json = headers
            .get("content-type")
            .and_then(Json::as_str)
            .is_some_and(|c| c.contains("json"));
        let mut body = Vec::new();
        while let Some(chunk) = answer.chunk().await.map_err(|e| describe(&e))? {
            if body.len() + chunk.len() > MAX_ANSWER {
                return Err(format!("an answer of more than {MAX_ANSWER} bytes"));
            }
            body.extend_from_slice(&chunk);
        }
        let text = String::from_utf8_lossy(&body).into_owned();
        let body = if is_json {
            serde_json::from_str::<Json>(&text).unwrap_or(Json::String(text))
        } else {
            Json::String(text)
        };
        Ok(Value::from_json(
            &json!({ "status": status, "headers": headers, "body": body }),
        ))
    }

    async fn produce(&self, topic: &str, key: Option<String>, value: String) -> Result<Value, String> {
        if let Some(shared) = &self.shared {
            let mut client = shared
                .lend()
                .await
                .map_err(|e| format!("cannot reach the broker: {e}"))?;
            return write_to(&mut client, topic, key, value).await;
        }
        let mut held = self.client.lock().await;
        if held.is_none() {
            let client = Routed::connect(&self.broker, Duration::from_secs(10))
                .await
                .map_err(|e| format!("cannot reach the broker: {e}"))?;
            *held = Some(client);
        }
        let client = held.as_mut().ok_or("no connection to the broker")?;
        let outcome = write_to(client, topic, key, value).await;
        if outcome.is_err() {
            *held = None;
        }
        outcome
    }
}

async fn write_to(
    client: &mut Routed,
    topic: &str,
    key: Option<String>,
    value: String,
) -> Result<Value, String> {
    let partitions = match client.partition_count(topic).await {
        Ok(n) => n,
        Err(_) => {
            // A topic a workflow writes to is made the first time, with one partition.
            match client.create_topic(topic, 1, None).await {
                Ok(()) | Err(openduct_broker::client::ClientError::Refused { code: 36, .. }) => {}
                Err(e) => return Err(format!("cannot make the topic {topic}: {e}")),
            }
            client
                .partition_count(topic)
                .await
                .map_err(|e| format!("cannot write to {topic}: {e}"))?
        }
    };
    let partition = key
        .as_ref()
        .map_or(0, |k| records::partition_for(k.as_bytes(), partitions));
    let message = Outgoing {
        key: key.map(|k| Bytes::from(k.into_bytes())),
        value: Some(Bytes::from(value.into_bytes())),
        headers: Vec::new(),
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
    let offset = client
        .produce(topic, partition, &[message], now)
        .await
        .map_err(|e| format!("cannot write to {topic}: {e}"))?;
    Ok(Value::from_json(
        &json!({ "topic": topic, "partition": partition, "offset": offset }),
    ))
}

impl Live {
    /// A `postgres` step's SQL, on the database its credential names: refused, unless allowed, when the database's
    /// name leads to this machine's own network.
    async fn run_sql(&self, r: SqlRequest) -> Result<Value, String> {
        let field = |name: &str| {
            r.credential
                .fields
                .get(name)
                .filter(|v| !v.trim().is_empty())
                .cloned()
        };
        let mut db = Database {
            url: field("url").ok_or("the credential has no url: postgres://host:port/database")?,
            username: field("username").ok_or("the credential has no username")?,
            password: field("password"),
            tls: field("tls").unwrap_or_else(|| "require".to_owned()),
            ca: field("ca"),
            address: None,
        };
        let (host, port) = db.host()?;
        let found: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|e| format!("cannot find the database {host}: {e}"))?
            .collect();
        if !self.allow_private && found.iter().any(|a| private(a.ip())) {
            return Err(format!(
                "{host} is on this machine's own network: start OpenDuct with --allow-private-network to let \
                 workflows reach it"
            ));
        }
        db.address = Some(
            *found
                .first()
                .ok_or_else(|| format!("cannot find the database {host}"))?,
        );
        match query(&db, &r.sql, &r.params, r.read_only).await {
            Ok(answer) => Ok(Value::from_json(&json!({
                "rows": answer.rows,
                "count": answer.rows.len(),
                "columns": answer.columns,
                "command": answer.command,
            }))),
            Err(Refusal::Writes) => Err(WRITES.to_owned()),
            Err(Refusal::Failed(e)) => Err(e),
        }
    }
}

impl Effects for Live {
    fn sql<'a>(&'a self, request: SqlRequest) -> Pending<'a> {
        Box::pin(self.run_sql(request))
    }

    fn email<'a>(&'a self, request: crate::run::EmailRequest) -> Pending<'a> {
        Box::pin(crate::email::send(request, self.allow_private))
    }

    fn workflow<'a>(&'a self, id: &'a str) -> Pending<'a> {
        match &self.workflows {
            Some(w) => w.definition(id),
            None => Box::pin(async move { Err(format!("there is no workflow {id} this run can reach")) }),
        }
    }

    fn script<'a>(&'a self, request: crate::run::ScriptRequest) -> Pending<'a> {
        match &self.scripts {
            Some(scripts) => scripts.run(request),
            None => Box::pin(async { Err("scripts are not set up here".to_owned()) }),
        }
    }

    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        Box::pin(self.send(request))
    }

    fn write<'a>(&'a self, topic: &'a str, key: Option<String>, value: String) -> Pending<'a> {
        Box::pin(async move {
            self.produce(&format!("{}{topic}", self.topic_prefix), key, value)
                .await
        })
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        match &self.files {
            Some(files) => files.rows(path),
            None => Box::pin(async move { Err(format!("there is no file {path} here")) }),
        }
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        self.secrets
            .as_ref()
            .and_then(|s| s.get(name))
            .ok_or_else(|| format!("there is no credential called {name:?}: add it under Credentials"))
    }

    fn registration(&self, app: &str) -> Option<Credential> {
        self.secrets.as_ref()?.registration(app)
    }

    fn keep_credential<'a>(&'a self, name: &'a str, fields: BTreeMap<String, String>) -> Pending<'a> {
        match &self.secrets {
            Some(s) => s.keep(name, fields),
            None => Box::pin(async { Err("credentials cannot be changed here".to_owned()) }),
        }
    }
}

fn describe(e: &reqwest::Error) -> String {
    let mut text = e.to_string();
    let mut source = std::error::Error::source(e);
    while let Some(s) = source {
        text = format!("{text}: {s}");
        source = s.source();
    }
    if e.is_timeout() {
        format!("no answer in time ({text})")
    } else {
        text
    }
}

pub use openduct_connect::reach::{literal_ip, private};
