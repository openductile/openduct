// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct's studio: the page in the browser where workflows are built and watched, and the API behind it. The
//! `openduct start` command runs it beside the broker, in the same program; what it keeps lives in the broker's own
//! topics, so it needs no database, and every node of a cluster serves the same studio.
//!
//! Safe on a network from the start: an owner's account is made on the first visit and every request after it is
//! signed in; sessions are signed cookies (HttpOnly, SameSite=Strict); a change carries the session's token and
//! comes as JSON from the studio's own page; a request must name one of the studio's host names, so a site that
//! points its own name at this machine reaches nothing; failed sign-ins slow an address down.

#![forbid(unsafe_code)]
// A request that cannot go on stops at its answer: the HTTP answer is the natural error of a request's checks.
#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;

pub use pipelines::{PipelineRun, PipelineRunner};
mod alerts;
mod api;
mod app_catalog;
mod assets;
mod auth;
mod credentials;
mod dashboard;
mod files;
mod history;
mod hooks;
mod http;
mod keys;
mod logs;
mod manager;
mod model;
mod oauth;
mod pipeline_secrets;
mod pipelines;
mod records;
mod scripts;
mod templates;
mod workflows;
mod workspaces;

/// How the studio runs.
#[derive(Debug, Clone)]
pub struct Config {
    /// The address to listen on.
    pub listen: SocketAddr,
    /// The broker to keep records in (`host:port`).
    pub broker: String,
    /// Host names the studio is reached by, beside `localhost`, `127.0.0.1`, `::1` and its listen address.
    pub hosts: Vec<String>,
    /// Where the key sessions are signed with is kept; `None` for a key of this run alone.
    pub key_dir: Option<PathBuf>,
    /// Mark cookies `Secure` (the studio is reached over HTTPS, through a proxy that ends TLS).
    pub secure_cookies: bool,
    /// Let workflows' HTTP steps reach this machine's own network (loopback, private ranges).
    pub allow_private_network: bool,
    /// The cluster secret, when OpenDuct runs as a cluster: every node then uses the same studio keys, kept in the
    /// cluster sealed with a key derived from it, instead of key files of its own.
    pub cluster_secret: Option<String>,
    /// Where OpenDuct keeps its own log (OpenTelemetry records, a file a day), for the Logs page; `None`: no file.
    pub log_dir: Option<PathBuf>,
    /// What runs the pipelines turned on in the studio; `None`: a program built without pipelines.
    pub pipelines: Option<Arc<dyn PipelineRunner>>,
    /// Whether this node runs pipelines that read a connector: one node of a cluster does, so none is read twice.
    pub runs_connector_sources: bool,
}

#[derive(Debug)]
pub(crate) struct State {
    log_dir: Option<PathBuf>,
    pipeline_runner: Option<Arc<dyn PipelineRunner>>,
    runs_connector_sources: bool,
    pipes: pipelines::Pipes,
    hosts: http::Hosts,
    keys: auth::Keys,
    failures: auth::Failures,
    setup: tokio::sync::Mutex<()>,
    secure_cookies: bool,
    allow_private_network: bool,
    model: Arc<model::Model>,
    vault: Arc<credentials::Vault>,
    broker: String,
    /// The node's connections to the cluster, shared by every workflow on it: a few, however many workflows run.
    shared: Arc<openduct_broker::client::Shared>,
    manager: manager::Manager,
    client: tokio::sync::Mutex<Option<openduct_broker::client::Routed>>,
    scripts: Arc<scripts::Scripts>,
}

/// Lookup files read from a folder of this machine, as the studio reads its own: for `openduct workflow test`.
#[must_use]
pub fn local_files(dir: &std::path::Path) -> Arc<dyn openduct_flow::Files> {
    Arc::new(files::LocalFiles {
        dir: dir.to_path_buf(),
    })
}

/// Where scripts run, each in a process of its own: for `openduct workflow test` as for the studio.
#[must_use]
pub fn script_runner() -> Arc<dyn openduct_flow::Scripts> {
    Arc::new(scripts::Scripts::of_this_program())
}

impl State {
    /// The effects of a workspace's runs: its own credentials and files, and nothing of any other workspace.
    pub(crate) fn effects_for(&self, workspace: &str) -> Result<Arc<dyn openduct_flow::Effects>, String> {
        let secrets = credentials::Secrets {
            vault: Arc::clone(&self.vault),
            model: Arc::clone(&self.model),
            workspace: workspace.to_owned(),
            environment: model::PRODUCTION.to_owned(),
        };
        let files = files::Files::new(Arc::clone(&self.model), &self.broker, workspace);
        let live = openduct_flow::Live::new(&self.broker, self.allow_private_network)?
            .with_shared(Arc::clone(&self.shared))
            .in_workspace(workspace)
            .with_secrets(Arc::new(secrets))
            .with_files(Arc::new(files))
            .with_scripts(Arc::clone(&self.scripts) as Arc<dyn openduct_flow::Scripts>)
            .with_workflows(self.workflows_for(workspace));
        Ok(Arc::new(live))
    }

    /// The workflows a workspace's runs may run (run_workflow): its own.
    pub(crate) fn workflows_for(&self, workspace: &str) -> Arc<dyn openduct_flow::Workflows> {
        Arc::new(workflows::Definitions {
            model: Arc::clone(&self.model),
            workspace: workspace.to_owned(),
        })
    }

    /// Where a test run's scripts run: as a live run's.
    pub(crate) fn scripts_for(&self) -> Arc<dyn openduct_flow::Scripts> {
        Arc::clone(&self.scripts) as Arc<dyn openduct_flow::Scripts>
    }

    /// A workspace's files, as a test run reads them.
    pub(crate) fn files_for(&self, workspace: &str) -> Arc<dyn openduct_flow::Files> {
        Arc::new(files::Files::new(
            Arc::clone(&self.model),
            &self.broker,
            workspace,
        ))
    }
}

type Using<'c, T> = std::pin::Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'c>>;

impl State {
    /// Runs `f` with the studio's own connection to the broker, made again after a failure.
    pub(crate) async fn with_client<T>(
        &self,
        f: impl for<'c> FnOnce(&'c mut openduct_broker::client::Routed) -> Using<'c, T>,
    ) -> Result<T, String> {
        let mut held = self.client.lock().await;
        if held.is_none() {
            let client =
                openduct_broker::client::Routed::connect(&self.broker, std::time::Duration::from_secs(10))
                    .await
                    .map_err(|e| format!("cannot reach the broker: {e}"))?;
            *held = Some(client);
        }
        let client = held.as_mut().ok_or("no connection to the broker")?;
        let outcome = f(client).await;
        if outcome.is_err() {
            *held = None;
        }
        outcome
    }
}

/// The studio, listening and ready to serve.
#[derive(Debug)]
pub struct Studio {
    listener: TcpListener,
    addr: SocketAddr,
    state: Arc<State>,
}

impl Studio {
    /// Listens, and reads what it keeps from the broker.
    pub async fn start(config: Config) -> Result<Self, String> {
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|e| format!("the studio cannot listen on {}: {e}", config.listen))?;
        let addr = listener
            .local_addr()
            .map_err(|e| format!("the studio's address: {e}"))?;
        let model = Arc::new(model::Model::open(&config.broker).await?);
        let (session_keys, vault) = match &config.cluster_secret {
            Some(secret) => {
                let shared =
                    keys::shared(&config.broker, secret.as_bytes(), config.key_dir.as_deref()).await?;
                (
                    auth::Keys::from_key(&shared.session),
                    credentials::Vault::from_key(&shared.credentials)?,
                )
            }
            None => (
                auth::Keys::load(config.key_dir.as_deref())?,
                credentials::Vault::load(config.key_dir.as_deref())?,
            ),
        };
        let vault = Arc::new(vault);
        vault.refresh(&model).await;
        let state = State {
            log_dir: config.log_dir.clone(),
            pipeline_runner: config.pipelines.clone(),
            runs_connector_sources: config.runs_connector_sources,
            pipes: pipelines::Pipes::default(),
            hosts: http::Hosts::new(addr, &config.hosts),
            keys: session_keys,
            failures: auth::Failures::default(),
            setup: tokio::sync::Mutex::new(()),
            secure_cookies: config.secure_cookies,
            allow_private_network: config.allow_private_network,
            model,
            vault,
            broker: config.broker.clone(),
            shared: Arc::new(openduct_broker::client::Shared::new(
                &config.broker,
                std::time::Duration::from_secs(10),
                &openduct_broker::client::ClientSecurity::default(),
            )),
            manager: manager::Manager::default(),
            client: tokio::sync::Mutex::new(None),
            scripts: Arc::new(scripts::Scripts::of_this_program()),
        };
        Ok(Self {
            listener,
            addr,
            state: Arc::new(state),
        })
    }

    /// The address it listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether its page is built into this program.
    pub fn has_page(&self) -> bool {
        assets::built()
    }

    /// Serves until `stop` completes, and keeps every workflow that is on running.
    pub async fn serve(self, stop: impl Future<Output = ()>) {
        let state = Arc::clone(&self.state);
        // Workflows changed by another node of the cluster are picked up within a few seconds.
        let syncing = tokio::spawn(async move {
            loop {
                state.manager.sync(&state).await;
                state.pipes.sync(&state).await;
                state.vault.refresh(&state.model).await;
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
        // Each workspace's new alerts, to the messengers its admins chose.
        let alerting = tokio::spawn(alerts::keep_alerting(Arc::clone(&self.state)));
        http::serve(self.listener, Arc::clone(&self.state), stop).await;
        syncing.abort();
        alerting.abort();
        self.state.manager.stop_all().await;
        self.state.pipes.stop_all().await;
    }
}
