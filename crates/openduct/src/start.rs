// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct start`: OpenDuct in one command - the broker, and beside it the studio, the page in the browser where
//! workflows are built - in one program, stopped together. Every `openduct serve` setting works here too.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use clap::Args;
use tokio::sync::watch;

use crate::serve::{self, ServeArgs};

#[derive(Debug, Args)]
pub(crate) struct StartArgs {
    #[command(flatten)]
    serve: ServeArgs,
    /// Address the studio listens on. The default accepts only this machine; 0.0.0.0:5480 accepts others (put it
    /// behind HTTPS, and see --studio-host).
    #[arg(long, env = "OPENDUCT_STUDIO_LISTEN", default_value = "127.0.0.1:5480")]
    studio_listen: SocketAddr,
    /// A name the studio is reached by, beside localhost and its address (flows.example.com); comma-separated or
    /// repeated. A request naming any other is refused, so no other site can reach the studio through its own name.
    #[arg(long = "studio-host", env = "OPENDUCT_STUDIO_HOSTS", value_delimiter = ',')]
    studio_hosts: Vec<String>,
    /// Mark session cookies Secure: the studio is reached over HTTPS, through a proxy that ends TLS.
    #[arg(long, env = "OPENDUCT_STUDIO_SECURE_COOKIES")]
    studio_secure_cookies: bool,
    /// Let workflows and the studio's pipelines reach this machine's own network: localhost, private ranges,
    /// link-local (a model server or a database beside OpenDuct, say). Off by default, so no workspace's workflow or
    /// pipeline can read what sits beside OpenDuct - a cloud's metadata service among it.
    #[arg(long, env = "OPENDUCT_ALLOW_PRIVATE_NETWORK")]
    allow_private_network: bool,
    /// A folder of apps of your own (`<dir>/<id>/app.yaml`, see docs/apps.md), beside those OpenDuct comes with.
    #[arg(long, env = "OPENDUCT_APPS_DIR")]
    apps_dir: Option<std::path::PathBuf>,
}

pub(crate) async fn run(args: StartArgs) -> Result<(), String> {
    serve::init_logging(args.serve.log_format, args.serve.log_files());
    if let Some(dir) = &args.apps_dir {
        let added = openduct_flow::app_files::add_dir(dir)?;
        tracing::info!(apps = added.len(), dir = %dir.display(), "apps of your own added");
    }
    let first_node = args.serve.first_node();
    let (stop, stopping) = watch::channel(false);
    let (ready, broker_ready) = tokio::sync::oneshot::channel();
    let key_dir = (!args.serve.memory).then(|| args.serve.data_dir.join("studio"));
    let cluster_secret = args.serve.cluster.shared_secret()?;
    let data = if args.serve.memory {
        "in memory (lost when it stops)".to_owned()
    } else {
        args.serve.data_dir.display().to_string()
    };
    let mut broker = tokio::spawn(serve::run_until(
        args.serve,
        stopped(stopping.clone()),
        move |addr| {
            let _ = ready.send(addr);
        },
        Some(openduct_flow::meters::prometheus),
    ));
    let listening = tokio::select! {
        addr = broker_ready => addr.ok(),
        ended = &mut broker => {
            return match ended {
                Ok(Ok(())) => Err("the broker stopped before it was ready".to_owned()),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(format!("the broker failed: {e}")),
            };
        }
    };
    let Some(listening) = listening else {
        return Err("the broker stopped before it was ready".to_owned());
    };
    let studio = openduct_studio::Studio::start(openduct_studio::Config {
        listen: args.studio_listen,
        broker: reachable(listening).to_string(),
        hosts: args.studio_hosts,
        key_dir,
        secure_cookies: args.studio_secure_cookies,
        allow_private_network: args.allow_private_network,
        cluster_secret,
        log_dir: crate::otel_log::dir(),
        pipelines: pipeline_runner(&reachable(listening).to_string()),
        runs_connector_sources: first_node,
    })
    .await;
    let studio = match studio {
        Ok(s) => s,
        Err(e) => {
            let _ = stop.send(true);
            let _ = broker.await;
            return Err(e);
        }
    };
    let page = studio.addr();
    let built = studio.has_page();
    say("OpenDuct is running.");
    say(&format!(
        "  Studio:  http://{}{}",
        reachable(page),
        if built { "" } else { "   (built without its page)" }
    ));
    say(&format!("  Broker:  {listening}   (Kafka clients connect here)"));
    say(&format!("  Data:    {data}"));
    say("Open the studio in a browser. Stop with Ctrl-C.");
    // The studio stops first, while the broker still answers: its workflows commit how far they got and leave their
    // groups, then the broker stops.
    let (stop_studio, studio_stopping) = watch::channel(false);
    let mut serving = tokio::spawn(studio.serve(stopped(studio_stopping)));
    let outcome = tokio::select! {
        () = serve::stopped() => Ok(()),
        ended = &mut broker => match ended {
            Ok(r) => r.and(Err("the broker stopped".to_owned())),
            Err(e) => Err(format!("the broker failed: {e}")),
        },
        _ = &mut serving => Err("the studio stopped".to_owned()),
    };
    let _ = stop_studio.send(true);
    if !serving.is_finished()
        && tokio::time::timeout(std::time::Duration::from_secs(20), &mut serving)
            .await
            .is_err()
    {
        serving.abort();
    }
    let _ = stop.send(true);
    if !broker.is_finished() {
        let _ = broker.await;
    }
    outcome
}

/// A line for whoever started OpenDuct. Nobody may be reading (output closed, `| head -1`): that is no reason to
/// stop, so a failed write is let go.
fn say(line: &str) {
    use std::io::Write as _;
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}").and_then(|()| out.flush());
}

/// Resolves once `stop` says so.
async fn stopped(mut stop: watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}

/// An address to connect to: a listen address of every interface (0.0.0.0) is reached on this machine's own.
/// What runs the studio's pipelines: the program's own run loop, against this node's broker - following each
/// partition's leader round a cluster.
#[cfg(feature = "pipelines")]
#[derive(Debug)]
struct PipelineRunner {
    broker: String,
}

#[cfg(feature = "pipelines")]
impl openduct_studio::PipelineRunner for PipelineRunner {
    fn run(
        &self,
        run: openduct_studio::PipelineRun,
        stop: watch::Receiver<bool>,
        progress: std::sync::Arc<dyn Fn(u64, u64, u64) + Send + Sync>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        // On a thread of its own: some connectors' clients cannot move between threads mid-run.
        let broker = self.broker.clone();
        let (done, outcome) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("openduct-pipeline".to_owned())
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("cannot start the pipeline's runtime: {e}"))
                    .and_then(|rt| {
                        rt.block_on(crate::pipeline::run_inside(
                            crate::pipeline::Inside {
                                source: run.source,
                                topic_prefix: run.topic_prefix,
                                group: run.group,
                                named: run.secrets,
                                private_network: run.private_network,
                            },
                            &broker,
                            stop,
                            progress,
                        ))
                    });
                let _ = done.send(result);
            });
        Box::pin(async move {
            spawned.map_err(|e| format!("cannot start the pipeline: {e}"))?;
            outcome
                .await
                .unwrap_or_else(|_| Err("the pipeline's thread ended".to_owned()))
        })
    }
}

#[cfg(feature = "pipelines")]
fn pipeline_runner(broker: &str) -> Option<std::sync::Arc<dyn openduct_studio::PipelineRunner>> {
    Some(std::sync::Arc::new(PipelineRunner {
        broker: broker.to_owned(),
    }))
}

#[cfg(not(feature = "pipelines"))]
fn pipeline_runner(_: &str) -> Option<std::sync::Arc<dyn openduct_studio::PipelineRunner>> {
    None
}

fn reachable(addr: SocketAddr) -> SocketAddr {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        IpAddr::V6(ip) if ip.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        _ => addr,
    }
}
