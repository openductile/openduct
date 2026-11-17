// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct`: run the broker, and look after it.
//!
//! ```text
//! openduct serve --data-dir /var/lib/openduct          a durable broker on 127.0.0.1:9092
//! openduct serve --memory                             a broker that keeps nothing on disk
//! openduct topic create sensors --partitions 3       make a topic
//! openduct topic list                                 list topics
//! openduct topic delete sensors                       delete a topic and its data
//! openduct send sensors '{"temp_c": 21.4}'            send a message (or one per line of standard input)
//! openduct read sensors                               print what a topic holds (--follow to keep waiting)
//! openduct pipeline run orders-clean.yaml             run a pipeline (also: check, test)
//! openduct health                                     is the broker up? (exit status 0 or 1)
//! ```
//!
//! Every `serve` option can also be set by an environment variable (`OPENDUCT_LISTEN`, `OPENDUCT_DATA_DIR`, ...),
//! which suits containers and service managers.

#![forbid(unsafe_code)]

mod admin;
#[cfg(feature = "studio")]
mod app_cmd;
#[cfg(feature = "studio")]
mod app_try;
mod cluster;
mod cluster_cmd;
mod codec;
mod messages;
mod otel_log;
#[cfg(feature = "pipelines")]
mod pipeline;
#[cfg(any(feature = "pipelines", feature = "studio"))]
mod secrets_file;
mod security_args;
mod serve;
#[cfg(feature = "studio")]
mod start;
#[cfg(feature = "studio")]
mod workflow;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "openduct", version, about = "OpenDuct: workflows and streams of events, in one program", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start OpenDuct: the broker and the studio, the page in the browser where workflows are built. Open the
    /// address it prints.
    #[cfg(feature = "studio")]
    Start(Box<start::StartArgs>),
    /// Check a workflow file, or try it out on sample events without sending anything.
    #[cfg(feature = "studio")]
    #[command(subcommand)]
    Workflow(workflow::WorkflowCommand),
    /// Build an app: start one, check it, run its recorded exchanges, try an action (see docs/apps.md).
    #[cfg(feature = "studio")]
    #[command(subcommand)]
    App(app_cmd::AppCommand),
    /// Run the broker alone until it is stopped (Ctrl-C or SIGTERM).
    Serve(Box<serve::ServeArgs>),
    /// Create, list and delete topics.
    #[command(subcommand)]
    Topic(admin::TopicCommand),
    /// A cluster's brokers and partitions: status, spreading partitions evenly, taking a broker out.
    #[command(subcommand)]
    Cluster(cluster_cmd::ClusterCommand),
    /// Send messages to a topic: given on the command line, or one per line of standard input.
    Send(messages::SendArgs),
    /// Print the messages a topic holds, and optionally wait for more.
    Read(messages::ReadArgs),
    /// Check, try out, or run a pipeline: read a topic, change each message with expressions, write another.
    #[cfg(feature = "pipelines")]
    #[command(subcommand)]
    Pipeline(pipeline::PipelineCommand),
    /// Check that a broker answers; exits 0 when it does, 1 when it does not.
    Health(admin::Target),
    /// Names that may sign in with SCRAM: added with a password read from standard input, or removed.
    #[command(subcommand)]
    User(security_args::UserCommand),
}

/// The stack of each runtime thread. An expression's parser and evaluator recurse once per level; release builds
/// hold OpenDuct's expression limits in 2 MiB with a wide margin, and this leaves room for a debug build too.
const THREAD_STACK: usize = 16 * 1024 * 1024;

fn main() -> ExitCode {
    // A workflow's script, in a process of its own: before anything else of the program starts.
    #[cfg(feature = "studio")]
    if std::env::args_os()
        .nth(1)
        .is_some_and(|a| a == openduct_script::CHILD_ARG)
    {
        return openduct_script::child::main();
    }
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(THREAD_STACK)
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("openduct: cannot start: {e}");
            return ExitCode::FAILURE;
        }
    };
    let outcome = runtime.block_on(async {
        match cli.command {
            #[cfg(feature = "studio")]
            Command::Start(args) => start::run(*args).await,
            #[cfg(feature = "studio")]
            Command::Workflow(command) => workflow::command(command).await,
            #[cfg(feature = "studio")]
            Command::App(command) => app_cmd::command(command).await,
            Command::Serve(args) => serve::run(*args).await,
            Command::Topic(command) => admin::topic(command).await,
            Command::Cluster(command) => cluster_cmd::run(command).await,
            Command::Send(args) => messages::send(args).await,
            Command::Read(args) => messages::read(args).await,
            #[cfg(feature = "pipelines")]
            Command::Pipeline(command) => pipeline::command(command).await,
            Command::Health(target) => admin::health(target).await,
            Command::User(command) => security_args::user(command),
        }
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("openduct: {message}");
            ExitCode::FAILURE
        }
    }
}
