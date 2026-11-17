// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct workflow`: a workflow file checked, or tried out on sample events without sending anything - the way to
//! get a workflow right before it goes into the studio.

use std::path::{Path, PathBuf};

use clap::Subcommand;
use openduct_expr::Value;
use openduct_flow::{DryRun, Event, Workflow};
use serde_json::json;

#[derive(Debug, Subcommand)]
pub(crate) enum WorkflowCommand {
    /// Check a workflow file: its trigger, every step, every expression - without running it.
    Check {
        /// The workflow file (YAML or JSON).
        file: PathBuf,
    },
    /// Run a workflow file on sample events, sending nothing: prints each run, step by step.
    Test {
        /// The workflow file (YAML or JSON).
        file: PathBuf,
        /// The events: one JSON value per line. Without it, the workflow's own `sample`.
        #[arg(long)]
        input: Option<PathBuf>,
        /// A folder its lookup files are read from (`/rates.csv` is `<dir>/rates.csv`).
        #[arg(long)]
        files_dir: Option<PathBuf>,
    },
}

fn load(file: &Path) -> Result<Workflow, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    Workflow::parse(&text).map_err(|e| format!("{}: {e}", file.display()))
}

pub(crate) async fn command(command: WorkflowCommand) -> Result<(), String> {
    match command {
        WorkflowCommand::Check { file } => {
            let w = load(&file)?;
            println!("workflow {} ({}): {} steps, ready", w.name, w.id, w.steps.len());
            Ok(())
        }
        WorkflowCommand::Test {
            file,
            input,
            files_dir,
        } => {
            let workflow = load(&file)?;
            let (text, from) = match &input {
                Some(input) => (
                    std::fs::read_to_string(input)
                        .map_err(|e| format!("cannot read {}: {e}", input.display()))?,
                    input.display().to_string(),
                ),
                None => {
                    let raw = std::fs::read_to_string(&file)
                        .map_err(|e| format!("cannot read {}: {e}", file.display()))?;
                    let json =
                        openduct_flow::yaml_to_json(&raw).map_err(|e| format!("{}: {e}", file.display()))?;
                    let sample = json.get("sample").ok_or_else(|| {
                        format!(
                            "{}: no sample in it: give the events with --input",
                            file.display()
                        )
                    })?;
                    (sample.to_string(), format!("{}'s sample", file.display()))
                }
            };
            for (n, line) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
                let value = Value::parse_json(line).map_err(|e| format!("{from} line {}: {e}", n + 1))?;
                let event = Event {
                    value,
                    details: Value::from_json(&json!({ "trigger": "test" })),
                };
                let test = DryRun {
                    scripts: Some(openduct_studio::script_runner()),
                    files: files_dir.as_deref().map(openduct_studio::local_files),
                    ..DryRun::default()
                };
                let record = openduct_flow::run(&workflow, &event, &test).await;
                let mut shown = record.to_json();
                if let Some(fields) = shown.as_object_mut() {
                    fields.insert("line".to_owned(), json!(n + 1));
                }
                println!("{shown}");
            }
            Ok(())
        }
    }
}
