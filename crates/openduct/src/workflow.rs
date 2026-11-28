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
    // developer CI/CD: pull, diff, push, rollback and promote against a running studio.
    #[command(flatten)]
    Remote(crate::workflow_remote::RemoteCommand),
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

/// How many next times `check` shows for a schedule.
const NEXT_SHOWN: usize = 5;

/// For a schedule: what it is in plain words, and its next times - on its own clock, and in UTC.
fn schedule_lines(workflow: &Workflow, now: i64) -> Vec<String> {
    let Some(says) = openduct_flow::calendar::says(&workflow.trigger) else {
        return Vec::new();
    };
    let mut lines = vec![format!("runs {says}; next:")];
    for at in openduct_flow::calendar::next_runs(&workflow.trigger, now, NEXT_SHOWN) {
        let local = openduct_flow::calendar::local(&workflow.trigger, at);
        let utc = openduct_flow::runner::iso(at);
        lines.push(if local == utc {
            format!("  {utc}")
        } else {
            format!("  {local}  ({utc})")
        });
    }
    lines
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

fn load(file: &Path) -> Result<Workflow, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    // developer CI/CD: a file's `on: true` (whether a push turns it on) is the file's, not the workflow's.
    let read = openduct_studio::workflow_files::read(&file.display().to_string(), &text)?;
    Workflow::from_json(&read.definition, None).map_err(|e| format!("{}: {e}", file.display()))
}

pub(crate) async fn command(command: WorkflowCommand) -> Result<(), String> {
    match command {
        WorkflowCommand::Check { file } => {
            let w = load(&file)?;
            println!("workflow {} ({}): {} steps, ready", w.name, w.id, w.steps.len());
            // cron-schedules: a schedule says when it runs next.
            for line in schedule_lines(&w, now_ms()) {
                println!("{line}");
            }
            // MCP: what AI assistants are offered.
            if let Some(tool) = &w.tool {
                println!(
                    "offered as the tool {}, taking {} argument(s)",
                    tool.name,
                    tool.inputs.len()
                );
            }
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
        WorkflowCommand::Remote(remote) => crate::workflow_remote::command(remote).await, // developer CI/CD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_schedule_says_when_it_runs_next() {
        let w = Workflow::parse(
            "name: Report\ntrigger:\n  schedule:\n    every: week\n    on: monday\n    at: '08:00'\n    \
             timezone: Europe/Berlin\n",
        )
        .unwrap();
        // Friday 27 November 2026, 10:00 UTC.
        let lines = schedule_lines(&w, 1_795_773_600_000);
        assert_eq!(
            lines[0],
            "runs every week on Monday at 08:00 (Europe/Berlin); next:"
        );
        assert_eq!(lines[1], "  2026-11-30T08:00:00+01:00  (2026-11-30T07:00:00Z)");
        assert_eq!(lines.len(), 1 + NEXT_SHOWN);
        let every = Workflow::parse("name: Tick\ntrigger: { schedule: { every: 1h } }\n").unwrap();
        assert_eq!(
            schedule_lines(&every, 1_795_773_600_000)[..2],
            [
                "runs every 1h, counted from midnight UTC; next:",
                "  2026-11-27T11:00:00Z"
            ]
        );
        assert!(schedule_lines(&Workflow::parse("name: By hand\n").unwrap(), 0).is_empty());
    }
}
