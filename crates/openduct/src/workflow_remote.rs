// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct workflow pull | diff | push | rollback | promote`: a running studio's workflows as files in a folder,
//! for CI/CD. Each signs in with an API token (see [`crate::studio_client`]) and acts in the token's workspace.
//!
//! Exit codes, for CI: 0 done (for `diff --exit-code`: nothing would change); 1 failed (the studio out of reach, a
//! refusal); 2 refused before anything changed (a file is wrong, the workspace lacks what a workflow names); 3 a
//! push would change something (`diff --exit-code`); 4 saved in part (the answer says which workflows were). `--json`
//! prints the studio's answer as one JSON document.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};

use clap::Subcommand;
use openduct_studio::workflow_files::{self, MAX_FILE, MAX_FILES, WorkflowFile};
use serde_json::{Value, json};

use crate::studio_client::{Client, Failure, StudioArgs};

/// The exit code a failure ends with.
static EXIT: AtomicU8 = AtomicU8::new(1);
/// The most a push sends: the studio takes requests of up to 1 MiB.
const MAX_PUSH: usize = 1024 * 1024 - 4096;

/// A field of an answer, or null.
fn at<'a>(v: &'a Value, key: &str) -> &'a Value {
    v.get(key).unwrap_or(&Value::Null)
}

/// The exit code of a command that failed: 1 unless it said otherwise.
pub(crate) fn exit_code() -> u8 {
    EXIT.load(Ordering::Relaxed)
}

fn ending(code: u8, message: String) -> String {
    EXIT.store(code, Ordering::Relaxed);
    message
}

#[derive(Debug, Subcommand)]
pub(crate) enum RemoteCommand {
    /// Write every workflow of the studio's workspace to a folder, a file each, named after it.
    Pull {
        /// The folder (made if it is not there).
        dir: PathBuf,
        #[command(flatten)]
        studio: StudioArgs,
        /// Remove the folder's workflow files that name no workflow of the workspace.
        #[arg(long)]
        prune: bool,
    },
    /// Show what a push of the folder would change, workflow by workflow and step by step.
    Diff {
        dir: PathBuf,
        #[command(flatten)]
        studio: StudioArgs,
        /// As push --activate would.
        #[arg(long)]
        activate: bool,
        /// As push --prune would.
        #[arg(long)]
        prune: bool,
        /// Exit 3 when a push would change something, 0 when it would not.
        #[arg(long)]
        exit_code: bool,
    },
    /// Push the folder's workflow files into the studio: each checked first, nothing changed if one is wrong; new and
    /// changed ones saved as new versions.
    Push {
        dir: PathBuf,
        #[command(flatten)]
        studio: StudioArgs,
        /// Turn on every workflow pushed whose file does not say `on: false`.
        #[arg(long)]
        activate: bool,
        /// Turn off (never delete) the workspace's workflows that are not in the folder.
        #[arg(long)]
        prune: bool,
        /// Say what would change, and change nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Save an earlier version of a workflow again as its newest: the one before, or --to.
    Rollback {
        /// The workflow's name.
        name: String,
        /// The version to go back to.
        #[arg(long)]
        to: Option<u64>,
        #[command(flatten)]
        studio: StudioArgs,
    },
    /// Copy workflows from one environment's workspace to the next (each a new version there), once the target is
    /// found to have every credential, lookup file and table they name. The token is the target's.
    Promote {
        /// The workflow's name (or --all).
        name: Option<String>,
        /// Every workflow of the source.
        #[arg(long, conflicts_with = "name")]
        all: bool,
        /// The workspace promoted from, by name or id.
        #[arg(long)]
        from: String,
        /// The workspace promoted to, by name or id: the token's.
        #[arg(long)]
        to: String,
        /// Turn on the workflows promoted.
        #[arg(long)]
        activate: bool,
        #[arg(long)]
        dry_run: bool,
        #[command(flatten)]
        studio: StudioArgs,
    },
}

pub(crate) async fn command(command: RemoteCommand) -> Result<(), String> {
    match command {
        RemoteCommand::Pull { dir, studio, prune } => pull(&dir, &studio, prune).await,
        RemoteCommand::Diff {
            dir,
            studio,
            activate,
            prune,
            exit_code,
        } => {
            let body = pushed(&dir, &studio, activate, prune, true).await?;
            if exit_code && body.pointer("/summary/changes").and_then(Value::as_bool) == Some(true) {
                return Err(ending(3, String::new()));
            }
            Ok(())
        }
        RemoteCommand::Push {
            dir,
            studio,
            activate,
            prune,
            dry_run,
        } => pushed(&dir, &studio, activate, prune, dry_run).await.map(drop),
        RemoteCommand::Rollback { name, to, studio } => {
            let client = Client::new(&studio)?;
            let body = client
                .post("/api/cicd/rollback", &json!({ "name": name, "to": to }))
                .await
                .map_err(|f| refused(&studio, &f))?;
            if studio.json {
                println!("{body}");
            } else if body.get("unchanged").and_then(Value::as_bool) == Some(true) {
                println!(
                    "{name}: v{} is the same as v{} already: nothing to do",
                    at(&body, "version"),
                    at(&body, "rolled_back_to")
                );
            } else {
                println!(
                    "{name}: v{} is v{} again",
                    at(&body, "version"),
                    at(&body, "rolled_back_to")
                );
            }
            Ok(())
        }
        RemoteCommand::Promote {
            name,
            all,
            from,
            to,
            activate,
            dry_run,
            studio,
        } => {
            if name.is_none() && !all {
                return Err(ending(2, "promote: the workflow's name, or --all".to_owned()));
            }
            let client = Client::new(&studio)?;
            let asked = json!({
                "from": from, "to": to, "workflows": name.map(|n| vec![n]), "activate": activate, "dry_run": dry_run,
            });
            let body = client
                .post("/api/cicd/promote", &asked)
                .await
                .map_err(|f| refused(&studio, &f))?;
            if studio.json {
                println!("{body}");
            } else {
                println!(
                    "{} {} to {}:",
                    if dry_run { "Promoting" } else { "Promoted" },
                    body.pointer("/from/name")
                        .and_then(Value::as_str)
                        .unwrap_or(&from),
                    body.pointer("/to/name").and_then(Value::as_str).unwrap_or(&to)
                );
                print_plan(&body);
            }
            Ok(())
        }
    }
}

/// A refusal said for people (each problem on a line) or as JSON; its exit code: 2 when nothing changed because of
/// what was sent, 4 when saved in part, else 1.
fn refused(studio: &StudioArgs, f: &Failure) -> String {
    if studio.json && !f.body.is_null() {
        println!("{}", f.body);
    }
    let detailed = f.body.get("problems").is_some() || f.body.get("missing").is_some();
    let mut lines = vec![if detailed {
        "the studio refused the workflows: nothing was changed".to_owned()
    } else {
        f.message.clone()
    }];
    for p in f
        .body
        .get("problems")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        lines.push(format!(
            "  {}: {}",
            p.get("file").and_then(Value::as_str).unwrap_or("?"),
            p.get("problem").and_then(Value::as_str).unwrap_or_default()
        ));
    }
    for m in f
        .body
        .get("missing")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let mut lacks = Vec::new();
        for (key, what) in [
            ("credentials", "credential"),
            ("files", "lookup file"),
            ("tables", "table"),
            ("workflows", "workflow"),
        ] {
            for n in m.get(key).and_then(Value::as_array).into_iter().flatten() {
                lacks.push(format!("{what} {}", n.as_str().unwrap_or_default()));
            }
        }
        lines.push(format!(
            "  {} ({}) needs {}",
            m.get("name").and_then(Value::as_str).unwrap_or("?"),
            m.get("file").and_then(Value::as_str).unwrap_or("?"),
            lacks.join(", ")
        ));
    }
    if f.body.get("applied").is_some() && f.status == 503 {
        print_plan(&f.body);
    }
    let code = match f.status {
        422 => 2,
        503 if f.body.get("applied").is_some() => 4,
        _ => 1,
    };
    ending(code, lines.join("\n"))
}

/// The workflow files of a folder, read; every file that is wrong said at once.
pub(crate) fn read_folder(dir: &Path) -> Result<Vec<WorkflowFile>, String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read the folder {}: {e}", dir.display()))?
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| workflow_files::is_workflow_file(n))
        .collect();
    names.sort();
    if names.len() > MAX_FILES {
        return Err(format!(
            "{} holds more than {MAX_FILES} workflow files",
            dir.display()
        ));
    }
    let (mut files, mut wrong) = (Vec::new(), Vec::new());
    for name in names {
        let path = dir.join(&name);
        let read = match std::fs::metadata(&path) {
            Ok(m) if m.len() > MAX_FILE as u64 => Err(format!("{name}: larger than {MAX_FILE} bytes")),
            _ => std::fs::read_to_string(&path)
                .map_err(|e| format!("{name}: cannot read it: {e}"))
                .and_then(|text| workflow_files::read(&name, &text)),
        };
        match read.and_then(|f| {
            openduct_flow::Workflow::from_json(&f.definition, None)
                .map(|_| f)
                .map_err(|e| format!("{name}: {e}"))
        }) {
            Ok(f) => files.push(f),
            Err(e) => wrong.push(format!("  {e}")),
        }
    }
    if !wrong.is_empty() {
        return Err(ending(
            2,
            format!(
                "{} file(s) are not workflows: nothing was sent\n{}",
                wrong.len(),
                wrong.join("\n")
            ),
        ));
    }
    Ok(files)
}

async fn pushed(
    dir: &Path,
    studio: &StudioArgs,
    activate: bool,
    prune: bool,
    dry_run: bool,
) -> Result<Value, String> {
    let files = read_folder(dir)?;
    let workflows: Vec<Value> = files
        .iter()
        .map(|f| json!({ "file": f.file, "definition": f.definition, "on": f.on }))
        .collect();
    let asked = json!({ "workflows": workflows, "activate": activate, "prune": prune, "dry_run": dry_run });
    let size = asked.to_string().len();
    if size > MAX_PUSH {
        return Err(ending(
            2,
            format!(
                "the folder's workflows come to {size} bytes: a push carries at most {MAX_PUSH}; push it in parts"
            ),
        ));
    }
    let client = Client::new(studio)?;
    let body = client
        .post("/api/cicd/push", &asked)
        .await
        .map_err(|f| refused(studio, &f))?;
    if studio.json {
        println!("{body}");
    } else {
        print_plan(&body);
    }
    Ok(body)
}

/// A plan or its outcome, for people: a line a workflow, its changes under it, then the counts.
fn print_plan(body: &Value) {
    let dry = body.get("dry_run").and_then(Value::as_bool).unwrap_or(false);
    for w in body
        .get("workflows")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = w.get("name").and_then(Value::as_str).unwrap_or("?");
        let file = w
            .get("file")
            .and_then(Value::as_str)
            .map(|f| format!(" ({f})"))
            .unwrap_or_default();
        let (was_on, on) = (
            at(w, "was_on").as_bool().unwrap_or(false),
            at(w, "on").as_bool().unwrap_or(false),
        );
        let running = match (was_on, on) {
            (false, true) => ", turned on",
            (true, false) => ", turned off",
            (true, true) => ", on",
            (false, false) => ", off",
        };
        let line = match w.get("action").and_then(Value::as_str).unwrap_or_default() {
            "create" => format!("+ {name}{file}: new, v1{running}"),
            "update" => format!(
                "~ {name}{file}: v{} -> v{}{running}",
                at(w, "from_version"),
                at(w, "to_version")
            ),
            "turn_off" => format!("- {name}: turned off (not in the folder)"),
            _ => format!("= {name}{file}: unchanged{running}"),
        };
        println!("{line}");
        if w.get("action").and_then(Value::as_str) == Some("create") {
            continue;
        }
        let changes = at(w, "changes");
        for s in changes
            .get("settings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            println!("    {}: changed", s.as_str().unwrap_or_default());
        }
        for s in changes
            .get("steps")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let step = s.get("step").and_then(Value::as_str).unwrap_or("?");
            match s.get("change").and_then(Value::as_str).unwrap_or_default() {
                "added" => println!("    + step {step}"),
                "removed" => println!("    - step {step}"),
                _ => {
                    let fields: Vec<&str> = s
                        .get("fields")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .collect();
                    println!("    ~ step {step}: {}", fields.join(", "));
                }
            }
        }
        if changes.get("reordered").and_then(Value::as_bool) == Some(true) {
            println!("    steps in another order");
        }
    }
    let s = at(body, "summary");
    let n = |k: &str| s.get(k).and_then(Value::as_u64).unwrap_or(0);
    let outcome = if dry {
        " (a dry run: nothing was changed)"
    } else {
        ""
    };
    println!(
        "{} new, {} changed, {} unchanged, {} turned on, {} turned off{outcome}",
        n("create"),
        n("update"),
        n("unchanged"),
        n("turn_on"),
        n("turn_off")
    );
    if let Some(not) = body
        .get("not_applied")
        .and_then(Value::as_array)
        .filter(|n| !n.is_empty())
    {
        let names: Vec<&str> = not.iter().filter_map(Value::as_str).collect();
        println!("not saved: {}", names.join(", "));
    }
}

async fn pull(dir: &Path, studio: &StudioArgs, prune: bool) -> Result<(), String> {
    let client = Client::new(studio)?;
    let body = client
        .get("/api/cicd/workflows")
        .await
        .map_err(|f| refused(studio, &f))?;
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot make {}: {e}", dir.display()))?;
    // A workflow keeps the file that names it already; a new one gets a name no file has.
    let mut by_name: BTreeMap<String, String> = BTreeMap::new();
    let mut taken: HashSet<String> = HashSet::new();
    let mut existing: Vec<(String, Option<String>)> = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
        .filter_map(Result::ok)
    {
        let Ok(file) = entry.file_name().into_string() else {
            continue;
        };
        if !workflow_files::is_workflow_file(&file) {
            continue;
        }
        taken.insert(file.clone());
        let named = std::fs::read_to_string(entry.path())
            .ok()
            .and_then(|t| workflow_files::read(&file, &t).ok())
            .and_then(|f| workflow_files::name_of(&f.definition).map(str::to_owned));
        if let Some(name) = &named {
            by_name.entry(name.clone()).or_insert_with(|| file.clone());
        }
        existing.push((file, named));
    }
    let mut written = Vec::new();
    let mut pulled = HashSet::new();
    for w in body
        .get("workflows")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = w
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let file = match by_name.get(&name) {
            Some(f) => f.clone(),
            None => {
                let proposed = w
                    .get("file")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                // The studio's name for the file, if it is a plain workflow file's name no file of the folder has.
                let plain = workflow_files::is_workflow_file(&proposed) && !proposed.contains(['/', '\\']);
                if !plain || taken.contains(&proposed) {
                    workflow_files::file_name(&name, &mut taken)
                } else {
                    taken.insert(proposed.clone());
                    proposed
                }
            }
        };
        let text = workflow_files::write(at(w, "definition"), at(w, "active").as_bool().unwrap_or(false));
        let path = dir.join(&file);
        let changed = std::fs::read_to_string(&path).map_or(true, |old| old != text);
        if changed {
            std::fs::write(&path, &text).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        }
        pulled.insert(file.clone());
        written.push(json!({ "file": file, "name": name, "version": at(w, "version"), "active": at(w, "active"), "written": changed }));
    }
    let mut stale = Vec::new();
    for (file, _) in existing.iter().filter(|(f, _)| !pulled.contains(f)) {
        if prune {
            std::fs::remove_file(dir.join(file)).map_err(|e| format!("cannot remove {file}: {e}"))?;
        }
        stale.push(file.clone());
    }
    let workspace = body
        .pointer("/workspace/name")
        .and_then(Value::as_str)
        .unwrap_or("the workspace");
    if studio.json {
        println!(
            "{}",
            json!({ "workspace": at(&body, "workspace"), "files": written, "stale": stale, "removed": prune })
        );
    } else {
        let changed = written
            .iter()
            .filter(|w| at(w, "written").as_bool() == Some(true))
            .count();
        println!(
            "{} workflow(s) of {workspace} in {}: {changed} written, {} unchanged",
            written.len(),
            dir.display(),
            written.len() - changed
        );
        if !stale.is_empty() {
            println!(
                "{} {} (no such workflow in {workspace})",
                if prune { "removed:" } else { "left as they are:" },
                stale.join(", ")
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_folder_is_read_whole_or_refused_saying_every_wrong_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.yaml"), "name: A\non: true\nsteps: []\n").unwrap();
        std::fs::write(dir.path().join("b.json"), r#"{"name": "B"}"#).unwrap();
        std::fs::write(dir.path().join("notes.md"), "not a workflow").unwrap();
        let files = read_folder(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!((files[0].file.as_str(), files[0].on), ("a.yaml", Some(true)));
        std::fs::write(dir.path().join("c.yaml"), "name: C\ntrigger: { nonsense: {} }\n").unwrap();
        std::fs::write(dir.path().join("d.yaml"), "steps: [\n").unwrap();
        let wrong = read_folder(dir.path()).unwrap_err();
        assert!(
            wrong.contains("2 file(s)") && wrong.contains("c.yaml") && wrong.contains("d.yaml"),
            "{wrong}"
        );
        assert_eq!(exit_code(), 2);
    }
}
