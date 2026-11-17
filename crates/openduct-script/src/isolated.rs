// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Starting a script's process - `openduct __script` for JavaScript, `openduct-python` for Python - with no
//! environment (a key in it would be the script's to read), `/` as its directory, only its input and output open,
//! and stopped when it runs past its time.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value as Json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::{Answer, CHILD_ARG, Failure, MAX_ANSWER, MAX_CODE, Script};

/// The time a script's process has to start, beyond the script's own.
const START: Duration = Duration::from_secs(3);

/// Python steps' program, beside `openduct`.
pub const PYTHON_PROGRAM: &str = "openduct-python";

/// Runs scripts each in a process of its own.
#[derive(Debug, Clone)]
pub struct Isolated {
    /// What runs JavaScript, as `<program> __script`.
    program: PathBuf,
    /// What runs Python.
    python: PathBuf,
}

fn seconds(time: Duration) -> String {
    format!("{} s", time.as_secs_f64())
}

impl Isolated {
    /// Scripts of both languages run by `program` (one built with both engines, as tests are).
    #[must_use]
    pub fn new(program: PathBuf) -> Self {
        Self {
            python: program.clone(),
            program,
        }
    }

    /// JavaScript run by this very program, Python by `openduct-python` beside it (or OPENDUCT_PYTHON_PROGRAM).
    pub fn this_program() -> Result<Self, String> {
        let program = std::env::current_exe()
            .map_err(|e| format!("cannot find this program, to run scripts with: {e}"))?;
        let python = std::env::var_os("OPENDUCT_PYTHON_PROGRAM")
            .map_or_else(|| program.with_file_name(PYTHON_PROGRAM), PathBuf::from);
        Ok(Self { program, python })
    }

    /// Runs `script` in a process of its own.
    pub async fn run(&self, script: &Script) -> Result<Answer, Failure> {
        if script.code.len() > MAX_CODE {
            return Err(Failure::new(format!(
                "a script is at most {} KiB",
                MAX_CODE / 1024
            )));
        }
        let mut command = match script.language {
            crate::Language::JavaScript => Command::new(&self.program),
            crate::Language::Python => {
                if !self.python.is_file() {
                    return Err(Failure::new(format!(
                        "Python steps need the {PYTHON_PROGRAM} program, installed beside openduct ({} is not there)",
                        self.python.display()
                    )));
                }
                Command::new(&self.python)
            }
        };
        let mut child = command
            .arg(CHILD_ARG)
            .env_clear()
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| Failure::new(format!("cannot start the script: {e}")))?;
        let (Some(mut input), Some(output)) = (child.stdin.take(), child.stdout.take()) else {
            return Err(Failure::new("cannot talk to the script's process"));
        };
        let request = script.to_json().to_string();
        let exchange = async {
            let writing = async {
                // A process that stopped early reads nothing: what it says (or that it said nothing) is the answer.
                let _ = input.write_all(request.as_bytes()).await;
                drop(input);
            };
            let reading = async {
                let mut out = Vec::new();
                let limit = u64::try_from(MAX_ANSWER).unwrap_or(u64::MAX) + 64 * 1024;
                let _ = output.take(limit).read_to_end(&mut out).await;
                out
            };
            tokio::join!(writing, reading).1
        };
        let Ok(out) = tokio::time::timeout(script.time + START, exchange).await else {
            let _ = child.kill().await;
            return Err(Failure::new(format!(
                "the script ran longer than {}: it was stopped",
                seconds(script.time)
            )));
        };
        let status = child.wait().await;
        if out.len() > MAX_ANSWER {
            return Err(Failure::new(format!(
                "the script's answer is larger than {} MiB",
                MAX_ANSWER / 1024 / 1024
            )));
        }
        let Ok(answer) = serde_json::from_slice::<Json>(&out) else {
            let stopped = status.as_ref().is_ok_and(|s| !s.success());
            return Err(Failure::new(if stopped {
                "the script was stopped: it used more memory or time than a script may"
            } else {
                "the script's process gave no answer"
            }));
        };
        let logs: Vec<String> = answer
            .get("logs")
            .and_then(Json::as_array)
            .map(|l| l.iter().filter_map(Json::as_str).map(str::to_owned).collect())
            .unwrap_or_default();
        match answer.get("error").and_then(Json::as_str) {
            Some(error) => Err(Failure {
                error: error.to_owned(),
                logs,
            }),
            None => Ok(Answer {
                result: answer.get("result").cloned().unwrap_or(Json::Null),
                logs,
            }),
        }
    }
}
