// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! JavaScript and Python steps for OpenDuct's workflows.
//!
//! A step's script runs in a process of its own (`openduct __script` for JavaScript, `openduct-python` for Python,
//! installed beside it), started with nothing of the program's
//! surroundings - no environment, no open files but its input and output, `/` as its directory - and with limits on
//! its memory, CPU time, files and processes. It is stopped when it runs past its time.
//!
//! Inside, both engines are pure Rust and hold nothing that reaches outside. JavaScript is the language alone: no
//! file, network, process or timer functions. Python is the language with the computing part of its standard library
//! (json, re, datetime, math, decimal, statistics, collections, itertools, hashlib, base64, csv ...), and none of the
//! modules that open a file, a socket or a process: those are not built in, so no Python code can call them. On Linux
//! the process also gives up all file and network access (Landlock) before the script runs.
//!
//! So a script cannot read OpenDuct's data, keys or credentials, reach the network, start a program, or bring the
//! studio down: it computes an answer from what it is given.
//!
//! A script is given `value` (what came in), `steps` (the earlier steps' results), `trigger`, and in a loop `item`
//! and `index`. JavaScript `return`s its answer; Python sets `result`. `console.log` and `print` are kept as the
//! step's logs.

use std::time::Duration;

use serde_json::{Map, Value as Json, json};

#[cfg(any(feature = "javascript", feature = "python"))]
pub mod child;
mod isolated;
#[cfg(feature = "javascript")]
mod javascript;
#[cfg(feature = "python")]
mod python;

pub use isolated::{Isolated, PYTHON_PROGRAM};

/// The argument that makes `openduct` the process a script runs in.
pub const CHILD_ARG: &str = "__script";
/// The longest script.
pub const MAX_CODE: usize = 256 * 1024;
/// The largest answer, as JSON.
pub const MAX_ANSWER: usize = 4 * 1024 * 1024;
/// The most a script's logs keep.
pub const MAX_LOGS: usize = 64 * 1024;
/// How long a script runs unless its step says otherwise, and the most a step may say.
pub const DEFAULT_TIME: Duration = Duration::from_secs(10);
pub const MAX_TIME: Duration = Duration::from_secs(60);
/// The memory a script's process may take.
pub const MEMORY: u64 = 1024 * 1024 * 1024;

/// The languages a step can be written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    JavaScript,
    Python,
}

impl Language {
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "javascript" => Some(Self::JavaScript),
            "python" => Some(Self::Python),
            _ => None,
        }
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::JavaScript => "javascript",
            Self::Python => "python",
        }
    }
}

/// A script to run, with the names it is given.
#[derive(Debug, Clone)]
pub struct Script {
    pub language: Language,
    pub code: String,
    /// `value`, `steps`, `trigger` ... as JSON.
    pub names: Map<String, Json>,
    pub time: Duration,
}

/// What a script gave: its answer, and what it logged.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub result: Json,
    pub logs: Vec<String>,
}

/// Why a script gave no answer, with what it logged before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub error: String,
    pub logs: Vec<String>,
}

impl Failure {
    pub(crate) fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            logs: Vec::new(),
        }
    }
}

impl Script {
    pub(crate) fn to_json(&self) -> Json {
        json!({
            "language": self.language.name(),
            "code": self.code,
            "names": self.names,
            "time_ms": u64::try_from(self.time.as_millis()).unwrap_or(u64::MAX),
        })
    }

    #[cfg(any(feature = "javascript", feature = "python"))]
    pub(crate) fn from_json(json: &Json) -> Result<Self, String> {
        let language = json
            .get("language")
            .and_then(Json::as_str)
            .and_then(Language::parse)
            .ok_or("no language")?;
        let code = json
            .get("code")
            .and_then(Json::as_str)
            .ok_or("no code")?
            .to_owned();
        let names = json
            .get("names")
            .and_then(Json::as_object)
            .cloned()
            .unwrap_or_default();
        let time = Duration::from_millis(json.get("time_ms").and_then(Json::as_u64).unwrap_or(10_000));
        Ok(Self {
            language,
            code,
            names,
            time,
        })
    }
}

/// Runs `script` in this process, for tests: nothing here limits its memory or time - [`Isolated`] does.
#[cfg(any(feature = "javascript", feature = "python"))]
pub fn run_here(script: &Script) -> Result<Answer, Failure> {
    let script = script.clone();
    on_big_stack(move || engine(&script))
}

#[cfg(any(feature = "javascript", feature = "python"))]
pub(crate) fn engine(script: &Script) -> Result<Answer, Failure> {
    if script.code.len() > MAX_CODE {
        return Err(Failure::new(format!(
            "a script is at most {} KiB",
            MAX_CODE / 1024
        )));
    }
    match script.language {
        #[cfg(feature = "javascript")]
        Language::JavaScript => javascript::run(script),
        #[cfg(feature = "python")]
        Language::Python => python::run(script),
        #[allow(unreachable_patterns)]
        other => Err(Failure::new(format!(
            "this program runs no {} (openduct runs JavaScript, openduct-python Python)",
            other.name()
        ))),
    }
}

/// Runs `work` on a thread with room for deep recursion, more than a process's first thread may have.
#[cfg(any(feature = "javascript", feature = "python"))]
pub(crate) fn on_big_stack<F>(work: F) -> Result<Answer, Failure>
where
    F: FnOnce() -> Result<Answer, Failure> + Send + 'static,
{
    std::thread::Builder::new()
        .name("script".to_owned())
        .stack_size(256 * 1024 * 1024)
        .spawn(work)
        .map_err(|e| Failure::new(format!("cannot start the script: {e}")))?
        .join()
        .unwrap_or_else(|_| Err(Failure::new("the script's engine stopped")))
}

/// Logs kept to [`MAX_LOGS`], with a last line saying what was left out.
#[cfg(any(feature = "javascript", feature = "python"))]
pub(crate) fn kept_logs(lines: Vec<String>) -> Vec<String> {
    let mut kept = Vec::new();
    let mut size = 0;
    let total = lines.len();
    for line in lines {
        size += line.len() + 1;
        if size > MAX_LOGS {
            kept.push(format!("... {} more lines left out", total - kept.len()));
            break;
        }
        kept.push(line);
    }
    kept
}
