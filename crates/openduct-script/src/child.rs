// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The process a script runs in: it reads the script, takes on its limits - memory, CPU time, no files written, no
//! processes or threads started, on Linux no file or network access at all - runs it, and writes the answer.

use std::io::{Read, Write};
use std::process::ExitCode;

use serde_json::{Value as Json, json};

use crate::{MEMORY, Script, engine, on_big_stack};

/// The most input read: the script and what it is given.
const MAX_INPUT: u64 = 64 * 1024 * 1024;

/// `openduct __script`.
pub fn main() -> ExitCode {
    let mut input = Vec::new();
    let read = std::io::stdin().take(MAX_INPUT).read_to_end(&mut input);
    let script = read
        .map_err(|e| e.to_string())
        .and_then(|_| serde_json::from_slice::<Json>(&input).map_err(|e| e.to_string()))
        .and_then(|json| Script::from_json(&json));
    drop(input);
    let answer = match script {
        Err(e) => json!({ "error": format!("not a script: {e}") }),
        Ok(script) => {
            let ran = on_big_stack(move || {
                // From here this process starts nothing more: not even a thread, so the script's is made first.
                limit(&script);
                confine();
                engine(&script)
            });
            match ran {
                Ok(answer) => json!({ "result": answer.result, "logs": answer.logs }),
                Err(failure) => json!({ "error": failure.error, "logs": failure.logs }),
            }
        }
    };
    let mut out = std::io::stdout().lock();
    let written = serde_json::to_writer(&mut out, &answer)
        .map_err(std::io::Error::from)
        .and_then(|()| out.flush());
    if written.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Limits the operating system holds the process to. One it does not take (macOS keeps no address-space limit) is let
/// go: the time limit and the engines' own confinement still hold.
fn limit(script: &Script) {
    use rustix::process::{Resource, Rlimit, setrlimit};
    // A backstop: the program stops the script at its time well before this.
    let cpu = script.time.as_secs() + 10;
    for (resource, value) in [
        (Resource::As, MEMORY),
        (Resource::Data, MEMORY),
        (Resource::Cpu, cpu),
        (Resource::Fsize, 0),
        (Resource::Core, 0),
        (Resource::Nofile, 16),
        (Resource::Nproc, 0),
    ] {
        let _ = setrlimit(
            resource,
            Rlimit {
                current: Some(value),
                maximum: Some(value),
            },
        );
    }
}

/// On Linux: no file opened, none made, no network connection, no signal to another process, from here on
/// (Landlock, as much of it as the kernel has).
#[cfg(target_os = "linux")]
fn confine() {
    use landlock::{ABI, Access, AccessFs, AccessNet, Ruleset, RulesetAttr, Scope};
    let abi = ABI::V6;
    let made = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))
        .and_then(|r| r.handle_access(AccessNet::from_all(abi)))
        .and_then(|r| r.scope(Scope::from_all(abi)))
        .and_then(|r| r.create());
    if let Ok(ruleset) = made {
        let _ = ruleset.restrict_self();
    }
}

#[cfg(not(target_os = "linux"))]
const fn confine() {}
