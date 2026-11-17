// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A run's JavaScript and Python steps, each in a process of its own (see `openduct-script`): the same for a live
//! run, a preview and a test run, since a script only computes.

use openduct_expr::Value;
use openduct_flow::ScriptRequest;
use openduct_script::{Isolated, Language, Script};
use serde_json::json;

type Pending<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// Scripts run by this program, as `openduct __script`.
#[derive(Debug)]
pub(crate) struct Scripts {
    runner: Result<Isolated, String>,
}

impl Scripts {
    pub(crate) fn of_this_program() -> Self {
        Self {
            runner: Isolated::this_program(),
        }
    }
}

impl openduct_flow::Scripts for Scripts {
    fn run<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        Box::pin(async move {
            let runner = self.runner.as_ref().map_err(Clone::clone)?;
            let language = Language::parse(request.language)
                .ok_or_else(|| format!("{} is not a language scripts are written in", request.language))?;
            let script = Script {
                language,
                code: request.code,
                names: request.names,
                time: request.time,
            };
            match runner.run(&script).await {
                Ok(answer) => Ok(Value::from_json(
                    &json!({ "result": answer.result, "logs": answer.logs }),
                )),
                Err(failure) if failure.logs.is_empty() => Err(failure.error),
                Err(failure) => Err(format!(
                    "{}\n--- what it logged ---\n{}",
                    failure.error,
                    failure.logs.join("\n")
                )),
            }
        })
    }
}
