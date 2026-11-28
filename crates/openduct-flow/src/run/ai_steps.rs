// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An AI step in a run (see [`crate::ai_steps`]): its task done - tried again as its settings say - and recorded,
//! then the steps of the routes it chose, as a switch's case's steps run.

use std::time::{Duration, Instant};

use openduct_expr::{Env, Value};
use serde_json::json;

use super::{Flow, MAX_RESULT, Run, millis};
use crate::ai_steps::AiStep;
use crate::model::Step;

impl Run<'_> {
    pub(super) async fn ai_step(
        &mut self,
        step: &Step,
        ai: &AiStep,
        scope: &[(String, Value)],
    ) -> Result<Flow, (String, String)> {
        let fail = |why: String| (step.name.clone(), why);
        let started = Instant::now();
        let mut wait = step.retry_wait;
        let mut attempt = 0;
        let did = loop {
            attempt += 1;
            let names = self.names(scope);
            let env = Env {
                value: &self.event.value,
                message: &self.event.details,
                names: &names,
            };
            match crate::ai_steps::run(ai, self.effects, &env).await {
                Ok(did) => break Ok(did),
                Err(error) if attempt < step.attempts => {
                    tracing::debug!(step = step.name, attempt, error, "a step failed: trying again");
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2).min(Duration::from_secs(300));
                }
                Err(error) => break Err(error),
            }
        };
        let did = match did {
            Ok(did) => did,
            Err(error) => {
                let result = Value::from_json(&json!({ "error": error }));
                self.record(
                    step,
                    attempt,
                    millis(started.elapsed()),
                    result,
                    Some(error.clone()),
                );
                return if step.continue_on_error {
                    Ok(Flow::Next)
                } else {
                    Err(fail(error))
                };
            }
        };
        if did.result.size() > MAX_RESULT {
            return Err(fail(format!("a result of more than {MAX_RESULT} bytes")));
        }
        self.record(step, attempt, millis(started.elapsed()), did.result, None);
        for route in did.routes {
            match self.steps_of(ai.steps_of(route), scope).await? {
                Flow::Next => {}
                other => return Ok(other),
            }
        }
        Ok(Flow::Next)
    }
}
