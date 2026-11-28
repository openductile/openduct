// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An agent step's run (see [`crate::agent`]). Its tools are steps of the workflow's own kinds, run by the run itself,
//! through its effects: a test run sends nothing, a preview shows a write and does not send it, and the rule against
//! this machine's own network holds. A tool reads the model's arguments as `args`, beside `value`, `steps` and
//! `trigger`. A tool's call is not a step of the run's history: the agent's record holds it.
//!
//! Another kind of tool is one more arm in [`Run::agent`]: its tools made there, each a [`Tool`].

use openduct_expr::{Env, Value};
use serde_json::Value as Json;

use super::{Done, Run};
use crate::agent::{Agent, Pending, Tool, ToolKind, ToolSpec};
use crate::model::{Kind, Step, Tree};

/// A step the model may call.
struct StepTool<'r, 'a> {
    run: &'r Run<'a>,
    spec: &'r ToolSpec,
    step: &'r Step,
    /// An app's action's inputs the model fills.
    fills: &'r [String],
    scope: &'r [(String, Value)],
}

impl Tool for StepTool<'_, '_> {
    fn spec(&self) -> &ToolSpec {
        self.spec
    }

    fn call<'b>(&'b self, args: Value) -> Pending<'b> {
        Box::pin(async move {
            let filled;
            let step: &Step = if self.fills.is_empty() {
                self.step
            } else {
                filled = with_args(self.step, self.fills, &args);
                &filled
            };
            let mut scope = self.scope.to_vec();
            scope.push(("args".to_owned(), args));
            match self.run.once(step, &scope).await? {
                Done::Value(v) | Done::Logged(v, _) | Done::Noted(v, _) => Ok(v), // test runs: Noted
                Done::Skip | Done::End(_) => Ok(Value::Null),
            }
        })
    }
}

/// An app's action, the inputs the model fills taken from its arguments as they are.
fn with_args(step: &Step, fills: &[String], args: &Value) -> Step {
    let mut step = step.clone();
    if let Kind::Defined(d) = &mut step.kind
        && let Value::Record(given) = args
    {
        for name in fills {
            let Some(value) = given.get(name) else { continue };
            let literal = Tree::Literal(serde_json::from_str::<Json>(&value.to_json()).unwrap_or(Json::Null));
            match d.inputs.iter_mut().find(|(n, _)| n == name) {
                Some((_, tree)) => *tree = literal,
                None => d.inputs.push((name.clone(), literal)),
            }
        }
    }
    step
}

impl Run<'_> {
    /// The agent's turns, its tools run here; its memory kept under this workflow and step.
    pub(super) async fn agent(
        &self,
        step: &Step,
        agent: &Agent,
        env: &Env<'_>,
        scope: &[(String, Value)],
    ) -> Result<Value, String> {
        let mut tools: Vec<Box<dyn Tool + '_>> = Vec::with_capacity(agent.tools.len());
        for def in &agent.tools {
            match &def.does {
                ToolKind::Step { step, fills } => tools.push(Box::new(StepTool {
                    run: self,
                    spec: &def.spec,
                    step,
                    fills,
                    scope,
                })),
            }
        }
        let tools: Vec<&dyn Tool> = tools.iter().map(AsRef::as_ref).collect();
        let scope_key = format!("{}/{}", self.workflow, step.name);
        crate::agent::converse(self.effects, agent, env, &tools, &scope_key).await
    }
}
