// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct's workflows: what starts a run (a webhook, a schedule, a topic's messages, a person, another workflow's
//! failed run) and the steps it takes - working out fields, deciding (if, switch), going through lists (for_each,
//! list, group, merge), dates and times, calling HTTP APIs and other workflows, answering a webhook's caller,
//! writing to topics, waiting. Fields
//! that compute a value are expressions ([`openduct_expr`]); what configures a step (a URL, a message's text) is text
//! with `{{ expression }}` in it.
//!
//! ```
//! use openduct_flow::{DryRun, Event, Outcome, Workflow, run};
//! use openduct_expr::Value;
//!
//! let workflow = Workflow::parse(r#"
//! name: Big orders
//! trigger: webhook
//! steps:
//!   - name: total
//!     set: { amount: "value.qty * value.price" }
//!   - where: "steps.total.amount > 100"
//!   - name: tell
//!     http: { method: POST, url: "https://example.com/hook", json: { text: "Order of {{ steps.total.amount }}" } }
//! "#).unwrap();
//! let event = Event { value: Value::parse_json(r#"{"qty": 3, "price": 50}"#).unwrap(), details: Value::Null };
//! let record = tokio::runtime::Runtime::new().unwrap().block_on(run(&workflow, &event, &DryRun::default()));
//! assert_eq!(record.outcome, Outcome::Done);
//! ```

#![forbid(unsafe_code)]

pub mod agent; // G2 AI agents
mod ai;
mod ai_steps; // G7 more AI steps
pub mod app_files;
pub mod apps;
// cron-schedules: schedules at set times, in a time zone.
pub mod calendar;
pub mod chat; // chat trigger (G8)
pub mod databases; // sql databases
mod effects;
mod email;
pub mod mcp; // MCP
// forms-and-resume
pub mod files; // G6 files
pub mod forms;
pub mod mailbox; // messaging
pub mod meters;
mod model;
mod preview;
mod retrieval; // retrieval
mod run;
pub mod runner;
mod schema; // G7 more AI steps: JSON schemas a model's answer must meet
pub mod storage; // storage
pub mod tables; // data tables
mod template;
mod tool; // MCP
// forms-and-resume
pub mod waits;
pub mod web; // web
mod yaml;

pub use agent::LogMemory; // G2 AI agents
pub use ai::{models_in, models_request};
pub use ai_steps::AiStep; // G7 more AI steps
pub use apps::{Credential, MASK};
pub use effects::Memory; // G2 AI agents
pub use effects::{
    Files, Live, MAX_ANSWER, Scripts, Secrets, Workflows, private, workspace_prefix, workspace_topic,
};
pub use model::AppTrigger; // G3 app triggers
pub use model::{App, Body, Email, Http, Kind, MAX_STEPS, MAX_WAIT, Step, Trigger, Workflow, duration, slug};
pub use preview::Preview;
pub use run::{
    DryRun, Effects, EmailRequest, Event, HttpRequest, MAX_ITEMS, MAX_RESULT, Outcome, RunRecord,
    ScriptRequest, SqlRequest, StepRecord, run,
};
pub use template::{Template, as_text};
pub use tool::{Input, InputKind, MAX_INPUTS, MAX_TOOL_TIME, TOOL_TIME, Tool, tool_name_ok}; // MCP

/// A workflow file's YAML (or JSON) as the JSON a workflow is kept as.
pub fn yaml_to_json(text: &str) -> Result<serde_json::Value, String> {
    yaml::to_json(text)
}
