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

mod ai;
pub mod app_files;
pub mod apps;
mod effects;
mod email;
mod model;
mod preview;
mod run;
pub mod runner;
mod template;
mod yaml;

pub use ai::{models_in, models_request};
pub use apps::{Credential, MASK};
pub use effects::{
    Files, Live, MAX_ANSWER, Scripts, Secrets, Workflows, private, workspace_prefix, workspace_topic,
};
pub use model::{App, Body, Email, Http, Kind, MAX_STEPS, MAX_WAIT, Step, Trigger, Workflow, duration, slug};
pub use preview::Preview;
pub use run::{
    DryRun, Effects, EmailRequest, Event, HttpRequest, MAX_ITEMS, MAX_RESULT, Outcome, RunRecord,
    ScriptRequest, SqlRequest, StepRecord, run,
};
pub use template::{Template, as_text};

/// A workflow file's YAML (or JSON) as the JSON a workflow is kept as.
pub fn yaml_to_json(text: &str) -> Result<serde_json::Value, String> {
    yaml::to_json(text)
}
