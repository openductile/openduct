// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Runs that wait for something outside: a form filled in, an approval, a call to a one-time address, a time. Such a
//! run is not held in memory: it stops at the step that waits, what it has done so far is kept on the log (with the
//! one-time address of its step), and the run's event is committed as any finished run's is. When the answer comes -
//! the first answer, or the timeout, whichever the log holds first - the run goes on from that step: the steps before
//! it are not run again, their results taken as they were. A restart, or the loss of the node it ran on, changes
//! nothing: what waits is on the log, and any node takes the answer.
//!
//! Each step that waits has its address before the run reaches it, as `links.<step>`, so an earlier step can send it
//! (an e-mail with the form's link, an HTTP call that hands a callback address over). An address is used once.

use std::collections::BTreeMap;
use std::time::Duration;

use openduct_expr::Compiled;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::forms::FormSpec;
use crate::model::{Kind, Step, Trigger};

mod resume;
mod store;

pub(crate) use resume::{Taken, launch, takes};
pub use store::{Answered, MAX_STATE, MAX_WAITING, Summary, TOPIC, Waits, keep_sweeping};

/// A step that waits: what for, how long, and what then.
#[derive(Debug, Clone)]
pub struct Pause {
    pub on: PauseOn,
    /// How long it waits for its answer (a time comes when it comes: it has none).
    pub timeout: Duration,
    pub on_timeout: OnTimeout,
}

/// What a step waits for.
#[derive(Debug, Clone)]
pub enum PauseOn {
    /// A form filled in (an approval is one too: approve or reject).
    Form(Box<FormSpec>),
    /// A call to its one-time address.
    Webhook,
    /// A time, as an expression: text such as 2026-12-01T09:00:00Z, or milliseconds since 1970.
    Until(Compiled),
    /// A while: longer than a run is held for.
    For(Duration),
}

/// What a run does when its answer does not come in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnTimeout {
    /// The step fails (and the run with it, unless the step goes on when it fails).
    Fail,
    /// The run goes on: the step's result says `timed_out: true`.
    Continue,
}

impl Pause {
    /// The step's kind, as a run's record names it.
    pub fn kind_name(&self) -> &'static str {
        match &self.on {
            PauseOn::Form(spec) if spec.is_approval() => "approval",
            PauseOn::Form(_) => "form",
            PauseOn::Webhook | PauseOn::Until(_) | PauseOn::For(_) => "wait",
        }
    }

    /// What it waits for, as its record says it.
    pub(crate) fn waits_for(&self) -> &'static str {
        match &self.on {
            PauseOn::Form(spec) if spec.is_approval() => "approval",
            PauseOn::Form(_) => "form",
            PauseOn::Webhook => "webhook",
            PauseOn::Until(_) | PauseOn::For(_) => "time",
        }
    }
}

/// What a run that stopped waits for: kept with it, and shown with its run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Waiting {
    /// The step that waits.
    pub step: String,
    /// `form`, `approval`, `webhook` or `time`.
    pub kind: String,
    /// Its one-time address's last part: `/waits/<token>`.
    pub token: String,
    /// The form its page shows (a form's, an approval's), its texts worked out from the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<Json>,
    /// When it goes on by itself (milliseconds since 1970): its time, or when its answer is too late.
    pub until: i64,
    pub on_timeout: OnTimeout,
    /// When it began to wait.
    pub since: i64,
}

/// Every step of these (and of their branches and loops) that waits.
pub(crate) fn pauses(steps: &[Step]) -> Vec<&Step> {
    let mut out = Vec::new();
    walk(steps, &mut out);
    out
}

fn walk<'a>(steps: &'a [Step], out: &mut Vec<&'a Step>) {
    for step in steps {
        match &step.kind {
            Kind::Pause(_) => out.push(step),
            Kind::If { then, otherwise, .. } => {
                walk(then, out);
                walk(otherwise, out);
            }
            Kind::ForEach { steps, .. } => walk(steps, out),
            Kind::Switch { cases, otherwise } => {
                for case in cases {
                    walk(&case.steps, out);
                }
                walk(otherwise, out);
            }
            _ => {}
        }
    }
}

/// Whether a workflow's steps that wait may wait under its trigger: a run that waits goes on through its workflow's
/// own topic, which a workflow started by another topic, or by another workflow's failures, does not read.
pub(crate) fn check(trigger: &Trigger, steps: &[Step]) -> Result<(), String> {
    let Some(first) = pauses(steps).first().map(|s| s.name.clone()) else {
        return Ok(());
    };
    match trigger {
        Trigger::Topic { .. } | Trigger::Failed { .. } => Err(format!(
            "steps.{first}: a workflow whose runs wait for an answer is started by a webhook, a form, a schedule or \
             by hand - not by a topic or another workflow's failures"
        )),
        _ => Ok(()),
    }
}

/// The token of a step's one-time address: the same for the same workflow, part of a run and step, on every node -
/// so a run cut short and run again hands out the same address - and never guessed without the key.
pub(crate) fn token(key: &hmac::Key, workflow: &str, seed: &str, step: &str) -> String {
    use base64::Engine as _;
    let tag = hmac::sign(
        key,
        format!("openduct-wait\n{workflow}\n{seed}\n{step}").as_bytes(),
    );
    let bytes = tag.as_ref().get(..16).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Whether text is a token as [`token`] makes them: 22 letters, digits, `-` and `_`.
pub fn is_token(text: &str) -> bool {
    text.len() == 22
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// The tokens of a part of a run, step by step: each step that waits gets one made from `seed` (the run's id for its
/// first part, the token it went on from for a later one); those in `kept` are kept as they were handed out.
pub(crate) fn tokens(
    key: &hmac::Key,
    workflow: &crate::model::Workflow,
    seed: &str,
    kept: &BTreeMap<String, String>,
    fresh: &str,
) -> BTreeMap<String, String> {
    pauses(&workflow.steps)
        .into_iter()
        .map(|s| {
            let made = match kept.get(&s.name) {
                Some(t) if s.name != fresh => t.clone(),
                _ => token(key, &workflow.id, seed, &s.name),
            };
            (s.name.clone(), made)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]

    use super::*;

    #[test]
    fn a_token_is_the_same_for_the_same_run_and_step_and_unlike_any_other() {
        let key = hmac::Key::new(hmac::HMAC_SHA256, b"a key of the store's own");
        let a = token(&key, "w1", "1-7", "approve");
        assert_eq!(a, token(&key, "w1", "1-7", "approve"));
        assert!(is_token(&a), "{a}");
        for other in [
            token(&key, "w1", "1-8", "approve"),
            token(&key, "w2", "1-7", "approve"),
            token(&key, "w1", "1-7", "other"),
            token(
                &hmac::Key::new(hmac::HMAC_SHA256, b"another key"),
                "w1",
                "1-7",
                "approve",
            ),
        ] {
            assert_ne!(a, other);
        }
        assert!(!is_token("short") && !is_token("../../../etc/passwd/xx"));
    }

    #[test]
    fn a_workflow_started_by_a_topic_cannot_wait() {
        let w = |trigger: &str| {
            crate::Workflow::parse(&format!(
                "name: w\ntrigger: {trigger}\nsteps:\n  - name: ok\n    approval: {{ title: Fine? }}\n"
            ))
        };
        assert!(w("webhook").is_ok());
        assert!(w("manual").is_ok());
        let refused = w("{ topic: { name: orders } }").unwrap_err();
        assert!(refused.contains("not by a topic"), "{refused}");
    }
}
