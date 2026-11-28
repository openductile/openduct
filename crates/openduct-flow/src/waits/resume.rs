// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow's runs that wait, as its runner runs them: a run that stops at a step that waits is kept (in the runs
//! that wait, then in its history as waiting) before its event is committed; the event that says its answer is in
//! takes it up again - once - and the run goes on from the step that waited, its history keeping one run from its
//! start to its end.

use std::collections::BTreeMap;

use base64::Engine as _;
use bytes::Bytes;
use openduct_broker::client::Shared;
use openduct_expr::Value;
use serde_json::{Value as Json, json};

use super::store::{Claim, Paused};
use super::{OnTimeout, Waiting, Waits, pauses};
use crate::model::Workflow;
use crate::run::pause::{Answer, Segment, kept_json};
use crate::run::{Effects, Event, Outcome, RunRecord, run_with};
use crate::runner::{Topics, iso, keep};

/// A record a workflow's runner took in, as a run of it is started with.
#[derive(Debug)]
pub(crate) struct Taken<'a> {
    /// `<partition>-<offset>`: the run's id, or for an event that takes a run up again, that event's.
    pub id: String,
    pub event: &'a Event,
    pub original: Option<&'a Bytes>,
    pub hops: u32,
}

/// The token an event names when it says a run's answer is in.
fn resume_token(event: &Event) -> Option<String> {
    let Value::Record(details) = &event.details else {
        return None;
    };
    if details.get("trigger").map(crate::template::as_text).as_deref() != Some("resume") {
        return None;
    }
    details
        .get("token")
        .map(crate::template::as_text)
        .filter(|t| super::is_token(t))
}

/// Whether this record's run is one for [`launch`]: a workflow with steps that wait, or an event taking a run up.
pub(crate) fn takes(workflow: &Workflow, event: &Event) -> bool {
    !pauses(&workflow.steps).is_empty() || resume_token(event).is_some()
}

fn meter(record: &RunRecord) {
    let ended = match record.outcome {
        Outcome::Done => crate::meters::Ended::Done,
        Outcome::Skipped { .. } => crate::meters::Ended::Skipped,
        Outcome::Failed { .. } => crate::meters::Ended::Failed,
        Outcome::Waiting { .. } => return,
    };
    crate::meters::run(ended, std::time::Duration::from_millis(record.ms));
}

/// Runs a record of a workflow that waits, or takes a waiting run up again; keeps what came of it.
pub(crate) async fn launch(
    workflow: &Workflow,
    shared: &Shared,
    topics: &Topics,
    effects: &dyn Effects,
    taken: Taken<'_>,
) -> Result<(), String> {
    let Some(waits) = effects.waits() else {
        // Nowhere to keep a wait: the run runs, and its steps that wait fail, saying why.
        let record = crate::run::run(workflow, taken.event, effects).await;
        meter(&record);
        return keep(
            shared,
            topics,
            &taken.id,
            taken.event,
            &record,
            taken.original,
            taken.hops,
        )
        .await;
    };
    if let Some(token) = resume_token(taken.event) {
        return go_on(workflow, shared, topics, effects, &waits, &token, &taken.id).await;
    }
    let tokens = waits.tokens(workflow, &taken.id, &BTreeMap::new(), "").await?;
    let segment = Segment::live(tokens.clone(), |t| waits.address(t), &[], None)?;
    let record = run_with(workflow, taken.event, effects, segment, Default::default()).await;
    let part = Part {
        run: taken.id.clone(),
        event: taken.event,
        original: taken.original.cloned(),
        hops: taken.hops,
        ms_before: 0,
        tokens,
    };
    finish(workflow, shared, topics, &waits, part, record).await
}

/// What a part of a run carries over from the parts before it.
struct Part<'a> {
    run: String,
    event: &'a Event,
    original: Option<Bytes>,
    hops: u32,
    ms_before: u64,
    tokens: BTreeMap<String, String>,
}

/// Keeps what a part of a run came to: a run that waits, kept to go on later; a run that ended, in its history.
async fn finish(
    workflow: &Workflow,
    shared: &Shared,
    topics: &Topics,
    waits: &Waits,
    part: Part<'_>,
    mut record: RunRecord,
) -> Result<(), String> {
    record.ms = record.ms.saturating_add(part.ms_before);
    if let Outcome::Waiting { waiting, at } = &record.outcome {
        let as_json = |v: &Value| serde_json::from_str::<Json>(&v.to_json()).unwrap_or(Json::Null);
        let paused = Paused {
            workflow: workflow.id.clone(),
            run: part.run.clone(),
            value: as_json(&part.event.value),
            details: as_json(&part.event.details),
            original: part
                .original
                .as_ref()
                .map(|b| base64::engine::general_purpose::STANDARD.encode(b)),
            hops: part.hops,
            journal: record.steps.iter().map(kept_json).collect(),
            tokens: part.tokens.clone(),
            ms: record.ms,
            waiting: (**waiting).clone(),
        };
        // Kept before its history is: a run whose wait is not kept is run again.
        if let Err(why) = waits.pause(&paused).await? {
            record.outcome = Outcome::Failed {
                at: at.clone(),
                error: why,
            };
        }
    }
    meter(&record);
    keep(
        shared,
        topics,
        &part.run,
        part.event,
        &record,
        part.original.as_ref(),
        part.hops,
    )
    .await
}

/// Takes the run waiting at `token` up again, as the event `id` says - or nothing, when another event took it up
/// first or nothing waits there any more.
async fn go_on(
    workflow: &Workflow,
    shared: &Shared,
    topics: &Topics,
    effects: &dyn Effects,
    waits: &Waits,
    token: &str,
    id: &str,
) -> Result<(), String> {
    let Some((paused, claim)) = waits.take(token, id).await? else {
        return Ok(());
    };
    let event = Event {
        value: Value::from_json(&paused.value),
        details: Value::from_json(&paused.details),
    };
    let original = paused
        .original
        .as_deref()
        .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
        .map(Bytes::from);
    // The step that waited gets a new token, should the run reach it again; the others keep theirs.
    let tokens = waits
        .tokens(workflow, token, &paused.tokens, &paused.waiting.step)
        .await?;
    let answer = answer_of(&paused.waiting, &claim);
    let record = match Segment::live(
        tokens.clone(),
        |t| waits.address(t),
        &paused.journal,
        Some(answer),
    ) {
        Ok(segment) => run_with(workflow, &event, effects, segment, Default::default()).await,
        Err(why) => RunRecord {
            outcome: Outcome::Failed {
                at: paused.waiting.step.clone(),
                error: why,
            },
            steps: Vec::new(),
            ms: 0,
        },
    };
    let part = Part {
        run: paused.run.clone(),
        event: &event,
        original,
        hops: paused.hops,
        ms_before: paused.ms,
        tokens,
    };
    finish(workflow, shared, topics, waits, part, record).await?;
    waits.done(token).await
}

/// How long a wait was, as a person says it.
fn shown(ms: i64) -> String {
    let secs = ms.max(0) / 1000;
    match secs {
        s if s % 86_400 == 0 && s > 0 => format!("{}d", s / 86_400),
        s if s % 3_600 == 0 && s > 0 => format!("{}h", s / 3_600),
        s if s % 60 == 0 && s > 0 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

/// The step's result from its answer: the answers, a call, a time - or its timeout.
pub(crate) fn answer_of(waiting: &Waiting, claim: &Claim) -> Answer {
    let waited_ms = u64::try_from(claim.at.saturating_sub(waiting.since)).unwrap_or(0);
    let at = iso(claim.at);
    let mut failed = None;
    let result = match claim.by.as_str() {
        "time" => json!({ "until": iso(waiting.until), "resumed_at": at, "timed_out": false }),
        "timeout" => {
            if waiting.on_timeout == OnTimeout::Fail {
                failed = Some(format!(
                    "no answer came within {}",
                    shown(waiting.until.saturating_sub(waiting.since))
                ));
            }
            match waiting.kind.as_str() {
                "approval" => {
                    json!({ "approved": false, "decision": "timed_out", "comment": "", "timed_out": true })
                }
                _ => json!({ "timed_out": true }),
            }
        }
        _ => {
            let mut answer = match &claim.answer {
                Json::Object(fields) => fields.clone(),
                _ => serde_json::Map::new(),
            };
            answer.insert("timed_out".to_owned(), json!(false));
            answer.insert("answered_at".to_owned(), json!(at));
            Json::Object(answer)
        }
    };
    Answer {
        step: waiting.step.clone(),
        result: Value::from_json(&result),
        waited_ms,
        failed,
    }
}

/// A run's answer as the test of a waiting step sees it (`now_ms` for a claim made now).
#[cfg(test)]
pub(crate) fn claim_now(by: &str, answer: Json) -> Claim {
    Claim {
        by: by.to_owned(),
        at: crate::runner::now_ms(),
        answer,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]

    use super::*;

    fn waiting(kind: &str, on_timeout: OnTimeout) -> Waiting {
        Waiting {
            step: "ask".to_owned(),
            kind: kind.to_owned(),
            token: "t".repeat(22),
            form: None,
            until: 3 * 86_400_000,
            on_timeout,
            since: 0,
        }
    }

    #[test]
    fn an_answer_becomes_the_steps_result_and_a_timeout_its_failure_or_its_flag() {
        let given = claim_now("answer", json!({ "size": "M" }));
        let a = answer_of(&waiting("form", OnTimeout::Fail), &given);
        assert!(a.failed.is_none());
        let shown_result: Json = serde_json::from_str(&a.result.to_json()).unwrap();
        assert_eq!(shown_result["size"], "M");
        assert_eq!(shown_result["timed_out"], false);
        let late = Claim {
            by: "timeout".to_owned(),
            at: 3 * 86_400_000,
            answer: Json::Null,
        };
        let failed = answer_of(&waiting("approval", OnTimeout::Fail), &late);
        assert_eq!(failed.failed.as_deref(), Some("no answer came within 3d"));
        assert_eq!(failed.waited_ms, 3 * 86_400_000);
        let goes_on = answer_of(&waiting("approval", OnTimeout::Continue), &late);
        assert!(goes_on.failed.is_none());
        let r: Json = serde_json::from_str(&goes_on.result.to_json()).unwrap();
        assert_eq!(
            (r["approved"].as_bool(), r["decision"].as_str()),
            (Some(false), Some("timed_out"))
        );
    }

    #[test]
    fn only_a_resume_event_with_a_token_takes_a_run_up() {
        let event = |details: Json| Event {
            value: Value::Null,
            details: Value::from_json(&details),
        };
        let token = "a".repeat(22);
        assert_eq!(
            resume_token(&event(json!({ "trigger": "resume", "token": token }))),
            Some(token)
        );
        assert_eq!(
            resume_token(&event(json!({ "trigger": "webhook", "token": "a".repeat(22) }))),
            None
        );
        assert_eq!(
            resume_token(&event(json!({ "trigger": "resume", "token": "../x" }))),
            None
        );
    }
}
