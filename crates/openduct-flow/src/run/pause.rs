// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A run's steps that wait (see [`crate::waits`]): a workflow's own run stops at one, a test run or a preview stands
//! in with a sample answer, and a run that cannot wait (one another workflow runs) fails there, saying why. A run
//! going on from where it waited takes its steps' results from before, up to the step that waited - each checked to
//! be the step the run had reached - and then runs on with the answer.

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use openduct_expr::{Env, Record, Value};
use serde_json::{Value as Json, json};

use super::{Flow, Run, StepRecord, kind_name, millis};
use crate::forms::MAX_TIMEOUT;
use crate::model::{Kind, Step, Workflow};
use crate::waits::{Pause, PauseOn, Waiting, pauses};

/// What a run's steps that wait do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// They stop the run, to go on when the answer comes: a workflow's own runs.
    Pause,
    /// They stand in with a sample answer: a test run, a preview.
    StandIn,
    /// They fail: a run that cannot wait.
    Refuse,
}

/// A step's record from before the wait, as it was kept.
#[derive(Debug, Clone)]
pub(crate) struct Kept {
    name: String,
    kind: String,
    attempts: u32,
    ms: u64,
    result: Value,
    error: Option<String>,
    logs: Vec<String>,
}

impl Kept {
    pub(crate) fn from_json(json: &Json) -> Option<Self> {
        Some(Self {
            name: json.get("name")?.as_str()?.to_owned(),
            kind: json.get("kind")?.as_str()?.to_owned(),
            attempts: json
                .get("attempts")
                .and_then(Json::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(1),
            ms: json.get("ms").and_then(Json::as_u64).unwrap_or(0),
            result: Value::from_json(json.get("result").unwrap_or(&Json::Null)),
            error: json.get("error").and_then(Json::as_str).map(str::to_owned),
            logs: json
                .get("logs")
                .and_then(Json::as_array)
                .map(|l| l.iter().filter_map(|x| x.as_str().map(str::to_owned)).collect())
                .unwrap_or_default(),
        })
    }
}

/// A step's record as a run that waits keeps it.
pub(crate) fn kept_json(s: &StepRecord) -> Json {
    let mut step = json!({
        "name": s.name, "kind": s.kind, "attempts": s.attempts, "ms": s.ms,
        "result": serde_json::from_str::<Json>(&s.result.to_json()).unwrap_or(Json::Null),
        "error": s.error,
    });
    if let (false, Some(fields)) = (s.logs.is_empty(), step.as_object_mut()) {
        fields.insert("logs".to_owned(), json!(s.logs));
    }
    step
}

/// The answer a run went on with: its step's result, how long it waited - or why the step fails instead.
#[derive(Debug, Clone)]
pub(crate) struct Answer {
    pub(crate) step: String,
    pub(crate) result: Value,
    pub(crate) waited_ms: u64,
    pub(crate) failed: Option<String>,
}

/// What a part of a run starts with.
#[derive(Debug)]
pub(crate) struct Segment {
    journal: VecDeque<Kept>,
    answer: Option<Answer>,
    /// `links.<step>`: each step that waits's address.
    pub(super) links: Record,
    tokens: BTreeMap<String, String>,
    mode: Mode,
}

impl Segment {
    /// A run that is not a workflow's own: a test run or a preview stands in for every wait (it has no place to keep
    /// one); a run with somewhere to keep waits that is not its workflow's own (one run by another) cannot wait.
    pub(crate) fn plain(workflow: &Workflow, effects: &dyn super::Effects) -> Self {
        let mode = if effects.waits().is_some() {
            Mode::Refuse
        } else {
            Mode::StandIn
        };
        let links = pauses(&workflow.steps)
            .into_iter()
            .map(|s| {
                (
                    s.name.clone(),
                    Value::text(format!(
                        "(the address of {}: made when the workflow runs)",
                        s.name
                    )),
                )
            })
            .collect();
        Self {
            journal: VecDeque::new(),
            answer: None,
            links,
            tokens: BTreeMap::new(),
            mode,
        }
    }

    /// A workflow's own run, or a later part of it: its steps' tokens and addresses, its steps from before, and the
    /// answer it goes on with.
    pub(crate) fn live(
        tokens: BTreeMap<String, String>,
        address: impl Fn(&str) -> String,
        journal: &[Json],
        answer: Option<Answer>,
    ) -> Result<Self, String> {
        let journal = journal
            .iter()
            .map(|j| {
                Kept::from_json(j)
                    .ok_or_else(|| "a run that waited kept a step that does not read".to_owned())
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            journal,
            answer,
            links: tokens
                .iter()
                .map(|(step, token)| (step.clone(), Value::text(address(token))))
                .collect(),
            tokens,
            mode: Mode::Pause,
        })
    }
}

/// A time as a step names it: RFC 3339 text (2026-12-01T09:00:00Z), a day (2026-12-01, at midnight UTC), or
/// milliseconds since 1970.
fn time_of(value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(n) => rust_decimal::prelude::ToPrimitive::to_i64(n)
            .ok_or_else(|| "until: milliseconds since 1970".to_owned()),
        Value::Text(t) => {
            let t = t.trim();
            if let Ok(at) = chrono::DateTime::parse_from_rfc3339(t) {
                return Ok(at.timestamp_millis());
            }
            if let Ok(at) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S") {
                return Ok(at.and_utc().timestamp_millis());
            }
            if let Ok(day) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
                return Ok(day
                    .and_hms_opt(0, 0, 0)
                    .map_or(0, |d| d.and_utc().timestamp_millis()));
            }
            Err(format!("until: {t:?} is not a time, as 2026-12-01T09:00:00Z"))
        }
        other => Err(format!("until: a time, not {}", other.describe())),
    }
}

fn ms(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

impl Run<'_> {
    /// A step of a run going on from where it waited: taken from before, as it was (`None`: it is run).
    pub(super) async fn replayed(
        &mut self,
        step: &Step,
        scope: &[(String, Value)],
    ) -> Result<Option<Flow>, (String, String)> {
        // A loop's own record comes after its steps': the loop runs, its steps taken from before.
        if matches!(step.kind, Kind::ForEach { .. }) {
            return Ok(None);
        }
        let Some(front) = self.segment.journal.front() else {
            return Ok(None);
        };
        let kind = kind_name(&step.kind);
        if front.name != step.name || front.kind != kind {
            return Err((
                step.name.clone(),
                format!(
                    "the workflow changed while this run waited: the run had reached {} ({}), not {} ({kind}) - run \
                     it again",
                    front.name, front.kind, step.name
                ),
            ));
        }
        let Some(kept) = self.segment.journal.pop_front() else {
            return Ok(None);
        };
        self.steps.insert(kept.name.clone(), kept.result.clone());
        let branch = |v: &Value, field: &str| match v {
            Value::Record(r) => r.get(field).map(crate::template::as_text).unwrap_or_default(),
            _ => String::new(),
        };
        let flow = match &step.kind {
            Kind::Where(_) if matches!(kept.result, Value::Bool(false)) => Flow::Skip(step.name.clone()),
            Kind::Stop { .. } => Flow::End,
            _ => Flow::Next,
        };
        let taken: Option<&[Step]> = match &step.kind {
            Kind::If { then, otherwise, .. } => Some(if branch(&kept.result, "branch") == "then" {
                then
            } else {
                otherwise
            }),
            Kind::Switch { cases, otherwise } => {
                let label = branch(&kept.result, "case");
                Some(
                    cases
                        .iter()
                        .find(|c| c.label == label)
                        .map_or(otherwise.as_slice(), |c| &c.steps),
                )
            }
            _ => None,
        };
        self.records.push(StepRecord {
            name: kept.name,
            kind,
            attempts: kept.attempts,
            ms: kept.ms,
            result: kept.result,
            error: kept.error,
            logs: kept.logs,
            test: None, // test runs: a run that waits is no test run
        });
        match taken {
            Some(steps) => self.steps_of(steps, scope).await.map(Some),
            None => Ok(Some(flow)),
        }
    }

    /// A loop's own record from before the wait: the loop makes it again, so it is not kept twice.
    pub(super) fn passed(&mut self, step: &Step) {
        if matches!(step.kind, Kind::ForEach { .. })
            && self.segment.journal.front().is_some_and(|k| k.name == step.name)
        {
            self.segment.journal.pop_front();
        }
    }

    /// A step that waits.
    pub(super) async fn pause(
        &mut self,
        step: &Step,
        pause: &Pause,
        scope: &[(String, Value)],
    ) -> Result<Flow, (String, String)> {
        let fail = |why: String| (step.name.clone(), why);
        // The answer this part of the run goes on with.
        if self.segment.answer.as_ref().is_some_and(|a| a.step == step.name)
            && let Some(answer) = self.segment.answer.take()
        {
            if let Some(error) = answer.failed {
                let result = Value::from_json(&json!({ "error": error, "timed_out": true }));
                self.record(step, 1, answer.waited_ms, result, Some(error.clone()));
                return if step.continue_on_error {
                    Ok(Flow::Next)
                } else {
                    Err(fail(error))
                };
            }
            self.record(step, 1, answer.waited_ms, answer.result, None);
            return Ok(Flow::Next);
        }
        let started = Instant::now();
        let names = self.names(scope);
        let env = Env {
            value: &self.event.value,
            message: &self.event.details,
            names: &names,
        };
        let form = match &pause.on {
            PauseOn::Form(spec) => Some(spec.render(&env).map_err(fail)?),
            _ => None,
        };
        let now = crate::runner::now_ms();
        let until = match &pause.on {
            PauseOn::Until(at) => time_of(&at.evaluate(&env).map_err(fail)?).map_err(fail)?,
            PauseOn::For(d) => now.saturating_add(ms(*d)),
            PauseOn::Form(_) | PauseOn::Webhook => now.saturating_add(ms(pause.timeout)),
        };
        if until > now.saturating_add(ms(MAX_TIMEOUT)) {
            return Err(fail("a run waits at most 90 days".to_owned()));
        }
        let is_time = matches!(pause.on, PauseOn::Until(_) | PauseOn::For(_));
        // A time already past: nothing to wait for.
        if is_time && until <= now && self.segment.mode != Mode::Refuse {
            let result = json!({ "until": crate::runner::iso(until), "timed_out": false });
            self.record(
                step,
                1,
                millis(started.elapsed()),
                Value::from_json(&result),
                None,
            );
            return Ok(Flow::Next);
        }
        match self.segment.mode {
            Mode::Refuse => Err(fail(
                "this run cannot wait for an answer: a workflow run by another one does not wait".to_owned(),
            )),
            Mode::StandIn => {
                let mut result = match (&pause.on, &form) {
                    (PauseOn::Form(_), Some(form)) => crate::forms::sample(form),
                    (PauseOn::Webhook, _) => {
                        json!({ "method": "POST", "body": {}, "query": {}, "headers": {} })
                    }
                    _ => json!({ "until": crate::runner::iso(until) }),
                };
                if let Some(fields) = result.as_object_mut() {
                    fields.insert("timed_out".to_owned(), json!(false));
                    fields.insert("preview".to_owned(), json!(true));
                }
                self.record(
                    step,
                    1,
                    millis(started.elapsed()),
                    Value::from_json(&result),
                    None,
                );
                Ok(Flow::Next)
            }
            Mode::Pause => {
                let token = self
                    .segment
                    .tokens
                    .get(&step.name)
                    .cloned()
                    .ok_or_else(|| fail("this step has no address to wait at".to_owned()))?;
                Ok(Flow::Pause(Box::new(Waiting {
                    step: step.name.clone(),
                    kind: pause.waits_for().to_owned(),
                    token,
                    form: form.map(|f| f.to_json()),
                    until,
                    on_timeout: pause.on_timeout,
                    since: now,
                })))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing, clippy::panic)]

    use super::*;
    use crate::run::{DryRun, Event, Outcome, run_with};

    const ASK: &str = "name: w\ntrigger: manual\nsteps:\n  - name: big\n    if:\n      when: \"value.total > 100\"\n      then: [{ name: hi, set: { x: \"1\" } }]\n      else: [{ name: lo, set: { x: \"2\" } }]\n  - name: ok\n    approval: { title: Fine? }\n  - name: after\n    set: { got: \"steps.ok.approved\", way: \"steps.big.branch\" }\n";

    fn kept(name: &str, kind: &str, result: Json) -> Json {
        json!({ "name": name, "kind": kind, "attempts": 1, "ms": 3, "result": result, "error": null })
    }

    fn answer() -> Option<Answer> {
        Some(Answer {
            step: "ok".to_owned(),
            result: Value::from_json(&json!({ "approved": true })),
            waited_ms: 9,
            failed: None,
        })
    }

    async fn go_on(journal: &[Json]) -> crate::run::RunRecord {
        let workflow = Workflow::parse(ASK).unwrap();
        let tokens: BTreeMap<String, String> = [("ok".to_owned(), "t".repeat(22))].into();
        let segment = Segment::live(tokens, |t| format!("http://x/waits/{t}"), journal, answer()).unwrap();
        // The value would take the other branch now: the run takes the one it took.
        let event = Event {
            value: Value::from_json(&json!({ "total": 500 })),
            details: Value::Null,
        };
        run_with(&workflow, &event, &DryRun::default(), segment, Default::default()).await
    }

    #[tokio::test]
    async fn a_run_going_on_takes_its_steps_from_before_and_its_answer() {
        let journal = [
            kept("big", "if", json!({ "branch": "else" })),
            kept("lo", "set", json!({ "x": 2 })),
        ];
        let record = go_on(&journal).await;
        assert_eq!(record.outcome, Outcome::Done);
        let names: Vec<&str> = record.steps.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["big", "lo", "ok", "after"]);
        assert_eq!(
            (record.steps[0].ms, record.steps[2].ms),
            (3, 9),
            "kept as they were; the wait's time"
        );
        assert_eq!(record.steps[3].result.to_json(), r#"{"got":true,"way":"else"}"#);
    }

    #[tokio::test]
    async fn a_run_whose_workflow_changed_before_its_wait_fails_saying_so() {
        let record = go_on(&[kept("gone", "set", json!({}))]).await;
        let Outcome::Failed { at, error } = record.outcome else {
            panic!("it goes on as if nothing changed")
        };
        assert_eq!(at, "big");
        assert!(
            error.contains("the workflow changed while this run waited"),
            "{error}"
        );
    }

    #[test]
    fn a_time_is_rfc_3339_a_day_or_milliseconds() {
        assert_eq!(time_of(&Value::text("1970-01-01T00:00:01Z")), Ok(1000));
        assert_eq!(time_of(&Value::text("1970-01-02")), Ok(86_400_000));
        assert_eq!(time_of(&Value::from_json(&json!(5))), Ok(5));
        assert!(time_of(&Value::text("soon")).is_err());
        assert!(time_of(&Value::Null).is_err());
    }

    #[test]
    fn a_kept_step_reads_back_as_it_was_kept() {
        let record = StepRecord {
            name: "a".to_owned(),
            kind: "set",
            attempts: 2,
            ms: 7,
            result: Value::from_json(&json!({ "x": 1 })),
            error: None,
            logs: vec!["hi".to_owned()],
            test: None, // test runs
        };
        let kept = Kept::from_json(&kept_json(&record)).unwrap();
        assert_eq!(
            (kept.name.as_str(), kept.kind.as_str(), kept.attempts, kept.ms),
            ("a", "set", 2, 7)
        );
        assert_eq!(kept.result.to_json(), record.result.to_json());
        assert_eq!(kept.logs, ["hi"]);
        assert!(Kept::from_json(&json!({ "kind": "set" })).is_none());
    }
}
