// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The AI steps built on the `ai` step ([`crate::ai`]): the same credentials, the same request and answer, the same
//! run record (`model`, `usage`, `transcript`, `metrics`) - each with a task of its own.
//!
//! - `classify`: a text into one of the given categories (or several), with a fallback, `other`; each category's own
//!   steps run, as a switch's cases do.
//! - `sentiment`: positive, neutral or negative (or labels of the step's own), with a score; each label's steps run.
//! - `extract`: named fields with types (or a JSON schema) pulled out of a text.
//! - `summarise`: a text, or a list of texts, to a length and a style; a long one in pieces, then the whole.
//! - `ai` with a `schema`: the answer must meet it.
//!
//! An answer that must be JSON is read leniently (a fenced block, words around it), checked against its schema
//! ([`crate::schema`]), and on a mismatch the model is asked once more, told what to put right; a second miss fails
//! the step, as a model that does not answer does - so its retries and `on_error` apply.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Instant;

use openduct_expr::{Env, Value};
use serde_json::{Value as Json, json};

use crate::apps::{Credential, put};
use crate::model::{Step, Tree};
use crate::run::Effects;
use crate::template::Template;

mod classify;
mod extract;
mod summarise;

pub(crate) use classify::{OTHER, plain_labels};
pub(crate) use extract::{FIELD_TYPES, Field, schema_of};

/// The longest text one model call is given, in characters.
pub(crate) const MAX_TEXT: usize = 200_000;
/// The most categories (or labels) a step sorts into.
pub(crate) const MAX_LABELS: usize = 50;
/// The most fields an `extract` names.
pub(crate) const MAX_FIELDS: usize = 100;

/// An AI step of one of the tasks below, on an `ai` credential.
#[derive(Debug, Clone)]
pub struct AiStep {
    pub(crate) task: Task,
    /// The credential, by name.
    pub(crate) credential: String,
    /// The model's settings, as the `ai` step has them: `model`, `temperature`, `top_p`, `max_tokens` (and, for an
    /// `ai` step with a schema, every one of its fields).
    pub(crate) fields: Vec<(String, Tree)>,
    /// What the task is done on (none for an `ai` step, which has its prompt).
    pub(crate) text: Option<Tree>,
    /// More for the model to go by, beside the task's own instructions.
    pub(crate) instructions: Option<Template>,
}

#[derive(Debug, Clone)]
pub(crate) enum Task {
    Classify {
        labels: Vec<Label>,
        several: bool,
        /// The fallback's steps, when the model may answer `other`; none when it must choose.
        other: Option<Vec<Step>>,
    },
    Sentiment {
        labels: Vec<Label>,
    },
    Extract {
        schema: Json,
        /// Made from `fields`: a field the answer leaves out is null, and one it adds is dropped.
        simple: bool,
    },
    Summarise {
        length: Option<String>,
        style: Option<String>,
        /// The most characters one piece of a long text holds.
        piece: usize,
    },
    /// An `ai` step whose answer must meet a schema.
    Ask {
        schema: Json,
    },
}

/// A category (or a sentiment's label): its name, what it means, and its steps.
#[derive(Debug, Clone)]
pub(crate) struct Label {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) steps: Vec<Step>,
}

/// Which steps run after an AI step: a label's (by its place), or the fallback's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    Label(usize),
    Other,
}

impl AiStep {
    /// The step's kind, as a run's record names it.
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        match self.task {
            Task::Classify { .. } => "classify",
            Task::Sentiment { .. } => "sentiment",
            Task::Extract { .. } => "extract",
            Task::Summarise { .. } => "summarise",
            Task::Ask { .. } => "ai",
        }
    }

    /// The lists of steps it holds: each label's, then the fallback's.
    pub fn routes(&self) -> impl Iterator<Item = &[Step]> {
        let (labels, other): (&[Label], Option<&Vec<Step>>) = match &self.task {
            Task::Classify { labels, other, .. } => (labels, other.as_ref()),
            Task::Sentiment { labels } => (labels, None),
            _ => (&[], None),
        };
        labels
            .iter()
            .map(|l| l.steps.as_slice())
            .chain(other.map(Vec::as_slice))
    }

    /// The steps a route runs.
    pub(crate) fn steps_of(&self, route: Route) -> &[Step] {
        match (&self.task, route) {
            (Task::Classify { labels, .. } | Task::Sentiment { labels }, Route::Label(i)) => {
                labels.get(i).map_or(&[], |l| l.steps.as_slice())
            }
            (
                Task::Classify {
                    other: Some(steps), ..
                },
                Route::Other,
            ) => steps,
            _ => &[],
        }
    }
}

/// What an AI step did: its result, and the routes to take after it, in order.
#[derive(Debug)]
pub(crate) struct Did {
    pub(crate) result: Value,
    pub(crate) routes: Vec<Route>,
}

/// Does the step's task: its model asked (more than once, for a repair or a long text), through the run's effects.
pub(crate) async fn run(step: &AiStep, effects: &dyn Effects, env: &Env<'_>) -> Result<Did, String> {
    let credential = effects.credential(&step.credential)?;
    if !matches!(credential.kind.as_str(), "ai" | "test") {
        return Err(format!(
            "{} is a {} credential: {} takes an ai one",
            step.credential,
            credential.kind,
            step.kind_name()
        ));
    }
    let mut fields = BTreeMap::new();
    for (name, tree) in &step.fields {
        fields.insert(
            name.clone(),
            crate::run::tree(tree, env).map_err(|e| format!("{name}: {e}"))?,
        );
    }
    let instructions = match &step.instructions {
        Some(t) => t.render(env).map_err(|e| format!("instructions: {e}"))?,
        None => String::new(),
    };
    let text = match &step.text {
        Some(tree) => crate::run::tree(tree, env).map_err(|e| format!("text: {e}"))?,
        None => Value::Null,
    };
    let call = Call {
        effects,
        credential: &credential,
        fields,
        instructions,
    };
    match &step.task {
        Task::Classify {
            labels,
            several,
            other,
        } => classify::sort(&call, &one_text(&text)?, labels, *several, other.is_some()).await,
        Task::Sentiment { labels } => classify::sentiment(&call, &one_text(&text)?, labels).await,
        Task::Extract { schema, simple } => extract::extract(&call, &one_text(&text)?, schema, *simple).await,
        Task::Summarise { length, style, piece } => {
            summarise::summarise(&call, &texts(&text), length.as_deref(), style.as_deref(), *piece).await
        }
        Task::Ask { schema } => extract::ask(&call, schema).await,
    }
}

/// A step's text, as one: a list or a record as its JSON.
fn one_text(value: &Value) -> Result<String, String> {
    let text = crate::template::as_text(value);
    if text.trim().is_empty() {
        return Err("text: the text to work on is empty".to_owned());
    }
    let length = text.chars().count();
    if length > MAX_TEXT {
        return Err(format!(
            "text: {length} characters, more than the {MAX_TEXT} one model call is given (summarise it first)"
        ));
    }
    Ok(text)
}

/// A step's texts: a list's items each one, anything else one text.
fn texts(value: &Value) -> Vec<String> {
    match value {
        Value::List(items) => items
            .iter()
            .map(crate::template::as_text)
            .filter(|t| !t.trim().is_empty())
            .collect(),
        other => {
            let text = crate::template::as_text(other);
            if text.trim().is_empty() {
                Vec::new()
            } else {
                vec![text]
            }
        }
    }
}

/// What every model call of one step shares.
pub(crate) struct Call<'a> {
    pub(crate) effects: &'a dyn Effects,
    pub(crate) credential: &'a Credential,
    /// The step's own model settings, worked out.
    pub(crate) fields: BTreeMap<String, Value>,
    /// The step's own instructions, worked out ("" when it has none).
    pub(crate) instructions: String,
}

/// A model's answer: the `ai` step's result for it, or - in a test run, which asks no model - the request shown.
#[derive(Debug)]
pub(crate) enum Asked {
    Answered(Json),
    Shown(Json),
}

fn millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn as_json(value: &Value) -> Json {
    serde_json::from_str(&value.to_json()).unwrap_or(Json::Null)
}

impl Call<'_> {
    /// The fields of one call: the step's settings, a system message (the task's, then the step's instructions), and
    /// the user's message.
    pub(crate) fn fields(&self, system: &str, prompt: &str, json: bool) -> BTreeMap<String, Value> {
        let mut fields = self.fields.clone();
        let mut said = system.to_owned();
        if !self.instructions.trim().is_empty() {
            said.push_str("\n\n");
            said.push_str(self.instructions.trim());
        }
        fields.insert("system".to_owned(), Value::text(said.as_str()));
        fields.insert("prompt".to_owned(), Value::text(prompt));
        if json {
            fields.insert("json".to_owned(), Value::Bool(true));
        }
        fields
    }

    /// One model call, as the `ai` step makes it: a sampling setting the model refuses is left out and it is asked
    /// again, once.
    pub(crate) async fn ask(&self, fields: &mut BTreeMap<String, Value>) -> Result<Asked, String> {
        let asked = Instant::now();
        let mut answer = self
            .effects
            .http(crate::ai::chat_request(self.credential, fields)?)
            .await?;
        let ignored = crate::ai::refused_settings(&answer);
        if !ignored.is_empty() {
            for setting in &ignored {
                fields.remove(*setting);
            }
            answer = self
                .effects
                .http(crate::ai::chat_request(self.credential, fields)?)
                .await?;
        }
        let ms = millis(asked);
        let shown = as_json(&answer);
        if shown.get("test").is_some() {
            return Ok(Asked::Shown(shown));
        }
        // The text as it came: JSON in it is read here, leniently, and checked against its schema.
        let mut plain = fields.clone();
        plain.remove("json");
        plain.remove("response_format");
        let mut result = as_json(&crate::apps::answer("ai", self.credential, &plain, answer, ms)?);
        if !ignored.is_empty()
            && let Some(metrics) = result.get_mut("metrics")
        {
            put(metrics, "ignored_settings", json!(ignored));
        }
        Ok(Asked::Answered(result))
    }

    /// A call whose answer is JSON that meets `schema`: read leniently, `tidy`'d (names put right, say), checked;
    /// on a mismatch the model is told what is wrong and asked once more.
    pub(crate) async fn ask_json(
        &self,
        mut fields: BTreeMap<String, Value>,
        schema: &Json,
        tidy: &(dyn Fn(&mut Json) + Sync),
    ) -> Result<Answered, String> {
        let first = match self.ask(&mut fields).await? {
            Asked::Answered(result) => result,
            Asked::Shown(shown) => return Ok(Answered::Shown(shown)),
        };
        let problems = match read(&first, schema, tidy) {
            Ok(json) => return Ok(Answered::Json { json, result: first }),
            Err(problems) => problems,
        };
        // Once more, the conversation so far and what to put right in it.
        let said = text_of(&first);
        let mut turns = match fields.get("messages").map(as_json) {
            Some(Json::Array(list)) => list,
            _ => Vec::new(),
        };
        if let Some(prompt) = fields.remove("prompt") {
            turns.push(json!({ "role": "user", "content": crate::template::as_text(&prompt) }));
        }
        turns.push(json!({ "role": "assistant", "content": said }));
        turns.push(json!({ "role": "user", "content": repair(&problems) }));
        fields.insert("messages".to_owned(), Value::from_json(&Json::Array(turns)));
        let second = match self.ask(&mut fields).await? {
            Asked::Answered(result) => result,
            Asked::Shown(shown) => return Ok(Answered::Shown(shown)),
        };
        let result = joined(&first, second, 1);
        match read(&result, schema, tidy) {
            Ok(json) => Ok(Answered::Json { json, result }),
            Err(problems) => Err(missed(&result, &problems)),
        }
    }
}

/// A JSON answer: what it says, and the call's result (both calls', when it was asked again).
#[derive(Debug)]
pub(crate) enum Answered {
    Json { json: Json, result: Json },
    Shown(Json),
}

/// What the model said.
pub(crate) fn text_of(result: &Json) -> String {
    result
        .get("text")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// An answer's JSON, tidied and checked; or what is wrong with it.
fn read(result: &Json, schema: &Json, tidy: &(dyn Fn(&mut Json) + Sync)) -> Result<Json, Vec<String>> {
    let Some(mut json) = json_in(&text_of(result)) else {
        return Err(vec!["the answer is not JSON".to_owned()]);
    };
    tidy(&mut json);
    crate::schema::validate(schema, &json)?;
    Ok(json)
}

/// What the model is told when its answer misses its schema.
fn repair(problems: &[String]) -> String {
    format!(
        "That answer does not meet the JSON schema: {}. Answer again with the corrected JSON only.",
        problems.iter().take(10).cloned().collect::<Vec<_>>().join("; ")
    )
}

/// Why a step fails when its model's answer missed its schema twice.
fn missed(result: &Json, problems: &[String]) -> String {
    let cut = if result.get("finish_reason").and_then(Json::as_str) == Some("length") {
        " (the answer was cut off at max_tokens: allow more)"
    } else {
        ""
    };
    format!(
        "the model's answer does not meet the schema, even asked again{cut}: {}",
        problems.iter().take(5).cloned().collect::<Vec<_>>().join("; ")
    )
}

/// The JSON in a model's answer: the answer itself, the inside of a fenced block, or the first value in its text.
pub(crate) fn json_in(text: &str) -> Option<Json> {
    let trimmed = text.trim();
    if let Ok(json) = serde_json::from_str(trimmed) {
        return Some(json);
    }
    let fenced = trimmed
        .split_once("```")
        .map(|(_, rest)| rest.trim_start_matches("json").trim_start_matches("JSON"))
        .and_then(|rest| rest.split_once("```"))
        .map(|(inside, _)| inside.trim());
    if let Some(inside) = fenced
        && let Ok(json) = serde_json::from_str(inside)
    {
        return Some(json);
    }
    let start = trimmed.find(['{', '['])?;
    let close = if trimmed.get(start..start + 1) == Some("{") {
        '}'
    } else {
        ']'
    };
    let end = trimmed.rfind(close)?;
    serde_json::from_str(trimmed.get(start..=end)?).ok()
}

/// Two calls' results as one: the later's answer and transcript, both calls' tokens and time.
pub(crate) fn joined(first: &Json, mut later: Json, repairs: u64) -> Json {
    let number = |r: &Json, path: &str| r.pointer(path).and_then(Json::as_u64);
    let sum = |path: &str| match (number(first, path), number(&later, path)) {
        (Some(a), Some(b)) => json!(a + b),
        (a, b) => json!(a.or(b)),
    };
    let usage = json!({
        "prompt_tokens": sum("/usage/prompt_tokens"),
        "completion_tokens": sum("/usage/completion_tokens"),
        "total_tokens": sum("/usage/total_tokens"),
    });
    let latency = sum("/metrics/latency_ms");
    put(&mut later, "usage", usage);
    if let Some(metrics) = later.get_mut("metrics") {
        put(metrics, "latency_ms", latency);
        put(metrics, "repairs", json!(repairs));
    }
    later
}

/// The result of an AI step: `fields` (its own answer), then the model's: `model`, `finish_reason`, `usage`,
/// `transcript`, `metrics`.
pub(crate) fn result(fields: Json, call: &Json) -> Value {
    let mut out = fields;
    for name in ["model", "finish_reason", "usage", "transcript", "metrics"] {
        if let Some(v) = call.get(name) {
            put(&mut out, name, v.clone());
        }
    }
    Value::from_json(&out)
}

/// A test run's result: what would have been sent, and in place of the answer, `fields` saying so.
pub(crate) fn shown(fields: Json, shown: Json) -> Value {
    let mut out = shown;
    if let (Some(out), Json::Object(fields)) = (out.as_object_mut(), fields) {
        out.extend(fields);
    }
    Value::from_json(&out)
}

type Job<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runs `jobs`, at most `at_once` of them at a time; their results in their order.
pub(crate) async fn at_most<'a, T: Send>(jobs: Vec<Job<'a, T>>, at_once: usize) -> Vec<T> {
    let total = jobs.len();
    let mut waiting = jobs.into_iter().enumerate();
    let mut running: Vec<(usize, Job<'a, T>)> = Vec::new();
    let mut done: Vec<Option<T>> = (0..total).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        while running.len() < at_once.max(1) {
            match waiting.next() {
                Some(job) => running.push(job),
                None => break,
            }
        }
        let mut i = 0;
        while let Some((index, job)) = running.get_mut(i) {
            if let Poll::Ready(value) = job.as_mut().poll(cx) {
                if let Some(slot) = done.get_mut(*index) {
                    *slot = Some(value);
                }
                drop(running.swap_remove(i));
                if let Some(job) = waiting.next() {
                    running.push(job);
                }
            } else {
                i += 1;
            }
        }
        if running.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    done.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_read_from_an_answer_however_it_is_wrapped() {
        assert_eq!(json_in(r#"{"a": 1}"#), Some(json!({ "a": 1 })));
        assert_eq!(json_in("```json\n{\"a\": 1}\n```"), Some(json!({ "a": 1 })));
        assert_eq!(
            json_in("Here it is:\n```\n[1, 2]\n```\nAnything else?"),
            Some(json!([1, 2]))
        );
        assert_eq!(
            json_in("Sure! {\"category\": \"bug\"} - hope that helps"),
            Some(json!({ "category": "bug" }))
        );
        assert_eq!(json_in("no JSON here"), None);
        assert_eq!(json_in("{ broken"), None);
    }

    #[test]
    fn two_calls_count_as_one_step() {
        let first = json!({ "usage": { "prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14 }, "metrics": { "latency_ms": 30 } });
        let later = json!({ "text": "ok", "usage": { "prompt_tokens": 20, "completion_tokens": 5, "total_tokens": 25 }, "metrics": { "latency_ms": 40, "api": "chat_completions" } });
        let both = joined(&first, later, 1);
        assert_eq!(
            both["usage"],
            json!({ "prompt_tokens": 30, "completion_tokens": 9, "total_tokens": 39 })
        );
        assert_eq!(
            both["metrics"],
            json!({ "latency_ms": 70, "api": "chat_completions", "repairs": 1 })
        );
        assert_eq!(both["text"], "ok");
    }

    #[test]
    fn jobs_run_a_few_at_a_time_and_answer_in_their_order() {
        let at_once = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let most = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let jobs: Vec<Job<'_, usize>> = (0..10_usize)
            .map(|i| {
                let (at_once, most) = (at_once.clone(), most.clone());
                Box::pin(async move {
                    let now = at_once.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    most.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(
                        u64::try_from(10 - i).unwrap_or(0),
                    ))
                    .await;
                    at_once.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    i * 2
                }) as Job<'_, usize>
            })
            .collect();
        let got = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(at_most(jobs, 3));
        assert_eq!(got, (0..10).map(|i| i * 2).collect::<Vec<_>>());
        assert_eq!(most.load(std::sync::atomic::Ordering::SeqCst), 3);
    }
}
