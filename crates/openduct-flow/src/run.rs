// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One run of a workflow, for one event: its steps in order, each step's result kept as `steps.<name>` for the
//! steps after it, retried as its settings say, and every step's result and time recorded for the run's history.
//!
//! Steps that reach outside - an HTTP call, a message to a topic - go through [`Effects`]; a test run (the studio's
//! Test button, `openduct workflow test`) uses effects that only say what they would have sent.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openduct_expr::{Env, Record, Value};
use serde_json::{Value as Json, json};

use crate::model::{Body, Http, Kind, Step, Tree, Workflow};

/// The most items a `for_each` goes through.
pub const MAX_ITEMS: usize = 10_000;
/// The most steps one run takes, loops counted.
pub(crate) const MAX_RUN_STEPS: usize = 100_000;
/// The largest result a step keeps, in bytes as JSON; a larger one is an error.
pub const MAX_RESULT: usize = 4 * 1024 * 1024;
/// How deep workflows run one inside another.
const MAX_DEPTH: u64 = 4;

/// How deep inside other workflows this run is.
fn depth_of(details: &Value) -> u64 {
    match details {
        Value::Record(r) => r
            .get("depth")
            .and_then(|d| crate::template::as_text(d).parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// How long a run may take.
pub(crate) const MAX_RUN: Duration = Duration::from_secs(3 * 3600);

/// What started a run: the event's value, and its details (`trigger`: what kind, when, from where).
#[derive(Debug, Clone)]
pub struct Event {
    pub value: Value,
    pub details: Value,
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Done,
    /// A `where` was false: the run had nothing to do.
    Skipped {
        at: String,
    },
    /// A step failed at its last try: the event is set aside, to run again once the cause is fixed.
    Failed {
        at: String,
        error: String,
    },
    /// The run stopped at a step that waits - a form, an approval, a call, a time - to go on when it comes.
    // forms-and-resume
    Waiting {
        at: String,
        waiting: Box<crate::waits::Waiting>,
    },
}

/// What one step did.
#[derive(Debug, Clone)]
pub struct StepRecord {
    pub name: String,
    pub kind: &'static str,
    pub attempts: u32,
    pub ms: u64,
    pub result: Value,
    pub error: Option<String>,
    /// What a script step logged (`console.log`, `print`).
    pub logs: Vec<String>,
    /// What a test run says beside the result: a sample answer and the request not sent (`sample`, `request`), or a
    /// table not made yet (`note`). // test runs
    pub test: Option<Value>,
}

/// A run, as its history keeps it.
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub outcome: Outcome,
    pub steps: Vec<StepRecord>,
    pub ms: u64,
}

impl RunRecord {
    pub fn to_json(&self) -> Json {
        let (status, at, error) = match &self.outcome {
            Outcome::Done => ("done", None, None),
            Outcome::Skipped { at } => ("skipped", Some(at.clone()), None),
            Outcome::Failed { at, error } => ("failed", Some(at.clone()), Some(error.clone())),
            // forms-and-resume
            Outcome::Waiting { at, .. } => ("waiting", Some(at.clone()), None),
        };
        let steps: Vec<Json> = self
            .steps
            .iter()
            .map(|s| {
                let mut step = json!({
                    "name": s.name, "kind": s.kind, "attempts": s.attempts, "ms": s.ms,
                    "result": serde_json::from_str::<Json>(&s.result.to_json()).unwrap_or(Json::Null),
                    "error": s.error,
                });
                if let (false, Some(fields)) = (s.logs.is_empty(), step.as_object_mut()) {
                    fields.insert("logs".to_owned(), json!(s.logs));
                }
                // test runs: what is said beside the result.
                let said = s
                    .test
                    .as_ref()
                    .and_then(|t| serde_json::from_str::<Json>(&t.to_json()).ok());
                if let (Some(Json::Object(said)), Some(fields)) = (said, step.as_object_mut()) {
                    fields.extend(said);
                }
                step
            })
            .collect();
        let mut out = json!({ "status": status, "at": at, "error": error, "ms": self.ms, "steps": steps });
        // forms-and-resume: what a run that waits waits for.
        if let (Outcome::Waiting { waiting, .. }, Some(fields)) = (&self.outcome, out.as_object_mut()) {
            fields.insert(
                "waiting".to_owned(),
                json!({ "step": waiting.step, "kind": waiting.kind, "until": waiting.until, "since": waiting.since }),
            );
        }
        out
    }
}

/// Where a run went: on to the next step, out at a `where`, or to its end.
type Going<'a> = Pin<Box<dyn Future<Output = Result<Flow, (String, String)>> + Send + 'a>>;

mod agent; // G2 AI agents
mod ai_steps; // G7 more AI steps
mod defined;
mod more;
mod outside;
// forms-and-resume
pub(crate) mod pause;
mod samples; // test runs
mod tables; // data tables

pub(crate) use samples::http_sample; // test runs

pub use outside::{DryRun, Effects, EmailRequest, HttpRequest, ScriptRequest, SqlRequest};
pub(crate) use outside::{WRITES, sql_shown};

struct Run<'a> {
    event: &'a Event,
    effects: &'a dyn Effects,
    /// The workflow's id: whose state a run keeps, and what an agent's memory is kept under. // data tables, G2 AI agents
    workflow: &'a str,
    steps: Record,
    records: Vec<StepRecord>,
    taken: usize,
    deadline: Instant,
    // forms-and-resume: what its steps that wait do, and where a run going on from a wait takes up.
    segment: pause::Segment,
    /// The files the run holds, shared with the workflows it runs. // G6 files
    files: Arc<crate::files::RunFiles>,
    /// The pieces its `store: memory` steps keep, for this run only. // vector stores
    vectors: crate::retrieval::RunVectors,
}

enum Flow {
    Next,
    Skip(String),
    End,
    // forms-and-resume
    Pause(Box<crate::waits::Waiting>),
}

/// Runs `workflow` for `event`.
pub async fn run(workflow: &Workflow, event: &Event, effects: &dyn Effects) -> RunRecord {
    // forms-and-resume, G6 files
    run_with(
        workflow,
        event,
        effects,
        pause::Segment::plain(workflow, effects),
        Arc::default(),
    )
    .await
}

/// Runs `workflow` for `event`, its waits as `segment` says, holding its files in `files`. // forms-and-resume, G6 files
pub(crate) async fn run_with(
    workflow: &Workflow,
    event: &Event,
    effects: &dyn Effects,
    segment: pause::Segment,
    files: Arc<crate::files::RunFiles>,
) -> RunRecord {
    let started = Instant::now();
    let mut run = Run {
        event,
        effects,
        workflow: &workflow.id, // data tables, G2 AI agents
        steps: Record::new(),
        records: Vec::new(),
        taken: 0,
        deadline: started + MAX_RUN,
        segment,
        files,
        vectors: crate::retrieval::RunVectors::default(), // vector stores
    };
    let outcome = match run.steps_of(&workflow.steps, &[]).await {
        Ok(Flow::Skip(at)) => Outcome::Skipped { at },
        Ok(Flow::Next | Flow::End) => Outcome::Done,
        // forms-and-resume
        Ok(Flow::Pause(waiting)) => Outcome::Waiting {
            at: waiting.step.clone(),
            waiting,
        },
        Err((at, error)) => Outcome::Failed { at, error },
    };
    RunRecord {
        outcome,
        steps: run.records,
        ms: millis(started.elapsed()),
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::Set(_) => "set",
        Kind::Where(_) => "where",
        Kind::If { .. } => "if",
        Kind::ForEach { .. } => "for_each",
        Kind::Http(_) => "http",
        Kind::Lookup { .. } => "lookup",
        Kind::Sql { .. } => "postgres",
        Kind::Email(_) => "email",
        Kind::Switch { .. } => "switch",
        Kind::Merge { .. } => "merge",
        Kind::List(_) => "list",
        Kind::Group(_) => "group",
        Kind::Respond { .. } => "respond",
        Kind::RunWorkflow { .. } => "run_workflow",
        Kind::DateTime(_) => "datetime",
        Kind::Script { language, .. } => language,
        Kind::App(app) => app.app,
        Kind::Defined(d) => d.app.id.as_str(),
        Kind::Topic { .. } => "topic",
        Kind::Wait(_) => "wait",
        Kind::Stop { .. } => "stop",
        Kind::Ai(ai) => ai.kind_name(), // G7 more AI steps
        Kind::Agent(_) => "agent",      // G2 AI agents
        Kind::Mcp(_) => "mcp",          // MCP
        Kind::Retrieval(r) => r.kind(), // retrieval
        // forms-and-resume
        Kind::Pause(p) => p.kind_name(),
        Kind::Table(_) => "table",              // data tables
        Kind::State(_) => "state",              // data tables
        Kind::Extract(_) => "extract_file",     // G6 files
        Kind::Convert(_) => "convert",          // G6 files
        Kind::FileDetails(_) => "file_details", // G6 files
        Kind::Database(q) => q.db.name(),       // sql databases
        Kind::Storage(s) => s.kind_name(),      // storage
        Kind::Web(_) => "web",                  // web
        Kind::Mailbox(_) => "mailbox",          // messaging
    }
}

impl Run<'_> {
    /// The names an expression reads: `steps`, `trigger`, and a loop's `item` and `index`.
    fn names(&self, scope: &[(String, Value)]) -> Vec<(String, Value)> {
        let mut names = vec![
            ("steps".to_owned(), Value::record(self.steps.clone())),
            ("trigger".to_owned(), self.event.details.clone()),
            // forms-and-resume: the addresses of the steps that wait.
            ("links".to_owned(), Value::record(self.segment.links.clone())),
        ];
        names.extend(scope.iter().cloned());
        names
    }

    fn steps_of<'s>(&'s mut self, steps: &'s [Step], scope: &'s [(String, Value)]) -> Going<'s> {
        Box::pin(async move {
            for step in steps {
                match self.step(step, scope).await? {
                    Flow::Next => {}
                    other => return Ok(other),
                }
            }
            Ok(Flow::Next)
        })
    }

    async fn step(&mut self, step: &Step, scope: &[(String, Value)]) -> Result<Flow, (String, String)> {
        self.taken += 1;
        let fail = |why: String| (step.name.clone(), why);
        if self.taken > MAX_RUN_STEPS {
            return Err(fail(format!("a run takes at most {MAX_RUN_STEPS} steps")));
        }
        if Instant::now() > self.deadline {
            return Err(fail("the run took longer than 3 hours".to_owned()));
        }
        // forms-and-resume: a run going on from where it waited takes its steps from before as they were.
        if let Some(flow) = self.replayed(step, scope).await? {
            return Ok(flow);
        }
        // Branches and loops run their own steps, which record themselves.
        match &step.kind {
            // forms-and-resume
            Kind::Pause(pause) => return self.pause(step, pause, scope).await,
            Kind::If {
                when,
                then,
                otherwise,
            } => {
                let names = self.names(scope);
                let env = Env {
                    value: &self.event.value,
                    message: &self.event.details,
                    names: &names,
                };
                let taken = matches!(when.evaluate(&env).map_err(fail)?, Value::Bool(true));
                self.record(
                    step,
                    1,
                    0,
                    Value::from_json(&json!({ "branch": if taken { "then" } else { "else" } })),
                    None,
                );
                return self.steps_of(if taken { then } else { otherwise }, scope).await;
            }
            Kind::ForEach { items, steps } => return self.for_each(step, items, steps, scope).await,
            Kind::Ai(ai) => return self.ai_step(step, ai, scope).await, // G7 more AI steps
            Kind::Switch { cases, otherwise } => {
                let names = self.names(scope);
                let env = Env {
                    value: &self.event.value,
                    message: &self.event.details,
                    names: &names,
                };
                let mut chosen = None;
                for (i, case) in cases.iter().enumerate() {
                    if matches!(
                        case.when
                            .evaluate(&env)
                            .map_err(|e| fail(format!("{}: {e}", case.label)))?,
                        Value::Bool(true)
                    ) {
                        chosen = Some(i);
                        break;
                    }
                }
                let (label, steps) = match chosen.and_then(|i| cases.get(i)) {
                    Some(case) => (case.label.clone(), &case.steps),
                    None => ("default".to_owned(), otherwise),
                };
                self.record(step, 1, 0, Value::from_json(&json!({ "case": label })), None);
                return self.steps_of(steps, scope).await;
            }
            _ => {}
        }
        let mut wait = step.retry_wait;
        let started = Instant::now();
        for attempt in 1..=step.attempts {
            match self.once(step, scope).await {
                Ok(Done::Value(result)) => {
                    if result.size() > MAX_RESULT {
                        return Err(fail(format!("a result of more than {MAX_RESULT} bytes")));
                    }
                    self.record(step, attempt, millis(started.elapsed()), result, None);
                    return Ok(Flow::Next);
                }
                Ok(Done::Logged(result, logs)) => {
                    if result.size() > MAX_RESULT {
                        return Err(fail(format!("a result of more than {MAX_RESULT} bytes")));
                    }
                    self.record(step, attempt, millis(started.elapsed()), result, None);
                    if let Some(last) = self.records.last_mut() {
                        last.logs = logs;
                    }
                    return Ok(Flow::Next);
                }
                // test runs: a sample answer, or a table not made yet, said beside the result.
                Ok(Done::Noted(result, said)) => {
                    self.record(step, attempt, millis(started.elapsed()), result, None);
                    if let Some(last) = self.records.last_mut() {
                        last.test = Some(said);
                    }
                    return Ok(Flow::Next);
                }
                Ok(Done::Skip) => {
                    self.record(step, attempt, millis(started.elapsed()), Value::Bool(false), None);
                    return Ok(Flow::Skip(step.name.clone()));
                }
                Ok(Done::End(None)) => {
                    self.record(step, attempt, millis(started.elapsed()), Value::Null, None);
                    return Ok(Flow::End);
                }
                Ok(Done::End(Some(error))) => {
                    self.record(
                        step,
                        attempt,
                        millis(started.elapsed()),
                        Value::Null,
                        Some(error.clone()),
                    );
                    return Err(fail(error));
                }
                Err(error) if attempt < step.attempts => {
                    tracing::debug!(step = step.name, attempt, error, "a step failed: trying again");
                    tokio::time::sleep(wait).await;
                    wait = wait.saturating_mul(2).min(Duration::from_secs(300));
                }
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
            }
        }
        Ok(Flow::Next)
    }

    fn record(&mut self, step: &Step, attempts: u32, ms: u64, result: Value, error: Option<String>) {
        // forms-and-resume
        self.passed(step);
        self.steps.insert(step.name.clone(), result.clone());
        self.records.push(StepRecord {
            name: step.name.clone(),
            kind: kind_name(&step.kind),
            attempts,
            ms,
            result,
            error,
            logs: Vec::new(),
            test: None, // test runs
        });
    }

    async fn for_each(
        &mut self,
        step: &Step,
        items: &openduct_expr::Compiled,
        steps: &[Step],
        scope: &[(String, Value)],
    ) -> Result<Flow, (String, String)> {
        let fail = |why: String| (step.name.clone(), why);
        let names = self.names(scope);
        let env = Env {
            value: &self.event.value,
            message: &self.event.details,
            names: &names,
        };
        let list = match items.evaluate(&env).map_err(fail)? {
            Value::List(items) => items,
            Value::Null => Arc::new(Vec::new()),
            other => {
                return Err(fail(format!(
                    "for_each.items is {}, not a list",
                    other.describe()
                )));
            }
        };
        if list.len() > MAX_ITEMS {
            return Err(fail(format!(
                "a list of {} items: at most {MAX_ITEMS}",
                list.len()
            )));
        }
        let started = Instant::now();
        let mut results = Vec::with_capacity(list.len());
        for (index, item) in list.iter().enumerate() {
            let mut inner = scope.to_vec();
            inner.push(("item".to_owned(), item.clone()));
            inner.push(("index".to_owned(), Value::from_json(&json!(index))));
            match self.steps_of(steps, &inner).await? {
                Flow::End => break,
                Flow::Next | Flow::Skip(_) => {}
                // forms-and-resume
                Flow::Pause(waiting) => return Ok(Flow::Pause(waiting)),
            }
            let of_item: Record = steps
                .iter()
                .filter_map(|s| self.steps.get(&s.name).map(|v| (s.name.clone(), v.clone())))
                .collect();
            results.push(Value::record(of_item));
        }
        self.record(step, 1, millis(started.elapsed()), Value::list(results), None);
        Ok(Flow::Next)
    }

    async fn once(&self, step: &Step, scope: &[(String, Value)]) -> Result<Done, String> {
        let names = self.names(scope);
        let env = Env {
            value: &self.event.value,
            message: &self.event.details,
            names: &names,
        };
        Ok(match &step.kind {
            Kind::Set(fields) => {
                let mut out = Record::new();
                for (name, expression) in fields {
                    out.insert(
                        name.clone(),
                        expression.evaluate(&env).map_err(|e| format!("{name}: {e}"))?,
                    );
                }
                Done::Value(Value::record(out))
            }
            Kind::Where(condition) => match condition.evaluate(&env)? {
                Value::Bool(true) => Done::Value(Value::Bool(true)),
                _ => Done::Skip,
            },
            Kind::Lookup { file, matching, all } => {
                let rows = self.effects.file(file).await?;
                let mut wanted = Vec::new();
                for (column, template) in matching {
                    wanted.push((
                        column.as_str(),
                        template.render(&env).map_err(|e| format!("{column}: {e}"))?,
                    ));
                }
                let Value::List(rows) = rows else {
                    return Err(format!("{file} is not a list of rows"));
                };
                let found: Vec<Value> = rows
                    .iter()
                    .filter(|row| {
                        let Value::Record(fields) = row else { return false };
                        wanted.iter().all(|(column, text)| {
                            fields
                                .get(*column)
                                .is_some_and(|v| crate::template::as_text(v) == *text)
                        })
                    })
                    .take(if *all { crate::run::MAX_ITEMS } else { 1 })
                    .cloned()
                    .collect();
                Done::Value(if *all {
                    Value::list(found)
                } else {
                    found.into_iter().next().unwrap_or(Value::Null)
                })
            }
            Kind::Sql {
                credential: name,
                sql,
                params,
            } => {
                let credential = self.effects.credential(name)?;
                if !matches!(credential.kind.as_str(), "postgres" | "test") {
                    return Err(format!(
                        "{name} is a {} credential: a postgres step takes a postgres one",
                        credential.kind
                    ));
                }
                let mut values = Vec::with_capacity(params.len());
                for (i, param) in params.iter().enumerate() {
                    values.push(
                        match param.evaluate(&env).map_err(|e| format!("params[{i}]: {e}"))? {
                            Value::Null => None,
                            other => Some(crate::template::as_text(&other)),
                        },
                    );
                }
                let request = SqlRequest {
                    credential,
                    sql: sql.clone(),
                    params: values,
                    read_only: false,
                };
                Done::Value(self.effects.sql(request).await?)
            }
            Kind::List(ops) => Done::Value(more::list(ops, &env)?),
            Kind::Group(ops) => Done::Value(more::group(ops, &env)?),
            Kind::Merge { of, mode, key } => Done::Value(more::merge(of, *mode, key.as_deref(), &env)?),
            Kind::DateTime(ops) => Done::Value(more::datetime(ops, &env)?),
            Kind::Respond {
                status,
                headers,
                body,
            } => {
                let mut shown = serde_json::Map::new();
                for (name, value) in headers {
                    shown.insert(
                        name.clone(),
                        Json::String(value.render(&env).map_err(|e| format!("{name}: {e}"))?),
                    );
                }
                let body = serde_json::from_str::<Json>(&tree(body, &env)?.to_json()).unwrap_or(Json::Null);
                Done::Value(Value::from_json(
                    &json!({ "status": status, "headers": shown, "body": body }),
                ))
            }
            Kind::RunWorkflow { workflow, input } => {
                let depth = depth_of(&self.event.details);
                if depth >= MAX_DEPTH {
                    return Err(format!(
                        "workflows run one inside another at most {MAX_DEPTH} deep"
                    ));
                }
                let definition = self.effects.workflow(workflow).await?;
                let json: Json = serde_json::from_str(&definition.to_json()).map_err(|e| e.to_string())?;
                let inner =
                    Workflow::from_json(&json, Some(workflow)).map_err(|e| format!("{workflow}: {e}"))?;
                let value = match input {
                    Some(expr) => expr.evaluate(&env).map_err(|e| format!("input: {e}"))?,
                    None => self.event.value.clone(),
                };
                let event = Event {
                    value,
                    details: Value::from_json(&json!({ "trigger": "workflow", "depth": depth + 1 })),
                };
                let record = Box::pin(run_with(
                    &inner,
                    &event,
                    self.effects,
                    pause::Segment::plain(&inner, self.effects),
                    Arc::clone(&self.files),
                ))
                .await; // G6 files
                if let Outcome::Failed { at, error } = &record.outcome {
                    return Err(format!("{} failed at {at}: {error}", inner.name));
                }
                let mut steps = Record::new();
                for s in &record.steps {
                    steps.insert(s.name.clone(), s.result.clone());
                }
                let mut out = Record::new();
                out.insert(
                    "status".to_owned(),
                    Value::text(if matches!(record.outcome, Outcome::Skipped { .. }) {
                        "skipped"
                    } else {
                        "done"
                    }),
                );
                out.insert(
                    "result".to_owned(),
                    record.steps.last().map_or(Value::Null, |s| s.result.clone()),
                );
                out.insert("steps".to_owned(), Value::record(steps));
                Done::Value(Value::record(out))
            }
            Kind::Email(mail) => {
                let credential = self.effects.credential(&mail.credential)?;
                if !matches!(credential.kind.as_str(), "smtp" | "test") {
                    return Err(format!(
                        "{} is a {} credential: an email step takes an smtp one",
                        mail.credential, credential.kind
                    ));
                }
                let render =
                    |t: &crate::Template, name: &str| t.render(&env).map_err(|e| format!("{name}: {e}"));
                let list = |t: Option<&crate::Template>, name: &str| -> Result<Vec<String>, String> {
                    Ok(match t {
                        None => Vec::new(),
                        Some(t) => render(t, name)?
                            .split([',', ';'])
                            .map(str::trim)
                            .filter(|a| !a.is_empty())
                            .map(str::to_owned)
                            .collect(),
                    })
                };
                let optional = |t: Option<&crate::Template>, name: &str| -> Result<Option<String>, String> {
                    t.map(|t| render(t, name)).transpose()
                };
                let request = EmailRequest {
                    credential,
                    to: list(Some(&mail.to), "to")?,
                    cc: list(mail.cc.as_ref(), "cc")?,
                    bcc: list(mail.bcc.as_ref(), "bcc")?,
                    reply_to: optional(mail.reply_to.as_ref(), "reply_to")?.filter(|r| !r.trim().is_empty()),
                    subject: render(&mail.subject, "subject")?,
                    text: optional(mail.text.as_ref(), "text")?,
                    html: optional(mail.html.as_ref(), "html")?,
                    // G6 files
                    attachments: crate::files::steps::attachments(
                        self.effects,
                        &self.files,
                        &mail.attachments,
                        &env,
                    )
                    .await?,
                };
                Done::Value(self.effects.email(request).await?)
            }
            // data tables: a script given the workflow's state.
            Kind::Script {
                language,
                code,
                time,
                state: true,
            } => {
                let (result, logs) = tables::script(self, language, code, *time, &env).await?;
                Done::Logged(result, logs)
            }
            Kind::Script {
                language, code, time, ..
            } => {
                let as_json = |v: &Value| serde_json::from_str::<Json>(&v.to_json()).unwrap_or(Json::Null);
                let mut given = serde_json::Map::new();
                given.insert("value".to_owned(), as_json(&self.event.value));
                for (name, value) in &names {
                    given.insert(name.clone(), as_json(value));
                }
                let answer = self
                    .effects
                    .script(ScriptRequest {
                        language,
                        code: code.clone(),
                        names: given,
                        time: *time,
                    })
                    .await?;
                let Value::Record(fields) = answer else {
                    return Err("the script gave no answer".to_owned());
                };
                let logs = match fields.get("logs") {
                    Some(Value::List(lines)) => lines.iter().map(crate::template::as_text).collect(),
                    _ => Vec::new(),
                };
                Done::Logged(fields.get("result").cloned().unwrap_or(Value::Null), logs)
            }
            Kind::App(app) => {
                let credential = self.effects.credential(&app.credential)?;
                let spec = crate::apps::app(app.app).ok_or("no such app")?;
                if credential.kind != "test" && !spec.credentials.contains(&credential.kind.as_str()) {
                    return Err(format!(
                        "{} is a {} credential: {} takes {}",
                        app.credential,
                        credential.kind,
                        app.app,
                        spec.credentials.join(" or ")
                    ));
                }
                let mut fields = std::collections::BTreeMap::new();
                for (name, value) in &app.fields {
                    fields.insert(
                        name.clone(),
                        tree(value, &env).map_err(|e| format!("{name}: {e}"))?,
                    );
                }
                let request = crate::apps::request(app.app, &credential, &fields)?;
                let asked = Instant::now();
                // chat trigger (G8): the step a chat shows as it is made asks for its answer as events
                let mut answer = match crate::chat::streams(&self.event.details, &step.name, app.app) {
                    Some(reply) => {
                        crate::chat::ask_streaming(self.effects, request, &credential, &reply).await?
                    }
                    None => self.effects.http(request).await?,
                };
                // A model that refuses an optional sampling setting is asked again without it, once.
                let ignored = if app.app == "ai" {
                    crate::ai::refused_settings(&answer)
                } else {
                    Vec::new()
                };
                if !ignored.is_empty() {
                    for setting in &ignored {
                        fields.remove(*setting);
                    }
                    answer = self
                        .effects
                        .http(crate::apps::request(app.app, &credential, &fields)?)
                        .await?;
                }
                let ms = millis(asked.elapsed());
                let mut result = crate::apps::answer(app.app, &credential, &fields, answer, ms)?;
                if !ignored.is_empty()
                    && let Value::Record(record) = &mut result
                    && let Some(Value::Record(metrics)) = std::sync::Arc::make_mut(record).get_mut("metrics")
                {
                    let names = ignored.iter().map(|s| Value::text(*s)).collect();
                    std::sync::Arc::make_mut(metrics)
                        .insert("ignored_settings".to_owned(), Value::list(names));
                }
                Done::Value(result)
            }
            Kind::Defined(d) => samples::app(self, d, &env).await?, // test runs: or its recorded example
            Kind::Http(http) => {
                let mut built = request(http, &env)?;
                if let Some(name) = &http.credential {
                    crate::apps::authorize(&mut built, &self.effects.credential(name)?)?;
                }
                let answer = crate::files::steps::http(self.effects, &self.files, http, built, &env).await?; // G6 files
                if http.fail_on_status
                    && let Some(status) = status_of(&answer).filter(|s| *s >= 400)
                {
                    let body = match &answer {
                        Value::Record(r) => r.get("body").map(crate::template::as_text).unwrap_or_default(),
                        _ => String::new(),
                    };
                    let shown: String = body.chars().take(300).collect();
                    return Err(format!("answered {status}: {shown}"));
                }
                samples::http(self.effects, http, answer) // test runs: or its sample answer
            }
            Kind::Topic { name, key, value } => {
                let key = key.as_ref().map(|k| k.render(&env)).transpose()?;
                let value = tree(value, &env)?.to_json();
                Done::Value(self.effects.write(name, key, value).await?)
            }
            Kind::Wait(wait) => {
                self.effects.sleep(*wait).await;
                Done::Value(Value::from_json(&json!({ "waited_ms": millis(*wait) })))
            }
            Kind::Stop { error } => Done::End(error.as_ref().map(|e| e.render(&env)).transpose()?),
            Kind::Agent(agent) => Done::Value(self.agent(step, agent, &env, scope).await?), // G2 AI agents
            Kind::Mcp(m) => Done::Value(crate::mcp::step::run(self.effects, m, &env).await?), // MCP
            // retrieval; vector stores: the run's own memory of pieces beside the effects
            Kind::Retrieval(r) => {
                Done::Value(crate::retrieval::run(r, self.effects, &env, &self.vectors).await?)
            }
            Kind::Table(t) => samples::table(self, t, &env).await?, // data tables; test runs: or a stand-in
            Kind::State(s) => Done::Value(tables::state(self, s, &env).await?), // data tables
            // G6 files
            Kind::Extract(x) => {
                Done::Value(crate::files::steps::extract(self.effects, &self.files, x, &env).await?)
            }
            Kind::Convert(c) => Done::Value(
                crate::files::steps::convert(self.effects, &self.files, c, &step.name, &env).await?,
            ),
            Kind::FileDetails(d) => {
                Done::Value(crate::files::steps::details(self.effects, &self.files, d, &env).await?)
            }
            Kind::Database(q) => Done::Value(crate::databases::run(self.effects, q, &env).await?), // sql databases
            Kind::Storage(s) => {
                Done::Value(crate::storage::run(self.effects, &self.files, s, &step.name, &env).await?)
            } // storage
            Kind::Web(w) => Done::Value(crate::web::run(self.effects, &self.files, w, &env).await?), // web
            Kind::Mailbox(m) => Done::Value(crate::mailbox::run(self.effects, &self.files, m, &env).await?), // messaging
            Kind::If { .. } | Kind::ForEach { .. } | Kind::Switch { .. } => Done::Value(Value::Null),
            Kind::Ai(_) => Done::Value(Value::Null), // G7 more AI steps: run by ai_step
            // forms-and-resume: a step that waits is taken by `step`.
            Kind::Pause(_) => Done::Value(Value::Null),
        })
    }
}

enum Done {
    Value(Value),
    /// A script's answer, and what it logged.
    Logged(Value, Vec<String>),
    /// A result, and what a test run says beside it (see [`StepRecord::test`]). // test runs
    Noted(Value, Value),
    Skip,
    End(Option<String>),
}

fn status_of(answer: &Value) -> Option<u64> {
    match answer {
        Value::Record(r) => match r.get("status") {
            Some(Value::Number(n)) => rust_decimal::prelude::ToPrimitive::to_u64(n),
            _ => None,
        },
        _ => None,
    }
}

fn request(http: &Http, env: &Env<'_>) -> Result<HttpRequest, String> {
    let pairs = |pairs: &[(String, crate::template::Template)]| -> Result<Vec<(String, String)>, String> {
        pairs
            .iter()
            .map(|(k, v)| Ok((k.clone(), v.render(env).map_err(|e| format!("{k}: {e}"))?)))
            .collect()
    };
    let body = match &http.body {
        Body::None => None,
        Body::Json(t) => Some((
            "application/json".to_owned(),
            tree(t, env)?.to_json().into_bytes(),
        )),
        Body::Text(t) => Some((
            "text/plain; charset=utf-8".to_owned(),
            t.render(env)?.into_bytes(),
        )),
        Body::Form(fields) => {
            let encoded: Vec<String> = pairs(fields)?
                .iter()
                .map(|(k, v)| format!("{}={}", form_encode(k), form_encode(v)))
                .collect();
            Some((
                "application/x-www-form-urlencoded".to_owned(),
                encoded.join("&").into_bytes(),
            ))
        }
        Body::File(_) | Body::Multipart(_) => None, // G6 files: made by crate::files::steps::http
    };
    Ok(HttpRequest {
        read: matches!(http.method.as_str(), "GET" | "HEAD"),
        method: http.method.clone(),
        url: http.url.render(env).map_err(|e| format!("url: {e}"))?,
        headers: pairs(&http.headers)?,
        query: pairs(&http.query)?,
        body,
        timeout: http.timeout,
    })
}

fn form_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else if b == b' ' {
            out.push('+');
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// A tree's value: its templates worked out.
pub(crate) fn tree(tree: &Tree, env: &Env<'_>) -> Result<Value, String> {
    Ok(match tree {
        Tree::Literal(j) => Value::from_json(j),
        Tree::Template(t) => t.value(env)?,
        Tree::List(items) => Value::list(
            items
                .iter()
                .map(|t| self::tree(t, env))
                .collect::<Result<_, _>>()?,
        ),
        Tree::Record(fields) => Value::record(
            fields
                .iter()
                .map(|(k, t)| Ok((k.clone(), self::tree(t, env).map_err(|e| format!("{k}: {e}"))?)))
                .collect::<Result<Record, String>>()?,
        ),
    })
}
