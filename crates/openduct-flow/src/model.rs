// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow as written - YAML in a file, JSON from the studio - read, checked and compiled: every expression and
//! template compiled, every step named, so a mistake is found before the first event and said with where it is.

use std::collections::HashSet;
use std::time::Duration;

use openduct_expr::Compiled;
use serde_json::{Map, Value as Json};

use crate::template::Template;

mod kinds;
mod more;

/// The most steps a workflow has, branches and loops included.
pub const MAX_STEPS: usize = 200;
/// The longest a `wait` step waits within a run.
pub const MAX_WAIT: Duration = Duration::from_secs(3600);

/// A workflow, ready to run.
#[derive(Debug, Clone)]
pub struct Workflow {
    /// Letters, digits and `-`: names its topics and its group.
    pub id: String,
    pub name: String,
    pub trigger: Trigger,
    pub steps: Vec<Step>,
    /// How many of its runs go at once on each node (`runs_at_once`, 1 to 50; 1 runs each record after the one
    /// before). Records with the same key always run in order.
    pub runs_at_once: u32,
}

/// What starts a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trigger {
    /// A request to `/hooks/<workflow id>`; a token, when given, must come with it.
    Webhook { token: Option<String> },
    /// A run at each interval, counted from midnight UTC (`every: 15m` runs at :00, :15, :30, :45).
    Every(Duration),
    /// A run for every message of a topic.
    Topic { name: String, earliest: bool },
    /// A run when someone asks for one: the studio's Run button, or `POST /api/workflows/<id>/run`.
    Manual,
    /// A run for each record another workflow sends to CatchErr: that record, with why its run failed.
    Failed { workflow: String },
}

/// One step.
#[derive(Debug, Clone)]
pub struct Step {
    /// What later steps read its result by: `steps.<name>`.
    pub name: String,
    pub kind: Kind,
    /// Tries in all, for a step that fails; at least 1.
    pub attempts: u32,
    /// The wait before trying again; doubled each time.
    pub retry_wait: Duration,
    /// Whether the run goes on when the step fails at its last try (its result is then `{"error": ...}`).
    pub continue_on_error: bool,
}

#[derive(Debug, Clone)]
pub enum Kind {
    /// A record of fields, each an expression.
    Set(Vec<(String, Compiled)>),
    /// The run goes on only when this is true.
    Where(Compiled),
    If {
        when: Compiled,
        then: Vec<Step>,
        otherwise: Vec<Step>,
    },
    ForEach {
        items: Compiled,
        steps: Vec<Step>,
    },
    Http(Http),
    /// A step that talks to a model (see [`crate::apps`]).
    App(App),
    /// A step that runs an action of an app defined by a file (see [`crate::app_files`]).
    Defined(Defined),
    /// Rows of a lookup file whose columns hold the given text: the first (or every one, with `all`).
    Lookup {
        file: String,
        matching: Vec<(String, Template)>,
        all: bool,
    },
    /// SQL run on a Postgres database, its values passed as parameters ($1, $2 ...), never written into it.
    Sql {
        credential: String,
        sql: String,
        params: Vec<Compiled>,
    },
    /// A JavaScript or Python script: it computes an answer from what it is given, and reaches nothing outside.
    Script {
        language: &'static str,
        code: String,
        time: Duration,
    },
    /// An e-mail, through the mail server a credential names.
    Email(Email),
    /// The steps of the first case whose condition holds; the default's when none does.
    Switch {
        cases: Vec<Case>,
        otherwise: Vec<Step>,
    },
    /// Lists, or records, made one.
    Merge {
        of: Vec<Compiled>,
        mode: MergeMode,
        key: Option<String>,
    },
    /// A list's items kept, ordered, made unique and cut short, in that order (each an expression over `item`).
    List(ListOps),
    /// A list's items grouped by a key, each group counted and summed, averaged, its least and greatest found.
    Group(GroupOps),
    /// The answer a webhook's caller gets.
    Respond {
        status: u16,
        headers: Vec<(String, Template)>,
        body: Tree,
    },
    /// Another workflow of the same workspace, run for a record and its result given back.
    RunWorkflow {
        workflow: String,
        input: Option<Compiled>,
    },
    /// A time: read, moved, and written in a zone and a form.
    DateTime(DateTimeOps),
    /// A message written to a topic.
    Topic {
        name: String,
        key: Option<Template>,
        value: Tree,
    },
    Wait(Duration),
    /// The run ends here: well, or failed with the reason.
    Stop {
        error: Option<Template>,
    },
}

/// One case of a switch: its label, its condition, its steps.
#[derive(Debug, Clone)]
pub struct Case {
    pub label: String,
    pub when: Compiled,
    pub steps: Vec<Step>,
}

/// How a merge makes one of several.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// The lists one after another.
    Append,
    /// The records' fields in one record, a later one's winning.
    Fields,
    /// The lists' records joined on a key: records with the same key made one.
    Key,
}

#[derive(Debug, Clone)]
pub struct ListOps {
    pub items: Compiled,
    pub filter: Option<Compiled>,
    pub sort: Option<Compiled>,
    pub descending: bool,
    pub unique: Option<Compiled>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    Sum,
    Average,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct GroupOps {
    pub items: Compiled,
    pub by: Compiled,
    /// Each a field of the group's result: what it is and of which expression.
    pub fields: Vec<(String, Aggregate, Compiled)>,
}

#[derive(Debug, Clone)]
pub struct DateTimeOps {
    pub value: Option<Compiled>,
    pub add: Option<Duration>,
    pub subtract: Option<Duration>,
    pub timezone: Option<String>,
    pub format: Option<String>,
}

/// An e-mail step: each field a template; to, cc and bcc hold addresses separated by commas.
#[derive(Debug, Clone)]
pub struct Email {
    pub credential: String,
    pub to: Template,
    pub cc: Option<Template>,
    pub bcc: Option<Template>,
    pub reply_to: Option<Template>,
    pub subject: Template,
    pub text: Option<Template>,
    pub html: Option<Template>,
}

/// A step that talks to an app.
#[derive(Debug, Clone)]
pub struct App {
    /// `ai`, `telegram`, `slack`...
    pub app: &'static str,
    /// The credential it uses, by name.
    pub credential: String,
    /// Its fields: text with `{{ }}` in it, or JSON with it in its texts (`messages`).
    pub fields: Vec<(String, Tree)>,
}

/// A step that runs an app's action: the app (by its definition), the action, the credential, and the inputs.
#[derive(Debug, Clone)]
pub struct Defined {
    pub app: &'static crate::app_files::AppDef,
    pub action: String,
    pub credential: String,
    pub inputs: Vec<(String, Tree)>,
}

#[derive(Debug, Clone)]
pub struct Http {
    /// A credential added to the request (bearer, header or basic), by name.
    pub credential: Option<String>,
    pub method: String,
    pub url: Template,
    pub headers: Vec<(String, Template)>,
    pub query: Vec<(String, Template)>,
    pub body: Body,
    pub timeout: Duration,
    /// An answer of 400 or above fails the step (unless false: the answer is the result either way).
    pub fail_on_status: bool,
}

#[derive(Debug, Clone)]
pub enum Body {
    None,
    Json(Tree),
    Text(Template),
    Form(Vec<(String, Template)>),
}

/// JSON with templates in its text: what a step sends.
#[derive(Debug, Clone)]
pub enum Tree {
    Literal(Json),
    Template(Template),
    List(Vec<Tree>),
    Record(Vec<(String, Tree)>),
}

impl Workflow {
    /// Reads a workflow file: YAML (JSON is YAML too).
    pub fn parse(text: &str) -> Result<Self, String> {
        let json = crate::yaml::to_json(text)?;
        Self::from_json(&json, None)
    }

    /// Reads a workflow from its JSON form; `id` when the studio keeps it under one.
    pub fn from_json(json: &Json, id: Option<&str>) -> Result<Self, String> {
        let top = json
            .as_object()
            .ok_or("a workflow is a set of settings: name, trigger, steps")?;
        for key in top.keys() {
            if !matches!(
                key.as_str(),
                "name"
                    | "id"
                    | "language"
                    | "trigger"
                    | "steps"
                    | "description"
                    | "category"
                    | "runs_at_once"
                    | "sample"
            ) {
                return Err(format!(
                    "{key:?} is not a workflow setting: name, trigger, steps, description, category, language, \
                     runs_at_once, sample"
                ));
            }
        }
        let name = text(top.get("name"))
            .ok_or("name: the workflow's name")?
            .trim()
            .to_owned();
        if name.is_empty() || name.chars().count() > 100 {
            return Err("name: a name of 1 to 100 characters".to_owned());
        }
        let id = match id
            .map(str::to_owned)
            .or_else(|| text(top.get("id")).map(str::to_owned))
        {
            Some(id) => id,
            None => slug(&name),
        };
        if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Err(format!("id: {id:?} is letters, digits and '-', at most 64"));
        }
        let language = text(top.get("language"))
            .unwrap_or(openduct_expr::DEFAULT)
            .to_owned();
        openduct_expr::language(&language).map_err(|e| format!("language: {e}"))?;
        let trigger = trigger(top.get("trigger")).map_err(|e| format!("trigger: {e}"))?;
        if matches!(&trigger, Trigger::Failed { workflow } if *workflow == id) {
            return Err("trigger: failed.workflow: another workflow, not this one".to_owned());
        }
        let mut reader = Reader {
            language,
            names: HashSet::new(),
            count: 0,
        };
        let steps = reader.steps(top.get("steps"), "steps")?;
        let runs_at_once = match top.get("runs_at_once") {
            None | Some(Json::Null) => crate::runner::RUNS_AT_ONCE,
            Some(n) => n
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| (1..=crate::runner::MAX_RUNS_AT_ONCE).contains(n))
                .ok_or_else(|| {
                    format!(
                        "runs_at_once: how many runs go at once, 1 to {}",
                        crate::runner::MAX_RUNS_AT_ONCE
                    )
                })?,
        };
        Ok(Self {
            id,
            name,
            trigger,
            steps,
            runs_at_once,
        })
    }
}

fn text(v: Option<&Json>) -> Option<&str> {
    v.and_then(Json::as_str)
}

/// A name as an id: small letters, digits and '-'.
pub fn slug(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('-') && !out.is_empty() {
            out.push('-');
        }
    }
    out.trim_end_matches('-').chars().take(64).collect()
}

fn trigger(value: Option<&Json>) -> Result<Trigger, String> {
    let Some(value) = value else {
        return Ok(Trigger::Manual);
    };
    if value.as_str() == Some("manual") || value.as_str() == Some("webhook") {
        return trigger(Some(
            &serde_json::json!({ value.as_str().unwrap_or_default(): {} }),
        ));
    }
    let fields = value
        .as_object()
        .ok_or("webhook, schedule, topic, manual or failed")?;
    let [(kind, settings)] = fields.iter().collect::<Vec<_>>()[..] else {
        return Err("one of webhook, schedule, topic, manual or failed".to_owned());
    };
    let settings = match settings {
        Json::Null => Map::new(),
        Json::Object(m) => m.clone(),
        _ => return Err(format!("{kind}: a set of settings")),
    };
    let only = |allowed: &[&str]| -> Result<(), String> {
        match settings.keys().find(|k| !allowed.contains(&k.as_str())) {
            Some(k) => Err(format!(
                "{kind}.{k}: not a setting of {kind} ({})",
                allowed.join(", ")
            )),
            None => Ok(()),
        }
    };
    match kind.as_str() {
        "webhook" => {
            only(&["token"])?;
            Ok(Trigger::Webhook {
                token: text(settings.get("token")).map(str::to_owned),
            })
        }
        "schedule" => {
            only(&["every"])?;
            let every = text(settings.get("every")).ok_or("schedule.every: how often, as 15m, 1h or 1d")?;
            let every = duration(every)
                .ok_or_else(|| format!("schedule.every: {every:?} is not 30s, 15m, 1h or 1d"))?;
            if every < Duration::from_secs(1) {
                return Err("schedule.every: at least 1s".to_owned());
            }
            Ok(Trigger::Every(every))
        }
        "topic" => {
            only(&["name", "start"])?;
            let name = text(settings.get("name"))
                .ok_or("topic.name: the topic to read")?
                .to_owned();
            let earliest = match text(settings.get("start")) {
                None | Some("latest") => false,
                Some("earliest") => true,
                Some(other) => return Err(format!("topic.start: earliest or latest, not {other:?}")),
            };
            Ok(Trigger::Topic { name, earliest })
        }
        "manual" => {
            only(&[])?;
            Ok(Trigger::Manual)
        }
        "failed" => {
            only(&["workflow"])?;
            let workflow = text(settings.get("workflow"))
                .filter(|w| !w.trim().is_empty())
                .ok_or("failed.workflow: the workflow whose failed runs start this one")?;
            Ok(Trigger::Failed {
                workflow: workflow.to_owned(),
            })
        }
        other => Err(format!(
            "{other:?} is not a trigger: webhook, schedule, topic or manual"
        )),
    }
}

/// `500ms`, `30s`, `5m`, `1h`, `1d`.
pub fn duration(text: &str) -> Option<Duration> {
    let text = text.trim();
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    let n: u64 = digits.parse().ok()?;
    let millis = match text.get(digits.len()..)?.trim() {
        "ms" => n,
        "s" | "" => n.checked_mul(1_000)?,
        "m" => n.checked_mul(60_000)?,
        "h" => n.checked_mul(3_600_000)?,
        "d" => n.checked_mul(86_400_000)?,
        _ => return None,
    };
    Some(Duration::from_millis(millis))
}

const KINDS: &[&str] = &[
    "set",
    "where",
    "if",
    "for_each",
    "http",
    "lookup",
    "postgres",
    "javascript",
    "python",
    "email",
    "switch",
    "merge",
    "list",
    "group",
    "respond",
    "run_workflow",
    "datetime",
    "topic",
    "wait",
    "stop",
];
/// The longest script, and how long one runs: by default and at most.
const MAX_SCRIPT: usize = 256 * 1024;
const SCRIPT_TIME: Duration = Duration::from_secs(10);
const MAX_SCRIPT_TIME: Duration = Duration::from_secs(60);
/// The most parameters a `postgres` step passes.
const MAX_PARAMS: usize = 100;
const COMMON: &[&str] = &["name", "retry", "on_error"];

struct Reader {
    language: String,
    names: HashSet<String>,
    count: usize,
}

impl Reader {
    fn expression(&self, source: &Json, at: &str) -> Result<Compiled, String> {
        let source = match source {
            Json::String(s) => s.clone(),
            Json::Bool(_) | Json::Number(_) => source.to_string(),
            _ => return Err(format!("{at}: an expression")),
        };
        openduct_expr::compile(&self.language, &source).map_err(|e| format!("{at}: {e}"))
    }

    fn template(&self, source: &Json, at: &str) -> Result<Template, String> {
        match source {
            Json::String(s) => Template::compile(s, &self.language).map_err(|e| format!("{at}: {e}")),
            Json::Bool(_) | Json::Number(_) => Ok(Template::text(&source.to_string())),
            _ => Err(format!("{at}: text")),
        }
    }

    fn tree(&self, source: &Json, at: &str) -> Result<Tree, String> {
        Ok(match source {
            Json::String(_) => Tree::Template(self.template(source, at)?),
            Json::Array(items) => Tree::List(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| self.tree(v, &format!("{at}[{i}]")))
                    .collect::<Result<_, _>>()?,
            ),
            Json::Object(fields) => Tree::Record(
                fields
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), self.tree(v, &format!("{at}.{k}"))?)))
                    .collect::<Result<_, String>>()?,
            ),
            other => Tree::Literal(other.clone()),
        })
    }

    fn pairs(&self, source: Option<&Json>, at: &str) -> Result<Vec<(String, Template)>, String> {
        match source {
            None | Some(Json::Null) => Ok(Vec::new()),
            Some(Json::Object(fields)) => fields
                .iter()
                .map(|(k, v)| Ok((k.clone(), self.template(v, &format!("{at}.{k}"))?)))
                .collect(),
            Some(_) => Err(format!("{at}: names and their text")),
        }
    }

    fn steps(&mut self, source: Option<&Json>, at: &str) -> Result<Vec<Step>, String> {
        let items = match source {
            None | Some(Json::Null) => return Ok(Vec::new()),
            Some(Json::Array(items)) => items,
            Some(_) => return Err(format!("{at}: a list of steps")),
        };
        items
            .iter()
            .enumerate()
            .map(|(i, item)| self.step(item, &format!("{at}[{i}]")))
            .collect()
    }

    fn step(&mut self, source: &Json, at: &str) -> Result<Step, String> {
        self.count += 1;
        if self.count > MAX_STEPS {
            return Err(format!("more than {MAX_STEPS} steps"));
        }
        let fields = source
            .as_object()
            .ok_or_else(|| format!("{at}: a step, as {{http: ...}} or {{set: ...}}"))?;
        let kinds: Vec<&String> = fields
            .keys()
            .filter(|k| {
                KINDS.contains(&k.as_str())
                    || crate::apps::app(k).is_some()
                    || crate::app_files::find(k).is_some()
            })
            .collect();
        let [kind] = kinds[..] else {
            return Err(format!("{at}: one of {} in each step", KINDS.join(", ")));
        };
        if let Some(other) = fields.keys().find(|k| {
            !KINDS.contains(&k.as_str())
                && crate::apps::app(k).is_none()
                && crate::app_files::find(k).is_none()
                && !COMMON.contains(&k.as_str())
        }) {
            return Err(format!(
                "{at}.{other}: not a step setting ({})",
                COMMON.join(", ")
            ));
        }
        let name = match text(fields.get("name")) {
            Some(n) => n.to_owned(),
            None => format!("{kind}_{}", self.count),
        };
        let at = format!("{at} ({name})");
        if !name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.len() > 64
        {
            return Err(format!(
                "{at}: a step's name is letters, digits and '_', starting with a letter"
            ));
        }
        if !self.names.insert(name.clone()) {
            return Err(format!("{at}: another step has this name"));
        }
        let (mut attempts, mut retry_wait) = (1, Duration::from_secs(1));
        if let Some(retry) = fields.get("retry") {
            let r = retry
                .as_object()
                .ok_or_else(|| format!("{at}.retry: {{attempts: 3, wait: 2s}}"))?;
            attempts = r
                .get("attempts")
                .and_then(Json::as_u64)
                .filter(|n| (1..=10).contains(n))
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| format!("{at}.retry.attempts: 1 to 10"))?;
            if let Some(w) = text(r.get("wait")) {
                retry_wait = duration(w).ok_or_else(|| format!("{at}.retry.wait: as 2s or 1m"))?;
            }
        }
        let continue_on_error = match text(fields.get("on_error")) {
            None | Some("stop") => false,
            Some("continue") => true,
            Some(other) => return Err(format!("{at}.on_error: stop or continue, not {other:?}")),
        };
        let body = fields.get(kind.as_str()).unwrap_or(&Json::Null);
        let kind = self.kind(kind, body, &format!("{at}.{kind}"))?;
        Ok(Step {
            name,
            kind,
            attempts,
            retry_wait,
            continue_on_error,
        })
    }

    fn kind(&mut self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        let settings = || body.as_object().ok_or_else(|| format!("{at}: a set of settings"));
        Ok(match kind {
            "set" => {
                let fields = settings()?;
                if fields.is_empty() {
                    return Err(format!("{at}: at least one field"));
                }
                Kind::Set(
                    fields
                        .iter()
                        .map(|(k, v)| Ok((k.clone(), self.expression(v, &format!("{at}.{k}"))?)))
                        .collect::<Result<_, String>>()?,
                )
            }
            "where" => Kind::Where(self.expression(body, at)?),
            "if" => {
                let s = settings()?;
                Kind::If {
                    when: self.expression(s.get("when").unwrap_or(&Json::Null), &format!("{at}.when"))?,
                    then: self.steps(s.get("then"), &format!("{at}.then"))?,
                    otherwise: self.steps(s.get("else"), &format!("{at}.else"))?,
                }
            }
            "for_each" => {
                let s = settings()?;
                Kind::ForEach {
                    items: self.expression(s.get("items").unwrap_or(&Json::Null), &format!("{at}.items"))?,
                    steps: self.steps(s.get("steps"), &format!("{at}.steps"))?,
                }
            }
            "http" => Kind::Http(self.http(settings()?, at)?),
            "lookup" => self.lookup(body, at)?,
            "postgres" => self.postgres(body, at)?,
            "email" => self.email(body, at)?,
            "javascript" | "python" => self.script(kind, body, at)?,
            "switch" => self.switch(body, at)?,
            "merge" => self.merge(body, at)?,
            "list" => self.list_ops(body, at)?,
            "group" => self.group(body, at)?,
            "respond" => self.respond(body, at)?,
            "run_workflow" => self.run_workflow(body, at)?,
            "datetime" => self.datetime(body, at)?,
            "topic" => {
                let s = settings()?;
                let name = text(s.get("name"))
                    .ok_or_else(|| format!("{at}.name: the topic to write"))?
                    .to_owned();
                Kind::Topic {
                    name,
                    key: s
                        .get("key")
                        .map(|k| self.template(k, &format!("{at}.key")))
                        .transpose()?,
                    value: self.tree(
                        s.get("value").unwrap_or(&Json::String("{{ value }}".into())),
                        &format!("{at}.value"),
                    )?,
                }
            }
            "wait" => {
                let written = match body {
                    Json::String(s) => s.as_str(),
                    Json::Object(o) => text(o.get("for")).unwrap_or_default(),
                    _ => "",
                };
                let wait = duration(written).ok_or_else(|| format!("{at}: how long, as 10s or 5m"))?;
                if wait > MAX_WAIT {
                    return Err(format!("{at}: at most 1h within a run"));
                }
                Kind::Wait(wait)
            }
            "stop" => Kind::Stop {
                error: body
                    .get("error")
                    .map(|e| self.template(e, &format!("{at}.error")))
                    .transpose()?,
            },
            other => match (crate::app_files::find(other), crate::apps::app(other)) {
                (Some(def), _) => Kind::Defined(self.defined(def, settings()?, at)?),
                (None, Some(spec)) => Kind::App(self.app(spec, settings()?, at)?),
                (None, None) => return Err(format!("{at}: not a step")),
            },
        })
    }

    /// An app's step: `action` (or the app's default), `credential`, and the action's inputs.
    fn defined(
        &self,
        def: &'static crate::app_files::AppDef,
        s: &Map<String, Json>,
        at: &str,
    ) -> Result<Defined, String> {
        let named = text(s.get("action"));
        let Some((action_name, action)) = def.action(named) else {
            let have: Vec<&str> = def.actions.iter().map(|(n, _)| n.as_str()).collect();
            return Err(match named {
                Some(n) => format!(
                    "{at}.action: {} has no action {n:?}: it has {}",
                    def.name,
                    have.join(", ")
                ),
                None => format!(
                    "{at}.action: which of {}'s actions ({})",
                    def.name,
                    have.join(", ")
                ),
            });
        };
        let kinds: Vec<&str> = def.sign_ins.iter().map(|s| s.id.as_str()).collect();
        let credential = text(s.get("credential"))
            .ok_or_else(|| {
                format!(
                    "{at}.credential: the credential to use (a {} one)",
                    kinds.join(" or ")
                )
            })?
            .to_owned();
        let known: Vec<&str> = action.inputs.iter().map(|i| i.name.as_str()).collect();
        let mut inputs = Vec::new();
        for (k, v) in s {
            if k == "credential" || k == "action" {
                continue;
            }
            if !known.contains(&k.as_str()) {
                return Err(format!(
                    "{at}.{k}: not an input of {}'s {action_name} ({}, credential)",
                    def.name,
                    known.join(", ")
                ));
            }
            inputs.push((k.clone(), self.tree(v, &format!("{at}.{k}"))?));
        }
        for required in action.inputs.iter().filter(|i| i.required) {
            if !s.contains_key(&required.name) {
                return Err(format!(
                    "{at}.{}: {}'s {action_name} needs it",
                    required.name, def.name
                ));
            }
        }
        Ok(Defined {
            app: def,
            action: action_name.to_owned(),
            credential,
            inputs,
        })
    }

    fn app(
        &self,
        spec: &'static crate::apps::AppKind,
        s: &Map<String, Json>,
        at: &str,
    ) -> Result<App, String> {
        let credential = text(s.get("credential"))
            .ok_or_else(|| {
                format!(
                    "{at}.credential: the credential to use ({})",
                    spec.credentials.join(" or ")
                )
            })?
            .to_owned();
        let known: Vec<&str> = spec.fields.iter().map(|f| f.trim_end_matches('?')).collect();
        let mut fields = Vec::new();
        for (k, v) in s {
            if k == "credential" {
                continue;
            }
            if !known.contains(&k.as_str()) {
                return Err(format!(
                    "{at}.{k}: not a setting of {} ({}, credential)",
                    spec.kind,
                    known.join(", ")
                ));
            }
            fields.push((k.clone(), self.tree(v, &format!("{at}.{k}"))?));
        }
        for required in spec.fields.iter().filter(|f| !f.ends_with('?')) {
            if !s.contains_key(*required) {
                return Err(format!("{at}.{required}: {} needs it", spec.kind));
            }
        }
        Ok(App {
            app: spec.kind,
            credential,
            fields,
        })
    }

    fn http(&self, s: &Map<String, Json>, at: &str) -> Result<Http, String> {
        const SETTINGS: &[&str] = &[
            "credential",
            "method",
            "url",
            "headers",
            "query",
            "json",
            "body",
            "form",
            "timeout",
            "fail_on_status",
        ];
        if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of http ({})",
                SETTINGS.join(", ")
            ));
        }
        let method = text(s.get("method")).unwrap_or("GET").to_ascii_uppercase();
        if !matches!(
            method.as_str(),
            "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD"
        ) {
            return Err(format!(
                "{at}.method: GET, POST, PUT, PATCH, DELETE or HEAD, not {method:?}"
            ));
        }
        let url = self.template(
            s.get("url")
                .ok_or_else(|| format!("{at}.url: the address to call"))?,
            &format!("{at}.url"),
        )?;
        let bodies = ["json", "body", "form"]
            .iter()
            .filter(|b| s.contains_key(**b))
            .count();
        if bodies > 1 {
            return Err(format!("{at}: one of json, body or form"));
        }
        let body = if let Some(j) = s.get("json") {
            Body::Json(self.tree(j, &format!("{at}.json"))?)
        } else if let Some(b) = s.get("body") {
            Body::Text(self.template(b, &format!("{at}.body"))?)
        } else if s.contains_key("form") {
            Body::Form(self.pairs(s.get("form"), &format!("{at}.form"))?)
        } else {
            Body::None
        };
        let timeout = match text(s.get("timeout")) {
            None => Duration::from_secs(30),
            Some(t) => duration(t)
                .filter(|d| *d <= Duration::from_secs(300))
                .ok_or_else(|| format!("{at}.timeout: as 30s, at most 5m"))?,
        };
        Ok(Http {
            credential: text(s.get("credential")).map(str::to_owned),
            method,
            url,
            headers: self.pairs(s.get("headers"), &format!("{at}.headers"))?,
            query: self.pairs(s.get("query"), &format!("{at}.query"))?,
            body,
            timeout,
            fail_on_status: s.get("fail_on_status").and_then(Json::as_bool).unwrap_or(true),
        })
    }
}
