// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The `agent` step: a model that, each run, chooses tools to reach a goal over several turns.
//!
//! Each turn the model is sent the conversation so far - its instructions, what its memory kept, the run's input,
//! then every call it made and what came back - with the tools it may use, each a name, a description and a JSON
//! schema of the arguments it fills. It answers with text (it is done) or with tool calls, which run - each checked
//! against its schema first - and whose results go back to it for the next turn. The request is the `ai` step's
//! own, so any model the `ai` step reaches is reached here, in either API shape ([`wire`]).
//!
//! **Tools** are the [`Tool`] trait: a [`ToolSpec`] (what the model is told) and a call. Steps of the workflow's own
//! kinds - an HTTP request, a script, another workflow, an app's action, another agent - are tools
//! ([`ToolKind::Step`], run by the run itself so a preview shows a write and the private-network rule holds).
//! Another kind of tool (an MCP server's tools, say) is one more [`ToolKind`] whose tools implement [`Tool`].
//!
//! **Limits**, each with a default and a most: turns, tool calls, the step's time, the tokens of one answer and of
//! the whole step. `allow` names the tools that may run; the others are not offered.
//!
//! **Memory**, off unless asked: the conversation kept under a key the step names (a chat's id, a customer's), the
//! last turns of it given back next time, forgotten when a condition holds or after a while ([`LogMemory`]).
//!
//! **The record**: the step's result holds every turn (its time and tokens) and every tool call (the tool, its
//! arguments, its result cut short, its time, its error) beside the answer - as a run's history shows other steps.

use std::time::{Duration, Instant};

use openduct_expr::{Compiled, Env, Value};
use serde_json::{Map, Value as Json, json};

use crate::model::Step;
use crate::run::Effects;
use crate::template::Template;

mod memory;
mod schema;
pub(crate) mod stores; // vector stores: memory kept in PostgreSQL, Redis or a data table
mod wire;

pub use memory::{LogMemory, MAX_CONVERSATIONS, MEMORY_TOPIC};
pub(crate) use schema::check as check_schema;
pub use stores::{MemoryStore, TABLE_COLUMNS}; // vector stores

/// The most tools an agent has.
pub const MAX_TOOLS: usize = 32;
/// How deep agents use other agents as tools.
pub const MAX_AGENT_DEPTH: usize = 3;
/// The largest arguments a model may give one call, in bytes.
const MAX_ARGUMENTS: usize = 64 * 1024;
/// How much of a tool's result the model reads, in characters.
const SHOWN_TO_MODEL: usize = 16 * 1024;
/// How much of a tool's result, or a turn's text, the run's record keeps, in characters.
const SHOWN_IN_RECORD: usize = 2_000;
/// The most characters of one message a memory keeps.
const REMEMBERED: usize = 8_000;
/// The largest input, in bytes.
const MAX_INPUT: usize = 256 * 1024;
/// The most tool calls one turn makes: those past it are let go, and the turn's record says how many.
const MAX_CALLS_A_TURN: usize = 32;

/// A future a tool's call or a memory gives.
pub type Pending<'a> = std::pin::Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// An agent step, as read.
#[derive(Debug, Clone)]
pub struct Agent {
    /// The credential of the model's API (an `ai` one).
    pub credential: String,
    /// The model; none: the credential's.
    pub model: Option<Template>,
    /// How the agent behaves: its system message.
    pub instructions: Option<Template>,
    /// The goal of this run: the user's message.
    pub input: Template,
    pub tools: Vec<ToolDef>,
    /// The only tools that may run; none: every tool.
    pub allow: Option<Vec<String>>,
    pub memory: Option<MemoryDef>,
    pub output: Output,
    pub limits: Limits,
    pub temperature: Option<f64>,
}

/// One tool of an agent: what the model is told about it, and what it does.
#[derive(Debug, Clone)]
pub struct ToolDef {
    pub spec: ToolSpec,
    pub does: ToolKind,
}

/// What the model is told about a tool: its name, what it does, and the JSON schema of the arguments it fills.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Json,
}

/// What a tool does.
#[derive(Debug, Clone)]
pub enum ToolKind {
    /// A step of the workflow's own kinds, run with the model's arguments as `args`. For an app's action, the inputs
    /// in `fills` are the model's arguments of the same names, as they are.
    Step { step: Box<Step>, fills: Vec<String> },
}

/// A tool as an agent's turns call it: the interface every kind of tool implements.
///
/// To add a kind of tool (an MCP server's tools, say): a [`ToolKind`] variant, read where an agent's tools are read
/// (`model/agent.rs`); one arm where a run makes its agent's tools (`run/agent.rs`), making one `Tool` for each tool
/// it has - which may be several, found when the run starts. A call's result and its failure both go back to the
/// model; the arguments are checked against [`ToolSpec::parameters`] before `call`, and the call is cut off when the
/// agent's time runs out.
pub trait Tool: Send + Sync {
    /// What the model is told about it.
    fn spec(&self) -> &ToolSpec;
    /// Runs it with the arguments the model gave (a record, checked against the tool's schema already): its
    /// result, which the model reads as JSON, or why it failed, which the model reads too.
    fn call<'a>(&'a self, args: Value) -> Pending<'a>;
}

/// An agent's memory: the conversation kept under a key, between runs.
#[derive(Debug, Clone)]
pub struct MemoryDef {
    /// Whose conversation: a chat's id, a customer's.
    pub key: Template,
    /// How many of its last turns (a message and its answer) are given back.
    pub window: usize,
    /// When this is true, what was kept is forgotten and the run starts afresh.
    pub clear: Option<Compiled>,
    /// How long a conversation is kept after its last turn.
    pub forget_after: Duration,
    /// Where it is kept: the broker unless the step names another store. // vector stores
    pub store: MemoryStore,
}

/// What an agent answers with.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Text,
    /// One JSON value; matching the schema, when one is given.
    Json(Option<Json>),
}

/// How far an agent goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_turns: u32,
    pub max_tool_calls: u32,
    /// The step's time, every turn and call in it.
    pub timeout: Duration,
    /// The tokens of one answer.
    pub max_tokens: u64,
    /// The tokens of the whole step: every turn's, asked and answered.
    pub max_total_tokens: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_turns: 10,
            max_tool_calls: 20,
            timeout: Duration::from_secs(300),
            max_tokens: 1024,
            max_total_tokens: 200_000,
        }
    }
}

/// Text cut to `most` characters, saying so.
fn cut(text: &str, most: usize) -> String {
    if text.chars().count() <= most {
        return text.to_owned();
    }
    let kept: String = text.chars().take(most).collect();
    format!(
        "{kept}... (cut short: {} characters in all)",
        text.chars().count()
    )
}

fn as_json(value: &Value) -> Json {
    serde_json::from_str(&value.to_json()).unwrap_or(Json::Null)
}

/// A result as the record keeps it: whole when small, else its JSON cut short.
fn shown(result: &Json) -> Json {
    let text = result.to_string();
    if text.chars().count() <= SHOWN_IN_RECORD {
        result.clone()
    } else {
        Json::String(cut(&text, SHOWN_IN_RECORD))
    }
}

/// A tool's result as the model reads it: text as it is, anything else as JSON.
fn for_model(result: &Json) -> String {
    let text = result.as_str().map_or_else(|| result.to_string(), str::to_owned);
    cut(&text, SHOWN_TO_MODEL)
}

/// What the run's record keeps of the agent's work.
#[derive(Default)]
struct Kept {
    turns: Vec<Json>,
    calls: Vec<Json>,
    transcript: Vec<Json>,
    prompt_tokens: u64,
    completion_tokens: u64,
    calls_made: u32,
}

/// Runs an agent: its turns, its tools' calls, its memory. `scope` names its memory apart from every other step's
/// (`<workflow>/<step>`).
pub(crate) async fn converse(
    effects: &dyn Effects,
    agent: &Agent,
    env: &Env<'_>,
    tools: &[&dyn Tool],
    scope: &str,
) -> Result<Value, String> {
    converse_with(effects, agent, env, tools, scope, &Hooks::default()).await
}

// agent hooks
/// What a caller of the loop may add to it: a word after each turn and each tool call (the record's own entry), and a
/// check of a final answer - its value, or what is wrong with it, said to the model, which answers again in the next
/// turn (counted as every turn is) until it passes or the turns run out.
#[derive(Default)]
pub(crate) struct Hooks<'h> {
    pub progress: Option<&'h (dyn Fn(&Json) + Send + Sync)>,
    pub answer: Option<&'h AnswerCheck<'h>>,
}

/// A check of a final answer: its value, or what is wrong with it.
pub(crate) type AnswerCheck<'a> = dyn Fn(&str) -> Result<Json, String> + Send + Sync + 'a;

/// [`converse`], with the caller's [`Hooks`].
pub(crate) async fn converse_with(
    effects: &dyn Effects,
    agent: &Agent,
    env: &Env<'_>,
    tools: &[&dyn Tool],
    scope: &str,
    hooks: &Hooks<'_>,
) -> Result<Value, String> {
    let started = Instant::now();
    let deadline = started + agent.limits.timeout;
    let credential = effects.credential(&agent.credential)?;
    if credential.kind != "test" && credential.kind != "ai" {
        return Err(format!(
            "{} is a {} credential: an agent takes an ai one",
            agent.credential, credential.kind
        ));
    }
    let model = match &agent.model {
        Some(m) => m.render(env).map_err(|e| format!("model: {e}"))?,
        None => String::new(),
    };
    let input = agent.input.render(env).map_err(|e| format!("input: {e}"))?;
    if input.trim().is_empty() {
        return Err("input: the agent's goal is empty in this run".to_owned());
    }
    if input.len() > MAX_INPUT {
        return Err(format!("input: at most {} KiB", MAX_INPUT / 1024));
    }
    let mut system = match &agent.instructions {
        Some(t) => t.render(env).map_err(|e| format!("instructions: {e}"))?,
        None => String::new(),
    };
    if let Output::Json(schema) = &agent.output {
        let asked = match schema {
            Some(s) => format!(
                "When you have the answer, give it as one JSON value and nothing else, matching this JSON schema: {s}"
            ),
            None => "When you have the answer, give it as one JSON value and nothing else.".to_owned(),
        };
        system = if system.trim().is_empty() {
            asked
        } else {
            format!("{system}\n\n{asked}")
        };
    }
    let memory = memory::recall(effects, agent, env, scope).await?;
    let mut conversation = Vec::new();
    let mut kept = Kept::default();
    if !system.trim().is_empty() {
        conversation.push(json!({ "role": "system", "content": system }));
        kept.transcript
            .push(json!({ "role": "system", "content": cut(&system, SHOWN_IN_RECORD) }));
    }
    for m in &memory.history {
        conversation.push(m.clone());
        let mut shown = m.clone();
        crate::apps::put(&mut shown, "remembered", Json::Bool(true));
        kept.transcript.push(shown);
    }
    conversation.push(json!({ "role": "user", "content": input }));
    kept.transcript
        .push(json!({ "role": "user", "content": cut(&input, SHOWN_IN_RECORD) }));
    let allowed = |name: &str| agent.allow.as_ref().is_none_or(|a| a.iter().any(|n| n == name));
    let offered: Vec<&dyn Tool> = tools
        .iter()
        .copied()
        .filter(|t| allowed(&t.spec().name))
        .collect();
    let specs: Vec<ToolSpec> = offered.iter().map(|t| t.spec().clone()).collect();
    let mut temperature = agent.temperature;
    let mut turn: u32 = 0;
    let mut repairing = false;
    let mut last = None;
    let (text, parsed) = loop {
        turn += 1;
        if turn > agent.limits.max_turns && !repairing {
            return Err(format!(
                "the agent did not finish in {} turns (max_turns)",
                agent.limits.max_turns
            ));
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(format!(
                "the agent took longer than its {}s (timeout)",
                agent.limits.timeout.as_secs()
            ));
        }
        let used = kept.prompt_tokens + kept.completion_tokens;
        if used >= agent.limits.max_total_tokens {
            return Err(format!(
                "the agent used {used} tokens, its most is {} (max_total_tokens)",
                agent.limits.max_total_tokens
            ));
        }
        let answer_now = repairing || kept.calls_made >= agent.limits.max_tool_calls;
        let ask = |temperature: Option<f64>| {
            wire::request(&wire::Asked {
                credential: &credential,
                model: &model,
                max_tokens: agent.limits.max_tokens,
                temperature,
                conversation: &conversation,
                tools: &specs,
                answer_now,
                json: matches!(agent.output, Output::Json(_)),
                timeout: left,
            })
        };
        let asked = Instant::now();
        let mut answer = effects.http(ask(temperature)?).await?;
        // A model that refuses an optional setting is asked again without it, as the ai step does.
        if crate::ai::refused_settings(&answer).contains(&"temperature") && temperature.is_some() {
            temperature = None;
            answer = effects.http(ask(None)?).await?;
        }
        let answer = as_json(&answer);
        // A test run sends nothing: it shows the first request, and answers as a real run would, saying so.
        if answer.get("test").is_some() {
            return Ok(stand_in(answer, agent, &specs));
        }
        let ms = millis(asked.elapsed());
        let mut reply = wire::reply(&credential, &answer)?;
        crate::meters::ai_call(reply.prompt_tokens, reply.completion_tokens);
        kept.prompt_tokens += reply.prompt_tokens.unwrap_or(0);
        kept.completion_tokens += reply.completion_tokens.unwrap_or(0);
        let let_go = reply.calls.len().saturating_sub(MAX_CALLS_A_TURN);
        reply.calls.truncate(MAX_CALLS_A_TURN);
        let names: Vec<&str> = reply.calls.iter().map(|c| c.name.as_str()).collect();
        let mut this_turn = json!({
            "turn": turn, "ms": ms,
            "prompt_tokens": reply.prompt_tokens, "completion_tokens": reply.completion_tokens,
            "said": cut(&reply.text, SHOWN_IN_RECORD), "tool_calls": names, "finish_reason": reply.finish,
        });
        if let_go > 0 {
            crate::apps::put(&mut this_turn, "calls_let_go", json!(let_go));
        }
        if let Some(progress) = hooks.progress {
            progress(&this_turn); // agent hooks
        }
        kept.turns.push(this_turn);
        if !reply.text.is_empty() {
            kept.transcript
                .push(json!({ "role": "assistant", "content": cut(&reply.text, SHOWN_IN_RECORD) }));
        }
        // agent hooks: a final answer the caller checks, put right turn by turn.
        if let Some(check) = hooks.answer
            && reply.calls.is_empty()
        {
            match check(&reply.text) {
                Ok(value) => break (reply.text.clone(), Some(value)),
                Err(problem) => {
                    conversation.push(wire::assistant(&reply));
                    kept.transcript
                        .push(json!({ "role": "user", "content": cut(&problem, SHOWN_IN_RECORD) }));
                    conversation.push(json!({ "role": "user", "content": problem }));
                    last = Some(reply);
                    continue;
                }
            }
        }
        if reply.calls.is_empty() || repairing {
            match &agent.output {
                Output::Text => break (reply.text.clone(), None),
                Output::Json(schema) => match json_answer(&reply.text, schema.as_ref()) {
                    Ok(v) => break (reply.text.clone(), Some(v)),
                    Err(problem) if !repairing => {
                        // One more turn, to put it right.
                        repairing = true;
                        conversation.push(wire::assistant(&wire::Reply {
                            calls: Vec::new(),
                            ..reply.clone()
                        }));
                        let again = format!(
                            "That answer is not what was asked: {problem}. Answer again with one JSON value and \
                             nothing else{}.",
                            if schema.is_some() {
                                ", matching the JSON schema given"
                            } else {
                                ""
                            }
                        );
                        kept.transcript.push(json!({ "role": "user", "content": again }));
                        conversation.push(json!({ "role": "user", "content": again }));
                        last = Some(reply);
                        continue;
                    }
                    Err(problem) => {
                        return Err(format!("the agent's answer is not the JSON asked for: {problem}"));
                    }
                },
            }
        }
        conversation.push(wire::assistant(&reply));
        for call in &reply.calls {
            let (result, failed) = call_tool(&offered, agent, call, &mut kept, turn, deadline).await;
            if let (Some(progress), Some(record)) = (hooks.progress, kept.calls.last()) {
                progress(record); // agent hooks
            }
            let mut message =
                json!({ "role": "tool", "tool_call_id": call.id, "content": for_model(&result) });
            if failed {
                crate::apps::put(&mut message, "is_error", Json::Bool(true));
            }
            conversation.push(message);
        }
        last = Some(reply);
    };
    let finish = last.as_ref().map_or("stop", |r| r.finish.as_str()).to_owned();
    let model_used = last
        .as_ref()
        .and_then(|r| r.model.clone())
        .unwrap_or_else(|| model.clone());
    let remembered = memory.keep(effects, agent, &input, &text).await;
    let total = kept.prompt_tokens + kept.completion_tokens;
    let mut result = json!({
        "text": text,
        "finish_reason": finish,
        "model": model_used,
        "usage": { "prompt_tokens": kept.prompt_tokens, "completion_tokens": kept.completion_tokens, "total_tokens": total },
        "turns": kept.turns,
        "calls": kept.calls,
        "transcript": kept.transcript,
        "metrics": {
            "latency_ms": millis(started.elapsed()),
            "turns": turn,
            "tool_calls": kept.calls_made,
            "api": if wire::messages_api(&credential) { "messages" } else { "chat_completions" },
        },
    });
    if let Some(parsed) = parsed {
        crate::apps::put(&mut result, "json", parsed);
    }
    if let Some(remembered) = remembered {
        crate::apps::put(&mut result, "memory", remembered);
    }
    Ok(Value::from_json(&result))
}

/// One tool call: refused, or run with its arguments checked; its result (or `{error}`) and whether it failed.
async fn call_tool(
    offered: &[&dyn Tool],
    agent: &Agent,
    call: &wire::Call,
    kept: &mut Kept,
    turn: u32,
    deadline: Instant,
) -> (Json, bool) {
    let started = Instant::now();
    let arguments: Json =
        serde_json::from_str(&call.arguments).unwrap_or(Json::String(cut(&call.arguments, 200)));
    let outcome: Result<Json, String> = async {
        if kept.calls_made >= agent.limits.max_tool_calls {
            return Err(format!(
                "no more tool calls: the most is {}. Answer now with what you have",
                agent.limits.max_tool_calls
            ));
        }
        let Some(tool) = offered.iter().find(|t| t.spec().name == call.name) else {
            let names: Vec<&str> = offered.iter().map(|t| t.spec().name.as_str()).collect();
            return Err(format!(
                "there is no tool {:?} you may use: the tools are {}",
                call.name,
                if names.is_empty() {
                    "none".to_owned()
                } else {
                    names.join(", ")
                }
            ));
        };
        kept.calls_made += 1;
        if call.arguments.len() > MAX_ARGUMENTS {
            return Err(format!("arguments of at most {} KiB", MAX_ARGUMENTS / 1024));
        }
        let args = if call.arguments.trim().is_empty() {
            json!({})
        } else {
            serde_json::from_str::<Json>(&call.arguments)
                .map_err(|e| format!("the arguments are not JSON: {e}"))?
        };
        if !args.is_object() {
            return Err("the arguments are a JSON record of names and values".to_owned());
        }
        let problems = schema::problems(&args, &tool.spec().parameters);
        if !problems.is_empty() {
            return Err(format!(
                "the arguments do not match the tool's schema: {}",
                problems.join("; ")
            ));
        }
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, tool.call(Value::from_json(&args))).await {
            Ok(Ok(v)) => Ok(as_json(&v)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("the agent's time ran out during this call".to_owned()),
        }
    }
    .await;
    let ms = millis(started.elapsed());
    let (result, error) = match outcome {
        Ok(v) => (v, None),
        Err(e) => (json!({ "error": e }), Some(e)),
    };
    let mut record = json!({
        "turn": turn, "tool": call.name, "arguments": shown(&arguments), "result": shown(&result), "ms": ms,
    });
    if let Some(e) = &error {
        crate::apps::put(&mut record, "error", json!(e));
    }
    kept.calls.push(record);
    kept.transcript.push(json!({
        "role": "tool",
        "tool": call.name,
        "content": cut(&format!("{}({}) -> {}", call.name, arguments, for_model(&result)), SHOWN_IN_RECORD),
    }));
    (result, error.is_some())
}

/// The JSON a final answer holds, checked against the schema; or what is wrong with it.
fn json_answer(text: &str, schema: Option<&Json>) -> Result<Json, String> {
    // Models sometimes wrap JSON in a fenced block: what is inside it is the answer.
    let inner = text
        .trim()
        .trim_start_matches("```json")
        .trim_start_matches("```")
        .trim_end_matches("```")
        .trim();
    let parsed: Json = serde_json::from_str(inner)
        .map_err(|_| format!("it is not JSON ({})", text.chars().take(120).collect::<String>()))?;
    if let Some(schema) = schema {
        let problems = schema::problems(&parsed, schema);
        if !problems.is_empty() {
            return Err(problems.join("; "));
        }
    }
    Ok(parsed)
}

/// What a test run or a preview shows for a memory it does not keep.
pub(crate) fn not_kept(key: &str, turns: Option<&Value>) -> Value {
    Value::from_json(&json!({
        "test": "not kept",
        "key": key,
        "turns": turns.map_or(Json::Null, as_json),
    }))
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A test run's answer: the first request as it would be sent, and the fields a real answer has, saying so.
fn stand_in(answer: Json, agent: &Agent, specs: &[ToolSpec]) -> Value {
    let mut shown = match answer {
        Json::Object(m) => m,
        _ => Map::new(),
    };
    shown.insert("text".to_owned(), json!("(the agent's answer, in a real run)"));
    if matches!(agent.output, Output::Json(_)) {
        shown.insert("json".to_owned(), json!({}));
    }
    let names: Vec<&str> = specs.iter().map(|t| t.name.as_str()).collect();
    shown.insert("tools".to_owned(), json!(names));
    shown.insert("turns".to_owned(), json!([]));
    shown.insert("calls".to_owned(), json!([]));
    Value::from_json(&Json::Object(shown))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_final_answer_is_read_as_json_inside_a_fence_and_checked() {
        let schema =
            json!({ "type": "object", "properties": { "n": { "type": "integer" } }, "required": ["n"] });
        assert_eq!(
            json_answer("```json\n{\"n\": 2}\n```", Some(&schema)).unwrap(),
            json!({ "n": 2 })
        );
        assert_eq!(
            json_answer("{\"m\": 2}", Some(&schema)).unwrap_err(),
            "n: missing"
        );
        assert!(
            json_answer("two", None)
                .unwrap_err()
                .starts_with("it is not JSON")
        );
    }

    #[test]
    fn what_is_kept_is_cut_short_saying_so() {
        let long = "x".repeat(SHOWN_IN_RECORD + 5);
        assert!(
            cut(&long, SHOWN_IN_RECORD)
                .ends_with(&format!("(cut short: {} characters in all)", SHOWN_IN_RECORD + 5))
        );
        assert_eq!(shown(&json!({ "a": 1 })), json!({ "a": 1 }));
        assert!(shown(&json!(long)).as_str().unwrap().contains("cut short"));
        assert_eq!(for_model(&json!("plain")), "plain");
        assert_eq!(for_model(&json!({ "a": 1 })), "{\"a\":1}");
    }
}
