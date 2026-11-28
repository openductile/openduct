// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading an `agent` step (see [`crate::agent`]): its model, instructions and input; its tools, each a step of the
//! workflow's own kinds with a name, a description and the schema of the arguments the model fills; which tools may
//! run; its memory, its output and its limits - every one bounded, and every mistake said with where it is.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::{Map, Value as Json, json};

use super::{KINDS, Kind, MAX_STEPS, Reader, Step, duration, text};
use crate::agent::{
    Agent, Limits, MAX_AGENT_DEPTH, MAX_TOOLS, MemoryDef, Output, ToolDef, ToolKind, ToolSpec, check_schema,
};

const SETTINGS: &[&str] = &[
    "credential",
    "model",
    "instructions",
    "input",
    "tools",
    "allow",
    "memory",
    "output",
    "max_turns",
    "max_tool_calls",
    "max_tokens",
    "max_total_tokens",
    "timeout",
    "temperature",
];
/// The steps that shape a run, which are not tools: a tool works something out or reaches out, and answers.
const NOT_TOOLS: &[&str] = &["where", "if", "for_each", "switch", "respond", "wait", "stop"];
/// The longest description of a tool, in characters.
const MAX_DESCRIPTION: usize = 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_WINDOW: u64 = 50;
const MAX_FORGET: Duration = Duration::from_secs(365 * 86_400);

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

/// How deep agents are inside this one's tools, this one counted.
fn depth(body: &Json) -> usize {
    let inner = body
        .get("tools")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
        .filter_map(|t| t.get("agent"))
        .map(depth)
        .max()
        .unwrap_or(0);
    inner + 1
}

/// A whole number setting in `range`, or its default.
fn count(
    s: &Map<String, Json>,
    name: &str,
    at: &str,
    default: u64,
    range: (u64, u64),
) -> Result<u64, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(v) => v
            .as_u64()
            .filter(|n| (range.0..=range.1).contains(n))
            .ok_or_else(|| format!("{at}.{name}: a whole number, {} to {}", range.0, range.1)),
    }
}

/// The JSON-schema type of an app's input kind.
fn json_type(kind: &str) -> &'static str {
    match kind {
        "number" => "number",
        "boolean" => "boolean",
        "list" => "array",
        "record" => "object",
        _ => "string",
    }
}

impl Reader {
    pub(super) fn agent(&mut self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of agent ({})",
                SETTINGS.join(", ")
            ));
        }
        if depth(body) > MAX_AGENT_DEPTH {
            return Err(format!(
                "{at}: agents use other agents as tools at most {MAX_AGENT_DEPTH} deep"
            ));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the model's credential (an ai one)"))?
            .to_owned();
        let optional = |name: &str| -> Result<Option<crate::template::Template>, String> {
            match s.get(name) {
                None | Some(Json::Null) => Ok(None),
                Some(Json::String(t)) if t.trim().is_empty() => Ok(None),
                Some(v) => self.template(v, &format!("{at}.{name}")).map(Some),
            }
        };
        let model = optional("model")?;
        let instructions = optional("instructions")?;
        let input = optional("input")?.ok_or_else(|| {
            format!("{at}.input: what the agent is to do in each run, as {{{{ value.message }}}}")
        })?;
        let tools = self.tools(s.get("tools"), at)?;
        let allow = match s.get("allow") {
            None | Some(Json::Null) => None,
            Some(Json::Array(list)) => {
                let mut names = Vec::with_capacity(list.len());
                for (i, name) in list.iter().enumerate() {
                    let name = name
                        .as_str()
                        .ok_or_else(|| format!("{at}.allow[{i}]: a tool's name"))?;
                    if !tools.iter().any(|t| t.spec.name == name) {
                        return Err(format!("{at}.allow[{i}]: {name:?} is not one of its tools"));
                    }
                    names.push(name.to_owned());
                }
                Some(names)
            }
            Some(_) => return Err(format!("{at}.allow: a list of the tools that may run")),
        };
        let temperature = match s.get("temperature") {
            None | Some(Json::Null) => None,
            Some(v) => Some(
                v.as_f64()
                    .filter(|t| (0.0..=2.0).contains(t))
                    .ok_or_else(|| format!("{at}.temperature: a number, 0 to 2"))?,
            ),
        };
        let defaults = Limits::default();
        let timeout = match text(s.get("timeout")) {
            None => defaults.timeout,
            Some(t) => duration(t)
                .filter(|d| *d >= Duration::from_secs(1) && *d <= MAX_TIMEOUT)
                .ok_or_else(|| format!("{at}.timeout: as 5m, at most 30m"))?,
        };
        let limits = Limits {
            max_turns: u32::try_from(count(s, "max_turns", at, u64::from(defaults.max_turns), (1, 50))?)
                .unwrap_or(defaults.max_turns),
            max_tool_calls: u32::try_from(count(
                s,
                "max_tool_calls",
                at,
                u64::from(defaults.max_tool_calls),
                (0, 100),
            )?)
            .unwrap_or(defaults.max_tool_calls),
            timeout,
            max_tokens: count(s, "max_tokens", at, defaults.max_tokens, (1, 200_000))?,
            max_total_tokens: count(
                s,
                "max_total_tokens",
                at,
                defaults.max_total_tokens,
                (1, 10_000_000),
            )?,
        };
        Ok(Kind::Agent(Box::new(Agent {
            credential,
            model,
            instructions,
            input,
            tools,
            allow,
            memory: self.memory(s.get("memory"), at)?,
            output: output(s.get("output"), at)?,
            limits,
            temperature,
        })))
    }

    fn tools(&mut self, source: Option<&Json>, at: &str) -> Result<Vec<ToolDef>, String> {
        let list = match source {
            None | Some(Json::Null) => return Ok(Vec::new()),
            Some(Json::Array(list)) => list,
            Some(_) => {
                return Err(format!(
                    "{at}.tools: a list of tools, each a name, a description and the step it runs"
                ));
            }
        };
        if list.len() > MAX_TOOLS {
            return Err(format!("{at}.tools: at most {MAX_TOOLS}"));
        }
        let mut names = HashSet::new();
        let mut out = Vec::with_capacity(list.len());
        for (i, tool) in list.iter().enumerate() {
            let place = format!("{at}.tools[{i}]");
            let t = settings(tool, &place)?;
            let name = text(t.get("name")).ok_or_else(|| format!("{place}.name: the tool's name"))?;
            if name.is_empty()
                || name.len() > 64
                || !name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                return Err(format!(
                    "{place}.name: letters, digits, '_' and '-', at most 64 (as order_status)"
                ));
            }
            if !names.insert(name.to_owned()) {
                return Err(format!("{place}: another tool has the name {name:?}"));
            }
            let place = format!("{place} ({name})");
            let description = text(t.get("description"))
                .map(str::trim)
                .filter(|d| !d.is_empty())
                .ok_or_else(|| {
                    format!("{place}.description: what the tool does, for the model to choose it by")
                })?;
            if description.chars().count() > MAX_DESCRIPTION {
                return Err(format!(
                    "{place}.description: at most {MAX_DESCRIPTION} characters"
                ));
            }
            out.push(self.tool(t, name, description, &place)?);
        }
        Ok(out)
    }

    /// One tool: its step, with the arguments it takes filled in where the step does not say.
    fn tool(
        &mut self,
        t: &Map<String, Json>,
        name: &str,
        description: &str,
        at: &str,
    ) -> Result<ToolDef, String> {
        let kinds: Vec<&String> = t
            .keys()
            .filter(|k| !matches!(k.as_str(), "name" | "description" | "parameters"))
            .collect();
        let known = |k: &str| {
            KINDS.contains(&k) || crate::apps::app(k).is_some() || crate::app_files::find(k).is_some()
        };
        let [kind] = kinds[..] else {
            return Err(format!(
                "{at}: one step it runs - http, javascript, python, run_workflow, agent, an app's action - beside its \
                 name, description and parameters"
            ));
        };
        if NOT_TOOLS.contains(&kind.as_str()) || !known(kind) {
            return Err(format!(
                "{at}.{kind}: not a step a tool runs (http, javascript, python, run_workflow, agent, an app's action, \
                 and the other steps that answer)"
            ));
        }
        self.count += 1;
        if self.count > MAX_STEPS {
            return Err(format!("more than {MAX_STEPS} steps"));
        }
        let mut body = t.get(kind.as_str()).cloned().unwrap_or(Json::Null);
        let mut fills = Vec::new();
        let mut parameters = t.get("parameters").cloned();
        match kind.as_str() {
            "agent" => {
                if let Some(b) = body.as_object_mut()
                    && !b.contains_key("input")
                {
                    b.insert("input".to_owned(), json!("{{ args.input }}"));
                    parameters.get_or_insert_with(|| {
                        json!({
                            "type": "object",
                            "properties": { "input": { "type": "string", "description": "What to do, in full" } },
                            "required": ["input"],
                        })
                    });
                }
            }
            "run_workflow" => {
                if let Some(b) = body.as_object_mut()
                    && !b.contains_key("input")
                {
                    b.insert("input".to_owned(), json!("args"));
                }
            }
            "web" => crate::web::tool_defaults(&mut body, &mut parameters), // web: the model gives the address
            other => {
                if let (Some(def), Some(b)) = (crate::app_files::find(other), body.as_object_mut())
                    && let Some((_, action)) = def.action(b.get("action").and_then(Json::as_str))
                {
                    // The inputs the step does not give are the model's to fill.
                    let mut properties = Map::new();
                    let mut required = Vec::new();
                    for input in &action.inputs {
                        if b.contains_key(&input.name) {
                            continue;
                        }
                        fills.push(input.name.clone());
                        let said = if input.description.trim().is_empty() {
                            input.label.clone()
                        } else {
                            input.description.clone()
                        };
                        properties.insert(
                            input.name.clone(),
                            json!({ "type": json_type(&input.kind), "description": said }),
                        );
                        if input.required {
                            required.push(input.name.clone());
                            b.insert(
                                input.name.clone(),
                                json!(format!("{{{{ args.{} }}}}", input.name)),
                            );
                        }
                    }
                    parameters.get_or_insert_with(
                        || json!({ "type": "object", "properties": properties, "required": required }),
                    );
                }
            }
        }
        let parameters = parameters.unwrap_or_else(|| json!({ "type": "object", "properties": {} }));
        check_schema(&parameters, &format!("{at}.parameters"))?;
        if parameters.get("type").and_then(Json::as_str) != Some("object") {
            return Err(format!(
                "{at}.parameters: a schema of type object - the arguments are a record of names and values"
            ));
        }
        let step_kind = self.kind(kind, &body, &format!("{at}.{kind}"))?;
        Ok(ToolDef {
            spec: ToolSpec {
                name: name.to_owned(),
                description: description.to_owned(),
                parameters,
            },
            does: ToolKind::Step {
                step: Box::new(Step {
                    name: name.to_owned(),
                    kind: step_kind,
                    attempts: 1,
                    retry_wait: Duration::from_secs(1),
                    continue_on_error: false,
                }),
                fills,
            },
        })
    }

    fn memory(&self, source: Option<&Json>, at: &str) -> Result<Option<MemoryDef>, String> {
        let at = format!("{at}.memory");
        let s = match source {
            None | Some(Json::Null | Json::Bool(false)) => return Ok(None),
            Some(Json::Object(s)) => s,
            Some(_) => {
                return Err(format!(
                    "{at}: off, or {{key: \"{{{{ value.chat_id }}}}\", window: 10}}"
                ));
            }
        };
        // vector stores: the store it is kept in (store, credential, table, create) beside its own settings.
        if let Some(k) = s.keys().find(|k| {
            !matches!(k.as_str(), "key" | "window" | "clear" | "forget_after")
                && !crate::agent::stores::SETTINGS.contains(&k.as_str())
        }) {
            return Err(format!(
                "{at}.{k}: not a setting of memory (key, window, clear, forget_after, store, credential, table, \
                 create)"
            ));
        }
        let key = match s.get("key") {
            Some(Json::String(k)) if !k.trim().is_empty() => {
                self.template(&json!(k), &format!("{at}.key"))?
            }
            _ => {
                return Err(format!(
                    "{at}.key: whose conversation it is, as {{{{ value.chat_id }}}}"
                ));
            }
        };
        let window = usize::try_from(count(s, "window", &at, 10, (1, MAX_WINDOW))?).unwrap_or(10);
        let forget_after = match text(s.get("forget_after")) {
            None => Duration::from_secs(30 * 86_400),
            Some(t) => duration(t)
                .filter(|d| *d >= Duration::from_secs(60) && *d <= MAX_FORGET)
                .ok_or_else(|| format!("{at}.forget_after: as 30d, from 1m to 365d"))?,
        };
        let clear = s
            .get("clear")
            .map(|c| self.expression(c, &format!("{at}.clear")))
            .transpose()?;
        Ok(Some(MemoryDef {
            key,
            window,
            clear,
            forget_after,
            store: crate::agent::stores::read(s, &at)?, // vector stores
        }))
    }
}

/// `text` (the default), `json`, or `{schema: ...}`.
fn output(source: Option<&Json>, at: &str) -> Result<Output, String> {
    let at = format!("{at}.output");
    match source {
        None | Some(Json::Null) => Ok(Output::Text),
        Some(Json::String(s)) if s == "text" => Ok(Output::Text),
        Some(Json::String(s)) if s == "json" => Ok(Output::Json(None)),
        Some(Json::Object(o)) => {
            if let Some(k) = o.keys().find(|k| k.as_str() != "schema") {
                return Err(format!("{at}.{k}: not a setting of output (schema)"));
            }
            let schema = o
                .get("schema")
                .ok_or_else(|| format!("{at}.schema: the JSON schema the answer matches"))?;
            check_schema(schema, &format!("{at}.schema"))?;
            Ok(Output::Json(Some(schema.clone())))
        }
        Some(_) => Err(format!("{at}: text, json, or {{schema: ...}}")),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use crate::model::{Kind, Workflow};

    fn agent_of(yaml: &str) -> crate::agent::Agent {
        let w = Workflow::parse(yaml).unwrap();
        match w.steps.into_iter().next().unwrap().kind {
            Kind::Agent(a) => *a,
            _ => panic!("not an agent"),
        }
    }

    fn wrong(yaml: &str) -> String {
        Workflow::parse(yaml).unwrap_err()
    }

    const BASE: &str = "name: a\nsteps:\n  - name: helper\n    agent:\n      credential: model\n      input: '{{ value.text }}'\n";

    #[test]
    fn an_agent_is_read_with_its_tools_memory_output_and_limits() {
        let a = agent_of(&format!(
            "{BASE}      tools:
        - name: order_status
          description: The status of an order.
          parameters: {{ type: object, properties: {{ id: {{ type: string }} }}, required: [id] }}
          http: {{ url: 'https://shop.example.com/orders/{{{{ args.id }}}}' }}
        - name: helper
          description: Another agent, for research.
          agent: {{ credential: model, instructions: You research. }}
        - name: total
          description: Adds two numbers.
          javascript: 'return args.a + args.b'
      allow: [order_status, total]
      memory: {{ key: '{{{{ value.chat }}}}', window: 4, clear: \"value.text == '/reset'\" }}
      output: {{ schema: {{ type: object, properties: {{ answer: {{ type: string }} }} }} }}
      max_turns: 6
      timeout: 2m
"
        ));
        assert_eq!(a.tools.len(), 3);
        assert_eq!(
            a.tools[1].spec.parameters["required"][0], "input",
            "another agent takes its task"
        );
        assert_eq!(
            a.allow.as_deref(),
            Some(&["order_status".to_owned(), "total".to_owned()][..])
        );
        let m = a.memory.unwrap();
        assert_eq!((m.window, m.forget_after.as_secs()), (4, 30 * 86_400));
        assert!(matches!(a.output, crate::agent::Output::Json(Some(_))));
        assert_eq!(
            (
                a.limits.max_turns,
                a.limits.timeout.as_secs(),
                a.limits.max_tool_calls
            ),
            (6, 120, 20)
        );
    }

    #[test]
    fn an_app_action_as_a_tool_leaves_the_inputs_not_given_to_the_model() {
        let a = agent_of(&format!(
            "{BASE}      tools:
        - name: add_row
          description: Adds a row to the orders sheet.
          google_sheets: {{ action: add_row, credential: sheets, spreadsheet_id: abc }}
"
        ));
        let crate::agent::ToolKind::Step { fills, .. } = &a.tools[0].does;
        assert!(!fills.contains(&"spreadsheet_id".to_owned()), "{fills:?}");
        let p = &a.tools[0].spec.parameters;
        assert!(p["properties"].get("spreadsheet_id").is_none());
        assert!(!p["required"].as_array().unwrap().is_empty(), "{p}");
    }

    #[test]
    fn a_mistake_in_an_agent_is_said_with_where_it_is() {
        assert!(wrong("name: a\nsteps:\n  - agent: { input: hi }\n").contains("agent.credential"));
        assert!(wrong("name: a\nsteps:\n  - agent: { credential: m }\n").contains("agent.input"));
        let tool = |t: &str| wrong(&format!("{BASE}      tools:\n        - {t}\n"));
        assert!(
            tool("{ name: 'a b', description: x, http: { url: 'https://x' } }").contains(".name: letters")
        );
        assert!(tool("{ name: a, http: { url: 'https://x' } }").contains(".description:"));
        assert!(tool("{ name: a, description: x, wait: 1s }").contains("not a step a tool runs"));
        assert!(tool("{ name: a, description: x }").contains("one step it runs"));
        assert!(
            tool("{ name: a, description: x, parameters: { type: string }, http: { url: 'https://x' } }")
                .contains("a schema of type object")
        );
        assert!(wrong(&format!("{BASE}      allow: [nope]\n")).contains("\"nope\" is not one of its tools"));
        assert!(
            wrong(&format!("{BASE}      max_turns: 99\n")).contains("max_turns: a whole number, 1 to 50")
        );
        assert!(wrong(&format!("{BASE}      memory: {{ window: 3 }}\n")).contains("memory.key"));
        assert!(wrong(&format!("{BASE}      output: xml\n")).contains("output: text, json"));
        assert!(wrong(&format!("{BASE}      timeout: 2h\n")).contains("at most 30m"));
        let deep = "{ name: d, description: x, agent: { credential: m, input: x, tools: [{ name: e, description: x, \
                    agent: { credential: m, input: x, tools: [{ name: f, description: x, agent: { credential: m, \
                    input: x } }] } }] } }";
        assert!(tool(deep).contains("at most 3 deep"), "{}", tool(deep));
    }
}
