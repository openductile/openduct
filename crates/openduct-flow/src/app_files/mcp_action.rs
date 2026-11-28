// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app's action that calls a tool of the app's MCP server instead of making a request of its own: what
//! `openduct app from-mcp` writes. The app's `base_url` is the server's MCP address; the action names the tool and,
//! like any action, its inputs:
//!
//! ```yaml
//! base_url: https://mcp.tasks.example/mcp
//! sign_in:
//!   - { id: tasks, label: An access token, fields: [{ name: token, label: The token, secret: true }],
//!       headers: { Authorization: "Bearer {{ credential.token }}" } }
//! actions:
//!   add_task:
//!     label: Add a task
//!     write: true                     # the tool changes something: a preview shows the call and does not send it
//!     mcp: add_task                   # the tool's name on the server, as the server spells it
//!     inputs: [{ name: title, label: Title, required: true }]
//!     send: { title: "{{ input.title }}" }   # the tool's arguments (by default, the inputs as they are)
//! ```
//!
//! The call goes through the MCP client every `mcp` step uses (so through the run's effects: the private-network
//! rule, test runs and previews hold), signed in with the one header (or user name and password) the sign-in
//! makes. Its result is the tool's answer: its structured content, else its text read as JSON, else its text - or,
//! with `result:`, an expression over that as `value`. An answer that is an error fails the step in the tool's words.
//!
//! A recording of such an action says what the tool was called with and what it answered:
//!
//! ```yaml
//! action: add_task
//! credential: { token: test-token }
//! input: { title: Call Ann }
//! call: { tool: add_task, arguments: { title: Call Ann }, headers: { authorization: Bearer test-token } }
//! answer: { structuredContent: { id: t1 } }    # or content: [{ type: text, text: ... }], isError: true
//! result: { id: t1 }                            # or error: words the refusal holds
//! ```

use std::collections::BTreeMap;

use openduct_expr::{Env, Value};
use serde::Deserialize;
use serde_json::{Value as Json, json};

use super::definition::{Action, AppDef, SignIn};
use crate::apps::Credential;
use crate::mcp::{Reached, Server, Session, ToolAnswer};
use crate::run::Effects;

/// The longest name of a tool.
const MAX_TOOL: usize = 128;
/// The longest refusal shown from a tool's answer.
const SHOWN: usize = 300;

/// An action's method: none for one that calls a tool (its calls are MCP's own POSTs), else as written.
pub(super) fn method_of(mcp: Option<&str>, method: &str, at: &str) -> Result<String, String> {
    let Some(tool) = mcp else {
        return Ok(method.to_ascii_uppercase());
    };
    if tool.trim().is_empty() || tool.len() > MAX_TOOL || tool.chars().any(char::is_control) {
        return Err(format!(
            "{at}.mcp: the name of the server's tool, of at most {MAX_TOOL} characters"
        ));
    }
    if !method.is_empty() && !method.eq_ignore_ascii_case("POST") {
        return Err(format!(
            "{at}.method: an action that calls an MCP tool names no method"
        ));
    }
    Ok("POST".to_owned())
}

/// Whether an app whose actions call MCP tools can: those actions name nothing a request would (a path, a query,
/// pages), and its sign-ins sign a call with one header, or a user name and password, alone.
pub(super) fn check_app(
    sign_ins: &[SignIn],
    actions: &[(String, Action)],
    at: &dyn Fn(&str) -> String,
) -> Result<(), String> {
    let calling: Vec<&(String, Action)> = actions.iter().filter(|(_, a)| a.mcp.is_some()).collect();
    if calling.is_empty() {
        return Ok(());
    }
    for (name, a) in &calling {
        let path = super::engine::render(&a.path, &[]).unwrap_or_else(|_| "?".to_owned());
        let request_only = !path.is_empty()
            || !a.query.is_empty()
            || !a.headers.is_empty()
            || a.items.is_some()
            || a.next.is_some()
            || !a.by.is_empty();
        if request_only {
            return Err(at(&format!(
                "actions.{name}: an action that calls an MCP tool has no path, query, headers, items, next or by"
            )));
        }
    }
    for s in sign_ins {
        let ways = s.headers.len() + usize::from(s.basic.is_some());
        if ways > 1 || !s.query.is_empty() || s.url_field.is_some() {
            return Err(at(&format!(
                "sign_in {}: an app that calls MCP tools signs in with one header, or basic, alone",
                s.id
            )));
        }
    }
    Ok(())
}

/// The names an action's templates read: its inputs and the credential's fields.
fn names(sign_in: &SignIn, credential: &Credential, inputs: &Value) -> Vec<(String, Value)> {
    vec![
        ("input".to_owned(), inputs.clone()),
        (
            "credential".to_owned(),
            super::engine::credential_value(sign_in, credential),
        ),
        ("page".to_owned(), Value::Null),
    ]
}

/// The tool's arguments: `send` worked out, or the inputs as they are; fields left empty are left out.
pub(super) fn arguments(
    app: &AppDef,
    action: &Action,
    credential: &Credential,
    inputs: &Value,
) -> Result<Json, String> {
    let sign_in = super::engine::sign_in_of(app, credential)?;
    for input in action.inputs.iter().filter(|i| i.required) {
        let given = match inputs {
            Value::Record(r) => r.get(&input.name),
            _ => None,
        };
        let empty = match given {
            None | Some(Value::Null) => true,
            Some(Value::Text(t)) => t.trim().is_empty(),
            Some(_) => false,
        };
        if empty {
            return Err(format!(
                "{}'s {} needs {} ({}): it came empty",
                app.name, action.label, input.name, input.label
            ));
        }
    }
    let value = match &action.send {
        Some(tree) => super::engine::tree_value(tree, &names(sign_in, credential, inputs))?,
        None => inputs.clone(),
    };
    let mut json: Json = serde_json::from_str(&value.to_json()).unwrap_or(Json::Null);
    super::engine::without_nulls(&mut json);
    match json {
        Json::Null => Ok(json!({})),
        record @ Json::Object(_) => Ok(record),
        _ => Err(format!(
            "{}'s {}: a tool's arguments are a record of named values",
            app.name, action.label
        )),
    }
}

/// The server's address (the app's own, on its hosts) and the credential a call is signed with: the one header
/// the sign-in makes, or its user name and password.
fn server(app: &AppDef, action: &Action, credential: &Credential) -> Result<Server, String> {
    let sign_in = super::engine::sign_in_of(app, credential)?;
    let names = names(sign_in, credential, &Value::Null);
    let base = sign_in.base.as_ref().unwrap_or(&app.base);
    let url = super::engine::render(base, &names)?;
    let parsed =
        reqwest::Url::parse(&url).map_err(|e| format!("{}: {url:?} is not an address: {e}", app.name))?;
    let host = parsed.host_str().unwrap_or_default();
    if !app.hosts.iter().any(|h| super::definition::host_matches(h, host)) {
        return Err(format!("{}: {host} is not one of the app's hosts", app.name));
    }
    let signed = if credential.kind == "test" {
        Credential {
            kind: "test".to_owned(),
            fields: BTreeMap::new(),
        }
    } else if let Some((user, password)) = &sign_in.basic {
        let fields = [
            ("username".to_owned(), super::engine::render(user, &names)?),
            ("password".to_owned(), super::engine::render(password, &names)?),
        ];
        Credential {
            kind: "basic".to_owned(),
            fields: fields.into_iter().collect(),
        }
    } else if let Some((name, value)) = sign_in.headers.first() {
        let fields = [
            ("name".to_owned(), name.to_ascii_lowercase()),
            ("value".to_owned(), super::engine::render(value, &names)?),
        ];
        Credential {
            kind: "header".to_owned(),
            fields: fields.into_iter().collect(),
        }
    } else {
        // A server that needs no sign-in: nothing is added to the call.
        return Ok(Server {
            url,
            credential: None,
            timeout: action.timeout,
        });
    };
    Ok(Server {
        url,
        credential: Some(signed),
        timeout: action.timeout,
    })
}

/// What a tool's answer gives the step: its value (or `result` worked out over it), or why it failed.
pub(super) fn result_of(app: &AppDef, action: &Action, answer: &ToolAnswer) -> Result<Value, String> {
    if answer.is_error {
        let said: String = answer.text.chars().take(SHOWN).collect();
        return Err(if said.trim().is_empty() {
            format!("{} refused it, saying nothing more", app.name)
        } else {
            format!("{} refused it: {said}", app.name)
        });
    }
    let value = Value::from_json(&answer.value());
    match &action.result {
        Some(r) => r.evaluate(&Env {
            value: &value,
            message: &Value::Null,
            names: &[],
        }),
        None => Ok(value),
    }
}

/// Runs an action that calls a tool: the step's result, or (a test run, a preview of a tool that changes
/// something) the call that would have been made.
pub(crate) async fn run(
    effects: &dyn Effects,
    app: &AppDef,
    action: &Action,
    credential: &Credential,
    inputs: &Value,
) -> Result<Value, String> {
    let tool = action.mcp.as_deref().unwrap_or_default();
    let arguments = arguments(app, action, credential, inputs)?;
    let mut session = Session::new(effects, server(app, action, credential)?);
    let reached = session.call(tool, &arguments, !action.write).await;
    session.close().await;
    match reached? {
        Reached::NotSent(shown) => Ok(shown),
        Reached::Answered(answer) => result_of(app, action, &answer),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Recorded {
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    action: String,
    #[serde(default)]
    sign_in: Option<String>,
    #[serde(default)]
    credential: BTreeMap<String, String>,
    #[serde(default)]
    input: Option<Json>,
    call: Call,
    answer: Answered,
    #[serde(default)]
    result: Option<Json>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Call {
    tool: String,
    #[serde(default)]
    arguments: Option<Json>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Answered {
    #[serde(default)]
    content: Vec<Json>,
    #[serde(default, rename = "structuredContent")]
    structured: Option<Json>,
    #[serde(default, rename = "isError")]
    is_error: bool,
}

/// Runs a recording of an action that calls a tool: the tool, its arguments and the header the call is signed
/// with as recorded, and the result (or the refusal) as recorded.
pub(super) fn check_recorded(app: &AppDef, json: Json, source: &str) -> Result<(), String> {
    let t: Recorded = serde_json::from_value(json).map_err(|e| format!("{source}: {e}"))?;
    let (_, action) = app
        .action(Some(&t.action))
        .ok_or_else(|| format!("{source}: action: {} has no action {}", app.name, t.action))?;
    let Some(tool) = &action.mcp else {
        return Err(format!(
            "{source}: call: {} makes a request, not a tool's call: record its exchanges",
            t.action
        ));
    };
    let kind = match &t.sign_in {
        Some(s) if app.sign_in(s).is_none() => {
            return Err(format!("{source}: sign_in: {} has no sign-in {s}", app.name));
        }
        Some(s) => s.clone(),
        None => app.sign_ins.first().map(|s| s.id.clone()).unwrap_or_default(),
    };
    let credential = Credential {
        kind,
        fields: t.credential.clone(),
    };
    let input = t.input.clone().unwrap_or_else(|| json!({}));
    if let Json::Object(given) = &input {
        for k in given.keys() {
            if !action.inputs.iter().any(|i| &i.name == k) {
                return Err(format!("{source}: input.{k}: not an input of {}", t.action));
            }
        }
    }
    let refused = |e: String| -> Result<(), String> {
        match &t.error {
            Some(want) if e.contains(want.as_str()) => Ok(()),
            Some(want) => Err(format!(
                "{source}: it was refused \"{e}\", the recording says \"{want}\""
            )),
            None => Err(format!("{source}: {e}")),
        }
    };
    let arguments = match arguments(app, action, &credential, &Value::from_json(&input)) {
        Ok(a) => a,
        Err(e) => return refused(e),
    };
    if &t.call.tool != tool {
        return Err(format!(
            "{source}: call.tool: it calls {tool}, the recording has {}",
            t.call.tool
        ));
    }
    if let Some(want) = &t.call.arguments
        && !super::recorded::same(&arguments, want)
    {
        return Err(format!(
            "{source}: call.arguments: it sent {arguments}, the recording has {want}"
        ));
    }
    let signed = server(app, action, &credential)?;
    for (name, want) in &t.call.headers {
        let got = signed
            .credential
            .as_ref()
            .and_then(|c| match (c.kind.as_str(), c.fields.get("name")) {
                ("header", Some(n)) if n.eq_ignore_ascii_case(name) => c.fields.get("value").cloned(),
                _ => None,
            });
        if got.as_deref() != Some(want.as_str()) {
            return Err(format!(
                "{source}: call.headers.{name}: it sent {}, the recording has {want}",
                got.as_deref().unwrap_or("nothing")
            ));
        }
    }
    let text = t
        .answer
        .content
        .iter()
        .filter(|c| c["type"] == "text")
        .filter_map(|c| c["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let answer = ToolAnswer {
        content: t.answer.content.clone(),
        text,
        structured: t.answer.structured.clone().filter(|s| !s.is_null()),
        is_error: t.answer.is_error,
    };
    let result = match result_of(app, action, &answer) {
        Ok(r) => r,
        Err(e) => return refused(e),
    };
    if let Some(want) = &t.error {
        return Err(format!(
            "{source}: it succeeded; the recording says it is refused (\"{want}\")"
        ));
    }
    if let Some(want) = &t.result {
        let got: Json = serde_json::from_str(&result.to_json()).unwrap_or(Json::Null);
        if !super::recorded::same(&got, want) {
            return Err(format!(
                "{source}: result: it gave {got}, the recording has {want}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    const TASKS: &str = r#"
app: tasks_mcp_test
name: Tasks
hosts: [mcp.tasks.example]
base_url: https://mcp.tasks.example/mcp
sign_in:
  - id: tasks_mcp_test
    label: An access token
    fields: [{ name: token, label: The token, secret: true }]
    headers: { Authorization: "Bearer {{ credential.token }}" }
actions:
  add_task:
    label: Add a task
    write: true
    mcp: add_task
    inputs:
      - { name: title, label: Title, required: true }
      - { name: due, label: Due }
    send: { title: "{{ input.title }}", due: "{{ input.due }}" }
    output: { id: text, title: text }
  find_tasks:
    label: Find tasks
    mcp: tasks.search
    inputs: [{ name: q, label: Words }]
    result: value.tasks
"#;

    fn app() -> AppDef {
        AppDef::parse(TASKS, "tasks.yaml").unwrap()
    }

    #[test]
    fn a_tool_s_arguments_come_from_the_inputs_and_empty_ones_are_left_out() {
        let app = app();
        let (_, add) = app.action(Some("add_task")).unwrap();
        let credential = Credential {
            kind: "tasks_mcp_test".to_owned(),
            fields: [("token".to_owned(), "t1".to_owned())].into_iter().collect(),
        };
        let got = arguments(
            &app,
            add,
            &credential,
            &Value::from_json(&json!({ "title": "Call Ann" })),
        )
        .unwrap();
        assert_eq!(got, json!({ "title": "Call Ann" }));
        let e = arguments(&app, add, &credential, &Value::from_json(&json!({}))).unwrap_err();
        assert!(e.contains("needs title"), "{e}");
        // Signed with the one header the sign-in makes.
        let s = server(&app, add, &credential).unwrap();
        let c = s.credential.unwrap();
        assert_eq!(
            (
                c.kind.as_str(),
                c.fields["name"].as_str(),
                c.fields["value"].as_str()
            ),
            ("header", "authorization", "Bearer t1")
        );
        assert_eq!(add.output.len(), 2);
    }

    #[test]
    fn a_tool_s_answer_is_its_structured_content_its_json_or_its_text() {
        let app = app();
        let (_, find) = app.action(Some("find_tasks")).unwrap();
        let answer = ToolAnswer {
            content: vec![json!({ "type": "text", "text": "{\"tasks\": [1, 2]}" })],
            text: "{\"tasks\": [1, 2]}".to_owned(),
            structured: None,
            is_error: false,
        };
        assert_eq!(result_of(&app, find, &answer).unwrap().to_json(), "[1,2]");
        let failed = ToolAnswer {
            is_error: true,
            text: "no such list".to_owned(),
            ..answer
        };
        assert_eq!(
            result_of(&app, find, &failed).unwrap_err(),
            "Tasks refused it: no such list"
        );
    }

    #[test]
    fn a_definition_that_mixes_a_tool_with_a_request_is_refused() {
        let wrong = |from: &str, to: &str| AppDef::parse(&TASKS.replace(from, to), "tasks.yaml").unwrap_err();
        assert!(wrong("    mcp: add_task\n", "    mcp: add_task\n    path: /tasks\n").contains("no path"));
        assert!(
            wrong("    mcp: add_task\n", "    mcp: add_task\n    method: GET\n").contains("names no method")
        );
        assert!(
            wrong(
                "headers: { Authorization: \"Bearer {{ credential.token }}\" }",
                "headers: { Authorization: \"Bearer {{ credential.token }}\", X-Other: \"1\" }"
            )
            .contains("one header")
        );
        assert!(wrong("    mcp: add_task\n", "    mcp: \"\"\n").contains("the name of the server's tool"));
    }

    #[test]
    fn a_recording_of_a_tool_s_call_holds_or_says_what_differs() {
        let app = app();
        let good = r#"
action: add_task
credential: { token: t1 }
input: { title: Call Ann }
call: { tool: add_task, arguments: { title: Call Ann }, headers: { authorization: Bearer t1 } }
answer: { structuredContent: { id: k1, title: Call Ann } }
result: { id: k1, title: Call Ann }
"#;
        let check = |text: &str| check_recorded(&app, crate::yaml::to_json(text).unwrap(), "r.yaml");
        check(good).unwrap();
        let e = check(&good.replace("arguments: { title: Call Ann }", "arguments: { title: Other }"))
            .unwrap_err();
        assert!(e.contains("call.arguments"), "{e}");
        let e = check(&good.replace("Bearer t1 }", "Bearer t2 }")).unwrap_err();
        assert!(e.contains("call.headers.authorization"), "{e}");
        let refused = r#"
action: find_tasks
credential: { token: t1 }
call: { tool: tasks.search }
answer: { content: [{ type: text, text: not allowed }], isError: true }
error: not allowed
"#;
        check(refused).unwrap();
    }
}
