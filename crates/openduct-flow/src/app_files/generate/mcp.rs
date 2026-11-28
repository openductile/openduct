// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An app whose actions are the tools of a vendor's MCP server: each tool an action that calls it (see
//! [`crate::app_files`]'s `mcp:` actions), its inputs from the tool's input schema, its output from its output
//! schema, a tool the server marks as only reading not a write. The app signs in with a token, as a bearer token.

use serde_json::{Map, Value as Json, json};

use super::rank::{cut, sentence, snake, words};
use super::schema::{Purpose, kind, properties, sample};
use super::{Generated, Proposed, ProposedSignIn, emit};
use crate::mcp::ToolInfo;

/// The most inputs an action is given.
const MAX_INPUTS: usize = 40;

/// What to make an app from an MCP server's tools with.
#[derive(Debug, Clone, Default)]
pub struct McpOptions {
    pub id: String,
    pub name: Option<String>,
    pub category: Option<String>,
    /// The server's MCP address.
    pub url: String,
    /// How many tools at most (by default all, up to the most an app has).
    pub actions: usize,
    /// Exactly these actions (by their proposed names).
    pub chosen: Option<Vec<String>>,
    pub docs: Option<String>,
    pub made_on: String,
}

fn is_write(t: &ToolInfo) -> bool {
    t.annotations.get("readOnlyHint").and_then(Json::as_bool) != Some(true)
}

/// One tool as an action: its definition and its recorded test; what of it an action cannot take goes in
/// `not_done`.
fn tool_action(action: &str, tool: &ToolInfo, not_done: &mut Vec<String>) -> (Json, (String, String)) {
    let root = json!({});
    let label = tool.title.clone().filter(|t| !t.trim().is_empty()).map_or_else(
        || words(action),
        |t| sentence(&t, 80).trim_end_matches('.').to_owned(),
    );
    let description = sentence(&tool.description, 300);
    let mut inputs = Vec::new();
    let mut send = Map::new();
    let mut given = Map::new();
    let mut arguments = Map::new();
    for (field, schema, required) in properties(&root, &tool.input_schema, Purpose::Request) {
        if inputs.len() >= MAX_INPUTS {
            not_done.push(format!(
                "{action}: the argument {field} (an action takes at most {MAX_INPUTS} inputs)"
            ));
            continue;
        }
        let mut input = cut(&snake(&field));
        if input.is_empty() || matches!(input.as_str(), "credential" | "action") {
            input = format!("{input}_field").trim_start_matches('_').to_owned();
        }
        while inputs.iter().any(|x: &Json| x["name"] == json!(input)) {
            input.push('_');
        }
        let about = schema
            .get("description")
            .and_then(Json::as_str)
            .unwrap_or_default();
        let short = sentence(about, 80).trim_end_matches('.').to_owned();
        let mut entry = Map::new();
        entry.insert("name".to_owned(), json!(input));
        entry.insert(
            "label".to_owned(),
            json!(if short.is_empty() { words(&input) } else { short }),
        );
        if required {
            entry.insert("required".to_owned(), json!(true));
        }
        let k = kind(&root, &schema);
        if k != "text" {
            entry.insert("kind".to_owned(), json!(k));
        }
        inputs.push(Json::Object(entry));
        send.insert(
            field.clone(),
            json!(format!(
                "{{{{ {} }}}}",
                super::pages::cel("input", &[input.clone()])
            )),
        );
        if required {
            let v = sample(&root, &schema, &field, Purpose::Request);
            given.insert(input, v.clone());
            arguments.insert(field, v);
        }
    }
    let output: Map<String, Json> = tool
        .output_schema
        .as_object()
        .map(|_| {
            properties(&root, &tool.output_schema, Purpose::Answer)
                .into_iter()
                .take(crate::app_files::quality::MAX_OUTPUT)
                .map(|(f, s, _)| (f, json!(kind(&root, &s))))
                .collect()
        })
        .unwrap_or_default();
    let mut def = Map::new();
    def.insert("label".to_owned(), json!(label));
    if !description.is_empty() {
        def.insert("description".to_owned(), json!(description));
    }
    if is_write(tool) {
        def.insert("write".to_owned(), json!(true));
    }
    def.insert("mcp".to_owned(), json!(tool.name));
    if !inputs.is_empty() {
        def.insert("inputs".to_owned(), Json::Array(inputs));
        def.insert("send".to_owned(), Json::Object(send));
    }
    if !output.is_empty() {
        def.insert("output".to_owned(), Json::Object(output));
    }
    // The answer a tool with an output schema gives as structured content; another, as text.
    let (answer, result) = if tool.output_schema.is_object() {
        let s = sample(&root, &tool.output_schema, "", Purpose::Answer);
        (json!({ "structuredContent": s }), s)
    } else {
        (
            json!({ "content": [{ "type": "text", "text": "done" }] }),
            json!("done"),
        )
    };
    let mut recording = Map::new();
    recording.insert(
        "description".to_owned(),
        json!(format!("{label}, made from the tool's schemas")),
    );
    recording.insert("action".to_owned(), json!(action));
    recording.insert("credential".to_owned(), json!({ "token": "test-token" }));
    if !given.is_empty() {
        recording.insert("input".to_owned(), Json::Object(given));
    }
    recording.insert(
        "call".to_owned(),
        json!({ "tool": tool.name, "arguments": arguments, "headers": { "authorization": "Bearer test-token" } }),
    );
    recording.insert("answer".to_owned(), answer);
    recording.insert("result".to_owned(), result);
    (
        Json::Object(def),
        (
            format!("{action}.yaml"),
            emit::file(
                &Json::Object(recording),
                &[format!("Made by `openduct app from-mcp`: {label}.")],
            ),
        ),
    )
}

/// Makes an app from the tools an MCP server listed.
pub fn from_mcp(tools: &[ToolInfo], options: &McpOptions) -> Result<Generated, String> {
    if !super::super::definition::named(&options.id) {
        return Err(format!(
            "{:?}: an app's id is lowercase letters, digits and '_', at most 40, starting with a letter",
            options.id
        ));
    }
    let url = reqwest::Url::parse(&options.url).map_err(|e| format!("{:?}: {e}", options.url))?;
    if !super::super::definition::secure_address(&url) {
        return Err(format!(
            "{}: an MCP server is reached at an https:// address",
            options.url
        ));
    }
    let host = url.host_str().unwrap_or_default().to_owned();
    if tools.is_empty() {
        return Err("the server lists no tools".to_owned());
    }
    let name = options
        .name
        .clone()
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| words(&options.id));
    let most = if options.actions == 0 {
        super::MAX_ACTIONS
    } else {
        options.actions.min(super::MAX_ACTIONS)
    };
    // Names, in the server's order.
    let mut named: Vec<(String, &ToolInfo)> = Vec::new();
    for t in tools {
        let base = cut(&snake(&t.name));
        let base = if base.is_empty() { "tool".to_owned() } else { base };
        let mut n = base.clone();
        let mut i = 2;
        while named.iter().any(|(m, _)| m == &n) {
            n = format!("{}_{i}", base.get(..base.len().min(36)).unwrap_or(&base));
            i += 1;
        }
        named.push((n, t));
    }
    let picked: Vec<usize> = (0..named.len())
        .filter(|&i| {
            options
                .chosen
                .as_ref()
                .is_none_or(|c| named.get(i).is_some_and(|(n, _)| c.contains(n)))
        })
        .take(most)
        .collect();
    if picked.is_empty() {
        return Err("none of the tools chosen is one the server lists".to_owned());
    }
    let mut actions = Map::new();
    let mut tests = Vec::new();
    let mut not_done = Vec::new();
    for &i in &picked {
        let Some((action, tool)) = named.get(i) else {
            continue;
        };
        let (def, test) = tool_action(action, tool, &mut not_done);
        actions.insert(action.clone(), def);
        tests.push(test);
    }
    // A refusal, in the server's words.
    if let Some((first, _)) = picked.first().and_then(|&i| named.get(i)) {
        let text = tests
            .iter()
            .find(|(f, _)| f == &format!("{first}.yaml"))
            .map(|(_, t)| t.clone());
        if let Some(mut r) = text.and_then(|t| crate::yaml::to_json(&t).ok())
            && let Some(o) = r.as_object_mut()
        {
            o.insert(
                "description".to_owned(),
                json!(format!("{name} refuses {first}: the step fails in its words")),
            );
            o.insert(
                "answer".to_owned(),
                json!({ "content": [{ "type": "text", "text": "not allowed" }], "isError": true }),
            );
            o.remove("result");
            o.insert(
                "error".to_owned(),
                json!(format!("{name} refused it: not allowed")),
            );
            tests.push((
                "refused.yaml".to_owned(),
                emit::file(&r, &["Made by `openduct app from-mcp`: a refusal.".to_owned()]),
            ));
        }
    }
    if named.len() > picked.len() {
        not_done.push(format!(
            "{} more tools of the server (made with {}: --actions or the studio's list chooses others)",
            named.len() - picked.len(),
            picked.len()
        ));
    }
    let docs = options.docs.clone().filter(|d| d.starts_with("https://"));
    let mut app = Map::new();
    app.insert("app".to_owned(), json!(options.id));
    app.insert("name".to_owned(), json!(name));
    app.insert(
        "category".to_owned(),
        json!(options.category.clone().unwrap_or_else(|| "other".to_owned())),
    );
    app.insert(
        "description".to_owned(),
        json!(format!("The tools of {name}'s MCP server.")),
    );
    if let Some(d) = &docs {
        app.insert("docs".to_owned(), json!(d));
    }
    app.insert("hosts".to_owned(), json!([host]));
    app.insert("base_url".to_owned(), json!(options.url));
    app.insert(
        "sign_in".to_owned(),
        json!([{
            "id": options.id, "label": "An access token",
            "fields": [{ "name": "token", "label": "The access token the server takes", "secret": true }],
            "headers": { "Authorization": "Bearer {{ credential.token }}" },
        }]),
    );
    app.insert("actions".to_owned(), Json::Object(actions.clone()));
    let comment =
        vec![
        format!("{name}: made by `openduct app from-mcp` from the tools of {}.", options.url),
        "Checked as made. Change what you like, then run `openduct app check` and `openduct app test` again."
            .to_owned(),
    ];
    let app_yaml = emit::file(&Json::Object(app), &comment);
    super::checked(&app_yaml, &options.id)?;
    let mut readme = format!(
        "# {name}\n\nThe tools of {name}'s MCP server (<{}>), as an app: each tool an action. Made by `openduct app \
         from-mcp`{}.\n\n## Signing in\n\n- **An access token** (`{}`): a token the server takes as \
         `Authorization: Bearer`, from {name}'s pages for developers{}.\n\n## Actions\n\n",
        options.url,
        if options.made_on.is_empty() {
            String::new()
        } else {
            format!(" on {}", options.made_on)
        },
        options.id,
        docs.as_ref().map_or_else(String::new, |d| format!(" (<{d}>)")),
    );
    for (a, def) in &actions {
        let label = def["label"].as_str().unwrap_or_default();
        let what = def["description"]
            .as_str()
            .map_or_else(String::new, |d| format!(": {}", d.trim_end_matches('.')));
        readme.push_str(&format!("- **{label}** (`{a}`){what}.\n"));
    }
    readme.push_str(
        "\n## Not done\n\n- Sign-in at the vendor (OAuth) for the server: give a token it takes.\n",
    );
    for n in &not_done {
        readme.push_str(&format!("- {n}\n"));
    }
    readme.push_str(&format!(
        "\n## Tested\n\nRecorded calls made from the tools' schemas (`tests/`). Not run against the live server.\n\n```sh\n\
         openduct app check apps/{id}\nopenduct app test apps/{id}\n```\n",
        id = options.id
    ));
    let proposed = named
        .iter()
        .enumerate()
        .map(|(i, (n, t))| Proposed {
            name: n.clone(),
            label: t.title.clone().unwrap_or_else(|| words(n)),
            method: "MCP".to_owned(),
            path: t.name.clone(),
            score: 0,
            chosen: picked.contains(&i),
            left_out: None,
        })
        .collect();
    Ok(Generated {
        id: options.id.clone(),
        name,
        app_yaml,
        readme,
        tests,
        proposed,
        sign_ins: vec![ProposedSignIn {
            id: options.id.clone(),
            label: "An access token".to_owned(),
            kind: "a bearer token".to_owned(),
            chosen: true,
        }],
        not_done,
    })
}
