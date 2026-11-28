// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The generator from end to end: a description in, an app out whose definition checks and whose recorded tests
//! hold - every way to the next page among them.

#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

use super::*;
use crate::app_files::{AppDef, recorded};

/// A small task service, in OpenAPI 3.1, with each kind of page and sign-in.
pub(crate) const TASKS: &str = r##"
openapi: 3.1.0
info: { title: Tasks API, version: "2.0", description: "Tasks and projects of a team. More words follow." }
externalDocs: { url: "https://tasks.example/docs/api" }
servers: [{ url: "https://api.tasks.example/v2" }]
security: [{ token: [] }]
components:
  securitySchemes:
    token: { type: http, scheme: bearer }
    code:
      type: oauth2
      flows:
        authorizationCode: { authorizationUrl: "https://tasks.example/oauth/authorize", tokenUrl: "https://tasks.example/oauth/token", scopes: { tasks: Tasks } }
    oidc: { type: openIdConnect, openIdConnectUrl: "https://tasks.example/.well-known/openid-configuration" }
  schemas:
    Task:
      type: object
      required: [title]
      properties:
        id: { type: string, readOnly: true }
        title: { type: string, description: "What is to be done." }
        due: { type: [string, "null"], format: date }
        done: { type: boolean }
        tags: { type: array, items: { type: string } }
paths:
  /tasks:
    get:
      summary: List tasks
      description: Every task of the team, newest first.
      parameters:
        - { name: cursor, in: query, schema: { type: string } }
        - { name: limit, in: query, schema: { type: integer, maximum: 200 } }
        - { name: status, in: query, description: "Only tasks of this status.", schema: { type: string, enum: [open, done] } }
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  data: { type: array, items: { $ref: "#/components/schemas/Task" } }
                  meta: { type: object, properties: { next_cursor: { type: [string, "null"] } } }
    post:
      summary: Add a task
      security: [{ code: [tasks] }]
      requestBody:
        required: true
        content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } }
      responses: { "201": { content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } } } }
  /tasks/{task_id}:
    parameters: [{ name: task_id, in: path, required: true, description: "The task's id.", schema: { type: string } }]
    get:
      summary: Look up a task
      responses: { "200": { content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } } } }
    patch:
      summary: Change a task
      requestBody: { content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } } }
      responses: { "200": { content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } } } }
    delete:
      summary: Remove a task
      responses: { "204": { description: Removed } }
  /tasks/{task_id}/complete:
    post:
      summary: Mark a task done
      parameters: [{ name: task_id, in: path, required: true, schema: { type: string } }]
      responses: { "200": { content: { application/json: { schema: { $ref: "#/components/schemas/Task" } } } } }
  /projects:
    get:
      summary: List projects
      parameters:
        - { name: page, in: query, schema: { type: integer } }
        - { name: per_page, in: query, schema: { type: integer, maximum: 50 } }
      responses:
        "200":
          content:
            application/json:
              schema: { type: array, items: { type: object, properties: { id: { type: integer }, name: { type: string } } } }
  /projects/{project_id}/members:
    get:
      summary: List a project's members
      parameters: [{ name: project_id, in: path, required: true, schema: { type: integer } }]
      responses:
        "200":
          headers: { Link: { schema: { type: string } } }
          content:
            application/json:
              schema: { type: array, items: { type: object, properties: { id: { type: string }, email: { type: string, format: email } } } }
  /search:
    get:
      summary: Search
      parameters:
        - { name: q, in: query, required: true, schema: { type: string } }
        - { name: offset, in: query, schema: { type: integer } }
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  results: { type: array, items: { type: object, properties: { id: { type: string } } } }
  /comments:
    get:
      summary: List comments
      parameters: [{ name: starting_after, in: query, schema: { type: string } }]
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  has_more: { type: boolean }
                  data: { type: array, items: { type: object, properties: { id: { type: string }, text: { type: string } } } }
  /attachments:
    post:
      summary: Upload a file
      requestBody: { content: { multipart/form-data: { schema: { type: object } } } }
      responses: { "201": { description: Uploaded } }
  /legacy/tasks:
    get:
      deprecated: true
      summary: Old list
      responses: { "200": { content: { application/json: { schema: { type: array, items: {} } } } } }
webhooks:
  taskDone: { post: { responses: { "200": { description: ok } } } }
"##;

/// A Swagger 2.0 service with a form body and basic sign-in.
pub(crate) const NOTES: &str = r##"{
  "swagger": "2.0",
  "info": { "title": "Notes", "version": "1" },
  "host": "api.notes.example",
  "basePath": "/v1",
  "schemes": ["https"],
  "securityDefinitions": { "user": { "type": "basic" }, "key": { "type": "apiKey", "in": "query", "name": "key" } },
  "paths": {
    "/notes": {
      "post": {
        "operationId": "createNote",
        "summary": "Add a note",
        "consumes": ["application/x-www-form-urlencoded"],
        "parameters": [
          { "name": "text", "in": "formData", "type": "string", "required": true, "description": "The note." },
          { "name": "pinned", "in": "formData", "type": "boolean" }
        ],
        "responses": { "201": { "description": "made", "schema": { "$ref": "#/definitions/Note" } } }
      },
      "get": {
        "summary": "List notes",
        "parameters": [{ "name": "tags", "in": "query", "type": "array", "items": { "type": "string" }, "collectionFormat": "csv" }],
        "responses": { "200": { "description": "ok", "schema": { "type": "array", "items": { "$ref": "#/definitions/Note" } } } }
      }
    }
  },
  "definitions": { "Note": { "type": "object", "properties": { "id": { "type": "integer" }, "text": { "type": "string" } } } }
}"##;

fn options(id: &str) -> Options {
    Options {
        id: id.to_owned(),
        source: "the test's own description".to_owned(),
        made_on: "2026-11-27".to_owned(),
        ..Options::default()
    }
}

/// Every recorded test of a made app holds against its definition.
fn holds(made: &Generated) -> AppDef {
    let app = AppDef::parse(&made.app_yaml, "made.yaml").unwrap();
    for (file, text) in &made.tests {
        recorded::check(&app, text, file)
            .unwrap_or_else(|e| panic!("{e}\n--- {file}\n{text}\n--- app.yaml\n{}", made.app_yaml));
    }
    app
}

#[test]
fn today_is_a_date() {
    let t = today();
    assert_eq!((t.len(), t.get(4..5), t.get(7..8)), (10, Some("-"), Some("-")));
    assert!(t.as_str() >= "2026-01-01", "{t}");
}

#[test]
fn an_openapi_3_1_description_makes_an_app_whose_recordings_hold() {
    let made = from_openapi(TASKS, &options("tasks_made")).unwrap();
    let app = holds(&made);
    assert_eq!(app.name, "Tasks");
    assert_eq!(app.base_url, "https://api.tasks.example/v2");
    assert_eq!(app.hosts, ["api.tasks.example"]);
    assert_eq!(app.docs.as_deref(), Some("https://tasks.example/docs/api"));
    let names: Vec<&str> = app.actions.iter().map(|(n, _)| n.as_str()).collect();
    for want in [
        "list_tasks",
        "create_task",
        "get_task",
        "update_task",
        "delete_task",
        "complete_task",
        "list_projects",
        "list_members",
        "list_search",
        "list_comments",
    ] {
        assert!(names.contains(&want), "{want} missing from {names:?}");
    }
    assert!(
        !names
            .iter()
            .any(|n| n.contains("upload") || n.contains("attachment") || n.contains("legacy")),
        "{names:?}"
    );
    // Pages: a token, a number, the Link header, an offset, the last record's id.
    let next = |a: &str| app.action(Some(a)).unwrap().1.next.is_some();
    assert!(next("list_tasks") && next("list_projects") && next("list_members") && next("list_comments"));
    let (_, list) = app.action(Some("list_tasks")).unwrap();
    assert_eq!(
        list.inputs.iter().map(|i| i.name.as_str()).collect::<Vec<_>>(),
        ["status"]
    );
    assert_eq!(
        list.output.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["id", "title", "due", "done", "tags"]
    );
    // A body's read-only field is not an input; its required one is.
    let (_, create) = app.action(Some("create_task")).unwrap();
    let title = create.inputs.iter().find(|i| i.name == "title").unwrap();
    assert!(title.required && title.label == "What is to be done");
    assert!(!create.inputs.iter().any(|i| i.name == "id"));
    assert!(create.write);
    // Sign-ins: the token first, the sign-in at the vendor with the scope its actions need; OpenID Connect not done.
    assert_eq!(
        app.sign_ins.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["tasks_made", "tasks_made_oauth"]
    );
    assert_eq!(app.sign_ins[1].oauth2.as_ref().unwrap().scopes, ["tasks"]);
    assert!(
        made.not_done.iter().any(|n| n.contains("OpenID Connect")),
        "{:?}",
        made.not_done
    );
    assert!(
        made.not_done.iter().any(|n| n.contains("file upload")),
        "{:?}",
        made.not_done
    );
    assert!(
        made.not_done.iter().any(|n| n.contains("deprecated")),
        "{:?}",
        made.not_done
    );
    assert!(
        made.not_done.iter().any(|n| n.contains("webhooks")),
        "{:?}",
        made.not_done
    );
    assert!(made.readme.contains("## Not done") && made.readme.contains("/oauth/callback"));
    assert!(made.tests.iter().any(|(f, _)| f == "refused.yaml"));
    // The quality report: every action described or not, each with an output (but delete) and a recorded test.
    let report = crate::app_files::quality::report(&app, &made.tests, true);
    assert_eq!(report.recorded, report.actions);
    assert!(report.refusal);
    assert!(
        report.score >= crate::app_files::quality::BAR,
        "{:?}",
        report.lines()
    );
}

#[test]
fn a_swagger_2_description_makes_an_app_too() {
    let made = from_openapi(NOTES, &options("notes_made")).unwrap();
    let app = holds(&made);
    assert_eq!(app.base_url, "https://api.notes.example/v1");
    let (_, create) = app.action(Some("create_note")).unwrap();
    assert_eq!(create.body, crate::app_files::BodyKind::Form);
    assert_eq!(create.inputs.len(), 2);
    // A list in the query of Swagger 2.0, with commas, as the description says.
    let (_, list) = app.action(Some("list_notes")).unwrap();
    assert!(list.inputs[0].label.ends_with("with commas"));
    // The key in the query, then the user name and password.
    assert_eq!(app.sign_ins[0].query.len(), 1);
    assert!(app.sign_ins[1].basic.is_some());
}

#[test]
fn the_person_chooses_actions_and_sign_ins_and_nothing_is_guessed() {
    let mut o = options("tasks_few");
    o.chosen = Some(vec![
        "get_task".to_owned(),
        "list_projects".to_owned(),
        "nonsense".to_owned(),
    ]);
    o.sign_ins = Some(vec!["tasks_few_oauth".to_owned()]);
    let made = from_openapi(TASKS, &o).unwrap();
    let app = holds(&made);
    assert_eq!(app.actions.len(), 2);
    assert_eq!(app.sign_ins.len(), 1);
    assert!(made.proposed.iter().filter(|p| p.chosen).count() == 2);
    assert!(made.sign_ins.iter().any(|s| s.id == "tasks_few" && !s.chosen));
    // At most N, the best first.
    let mut o = options("tasks_three");
    o.actions = 3;
    let made = from_openapi(TASKS, &o).unwrap();
    assert_eq!(AppDef::parse(&made.app_yaml, "x").unwrap().actions.len(), 3);
    // A description that does not say how it signs in needs a sign-in given by hand.
    let silent = NOTES.replace(r#""securityDefinitions": { "user": { "type": "basic" }, "key": { "type": "apiKey", "in": "query", "name": "key" } },"#, "");
    let e = from_openapi(&silent, &options("notes_silent")).unwrap_err();
    assert!(
        e.contains("does not say how it signs in") && e.contains("--sign-in"),
        "{e}"
    );
    let mut o = options("notes_given");
    o.sign_in = Some("header:X-Notes-Key".parse().unwrap());
    holds(&from_openapi(&silent, &o).unwrap());
    // Not a description; a bad id; no server.
    assert!(
        from_openapi("hello: world", &options("x"))
            .unwrap_err()
            .contains("not an OpenAPI")
    );
    assert!(
        from_openapi(TASKS, &options("Bad Id"))
            .unwrap_err()
            .contains("lowercase")
    );
    let no_server = NOTES.replace(r#""host": "api.notes.example","#, "");
    assert!(
        from_openapi(&no_server, &options("x"))
            .unwrap_err()
            .contains("--base-url")
    );
    let http_only = TASKS.replace("https://api.tasks.example/v2", "http://api.tasks.example/v2");
    assert!(
        from_openapi(&http_only, &options("x"))
            .unwrap_err()
            .contains("https://")
    );
}

#[test]
fn an_mcp_server_s_tools_make_an_app_whose_recordings_hold() {
    let tools = vec![
        crate::mcp::ToolInfo {
            name: "create-issue".to_owned(),
            title: Some("Open an issue".to_owned()),
            description: "Opens an issue in a project.".to_owned(),
            input_schema: serde_json::json!({ "type": "object", "required": ["title"], "properties": {
                "title": { "type": "string", "description": "The issue's title." },
                "labels": { "type": "array", "items": { "type": "string" } },
            } }),
            annotations: serde_json::json!({}),
            output_schema: serde_json::json!({ "type": "object", "properties": { "id": { "type": "string" }, "url": { "type": "string" } } }),
        },
        crate::mcp::ToolInfo {
            name: "search".to_owned(),
            title: None,
            description: String::new(),
            input_schema: serde_json::json!({ "type": "object", "properties": { "q": { "type": "string" } } }),
            annotations: serde_json::json!({ "readOnlyHint": true }),
            output_schema: serde_json::Value::Null,
        },
    ];
    let made = from_mcp(
        &tools,
        &McpOptions {
            id: "issues_mcp".to_owned(),
            name: Some("Issues".to_owned()),
            url: "https://mcp.issues.example/mcp".to_owned(),
            docs: Some("https://issues.example/docs/mcp".to_owned()),
            ..McpOptions::default()
        },
    )
    .unwrap();
    let app = holds(&made);
    let (_, open) = app.action(Some("create_issue")).unwrap();
    assert_eq!(
        (open.mcp.as_deref(), open.write, open.label.as_str()),
        (Some("create-issue"), true, "Open an issue")
    );
    assert_eq!(open.output.len(), 2);
    let (_, search) = app.action(Some("search")).unwrap();
    assert!(!search.write, "a tool the server says only reads is not a write");
    let report = crate::app_files::quality::report(&app, &made.tests, true);
    assert_eq!(report.recorded, 2);
    assert!(report.refusal);
    assert!(
        from_mcp(
            &tools,
            &McpOptions {
                id: "x".to_owned(),
                url: "http://mcp.example/mcp".to_owned(),
                ..McpOptions::default()
            }
        )
        .unwrap_err()
        .contains("https://")
    );
}
