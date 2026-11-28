// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Test runs that send nothing and still go through the steps after a request as a real run would: an app's action
//! answers with the app's recorded example, an HTTP step with the sample answer it was given, each step's record
//! marked "sample answer, nothing sent" beside the request it would have sent; and a table not made yet is an empty
//! stand-in, said in the step's record. An app with no recording answers as before; a preview reads for real; a real
//! run against a table not made yet still fails, plainly. The workflows are two a model built; their business data is
//! made up.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openduct_expr::Value;
use openduct_flow::tables::{Access, Column, ColumnType, MemoryTables};
use openduct_flow::{Credential, DryRun, Effects, Event, HttpRequest, Outcome, Preview, Workflow, run};
use serde_json::{Value as Json, json};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Weekday digest of urgent tickets: Zendesk's tickets read, an agent picks the urgent ones, an e-mail sent.
const TICKETS: &str = r#"
name: Weekday urgent ticket digest
trigger:
  schedule:
    every: weekday
    at: "09:00"
    timezone: Europe/Berlin
sample:
  scheduled_at: "2026-11-17T08:00:00Z"
steps:
  - name: tickets
    zendesk:
      credential: zendesk
      action: list_tickets
    retry:
      attempts: 3
      wait: 30s
  - name: digest
    agent:
      credential: anthropic
      instructions: "You help a support lead. Pick out the urgent tickets and summarise each in two lines."
      input: |-
        The tickets:
        {{ type(steps.tickets) == list ? steps.tickets.map(t, '#' + string(t.id) + ' | ' + string(t.subject) + ' | status: ' + string(t.status) + ' | priority: ' + string(t.priority) + ' | ' + string(t.description)).join('\n') : string(steps.tickets) }}
      max_turns: 3
  - name: send
    email:
      credential: mail
      to: support-lead@example.com
      subject: Urgent support tickets
      text: "{{ steps.digest.text }}"
"#;

/// The same digest as built a second time: the open tickets kept by a list step before the agent reads them.
const TICKETS_LISTED: &str = r#"
name: Weekday urgent ticket digest
trigger:
  schedule:
    every: weekday
    at: "09:00"
    timezone: Europe/Berlin
steps:
  - name: tickets
    zendesk:
      credential: zendesk
      action: list_tickets
  - name: open_tickets
    list:
      items: steps.tickets
      filter: "item.status == 'open' || item.status == 'new'"
  - name: digest
    agent:
      credential: anthropic
      instructions: "Pick the urgent tickets. If none is urgent, answer with exactly: NONE"
      input: |-
        Open tickets (JSON):
        {{ string(steps.open_tickets) }}
      max_turns: 3
  - where: "!steps.digest.text.contains('NONE')"
  - name: send
    email:
      credential: mail
      to: support-lead@example.com
      subject: Urgent support tickets
      text: "{{ steps.digest.text }}"
"#;

/// A contact form's answers kept in the leads table, the message classified, a HubSpot contact for sales.
const LEADS: &str = r#"
name: Contact form lead intake
trigger:
  form:
    title: Contact us
    fields:
      - { name: name, label: Name, type: text, required: true }
      - { name: company, label: Company, type: text }
      - { name: email, label: E-mail, type: email, required: true }
      - { name: message, label: Message, type: long_text, required: true, max_length: 3000 }
steps:
  - name: save_lead
    table:
      name: leads
      action: insert
      row:
        name: "{{ value.name }}"
        company: "{{ value.company }}"
        email: "{{ value.email }}"
        message: "{{ value.message }}"
  - name: sort
    classify:
      credential: anthropic
      text: "{{ value.message }}"
      categories:
        - name: sales
          description: "Asks about prices, quotes, plans, demos or buying"
          steps:
            - name: crm_contact
              hubspot:
                credential: hubspot
                action: create_contact
                email: "{{ value.email }}"
                firstname: "{{ value.name }}"
                company: "{{ value.company }}"
        - name: support
          description: Needs help with something that does not work or an existing order
      default:
        - name: other_kept
          set:
            kind: "'other'"
"#;

fn block<T>(f: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn event(value: Json) -> Event {
    Event {
        value: Value::from_json(&value),
        details: Value::from_json(&json!({ "trigger": "test" })),
    }
}

/// A test run's effects: the AI credential the workspace had (its key hidden), the others not added yet.
fn test_run() -> DryRun {
    let mut credentials = BTreeMap::new();
    credentials.insert(
        "anthropic".to_owned(),
        Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), "https://api.anthropic.com/v1".to_owned()),
                ("api_key".to_owned(), "a-made-up-key".to_owned()),
                ("api".to_owned(), "messages".to_owned()),
                ("model".to_owned(), "claude-sonnet-5-5".to_owned()),
            ]
            .into_iter()
            .collect(),
        },
    );
    DryRun {
        credentials,
        ..DryRun::default()
    }
}

fn lead() -> Json {
    json!({ "name": "Sam Lee", "company": "Lee Bakery", "email": "sam@leebakery.example", "message": "Could you send a quote for 200 bags?" })
}

#[test]
fn the_ticket_digest_s_test_run_goes_through_every_step_on_zendesk_s_example() {
    let workflow = Workflow::parse(TICKETS).unwrap();
    let ran = block(run(
        &workflow,
        &event(json!({ "scheduled_at": "2026-11-17T08:00:00Z" })),
        &test_run(),
    ));
    assert_eq!(ran.outcome, Outcome::Done, "{}", ran.to_json());
    let shown = ran.to_json();
    let steps = shown["steps"].as_array().unwrap();
    assert_eq!(
        steps
            .iter()
            .map(|s| s["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["tickets", "digest", "send"]
    );
    // The tickets: a list, as Zendesk answers - its recorded example - marked as a sample, beside the request.
    let tickets = &steps[0];
    assert!(tickets["result"].is_array(), "{tickets}");
    assert_eq!(tickets["result"][0]["subject"], "Cannot sign in");
    assert_eq!(tickets["sample"], "sample answer, nothing sent");
    assert_eq!(tickets["request"]["method"], "GET");
    assert!(
        tickets["request"]["url"]
            .as_str()
            .unwrap()
            .ends_with(".zendesk.com/api/v2/tickets.json"),
        "{tickets}"
    );
    // The agent was given the tickets, one a line: what it would have asked shows them.
    let asked = steps[1]["result"].to_string();
    assert!(
        asked.contains("#1 | Cannot sign in | status: open | priority: urgent"),
        "{asked}"
    );
    assert!(
        steps[1].get("sample").is_none(),
        "the agent's stand-in is as before"
    );
    assert_eq!(steps[2]["result"]["test"], "not sent");
}

#[test]
fn a_list_step_after_an_app_s_action_goes_through_its_example_s_items() {
    // As it was built, the agent's input is string() of a list, which a real run refuses too: the test run, given
    // a list as Zendesk gives one, now says so before the workflow is turned on.
    let built = Workflow::parse(TICKETS_LISTED).unwrap();
    let ran = block(run(&built, &event(json!({})), &test_run()));
    assert_eq!(
        ran.outcome,
        Outcome::Failed {
            at: "digest".to_owned(),
            error: "input: found no matching overload for 'string' applied to '(list)'".to_owned(),
        }
    );
    let workflow =
        Workflow::parse(&TICKETS_LISTED.replace("string(steps.open_tickets)", "steps.open_tickets")).unwrap();
    let ran = block(run(&workflow, &event(json!({})), &test_run()));
    assert_eq!(ran.outcome, Outcome::Done, "{}", ran.to_json());
    let shown = ran.to_json();
    let open = &shown["steps"][1];
    assert_eq!(open["name"], "open_tickets");
    assert_eq!(open["error"], Json::Null);
    assert_eq!(
        open["result"].as_array().unwrap().len(),
        1,
        "the open one of the two: {open}"
    );
    assert_eq!(shown["steps"].as_array().unwrap().len(), 5);
}

#[test]
fn an_app_with_no_recording_answers_with_the_request_as_before() {
    openduct_flow::app_files::add(
        r#"
app: notebook_test
name: Notebook
hosts: [api.notebook.example]
base_url: https://api.notebook.example/v1
sign_in:
  - id: notebook_test_key
    label: An API key
    fields:
      - { name: key, label: The key, secret: true }
    headers: { Authorization: "Bearer {{ credential.key }}" }
actions:
  pages:
    label: Pages
    method: GET
    path: /pages
    items: value.pages
"#,
        "notebook.yaml",
    )
    .unwrap();
    let workflow = Workflow::parse(
        "name: Pages\ntrigger: manual\nsteps:\n  - name: pages\n    notebook_test: { credential: notes, action: pages }\n",
    )
    .unwrap();
    let ran = block(run(&workflow, &event(json!({})), &test_run()));
    assert_eq!(ran.outcome, Outcome::Done);
    let step = &ran.to_json()["steps"][0];
    assert_eq!(step["result"]["test"], "not sent", "{step}");
    assert_eq!(
        step["result"]["request"]["url"],
        "https://api.notebook.example/v1/pages"
    );
    assert!(
        step.get("sample").is_none() && step.get("request").is_none(),
        "{step}"
    );
}

const RATES: &str = r#"
name: Rates
trigger: manual
steps:
  - name: rates
    http:
      url: https://rates.example.com/latest
      sample: { base: USD, rates: { EUR: 0.92 } }
  - name: euro
    set:
      rate: steps.rates.body.rates.EUR
"#;

#[test]
fn an_http_step_answers_with_its_sample_only_when_it_is_given_one() {
    let workflow = Workflow::parse(RATES).unwrap();
    let ran = block(run(&workflow, &event(json!({})), &test_run()));
    assert_eq!(ran.outcome, Outcome::Done);
    let shown = ran.to_json();
    let rates = &shown["steps"][0];
    assert_eq!(rates["result"]["status"], 200);
    assert_eq!(rates["sample"], "sample answer, nothing sent");
    assert_eq!(rates["request"]["url"], "https://rates.example.com/latest");
    assert_eq!(shown["steps"][1]["result"]["rate"], json!(0.92));
    // Without one, unchanged: what it would have sent.
    let plain =
        Workflow::parse(&RATES.replace("      sample: { base: USD, rates: { EUR: 0.92 } }\n", "")).unwrap();
    let ran = block(run(&plain, &event(json!({})), &test_run()));
    let shown = ran.to_json();
    assert_eq!(shown["steps"][0]["result"]["test"], "not sent");
    assert!(shown["steps"][0].get("sample").is_none());
    // A sample answer is bounded, and checked when the workflow is read.
    let big = format!("{{ big: \"{}\" }}", "x".repeat(70 * 1024));
    let e = Workflow::parse(&RATES.replace("{ base: USD, rates: { EUR: 0.92 } }", &big)).unwrap_err();
    assert!(e.contains("sample: a sample answer of at most 64 KiB"), "{e}");
}

/// A service that answers every request it is sent with `answer`, and keeps what it was sent.
#[derive(Debug)]
struct Service {
    answer: Json,
    sent: Mutex<Vec<String>>,
}

impl Effects for Service {
    fn http<'a>(&'a self, request: HttpRequest) -> Pending<'a> {
        self.sent.lock().unwrap().push(request.url);
        let answer = self.answer.clone();
        Box::pin(async move { Ok(Value::from_json(&answer)) })
    }
    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no topics here".to_owned()) })
    }
    fn credential(&self, name: &str) -> Result<Credential, String> {
        Ok(Credential {
            kind: if name == "zendesk" { "zendesk" } else { "smtp" }.to_owned(),
            fields: [
                ("subdomain".to_owned(), "acme".to_owned()),
                ("email".to_owned(), "help@acme.example".to_owned()),
                ("api_token".to_owned(), "made-up-token".to_owned()),
            ]
            .into_iter()
            .collect(),
        })
    }
    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path}")) })
    }
}

#[test]
fn a_preview_reads_for_real_and_shows_a_write_as_before() {
    let service = Arc::new(Service {
        answer: json!({ "status": 200, "headers": {}, "body": { "tickets": [{ "id": 77, "status": "open" }], "next_page": null } }),
        sent: Mutex::new(Vec::new()),
    });
    let preview = Preview::new(Arc::clone(&service) as Arc<dyn Effects>);
    let workflow = Workflow::parse(
        r#"
name: Tickets
trigger: manual
steps:
  - name: tickets
    zendesk: { credential: zendesk, action: list_tickets }
  - name: answer
    zendesk: { credential: zendesk, action: answer_ticket, ticket_id: "77", text: Thanks }
"#,
    )
    .unwrap();
    let ran = block(run(&workflow, &event(json!({})), &preview));
    assert_eq!(ran.outcome, Outcome::Done, "{}", ran.to_json());
    let shown = ran.to_json();
    assert_eq!(
        shown["steps"][0]["result"],
        json!([{ "id": 77, "status": "open" }]),
        "read for real"
    );
    assert!(shown["steps"][0].get("sample").is_none());
    assert_eq!(
        shown["steps"][1]["result"]["test"], "not sent",
        "a write shown, not answered with a sample"
    );
    assert!(shown["steps"][1].get("sample").is_none());
    assert_eq!(service.sent.lock().unwrap().len(), 1, "only the read was sent");
}

#[test]
fn the_lead_intake_s_test_run_uses_an_empty_stand_in_for_the_table_not_made_yet() {
    let tables = Arc::new(MemoryTables::new());
    let effects = DryRun {
        tables: Some(Arc::clone(&tables) as Arc<dyn openduct_flow::tables::Tables>),
        ..test_run()
    };
    let workflow = Workflow::parse(LEADS).unwrap();
    let ran = block(run(&workflow, &event(lead()), &effects));
    assert_eq!(ran.outcome, Outcome::Done, "{}", ran.to_json());
    let shown = ran.to_json();
    let save = &shown["steps"][0];
    assert_eq!(save["error"], Json::Null, "not a failure: {save}");
    assert_eq!(
        save["note"],
        "the table leads does not exist yet: this test used an empty one"
    );
    assert_eq!(save["result"]["test"], "not written");
    assert_eq!(save["result"]["rows"][0]["company"], "Lee Bakery", "{save}");
    assert!(
        block(openduct_flow::tables::Tables::read(tables.as_ref(), "leads")).is_err(),
        "nothing made"
    );
    // Once made, the table itself is read: no note.
    let text = |n: &str| Column {
        name: n.to_owned(),
        kind: ColumnType::Text,
    };
    tables
        .create(
            "leads",
            vec![text("name"), text("company"), text("email"), text("message")],
        )
        .unwrap();
    let ran = block(run(&workflow, &event(lead()), &effects));
    let shown = ran.to_json();
    assert_eq!(ran.outcome, Outcome::Done);
    assert!(shown["steps"][0].get("note").is_none(), "{}", shown["steps"][0]);
    // A step's own mistake is still its failure, table or none.
    let wrong =
        Workflow::parse(&LEADS.replace("name: \"{{ value.name }}\"", "name: \"{{ value.name + 1 }}\""))
            .unwrap();
    let ran = block(run(&wrong, &event(lead()), &effects));
    assert!(matches!(ran.outcome, Outcome::Failed { .. }), "{}", ran.to_json());
}

/// A real run's effects: tables it writes, and no service reached.
#[derive(Debug, Default)]
struct Real {
    tables: MemoryTables,
}

impl Effects for Real {
    fn http<'a>(&'a self, _: HttpRequest) -> Pending<'a> {
        Box::pin(async { Err("no service here".to_owned()) })
    }
    fn write<'a>(&'a self, _: &'a str, _: Option<String>, _: String) -> Pending<'a> {
        Box::pin(async { Err("no topics here".to_owned()) })
    }
    fn credential(&self, name: &str) -> Result<Credential, String> {
        Err(format!("no credential {name}"))
    }
    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        Box::pin(async move { Err(format!("no file {path}")) })
    }
    fn tables(&self) -> Access<'_> {
        Access::Write(&self.tables)
    }
}

#[test]
fn a_real_run_against_a_table_not_made_yet_still_fails_plainly() {
    let workflow = Workflow::parse(LEADS).unwrap();
    let ran = block(run(&workflow, &event(lead()), &Real::default()));
    assert_eq!(
        ran.outcome,
        Outcome::Failed {
            at: "save_lead".to_owned(),
            error: "there is no table leads: make it under Tables".to_owned(),
        }
    );
    assert!(ran.to_json()["steps"][0].get("note").is_none());
}
