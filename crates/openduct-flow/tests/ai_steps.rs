// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The AI steps built on the `ai` step - classify, sentiment, extract, summarise, and `ai` with a schema - read,
//! checked and run against a stand-in model: what each sends, how its answer is held to its schema (asked once more
//! when it misses), the routes it takes, and what a test run shows.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Mutex;

use openduct_expr::Value;
use openduct_flow::{DryRun, Effects, Event, HttpRequest, Live, Outcome, RunRecord, Workflow, run};
use serde_json::{Value as Json, json};

type Pending<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type Answer = Box<dyn Fn(&Json, usize) -> Json + Send + Sync>;

/// A stand-in model: each request answered by `answer` (given the request's body and how many came before it) -
/// a text as the chat completions API gives it, or `{status, body}` as it stands - and every request kept.
struct Model {
    answer: Answer,
    sent: Mutex<Vec<Json>>,
    written: Mutex<Vec<String>>,
}

impl Model {
    fn new(answer: impl Fn(&Json, usize) -> Json + Send + Sync + 'static) -> Self {
        Self {
            answer: Box::new(answer),
            sent: Mutex::new(Vec::new()),
            written: Mutex::new(Vec::new()),
        }
    }

    /// Answers each request with the next of `texts`, in turn.
    fn saying(texts: &[&str]) -> Self {
        let texts: Vec<String> = texts.iter().map(|t| (*t).to_owned()).collect();
        Self::new(move |_, n| json!(texts[n.min(texts.len() - 1)]))
    }

    fn sent(&self) -> Vec<Json> {
        self.sent.lock().unwrap().clone()
    }
}

impl Effects for Model {
    fn http<'a>(&'a self, r: HttpRequest) -> Pending<'a> {
        Box::pin(async move {
            assert!(r.url.ends_with("/chat/completions"), "{}", r.url);
            let body: Json = serde_json::from_slice(&r.body.unwrap().1).unwrap();
            let n = {
                let mut sent = self.sent.lock().unwrap();
                sent.push(body.clone());
                sent.len() - 1
            };
            let said = (self.answer)(&body, n);
            let answer = match said {
                Json::String(text) => json!({ "status": 200, "body": {
                    "model": "stand-in-1",
                    "choices": [{ "message": { "content": text }, "finish_reason": "stop" }],
                    "usage": { "prompt_tokens": 10, "completion_tokens": 3 },
                } }),
                other => other,
            };
            Ok(Value::from_json(&answer))
        })
    }

    fn write<'a>(&'a self, topic: &'a str, _: Option<String>, value: String) -> Pending<'a> {
        Box::pin(async move {
            self.written.lock().unwrap().push(format!("{topic} {value}"));
            Ok(Value::from_json(&json!({ "topic": topic })))
        })
    }

    fn credential(&self, _: &str) -> Result<openduct_flow::Credential, String> {
        Ok(openduct_flow::Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), "https://models.example.com/v1".to_owned()),
                ("api_key".to_owned(), "key-SECRET".to_owned()),
                ("model".to_owned(), "stand-in-1".to_owned()),
            ]
            .into(),
        })
    }

    fn file<'a>(&'a self, _: &'a str) -> Pending<'a> {
        Box::pin(async { Err("no files".to_owned()) })
    }
}

fn go(workflow: &str, value: Json, effects: &dyn Effects) -> RunRecord {
    let workflow = Workflow::parse(workflow).unwrap_or_else(|e| panic!("{e}"));
    let event = Event {
        value: Value::from_json(&value),
        details: Value::from_json(&json!({ "trigger": "webhook" })),
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&workflow, &event, effects))
}

fn result(record: &RunRecord, step: &str) -> Json {
    let found = record
        .steps
        .iter()
        .find(|s| s.name == step)
        .unwrap_or_else(|| panic!("no step {step} in {:?}", names(record)));
    serde_json::from_str(&found.result.to_json()).unwrap()
}

fn names(record: &RunRecord) -> Vec<String> {
    record.steps.iter().map(|s| s.name.clone()).collect()
}

fn system_of(body: &Json) -> String {
    body["messages"][0]["content"].as_str().unwrap().to_owned()
}

const SORT: &str = r#"
name: Sort support
trigger: webhook
steps:
  - name: sort
    classify:
      credential: model
      text: "{{ value.text }}"
      instructions: Messages about invoices are billing.
      categories:
        - name: billing
          description: Payments, invoices, refunds
          steps:
            - name: to_billing
              topic: { name: billing, value: "{{ value.text }}" }
        - name: bug
          description: Something does not work
          steps:
            - name: to_bugs
              topic: { name: bugs, value: "{{ value.text }}" }
      default:
        - name: to_others
          topic: { name: others, value: "{{ value.text }}" }
  - name: after
    set: { said: steps.sort.category }
"#;

#[test]
fn classify_routes_to_the_category_the_model_chose_and_takes_its_name_in_any_case() {
    let model = Model::saying(&[r#"{"category": " Billing"}"#]);
    let record = go(SORT, json!({ "text": "My card was charged twice" }), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        names(&record),
        ["sort", "to_billing", "after"],
        "only the chosen category's steps"
    );
    let sort = result(&record, "sort");
    assert_eq!(sort["category"], "billing");
    assert_eq!(sort["categories"], json!(["billing"]));
    assert_eq!(sort["model"], "stand-in-1");
    assert_eq!(sort["usage"]["total_tokens"], 13);
    assert_eq!(
        sort["transcript"].as_array().unwrap().len(),
        3,
        "system, the text, the answer"
    );
    assert!(sort["metrics"]["latency_ms"].is_number());
    assert_eq!(result(&record, "after")["said"], "billing");
    let sent = model.sent();
    assert_eq!(sent.len(), 1);
    let system = system_of(&sent[0]);
    for wanted in [
        "- billing: Payments, invoices, refunds",
        "- bug: Something does not work",
        "- other: none of the categories above fits",
        "{\"category\":",
        "Messages about invoices are billing.",
    ] {
        assert!(system.contains(wanted), "{wanted:?} not in {system}");
    }
    assert_eq!(sent[0]["messages"][1]["content"], "My card was charged twice");
    assert_eq!(sent[0]["response_format"], json!({ "type": "json_object" }));
    assert_eq!(sent[0]["model"], "stand-in-1", "the credential's model");

    let fallback = Model::saying(&["```json\n{\"category\": \"other\"}\n```"]);
    let record = go(SORT, json!({ "text": "Hello" }), &fallback);
    assert_eq!(
        names(&record),
        ["sort", "to_others", "after"],
        "other runs the default's steps"
    );
    assert_eq!(result(&record, "after")["said"], "other");
}

#[test]
fn an_answer_outside_the_categories_is_asked_again_once_and_a_second_miss_fails_the_step() {
    let strict = SORT.replace("      default:\n        - name: to_others\n          topic: { name: others, value: \"{{ value.text }}\" }\n", "      other: false\n");
    let model = Model::saying(&[r#"{"category": "refund"}"#, r#"{"category": "bug"}"#]);
    let record = go(&strict, json!({ "text": "It crashes" }), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(names(&record), ["sort", "to_bugs", "after"]);
    let sent = model.sent();
    assert_eq!(sent.len(), 2, "asked once more");
    assert!(
        !system_of(&sent[0]).contains("- other"),
        "with other: false the model must choose"
    );
    let again = sent[1]["messages"].as_array().unwrap();
    assert_eq!(
        again.len(),
        4,
        "system, the text, the first answer, what to put right"
    );
    assert_eq!(
        again[2],
        json!({ "role": "assistant", "content": r#"{"category": "refund"}"# })
    );
    let told = again[3]["content"].as_str().unwrap();
    assert!(
        told.contains(r#"$.category: one of "billing", "bug", not "refund""#),
        "{told}"
    );
    let sort = result(&record, "sort");
    assert_eq!(sort["usage"]["total_tokens"], 26, "both calls counted");
    assert_eq!(sort["metrics"]["repairs"], 1);
    assert_eq!(sort["transcript"].as_array().unwrap().len(), 5);

    let wrong_twice = Model::saying(&["I think it is a refund.", r#"{"category": "refund"}"#]);
    let record = go(&strict, json!({ "text": "x" }), &wrong_twice);
    match &record.outcome {
        Outcome::Failed { at, error } => {
            assert_eq!(at, "sort");
            assert!(
                error.starts_with("the model's answer does not meet the schema, even asked again"),
                "{error}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(
        wrong_twice.sent()[1]["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains("the answer is not JSON"),
        "an answer that is not JSON is said to be so"
    );
    // With on_error: continue the run goes on, the error in the step's result; no route is taken.
    let going_on = strict.replace("  - name: sort\n", "  - name: sort\n    on_error: continue\n");
    let record = go(&going_on, json!({ "text": "x" }), &Model::saying(&["nope"]));
    assert_eq!(record.outcome, Outcome::Done);
    assert_eq!(names(&record), ["sort", "after"]);
    assert!(
        result(&record, "sort")["error"]
            .as_str()
            .unwrap()
            .contains("does not meet the schema")
    );
}

#[test]
fn classify_with_several_runs_every_chosen_category_s_steps_in_order() {
    let several = SORT.replace("      instructions:", "      several: true\n      instructions:");
    let model = Model::saying(&[r#"{"categories": ["BUG", "billing", "other", "bug"]}"#]);
    let record = go(
        &several,
        json!({ "text": "Charged twice and the page crashes" }),
        &model,
    );
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(names(&record), ["sort", "to_billing", "to_bugs", "after"]);
    assert_eq!(
        result(&record, "sort")["categories"],
        json!(["bug", "billing"]),
        "other beside a category, and a name twice, drop out"
    );
    assert!(system_of(&model.sent()[0]).contains("every one of these categories that fits"));
    let none = Model::saying(&[r#"{"categories": ["other"]}"#]);
    let record = go(&several, json!({ "text": "Hi" }), &none);
    assert_eq!(names(&record), ["sort", "to_others", "after"]);
}

#[test]
fn sentiment_gives_a_label_and_a_score_and_takes_the_label_s_steps() {
    let w = r"
name: Reviews
steps:
  - name: mood
    sentiment:
      credential: model
      text: '{{ value.review }}'
      labels:
        - positive
        - neutral
        - name: negative
          description: Unhappy, angry or disappointed
          steps:
            - name: tell
              topic: { name: unhappy, value: '{{ steps.mood.score }}' }
";
    let model = Model::saying(&[r#"{"label": "Negative", "score": "0.9"}"#]);
    let record = go(w, json!({ "review": "Never again." }), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let mood = result(&record, "mood");
    assert_eq!(
        (mood["label"].clone(), mood["score"].clone()),
        (json!("negative"), json!(0.9))
    );
    assert_eq!(names(&record), ["mood", "tell"]);
    assert_eq!(model.written.lock().unwrap().as_slice(), ["unhappy 0.9"]);
    assert!(system_of(&model.sent()[0]).contains("- negative: Unhappy, angry or disappointed"));
    // With no labels: positive, neutral, negative; a score out of 0 to 1 is asked again.
    let plain =
        "name: r\nsteps:\n  - name: mood\n    sentiment: { credential: model, text: '{{ value.review }}' }\n";
    let model = Model::saying(&[
        r#"{"label": "positive", "score": 7}"#,
        r#"{"label": "positive", "score": 0.7}"#,
    ]);
    let record = go(plain, json!({ "review": "Lovely" }), &model);
    assert_eq!(result(&record, "mood")["score"], 0.7);
    assert!(system_of(&model.sent()[0]).contains("- positive\n- neutral\n- negative"));
    assert!(
        model.sent()[1]["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains("$.score: at most 1, not 7")
    );
}

const LEAD: &str = r#"
name: Leads
steps:
  - name: lead
    extract:
      credential: model
      text: "{{ value.text }}"
      temperature: 0
      fields:
        - { name: name, description: "the person's full name" }
        - { name: email, type: string, required: true }
        - { name: seats, type: integer, description: how many they ask for }
        - { name: start, type: date }
        - { name: wants, type: list }
  - where: steps.lead.json.email != null
  - name: keep
    topic: { name: leads, value: "{{ steps.lead.json }}" }
"#;

#[test]
fn extract_pulls_fields_out_holds_them_to_their_types_and_asks_once_more_on_a_mismatch() {
    let model = Model::saying(&[
        r#"{"name": "Sam Lee", "seats": "two hundred", "start": "next week", "colour": "red"}"#,
        r#"Here: {"name": "Sam Lee", "email": "sam@lee.example", "seats": 200, "start": "2026-12-01", "wants": ["tote bags"]}"#,
    ]);
    let record = go(
        LEAD,
        json!({ "text": "Sam Lee, sam@lee.example: 200 tote bags from 1 Dec" }),
        &model,
    );
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let lead = result(&record, "lead");
    assert_eq!(
        lead["json"],
        json!({ "name": "Sam Lee", "email": "sam@lee.example", "seats": 200, "start": "2026-12-01", "wants": ["tote bags"] })
    );
    let sent = model.sent();
    let system = system_of(&sent[0]);
    assert!(
        system.contains("\"seats\": {") && system.contains("how many they ask for"),
        "{system}"
    );
    assert!(
        system.contains("a field the text does not give is null"),
        "{system}"
    );
    assert_eq!(
        sent[0]["temperature"], 0.0,
        "the model's settings, as the ai step takes them"
    );
    let told = sent[1]["messages"][3]["content"].as_str().unwrap();
    for wanted in [
        "$.email: string, not null",
        "$.seats: integer or null, not a text",
        "$.start: a date, as 2026-11-27, not \"next week\"",
    ] {
        assert!(told.contains(wanted), "{wanted:?} not in {told}");
    }
    assert!(
        !told.contains("colour"),
        "a field the step does not name is dropped, not complained of"
    );
    assert_eq!(lead["metrics"]["repairs"], 1);
    assert_eq!(model.written.lock().unwrap().len(), 1);

    // Fields the text does not give are null, without asking again.
    let model = Model::saying(&[r#"{"email": "a@b.example"}"#]);
    let record = go(LEAD, json!({ "text": "a@b.example" }), &model);
    assert_eq!(model.sent().len(), 1);
    assert_eq!(result(&record, "lead")["json"]["seats"], Json::Null);
}

#[test]
fn extract_takes_a_json_schema_of_its_own() {
    let w = r#"
name: Orders
steps:
  - name: order
    extract:
      credential: model
      text: "{{ value.text }}"
      schema:
        type: object
        properties:
          lines:
            type: array
            minItems: 1
            items:
              type: object
              properties: { sku: { type: string }, qty: { type: integer, minimum: 1 } }
              required: [sku, qty]
        required: [lines]
"#;
    let model = Model::saying(&[
        r#"{"lines": [{"sku": "CAP", "qty": 0}]}"#,
        r#"{"lines": [{"sku": "CAP", "qty": 2}]}"#,
    ]);
    let record = go(w, json!({ "text": "two caps" }), &model);
    assert_eq!(result(&record, "order")["json"]["lines"][0]["qty"], 2);
    assert!(
        model.sent()[1]["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains("$.lines[0].qty: at least 1, not 0")
    );
}

#[test]
fn an_ai_step_with_a_schema_holds_its_answer_to_it() {
    let w = r#"
name: Plan
steps:
  - name: plan
    ai:
      credential: model
      model: big-1
      system: You plan trips.
      prompt: "Plan {{ value.days }} days in {{ value.city }}"
      schema:
        type: object
        properties:
          days: { type: array, items: { type: string }, minItems: 1 }
        required: [days]
  - name: first
    set: { day: "steps.plan.json.days[0]" }
"#;
    let model = Model::saying(&[r#"{"plan": []}"#, r#"{"days": ["Museums", "Old town"]}"#]);
    let record = go(w, json!({ "days": 2, "city": "Porto" }), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(result(&record, "first")["day"], "Museums");
    let plan = result(&record, "plan");
    assert_eq!(
        plan["text"], r#"{"days": ["Museums", "Old town"]}"#,
        "the ai step's own result"
    );
    assert_eq!(record.steps[0].kind, "ai");
    let sent = model.sent();
    assert_eq!(sent[0]["model"], "big-1");
    let system = system_of(&sent[0]);
    assert!(
        system
            .starts_with("You plan trips.\n\nAnswer with JSON only: one value that meets this JSON schema:"),
        "{system}"
    );
    assert_eq!(sent[1]["messages"][1]["content"], "Plan 2 days in Porto");
    assert!(
        sent[1]["messages"][3]["content"]
            .as_str()
            .unwrap()
            .contains("$: days is missing")
    );
}

#[test]
fn summarise_does_a_short_text_in_one_call_and_a_long_one_in_pieces() {
    let short = "name: s\nsteps:\n  - name: sum\n    summarise: { credential: model, text: '{{ value.text }}', length: short, style: bullets }\n";
    let model = Model::saying(&["- It rained."]);
    let record = go(short, json!({ "text": "It rained all day." }), &model);
    assert_eq!(result(&record, "sum")["summary"], "- It rained.");
    assert_eq!(result(&record, "sum")["pieces"], 1);
    let system = system_of(&model.sent()[0]);
    assert_eq!(
        system,
        "Summarise the text the user gives in one or two sentences, as bullet points. Keep to what it says."
    );

    let long = "name: s\nsteps:\n  - name: sum\n    summarise: { credential: model, text: '{{ value.texts }}', piece_size: 1000, length: in 50 words }\n";
    let texts: Vec<String> = (0..5)
        .map(|i| format!("Chapter {i}. {}", "Words go on. ".repeat(60)))
        .collect();
    let model = Model::new(|body, _| {
        let system = body["messages"][0]["content"].as_str().unwrap();
        if system.starts_with("This is part") {
            json!(format!("note of {}", &system[13..14]))
        } else {
            json!(format!(
                "the whole, from: {}",
                body["messages"][1]["content"].as_str().unwrap()
            ))
        }
    });
    let record = go(long, json!({ "texts": texts }), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    let sum = result(&record, "sum");
    let sent = model.sent();
    assert_eq!(sent.len(), 6, "five pieces noted, then the summary");
    assert_eq!(sum["pieces"], 5);
    assert_eq!(sum["metrics"]["calls"], 6);
    assert_eq!(sum["usage"]["total_tokens"], 78, "every call's tokens");
    let last = system_of(&sent[5]);
    assert!(
        last.starts_with("These are notes on the parts of a longer text") && last.ends_with("in 50 words."),
        "{last}"
    );
    assert!(
        sum["summary"]
            .as_str()
            .unwrap()
            .starts_with("the whole, from: note of 1"),
        "the notes in order: {sum}"
    );
    assert_eq!(
        sum["transcript"].as_array().unwrap().len(),
        3,
        "the summary's own call"
    );
    for body in &sent[..5] {
        assert!(body["messages"][1]["content"].as_str().unwrap().chars().count() <= 1000);
    }

    let too_long = long
        .replace("piece_size: 1000", "piece_size: 1000, text: '{{ value.texts }}'")
        .replace("text: '{{ value.texts }}', piece_size", "piece_size");
    let huge: Vec<String> = vec!["x ".repeat(25_000)];
    let record = go(&too_long, json!({ "texts": huge }), &Model::saying(&["-"]));
    match record.outcome {
        Outcome::Failed { error, .. } => assert!(error.contains("more than 40 pieces of 1000"), "{error}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_setting_the_model_refuses_is_left_out_and_it_is_asked_again() {
    let model = Model::new(|body, n| {
        if n == 0 {
            assert_eq!(body["temperature"], 0.2);
            json!({ "status": 400, "body": { "error": { "message": "Unsupported value: 'temperature' does not support 0.2" } } })
        } else {
            assert!(body.get("temperature").is_none());
            json!(r#"{"label": "neutral", "score": 0.5}"#)
        }
    });
    let w =
        "name: r\nsteps:\n  - name: mood\n    sentiment: { credential: model, text: hi, temperature: 0.2 }\n";
    let record = go(w, json!({}), &model);
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        result(&record, "mood")["metrics"]["ignored_settings"],
        json!(["temperature"])
    );
    let refused = Model::new(|_, _| json!({ "status": 401, "body": { "error": { "message": "bad key" } } }));
    match go(w, json!({}), &refused).outcome {
        Outcome::Failed { error, .. } => assert_eq!(error, "ai answered 401: bad key"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_test_run_shows_what_each_would_send_and_takes_no_route() {
    let all = r#"
name: All
steps:
  - name: sort
    classify:
      credential: model
      text: "{{ value.text }}"
      categories: [{ name: a, steps: [{ name: in_a, set: { x: "1" } }] }]
  - name: mood
    sentiment: { credential: model, text: "{{ value.text }}" }
  - name: lead
    extract: { credential: model, text: "{{ value.text }}", fields: [email, phone] }
  - name: sum
    summarise: { credential: model, text: "{{ value.text }}" }
  - name: plan
    ai: { credential: model, prompt: hi, schema: { type: object, properties: { days: { type: array } } } }
  - where: steps.lead.json.email == null && steps.plan.json.days == null
"#;
    let record = go(all, json!({ "text": "hello" }), &DryRun::default());
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(
        names(&record),
        ["sort", "mood", "lead", "sum", "plan", "where_7"],
        "no category's steps"
    );
    let sort = result(&record, "sort");
    assert_eq!(sort["test"], "not sent");
    assert_eq!(sort["request"]["url"], "https://•••/chat/completions");
    assert_eq!(sort["category"], "(the model's choice, in a real run)");
    assert_eq!(
        result(&record, "lead")["json"],
        json!({ "email": null, "phone": null })
    );
    assert_eq!(result(&record, "sum")["summary"], "(the summary, in a real run)");
    assert_eq!(result(&record, "plan")["json"], json!({ "days": null }));
    assert_eq!(result(&record, "mood")["score"], Json::Null);
}

#[test]
fn a_route_s_where_or_stop_ends_the_run_as_a_switch_s_would() {
    let w = r"
name: r
steps:
  - name: sort
    classify:
      credential: model
      text: x
      categories:
        - name: spam
          steps: [{ stop: {} }]
        - ham
  - name: after
    set: { ok: 'true' }
";
    let record = go(w, json!({}), &Model::saying(&[r#"{"category": "spam"}"#]));
    assert_eq!(record.outcome, Outcome::Done);
    assert_eq!(names(&record), ["sort", "stop_2"]);
    let record = go(w, json!({}), &Model::saying(&[r#"{"category": "ham"}"#]));
    assert_eq!(names(&record), ["sort", "after"]);
}

#[test]
fn a_mistake_in_an_ai_step_is_found_when_the_workflow_is_read() {
    let wrong =
        |step: &str| Workflow::parse(&format!("name: x\nsteps:\n  - name: s\n    {step}\n")).unwrap_err();
    for (step, said) in [
        (
            "classify: { text: hi, categories: [a] }",
            "classify.credential: the model's credential",
        ),
        (
            "classify: { credential: m, categories: [a] }",
            "classify.text: what to work on",
        ),
        (
            "classify: { credential: m, text: hi, categories: [] }",
            "classify.categories: 1 to 50 of them",
        ),
        (
            "classify: { credential: m, text: hi, categories: [a, A] }",
            "categories[1].name: \"A\" is there twice",
        ),
        (
            "classify: { credential: m, text: hi, categories: [Other] }",
            "\"Other\" is kept for the fallback",
        ),
        (
            "classify: { credential: m, text: hi, categories: [' a'] }",
            "with no spaces around it",
        ),
        (
            "classify: { credential: m, text: hi, categories: [a], other: false, default: [] }",
            "which other: false leaves out",
        ),
        (
            "classify: { credential: m, text: hi, categories: [{ name: a, colour: red }] }",
            "categories[0].colour: not one of its settings",
        ),
        (
            "classify: { credential: m, text: hi, categories: [a], several: 'yes' }",
            "classify.several: true or false",
        ),
        (
            "classify: { credential: m, text: hi, categories: [a], prompt: x }",
            "classify.prompt: not a setting of classify",
        ),
        (
            "sentiment: { credential: m, text: hi, labels: [good] }",
            "sentiment.labels: 2 to 50 of them",
        ),
        (
            "extract: { credential: m, text: hi }",
            "extract.fields: the fields to pull out",
        ),
        (
            "extract: { credential: m, text: hi, fields: [a], schema: {} }",
            "fields or a schema, not both",
        ),
        (
            "extract: { credential: m, text: hi, fields: [{ name: a, type: money }] }",
            "fields[0].type: string, number, integer, boolean, date, list, not \"money\"",
        ),
        (
            "extract: { credential: m, text: hi, fields: [a, a] }",
            "fields[1].name: \"a\" is there twice",
        ),
        (
            "extract: { credential: m, text: hi, fields: ['1a'] }",
            "fields[0].name: letters, digits and '_'",
        ),
        (
            "extract: { credential: m, text: hi, schema: { type: text } }",
            "extract.schema: $.type: \"text\" is not a type",
        ),
        (
            "summarise: { credential: m, text: hi, piece_size: 10 }",
            "summarise.piece_size: the characters of each piece",
        ),
        (
            "ai: { credential: m, prompt: hi, schema: { if: {} } }",
            "ai.schema: $: \"if\" is not a keyword OpenDuct checks",
        ),
        (
            "ai: { credential: m, prompt: hi, schema: {}, colour: red }",
            "ai.colour: not a setting of ai",
        ),
        (
            "classify: { credential: m, text: hi, categories: [{ name: a, steps: [{ name: s, set: { x: '1' } }] }] }",
            "another step has this name",
        ),
    ] {
        let error = wrong(step);
        assert!(error.contains(said), "{step}: {error:?} does not say {said:?}");
    }
    let mut many = String::from("classify: { credential: m, text: hi, categories: [");
    many.push_str(&(0..51).map(|i| format!("c{i}")).collect::<Vec<_>>().join(", "));
    many.push_str("] }");
    assert!(wrong(&many).contains("1 to 50 of them"));
}

/// A stand-in server answering each request in turn with the next of `bodies`, keeping what it was sent.
fn answering(bodies: Vec<String>) -> (String, std::thread::JoinHandle<Vec<String>>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = std::thread::spawn(move || {
        let mut got_all = Vec::new();
        for body in bodies {
            let (mut stream, _) = listener.accept().unwrap();
            let mut got = Vec::new();
            let mut buf = vec![0u8; 65536];
            loop {
                let n = stream.read(&mut buf).unwrap();
                got.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&got);
                if let Some((head, rest)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if rest.len() >= length {
                        break;
                    }
                }
            }
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).unwrap();
            got_all.push(String::from_utf8_lossy(&got).into_owned());
        }
        got_all
    });
    (addr, handle)
}

#[derive(Debug)]
struct Kept(std::collections::BTreeMap<String, openduct_flow::Credential>);

impl openduct_flow::Secrets for Kept {
    fn get(&self, name: &str) -> Option<openduct_flow::Credential> {
        self.0.get(name).cloned()
    }
}

#[test]
fn over_http_a_messages_api_model_is_asked_again_and_its_key_kept_out_of_the_run() {
    // The other API shape: a `system` beside the messages, the answer's text in content parts.
    let answer = |text: &str| {
        json!({ "model": "m-1", "content": [{ "type": "text", "text": text }], "stop_reason": "end_turn", "usage": { "input_tokens": 9, "output_tokens": 2 } }).to_string()
    };
    let (addr, server) = answering(vec![
        answer("billing, I think"),
        answer(r#"{"category": "billing"}"#),
    ]);
    let mut kept = std::collections::BTreeMap::new();
    kept.insert(
        "model".to_owned(),
        openduct_flow::Credential {
            kind: "ai".to_owned(),
            fields: [
                ("base_url".to_owned(), format!("http://{addr}/v1")),
                ("api".to_owned(), "messages".to_owned()),
                ("api_key".to_owned(), "key-SECRET-1".to_owned()),
                ("key_header".to_owned(), "x-api-key".to_owned()),
                ("model".to_owned(), "m-1".to_owned()),
            ]
            .into(),
        },
    );
    let live = Live::new("127.0.0.1:1", true)
        .unwrap()
        .with_secrets(std::sync::Arc::new(Kept(kept)));
    let sort = "name: s\nsteps:\n  - name: sort\n    classify:\n      credential: model\n      text: '{{ value.text }}'\n      categories:\n        - { name: billing, steps: [{ name: to_billing, set: { to: \"'billing'\" } }] }\n        - bug\n";
    let record = go(sort, json!({ "text": "Refund please" }), &live);
    let sent = server.join().unwrap();
    assert_eq!(record.outcome, Outcome::Done, "{:?}", record.outcome);
    assert_eq!(names(&record), ["sort", "to_billing"]);
    assert!(sent[0].starts_with("POST /v1/messages"), "{}", sent[0]);
    assert!(sent[0].to_ascii_lowercase().contains("x-api-key: key-secret-1"));
    let body: Json = serde_json::from_str(sent[1].split_once("\r\n\r\n").unwrap().1).unwrap();
    assert!(
        body["system"]
            .as_str()
            .unwrap()
            .contains("Answer with one JSON value and nothing else.")
    );
    assert_eq!(
        body["messages"].as_array().unwrap().len(),
        3,
        "the text, the first answer, what to put right"
    );
    assert_eq!(result(&record, "sort")["usage"]["total_tokens"], 22);
    assert!(
        !format!("{:?}", record.steps).contains("key-SECRET-1"),
        "the key is in no step's record"
    );
}
