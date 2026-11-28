// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `classify` and `sentiment`: a text sorted into labels the step names, the answer held to them (a name in another
//! case is put right, anything else asked again), and each chosen label's steps run after it.

use serde_json::{Value as Json, json};

use super::{Answered, Call, Did, Label, Route, result, shown};

/// The category a model answers when none of the step's fits.
pub(crate) const OTHER: &str = "other";

/// What a test run, which asks no model, shows for its choice.
const NOT_ASKED: &str = "(the model's choice, in a real run)";

/// The labels as the model reads them: one a line, with what each means.
fn listed(labels: &[Label], other: Option<&str>) -> String {
    let mut out = String::new();
    for label in labels {
        out.push_str("- ");
        out.push_str(&label.name);
        if let Some(d) = label.description.as_deref().filter(|d| !d.trim().is_empty()) {
            out.push_str(": ");
            out.push_str(d.trim());
        }
        out.push('\n');
    }
    if let Some(other) = other {
        out.push_str(&format!("- {OTHER}: {other}\n"));
    }
    out
}

/// A name as the step has it: the model's answer matched without regard to case or spaces around it.
fn named(names: &[String], said: &Json) -> Json {
    match said.as_str() {
        Some(text) => names
            .iter()
            .find(|n| n.eq_ignore_ascii_case(text.trim()))
            .map_or_else(|| said.clone(), |n| Json::String(n.clone())),
        None => said.clone(),
    }
}

/// Sorts `text` into one of `labels` (or every one that fits, with `several`); with `other`, the model may say
/// that none does.
pub(crate) async fn sort(
    call: &Call<'_>,
    text: &str,
    labels: &[Label],
    several: bool,
    other: bool,
) -> Result<Did, String> {
    let mut names: Vec<String> = labels.iter().map(|l| l.name.clone()).collect();
    if other {
        names.push(OTHER.to_owned());
    }
    let list = listed(labels, other.then_some("none of the categories above fits"));
    let (system, schema) = if several {
        (
            format!(
                "Sort the text the user gives into every one of these categories that fits it:\n{list}\nAnswer \
                 with JSON only: {{\"categories\": [\"<a category's name>\", ...]}}{}.",
                if other {
                    ", or [\"other\"] when none fits"
                } else {
                    ", at least one"
                }
            ),
            json!({
                "type": "object",
                "properties": { "categories": { "type": "array", "items": { "enum": names }, "minItems": 1 } },
                "required": ["categories"],
            }),
        )
    } else {
        (
            format!(
                "Sort the text the user gives into one of these categories:\n{list}\nAnswer with JSON only: \
                 {{\"category\": \"<the category's name>\"}}."
            ),
            json!({
                "type": "object",
                "properties": { "category": { "enum": names } },
                "required": ["category"],
            }),
        )
    };
    let tidy = |answer: &mut Json| {
        if let Some(fields) = answer.as_object_mut() {
            if let Some(one) = fields.get_mut("category") {
                *one = named(&names, one);
            }
            if let Some(Json::Array(many)) = fields.get_mut("categories") {
                for one in many.iter_mut() {
                    *one = named(&names, one);
                }
                // `other` beside a category that fits says nothing more; a name twice counts once.
                if many.iter().any(|c| c.as_str() != Some(OTHER)) {
                    many.retain(|c| c.as_str() != Some(OTHER));
                }
                let mut seen = Vec::new();
                many.retain(|c| {
                    let new = !seen.contains(c);
                    seen.push(c.clone());
                    new
                });
            }
        }
    };
    let asked = call
        .ask_json(call.fields(&system, text, true), &schema, &tidy)
        .await?;
    let (json, answer) = match asked {
        Answered::Shown(request) => {
            return Ok(Did {
                result: shown(json!({ "category": NOT_ASKED, "categories": [] }), request),
                routes: Vec::new(),
            });
        }
        Answered::Json { json, result } => (json, result),
    };
    let chosen: Vec<String> = if several {
        json.get("categories")
            .and_then(Json::as_array)
            .map(|l| l.iter().filter_map(Json::as_str).map(str::to_owned).collect())
            .unwrap_or_default()
    } else {
        json.get("category")
            .and_then(Json::as_str)
            .map(|c| vec![c.to_owned()])
            .unwrap_or_default()
    };
    let routes = labels
        .iter()
        .enumerate()
        .filter(|(_, l)| chosen.contains(&l.name))
        .map(|(i, _)| Route::Label(i))
        .chain(chosen.iter().any(|c| c == OTHER).then_some(Route::Other))
        .collect();
    let first = chosen.first().cloned().unwrap_or_else(|| OTHER.to_owned());
    Ok(Did {
        result: result(json!({ "category": first, "categories": chosen }), &answer),
        routes,
    })
}

/// The labels a sentiment step reads when it names none.
pub(crate) fn plain_labels() -> Vec<Label> {
    ["positive", "neutral", "negative"]
        .into_iter()
        .map(|name| Label {
            name: name.to_owned(),
            description: None,
            steps: Vec::new(),
        })
        .collect()
}

/// Reads the sentiment of `text` as one of `labels`, with how strongly the text carries it.
pub(crate) async fn sentiment(call: &Call<'_>, text: &str, labels: &[Label]) -> Result<Did, String> {
    let names: Vec<String> = labels.iter().map(|l| l.name.clone()).collect();
    let system = format!(
        "Read the sentiment of the text the user gives, as one of these labels:\n{}\nAnswer with JSON only: \
         {{\"label\": \"<the label>\", \"score\": <how strongly the text carries it, from 0 to 1>}}.",
        listed(labels, None)
    );
    let schema = json!({
        "type": "object",
        "properties": {
            "label": { "enum": names },
            "score": { "type": "number", "minimum": 0, "maximum": 1 },
        },
        "required": ["label", "score"],
    });
    let tidy = |answer: &mut Json| {
        if let Some(label) = answer.get_mut("label") {
            *label = named(&names, label);
        }
        // A score given as text ("0.8") is the number it says.
        if let Some(score) = answer.get_mut("score")
            && let Some(n) = score.as_str().and_then(|s| s.trim().parse::<f64>().ok())
        {
            *score = json!(n);
        }
    };
    let asked = call
        .ask_json(call.fields(&system, text, true), &schema, &tidy)
        .await?;
    let (json, answer) = match asked {
        Answered::Shown(request) => {
            return Ok(Did {
                result: shown(json!({ "label": NOT_ASKED, "score": null }), request),
                routes: Vec::new(),
            });
        }
        Answered::Json { json, result } => (json, result),
    };
    let label = json
        .get("label")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_owned();
    let routes = labels
        .iter()
        .position(|l| l.name == label)
        .map(Route::Label)
        .into_iter()
        .collect();
    let score = json.get("score").cloned().unwrap_or(Json::Null);
    Ok(Did {
        result: result(json!({ "label": label, "score": score }), &answer),
        routes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_in_another_case_is_the_step_s_own() {
        let names = vec!["Billing".to_owned(), "bug".to_owned()];
        assert_eq!(named(&names, &json!(" billing ")), json!("Billing"));
        assert_eq!(named(&names, &json!("BUG")), json!("bug"));
        assert_eq!(
            named(&names, &json!("refund")),
            json!("refund"),
            "anything else is left for the check"
        );
        assert_eq!(named(&names, &json!(3)), json!(3));
    }

    #[test]
    fn the_labels_read_one_a_line_with_what_each_means() {
        let labels = vec![
            Label {
                name: "billing".to_owned(),
                description: Some(" Payments and refunds ".to_owned()),
                steps: Vec::new(),
            },
            Label {
                name: "bug".to_owned(),
                description: None,
                steps: Vec::new(),
            },
        ];
        assert_eq!(
            listed(&labels, Some("none fits")),
            "- billing: Payments and refunds\n- bug\n- other: none fits\n"
        );
    }
}
