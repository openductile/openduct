// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An answer to a form, checked on the server as its page checks it in the browser: each field's kind, whether it
//! must be filled in, its lengths, bounds and choices, a file's size and kind - and an approval's decision.

use base64::Engine as _;
use serde_json::{Map, Value as Json, json};

use super::{Field, FieldKind, Form, MAX_DESCRIPTION, MAX_FILES};

/// One value an answer gives for a field: text, or a file.
#[derive(Debug, Clone)]
pub enum Given {
    Text(String),
    File {
        name: String,
        content_type: String,
        bytes: Vec<u8>,
    },
}

/// What is wrong with an answer: each field's problem, said to the person filling it in.
pub type Problems = Vec<(String, String)>;

/// Checks an answer against `form`, as its page did in the browser: the answers by field name (an approval's
/// decision and comment among them), or each field's problem.
pub fn check(form: &Form, given: &[(String, Given)]) -> Result<Json, Problems> {
    let mut answers = Map::new();
    let mut problems = Vec::new();
    let mut files = 0usize;
    for field in &form.fields {
        let values: Vec<&Given> = given
            .iter()
            .filter(|(n, _)| *n == field.name)
            .map(|(_, g)| g)
            .collect();
        match one(field, &values, &mut files) {
            Ok(v) => {
                answers.insert(field.name.clone(), v);
            }
            Err(why) => problems.push((field.name.clone(), why)),
        }
    }
    if files > MAX_FILES {
        problems.push((
            String::new(),
            format!(
                "The files are {} together: at most {} in all.",
                shown_size(files),
                shown_size(MAX_FILES)
            ),
        ));
    }
    if let Some(approval) = &form.approval {
        let decision = given.iter().find_map(|(n, g)| match g {
            Given::Text(t) if n == "_decision" => Some(t.as_str()),
            _ => None,
        });
        match decision {
            Some("approve" | "reject") => {
                let approved = decision == Some("approve");
                answers.insert("approved".to_owned(), Json::Bool(approved));
                answers.insert(
                    "decision".to_owned(),
                    json!(if approved { "approved" } else { "rejected" }),
                );
            }
            _ => problems.push((
                String::new(),
                format!("Choose {} or {}.", approval.approve, approval.reject),
            )),
        }
        let comment = given
            .iter()
            .find_map(|(n, g)| match g {
                Given::Text(t) if n == "_comment" => Some(t.trim()),
                _ => None,
            })
            .unwrap_or_default();
        if comment.chars().count() > MAX_DESCRIPTION {
            problems.push((
                "_comment".to_owned(),
                format!("At most {MAX_DESCRIPTION} characters."),
            ));
        }
        answers.insert(
            "comment".to_owned(),
            json!(if approval.comment { comment } else { "" }),
        );
    }
    if problems.is_empty() {
        Ok(Json::Object(answers))
    } else {
        Err(problems)
    }
}

fn shown_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{} KB", bytes.div_ceil(1024))
    }
}

/// One field's answer, or what is wrong with it.
fn one(field: &Field, values: &[&Given], files: &mut usize) -> Result<Json, String> {
    let texts: Vec<&str> = values
        .iter()
        .filter_map(|g| match g {
            Given::Text(t) => Some(t.as_str()),
            Given::File { .. } => None,
        })
        .collect();
    match field.kind {
        FieldKind::Checkbox if !field.options.is_empty() => {
            if texts.len() > field.options.len() {
                return Err("Too many choices.".to_owned());
            }
            let mut chosen = Vec::new();
            for t in texts {
                if !field.options.iter().any(|o| o == t) {
                    return Err("Choose from the choices given.".to_owned());
                }
                if !chosen.contains(&t) {
                    chosen.push(t);
                }
            }
            if field.required && chosen.is_empty() {
                return Err("Choose at least one.".to_owned());
            }
            Ok(json!(chosen))
        }
        FieldKind::Checkbox => {
            let ticked = texts.iter().any(|t| !t.is_empty());
            if field.required && !ticked {
                return Err("Please tick this.".to_owned());
            }
            Ok(Json::Bool(ticked))
        }
        FieldKind::File => file(field, values, files),
        _ => {
            if texts.len() > 1 {
                return Err("One answer only.".to_owned());
            }
            let raw = texts.first().copied().unwrap_or_default();
            let value = if field.kind == FieldKind::LongText {
                raw.replace("\r\n", "\n").trim().to_owned()
            } else {
                raw.trim().to_owned()
            };
            if value.is_empty() {
                return if field.required {
                    Err("Please fill this in.".to_owned())
                } else {
                    Ok(Json::Null)
                };
            }
            if value
                .chars()
                .any(|c| c.is_control() && !(c == '\n' && field.kind == FieldKind::LongText))
            {
                return Err("Plain text, please.".to_owned());
            }
            if value.chars().count() > field.longest() {
                return Err(format!("At most {} characters.", field.longest()));
            }
            typed(field, value)
        }
    }
}

/// A text answer as its field's kind reads it.
fn typed(field: &Field, value: String) -> Result<Json, String> {
    match field.kind {
        FieldKind::Number => {
            let n: rust_decimal::Decimal = value.parse().map_err(|_| "A number, please.".to_owned())?;
            let bound =
                |b: &Option<String>| b.as_deref().and_then(|b| b.parse::<rust_decimal::Decimal>().ok());
            if let Some(min) = bound(&field.min).filter(|min| n < *min) {
                return Err(format!("At least {min}."));
            }
            if let Some(max) = bound(&field.max).filter(|max| n > *max) {
                return Err(format!("At most {max}."));
            }
            Ok(serde_json::from_str::<Json>(&n.normalize().to_string()).unwrap_or(Json::String(value)))
        }
        FieldKind::Email => {
            let ok = value.len() <= 254
                && !value.contains(char::is_whitespace)
                && value.split_once('@').is_some_and(|(local, domain)| {
                    !local.is_empty()
                        && !domain.contains('@')
                        && domain.contains('.')
                        && !domain.starts_with('.')
                        && !domain.ends_with('.')
                });
            if ok {
                Ok(Json::String(value))
            } else {
                Err("An e-mail address, as name@example.com.".to_owned())
            }
        }
        FieldKind::Date => {
            let day = chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .map_err(|_| "A date, as 2026-01-31.".to_owned())?;
            let bound = |b: &Option<String>| {
                b.as_deref()
                    .and_then(|b| chrono::NaiveDate::parse_from_str(b, "%Y-%m-%d").ok())
            };
            if let Some(min) = bound(&field.min).filter(|min| day < *min) {
                return Err(format!("On {min} or after."));
            }
            if let Some(max) = bound(&field.max).filter(|max| day > *max) {
                return Err(format!("On {max} or before."));
            }
            Ok(Json::String(value))
        }
        FieldKind::Dropdown => {
            if field.options.contains(&value) {
                Ok(Json::String(value))
            } else {
                Err("Choose from the choices given.".to_owned())
            }
        }
        _ => Ok(Json::String(value)),
    }
}

fn file(field: &Field, values: &[&Given], files: &mut usize) -> Result<Json, String> {
    let sent: Vec<(&String, &String, &Vec<u8>)> = values
        .iter()
        .filter_map(|g| match g {
            Given::File {
                name,
                content_type,
                bytes,
            } if !name.is_empty() || !bytes.is_empty() => Some((name, content_type, bytes)),
            _ => None,
        })
        .collect();
    let Some((name, content_type, bytes)) = (match sent.as_slice() {
        [] => None,
        [one] => Some(*one),
        _ => return Err("One file only.".to_owned()),
    }) else {
        return if field.required {
            Err("Please choose a file.".to_owned())
        } else {
            Ok(Json::Null)
        };
    };
    if bytes.len() > field.largest() {
        return Err(format!("A file of at most {}.", shown_size(field.largest())));
    }
    if !accepts(&field.accept, name, content_type) {
        return Err(format!("A file of the kinds {}.", field.accept));
    }
    *files = files.saturating_add(bytes.len());
    let name: String = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    Ok(json!({
        "name": name,
        "type": content_type.chars().take(100).collect::<String>(),
        "size": bytes.len(),
        "base64": base64::engine::general_purpose::STANDARD.encode(bytes),
    }))
}

/// Whether a file fits a field's `accept`: none takes any; else its name's ending or its type must be named.
fn accepts(accept: &str, name: &str, content_type: &str) -> bool {
    let wanted: Vec<String> = accept
        .split(',')
        .map(|a| a.trim().to_ascii_lowercase())
        .filter(|a| !a.is_empty())
        .collect();
    if wanted.is_empty() {
        return true;
    }
    let name = name.to_ascii_lowercase();
    let kind = content_type.to_ascii_lowercase();
    wanted.iter().any(|w| {
        if w.starts_with('.') {
            name.ends_with(w.as_str())
        } else if let Some(family) = w.strip_suffix("/*") {
            kind.split('/').next() == Some(family)
        } else {
            kind == *w
        }
    })
}

/// The answer a test run or a preview stands in with: each field's default, or a sample of its kind.
pub fn sample(form: &Form) -> Json {
    let mut out = Map::new();
    for f in &form.fields {
        let value = if !f.default.is_empty() {
            typed(f, f.default.clone()).unwrap_or(Json::String(f.default.clone()))
        } else {
            match f.kind {
                FieldKind::Text | FieldKind::LongText => json!("sample"),
                FieldKind::Number => f
                    .min
                    .as_deref()
                    .and_then(|m| serde_json::from_str(m).ok())
                    .unwrap_or(json!(1)),
                FieldKind::Email => json!("person@example.com"),
                FieldKind::Date => json!(chrono::Utc::now().format("%Y-%m-%d").to_string()),
                FieldKind::Dropdown => f.options.first().map_or(Json::Null, |o| json!(o)),
                FieldKind::Checkbox if f.options.is_empty() => json!(true),
                FieldKind::Checkbox => f.options.first().map_or(json!([]), |o| json!([o])),
                FieldKind::File => Json::Null,
            }
        };
        out.insert(f.name.clone(), value);
    }
    if form.approval.is_some() {
        out.insert("approved".to_owned(), json!(true));
        out.insert("decision".to_owned(), json!("approved"));
        out.insert("comment".to_owned(), json!(""));
    }
    Json::Object(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use openduct_expr::{Env, Value};

    use super::*;
    use crate::forms::{FormSpec, Use};

    fn spec(json: Json, kind: Use) -> Result<FormSpec, String> {
        FormSpec::read(json.as_object().unwrap(), "form", openduct_expr::DEFAULT, kind)
    }

    fn rendered(json: Json) -> Form {
        let names: Vec<(String, Value)> = Vec::new();
        let env = Env {
            value: &Value::Null,
            message: &Value::Null,
            names: &names,
        };
        spec(json, Use::Step).unwrap().render(&env).unwrap()
    }

    fn texts(pairs: &[(&str, &str)]) -> Vec<(String, Given)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), Given::Text((*v).to_owned())))
            .collect()
    }

    #[test]
    fn an_answer_is_checked_field_by_field_as_the_page_checks_it() {
        let form = rendered(json!({
            "title": "Sign up",
            "fields": [
                { "name": "name", "required": true, "max_length": 10 },
                { "name": "email", "type": "email", "required": true },
                { "name": "age", "type": "number", "min": 18, "max": 120 },
                { "name": "day", "type": "date", "min": "2026-01-01" },
                { "name": "size", "type": "dropdown", "options": ["S", "M"] },
                { "name": "terms", "type": "checkbox", "required": true },
                { "name": "extras", "type": "checkbox", "options": ["gift", "card"] },
                { "name": "about", "type": "long_text" },
            ],
        }));
        let good = check(
            &form,
            &texts(&[
                ("name", " Ada "),
                ("email", "ada@example.com"),
                ("age", "36"),
                ("day", "2026-02-03"),
                ("size", "M"),
                ("terms", "on"),
                ("extras", "gift"),
                ("extras", "card"),
                ("about", "line one\r\nline two"),
                ("unknown", "ignored"),
            ]),
        )
        .unwrap();
        assert_eq!(
            good,
            json!({
                "name": "Ada", "email": "ada@example.com", "age": 36, "day": "2026-02-03", "size": "M",
                "terms": true, "extras": ["gift", "card"], "about": "line one\nline two",
            })
        );
        let problems = check(
            &form,
            &texts(&[
                ("name", "Ada Lovelace Byron"),
                ("email", "ada@"),
                ("age", "12"),
                ("day", "2025-12-31"),
                ("size", "XL"),
                ("extras", "pony"),
            ]),
        )
        .unwrap_err();
        let fields: Vec<&str> = problems.iter().map(|(f, _)| f.as_str()).collect();
        assert_eq!(fields, ["name", "email", "age", "day", "size", "terms", "extras"]);
        assert_eq!(problems[0].1, "At most 10 characters.");
        assert_eq!(problems[2].1, "At least 18.");
        assert_eq!(problems[5].1, "Please tick this.");
        // Empty answers are none; a required one is missing.
        let empty = check(
            &form,
            &texts(&[("name", "  "), ("email", "a@b.test"), ("terms", "on")]),
        )
        .unwrap_err();
        assert_eq!(
            empty,
            vec![("name".to_owned(), "Please fill this in.".to_owned())]
        );
        // One answer to a field that takes one; no control characters in a line of text.
        assert!(
            check(
                &form,
                &texts(&[
                    ("name", "A"),
                    ("name", "B"),
                    ("email", "a@b.test"),
                    ("terms", "on")
                ])
            )
            .is_err()
        );
        assert!(
            check(
                &form,
                &texts(&[("name", "A\u{0}"), ("email", "a@b.test"), ("terms", "on")])
            )
            .is_err()
        );
    }

    #[test]
    fn a_file_is_kept_whole_within_its_limits() {
        let form = rendered(json!({
            "title": "Upload",
            "fields": [{ "name": "cv", "type": "file", "required": true, "max_size": "1KB", "accept": ".pdf,image/*" }],
        }));
        let sent = |name: &str, kind: &str, bytes: Vec<u8>| {
            vec![(
                "cv".to_owned(),
                Given::File {
                    name: name.to_owned(),
                    content_type: kind.to_owned(),
                    bytes,
                },
            )]
        };
        let ok = check(
            &form,
            &sent("C:\\me\\cv.pdf", "application/pdf", vec![0, 1, 2, 255]),
        )
        .unwrap();
        assert_eq!(
            ok["cv"],
            json!({ "name": "cv.pdf", "type": "application/pdf", "size": 4, "base64": "AAEC/w==" })
        );
        assert!(check(&form, &sent("photo.png", "image/png", vec![1])).is_ok());
        assert_eq!(
            check(&form, &sent("big.pdf", "application/pdf", vec![0; 2048])).unwrap_err()[0].1,
            "A file of at most 1 KB."
        );
        assert!(check(&form, &sent("run.exe", "application/octet-stream", vec![1])).is_err());
        assert_eq!(
            check(&form, &sent("", "application/octet-stream", vec![])).unwrap_err()[0].1,
            "Please choose a file."
        );
    }

    #[test]
    fn an_approval_takes_a_decision_and_a_comment() {
        let names: Vec<(String, Value)> = Vec::new();
        let env = Env {
            value: &Value::Null,
            message: &Value::Null,
            names: &names,
        };
        let form = spec(json!({ "title": "Refund?", "comment": true }), Use::Approval)
            .unwrap()
            .render(&env)
            .unwrap();
        let yes = check(&form, &texts(&[("_decision", "approve"), ("_comment", " fine ")])).unwrap();
        assert_eq!(
            yes,
            json!({ "approved": true, "decision": "approved", "comment": "fine" })
        );
        let no = check(&form, &texts(&[("_decision", "reject")])).unwrap();
        assert_eq!(no["decision"], "rejected");
        assert!(check(&form, &texts(&[("_decision", "maybe")])).is_err());
        assert_eq!(sample(&form)["approved"], true);
    }
}
