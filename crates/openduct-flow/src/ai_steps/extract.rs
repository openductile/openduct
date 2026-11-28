// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `extract` - named fields pulled out of a text - and an `ai` step with a `schema`: in both, the answer is JSON that
//! must meet the schema.

use serde_json::{Map, Value as Json, json};

use super::{Answered, Call, Did, result, shown};
use crate::apps::put;

/// The kinds of field `extract` takes, and the JSON schema each is.
pub(crate) const FIELD_TYPES: &[&str] = &["string", "number", "integer", "boolean", "date", "list"];

/// One field of an `extract` step, as its `fields` give it.
#[derive(Debug, Clone)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) description: Option<String>,
    pub(crate) required: bool,
}

/// The JSON schema of `fields`: a record of them, each one null when the text does not give it - unless required.
pub(crate) fn schema_of(fields: &[Field]) -> Json {
    let mut properties = Map::new();
    for field in fields {
        let mut one = match field.kind.as_str() {
            "date" => json!({ "type": "string", "format": "date" }),
            "list" => json!({ "type": "array", "items": { "type": "string" } }),
            other => json!({ "type": other }),
        };
        if !field.required
            && let Some(kind) = one.get("type").cloned()
        {
            put(&mut one, "type", json!([kind, "null"]));
        }
        if let Some(d) = field.description.as_deref().filter(|d| !d.trim().is_empty()) {
            put(&mut one, "description", json!(d.trim()));
        }
        properties.insert(field.name.clone(), one);
    }
    let names: Vec<&String> = fields.iter().map(|f| &f.name).collect();
    json!({ "type": "object", "properties": properties, "required": names, "additionalProperties": false })
}

/// Pulls the fields of `schema` out of `text`.
pub(crate) async fn extract(call: &Call<'_>, text: &str, schema: &Json, simple: bool) -> Result<Did, String> {
    let system = format!(
        "Pull the fields of this JSON schema out of the text the user gives:\n{}\nTake every value from the text \
         and invent none{}. Answer with JSON only: one value that meets the schema.",
        serde_json::to_string_pretty(schema).unwrap_or_default(),
        if simple {
            "; a field the text does not give is null"
        } else {
            ""
        }
    );
    // A field left out is one the text does not give, and one the step does not name is dropped.
    let tidy = |answer: &mut Json| {
        if !simple {
            return;
        }
        let Some(properties) = schema.get("properties").and_then(Json::as_object) else {
            return;
        };
        if let Some(fields) = answer.as_object_mut() {
            fields.retain(|k, _| properties.contains_key(k));
            for name in properties.keys() {
                fields.entry(name.clone()).or_insert(Json::Null);
            }
        }
    };
    match call
        .ask_json(call.fields(&system, text, true), schema, &tidy)
        .await?
    {
        Answered::Shown(request) => Ok(Did {
            result: shown(json!({ "json": crate::schema::blank(schema) }), request),
            routes: Vec::new(),
        }),
        Answered::Json { json, result: answer } => Ok(Did {
            result: result(json!({ "json": json }), &answer),
            routes: Vec::new(),
        }),
    }
}

/// An `ai` step with a `schema`: its own fields, its answer held to the schema; its result the `ai` step's, `json`
/// the answer.
pub(crate) async fn ask(call: &Call<'_>, schema: &Json) -> Result<Did, String> {
    let mut fields = call.fields.clone();
    let system = fields
        .get("system")
        .map(crate::template::as_text)
        .unwrap_or_default();
    let rule = format!(
        "Answer with JSON only: one value that meets this JSON schema:\n{}",
        serde_json::to_string_pretty(schema).unwrap_or_default()
    );
    let said = if system.trim().is_empty() {
        rule
    } else {
        format!("{}\n\n{rule}", system.trim_end())
    };
    fields.insert("system".to_owned(), openduct_expr::Value::text(said.as_str()));
    fields.insert("json".to_owned(), openduct_expr::Value::Bool(true));
    match call.ask_json(fields, schema, &|_: &mut Json| {}).await? {
        Answered::Shown(request) => Ok(Did {
            result: shown(
                json!({ "text": "(the model's answer, in a real run)", "json": crate::schema::blank(schema) }),
                request,
            ),
            routes: Vec::new(),
        }),
        Answered::Json {
            json,
            result: mut answer,
        } => {
            put(&mut answer, "json", json);
            Ok(Did {
                result: openduct_expr::Value::from_json(&answer),
                routes: Vec::new(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_make_a_schema_where_only_the_required_ones_may_not_be_null() {
        let field = |name: &str, kind: &str, required: bool| Field {
            name: name.to_owned(),
            kind: kind.to_owned(),
            description: Some(format!("the {name}")),
            required,
        };
        let schema = schema_of(&[
            field("email", "string", true),
            field("employees", "integer", false),
            field("since", "date", false),
            field("tags", "list", false),
        ]);
        crate::schema::check(&schema).unwrap();
        assert_eq!(
            schema["properties"]["email"],
            json!({ "type": "string", "description": "the email" })
        );
        assert_eq!(
            schema["properties"]["employees"]["type"],
            json!(["integer", "null"])
        );
        assert_eq!(schema["properties"]["since"]["format"], "date");
        assert_eq!(schema["properties"]["tags"]["items"], json!({ "type": "string" }));
        assert_eq!(schema["required"], json!(["email", "employees", "since", "tags"]));
        let ok = json!({ "email": "a@b.example", "employees": null, "since": "2020-01-31", "tags": ["x"] });
        assert_eq!(crate::schema::validate(&schema, &ok), Ok(()));
        let missing = json!({ "email": null, "employees": 4, "since": null, "tags": null });
        assert_eq!(
            crate::schema::validate(&schema, &missing),
            Err(vec!["$.email: string, not null".to_owned()])
        );
    }
}
