// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! One message through a pipeline: `let`, `where`, `each`, `map`, then `key` and `headers`.

use std::sync::Arc;

use openduct_expr::{Compiled, Env, Record, Value};

use crate::config::{Format, Pipeline};

/// A message read from the topic.
#[derive(Debug, Clone, Copy)]
pub struct Message<'a> {
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [(String, Vec<u8>)],
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

/// A message to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    pub key: Option<String>,
    pub value: Vec<u8>,
    /// The headers read, with the pipeline's own added.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Only the headers the pipeline added, for `pipeline test`.
    pub added_headers: Vec<(String, String)>,
}

/// Why one message could not go through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub reason: String,
}

fn fail(step: &str, why: impl std::fmt::Display) -> Failure {
    Failure {
        reason: format!("{step}: {why}"),
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

impl Pipeline {
    /// The messages this one becomes: none (dropped by `where`, or `each` over an empty list), one, or several.
    pub fn process(&self, message: &Message<'_>) -> Result<Vec<Output>, Failure> {
        let value = match (self.format, message.value) {
            (_, None) => Value::Null,
            (Format::Json, Some(bytes)) => {
                let json = std::str::from_utf8(bytes).map_err(|_| fail("value", "is not UTF-8 text"))?;
                Value::parse_json(json).map_err(|e| fail("value", format!("is not valid JSON: {e}")))?
            }
            (Format::Text, Some(bytes)) => {
                Value::text(std::str::from_utf8(bytes).map_err(|_| fail("value", "is not UTF-8 text"))?)
            }
        };
        let details = details(message);

        let mut names: Vec<(String, Value)> = Vec::with_capacity(self.lets.len() + 1);
        for (name, expression) in &self.lets {
            let answer =
                run(expression, &value, &details, &names).map_err(|e| fail(&format!("let.{name}"), e))?;
            names.push((name.clone(), answer));
        }
        if let Some(filter) = &self.filter {
            match run(filter, &value, &details, &names).map_err(|e| fail("where", e))? {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => return Ok(Vec::new()),
                other => {
                    return Err(fail(
                        "where",
                        format!("must be true or false, but it is {}", other.describe()),
                    ));
                }
            }
        }
        let items = match &self.each {
            None => vec![None],
            Some(each) => match run(each, &value, &details, &names).map_err(|e| fail("each", e))? {
                Value::Null => Vec::new(),
                Value::List(items) => items.iter().cloned().map(Some).collect(),
                other => {
                    return Err(fail(
                        "each",
                        format!("must be a list, but it is {}", other.describe()),
                    ));
                }
            },
        };
        let mut outputs = Vec::with_capacity(items.len());
        for item in items {
            let mut scope = names.clone();
            if let Some(item) = &item {
                scope.push(("item".to_owned(), item.clone()));
            }
            outputs.push(self.build(message, &value, item.as_ref(), &details, &scope)?);
        }
        Ok(outputs)
    }

    fn build(
        &self,
        message: &Message<'_>,
        value: &Value,
        item: Option<&Value>,
        details: &Value,
        names: &[(String, Value)],
    ) -> Result<Output, Failure> {
        let written = match &self.map {
            Some(fields) => {
                let mut record = Record::new();
                for (field, expression) in fields {
                    let answer = run(expression, value, details, names)
                        .map_err(|e| fail(&format!("map.{field}"), e))?;
                    record.insert(field.clone(), answer);
                }
                Value::record(record)
            }
            None => item.cloned().unwrap_or_else(|| value.clone()),
        };
        // Unchanged messages keep their exact bytes; anything built is written as JSON.
        let bytes = match (&self.map, item, self.format) {
            (None, None, Format::Text) | (None, None, Format::Json) => {
                message.value.map(<[u8]>::to_vec).unwrap_or_default()
            }
            _ => written.to_json().into_bytes(),
        };
        let key = match &self.key {
            None => message.key.map(text),
            Some(expression) => {
                match run(expression, &written, details, names).map_err(|e| fail("key", e))? {
                    Value::Null => None,
                    v => Some(v.to_text()),
                }
            }
        };
        let mut headers: Vec<(String, Vec<u8>)> = message.headers.to_vec();
        let mut added = Vec::new();
        for (name, expression) in &self.headers {
            let answer =
                run(expression, &written, details, names).map_err(|e| fail(&format!("headers.{name}"), e))?;
            if answer.is_null() {
                continue;
            }
            let shown = answer.to_text();
            headers.retain(|(n, _)| n != name);
            headers.push((name.clone(), shown.clone().into_bytes()));
            added.push((name.clone(), shown));
        }
        Ok(Output {
            key,
            value: bytes,
            headers,
            added_headers: added,
        })
    }
}

fn run(
    expression: &Compiled,
    value: &Value,
    details: &Value,
    names: &[(String, Value)],
) -> Result<Value, String> {
    expression.evaluate(&Env {
        value,
        message: details,
        names,
    })
}

/// `message`: the key, headers, topic, partition, offset and timestamp, for expressions that ask.
fn details(message: &Message<'_>) -> Value {
    let mut headers = Record::new();
    for (name, value) in message.headers {
        headers.insert(name.clone(), Value::text(text(value)));
    }
    let mut record = Record::new();
    record.insert(
        "key".to_owned(),
        message.key.map_or(Value::Null, |k| Value::text(text(k))),
    );
    record.insert("headers".to_owned(), Value::Record(Arc::new(headers)));
    record.insert("topic".to_owned(), Value::text(message.topic));
    record.insert("partition".to_owned(), Value::number(message.partition.into()));
    record.insert("offset".to_owned(), Value::number(message.offset.into()));
    record.insert(
        "timestamp".to_owned(),
        chrono_time(message.timestamp_ms).map_or(Value::Null, Value::Time),
    );
    Value::record(record)
}

fn chrono_time(millis: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp_millis(millis)
}

/// `openduct pipeline test`: each input line through the pipeline, written as the lines the command prints - one per
/// message written (`{"key":...,"value":...}`) and one per message that failed (`{"line":3,"error":"..."}`).
pub fn test_lines(pipeline: &Pipeline, input: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (at, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let number = at + 1;
        let message = Message {
            key: None,
            value: Some(line.as_bytes()),
            headers: &[],
            topic: &pipeline.from_topic,
            partition: 0,
            offset: i64::try_from(at).unwrap_or(i64::MAX),
            timestamp_ms: 0,
        };
        match pipeline.process(&message) {
            Ok(outputs) => {
                for output in outputs {
                    let key = output.key.as_deref().map_or_else(|| "null".to_owned(), json_text);
                    let raw = String::from_utf8_lossy(&output.value).into_owned();
                    let value = if Value::parse_json(&raw).is_ok() {
                        raw
                    } else {
                        json_text(&raw)
                    };
                    let mut line = format!("{{\"key\":{key},\"value\":{value}");
                    if !output.added_headers.is_empty() {
                        let fields: Vec<String> = output
                            .added_headers
                            .iter()
                            .map(|(n, v)| format!("{}:{}", json_text(n), json_text(v)))
                            .collect();
                        line.push_str(&format!(",\"headers\":{{{}}}", fields.join(",")));
                    }
                    line.push('}');
                    out.push(line);
                }
            }
            Err(failure) => out.push(format!(
                "{{\"line\":{number},\"error\":{}}}",
                json_text(&failure.reason)
            )),
        }
    }
    out
}

fn json_text(text: &str) -> String {
    Value::text(text).to_json()
}
