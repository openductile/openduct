// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Text with values in it: `New order {{ value.id }} for {{ value.total }}`. What is between `{{` and `}}` is an
//! expression; the rest is text as written. A template that is one expression and nothing else (`{{ value.lines }}`)
//! gives that expression's value as it is - a number, a list, a record - so a field can carry any value. A template
//! with no braces is plain text.

use openduct_expr::{Compiled, Env, Value};

/// The most expressions one template holds.
const MAX_PARTS: usize = 64;

#[derive(Debug, Clone)]
enum Part {
    Text(String),
    Value(Compiled),
}

/// A compiled template.
#[derive(Debug, Clone)]
pub struct Template {
    parts: Vec<Part>,
}

impl Template {
    /// Reads `source`; a mistake in an expression says where it is.
    pub fn compile(source: &str, language: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut rest = source;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                parts.push(Part::Text(rest.get(..open).unwrap_or_default().to_owned()));
            }
            let after = rest.get(open + 2..).unwrap_or_default();
            let close = after
                .find("}}")
                .ok_or_else(|| "a {{ with no }} after it".to_owned())?;
            let expression = after.get(..close).unwrap_or_default().trim();
            if expression.is_empty() {
                return Err("{{ }} with nothing in it".to_owned());
            }
            let compiled = openduct_expr::compile(language, expression)
                .map_err(|e| format!("in {{{{ {expression} }}}}: {e}"))?;
            parts.push(Part::Value(compiled));
            if parts.len() > MAX_PARTS {
                return Err(format!("more than {MAX_PARTS} {{{{ }}}} in one text"));
            }
            rest = after.get(close + 2..).unwrap_or_default();
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_owned()));
        }
        Ok(Self { parts })
    }

    /// A template of text alone.
    pub fn text(text: &str) -> Self {
        Self {
            parts: vec![Part::Text(text.to_owned())],
        }
    }

    /// Its value: the expression's own value when it is one expression alone, else the text it makes.
    pub fn value(&self, env: &Env<'_>) -> Result<Value, String> {
        if let [Part::Value(only)] = self.parts.as_slice() {
            return only.evaluate(env);
        }
        self.render(env).map(|t| Value::text(t.as_str()))
    }

    /// The text it makes: values written as text (`null` as nothing, records and lists as JSON).
    pub fn render(&self, env: &Env<'_>) -> Result<String, String> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Text(t) => out.push_str(t),
                Part::Value(e) => out.push_str(&as_text(&e.evaluate(env)?)),
            }
        }
        Ok(out)
    }
}

/// A value as text in a message: text as it is, nothing for null, anything else as JSON.
pub fn as_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Text(t) => t.to_string(),
        other => other.to_json(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order() -> Value {
        Value::parse_json(r#"{"id": "A-1", "total": 24.5, "lines": [1, 2], "note": null}"#).unwrap()
    }

    #[test]
    fn text_with_values_in_it() {
        let t = Template::compile("Order {{ id }} for {{ total * 2 }}{{ note }}!", "cel").unwrap();
        assert_eq!(t.render(&Env::of(&order())).unwrap(), "Order A-1 for 49!");
        let only = Template::compile("{{ lines }}", "cel").unwrap();
        assert_eq!(
            only.value(&Env::of(&order())).unwrap().to_json(),
            "[1,2]",
            "one expression keeps its value"
        );
        assert_eq!(
            Template::compile("plain", "cel")
                .unwrap()
                .value(&Env::of(&order()))
                .unwrap()
                .to_json(),
            r#""plain""#
        );
    }

    #[test]
    fn a_mistake_says_which_expression() {
        assert!(Template::compile("a {{ id", "cel").unwrap_err().contains("no }}"));
        assert!(
            Template::compile("a {{ }}", "cel")
                .unwrap_err()
                .contains("nothing in it")
        );
        assert!(
            Template::compile("{{ nosuch(1) }}", "cel")
                .unwrap_err()
                .starts_with("in {{ nosuch(1) }}:")
        );
    }
}
