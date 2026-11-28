// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A JavaScript step: the code is the body of a function, so it `return`s its answer. The language alone - no
//! module loading, files, network, processes, timers or time-zone data - and `console.log` kept as the step's logs.

use boa_engine::property::Attribute;
use boa_engine::{Context, JsError, JsString, JsValue, Source};
use serde_json::Value as Json;

use crate::{Answer, Failure, Script, kept_logs};

/// `console`, its lines kept in a list the step reads back.
const PRELUDE: &str = r#"
var __openduct_logs = [];
globalThis.console = {
  log: function () {
    __openduct_logs.push(Array.prototype.map.call(arguments, function (a) {
      return typeof a === "string" ? a : JSON.stringify(a);
    }).join(" "));
  },
};
console.info = console.warn = console.error = console.debug = console.log;
"#;

fn shown(error: &JsError, context: &mut Context) -> String {
    let text = match error.try_native(context) {
        Ok(native) => native.to_string(),
        Err(_) => error.to_string(),
    };
    // What the engine adds is where the outermost call was - the step's own wrapping - not where the error was.
    match text.rfind(" (unknown at ") {
        Some(at) if text.ends_with(')') => text[..at].to_owned(),
        _ => text,
    }
}

/// The largest whole number a double holds exactly.
const EXACT: f64 = 9_007_199_254_740_992.0;

/// A JavaScript number is a double: one that is whole is written as a whole number, as `JSON.stringify` writes it.
fn whole_numbers(json: Json) -> Json {
    match json {
        Json::Number(n) if n.is_f64() => match n.as_f64() {
            Some(f) if f.fract() == 0.0 && f.abs() < EXACT => format!("{f:.0}")
                .parse::<i64>()
                .map_or(Json::Number(n), Json::from),
            _ => Json::Number(n),
        },
        Json::Array(items) => Json::Array(items.into_iter().map(whole_numbers).collect()),
        Json::Object(fields) => {
            Json::Object(fields.into_iter().map(|(k, v)| (k, whole_numbers(v))).collect())
        }
        other => other,
    }
}

fn logs(context: &mut Context) -> Vec<String> {
    let found = context
        .global_object()
        .get(JsString::from("__openduct_logs"), context)
        .ok()
        .and_then(|l| l.to_json(context).ok().flatten());
    let lines = match found {
        Some(Json::Array(items)) => items
            .into_iter()
            .map(|i| match i {
                Json::String(s) => s,
                other => other.to_string(),
            })
            .collect(),
        _ => Vec::new(),
    };
    kept_logs(lines)
}

pub(crate) fn run(script: &Script) -> Result<Answer, Failure> {
    let mut context = Context::default();
    for (name, value) in &script.names {
        let value =
            JsValue::from_json(value, &mut context).map_err(|e| Failure::new(shown(&e, &mut context)))?;
        context
            .register_global_property(JsString::from(name.as_str()), value, Attribute::all())
            .map_err(|e| Failure::new(shown(&e, &mut context)))?;
    }
    context
        .eval(Source::from_bytes(PRELUDE))
        .map_err(|e| Failure::new(shown(&e, &mut context)))?;
    // The code's first line stays the first line, so the line numbers in an error are the code's own.
    let wrapped = format!("(function () {{ {}\n}})()", script.code);
    let answered = context.eval(Source::from_bytes(wrapped.as_bytes()));
    let ran = answered.and_then(|value| {
        context.run_jobs()?;
        Ok(value)
    });
    match ran {
        Ok(value) => {
            let result = match value.to_json(&mut context) {
                Ok(json) => whole_numbers(json.unwrap_or(Json::Null)),
                Err(e) => {
                    return Err(Failure {
                        error: format!("the answer cannot be JSON: {}", shown(&e, &mut context)),
                        logs: logs(&mut context),
                    });
                }
            };
            // data tables: what the script left in `state`, when it was given one.
            let state = if script.names.contains_key(crate::STATE) {
                let left = context
                    .global_object()
                    .get(JsString::from(crate::STATE), &mut context)
                    .and_then(|v| v.to_json(&mut context));
                match left {
                    Ok(json) => Some(whole_numbers(json.unwrap_or(Json::Null))),
                    Err(e) => {
                        return Err(Failure {
                            error: format!("state cannot be JSON: {}", shown(&e, &mut context)),
                            logs: logs(&mut context),
                        });
                    }
                }
            } else {
                None
            };
            Ok(Answer {
                result,
                logs: logs(&mut context),
                state,
            })
        }
        Err(e) => Err(Failure {
            error: shown(&e, &mut context),
            logs: logs(&mut context),
        }),
    }
}
