// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Expressions: what a pipeline or a connector works out for each message - a condition to keep it, a field to
//! write, a key, the records in a web page.
//!
//! The language is [CEL](https://cel.dev), the Common Expression Language: small, safe and an open standard. An
//! expression cannot loop forever, cannot reach files or the network, and cannot change the message it reads. OpenDuct
//! adds four rules of its own, so business data works as people expect (see [`cel`] and `docs/expressions.md`):
//! every number is a decimal (`12.5 * 2` is `25`), reading something a message does not have gives `null`, a few
//! functions for text and rounding, and limits on an expression's size.
//!
//! ```
//! use openduct_expr::{Env, Value, compile};
//!
//! let order = Value::parse_json(r#"{"qty": 3, "price": 12.5, "status": "paid"}"#).unwrap();
//! let total = compile("cel", "status == 'paid' ? qty * price : 0").unwrap();
//! assert_eq!(total.evaluate(&Env::of(&order)).unwrap().to_json(), "37.5");
//! ```
//!
//! Other languages plug in through [`Language`] and [`register`]; a pipeline names the one it is written in.

#![forbid(unsafe_code)]

use std::sync::{Arc, RwLock};

pub use openduct_value::{Record, Value};

pub mod cel;
mod convert;
mod functions;
mod guard;
mod patterns;

pub use guard::{MAX_LENGTH, MAX_NESTING, MAX_STRUCTURE};

/// What an expression reads: the message's value, its details (key, topic, partition, offset, time), and names worked
/// out before it (a pipeline's `let`, the `item` of `each`).
#[derive(Debug, Clone, Copy)]
pub struct Env<'a> {
    pub value: &'a Value,
    pub message: &'a Value,
    pub names: &'a [(String, Value)],
}

impl<'a> Env<'a> {
    /// A message value with no details and no names.
    pub fn of(value: &'a Value) -> Self {
        Self {
            value,
            message: &Value::Null,
            names: &[],
        }
    }

    /// What `name` means here: a name worked out before (the last one wins), the whole `value` or `message`, or a
    /// field of the value.
    pub fn lookup(&self, name: &str) -> Option<&'a Value> {
        if let Some((_, v)) = self.names.iter().rev().find(|(n, _)| n == name) {
            return Some(v);
        }
        match name {
            "value" => Some(self.value),
            "message" => Some(self.message),
            _ => match self.value {
                Value::Record(r) => r.get(name),
                _ => None,
            },
        }
    }
}

/// One compiled expression.
pub trait Expression: Send + Sync + std::fmt::Debug {
    /// The expression as written.
    fn source(&self) -> &str;
    /// Works it out for one message. The error says why, in words a person can act on.
    fn evaluate(&self, env: &Env<'_>) -> Result<Value, String>;
}

/// A compiled expression, shared by every worker that uses it.
pub type Compiled = Arc<dyn Expression>;

/// A language expressions are written in.
pub trait Language: Send + Sync + std::fmt::Debug {
    /// The name a pipeline gives (`language: <name>`).
    fn name(&self) -> &'static str;
    /// Reads and checks an expression: every mistake that would fail for every message is found here.
    fn compile(&self, source: &str) -> Result<Compiled, String>;
}

/// The language a pipeline uses when it names none.
pub const DEFAULT: &str = "cel";

fn others() -> &'static RwLock<Vec<Arc<dyn Language>>> {
    static OTHERS: RwLock<Vec<Arc<dyn Language>>> = RwLock::new(Vec::new());
    &OTHERS
}

/// Makes another language available to every pipeline of this process (a build of OpenDuct that carries one
/// registers it as it starts). A language of a name already registered replaces it.
pub fn register(language: Arc<dyn Language>) {
    if let Ok(mut all) = others().write() {
        all.retain(|l| l.name() != language.name());
        all.push(language);
    }
}

/// The language called `name`.
pub fn language(name: &str) -> Result<Arc<dyn Language>, String> {
    if name == DEFAULT {
        return Ok(Arc::new(cel::Cel));
    }
    let found = others()
        .read()
        .ok()
        .and_then(|all| all.iter().find(|l| l.name() == name).cloned());
    found.ok_or_else(|| {
        let mut have = vec![format!("{DEFAULT:?}")];
        if let Ok(all) = others().read() {
            have.extend(all.iter().map(|l| format!("{:?}", l.name())));
        }
        format!(
            "this build of OpenDuct has no expression language {name:?}: it has {}",
            have.join(" and ")
        )
    })
}

/// Compiles `source` in the language called `name`.
pub fn compile(name: &str, source: &str) -> Result<Compiled, String> {
    language(name)?.compile(source)
}
