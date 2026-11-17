// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CEL's regex extension, as the CEL project defines it, but for one thing: `regex.extract(text, pattern)` - the
//! first match, or its group in brackets - is null when nothing matches, not an optional, so it reads as every other
//! missing value in OpenDuct does (`double(regex.extract(...))` is null too). `regex.extractAll(text, pattern)` -
//! every match; `regex.replace(text, pattern, replacement)` and `regex.replace(text, pattern, replacement, count)` -
//! with `\1` ... `\9` for the groups and `\\` for a backslash.
//!
//! Patterns are RE2's syntax (no look-around, no back-references), so matching takes time in proportion to the text
//! however a pattern is written. A pattern compiles to at most 1 MiB, the texts made are at most
//! [`MAX_TEXT`] and the lists at most [`MAX_MATCHES`] long; the last patterns compiled on a thread are kept, so a
//! pipeline does not compile its patterns again for every message.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use cel::extractors::Arguments;
use cel::{Context, ExecutionError, FunctionContext, Value};
use regex::{Regex, RegexBuilder};

use crate::functions::{MAX_TEXT, null_given, number, text};

/// The most matches `regex.extractAll` lists.
pub(crate) const MAX_MATCHES: usize = 10_000;
/// The largest a pattern may compile to, in bytes.
const MAX_COMPILED: usize = 1024 * 1024;
/// How many compiled patterns a thread keeps.
const KEPT: usize = 64;

thread_local! {
    static COMPILED: RefCell<HashMap<String, Arc<Regex>>> = RefCell::new(HashMap::new());
}

pub(crate) const ADDED: &[&str] = &["regex.extract", "regex.extractAll", "regex.replace"];

pub(crate) fn add(ctx: &mut Context<'_, '_>) -> Result<(), cel::DeclarationError> {
    ctx.add_function(
        "regex.extract",
        |ftx: &FunctionContext, t: Arc<String>, p: Arc<String>| {
            extract(ftx, t, p).map_err(qualified("regex.extract"))
        },
    )?;
    ctx.add_function(
        "regex.extractAll",
        |ftx: &FunctionContext, t: Arc<String>, p: Arc<String>| {
            extract_all(ftx, t, p).map_err(qualified("regex.extractAll"))
        },
    )?;
    ctx.add_function("regex.replace", |ftx: &FunctionContext, args: Arguments| {
        replace(ftx, args).map_err(qualified("regex.replace"))
    })?;
    Ok(())
}

/// An error named by the function's whole name (`regex.replace`, not `replace`, which is the text function).
fn qualified(name: &'static str) -> impl Fn(ExecutionError) -> ExecutionError {
    move |e| match e {
        ExecutionError::FunctionError { message, .. } => ExecutionError::function_error(name, &message),
        other => other,
    }
}

fn pattern(ftx: &FunctionContext, source: &str) -> Result<Arc<Regex>, ExecutionError> {
    if let Some(found) = COMPILED.with(|c| c.borrow().get(source).cloned()) {
        return Ok(found);
    }
    let compiled = RegexBuilder::new(source)
        .size_limit(MAX_COMPILED)
        .dfa_size_limit(MAX_COMPILED)
        .build()
        .map_err(|e| {
            ftx.error(format!(
                "{source:?} is not a pattern: {}",
                first_line(&e.to_string())
            ))
        })?;
    let compiled = Arc::new(compiled);
    COMPILED.with(|c| {
        let mut kept = c.borrow_mut();
        if kept.len() >= KEPT {
            kept.clear();
        }
        kept.insert(source.to_owned(), Arc::clone(&compiled));
    });
    Ok(compiled)
}

/// The regex crate's errors draw the pattern over several lines; the last one says what is wrong.
fn first_line(message: &str) -> &str {
    message
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .map_or(message, |l| l.trim().trim_start_matches("error: "))
}

/// What a match gives: the whole match, or the text of the pattern's one group.
fn group(ftx: &FunctionContext, re: &Regex) -> Result<usize, ExecutionError> {
    match re.captures_len() {
        1 => Ok(0),
        2 => Ok(1),
        _ => {
            Err(ftx
                .error("takes a pattern with at most one group in brackets ((?:...) groups without taking)"))
        }
    }
}

fn extract(ftx: &FunctionContext, text: Arc<String>, source: Arc<String>) -> Result<Value, ExecutionError> {
    let re = pattern(ftx, &source)?;
    let at = group(ftx, &re)?;
    Ok(re
        .captures(&text)
        .and_then(|c| c.get(at))
        .map_or(Value::Null, |m| Value::String(Arc::new(m.as_str().to_owned()))))
}

fn extract_all(
    ftx: &FunctionContext,
    text: Arc<String>,
    source: Arc<String>,
) -> Result<Value, ExecutionError> {
    let re = pattern(ftx, &source)?;
    let at = group(ftx, &re)?;
    let mut found = Vec::new();
    for c in re.captures_iter(&text) {
        if let Some(m) = c.get(at) {
            if found.len() == MAX_MATCHES {
                return Err(ftx.error(format!("more than {MAX_MATCHES} matches")));
            }
            found.push(Value::String(Arc::new(m.as_str().to_owned())));
        }
    }
    Ok(Value::List(Arc::new(found)))
}

enum Part {
    Text(String),
    Group(usize),
}

/// A replacement: text, `\1` ... `\9` for a group (`\0` for the whole match), `\\` for a backslash.
fn replacement(ftx: &FunctionContext, with: &str, groups: usize) -> Result<Vec<Part>, ExecutionError> {
    let mut parts = Vec::new();
    let mut plain = String::new();
    let mut chars = with.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            plain.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => plain.push('\\'),
            Some(d) if d.is_ascii_digit() => {
                let n = usize::from(u8::try_from(d).unwrap_or(b'0') - b'0');
                if n >= groups {
                    return Err(ftx.error(format!(
                        "\\{n} in the replacement: the pattern has {} groups",
                        groups - 1
                    )));
                }
                if !plain.is_empty() {
                    parts.push(Part::Text(std::mem::take(&mut plain)));
                }
                parts.push(Part::Group(n));
            }
            _ => {
                return Err(ftx.error(
                    "a backslash in the replacement is \\1 ... \\9 for a group, or \\\\ for a backslash",
                ));
            }
        }
    }
    if !plain.is_empty() {
        parts.push(Part::Text(plain));
    }
    Ok(parts)
}

fn replace(ftx: &FunctionContext, Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    if args.iter().take(3).any(|a| matches!(a, Value::Null)) {
        return Err(null_given("text"));
    }
    let (Some(Value::String(text_in)), Some(Value::String(source)), Some(Value::String(with))) =
        (args.first(), args.get(1), args.get(2))
    else {
        return Err(ftx.error("takes text, a pattern and a replacement - and a count, if not every match"));
    };
    let count = match args.get(3) {
        None => usize::MAX,
        Some(n) => {
            let n = number(ftx, n)?;
            if n < 0.0 {
                usize::MAX
            } else if n.fract() == 0.0 && n <= 1e15 {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                // whole, not negative, small
                let n = n as usize;
                n
            } else {
                return Err(ftx.error(format!("needs a whole number of replacements, not {n}")));
            }
        }
    };
    if args.len() > 4 {
        return Err(ftx.error(format!("takes at most 4 values, not {}", args.len())));
    }
    let re = pattern(ftx, source)?;
    let parts = replacement(ftx, with, re.captures_len())?;
    let mut out = String::new();
    let mut last = 0;
    for c in re.captures_iter(text_in).take(count) {
        let Some(whole) = c.get(0) else { continue };
        out.push_str(text_in.get(last..whole.start()).unwrap_or_default());
        for part in &parts {
            match part {
                Part::Text(t) => out.push_str(t),
                Part::Group(n) => out.push_str(c.get(*n).map_or("", |m| m.as_str())),
            }
        }
        last = whole.end();
        if out.len() > MAX_TEXT {
            return Err(ftx.error(format!("would make text over {MAX_TEXT} bytes")));
        }
    }
    out.push_str(text_in.get(last..).unwrap_or_default());
    text(ftx, out)
}
