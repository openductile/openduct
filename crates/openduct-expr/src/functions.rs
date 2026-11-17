// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The functions OpenDuct gives expressions beside CEL's standard library and its math, sets, encoders and bindings
//! extensions: the text functions of CEL's strings extension (`lowerAscii`, `upperAscii`, `trim`, `replace`,
//! `split`, `substring`, `indexOf`, `lastIndexOf`, `charAt`, `join`), with its names and meaning - written here, not
//! taken from the extension, so that every text they make is at most [`MAX_TEXT`] long and no chain of them can fill
//! the memory (the extension's `replace` has no such limit), with its `format` - and, OpenDuct's own,
//! `round(x, places)` (rounding as people round money), `list.sum()`, `sha256(text)` (to stand in for personal data), `timestamp(seconds)` for seconds that are not whole and
//! `timestamp(text, layout)` for times not written as RFC 3339.
//! Every number they take may be whole or not, as every number in OpenDuct's expressions is a decimal.

use std::sync::Arc;

use cel::common::types::{CelDouble, CelString, CelTimestamp, DOUBLE_TYPE, STRING_TYPE};
use cel::common::value::CowVal;
use cel::extractors::{Arguments, This};
use cel::{Context, Env, ExecutionError, FunctionContext, Value};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use sha2::{Digest, Sha256};

/// The longest text a function makes, in bytes.
pub(crate) const MAX_TEXT: usize = 1024 * 1024;

/// What `a / b` and `a % b` compile to: a division by zero is an error, and `%` works on decimals. Names an
/// expression cannot write, so they are reached only through the operators.
pub(crate) const DIVIDE: &str = "_/_decimal";
pub(crate) const REMAINDER: &str = "_%_decimal";

/// The functions added here: with the environment's own, the names an expression may call.
pub(crate) const ADDED: &[&str] = &[
    "round",
    "lowerAscii",
    "upperAscii",
    "trim",
    "replace",
    "split",
    "substring",
    "indexOf",
    "lastIndexOf",
    "charAt",
    "join",
    "format",
    "sum",
    "sha256",
    "json.decode",
];

/// Adds them to a root context. Fails only if a name is already declared by the environment.
pub(crate) fn add(ctx: &mut Context<'_, '_>) -> Result<(), cel::DeclarationError> {
    ctx.add_function("round", round)?;
    ctx.add_function("lowerAscii", |This(s): This<Arc<String>>| s.to_ascii_lowercase())?;
    ctx.add_function("upperAscii", |This(s): This<Arc<String>>| s.to_ascii_uppercase())?;
    ctx.add_function("trim", |This(s): This<Arc<String>>| s.trim().to_owned())?;
    ctx.add_function("replace", replace)?;
    ctx.add_function("split", split)?;
    ctx.add_function("substring", substring)?;
    ctx.add_function("indexOf", |This(s): This<Arc<String>>, part: Arc<String>| {
        s.find(part.as_str()).map_or(-1.0, |at| chars_before(&s, at))
    })?;
    ctx.add_function("lastIndexOf", |This(s): This<Arc<String>>, part: Arc<String>| {
        s.rfind(part.as_str()).map_or(-1.0, |at| chars_before(&s, at))
    })?;
    ctx.add_function("charAt", char_at)?;
    ctx.add_function("join", join)?;
    ctx.add_function("format", format)?;
    ctx.add_function("sum", sum)?;
    ctx.add_function("sha256", sha256)?;
    ctx.add_function("json.decode", json_decode)?;
    ctx.add_function(DIVIDE, |ftx: &FunctionContext, a: Value, b: Value| {
        arithmetic(ftx, "divide", &a, &b, |x, y| x / y)
    })?;
    ctx.add_function(REMAINDER, |ftx: &FunctionContext, a: Value, b: Value| {
        arithmetic(ftx, "take the remainder of", &a, &b, |x, y| x % y)
    })?;
    Ok(())
}

/// Adds to the environment what extends CEL's own functions: `timestamp(seconds)` beside `timestamp(int)`, as a
/// message's numbers are decimals, and `timestamp(text, layout)` beside `timestamp(text)`.
pub(crate) fn add_to_env(env: &mut Env) -> Result<(), cel::DeclarationError> {
    env.add_overload_with("timestamp", "double_to_timestamp", vec![DOUBLE_TYPE], |args| {
        let seconds = args
            .first()
            .and_then(|a| a.downcast_ref::<CelDouble>())
            .map_or(f64::NAN, |d| *d.inner());
        let at = if seconds.is_finite() && seconds.abs() < 253_402_300_800.0 {
            let whole = seconds.floor();
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            // in range; a fraction of a second
            let nanos = (((seconds - whole) * 1e9).round() as u32).min(999_999_999);
            #[allow(clippy::cast_possible_truncation)] // checked in range above
            chrono::DateTime::from_timestamp(whole as i64, nanos)
        } else {
            None
        };
        at.map(|t| CowVal::owned(CelTimestamp::from(t.fixed_offset())))
            .ok_or_else(|| ExecutionError::function_error("timestamp", "seconds out of range"))
    })?;
    env.add_overload_with(
        "timestamp",
        "string_layout_to_timestamp",
        vec![STRING_TYPE, STRING_TYPE],
        |args| {
            let text = args
                .first()
                .and_then(|a| a.downcast_ref::<CelString>())
                .map_or("", |s| s.inner());
            let layout = args
                .get(1)
                .and_then(|a| a.downcast_ref::<CelString>())
                .map_or("", |s| s.inner());
            read_time(text, layout)
                .map(|t| CowVal::owned(CelTimestamp::from(t)))
                .ok_or_else(|| {
                    ExecutionError::function_error(
                        "timestamp",
                        format!("{text:?} is not a time written as {layout:?}"),
                    )
                })
        },
    )
}

/// `timestamp(text, layout)`: a time written another way than RFC 3339, in strftime's layout (`%Y-%m-%d %H:%M:%S`,
/// `%d/%b/%Y:%H:%M:%S %z`). Without a zone in the layout the time is UTC; a date alone is its midnight.
fn read_time(text: &str, layout: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_str(text, layout)
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(text, layout)
                .ok()
                .map(|t| t.and_utc().fixed_offset())
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(text, layout)
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|t| t.and_utc().fixed_offset())
        })
}

/// A number argument, whole or not.
pub(crate) fn number(ftx: &FunctionContext, v: &Value) -> Result<f64, ExecutionError> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(n) => n.to_f64().ok_or_else(|| ftx.error("a number too large")),
        Value::UInt(n) => n.to_f64().ok_or_else(|| ftx.error("a number too large")),
        Value::Null => Err(null_given("a number")),
        other => Err(ftx.error(format!("needs a number, not {}", kind(other)))),
    }
}

/// `a / b` or `a % b` on two numbers: an error for a division by zero, null for null (the whole answer, see
/// [`crate::cel`]).
fn arithmetic(
    ftx: &FunctionContext,
    doing: &str,
    a: &Value,
    b: &Value,
    op: impl Fn(f64, f64) -> f64,
) -> Result<Value, ExecutionError> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Err(null_given("a number"));
    }
    let is_number = |v: &Value| matches!(v, Value::Float(_) | Value::Int(_) | Value::UInt(_));
    if !is_number(a) || !is_number(b) {
        return Err(ftx.error(format!("cannot {doing} {} and {}", kind(a), kind(b))));
    }
    let (x, y) = (number(ftx, a)?, number(ftx, b)?);
    if y == 0.0 {
        return Err(ftx.error("a division by zero"));
    }
    Ok(Value::Float(op(x, y)))
}

/// A position or a count: a whole number, not negative.
fn whole(ftx: &FunctionContext, v: &Value) -> Result<usize, ExecutionError> {
    let n = number(ftx, v)?;
    if n < 0.0 || n.fract() != 0.0 || n > 1e15 {
        return Err(ftx.error(format!("needs a whole number, not {n}")));
    }
    n.to_usize().ok_or_else(|| ftx.error("a number too large"))
}

/// A function given null: the whole answer is null (see [`crate::cel`]).
pub(crate) fn null_given(wanted: &str) -> ExecutionError {
    ExecutionError::UnexpectedType {
        got: "Null".to_owned(),
        want: wanted.to_owned(),
    }
}

pub(crate) fn kind(v: &Value) -> &'static str {
    match v {
        Value::List(_) => "a list",
        Value::Map(_) => "a record",
        Value::Function(..) => "a function",
        Value::Int(_) | Value::UInt(_) | Value::Float(_) => "a number",
        Value::String(_) => "text",
        Value::Bytes(_) => "bytes",
        Value::Bool(_) => "true or false",
        Value::Duration(_) => "a duration",
        Value::Timestamp(_) => "a time",
        Value::Opaque(_) | Value::Struct(_) => "an object",
        Value::Null => "null",
    }
}

pub(crate) fn text(ftx: &FunctionContext, s: String) -> Result<Value, ExecutionError> {
    if s.len() > MAX_TEXT {
        return Err(ftx.error(format!(
            "would make text of {} bytes: at most {MAX_TEXT}",
            s.len()
        )));
    }
    Ok(Value::String(Arc::new(s)))
}

/// `round(x)`, `round(x, places)`: halves away from zero, on the decimal as it reads (`round(2.675, 2)` is
/// `2.68`, as people round money).
fn round(ftx: &FunctionContext, Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    let x = number(
        ftx,
        args.first().ok_or_else(|| ftx.error("round needs a number"))?,
    )?;
    let places = match args.get(1) {
        None => 0,
        Some(p) => whole(ftx, p)?,
    };
    if args.len() > 2 || places > 12 {
        return Err(ftx.error("round takes a number and at most 12 decimal places"));
    }
    let exact = Decimal::from_str_exact(&format!("{x}"))
        .ok()
        .or_else(|| Decimal::from_f64(x));
    let Some(exact) = exact else {
        return Ok(Value::Float(x.round()));
    };
    let rounded = exact.round_dp_with_strategy(
        u32::try_from(places).unwrap_or(0),
        RoundingStrategy::MidpointAwayFromZero,
    );
    Ok(Value::Float(rounded.to_f64().unwrap_or(x)))
}

fn replace(
    ftx: &FunctionContext,
    This(s): This<Arc<String>>,
    old: Arc<String>,
    new: Arc<String>,
) -> Result<Value, ExecutionError> {
    // Check the size before making it: an empty `old` matches between every character.
    let matches = if old.is_empty() {
        s.chars().count() + 1
    } else {
        s.matches(old.as_str()).count()
    };
    let size = s.len() - matches.saturating_mul(old.len()).min(s.len()) + matches.saturating_mul(new.len());
    if size > MAX_TEXT {
        return Err(ftx.error(format!("would make text of {size} bytes: at most {MAX_TEXT}")));
    }
    text(ftx, s.replace(old.as_str(), new.as_str()))
}

fn split(This(s): This<Arc<String>>, sep: Arc<String>) -> Result<Value, ExecutionError> {
    let parts: Vec<Value> = if sep.is_empty() {
        s.chars()
            .map(|c| Value::String(Arc::new(c.to_string())))
            .collect()
    } else {
        s.split(sep.as_str())
            .map(|p| Value::String(Arc::new(p.to_owned())))
            .collect()
    };
    Ok(Value::List(Arc::new(parts)))
}

/// `substring(start)`, `substring(start, end)`: by character, `end` not included.
fn substring(
    ftx: &FunctionContext,
    This(s): This<Arc<String>>,
    Arguments(args): Arguments,
) -> Result<Value, ExecutionError> {
    let count = s.chars().count();
    let start = whole(
        ftx,
        args.first().ok_or_else(|| ftx.error("substring needs a start"))?,
    )?;
    let end = match args.get(1) {
        None => count,
        Some(e) => whole(ftx, e)?,
    };
    if args.len() > 2 || start > end || end > count {
        return Err(ftx.error(format!(
            "substring({start}, {end}) of text {count} characters long"
        )));
    }
    text(ftx, s.chars().skip(start).take(end - start).collect())
}

fn char_at(ftx: &FunctionContext, This(s): This<Arc<String>>, at: Value) -> Result<Value, ExecutionError> {
    let at = whole(ftx, &at)?;
    let count = s.chars().count();
    match s.chars().nth(at) {
        Some(c) => Ok(Value::String(Arc::new(c.to_string()))),
        None if at == count => Ok(Value::String(Arc::new(String::new()))),
        None => Err(ftx.error(format!("charAt({at}) of text {count} characters long"))),
    }
}

/// `list.join()`, `list.join(separator)`: a list of text as one text.
fn join(
    ftx: &FunctionContext,
    This(list): This<Arc<Vec<Value>>>,
    Arguments(args): Arguments,
) -> Result<Value, ExecutionError> {
    let sep = match args.first() {
        None => String::new(),
        Some(Value::String(s)) => s.to_string(),
        Some(other) => return Err(ftx.error(format!("join's separator is text, not {}", kind(other)))),
    };
    let mut out = String::new();
    for (i, item) in list.iter().enumerate() {
        let Value::String(s) = item else {
            return Err(ftx.error(format!("join takes a list of text; item {i} is {}", kind(item))));
        };
        if i > 0 {
            out.push_str(&sep);
        }
        out.push_str(s);
        if out.len() > MAX_TEXT {
            return Err(ftx.error(format!("would make text over {MAX_TEXT} bytes")));
        }
    }
    text(ftx, out)
}

/// The characters before byte `at` of `s`, as a CEL number.
fn chars_before(s: &str, at: usize) -> f64 {
    s.get(..at)
        .map_or(0, |before| before.chars().count())
        .to_f64()
        .unwrap_or(0.0)
}

/// `template.format([values])`: the strings extension's clauses - `%s` (any value, as text), `%d` (a whole number),
/// `%f` and `%e` (with places: `%.2f`), `%x` and `%X` (hexadecimal), `%o`, `%b`, and `%%` for a percent sign.
fn format(
    ftx: &FunctionContext,
    This(template): This<Arc<String>>,
    values: Arc<Vec<Value>>,
) -> Result<Value, ExecutionError> {
    let mut out = String::new();
    let mut rest = template.as_str();
    let mut next = 0;
    while let Some(at) = rest.find('%') {
        out.push_str(rest.get(..at).unwrap_or_default());
        rest = rest.get(at + 1..).unwrap_or_default();
        if let Some(after) = rest.strip_prefix('%') {
            out.push('%');
            rest = after;
            continue;
        }
        let (places, after) = match rest.strip_prefix('.') {
            None => (None, rest),
            Some(r) => {
                let digits = r.bytes().take_while(u8::is_ascii_digit).count();
                let places: usize = r
                    .get(..digits)
                    .and_then(|d| d.parse().ok())
                    .filter(|p| *p <= 20)
                    .ok_or_else(|| ftx.error("places after a '.' are 0 to 20, as in %.2f"))?;
                (Some(places), r.get(digits..).unwrap_or_default())
            }
        };
        let Some(clause) = after.chars().next() else {
            return Err(ftx.error("a % at the end: %% writes a percent sign"));
        };
        rest = after.get(clause.len_utf8()..).unwrap_or_default();
        let value = values
            .get(next)
            .ok_or_else(|| ftx.error(format!("has more clauses than the {} values given", values.len())))?;
        next += 1;
        out.push_str(&clause_text(ftx, clause, places, value)?);
        if out.len() > MAX_TEXT {
            return Err(ftx.error(format!("would make text over {MAX_TEXT} bytes")));
        }
    }
    out.push_str(rest);
    text(ftx, out)
}

fn clause_text(
    ftx: &FunctionContext,
    clause: char,
    places: Option<usize>,
    v: &Value,
) -> Result<String, ExecutionError> {
    let whole_number = |v: &Value| -> Result<i128, ExecutionError> {
        let n = number(ftx, v)?;
        if n.fract() != 0.0 || n.abs() > 1e30 {
            return Err(ftx.error(format!("%{clause} needs a whole number, not {n}")));
        }
        #[allow(clippy::cast_possible_truncation)] // whole and in range
        Ok(n as i128)
    };
    Ok(match clause {
        's' => display(v),
        'd' => whole_number(v)?.to_string(),
        'f' => {
            let p = places.unwrap_or(6);
            finite(number(ftx, v)?, |x| format!("{x:.p$}"))
        }
        'e' => {
            let p = places.unwrap_or(6);
            finite(number(ftx, v)?, |x| scientific(x, p))
        }
        'x' | 'X' => {
            let hex = match v {
                Value::String(s) => s.bytes().map(|b| format!("{b:02x}")).collect(),
                Value::Bytes(b) => b.iter().map(|b| format!("{b:02x}")).collect(),
                other => {
                    let n = whole_number(other)?;
                    let sign = if n < 0 { "-" } else { "" };
                    format!("{sign}{:x}", n.unsigned_abs())
                }
            };
            if clause == 'X' { hex.to_uppercase() } else { hex }
        }
        'o' => {
            let n = whole_number(v)?;
            format!("{}{:o}", if n < 0 { "-" } else { "" }, n.unsigned_abs())
        }
        'b' => match v {
            Value::Bool(b) => u8::from(*b).to_string(),
            other => {
                let n = whole_number(other)?;
                format!("{}{:b}", if n < 0 { "-" } else { "" }, n.unsigned_abs())
            }
        },
        other => {
            return Err(ftx.error(format!(
                "%{other} is not a clause: %s, %d, %f, %e, %x, %X, %o, %b, or %% for a percent sign"
            )));
        }
    })
}

fn finite(x: f64, write: impl Fn(f64) -> String) -> String {
    if x.is_nan() {
        "NaN".to_owned()
    } else if x.is_infinite() {
        if x > 0.0 {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        }
    } else {
        write(x)
    }
}

/// `1.500000e+03`, as C and CEL write it.
fn scientific(x: f64, places: usize) -> String {
    let written = format!("{x:.places$e}");
    match written.split_once('e') {
        Some((mantissa, exponent)) => {
            let e: i32 = exponent.parse().unwrap_or(0);
            format!(
                "{mantissa}e{}{:02}",
                if e < 0 { '-' } else { '+' },
                e.unsigned_abs()
            )
        }
        None => written,
    }
}

/// A value as `%s` writes it: text as it is, numbers as briefly as they read, records and lists as JSON.
fn display(v: &Value) -> String {
    match v {
        Value::String(s) => s.to_string(),
        Value::Float(f) => finite(*f, |x| format!("{x}")),
        Value::Int(n) => n.to_string(),
        Value::UInt(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_owned(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Timestamp(t) => t.to_utc().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        Value::Duration(d) => crate::convert::duration(*d),
        other => {
            crate::convert::from_cel(other.clone()).map_or_else(|_| kind(other).to_owned(), |v| v.to_json())
        }
    }
}

/// `list.sum()`: the numbers added, as `+` adds them, nulls left out as SQL's SUM leaves them; an empty list sums to 0.
fn sum(ftx: &FunctionContext, This(list): This<Arc<Vec<Value>>>) -> Result<Value, ExecutionError> {
    let mut total = 0.0;
    for (i, item) in list.iter().enumerate() {
        total += match item {
            Value::Float(_) | Value::Int(_) | Value::UInt(_) => number(ftx, item)?,
            Value::Null => continue,
            other => return Err(ftx.error(format!("adds a list of numbers; item {i} is {}", kind(other)))),
        };
    }
    Ok(Value::Float(total))
}

/// `json.decode(text)`: the value JSON text holds - a form field or a header that carries JSON (Jotform's
/// `rawRequest`), read as a record, a list, text, a number, true or false.
fn json_decode(ftx: &FunctionContext, v: Value) -> Result<Value, ExecutionError> {
    let text = match &v {
        Value::String(s) => s.clone(),
        Value::Null => return Err(null_given("text")),
        other => return Err(ftx.error(format!("needs JSON text, not {}", kind(other)))),
    };
    let value = openduct_value::Value::parse_json(&text).map_err(|e| ftx.error(format!("not JSON: {e}")))?;
    Ok(crate::convert::to_cel(&value))
}

/// `sha256(text)`: the SHA-256 of text (as UTF-8) or bytes, in lowercase hexadecimal.
fn sha256(ftx: &FunctionContext, v: Value) -> Result<Value, ExecutionError> {
    let digest = match &v {
        Value::String(s) => Sha256::digest(s.as_bytes()),
        Value::Bytes(b) => Sha256::digest(b.as_slice()),
        Value::Null => return Err(null_given("text")),
        other => return Err(ftx.error(format!("needs text, not {}", kind(other)))),
    };
    Ok(Value::String(Arc::new(
        digest.iter().map(|b| format!("{b:02x}")).collect(),
    )))
}
