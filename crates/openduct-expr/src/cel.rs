// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CEL, the Common Expression Language (<https://cel.dev>), as OpenDuct runs it, through the `cel` crate: CEL's
//! standard library, its math, sets, encoders and bindings extensions, and its strings extension's functions.
//!
//! OpenDuct's rules on top of the standard, each because business data needs it:
//! 1. **Every number is a decimal.** A message's numbers, the numbers written in an expression and the whole numbers
//!    CEL's functions answer (`size()`, `int()`, `getHours()`) are all CEL `double`s, so `price * 2` works when the
//!    price is `12.5` and `total / size(items)` is an average (standard CEL refuses to mix `int` and `double`). A
//!    division by zero is an error, not infinity, and `%` works on decimals. A whole number of more than 15 digits
//!    (an identifier) is kept exactly, as CEL's `int`, so it is written back as it came.
//! 2. **What a message does not have is `null`**, and an operator or a function given null makes the whole answer
//!    null, as in SQL (`temp_f - 32` is null when there is no reading) - instead of an error. `x == null` tests for
//!    it; `has(value.x)` tells whether a field is there at all. A wrong kind of value (`'two' * 2`) is still an
//!    error.
//! 3. **`round(x, places)`**, rounding halves away from zero on the decimal as it reads, as people round money.
//! 4. **Limits** ([`crate::guard`] and here): an expression's size, nesting and operators; at most three lists gone
//!    through one inside another; [`MAX_ITERATIONS`] steps through lists and [`TIME_LIMIT`] per message; an answer of
//!    at most 1 MiB. A function an expression calls must exist - checked when it is compiled.
//!
//! Limits measured on `cel` 0.16 with a release build hold on a 2 MiB thread stack; a debug build needs far more.
//!
//! A message's top-level fields are names of their own (`status`, `after.id`), as are `value` (the whole message),
//! `message` (its key, topic, partition, offset and time) and the names a pipeline worked out before (`let`, `item`).
//! A field whose name is not a CEL name is read as `value['first-name']`.

use std::cell::Cell;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use cel::common::ast::{CallExpr, EntryExpr, Expr, IdedExpr, LiteralValue};
use cel::common::types::CelDouble;
use cel::{Context, Env, ExecutionError, Interrupt, RuntimeOptions};
use openduct_value::Value;

use crate::{Compiled, Env as Message, Expression, Language, convert, functions, guard, patterns};

/// The deepest an expression's tree may be once parsed: a backstop behind [`crate::guard`], whose limits make trees of
/// up to about 260 levels (a chain of 256 operators). Measured on `cel` 0.16 (2026-11-16): a release build on a 2 MiB
/// thread stack evaluates chains of 512 operators and 2,048 nested brackets or lists.
pub const MAX_DEPTH: usize = 320;
/// The most lists gone through one inside another (`orders.map(o, o.lines.map(l, ...))` is two).
pub const MAX_LOOPS: usize = 3;
/// The most steps through lists one evaluation may take, all its lists together.
pub const MAX_ITERATIONS: u64 = 100_000;
/// How long one evaluation may take.
pub const TIME_LIMIT: Duration = Duration::from_millis(100);
/// The largest answer, in bytes as JSON.
pub const MAX_ANSWER: usize = 1024 * 1024;

/// The names of CEL's types: a message field of the name is read, but one it does not have is the type.
const TYPE_NAMES: &[&str] = &[
    "bool",
    "int",
    "uint",
    "double",
    "string",
    "bytes",
    "list",
    "map",
    "null_type",
    "type",
    "optional_type",
];

/// CEL's functions that answer a whole number: their answer is made a decimal.
const WHOLE_ANSWERS: &[&str] = &[
    "size",
    "int",
    "uint",
    "getFullYear",
    "getMonth",
    "getDayOfYear",
    "getDayOfMonth",
    "getDate",
    "getDayOfWeek",
    "getHours",
    "getMinutes",
    "getSeconds",
    "getMilliseconds",
];

/// The CEL language.
#[derive(Debug, Default, Clone, Copy)]
pub struct Cel;

/// CEL's standard library and the extensions OpenDuct uses, with the step budget. The `lists` and `strings`
/// extensions are left out: `lists.range(n)` makes a list of any size before a step is counted, and the strings
/// extension's `replace` makes text of any size - OpenDuct's own text functions take their place.
fn environment() -> &'static Arc<Env> {
    static ENV: OnceLock<Arc<Env>> = OnceLock::new();
    ENV.get_or_init(|| {
        let mut env = Env::stdlib();
        for add in [
            cel::extensions::math,
            cel::extensions::sets,
            cel::extensions::encoders,
            cel::extensions::bindings,
        ] {
            if let Err(e) = add(&mut env) {
                debug_assert!(false, "a CEL extension did not register: {e}");
            }
        }
        if let Err(e) = functions::add_to_env(&mut env) {
            debug_assert!(false, "a function did not register: {e}");
        }
        env.set_options(RuntimeOptions::default().with_max_iterations(MAX_ITERATIONS));
        Arc::new(env)
    })
}

thread_local! {
    /// When the evaluation running on this thread must stop.
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// The deadline of whichever evaluation runs on the thread asking.
struct ThreadDeadline;

impl Interrupt for ThreadDeadline {
    fn is_interrupted(&self) -> bool {
        DEADLINE.with(|d| d.get().is_some_and(|t| Instant::now() >= t))
    }
}

static THREAD_DEADLINE: ThreadDeadline = ThreadDeadline;

/// The root every evaluation's context is a scope of: the environment, the added functions and the deadline.
fn root() -> &'static Context<'static, 'static> {
    static ROOT: OnceLock<Context<'static, 'static>> = OnceLock::new();
    ROOT.get_or_init(|| {
        let mut ctx = Context::with_env(Arc::clone(environment()));
        if let Err(e) = functions::add(&mut ctx).and_then(|()| patterns::add(&mut ctx)) {
            debug_assert!(false, "a function did not register: {e}");
        }
        ctx.set_interrupt(&THREAD_DEADLINE);
        ctx
    })
}

fn exists(name: &str) -> bool {
    functions::ADDED.contains(&name)
        || patterns::ADDED.contains(&name)
        || environment().function(name).is_some()
}

impl Language for Cel {
    fn name(&self) -> &'static str {
        "cel"
    }

    fn compile(&self, source: &str) -> Result<Compiled, String> {
        guard::check(source)?;
        let mut expr = environment()
            .parser()
            .parse(source)
            .map_err(|e| syntax_error(source, &e))?;
        let spaces = check_tree(&expr)?;
        let mut next_id = u64::MAX / 2;
        as_decimals(&mut expr, &mut next_id);
        // A name the message does not have is null - unless it names a type (`double`) or functions (`math`).
        let variables = expr
            .references()
            .variables()
            .into_iter()
            .map(|n| {
                (
                    n.to_owned(),
                    !TYPE_NAMES.contains(&n) && !spaces.iter().any(|s| s == n),
                )
            })
            .collect();
        Ok(Arc::new(CelExpression {
            source: source.to_owned(),
            expr,
            variables,
        }))
    }
}

#[derive(Debug)]
struct CelExpression {
    source: String,
    expr: IdedExpr,
    /// The names it reads - only these are converted for each message - and whether one the message does not have is
    /// null.
    variables: Vec<(String, bool)>,
}

impl Expression for CelExpression {
    fn source(&self) -> &str {
        &self.source
    }

    fn evaluate(&self, env: &Message<'_>) -> Result<Value, String> {
        let mut ctx = root().new_inner_scope();
        for (name, null_if_absent) in &self.variables {
            match env.lookup(name) {
                Some(v) => ctx.add_variable_from_value(name.as_str(), convert::to_cel(v)),
                None if *null_if_absent => ctx.add_variable_from_value(name.as_str(), cel::Value::Null),
                None => {}
            }
        }
        DEADLINE.with(|d| d.set(Some(Instant::now() + TIME_LIMIT)));
        let result = cel::Value::resolve(&self.expr, &ctx);
        DEADLINE.with(|d| d.set(None));
        match result {
            Ok(v) => {
                let answer = convert::from_cel(v)?;
                if answer.size() > MAX_ANSWER {
                    return Err(format!(
                        "an answer of {} bytes: at most {MAX_ANSWER}",
                        answer.size()
                    ));
                }
                Ok(answer)
            }
            // Something the message does not have, or an operation on null: the answer is null.
            Err(e) if given_null(&e) => Ok(Value::Null),
            Err(ExecutionError::Interrupted) => {
                Err(format!("took longer than {} ms", TIME_LIMIT.as_millis()))
            }
            Err(e) => Err(describe(&e)),
        }
    }
}

/// Whether evaluating stopped at something the message does not have (a field, or a field of a field that is null),
/// or at an operator or a function given null: as in SQL, the whole answer is then null.
fn given_null(e: &ExecutionError) -> bool {
    match e {
        ExecutionError::NoSuchKey(_) | ExecutionError::UndeclaredReference(_) => true,
        ExecutionError::NoSuchOverload(o) => o.argument_types().iter().any(|t| t == "null_type"),
        ExecutionError::UnsupportedBinaryOperator(_, a, b) | ExecutionError::ValuesNotComparable(a, b) => {
            matches!(a, cel::Value::Null) || matches!(b, cel::Value::Null)
        }
        ExecutionError::UnexpectedType { got, .. } => got == "Null",
        _ => false,
    }
}

/// A mistake in how an expression is written, in plain words, with where it is:
///
/// ```text
/// the expression ends too soon, at character 6:
///   qty *
///        ^
/// ```
fn syntax_error(source: &str, errors: &cel::ParseErrors) -> String {
    let Some(first) = errors.errors.first() else {
        return "cannot be read".to_owned();
    };
    let msg = first.msg.trim();
    let msg = msg.strip_prefix("Syntax error: ").unwrap_or(msg);
    // The parser quotes the input it stopped at: `'x' expecting ...`, or `'''` when it is a quote itself.
    let quoted = |rest: &str| {
        let inner = rest.strip_prefix('\'').unwrap_or(rest);
        let end = inner
            .get(1..)
            .and_then(|after| after.find('\''))
            .map_or(inner.len(), |i| i + 1);
        inner.get(..end).unwrap_or_default().to_owned()
    };
    let why = if msg.starts_with("mismatched input '<EOF>'")
        || msg.starts_with("missing") && msg.ends_with("<EOF>'")
    {
        "the expression ends too soon".to_owned()
    } else if let Some(rest) = msg
        .strip_prefix("mismatched input ")
        .or_else(|| msg.strip_prefix("extraneous input "))
    {
        format!("{:?} is not expected here", quoted(rest))
    } else if let Some(rest) = msg.strip_prefix("no viable alternative at input ") {
        format!("cannot read {:?}", quoted(rest))
    } else if let Some(rest) = msg.strip_prefix("token recognition error at: ") {
        let token = quoted(rest);
        if token.starts_with(['\'', '"']) {
            "text that is never closed: it needs its closing quote".to_owned()
        } else {
            format!("{token:?} is not part of an expression")
        }
    } else {
        msg.to_owned()
    };
    let (line, column) = first.pos;
    let lines: Vec<&str> = source.lines().collect();
    let shown = usize::try_from(line - 1).ok().and_then(|l| lines.get(l));
    match (shown, usize::try_from(column)) {
        (Some(text), Ok(col)) if col > 0 => {
            let place = if lines.len() > 1 {
                format!("line {line}, character {col}")
            } else {
                format!("character {col}")
            };
            format!("{why}, at {place}:\n  {text}\n  {}^", " ".repeat(col - 1))
        }
        _ => why,
    }
}

/// An error in words a person can act on.
fn describe(e: &ExecutionError) -> String {
    match e {
        ExecutionError::UnsupportedBinaryOperator(op, a, b) if exact_and_decimal(a, b) => format!(
            "cannot {} a whole number of more than 15 digits (kept exactly, as identifiers are) and a decimal: \
             write double(...) to calculate with it",
            operation(op)
        ),
        ExecutionError::UnsupportedBinaryOperator(op, a, b) => format!(
            "cannot {} {} and {}",
            operation(op),
            functions::kind(a),
            functions::kind(b)
        ),
        ExecutionError::ValuesNotComparable(a, b) => format!(
            "cannot compare {} with {}",
            functions::kind(a),
            functions::kind(b)
        ),
        ExecutionError::DivisionByZero(_) | ExecutionError::RemainderByZero(_) => {
            "a division by zero".to_owned()
        }
        ExecutionError::FunctionError { function, message }
            if matches!(function.as_str(), "double" | "int" | "uint") && message.contains("parse error") =>
        {
            format!("{function}: given text that is not a number")
        }
        // `/` and `%` are OpenDuct's functions under the operators' signs: the message alone says it.
        ExecutionError::FunctionError { function, message }
            if function == functions::DIVIDE || function == functions::REMAINDER =>
        {
            message.clone()
        }
        ExecutionError::FunctionError { function, message } => format!("{function}: {message}"),
        other => other.to_string(),
    }
}

fn exact_and_decimal(a: &cel::Value, b: &cel::Value) -> bool {
    use cel::Value::{Float, Int, UInt};
    matches!(
        (a, b),
        (Int(_) | UInt(_), Float(_)) | (Float(_), Int(_) | UInt(_))
    )
}

fn operation(op: &str) -> &str {
    match op {
        "sub" => "subtract",
        "mul" => "multiply",
        "div" => "divide",
        "rem" => "take the remainder of",
        "minus" | "not" => "negate",
        other => other,
    }
}

/// An operator's call, not a function's (`_+_`, `!_`, `@in`).
fn is_operator(name: &str) -> bool {
    !name.chars().next().is_some_and(char::is_alphabetic)
}

/// The tree's depth, its lists inside lists, and the functions it calls - walked without recursion, so a tree too
/// deep cannot stop the process while it is checked.
/// Answers the names that qualify functions (`math` in `math.abs(x)`).
fn check_tree(root: &IdedExpr) -> Result<Vec<String>, String> {
    let mut spaces = Vec::new();
    let mut stack: Vec<(&IdedExpr, usize, usize)> = vec![(root, 1, 0)];
    while let Some((node, depth, loops)) = stack.pop() {
        if depth > MAX_DEPTH {
            return Err(format!("an expression {depth} levels deep: at most {MAX_DEPTH}"));
        }
        if loops > MAX_LOOPS {
            return Err(format!(
                "lists gone through {loops} deep, one inside another: at most {MAX_LOOPS}"
            ));
        }
        let next = depth + 1;
        match &node.expr {
            Expr::Call(call) => {
                // `math.abs(x)` parses as `abs` called on `math`: the qualified name is the function's.
                let qualified = match call.target.as_deref().map(|t| &t.expr) {
                    Some(Expr::Ident(space)) if exists(&format!("{space}.{}", call.func_name)) => {
                        spaces.push(space.clone());
                        true
                    }
                    _ => false,
                };
                if !is_operator(&call.func_name) && !qualified && !exists(&call.func_name) {
                    return Err(format!("there is no function {:?}", call.func_name));
                }
                if let Some(target) = &call.target {
                    stack.push((target, next, loops));
                }
                stack.extend(call.args.iter().map(|a| (a, next, loops)));
            }
            Expr::Comprehension(c) => {
                stack.push((&c.iter_range, next, loops));
                stack.push((&c.accu_init, next, loops));
                for body in [&c.loop_cond, &c.loop_step, &c.result] {
                    stack.push((body, next, loops + 1));
                }
            }
            Expr::List(list) => stack.extend(list.elements.iter().map(|e| (e, next, loops))),
            Expr::Map(map) => {
                for entry in &map.entries {
                    push_entry(&mut stack, &entry.expr, next, loops);
                }
            }
            Expr::Struct(s) => {
                for entry in &s.entries {
                    push_entry(&mut stack, &entry.expr, next, loops);
                }
            }
            Expr::Select(select) => stack.push((&select.operand, next, loops)),
            Expr::Ident(_) | Expr::Literal(_) | Expr::Unspecified => {}
        }
    }
    Ok(spaces)
}

fn push_entry<'a>(
    stack: &mut Vec<(&'a IdedExpr, usize, usize)>,
    entry: &'a EntryExpr,
    depth: usize,
    loops: usize,
) {
    match entry {
        EntryExpr::MapEntry(m) => {
            stack.push((&m.key, depth, loops));
            stack.push((&m.value, depth, loops));
        }
        EntryExpr::StructField(f) => stack.push((&f.value, depth, loops)),
    }
}

/// Every whole number written in the expression becomes a decimal, and so does every whole number CEL's functions
/// answer (`size()`, `int()`, `getHours()`): a list position may be a decimal, as long as it is whole. `a / b` and
/// `a % b` become OpenDuct's: a division by zero is an error, not infinity, and `%` works on decimals. A map's keys
/// stay as written (`{1: 'a'}`). The tree is at most [`MAX_DEPTH`] deep by now, so this recursion is bounded.
fn as_decimals(node: &mut IdedExpr, next_id: &mut u64) {
    match &mut node.expr {
        Expr::Literal(LiteralValue::Int(n)) => {
            #[allow(clippy::cast_precision_loss)]
            // a written number; a very large one rounds, as every double does
            let f = *n.inner() as f64;
            node.expr = Expr::Literal(LiteralValue::Double(CelDouble::from(f)));
        }
        Expr::Call(call) => {
            if let Some(target) = &mut call.target {
                as_decimals(target, next_id);
            }
            for arg in &mut call.args {
                as_decimals(arg, next_id);
            }
            match call.func_name.as_str() {
                "_/_" => functions::DIVIDE.clone_into(&mut call.func_name),
                "_%_" => functions::REMAINDER.clone_into(&mut call.func_name),
                name if WHOLE_ANSWERS.contains(&name) => {
                    let inner = std::mem::take(node);
                    *next_id += 1;
                    *node = IdedExpr {
                        id: *next_id,
                        expr: Expr::Call(CallExpr {
                            func_name: "double".to_owned(),
                            target: None,
                            args: vec![inner],
                        }),
                    };
                }
                _ => {}
            }
        }
        Expr::Comprehension(c) => {
            for e in [
                &mut c.iter_range,
                &mut c.accu_init,
                &mut c.loop_cond,
                &mut c.loop_step,
                &mut c.result,
            ] {
                as_decimals(e, next_id);
            }
        }
        Expr::List(list) => list.elements.iter_mut().for_each(|e| as_decimals(e, next_id)),
        Expr::Map(map) => map
            .entries
            .iter_mut()
            .for_each(|e| entry_decimals(&mut e.expr, next_id)),
        Expr::Struct(s) => s
            .entries
            .iter_mut()
            .for_each(|e| entry_decimals(&mut e.expr, next_id)),
        Expr::Select(select) => as_decimals(&mut select.operand, next_id),
        Expr::Ident(_) | Expr::Literal(_) | Expr::Unspecified => {}
    }
}

fn entry_decimals(entry: &mut EntryExpr, next_id: &mut u64) {
    match entry {
        // A map's keys stay as written: `{1: 'a'}` is keyed by the whole number 1.
        EntryExpr::MapEntry(m) => as_decimals(&mut m.value, next_id),
        EntryExpr::StructField(f) => as_decimals(&mut f.value, next_id),
    }
}
