// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Every example in docs/expressions.md gives the result the page says, on the page's own sample message.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use openduct_expr::{Env, Value, compile};

const PAGE: &str = include_str!("../../../docs/expressions.md");

/// The text of a cell that is one code span, `\|` read as `|`.
fn code(cell: &str) -> Option<String> {
    let cell = cell.trim();
    let inner = cell.strip_prefix('`')?.strip_suffix('`')?;
    (!inner.contains('`')).then(|| inner.replace("\\|", "|"))
}

/// The cells of a table row; `\|` inside a cell does not split it.
fn cells(row: &str) -> Vec<String> {
    let row = row.trim().trim_start_matches('|').trim_end_matches('|');
    let mut out = vec![String::new()];
    let mut chars = row.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek() == Some(&'|') {
            out.last_mut().unwrap().push_str("\\|");
            chars.next();
        } else if c == '|' {
            out.push(String::new());
        } else {
            out.last_mut().unwrap().push(c);
        }
    }
    out
}

#[test]
fn every_example_on_the_expressions_page_gives_its_result() {
    let sample = PAGE
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let message = Value::parse_json(sample).unwrap();
    let mut checked = 0;
    let mut wrong = Vec::new();
    let mut columns: Option<(usize, usize)> = None;
    for line in PAGE.lines() {
        if !line.starts_with('|') {
            columns = None;
            continue;
        }
        let row = cells(line);
        if row
            .iter()
            .all(|c| c.trim().chars().all(|ch| ch == '-' || ch == ':'))
        {
            continue;
        }
        let Some((example, result)) = columns else {
            let find = |name: &str| row.iter().position(|c| c.trim() == name);
            columns = find("Example").zip(find("Result"));
            continue;
        };
        let (Some(source), Some(want)) = (
            row.get(example).and_then(|c| code(c)),
            row.get(result).and_then(|c| code(c)),
        ) else {
            continue;
        };
        let want =
            Value::parse_json(&want).unwrap_or_else(|e| panic!("{source}: the page's result {want:?}: {e}"));
        let got = compile("cel", &source).and_then(|e| e.evaluate(&Env::of(&message)));
        match got {
            Ok(v) if v.to_json() == want.to_json() => {}
            other => wrong.push(format!(
                "{source}\n    page: {}\n    got:  {other:?}",
                want.to_json()
            )),
        }
        checked += 1;
    }
    assert!(
        checked >= 90,
        "only {checked} examples found: the page's tables have changed shape"
    );
    assert!(
        wrong.is_empty(),
        "{} of {checked} examples differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}
