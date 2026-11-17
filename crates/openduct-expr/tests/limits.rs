// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Expressions at and past every limit: refused when compiled, with the reason - and the largest ones allowed still
//! compile and run (on a thread as large as a debug build needs; a release build needs far less).

// A test file: a failed step should stop the test where it failed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use openduct_expr::{Env, MAX_NESTING, MAX_STRUCTURE, Value, compile};

/// Runs `f` on a thread with a 64 MiB stack: a debug build's frames are many times a release build's.
fn big_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn run(source: String) -> Result<String, String> {
    big_stack(move || {
        let e = compile("cel", &source)?;
        e.evaluate(&Env::of(&Value::Null)).map(|v| v.to_json())
    })
}

#[test]
fn the_largest_allowed_expressions_compile_and_run() {
    let nested = format!("{}1{}", "(".repeat(MAX_NESTING), ")".repeat(MAX_NESTING));
    assert_eq!(run(nested).unwrap(), "1");
    let lists = format!("{}1{}", "[".repeat(MAX_NESTING), "]".repeat(MAX_NESTING));
    assert!(run(lists).is_ok());
    let chain = vec!["1"; MAX_STRUCTURE].join("+");
    assert_eq!(run(chain).unwrap(), MAX_STRUCTURE.to_string());
    let loops = "[[[1]]].map(a, a.map(b, b.map(c, c + 1)))".to_owned();
    assert_eq!(run(loops).unwrap(), "[[[2]]]");
}

#[test]
fn past_a_limit_is_refused_with_the_limit() {
    for (source, says) in [
        (
            format!("{}1{}", "(".repeat(MAX_NESTING + 1), ")".repeat(MAX_NESTING + 1)),
            "deep",
        ),
        (format!("{}1{}", "[".repeat(5_000), "]".repeat(5_000)), "bytes"),
        (format!("{}1{}", "[".repeat(40), "]".repeat(40)), "deep"),
        (vec!["1"; MAX_STRUCTURE + 2].join("+"), "at most 256"),
        ("x".repeat(5_000), "bytes"),
        (
            "[[[[1]]]].map(a, a.map(b, b.map(c, c.map(d, d))))".to_owned(),
            "lists gone through",
        ),
    ] {
        let refused = compile("cel", &source).unwrap_err();
        assert!(refused.contains(says), "{refused}");
    }
}

#[test]
fn no_text_grows_past_a_mebibyte() {
    let grow = (0..12).fold("'aaaa'".to_owned(), |e, _| format!("{e}.replace('', 'aaaa')"));
    let refused = run(grow).unwrap_err();
    assert!(refused.contains("at most 1048576"), "{refused}");
}

#[test]
fn lists_gone_through_inside_each_other_stop_at_the_step_budget() {
    // A thousand items, three loops deep: a billion steps if nothing stopped it.
    let items: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    let message = Value::parse_json(&format!("{{\"items\": [{}]}}", items.join(","))).unwrap();
    let e = compile("cel", "items.map(a, items.map(b, items.map(c, a + b + c)))").unwrap();
    let started = std::time::Instant::now();
    let stopped = big_stack(move || e.evaluate(&Env::of(&message))).unwrap_err();
    // The step budget, or the time limit if a slow (debug) build gets there first.
    assert!(
        stopped.contains("100000") || stopped.contains("100 ms"),
        "{stopped}"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "stopped early, not after a billion steps"
    );
    // Within the budget, the same shape works.
    let small = Value::parse_json(r#"{"items": [1, 2]}"#).unwrap();
    let e = compile("cel", "items.map(a, items.map(b, a * b))").unwrap();
    assert_eq!(e.evaluate(&Env::of(&small)).unwrap().to_json(), "[[1,2],[2,4]]");
}
