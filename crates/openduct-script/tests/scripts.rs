// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! JavaScript and Python steps: what they compute, what they log, what they are refused - files, the network,
//! processes, the environment - and the limits of the process they run in.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::PathBuf;
use std::time::Duration;

use openduct_script::{Answer, Failure, Isolated, Language, Script, run_here};
use serde_json::{Map, Value as Json, json};

fn script(language: Language, code: &str, names: Json) -> Script {
    let names: Map<String, Json> = names.as_object().cloned().unwrap_or_default();
    Script {
        language,
        code: code.to_owned(),
        names,
        time: Duration::from_secs(10),
    }
}

fn js(code: &str, names: Json) -> Result<Answer, Failure> {
    run_here(&script(Language::JavaScript, code, names))
}

fn py(code: &str, names: Json) -> Result<Answer, Failure> {
    run_here(&script(Language::Python, code, names))
}

fn order() -> Json {
    json!({ "value": { "id": "A-1", "lines": [{ "qty": 2, "price": 9.5 }, { "qty": 1, "price": 20 }] }, "steps": { "seen": { "n": 3 } } })
}

#[test]
fn javascript_returns_its_answer_from_what_it_is_given_and_keeps_its_logs() {
    let answer = js(
        "const total = value.lines.reduce((t, l) => t + l.qty * l.price, 0);\nconsole.log('total', total, { id: value.id });\nreturn { id: value.id, total, big: total > 30, seen: steps.seen.n };",
        order(),
    )
    .unwrap();
    assert_eq!(
        answer.result,
        json!({ "id": "A-1", "total": 39, "big": true, "seen": 3 })
    );
    assert_eq!(answer.logs, ["total 39 {\"id\":\"A-1\"}"]);
    assert_eq!(
        js("const x = 1;", json!({})).unwrap().result,
        Json::Null,
        "no return is null"
    );
}

#[test]
fn javascript_errors_say_what_and_where() {
    let thrown = js("console.log('before');\nthrow new Error('no lines');", json!({})).unwrap_err();
    assert_eq!(thrown.error, "Error: no lines");
    assert_eq!(thrown.logs, ["before"]);
    let missing = js("return nothing.here;", json!({})).unwrap_err();
    assert!(missing.error.contains("nothing"), "{}", missing.error);
    let syntax = js("return {;", json!({})).unwrap_err();
    assert!(syntax.error.contains("SyntaxError"), "{}", syntax.error);
}

#[test]
fn javascript_has_the_language_and_nothing_that_reaches_outside() {
    let answer = js(
        "return [typeof require, typeof fetch, typeof process, typeof Deno, typeof setTimeout, typeof XMLHttpRequest, typeof importScripts, typeof Temporal];",
        json!({}),
    )
    .unwrap();
    assert!(
        answer.result.as_array().unwrap().iter().all(|t| t == "undefined"),
        "{}",
        answer.result
    );
    let language = js(
        "const m = new Map([['a', 1]]); return { json: JSON.parse('{\"x\": [1, 2]}').x.length, regex: /o(\\d+)/.exec('no42')[1], date: new Date(0).toISOString(), map: m.get('a'), big: (2n ** 64n).toString() };",
        json!({}),
    )
    .unwrap();
    assert_eq!(
        language.result,
        json!({ "json": 2, "regex": "42", "date": "1970-01-01T00:00:00.000Z", "map": 1, "big": "18446744073709551616" })
    );
    let module = js("return import('fs')", json!({}));
    let shown = format!("{module:?}");
    assert!(!shown.contains("readFile"), "{shown}");
}

#[test]
fn python_sets_its_result_from_what_it_is_given_and_keeps_its_prints() {
    let answer = py(
        "total = sum(l['qty'] * l['price'] for l in value['lines'])\nprint('total', total)\nresult = {'id': value['id'], 'total': total, 'big': total > 30, 'seen': steps['seen']['n']}",
        order(),
    )
    .unwrap();
    assert_eq!(
        answer.result,
        json!({ "id": "A-1", "total": 39.0, "big": true, "seen": 3 })
    );
    assert_eq!(answer.logs, ["total 39.0"]);
    assert_eq!(
        py("x = 1", json!({})).unwrap().result,
        Json::Null,
        "no result is null"
    );
}

#[test]
fn python_has_the_computing_part_of_its_standard_library() {
    let answer = py(
        r#"
import json, re, math, datetime, decimal, statistics, collections, itertools, functools, hashlib, base64, textwrap, string, csv, io, uuid, random, fractions, heapq, bisect, operator, unicodedata, html, urllib.parse, copy, enum, dataclasses, typing
rows = list(csv.DictReader(io.StringIO("a,b\n1,2\n3,4\n")))
result = {
    "json": json.loads('{"x": [1, 2]}')["x"],
    "re": re.findall(r"\d+", "a1b22c333"),
    "math": math.isqrt(17),
    "date": (datetime.date(2026, 11, 16) + datetime.timedelta(days=30)).isoformat(),
    "decimal": str(decimal.Decimal("0.1") + decimal.Decimal("0.2")),
    "mean": statistics.mean([1, 2, 3, 4]),
    "count": collections.Counter("abca").most_common(1)[0][0],
    "sha256": len(hashlib.sha256(b"openduct").hexdigest()),
    "b64": base64.b64encode(b"hi").decode(),
    "csv": rows[1]["b"],
    "quote": urllib.parse.quote("a b&c"),
    "fraction": str(fractions.Fraction(3, 6)),
    "uuid": len(str(uuid.uuid4())),
    "random": 0 <= random.random() < 1,
}
"#,
        json!({}),
    )
    .unwrap();
    assert_eq!(
        answer.result,
        json!({
            "json": [1, 2], "re": ["1", "22", "333"], "math": 4, "date": "2026-12-16", "decimal": "0.3",
            "mean": 2.5, "count": "a", "sha256": 64, "b64": "aGk=", "csv": "4",
            "quote": "a%20b%26c", "fraction": "1/2", "uuid": 36, "random": true,
        })
    );
}

#[test]
fn python_cannot_open_a_file_reach_the_network_start_a_program_or_read_the_environment() {
    for (code, what) in [
        ("open('/etc/hosts').read()", "open"),
        ("import os; os.listdir('/')", "os.listdir"),
        ("import os; os.system('true')", "os.system"),
        ("import os; os.open('/etc/hosts', 0)", "os.open"),
        ("import os; os.stat('/etc/hosts')", "os.stat"),
        (
            "import os.path; os.path.exists('/etc/hosts') or 1/0",
            "os.path.exists",
        ),
        ("import posix; posix.listdir('/')", "posix.listdir"),
        ("import socket", "socket"),
        ("import subprocess", "subprocess"),
        ("import ctypes", "ctypes"),
        ("import _io; _io.FileIO('/etc/hosts')", "FileIO"),
        (
            "import pathlib; pathlib.Path('/etc/hosts').read_text()",
            "pathlib",
        ),
        ("import shutil", "shutil"),
        ("import urllib.request", "urllib.request"),
        ("import http.client", "http.client"),
        ("import select", "select"),
        ("import mmap", "mmap"),
        ("import signal", "signal"),
    ] {
        let refused = py(code, json!({}));
        assert!(refused.is_err(), "{what} is refused, not {refused:?}");
    }
    let classes = py(
        "names = [c.__name__ for c in ().__class__.__base__.__subclasses__()]\nresult = [n for n in names if n in ('FileIO', 'Popen', 'socket', '_wrap_close', 'DirEntry')]",
        json!({}),
    )
    .unwrap();
    assert_eq!(
        classes.result,
        json!([]),
        "no class that reaches outside is loaded"
    );
}

#[test]
fn python_errors_say_what_and_where() {
    let raised = py("print('before')\nraise ValueError('no lines')", json!({})).unwrap_err();
    assert!(raised.error.contains("ValueError: no lines"), "{}", raised.error);
    assert!(raised.error.contains("line 2"), "{}", raised.error);
    assert_eq!(raised.logs, ["before"]);
    let syntax = py("result = (", json!({})).unwrap_err();
    assert!(syntax.error.starts_with("SyntaxError"), "{}", syntax.error);
    let unjson = py("result = {1, 2}", json!({}));
    assert!(
        unjson.is_err() || unjson.as_ref().unwrap().result.is_array(),
        "{unjson:?}"
    );
}

// --- in a process of its own ---

fn isolated() -> Isolated {
    Isolated::new(PathBuf::from(env!("CARGO_BIN_EXE_openduct-python")))
}

fn in_process(s: &Script) -> Result<Answer, Failure> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(isolated().run(s))
}

#[test]
fn a_script_in_its_own_process_answers_the_same() {
    let answer = in_process(&script(
        Language::JavaScript,
        "console.log('hi'); return value.lines.length;",
        order(),
    ))
    .unwrap();
    assert_eq!(answer.result, json!(2));
    assert_eq!(answer.logs, ["hi"]);
    let answer = in_process(&script(Language::Python, "result = len(value['lines'])", order())).unwrap();
    assert_eq!(answer.result, json!(2));
    let failed = in_process(&script(Language::Python, "raise KeyError('x')", json!({}))).unwrap_err();
    assert!(failed.error.contains("KeyError"), "{}", failed.error);
}

#[test]
fn a_script_that_runs_past_its_time_is_stopped() {
    for (language, code) in [
        (Language::JavaScript, "while (true) {}"),
        (Language::Python, "while True:\n    pass"),
    ] {
        let mut s = script(language, code, json!({}));
        s.time = Duration::from_secs(1);
        let started = std::time::Instant::now();
        let stopped = in_process(&s).unwrap_err();
        assert!(stopped.error.contains("longer than 1 s"), "{}", stopped.error);
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{:?}",
            started.elapsed()
        );
    }
}

#[test]
fn a_script_never_sees_the_programs_environment() {
    // The process starts with none: not even what this test's own process has.
    let answer = in_process(&script(
        Language::JavaScript,
        "return typeof process === 'undefined' && typeof Deno === 'undefined';",
        json!({}),
    ))
    .unwrap();
    assert_eq!(answer.result, json!(true));
    let python = in_process(&script(
        Language::Python,
        "import os\nresult = [dict(os.environ), os.getenv('HOME'), os.getenv('PATH', 'none')]",
        json!({}),
    ))
    .unwrap();
    assert_eq!(python.result, json!([{}, null, "none"]));
}

#[test]
fn python_running_out_of_memory_is_said_plainly() {
    let failed = py("raise MemoryError()", json!({})).unwrap_err();
    assert_eq!(
        failed.error,
        "the script used more memory than a script may: it was stopped"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_script_that_takes_too_much_memory_is_stopped() {
    let stopped = in_process(&script(
        Language::Python,
        "x = 'a' * (3 * 1024 ** 3)\nresult = len(x)",
        json!({}),
    ))
    .unwrap_err();
    assert_eq!(
        stopped.error, "the script used more memory than a script may: it was stopped",
        "Python's own MemoryError is said plainly"
    );
}
