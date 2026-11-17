// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CEL as OpenDuct runs it: the standard where it serves business data, and OpenDuct's four rules where it does not
//! (numbers, absent values, functions, limits).

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use openduct_expr::{Env, Value, compile};

fn order() -> Value {
    Value::parse_json(
        r#"{"id": "o-1", "qty": 3, "price": 12.5, "status": "paid", "tags": ["gift", "rush"],
            "lines": [{"sku": "a", "qty": 2, "price": 5}, {"sku": "b", "qty": 4, "price": 3.625}],
            "customer": {"name": " Ada Lovelace ", "email": "ADA@EXAMPLE.COM"}, "note": null,
            "first-name": "Ada", "placed": "2026-11-16T09:30:00Z", "big": 123456789012345678901234567890}"#,
    )
    .unwrap()
}

fn eval(source: &str) -> String {
    let message = order();
    match compile("cel", source) {
        Ok(e) => match e.evaluate(&Env::of(&message)) {
            Ok(v) => v.to_json(),
            Err(why) => format!("error: {why}"),
        },
        Err(why) => format!("compile error: {why}"),
    }
}

#[test]
fn every_number_is_a_decimal() {
    assert_eq!(eval("qty * price"), "37.5");
    assert_eq!(eval("price * 2"), "25");
    assert_eq!(eval("qty + 1"), "4");
    assert_eq!(eval("1 + 2.5"), "3.5");
    assert_eq!(eval("qty / 2"), "1.5", "no whole-number division");
    assert_eq!(eval("0.1 + 0.2"), "0.30000000000000004", "as in JavaScript");
    assert_eq!(eval("size(tags) == 2"), "true");
    assert_eq!(eval("lines.map(l, l.qty * l.price)"), "[10,14.5]");
    // Whole where CEL needs it: a list position, int().
    assert_eq!(eval("tags[1]"), r#""rush""#);
    assert_eq!(eval("tags[size(tags) - 1]"), r#""rush""#);
    assert_eq!(eval("int(price)"), "12");
    // Too large to calculate with: kept as it came, as text.
    assert_eq!(eval("big"), r#""123456789012345678901234567890""#);
}

#[test]
fn what_a_message_does_not_have_is_null() {
    assert_eq!(eval("missing"), "null");
    assert_eq!(eval("customer.phone"), "null");
    assert_eq!(eval("note.text"), "null", "a field of null");
    assert_eq!(eval("has(customer.phone)"), "false");
    assert_eq!(eval("has(customer.name) ? 'named' : 'anonymous'"), r#""named""#);
    assert_eq!(eval("value['first-name']"), r#""Ada""#);
}

#[test]
fn the_added_functions_round_and_clean_text() {
    assert_eq!(eval("round(2.675, 2)"), "2.68", "as people round money");
    assert_eq!(eval("round(price)"), "13");
    assert_eq!(
        eval("math.floor(price) + math.ceil(0.2) + math.abs(-1)"),
        "14",
        "CEL's math extension"
    );
    assert_eq!(eval("math.greatest([qty, price, 4])"), "12.5");
    assert_eq!(eval("customer.name.trim()"), r#""Ada Lovelace""#);
    assert_eq!(eval("customer.email.lowerAscii()"), r#""ada@example.com""#);
    assert_eq!(
        eval("customer.name.trim().split(' ')[0].upperAscii()"),
        r#""ADA""#
    );
    assert_eq!(eval("tags.join(', ')"), r#""gift, rush""#);
    assert_eq!(eval("id.replace('o-', 'order ')"), r#""order 1""#);
    assert_eq!(eval("id.substring(2)"), r#""1""#);
    assert_eq!(eval("customer.email.indexOf('@')"), "3");
    assert_eq!(eval("id.charAt(0)"), r#""o""#);
    // CEL's own.
    assert_eq!(eval("status in ['paid', 'shipped']"), "true");
    assert_eq!(eval("customer.email.matches('@EXAMPLE')"), "true");
    assert_eq!(eval("timestamp(placed).getHours()"), "9");
    assert_eq!(eval("lines.filter(l, l.qty > 2).map(l, l.sku)"), r#"["b"]"#);
    assert_eq!(
        eval("lines.all(l, l.qty > 0) && tags.exists(t, t == 'gift')"),
        "true"
    );
    assert_eq!(
        eval("{'total': qty * price, 'id': id}"),
        r#"{"id":"o-1","total":37.5}"#
    );
}

#[test]
fn mistakes_are_found_when_compiled_and_said_plainly() {
    assert!(eval("qty *").starts_with("compile error"));
    assert_eq!(
        eval("nosuch(qty)"),
        r#"compile error: there is no function "nosuch""#
    );
    assert_eq!(
        eval("id.nosuch()"),
        r#"compile error: there is no function "nosuch""#
    );
    assert_eq!(eval("id + 1"), "error: cannot add text and a number");
    assert_eq!(eval("round(id)"), "error: round: needs a number, not text");
    assert_eq!(
        eval("id.substring(5)"),
        "error: substring: substring(5, 3) of text 3 characters long"
    );
}

#[test]
fn names_worked_out_before_and_the_messages_details_are_read_too() {
    let message = order();
    let details = Value::parse_json(r#"{"topic": "orders", "partition": 2, "offset": 41}"#).unwrap();
    let names = vec![
        ("total".to_owned(), Value::parse_json("37.5").unwrap()),
        ("qty".to_owned(), Value::parse_json("9").unwrap()),
    ];
    let env = Env {
        value: &message,
        message: &details,
        names: &names,
    };
    let e = compile("cel", "total > 30.0 && message.topic == 'orders' ? qty : 0").unwrap();
    assert_eq!(
        e.evaluate(&env).unwrap().to_json(),
        "9",
        "a name worked out before comes first"
    );
    assert_eq!(e.source(), "total > 30.0 && message.topic == 'orders' ? qty : 0");
}

#[test]
fn another_language_is_named_or_refused() {
    let refused = compile("basic", "1").unwrap_err();
    assert!(refused.contains("no expression language \"basic\""), "{refused}");
}

#[test]
fn a_name_the_message_does_not_have_is_null_where_it_stands() {
    // So a condition on it reads as people mean it, not as the whole answer gone.
    assert_eq!(eval("discount == null ? 'none' : 'some'"), r#""none""#);
    assert_eq!(eval("enabled != false"), "true");
    // A field the message has, named like a type, is the field; `double` it does not have is still the type.
    let payment = Value::parse_json(r#"{"type": "charge.succeeded", "amount": 2.5}"#).unwrap();
    let e = compile("cel", "type.startsWith('charge.') && type(amount) == double").unwrap();
    assert_eq!(e.evaluate(&Env::of(&payment)).unwrap().to_json(), "true");
    // A name that qualifies functions is not a field.
    assert_eq!(eval("math.abs(-2) + 1"), "3");
}

#[test]
fn the_regex_extension_finds_and_replaces() {
    let line =
        Value::parse_json(r#""10.0.0.7 - - [16/Nov/2026:10:00:00 +0000] \"GET /cart HTTP/1.1\" 503 1042""#)
            .unwrap();
    let run = |source: &str| {
        let e = compile("cel", source).unwrap();
        match e.evaluate(&Env::of(&line)) {
            Ok(v) => v.to_json(),
            Err(why) => format!("error: {why}"),
        }
    };
    assert_eq!(
        run(r#"regex.extract(value, r'" (\d{3}) ')"#),
        r#""503""#,
        "the group in brackets"
    );
    assert_eq!(
        run(r"regex.extract(value, r'^\S+')"),
        r#""10.0.0.7""#,
        "the whole match"
    );
    assert_eq!(
        run(r"regex.extract(value, r'nothing here')"),
        "null",
        "nothing matched is null, not an optional as in cel-go"
    );
    assert_eq!(
        run(r"regex.extract(value, r'(\d+) (\d+)$')"),
        "error: regex.extract: takes a pattern with at most one group in brackets ((?:...) groups without taking)"
    );
    assert_eq!(
        run(r"regex.extractAll(value, r'\d{3,}')"),
        r#"["2026","0000","503","1042"]"#
    );
    assert_eq!(
        run(r"regex.replace('ada@example.com', r'^(.).*@', r'\1***@')"),
        r#""a***@example.com""#
    );
    assert_eq!(run(r"regex.replace('banana', 'a', 'o', 2)"), r#""bonona""#);
    assert_eq!(
        run(r"regex.replace('(555) 010-9999', r'\D', '')"),
        r#""5550109999""#
    );
    assert!(run(r"regex.replace('x', '(x)', r'\2')").contains("the pattern has 1 groups"));
    assert!(run(r"regex.extract('x', '(')").starts_with("error: regex.extract: \"(\" is not a pattern"));
}

#[test]
fn format_sum_sha256_and_seconds_since_1970() {
    assert_eq!(
        eval("'%s x %s = %.2f'.format([qty, price, qty * price])"),
        r#""3 x 12.5 = 37.50""#
    );
    assert_eq!(eval("'%d%%'.format([qty])"), r#""3%""#);
    assert_eq!(
        eval("'%d'.format([price])"),
        "error: format: %d needs a whole number, not 12.5"
    );
    assert_eq!(eval("'%e'.format([1500])"), r#""1.500000e+03""#);
    assert_eq!(eval("'%s'.format([tags])"), r#""[\"gift\",\"rush\"]""#);
    assert_eq!(
        eval("'%s %s'.format([1])"),
        "error: format: has more clauses than the 1 values given"
    );
    assert_eq!(eval("lines.map(l, l.qty * l.price).sum()"), "24.5");
    assert_eq!(eval("[0.1, 0.2].sum()"), "0.30000000000000004", "as + adds");
    assert_eq!(eval("[].sum()"), "0");
    // printf 'ada@example.com' | shasum -a 256
    assert_eq!(
        eval("sha256(customer.email.lowerAscii())"),
        r#""b5fc85e55755f9e0d030a10ab4429b6b2944855f9a0d60077fe832becbc41d72""#
    );
    assert_eq!(eval("timestamp(1700000000)"), r#""2023-11-14T22:13:20Z""#);
    assert_eq!(eval("timestamp(1700000000.25)"), r#""2023-11-14T22:13:20.250Z""#);
    assert_eq!(eval("timestamp(qty * 3600).getHours()"), "3");
    assert_eq!(
        eval("timestamp('2026-11-14 22:30:00', '%Y-%m-%d %H:%M:%S')"),
        r#""2026-11-14T22:30:00Z""#
    );
    assert_eq!(
        eval("timestamp('15/Nov/2026:10:01:12 +0100', '%d/%b/%Y:%H:%M:%S %z')"),
        r#""2026-11-15T09:01:12Z""#
    );
    assert_eq!(
        eval("timestamp('16.11.2026', '%d.%m.%Y')"),
        r#""2026-11-16T00:00:00Z""#
    );
    assert_eq!(
        eval("timestamp('soon', '%Y')"),
        r#"error: timestamp: "soon" is not a time written as "%Y""#
    );
}

#[test]
fn an_operation_on_null_gives_a_null_answer_as_in_sql() {
    for source in [
        "discount - 1",
        "discount < 5",
        "!discount",
        "note.trim()",
        "round(discount, 2)",
        "timestamp(note)",
        "size(note)",
        "regex.extract(note, 'x')",
        "sha256(note)",
    ] {
        assert_eq!(eval(source), "null", "{source}");
    }
    assert_eq!(
        eval("[qty, note].sum()"),
        "3",
        "nulls left out of a sum, as SQL leaves them"
    );
    // A value of the wrong kind is still an error: it is a mistake in the data, not something missing.
    assert_eq!(eval("status * 2"), "error: cannot multiply text and a number");
    assert_eq!(
        eval("double(status)"),
        "error: double: given text that is not a number"
    );
    assert_eq!(eval("double(regex.extract(status, '[0-9]+'))"), "null");
}

#[test]
fn division_remainders_whole_answers_and_long_identifiers() {
    assert_eq!(eval("qty % 2"), "1");
    assert_eq!(eval("price % 5"), "2.5", "a remainder of decimals");
    assert_eq!(
        eval("price / 0"),
        "error: a division by zero",
        "not infinity, nor a quiet null"
    );
    assert_eq!(eval("qty % 0"), "error: a division by zero");
    assert_eq!(eval("status / 2"), "error: cannot divide text and a number");
    assert_eq!(eval("int(price) * 2"), "24", "int() answers a decimal too");
    assert_eq!(eval("int(timestamp(placed)) + 60"), "1794821460");
    assert_eq!(
        eval("tags[size(tags) - 1]"),
        r#""rush""#,
        "a whole decimal is a list position"
    );
    assert_eq!(eval("tags[qty - 2]"), r#""rush""#);
    assert!(eval("tags[price]").starts_with("error:"), "12.5 is no position");
    // A whole number a decimal would change is kept exactly: identifiers pass through as they came.
    let order = Value::parse_json(r#"{"id": 1234567890123456789, "total": 9007199254740993}"#).unwrap();
    let run = |source: &str| {
        compile("cel", source)
            .unwrap()
            .evaluate(&Env::of(&order))
            .map(|v| v.to_json())
    };
    assert_eq!(run("id").unwrap(), "1234567890123456789");
    assert_eq!(run("{'id': id}").unwrap(), r#"{"id":1234567890123456789}"#);
    assert!(run("id + 1").unwrap_err().contains("write double(...)"));
    assert_eq!(run("double(id) > 0").unwrap(), "true");
}

#[test]
fn a_mistake_in_writing_says_what_and_where() {
    assert_eq!(
        eval("qty *"),
        "compile error: the expression ends too soon, at character 6:\n  qty *\n       ^"
    );
    assert_eq!(
        eval("status == 'paid"),
        "compile error: text that is never closed: it needs its closing quote, at character 11:\n  status == 'paid\n            ^"
    );
    assert!(
        eval("qty + + ").starts_with("compile error: \"+\" is not expected here, at character 7"),
        "{}",
        eval("qty + + ")
    );
    assert!(
        eval("(qty * 2").starts_with("compile error: "),
        "{}",
        eval("(qty * 2")
    );
    assert!(!eval("price ) 2").contains("expecting"), "{}", eval("price ) 2"));
}
