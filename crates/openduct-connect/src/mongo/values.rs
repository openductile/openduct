// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MongoDB documents as plain JSON: object ids as their hex, dates as UTC times, decimals exact, UUIDs as UUIDs,
//! other binary data as `\x` and hex, so expressions read them as they read any JSON.

use mongodb::bson::{Bson, Document, spec::BinarySubtype};

use crate::json::{is_json_number, json_string};

const MAX_DEPTH: usize = 128;

/// A document as a JSON object, its fields in their order.
pub(super) fn document(doc: &Document) -> String {
    let mut out = String::new();
    object(doc, 0, &mut out);
    out
}

fn object(doc: &Document, depth: usize, out: &mut String) {
    out.push('{');
    for (i, (name, value)) in doc.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_string(name));
        out.push(':');
        write(value, depth + 1, out);
    }
    out.push('}');
}

/// One value as JSON.
pub(super) fn value(v: &Bson) -> String {
    let mut out = String::new();
    write(v, 0, &mut out);
    out
}

fn write(v: &Bson, depth: usize, out: &mut String) {
    if depth > MAX_DEPTH {
        out.push_str("null");
        return;
    }
    match v {
        Bson::Double(f) => {
            let s = format!("{f}");
            out.push_str(&if f.is_finite() && is_json_number(&s) {
                s
            } else {
                json_string(&f.to_string())
            });
        }
        Bson::String(s) | Bson::Symbol(s) => out.push_str(&json_string(s)),
        Bson::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(item, depth + 1, out);
            }
            out.push(']');
        }
        Bson::Document(doc) => object(doc, depth, out),
        Bson::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Bson::Null | Bson::Undefined | Bson::DbPointer(_) => out.push_str("null"),
        Bson::RegularExpression(r) => out.push_str(&json_string(&format!("/{}/{}", r.pattern, r.options))),
        Bson::Int32(n) => out.push_str(&n.to_string()),
        Bson::Int64(n) => out.push_str(&n.to_string()),
        Bson::Timestamp(t) => out.push_str(&format!("{{\"t\":{},\"i\":{}}}", t.time, t.increment)),
        Bson::Binary(b) => {
            if b.subtype == BinarySubtype::Uuid && b.bytes.len() == 16 {
                out.push_str(&json_string(&uuid(&b.bytes)));
            } else {
                let hex: String = b.bytes.iter().map(|x| format!("{x:02x}")).collect();
                out.push_str(&json_string(&format!("\\x{hex}")));
            }
        }
        Bson::ObjectId(id) => out.push_str(&json_string(&id.to_hex())),
        Bson::DateTime(t) => {
            let at = chrono::DateTime::from_timestamp_millis(t.timestamp_millis())
                .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                .unwrap_or_else(|| t.timestamp_millis().to_string());
            out.push_str(&json_string(&at));
        }
        Bson::Decimal128(d) => {
            let s = d.to_string();
            out.push_str(&if is_json_number(&s) { s } else { json_string(&s) });
        }
        Bson::MaxKey => out.push_str("\"MaxKey\""),
        Bson::MinKey => out.push_str("\"MinKey\""),
        // Stored code, and any kind a later driver adds: as text.
        other => out.push_str(&json_string(&other.to_string())),
    }
}

fn uuid(b: &[u8]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    let part = |from: usize, to: usize| hex.get(from..to).unwrap_or_default();
    format!(
        "{}-{}-{}-{}-{}",
        part(0, 8),
        part(8, 12),
        part(12, 16),
        part(16, 20),
        part(20, 32)
    )
}

/// A document's id as text for a message key: an object id's hex, a string as it is, anything else as JSON.
pub(super) fn id_text(id: &Bson) -> String {
    match id {
        Bson::ObjectId(o) => o.to_hex(),
        Bson::String(s) => s.clone(),
        other => value(other),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::str::FromStr;

    use mongodb::bson::{Binary, DateTime, Decimal128, doc, oid::ObjectId};

    use super::*;

    #[test]
    fn documents_read_as_plain_json() {
        let id = ObjectId::parse_str("6738a1f2c4e5d6a7b8c9d0e1").unwrap();
        let d = doc! {
            "_id": id,
            "total": Decimal128::from_str("19.90").unwrap(),
            "qty": 3,
            "big": 9_007_199_254_740_993i64,
            "ratio": 0.5,
            "paid": true,
            "note": "O'Brien \"Jr\"",
            "placed": DateTime::from_millis(1_794_736_351_977),
            "tags": ["a", Bson::Null],
            "ref": Binary { subtype: BinarySubtype::Uuid, bytes: (0u8..16).collect() },
            "raw": Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 255] },
            "nested": { "x": 1 },
        };
        assert_eq!(
            document(&d),
            r#"{"_id":"6738a1f2c4e5d6a7b8c9d0e1","total":19.90,"qty":3,"big":9007199254740993,"ratio":0.5,"paid":true,"note":"O'Brien \"Jr\"","placed":"2026-11-15T09:52:31.977Z","tags":["a",null],"ref":"00010203-0405-0607-0809-0a0b0c0d0e0f","raw":"\\x01ff","nested":{"x":1}}"#
        );
        assert_eq!(id_text(&Bson::ObjectId(id)), "6738a1f2c4e5d6a7b8c9d0e1");
        assert_eq!(id_text(&Bson::Int32(7)), "7");
        assert_eq!(value(&Bson::Double(f64::NAN)), "\"NaN\"");
    }
}
