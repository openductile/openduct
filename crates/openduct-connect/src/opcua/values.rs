// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OPC UA values as plain JSON: numbers as numbers (a float that is not a number as text), times in ISO 8601 UTC,
//! text as text, byte strings in base64, node ids and status codes in their usual written form, arrays as lists. A
//! structure (an extension object) is given by its type and its encoded body, as the server sent it.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use opcua::types::{DataValue, DateTime, StatusCode, Variant};
use serde_json::{Value, json};

/// A float as JSON: a number, or its text when it is not one (`NaN`, `inf`).
fn float(f: f64) -> Value {
    serde_json::Number::from_f64(f).map_or_else(|| json!(f.to_string()), Value::Number)
}

/// A time as ISO 8601 UTC, to the millisecond; `None` for OPC UA's "no time".
pub(super) fn time(t: &DateTime) -> Option<String> {
    if t.is_null() {
        return None;
    }
    Some(t.as_chrono().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}

pub(super) fn status(code: StatusCode) -> String {
    code.sub_code().name().to_owned()
}

/// One OPC UA value as JSON.
pub(super) fn to_json(v: &Variant) -> Value {
    match v {
        Variant::Empty => Value::Null,
        Variant::Boolean(b) => json!(b),
        Variant::SByte(n) => json!(n),
        Variant::Byte(n) => json!(n),
        Variant::Int16(n) => json!(n),
        Variant::UInt16(n) => json!(n),
        Variant::Int32(n) => json!(n),
        Variant::UInt32(n) => json!(n),
        Variant::Int64(n) => json!(n),
        Variant::UInt64(n) => json!(n),
        Variant::Float(f) => float(f64::from(*f)),
        Variant::Double(f) => float(*f),
        Variant::String(s) => s.value().as_ref().map_or(Value::Null, |s| json!(s)),
        Variant::DateTime(t) => time(t).map_or(Value::Null, Value::String),
        Variant::Guid(g) => json!(g.to_string()),
        Variant::StatusCode(c) => json!(status(*c)),
        Variant::ByteString(b) => b
            .value
            .as_ref()
            .map_or(Value::Null, |b| json!(STANDARD.encode(b))),
        Variant::XmlElement(x) => json!(x.to_string()),
        Variant::QualifiedName(q) => json!(q.name.to_string()),
        Variant::LocalizedText(t) => json!(t.text.to_string()),
        Variant::NodeId(n) => json!(n.to_string()),
        Variant::ExpandedNodeId(n) => json!(n.to_string()),
        Variant::ExtensionObject(o) => {
            let type_id = o.binary_type_id().to_string();
            json!({ "type": o.type_name().map_or(type_id, str::to_owned) })
        }
        Variant::Variant(inner) => to_json(inner),
        Variant::DataValue(d) => d.value.as_ref().map_or(Value::Null, to_json),
        Variant::DiagnosticInfo(_) => Value::Null,
        Variant::Array(a) => {
            let values: Vec<Value> = a.values.iter().map(to_json).collect();
            match &a.dimensions {
                Some(d) if d.len() > 1 => json!({ "values": values, "dimensions": d }),
                _ => Value::Array(values),
            }
        }
    }
}

/// A change of one value, as the message's JSON. The value is `reading`: in an expression, `value` is the whole message.
pub(super) fn change(name: &str, node: &str, d: &DataValue) -> String {
    json!({
        "name": name,
        "node": node,
        "reading": d.value.as_ref().map_or(Value::Null, to_json),
        "status": status(d.status.unwrap_or(StatusCode::Good)),
        "source_time": d.source_timestamp.as_ref().and_then(time),
        "server_time": d.server_timestamp.as_ref().and_then(time),
    })
    .to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use opcua::types::{ByteString, NodeId, UAString};

    use super::*;

    #[test]
    fn values_become_plain_json() {
        let cases: Vec<(Variant, &str)> = vec![
            (Variant::Boolean(true), "true"),
            (Variant::Int16(-12), "-12"),
            (Variant::UInt64(u64::MAX), "18446744073709551615"),
            (Variant::Double(21.5), "21.5"),
            (Variant::Double(f64::NAN), "\"NaN\""),
            (Variant::Float(f32::INFINITY), "\"inf\""),
            (Variant::String(UAString::from("Press 7")), "\"Press 7\""),
            (Variant::String(UAString::null()), "null"),
            (Variant::ByteString(ByteString::from(vec![1u8, 2, 3])), "\"AQID\""),
            (
                Variant::NodeId(Box::new(NodeId::new(2, "Press7.Temperature"))),
                "\"ns=2;s=Press7.Temperature\"",
            ),
            (
                Variant::StatusCode(StatusCode::BadNodeIdUnknown),
                "\"BadNodeIdUnknown\"",
            ),
            (Variant::from(vec![1i32, 2, 3]), "[1,2,3]"),
            (Variant::Empty, "null"),
        ];
        for (v, want) in cases {
            assert_eq!(to_json(&v).to_string(), want, "{v:?}");
        }
        let t = DateTime::from(chrono::DateTime::from_timestamp(1_763_200_000, 123_000_000).unwrap());
        assert_eq!(time(&t).unwrap(), "2025-11-15T09:46:40.123Z");
        assert_eq!(time(&DateTime::null()), None);
    }

    #[test]
    fn a_change_is_one_message() {
        let d = DataValue {
            value: Some(Variant::Double(81.4)),
            status: Some(StatusCode::Good),
            ..DataValue::default()
        };
        assert_eq!(
            change("temperature", "ns=2;s=T", &d),
            r#"{"name":"temperature","node":"ns=2;s=T","reading":81.4,"status":"Good","source_time":null,"server_time":null}"#
        );
    }
}
