// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A message's values to CEL's and back. Every number becomes a CEL `double`, so arithmetic mixes them freely; a
//! number too large to calculate with stays text, as it came. A record made by an expression has its fields in
//! name order (CEL's maps have none of their own), so the same expression always writes the same message.

use std::collections::HashMap;
use std::sync::Arc;

use cel::Value as Cel;
use cel::objects::{Key, Map, OptionalValue};
use openduct_value::{Record, Value};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

pub(crate) fn to_cel(value: &Value) -> Cel {
    match value {
        Value::Null => Cel::Null,
        Value::Bool(b) => Cel::Bool(*b),
        Value::Number(n) => number_to_cel(*n),
        Value::BigNumber(n) => Cel::String(Arc::new(n.to_string())),
        Value::Text(t) => Cel::String(Arc::new(t.to_string())),
        Value::Time(t) => Cel::Timestamp(t.fixed_offset()),
        Value::List(items) => Cel::List(Arc::new(items.iter().map(to_cel).collect())),
        Value::Record(r) => {
            let map: HashMap<Key, Cel> = r
                .iter()
                .map(|(k, v)| (Key::String(Arc::new(k.clone())), to_cel(v)))
                .collect();
            Cel::Map(Map { map: Arc::new(map) })
        }
    }
}

/// A number as a CEL `double` - unless it is whole and a double would change it (an identifier of more than 15
/// digits): then it is CEL's `int` (or `uint`), kept exactly, or text if it is larger still.
fn number_to_cel(n: Decimal) -> Cel {
    let f = n.to_f64().unwrap_or(f64::NAN);
    let exact = Decimal::from_str_exact(&format!("{f}")).is_ok_and(|back| back == n);
    if exact || !n.fract().is_zero() {
        return Cel::Float(f);
    }
    n.to_i64()
        .map(Cel::Int)
        .or_else(|| n.to_u64().map(Cel::UInt))
        .unwrap_or_else(|| Cel::String(Arc::new(n.to_string())))
}

/// A CEL answer as a message value; `Err` for what a message cannot hold (a function).
pub(crate) fn from_cel(value: Cel) -> Result<Value, String> {
    Ok(match value {
        Cel::Null => Value::Null,
        Cel::Bool(b) => Value::Bool(b),
        Cel::Int(n) => Value::number(Decimal::from(n)),
        Cel::UInt(n) => Value::number(Decimal::from(n)),
        Cel::Float(f) if !f.is_finite() => return Err("a number too large to write".to_owned()),
        Cel::Float(f) => number(f),
        Cel::String(s) => Value::text(s.as_str()),
        Cel::Bytes(b) => Value::text(hex(&b)),
        Cel::Timestamp(t) => Value::Time(t.to_utc()),
        Cel::Duration(d) => Value::text(duration(d)),
        Cel::List(items) => Value::list(
            items
                .iter()
                .cloned()
                .map(from_cel)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Cel::Map(m) => {
            let mut fields: Vec<(String, Value)> = Vec::with_capacity(m.map.len());
            for (k, v) in m.map.iter() {
                fields.push((key(k), from_cel(v.clone())?));
            }
            fields.sort_by(|a, b| a.0.cmp(&b.0));
            Value::record(fields.into_iter().collect::<Record>())
        }
        Cel::Function(name, _) => return Err(format!("{name} is a function, not a value")),
        // An optional (`regex.extract`, `m.?field`): its value, or null when it has none.
        Cel::Opaque(o) if o.downcast_ref::<OptionalValue>().is_some() => {
            match o.downcast_ref::<OptionalValue>().and_then(OptionalValue::value) {
                Some(v) => from_cel(v.clone())?,
                None => Value::Null,
            }
        }
        Cel::Opaque(_) | Cel::Struct(_) => return Err("a value a message cannot hold".to_owned()),
    })
}

/// A double as a decimal, written as briefly as it reads back (`0.1 + 0.2` is `0.30000000000000004`, as in
/// JavaScript); not a number, or infinite (a division by zero), is null.
pub(crate) fn number(f: f64) -> Value {
    if !f.is_finite() {
        return Value::Null;
    }
    let shortest = format!("{f}");
    Decimal::from_str_exact(&shortest)
        .or_else(|_| Decimal::from_scientific(&format!("{f:e}")))
        .map(Value::number)
        .unwrap_or_else(|_| Value::BigNumber(Arc::from(shortest.as_str())))
}

fn key(k: &Key) -> String {
    match k {
        Key::Int(n) => n.to_string(),
        Key::Uint(n) => n.to_string(),
        Key::Bool(b) => b.to_string(),
        Key::String(s) => s.to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A duration as CEL writes one: seconds, with a fraction when there is one (`90s`, `1.5s`).
pub(crate) fn duration(d: chrono::Duration) -> String {
    let micros = d.num_microseconds().unwrap_or(i64::MAX);
    let seconds = Decimal::new(micros, 6).normalize();
    format!("{seconds}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_become_doubles_and_come_back_as_briefly_as_they_read() {
        let v = Value::parse_json(r#"{"a": 12.50, "b": 3, "c": 123456789012345678901234567890123}"#).unwrap();
        let Cel::Map(m) = to_cel(&v) else { panic!() };
        assert_eq!(
            m.map.get(&Key::String(Arc::new("a".into()))),
            Some(&Cel::Float(12.5))
        );
        assert_eq!(
            m.map.get(&Key::String(Arc::new("b".into()))),
            Some(&Cel::Float(3.0))
        );
        assert!(
            matches!(
                m.map.get(&Key::String(Arc::new("c".into()))),
                Some(Cel::String(_))
            ),
            "kept as written"
        );
        assert_eq!(
            from_cel(Cel::Float(0.1 + 0.2)).unwrap().to_json(),
            "0.30000000000000004"
        );
        assert_eq!(from_cel(Cel::Float(37.5)).unwrap().to_json(), "37.5");
        assert_eq!(
            from_cel(Cel::Float(1e21)).unwrap().to_json(),
            "1000000000000000000000"
        );
        assert!(
            from_cel(Cel::Float(f64::INFINITY)).is_err(),
            "no message can hold it"
        );
    }

    #[test]
    fn a_record_made_by_an_expression_has_its_fields_in_name_order() {
        let mut map = HashMap::new();
        for (k, n) in [("zeta", 1), ("alpha", 2), ("mid", 3)] {
            map.insert(Key::String(Arc::new(k.into())), Cel::Int(n));
        }
        let v = from_cel(Cel::Map(Map { map: Arc::new(map) })).unwrap();
        assert_eq!(v.to_json(), r#"{"alpha":2,"mid":3,"zeta":1}"#);
        assert_eq!(
            from_cel(Cel::Duration(chrono::Duration::milliseconds(1500)))
                .unwrap()
                .to_json(),
            r#""1.5s""#
        );
    }
}
