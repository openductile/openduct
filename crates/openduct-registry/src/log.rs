// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The registry's changes as records of its topic (`_schemas`, compacted): keys and values in the JSON Confluent's
//! registry writes, so a topic written by one can be read by the other.
//!
//! | Change | Key | Value |
//! |---|---|---|
//! | a version | `{"keytype":"SCHEMA","subject":…,"version":…,"magic":1}` | `{"subject","version","id","schema","deleted"}`, and `"schemaType"` when it is not Avro |
//! | a version deleted for good | the same key | none (a tombstone) |
//! | a compatibility level | `{"keytype":"CONFIG","subject":… or null,"magic":0}` | `{"compatibilityLevel":…}`, or none when cleared |
//! | the mode | `{"keytype":"MODE","subject":null,"magic":0}` | `{"mode":…}` |
//!
//! A key is written the same way every time, character for character - compaction keeps the newest record of each
//! key, so a version's record replaces the one before it and a tombstone removes it. Reading takes the same keys
//! with their attributes in any order, Confluent's `DELETE_SUBJECT` and `NOOP` records among them.

use serde_json::{Value, json};

use crate::registry::{Change, Compatibility, Mode, Reference, SchemaType};

/// The topic the registry keeps its changes in.
pub const TOPIC: &str = "_schemas";

/// One record of the topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    /// `None`: a tombstone.
    pub value: Option<Vec<u8>>,
}

fn quoted(text: &str) -> String {
    Value::String(text.to_owned()).to_string()
}

fn key(keytype: &str, subject: Option<&str>, version: Option<i32>, magic: u8) -> Vec<u8> {
    let subject = subject.map_or_else(|| "null".to_owned(), quoted);
    let version = version.map_or_else(String::new, |v| format!("\"version\":{v},"));
    format!("{{\"keytype\":\"{keytype}\",\"subject\":{subject},{version}\"magic\":{magic}}}").into_bytes()
}

/// A change as the record that carries it.
pub fn encode(change: &Change) -> Record {
    match change {
        Change::Schema {
            subject,
            version,
            id,
            schema,
            kind,
            references,
            deleted,
        } => {
            // Left out for Avro, as Confluent's registry leaves it out; references, when there are none.
            let mut kind = match kind {
                SchemaType::Avro => String::new(),
                other => format!(",\"schemaType\":\"{}\"", other.name()),
            };
            if !references.is_empty() {
                let list: Vec<String> = references
                    .iter()
                    .map(|r| {
                        format!(
                            "{{\"name\":{},\"subject\":{},\"version\":{}}}",
                            quoted(&r.name),
                            quoted(&r.subject),
                            r.version
                        )
                    })
                    .collect();
                kind.push_str(&format!(",\"references\":[{}]", list.join(",")));
            }
            Record {
                key: key("SCHEMA", Some(subject), Some(*version), 1),
                value: Some(
                    format!(
                        "{{\"subject\":{},\"version\":{version},\"id\":{id},\"schema\":{}{kind},\"deleted\":{deleted}}}",
                        quoted(subject),
                        quoted(schema)
                    )
                    .into_bytes(),
                ),
            }
        }
        Change::Forget { subject, version } => Record {
            key: key("SCHEMA", Some(subject), Some(*version), 1),
            value: None,
        },
        Change::Config { subject, level } => Record {
            key: key("CONFIG", subject.as_deref(), None, 0),
            value: level.map(|l| json!({ "compatibilityLevel": l.name() }).to_string().into_bytes()),
        },
        Change::Mode(mode) => Record {
            key: key("MODE", None, None, 0),
            value: Some(json!({ "mode": mode.name() }).to_string().into_bytes()),
        },
        Change::DeleteSubject { subject, upto } => Record {
            key: key("DELETE_SUBJECT", Some(subject), None, 0),
            value: Some(format!("{{\"subject\":{},\"version\":{upto}}}", quoted(subject)).into_bytes()),
        },
    }
}

/// The change a record carries; `None` for one that carries none (Confluent's `NOOP`, a kind of key this
/// registry does not know). An error names what is wrong with a record that should have carried one.
pub fn decode(key: &[u8], value: Option<&[u8]>) -> Result<Option<Change>, String> {
    let key: Value = serde_json::from_slice(key).map_err(|e| format!("a key that is not JSON: {e}"))?;
    let keytype = key.get("keytype").and_then(Value::as_str).unwrap_or_default();
    let subject = key.get("subject").and_then(Value::as_str).map(str::to_owned);
    let value: Option<Value> = match value {
        Some(bytes) => Some(
            serde_json::from_slice(bytes).map_err(|e| format!("a {keytype} value that is not JSON: {e}"))?,
        ),
        None => None,
    };
    let number = |v: &Value, name: &str| {
        v.get(name)
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or_else(|| format!("a {keytype} record without a {name}"))
    };
    match keytype {
        "SCHEMA" => {
            let subject = subject.ok_or("a SCHEMA key without a subject")?;
            let version = number(&key, "version")?;
            let Some(value) = value else {
                return Ok(Some(Change::Forget { subject, version }));
            };
            let schema = value
                .get("schema")
                .and_then(Value::as_str)
                .ok_or("a SCHEMA record without a schema")?;
            let kind = match value.get("schemaType").and_then(Value::as_str) {
                None => SchemaType::Avro,
                Some(name) => SchemaType::parse(name).ok_or_else(|| {
                    format!(
                        "{subject:?} version {version} is a {name} schema, a kind this registry does not have"
                    )
                })?,
            };
            let mut references = Vec::new();
            for r in value
                .get("references")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let text = |name: &str| r.get(name).and_then(Value::as_str).map(str::to_owned);
                match (text("name"), text("subject")) {
                    (Some(name), Some(subject)) => references.push(Reference {
                        name,
                        subject,
                        version: number(r, "version")?,
                    }),
                    _ => return Err("a reference without a name or a subject".to_owned()),
                }
            }
            Ok(Some(Change::Schema {
                subject,
                version,
                id: number(&value, "id")?,
                schema: schema.to_owned(),
                kind,
                references,
                deleted: value.get("deleted").and_then(Value::as_bool).unwrap_or(false),
            }))
        }
        "CONFIG" => {
            let level = match &value {
                Some(v) => {
                    let name = v
                        .get("compatibilityLevel")
                        .and_then(Value::as_str)
                        .ok_or("a CONFIG record without a compatibilityLevel")?;
                    Some(
                        Compatibility::parse(name)
                            .ok_or_else(|| format!("{name:?} is not a compatibility level"))?,
                    )
                }
                None => None,
            };
            Ok(Some(Change::Config { subject, level }))
        }
        "MODE" => {
            let Some(value) = value else {
                return Ok(Some(Change::Mode(Mode::ReadWrite)));
            };
            let name = value.get("mode").and_then(Value::as_str).unwrap_or_default();
            // A subject's own mode is not kept here: only the registry's.
            if subject.is_some() {
                return Ok(None);
            }
            Ok(Some(Change::Mode(Mode::parse(name).ok_or_else(|| {
                format!("{name:?} is not a mode this registry has")
            })?)))
        }
        "DELETE_SUBJECT" => {
            let subject = subject.ok_or("a DELETE_SUBJECT key without a subject")?;
            match value {
                Some(value) => Ok(Some(Change::DeleteSubject {
                    upto: number(&value, "version")?,
                    subject,
                })),
                None => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use crate::registry::tests::{V1, V2, register};

    #[test]
    fn a_registry_read_back_from_its_records_is_the_same_registry() {
        // Every change a registry makes, kept as the record it would write.
        let mut live = Registry::default();
        let mut topic: Vec<Record> = Vec::new();
        let mut write = |r: &mut Registry, changes: Vec<Change>| {
            for c in &changes {
                topic.push(encode(c));
                r.apply(c).unwrap();
            }
        };
        let (_, c) = live.register("orders-value", V1).unwrap();
        write(&mut live, c);
        let (_, c) = live.register("orders-value", V2).unwrap();
        write(&mut live, c);
        let (_, c) = live.register("with \"quotes\" and \\ in it", V1).unwrap();
        write(&mut live, c);
        let c = live
            .set_level(Some("orders-value"), Some(Compatibility::FullTransitive))
            .unwrap();
        write(&mut live, c);
        let c = live.set_level(None, Some(Compatibility::None)).unwrap();
        write(&mut live, c);
        let (_, c) = live
            .delete_version("orders-value", crate::registry::Wanted::Number(1), false)
            .unwrap();
        write(&mut live, c);
        let (_, c) = live
            .delete_version("orders-value", crate::registry::Wanted::Number(1), true)
            .unwrap();
        write(&mut live, c);
        // A JSON Schema and a Protobuf schema: each says its kind in its record, and reads back as that kind.
        let (_, c) = live
            .register_as("clicks-value", r#"{"type":"object"}"#, SchemaType::Json)
            .unwrap();
        write(&mut live, c);
        let (_, c) = live
            .register_as(
                "views-value",
                "syntax = \"proto3\"; message View { string page = 1; }",
                SchemaType::Protobuf,
            )
            .unwrap();
        write(&mut live, c);
        // A schema that uses another subject's: its record says which, and reads back with it.
        let money = r#"{"type":"record","name":"Money","namespace":"shop","fields":[{"name":"amount","type":"long"}]}"#;
        let priced = r#"{"type":"record","name":"Priced","namespace":"shop","fields":[{"name":"price","type":"shop.Money"}]}"#;
        let (_, c) = live.register("money-value", money).unwrap();
        write(&mut live, c);
        let uses = [Reference {
            name: "shop.Money".into(),
            subject: "money-value".into(),
            version: 1,
        }];
        let (_, c) = live
            .register_with("priced-value", priced, SchemaType::Avro, &uses)
            .unwrap();
        write(&mut live, c);
        let c = live.set_mode(Mode::ReadOnly);
        write(&mut live, c);
        assert!(topic.iter().filter_map(|r| r.value.as_ref()).any(|v| {
            String::from_utf8_lossy(v)
                .contains(r#""references":[{"name":"shop.Money","subject":"money-value","version":1}]"#)
        }));
        let typed: Vec<String> = topic
            .iter()
            .filter_map(|r| r.value.as_ref())
            .map(|v| String::from_utf8(v.clone()).unwrap())
            .filter(|v| v.contains("schemaType"))
            .collect();
        assert_eq!(typed.len(), 2, "Avro records say no type, as Confluent's do not");
        assert!(
            typed[0].contains(r#""schemaType":"JSON""#) && typed[1].contains(r#""schemaType":"PROTOBUF""#)
        );

        let mut read_back = Registry::default();
        for record in &topic {
            let change = decode(&record.key, record.value.as_deref()).unwrap().unwrap();
            read_back.apply(&change).unwrap();
        }
        assert_eq!(read_back, live);
        // And from the topic as compaction leaves it: only the newest record of each key, tombstones gone.
        let mut newest: Vec<&Record> = Vec::new();
        for record in &topic {
            newest.retain(|kept| kept.key != record.key);
            newest.push(record);
        }
        assert!(newest.len() < topic.len());
        let mut compacted = Registry::default();
        for record in newest.iter().filter(|r| r.value.is_some()) {
            let change = decode(&record.key, record.value.as_deref()).unwrap().unwrap();
            compacted.apply(&change).unwrap();
        }
        assert_eq!(compacted, live);
    }

    #[test]
    fn keys_are_written_the_same_every_time_and_read_in_any_order() {
        let change = Change::Schema {
            subject: "orders-value".into(),
            version: 3,
            id: 7,
            schema: V1.into(),
            kind: SchemaType::Avro,
            references: Vec::new(),
            deleted: false,
        };
        let record = encode(&change);
        assert_eq!(
            String::from_utf8(record.key.clone()).unwrap(),
            r#"{"keytype":"SCHEMA","subject":"orders-value","version":3,"magic":1}"#
        );
        assert_eq!(
            encode(&Change::Config {
                subject: None,
                level: Some(Compatibility::Full)
            }),
            Record {
                key: br#"{"keytype":"CONFIG","subject":null,"magic":0}"#.to_vec(),
                value: Some(br#"{"compatibilityLevel":"FULL"}"#.to_vec()),
            }
        );
        // As another registry might write the same key.
        let other = br#"{"magic":1,"version":3,"subject":"orders-value","keytype":"SCHEMA"}"#;
        assert_eq!(decode(other, record.value.as_deref()).unwrap(), Some(change));
        assert_eq!(
            decode(other, None).unwrap(),
            Some(Change::Forget {
                subject: "orders-value".into(),
                version: 3
            })
        );
    }

    #[test]
    fn records_of_another_registry_are_read_skipped_or_refused_with_the_reason() {
        assert_eq!(
            decode(br#"{"keytype":"NOOP","magic":0}"#, Some(b"{}")).unwrap(),
            None
        );
        assert_eq!(
            decode(br#"{"keytype":"SOMETHING_NEW","magic":0}"#, None).unwrap(),
            None
        );
        // Confluent's subject delete: every version up to this one, marked deleted.
        let mut r = Registry::default();
        register(&mut r, "s", V1).unwrap();
        register(&mut r, "s", V2).unwrap();
        let delete = decode(
            br#"{"keytype":"DELETE_SUBJECT","subject":"s","magic":0}"#,
            Some(br#"{"subject":"s","version":2}"#),
        )
        .unwrap()
        .unwrap();
        r.apply(&delete).unwrap();
        assert!(r.subjects(false).is_empty());
        assert_eq!(r.versions("s", true).unwrap(), vec![1, 2]);
        for (key, value, why) in [
            (&b"not json"[..], None, "not JSON"),
            (
                br#"{"keytype":"SCHEMA","subject":"s","magic":1}"#,
                Some(&b"{}"[..]),
                "without a version",
            ),
            (
                br#"{"keytype":"SCHEMA","subject":"s","version":1,"magic":1}"#,
                Some(&b"{}"[..]),
                "without a schema",
            ),
            (
                br#"{"keytype":"SCHEMA","subject":"s","version":1,"magic":1}"#,
                Some(&br#"{"id":1,"schema":"<a/>","schemaType":"XSD"}"#[..]),
                "XSD",
            ),
            (
                br#"{"keytype":"CONFIG","subject":null,"magic":0}"#,
                Some(&br#"{"compatibilityLevel":"SOME"}"#[..]),
                "not a compatibility level",
            ),
        ] {
            let e = decode(key, value).unwrap_err();
            assert!(e.contains(why), "{e}");
        }
    }
}
