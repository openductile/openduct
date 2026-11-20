// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Avro schemas, as far as a registry needs them: read from their JSON, checked, and compared - can data written
//! with one schema be read with another?
//!
//! The rules are the Avro specification's "schema resolution": the same ones Avro's own readers apply, and so the
//! ones a registry must hold new versions to.
//!
//! - A primitive is read as itself, or as a wider one: int as long, float or double; long as float or double;
//!   float as double; string and bytes as each other.
//! - Arrays and maps are read when their items and values are.
//! - A record is read by a record of the same name (or one that lists it as an alias): each of the reader's fields
//!   is matched by name (or the reader field's aliases) and read in turn; a reader field the writer does not have
//!   needs a default. Fields only the writer has are skipped.
//! - An enum is read by an enum of the same name when the reader knows every symbol the writer may send - or has
//!   a default symbol.
//! - A fixed is read by a fixed of the same name and size.
//! - A union written is read when every one of its branches is; a union reading takes what any of its branches
//!   reads.
//!
//! Nothing here panics on any input, and a schema that refers to itself (a linked list, a tree) is followed once.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde_json::Value;

/// How deep a schema may nest: more is refused rather than followed.
const MAX_DEPTH: usize = 64;

/// One Avro type.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(Box<Type>),
    Map(Box<Type>),
    Union(Vec<Type>),
    /// A record, enum or fixed, by its full name: defined in [`Schema::named`].
    Named(String),
}

/// A record's field.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub aliases: Vec<String>,
    pub of: Type,
    pub has_default: bool,
}

/// What a name stands for.
#[derive(Debug, Clone, PartialEq)]
pub enum Definition {
    Record { fields: Vec<Field> },
    Enum { symbols: Vec<String>, has_default: bool },
    Fixed { size: u64 },
}

/// A named type: its definition and the other names it answers to.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedType {
    pub aliases: Vec<String>,
    pub definition: Definition,
}

/// A schema read and checked: its top type and every record, enum and fixed it defines, by full name.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub top: Type,
    pub named: BTreeMap<String, NamedType>,
}

/// Why a schema is not a valid Avro schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub String);

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn invalid<T>(why: impl Into<String>) -> Result<T, Invalid> {
    Err(Invalid(why.into()))
}

/// A name as Avro allows it: a letter or underscore, then letters, digits and underscores.
fn simple_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn full_name(name: &str, namespace: &str) -> Result<String, Invalid> {
    if name.contains('.') {
        if name.split('.').all(simple_name) {
            return Ok(name.to_owned());
        }
        return invalid(format!("{name:?} is not a valid name"));
    }
    if !simple_name(name) {
        return invalid(format!("{name:?} is not a valid name"));
    }
    if !namespace.is_empty() && !namespace.split('.').all(simple_name) {
        return invalid(format!("{namespace:?} is not a valid namespace"));
    }
    Ok(if namespace.is_empty() {
        name.to_owned()
    } else {
        format!("{namespace}.{name}")
    })
}

/// The part of a full name after its last dot.
fn short(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn primitive(name: &str) -> Option<Type> {
    Some(match name {
        "null" => Type::Null,
        "boolean" => Type::Boolean,
        "int" => Type::Int,
        "long" => Type::Long,
        "float" => Type::Float,
        "double" => Type::Double,
        "bytes" => Type::Bytes,
        "string" => Type::String,
        _ => return None,
    })
}

struct Parser {
    named: BTreeMap<String, NamedType>,
    /// Names used before their definition was finished (a record that refers to itself) or at all: checked at
    /// the end.
    used: BTreeSet<String>,
}

impl Parser {
    fn text<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
        object.get(key).and_then(Value::as_str)
    }

    fn aliases(
        object: &serde_json::Map<String, Value>,
        namespace: &str,
        full: bool,
    ) -> Result<Vec<String>, Invalid> {
        let Some(list) = object.get("aliases") else {
            return Ok(Vec::new());
        };
        let Some(list) = list.as_array() else {
            return invalid("aliases must be a list of names");
        };
        list.iter()
            .map(|a| match a.as_str() {
                Some(a) if full => full_name(a, namespace),
                Some(a) if simple_name(a) => Ok(a.to_owned()),
                _ => invalid("an alias must be a name"),
            })
            .collect()
    }

    fn parse(&mut self, value: &Value, namespace: &str, depth: usize) -> Result<Type, Invalid> {
        if depth > MAX_DEPTH {
            return invalid(format!("the schema nests deeper than {MAX_DEPTH} levels"));
        }
        match value {
            Value::String(name) => Ok(match primitive(name) {
                Some(t) => t,
                None => {
                    let full = full_name(name, namespace)?;
                    self.used.insert(full.clone());
                    Type::Named(full)
                }
            }),
            Value::Array(branches) => {
                let mut out = Vec::with_capacity(branches.len());
                for b in branches {
                    let t = self.parse(b, namespace, depth + 1)?;
                    if matches!(t, Type::Union(_)) {
                        return invalid("a union may not hold another union");
                    }
                    if out.iter().any(|seen| same_kind(seen, &t)) {
                        return invalid("a union may not hold two branches of the same kind");
                    }
                    out.push(t);
                }
                Ok(Type::Union(out))
            }
            Value::Object(object) => self.object(object, namespace, depth),
            _ => invalid("a schema is a name, a list of schemas (a union) or an object"),
        }
    }

    fn object(
        &mut self,
        object: &serde_json::Map<String, Value>,
        namespace: &str,
        depth: usize,
    ) -> Result<Type, Invalid> {
        let Some(kind) = object.get("type") else {
            return invalid("a schema object needs a \"type\"");
        };
        let Some(kind) = kind.as_str() else {
            // {"type": {...}} and {"type": [...]}: the inner schema.
            return self.parse(kind, namespace, depth + 1);
        };
        match kind {
            "array" => {
                let items = object
                    .get("items")
                    .ok_or(Invalid("an array needs \"items\"".into()))?;
                Ok(Type::Array(Box::new(self.parse(items, namespace, depth + 1)?)))
            }
            "map" => {
                let values = object
                    .get("values")
                    .ok_or(Invalid("a map needs \"values\"".into()))?;
                Ok(Type::Map(Box::new(self.parse(values, namespace, depth + 1)?)))
            }
            "record" | "error" | "enum" | "fixed" => self.definition(kind, object, namespace, depth),
            // A primitive with attributes (a logical type): read as the primitive.
            other => match primitive(other) {
                Some(t) => Ok(t),
                None => {
                    let full = full_name(other, namespace)?;
                    self.used.insert(full.clone());
                    Ok(Type::Named(full))
                }
            },
        }
    }

    fn definition(
        &mut self,
        kind: &str,
        object: &serde_json::Map<String, Value>,
        namespace: &str,
        depth: usize,
    ) -> Result<Type, Invalid> {
        let name = Self::text(object, "name").ok_or(Invalid(format!("a {kind} needs a \"name\"")))?;
        let own_namespace = match name.rsplit_once('.') {
            Some((space, _)) => space,
            None => Self::text(object, "namespace").unwrap_or(namespace),
        };
        let full = full_name(name, own_namespace)?;
        if primitive(short(&full)).is_some() && !full.contains('.') {
            return invalid(format!("{full:?} is the name of a primitive type"));
        }
        if self.named.contains_key(&full) {
            return invalid(format!("{full:?} is defined twice"));
        }
        let aliases = Self::aliases(object, own_namespace, true)?;
        // Known from here on, so a record may refer to itself.
        self.named.insert(
            full.clone(),
            NamedType {
                aliases: aliases.clone(),
                definition: Definition::Fixed { size: 0 },
            },
        );
        let definition = match kind {
            "enum" => {
                let symbols = object
                    .get("symbols")
                    .and_then(Value::as_array)
                    .ok_or(Invalid(format!("the enum {full:?} needs \"symbols\"")))?;
                let mut names = Vec::with_capacity(symbols.len());
                for s in symbols {
                    match s.as_str() {
                        Some(s) if simple_name(s) && !names.iter().any(|n| n == s) => {
                            names.push(s.to_owned())
                        }
                        _ => {
                            return invalid(format!(
                                "the enum {full:?} has a symbol that is not a name, or twice"
                            ));
                        }
                    }
                }
                let default = object.get("default");
                if let Some(d) = default
                    && !d.as_str().is_some_and(|d| names.iter().any(|n| n == d))
                {
                    return invalid(format!(
                        "the enum {full:?} has a default that is not one of its symbols"
                    ));
                }
                Definition::Enum {
                    symbols: names,
                    has_default: default.is_some(),
                }
            }
            "fixed" => {
                let size = object.get("size").and_then(Value::as_u64).ok_or(Invalid(format!(
                    "the fixed {full:?} needs a \"size\" of 0 or more"
                )))?;
                Definition::Fixed { size }
            }
            _ => {
                let list = object
                    .get("fields")
                    .and_then(Value::as_array)
                    .ok_or(Invalid(format!("the record {full:?} needs \"fields\"")))?;
                let mut fields: Vec<Field> = Vec::with_capacity(list.len());
                for f in list {
                    let f = f
                        .as_object()
                        .ok_or(Invalid(format!("a field of {full:?} is not an object")))?;
                    let field =
                        Self::text(f, "name").ok_or(Invalid(format!("a field of {full:?} has no name")))?;
                    if !simple_name(field) || fields.iter().any(|seen| seen.name == field) {
                        return invalid(format!(
                            "{full:?} has a field {field:?} that is not a name, or twice"
                        ));
                    }
                    let of = f
                        .get("type")
                        .ok_or(Invalid(format!("the field {field:?} of {full:?} has no type")))?;
                    fields.push(Field {
                        name: field.to_owned(),
                        aliases: Self::aliases(f, own_namespace, false)?,
                        of: self.parse(of, own_namespace, depth + 1)?,
                        has_default: f.contains_key("default"),
                    });
                }
                Definition::Record { fields }
            }
        };
        self.named.insert(full.clone(), NamedType { aliases, definition });
        Ok(Type::Named(full))
    }
}

/// Two union branches a reader could not tell apart: the same primitive, two arrays, two maps, or the same name.
fn same_kind(a: &Type, b: &Type) -> bool {
    match (a, b) {
        (Type::Array(_), Type::Array(_)) | (Type::Map(_), Type::Map(_)) => true,
        (Type::Named(x), Type::Named(y)) => x == y,
        (Type::Array(_) | Type::Map(_) | Type::Named(_), _)
        | (_, Type::Array(_) | Type::Map(_) | Type::Named(_)) => false,
        _ => a == b,
    }
}

impl Schema {
    /// Reads a schema from its JSON text.
    pub fn parse(text: &str) -> Result<Self, Invalid> {
        let value: Value = serde_json::from_str(text).map_err(|e| Invalid(format!("not JSON: {e}")))?;
        Self::from_json(&value)
    }

    pub fn from_json(value: &Value) -> Result<Self, Invalid> {
        Self::from_json_with(value, &BTreeMap::new())
    }

    /// As [`Schema::from_json`], for a schema that uses named types defined elsewhere - in the schemas it
    /// refers to: `known` holds them, and the schema may name any of them (and may not define one again).
    pub fn from_json_with(value: &Value, known: &BTreeMap<String, NamedType>) -> Result<Self, Invalid> {
        let mut parser = Parser {
            named: known.clone(),
            used: BTreeSet::new(),
        };
        let top = parser.parse(value, "", 0)?;
        if let Some(unknown) = parser.used.iter().find(|name| !parser.named.contains_key(*name)) {
            return invalid(format!("{unknown:?} is not a type this schema defines"));
        }
        Ok(Self {
            top,
            named: parser.named,
        })
    }

    /// Whether data written with `writer` can be read with this schema; if not, the first reason why.
    pub fn can_read(&self, writer: &Schema) -> Result<(), String> {
        Check {
            reader: self,
            writer,
            seen: BTreeSet::new(),
        }
        .types(&self.top, &writer.top, "the schema", 0)
    }
}

struct Check<'a> {
    reader: &'a Schema,
    writer: &'a Schema,
    /// Pairs of names already being compared: a schema that refers to itself is followed once.
    seen: BTreeSet<(String, String)>,
}

fn kind_of(t: &Type, schema: &Schema) -> String {
    match t {
        Type::Null => "null".into(),
        Type::Boolean => "boolean".into(),
        Type::Int => "int".into(),
        Type::Long => "long".into(),
        Type::Float => "float".into(),
        Type::Double => "double".into(),
        Type::Bytes => "bytes".into(),
        Type::String => "string".into(),
        Type::Array(_) => "an array".into(),
        Type::Map(_) => "a map".into(),
        Type::Union(_) => "a union".into(),
        Type::Named(name) => match schema.named.get(name).map(|n| &n.definition) {
            Some(Definition::Record { .. }) => format!("the record {name}"),
            Some(Definition::Enum { .. }) => format!("the enum {name}"),
            Some(Definition::Fixed { .. }) => format!("the fixed {name}"),
            None => name.clone(),
        },
    }
}

impl Check<'_> {
    fn types(&mut self, reader: &Type, writer: &Type, at: &str, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH * 2 {
            return Err(format!("{at}: nested too deep to compare"));
        }
        // A union written: every branch must be readable.
        if let Type::Union(branches) = writer {
            for b in branches {
                self.types(reader, b, at, depth + 1)?;
            }
            return Ok(());
        }
        // A union reading: any branch that reads it.
        if let Type::Union(branches) = reader {
            let before = self.seen.clone();
            for b in branches {
                if self.types(b, writer, at, depth + 1).is_ok() {
                    return Ok(());
                }
                self.seen = before.clone();
            }
            return Err(format!(
                "{at}: no branch of the reader's union reads {}",
                kind_of(writer, self.writer)
            ));
        }
        let promoted = matches!(
            (writer, reader),
            (Type::Int, Type::Long | Type::Float | Type::Double)
                | (Type::Long, Type::Float | Type::Double)
                | (Type::Float, Type::Double)
                | (Type::String, Type::Bytes)
                | (Type::Bytes, Type::String)
        );
        match (reader, writer) {
            _ if promoted => Ok(()),
            (Type::Array(r), Type::Array(w)) => self.types(r, w, &format!("{at}, its items"), depth + 1),
            (Type::Map(r), Type::Map(w)) => self.types(r, w, &format!("{at}, its values"), depth + 1),
            (Type::Named(r), Type::Named(w)) => self.named(r, w, at, depth),
            (r, w) if r == w && !matches!(r, Type::Array(_) | Type::Map(_) | Type::Named(_)) => Ok(()),
            _ => Err(format!(
                "{at}: written as {}, read as {}",
                kind_of(writer, self.writer),
                kind_of(reader, self.reader)
            )),
        }
    }

    fn named(&mut self, reader: &str, writer: &str, at: &str, depth: usize) -> Result<(), String> {
        let (Some(r), Some(w)) = (self.reader.named.get(reader), self.writer.named.get(writer)) else {
            return Err(format!("{at}: a type is not defined"));
        };
        // The same name (its last part, as Avro's readers compare), or one the reader lists as an alias.
        let answers = short(reader) == short(writer)
            || r.aliases.iter().any(|a| a == writer || short(a) == short(writer));
        if !answers {
            return Err(format!("{at}: written as {writer}, read as {reader}"));
        }
        if !self.seen.insert((reader.to_owned(), writer.to_owned())) {
            return Ok(());
        }
        match (&r.definition, &w.definition) {
            (Definition::Fixed { size: rs }, Definition::Fixed { size: ws }) if rs == ws => Ok(()),
            (Definition::Fixed { size: rs }, Definition::Fixed { size: ws }) => Err(format!(
                "{at}: the fixed {writer} is {ws} bytes written and {rs} read"
            )),
            (
                Definition::Enum {
                    symbols: known,
                    has_default,
                },
                Definition::Enum { symbols: sent, .. },
            ) => match sent.iter().find(|s| !known.contains(s)) {
                Some(missing) if !has_default => Err(format!(
                    "{at}: the reader's enum {reader} has no symbol {missing} and no default"
                )),
                _ => Ok(()),
            },
            (Definition::Record { fields: wanted }, Definition::Record { fields: sent }) => {
                for field in wanted {
                    let written = sent
                        .iter()
                        .find(|w| w.name == field.name || field.aliases.contains(&w.name));
                    match written {
                        Some(w) => {
                            let at = format!("{at}, field {}", field.name);
                            self.types(&field.of, &w.of, &at, depth + 1)?;
                        }
                        None if field.has_default => {}
                        None => {
                            return Err(format!(
                                "{at}: the reader's field {} of {reader} has no default, and the writer does not \
                                 have it",
                                field.name
                            ));
                        }
                    }
                }
                Ok(())
            }
            _ => Err(format!(
                "{at}: written as {}, read as {}",
                kind_of(&Type::Named(writer.to_owned()), self.writer),
                kind_of(&Type::Named(reader.to_owned()), self.reader)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> Schema {
        Schema::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    fn reads(reader: &str, writer: &str) -> Result<(), String> {
        s(reader).can_read(&s(writer))
    }

    const USER_V1: &str = r#"{"type":"record","name":"User","namespace":"shop","fields":[
        {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

    #[test]
    fn schemas_are_read_and_what_is_not_avro_is_refused_with_the_reason() {
        let user = s(USER_V1);
        assert_eq!(user.top, Type::Named("shop.User".into()));
        assert!(matches!(
            &user.named["shop.User"].definition,
            Definition::Record { fields } if fields.len() == 2 && fields[0].of == Type::Long
        ));
        assert_eq!(s(r#""string""#).top, Type::String);
        assert_eq!(
            s(r#"{"type":"long","logicalType":"timestamp-millis"}"#).top,
            Type::Long
        );
        assert_eq!(
            s(r#"["null","string"]"#).top,
            Type::Union(vec![Type::Null, Type::String])
        );
        // A record that refers to itself.
        let list = s(r#"{"type":"record","name":"Node","fields":[
            {"name":"value","type":"int"},{"name":"next","type":["null","Node"]}]}"#);
        assert!(list.named.contains_key("Node"));
        for (bad, why) in [
            ("{", "not JSON"),
            (r#""strng""#, "not a type this schema defines"),
            (r#"{"type":"record","name":"R"}"#, "needs \"fields\""),
            (r#"{"type":"record","name":"1R","fields":[]}"#, "not a valid name"),
            (
                r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"a","type":"int"}]}"#,
                "twice",
            ),
            (r#"{"type":"enum","name":"E","symbols":["A","A"]}"#, "twice"),
            (
                r#"{"type":"enum","name":"E","symbols":["A"],"default":"B"}"#,
                "default",
            ),
            (r#"{"type":"fixed","name":"F","size":-1}"#, "size"),
            (r#"["int","int"]"#, "same kind"),
            (r#"["int",["null","string"]]"#, "another union"),
            (r#"{"type":"array"}"#, "items"),
            (r#"7"#, "a schema is"),
        ] {
            let e = Schema::parse(bad).unwrap_err();
            assert!(e.0.contains(why), "{bad}: {e}");
        }
        // Nested without end: refused, not followed - by this reader's limit, and past that by the JSON reader's.
        let nested = |levels: usize| {
            format!(
                "{}\"int\"{}",
                r#"{"type":"array","items":"#.repeat(levels),
                "}".repeat(levels)
            )
        };
        assert!(Schema::parse(&nested(60)).is_ok());
        assert!(Schema::parse(&nested(100)).unwrap_err().0.contains("deeper"));
        assert!(Schema::parse(&nested(5_000)).is_err());
    }

    #[test]
    fn a_new_field_needs_a_default_to_read_old_data_and_a_removed_one_is_skipped() {
        let with_email = r#"{"type":"record","name":"User","namespace":"shop","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"email","type":"string"}]}"#;
        let with_default = r#"{"type":"record","name":"User","namespace":"shop","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},
            {"name":"email","type":["null","string"],"default":null}]}"#;
        let why = reads(with_email, USER_V1).unwrap_err();
        assert!(why.contains("field email") && why.contains("no default"), "{why}");
        assert_eq!(reads(with_default, USER_V1), Ok(()));
        // The old schema reads the new data: the extra field is skipped.
        assert_eq!(reads(USER_V1, with_email), Ok(()));
        // A field renamed is found by the reader's alias.
        let renamed = r#"{"type":"record","name":"User","namespace":"shop","fields":[
            {"name":"id","type":"long"},{"name":"full_name","type":"string","aliases":["name"]}]}"#;
        assert_eq!(reads(renamed, USER_V1), Ok(()));
        assert!(reads(USER_V1, renamed).is_err());
    }

    #[test]
    fn primitives_widen_one_way_only() {
        for (writer, reader) in [
            ("int", "long"),
            ("int", "double"),
            ("long", "float"),
            ("float", "double"),
            ("string", "bytes"),
            ("bytes", "string"),
        ] {
            assert_eq!(
                reads(&format!("{reader:?}"), &format!("{writer:?}")),
                Ok(()),
                "{writer} as {reader}"
            );
        }
        for (writer, reader) in [
            ("long", "int"),
            ("double", "float"),
            ("string", "int"),
            ("boolean", "int"),
        ] {
            let why = reads(&format!("{reader:?}"), &format!("{writer:?}")).unwrap_err();
            assert!(
                why.contains(&format!("written as {writer}, read as {reader}")),
                "{why}"
            );
        }
        let id_widened = USER_V1.replace(r#""type":"long""#, r#""type":"double""#);
        assert_eq!(reads(&id_widened, USER_V1), Ok(()));
        assert!(reads(USER_V1, &id_widened).unwrap_err().contains("field id"));
    }

    #[test]
    fn enums_fixed_arrays_maps_and_unions_follow_the_specification() {
        let colours = |symbols: &str, extra: &str| {
            format!(r#"{{"type":"enum","name":"Colour","symbols":[{symbols}]{extra}}}"#)
        };
        assert_eq!(
            reads(&colours(r#""RED","GREEN""#, ""), &colours(r#""RED""#, "")),
            Ok(())
        );
        let why = reads(&colours(r#""RED""#, ""), &colours(r#""RED","GREEN""#, "")).unwrap_err();
        assert!(why.contains("no symbol GREEN"), "{why}");
        assert_eq!(
            reads(
                &colours(r#""RED""#, r#","default":"RED""#),
                &colours(r#""RED","GREEN""#, "")
            ),
            Ok(())
        );
        let fixed = |size: u32| format!(r#"{{"type":"fixed","name":"Hash","size":{size}}}"#);
        assert_eq!(reads(&fixed(16), &fixed(16)), Ok(()));
        assert!(
            reads(&fixed(32), &fixed(16))
                .unwrap_err()
                .contains("16 bytes written and 32 read")
        );
        assert_eq!(
            reads(
                r#"{"type":"array","items":"long"}"#,
                r#"{"type":"array","items":"int"}"#
            ),
            Ok(())
        );
        assert!(
            reads(
                r#"{"type":"map","values":"int"}"#,
                r#"{"type":"map","values":"long"}"#
            )
            .is_err()
        );
        assert!(
            reads(
                r#"{"type":"map","values":"int"}"#,
                r#"{"type":"array","items":"int"}"#
            )
            .is_err()
        );
        // A value becomes optional: old data still reads; data that may be null does not read as never null.
        assert_eq!(reads(r#"["null","string"]"#, r#""string""#), Ok(()));
        assert!(
            reads(r#""string""#, r#"["null","string"]"#)
                .unwrap_err()
                .contains("written as null")
        );
        assert_eq!(reads(r#"["null","long","string"]"#, r#"["int","null"]"#), Ok(()));
        // Names must match, unless the reader says the old name is an alias.
        let other = r#"{"type":"record","name":"Customer","namespace":"shop","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
        assert!(
            reads(other, USER_V1)
                .unwrap_err()
                .contains("written as shop.User, read as shop.Customer")
        );
        assert_eq!(
            reads(
                &other.replace(r#""namespace""#, r#""aliases":["shop.User"],"namespace""#),
                USER_V1
            ),
            Ok(())
        );
    }

    #[test]
    fn a_schema_may_use_types_defined_in_the_schemas_it_refers_to() {
        let money = s(r#"{"type":"record","name":"Money","namespace":"shop","fields":[
            {"name":"amount","type":"long"},{"name":"currency","type":"string"}]}"#);
        let order = r#"{"type":"record","name":"Order","namespace":"shop","fields":[
            {"name":"id","type":"long"},{"name":"total","type":"shop.Money"}]}"#;
        let json: Value = serde_json::from_str(order).unwrap();
        // On its own the type is unknown; with what it refers to, it is read - and compared through it.
        assert!(Schema::from_json(&json).unwrap_err().0.contains("shop.Money"));
        let with = Schema::from_json_with(&json, &money.named).unwrap();
        assert!(with.named.contains_key("shop.Money") && with.named.contains_key("shop.Order"));
        let narrower = s(r#"{"type":"record","name":"Money","namespace":"shop","fields":[
            {"name":"amount","type":"int"},{"name":"currency","type":"string"}]}"#);
        let old = Schema::from_json_with(&json, &narrower.named).unwrap();
        assert_eq!(with.can_read(&old), Ok(()), "long reads int");
        assert!(old.can_read(&with).unwrap_err().contains("field amount"));
        // A schema may not define again what it refers to.
        let again: Value =
            serde_json::from_str(r#"{"type":"record","name":"Money","namespace":"shop","fields":[]}"#)
                .unwrap();
        assert!(
            Schema::from_json_with(&again, &money.named)
                .unwrap_err()
                .0
                .contains("defined twice")
        );
    }

    #[test]
    fn a_schema_that_refers_to_itself_is_compared_without_going_round_for_ever() {
        let node = |value: &str| {
            format!(
                r#"{{"type":"record","name":"Node","fields":[
                {{"name":"value","type":"{value}"}},{{"name":"next","type":["null","Node"]}}]}}"#
            )
        };
        assert_eq!(reads(&node("long"), &node("int")), Ok(()));
        assert!(
            reads(&node("int"), &node("long"))
                .unwrap_err()
                .contains("field value")
        );
    }
}
