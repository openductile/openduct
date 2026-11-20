// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Protobuf schemas (`.proto` files, proto2 and proto3), as far as a registry needs them: read, checked, and
//! compared - can messages written with one be read with another?
//!
//! Protobuf reads by field number, and skips numbers it does not know, so most changes are safe. What is not,
//! and so refused, is what Confluent's registry refuses:
//!
//! - a message removed;
//! - a field (the same number) that changes kind - scalar, message or enum, map - or changes to a scalar that is
//!   not written the same way on the wire. These are written alike and may be swapped: `int32`, `uint32`,
//!   `int64`, `uint64`, `bool` and enums; `sint32` and `sint64`; `string` and `bytes`; `fixed32` and `sfixed32`;
//!   `fixed64` and `sfixed64`;
//! - a field whose message or enum type changes to another;
//! - a numeric field that changes between single and `repeated`;
//! - a `required` field added or removed;
//! - a field taken out of a `oneof`, or several existing fields moved into one.
//!
//! Adding and removing fields, renaming them, and changing an enum's values are all taken.
//!
//! Imports of the well-known types (`google/protobuf/...`) are accepted; an import of another schema is refused,
//! since references between subjects are not taken yet. Services, extensions and options are read past. Nothing
//! here panics on any input.

use std::collections::BTreeMap;
use std::fmt;

const MAX_DEPTH: usize = 32;

/// Why a text is not a Protobuf schema this registry takes.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Label {
    Single,
    Required,
    Repeated,
}

/// What a field holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Scalar(&'static str),
    /// A message or enum, by full name.
    Named(String),
    Map,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub label: Label,
    pub kind: Kind,
    /// The `oneof` it is part of.
    pub oneof: Option<String>,
}

/// A schema read and checked: its messages (fields by number) and the names of its enums, by full name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub messages: BTreeMap<String, BTreeMap<i32, Field>>,
    pub enums: Vec<String>,
    /// Enums of the schemas it imports, which its fields may hold.
    pub imported_enums: Vec<String>,
    /// The schema's words in order, comments and spacing gone: the same schema laid out two ways is the same.
    pub canonical: String,
}

const SCALARS: [&str; 15] = [
    "double", "float", "int32", "int64", "uint32", "uint64", "sint32", "sint64", "fixed32", "fixed64",
    "sfixed32", "sfixed64", "bool", "string", "bytes",
];

/// Scalars written the same way on the wire, by group.
fn wire_group(scalar: &str) -> u8 {
    match scalar {
        "int32" | "uint32" | "int64" | "uint64" | "bool" => 1,
        "sint32" | "sint64" => 2,
        "string" | "bytes" => 3,
        "fixed32" | "sfixed32" => 4,
        "fixed64" | "sfixed64" => 5,
        "float" => 6,
        _ => 7,
    }
}

fn numeric(scalar: &str) -> bool {
    !matches!(scalar, "string" | "bytes")
}

/// The words and marks of a `.proto` file, comments left out.
fn tokens(text: &str) -> Result<Vec<String>, Invalid> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut at = 0;
    while let Some(&c) = chars.get(at) {
        let next = chars.get(at + 1).copied();
        if c.is_whitespace() {
            at += 1;
        } else if c == '/' && next == Some('/') {
            while chars.get(at).is_some_and(|c| *c != '\n') {
                at += 1;
            }
        } else if c == '/' && next == Some('*') {
            at += 2;
            loop {
                match (chars.get(at), chars.get(at + 1)) {
                    (Some('*'), Some('/')) => break,
                    (Some(_), _) => at += 1,
                    (None, _) => return invalid("a comment is never closed"),
                }
            }
            at += 2;
        } else if c == '"' || c == '\'' {
            let mut word = String::from('"');
            at += 1;
            loop {
                match chars.get(at) {
                    Some('\\') => {
                        word.push('\\');
                        word.extend(chars.get(at + 1));
                        at += 2;
                    }
                    Some(end) if *end == c => break,
                    Some(other) => {
                        word.push(*other);
                        at += 1;
                    }
                    None => return invalid("a quoted text is never closed"),
                }
            }
            at += 1;
            word.push('"');
            out.push(word);
        } else if c.is_alphanumeric() || c == '_' || c == '.' || c == '-' || c == '+' {
            let start = at;
            while chars
                .get(at)
                .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '.' | '-' | '+'))
            {
                at += 1;
            }
            out.push(chars.get(start..at).unwrap_or_default().iter().collect());
        } else if "{}[]()<>=;,:".contains(c) {
            out.push(c.to_string());
            at += 1;
        } else {
            return invalid(format!("{c:?} is not part of a .proto file"));
        }
    }
    Ok(out)
}

fn name_ok(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A field as written, its type not yet resolved to a full name.
struct Raw {
    number: i32,
    name: String,
    label: Label,
    of: String,
    map: bool,
    oneof: Option<String>,
    scope: String,
}

struct Parser {
    words: Vec<String>,
    at: usize,
    package: String,
    messages: BTreeMap<String, Vec<Raw>>,
    enums: Vec<String>,
    /// The files this one may import, and the full names of the messages and enums they give it.
    imports: Vec<String>,
    imported: Vec<String>,
}

impl Parser {
    fn peek(&self) -> Option<&str> {
        self.words.get(self.at).map(String::as_str)
    }

    fn next(&mut self) -> Result<String, Invalid> {
        let word = self.words.get(self.at).cloned();
        self.at += 1;
        word.ok_or(Invalid("the file ends in the middle of something".into()))
    }

    fn expect(&mut self, what: &str) -> Result<(), Invalid> {
        let got = self.next()?;
        if got == what {
            Ok(())
        } else {
            invalid(format!("{what:?} was expected, not {got:?}"))
        }
    }

    /// Reads past a statement to its `;`, or past a block to its `}`.
    fn skip(&mut self) -> Result<(), Invalid> {
        let mut depth = 0usize;
        loop {
            match self.next()?.as_str() {
                "{" => depth += 1,
                "}" if depth <= 1 => return Ok(()),
                "}" => depth -= 1,
                ";" if depth == 0 => return Ok(()),
                _ => {}
            }
        }
    }

    /// Reads past `[ ... ]` options, when there are any.
    fn options(&mut self) -> Result<(), Invalid> {
        if self.peek() == Some("[") {
            while self.next()? != "]" {}
        }
        Ok(())
    }

    fn number(&mut self) -> Result<i32, Invalid> {
        let word = self.next()?;
        let parsed = match word.strip_prefix("0x").or_else(|| word.strip_prefix("0X")) {
            Some(hex) => i32::from_str_radix(hex, 16).ok(),
            None => word.parse().ok(),
        };
        parsed.ok_or(Invalid(format!("{word:?} is not a number")))
    }

    fn file(&mut self) -> Result<(), Invalid> {
        while let Some(word) = self.peek().map(str::to_owned) {
            self.at += 1;
            match word.as_str() {
                ";" => {}
                "syntax" => {
                    self.expect("=")?;
                    let syntax = self.next()?;
                    if syntax != "\"proto2\"" && syntax != "\"proto3\"" {
                        return invalid(format!("syntax {syntax} is not proto2 or proto3"));
                    }
                    self.expect(";")?;
                }
                "package" => {
                    self.package = self.next()?;
                    if !self.package.split('.').all(name_ok) {
                        return invalid(format!("{:?} is not a package name", self.package));
                    }
                    self.expect(";")?;
                }
                "import" => {
                    let mut file = self.next()?;
                    if file == "public" || file == "weak" {
                        file = self.next()?;
                    }
                    let name = file.trim_matches('"');
                    if !name.starts_with("google/protobuf/") && !self.imports.iter().any(|i| i == name) {
                        return invalid(format!(
                            "import {file}: it is not among the schema's references (the well-known types under \
                             google/protobuf need none)"
                        ));
                    }
                    self.expect(";")?;
                }
                "option" | "service" | "extend" => self.skip()?,
                "message" => {
                    let scope = self.package.clone();
                    self.message(&scope, 0)?;
                }
                "enum" => {
                    let scope = self.package.clone();
                    self.enumeration(&scope)?;
                }
                other => return invalid(format!("{other:?} does not start anything a .proto file has")),
            }
        }
        Ok(())
    }

    fn named(&mut self, scope: &str) -> Result<String, Invalid> {
        let name = self.next()?;
        if !name_ok(&name) {
            return invalid(format!("{name:?} is not a name"));
        }
        Ok(if scope.is_empty() {
            name
        } else {
            format!("{scope}.{name}")
        })
    }

    fn enumeration(&mut self, scope: &str) -> Result<(), Invalid> {
        let full = self.named(scope)?;
        if self.enums.contains(&full) || self.messages.contains_key(&full) {
            return invalid(format!("{full} is defined twice"));
        }
        self.expect("{")?;
        loop {
            let word = self.next()?;
            match word.as_str() {
                "}" => break,
                ";" => {}
                "option" | "reserved" => self.skip()?,
                value if name_ok(value) => {
                    self.expect("=")?;
                    self.number()?;
                    self.options()?;
                    self.expect(";")?;
                }
                other => return invalid(format!("{other:?} is not an enum value in {full}")),
            }
        }
        self.enums.push(full);
        Ok(())
    }

    fn message(&mut self, scope: &str, depth: usize) -> Result<(), Invalid> {
        if depth > MAX_DEPTH {
            return invalid(format!("messages nest deeper than {MAX_DEPTH} levels"));
        }
        let full = self.named(scope)?;
        if self.enums.contains(&full) || self.messages.contains_key(&full) {
            return invalid(format!("{full} is defined twice"));
        }
        self.messages.insert(full.clone(), Vec::new());
        self.expect("{")?;
        let mut oneof: Option<String> = None;
        loop {
            let word = self.next()?;
            match word.as_str() {
                "}" if oneof.is_some() => oneof = None,
                "}" => break,
                ";" => {}
                "option" | "reserved" | "extensions" | "extend" => self.skip()?,
                "message" => self.message(&full, depth + 1)?,
                "enum" => self.enumeration(&full)?,
                "group" => return invalid(format!("{full} has a group, which is not taken")),
                "oneof" if oneof.is_none() => {
                    let name = self.next()?;
                    self.expect("{")?;
                    oneof = Some(name);
                }
                _ => {
                    let field = self.field(word, oneof.clone(), &full)?;
                    let fields = self.messages.entry(full.clone()).or_default();
                    if fields.iter().any(|f| f.number == field.number) {
                        return invalid(format!("{full} uses the field number {} twice", field.number));
                    }
                    if fields.iter().any(|f| f.name == field.name) {
                        return invalid(format!("{full} has two fields named {}", field.name));
                    }
                    fields.push(field);
                }
            }
        }
        Ok(())
    }

    fn field(&mut self, first: String, oneof: Option<String>, scope: &str) -> Result<Raw, Invalid> {
        let (label, mut of) = match first.as_str() {
            "optional" => (Label::Single, self.next()?),
            "required" => (Label::Required, self.next()?),
            "repeated" => (Label::Repeated, self.next()?),
            _ => (Label::Single, first),
        };
        let mut map = false;
        if of == "map" && self.peek() == Some("<") {
            self.at += 1;
            let key = self.next()?;
            self.expect(",")?;
            of = self.next()?;
            self.expect(">")?;
            if !SCALARS.contains(&key.as_str()) || matches!(key.as_str(), "double" | "float" | "bytes") {
                return invalid(format!("{key} cannot be the key of a map"));
            }
            map = true;
        }
        let name = self.next()?;
        if !name_ok(&name) {
            return invalid(format!("{name:?} is not a field name in {scope}"));
        }
        self.expect("=")?;
        let number = self.number()?;
        if !(1..=536_870_911).contains(&number) || (19_000..=19_999).contains(&number) {
            return invalid(format!("{number} is not a field number {scope}.{name} may have"));
        }
        self.options()?;
        self.expect(";")?;
        Ok(Raw {
            number,
            name,
            label,
            of,
            map,
            oneof,
            scope: scope.to_owned(),
        })
    }

    /// The full name a type written as `of` inside `scope` stands for: looked for in the scope, then each one
    /// around it.
    fn resolve(&self, of: &str, scope: &str) -> Result<Kind, Invalid> {
        if let Some(scalar) = SCALARS.iter().find(|s| **s == of) {
            return Ok(Kind::Scalar(scalar));
        }
        let known = |full: &str| {
            self.messages.contains_key(full)
                || self.enums.iter().any(|e| e == full)
                || self.imported.iter().any(|i| i == full)
        };
        if let Some(absolute) = of.strip_prefix('.') {
            if known(absolute) || absolute.starts_with("google.protobuf.") {
                return Ok(Kind::Named(absolute.to_owned()));
            }
            return invalid(format!("{of} is not a type this schema defines"));
        }
        if of.starts_with("google.protobuf.") {
            return Ok(Kind::Named(of.to_owned()));
        }
        let mut scope = scope;
        loop {
            let candidate = if scope.is_empty() {
                of.to_owned()
            } else {
                format!("{scope}.{of}")
            };
            if known(&candidate) {
                return Ok(Kind::Named(candidate));
            }
            if scope.is_empty() {
                return invalid(format!("{of} is not a type this schema defines"));
            }
            scope = scope.rsplit_once('.').map_or("", |(outer, _)| outer);
        }
    }
}

impl Schema {
    /// Reads a schema from the text of a `.proto` file.
    pub fn parse(text: &str) -> Result<Self, Invalid> {
        Self::parse_with(text, &[])
    }

    /// As [`Schema::parse`], for a file that imports others - each given here by the name the `import` uses: its
    /// fields may hold their messages and enums.
    pub fn parse_with(text: &str, imports: &[(String, &Schema)]) -> Result<Self, Invalid> {
        let words = tokens(text)?;
        if words.is_empty() {
            return invalid("the schema is empty");
        }
        let canonical = words.join(" ");
        let mut parser = Parser {
            words,
            at: 0,
            package: String::new(),
            messages: BTreeMap::new(),
            enums: Vec::new(),
            imports: imports.iter().map(|(name, _)| name.clone()).collect(),
            imported: imports
                .iter()
                .flat_map(|(_, schema)| schema.messages.keys().chain(&schema.enums))
                .cloned()
                .collect(),
        };
        parser.file()?;
        if parser.messages.is_empty() && parser.enums.is_empty() {
            return invalid("the schema defines no message and no enum");
        }
        let mut messages = BTreeMap::new();
        for (full, fields) in &parser.messages {
            let mut by_number = BTreeMap::new();
            for raw in fields {
                let kind = match parser.resolve(&raw.of, &raw.scope)? {
                    _ if raw.map => Kind::Map,
                    kind => kind,
                };
                by_number.insert(
                    raw.number,
                    Field {
                        name: raw.name.clone(),
                        label: raw.label,
                        kind,
                        oneof: raw.oneof.clone(),
                    },
                );
            }
            messages.insert(full.clone(), by_number);
        }
        Ok(Self {
            messages,
            enums: parser.enums,
            imported_enums: imports
                .iter()
                .flat_map(|(_, schema)| schema.enums.iter().chain(&schema.imported_enums))
                .cloned()
                .collect(),
            canonical,
        })
    }

    /// Whether messages written with `writer` can be read with this schema; if not, the first reason why.
    pub fn can_read(&self, writer: &Schema) -> Result<(), String> {
        let is_enum = |schema: &Schema, name: &str| {
            schema
                .enums
                .iter()
                .chain(&schema.imported_enums)
                .any(|e| e == name)
        };
        for (message, old_fields) in &writer.messages {
            let Some(new_fields) = self.messages.get(message) else {
                return Err(format!("the message {message} was removed"));
            };
            for (number, old) in old_fields {
                let at = format!("{message}, field {number} ({})", old.name);
                let Some(new) = new_fields.get(number) else {
                    if old.label == Label::Required {
                        return Err(format!("{at}: a required field was removed"));
                    }
                    continue;
                };
                // An enum is written as an int32 is.
                let as_scalar = |schema: &Schema, kind: &Kind| match kind {
                    Kind::Named(name) if is_enum(schema, name) => Some("int32"),
                    Kind::Scalar(s) => Some(*s),
                    _ => None,
                };
                match (as_scalar(writer, &old.kind), as_scalar(self, &new.kind)) {
                    (Some(was), Some(is)) => {
                        if wire_group(was) != wire_group(is) {
                            return Err(format!("{at}: {was} is not written as {is} is"));
                        }
                        if (old.label == Label::Repeated) != (new.label == Label::Repeated) && numeric(was) {
                            return Err(format!("{at}: a number changed between single and repeated"));
                        }
                    }
                    (None, None) => match (&old.kind, &new.kind) {
                        (Kind::Named(was), Kind::Named(is)) if was != is => {
                            return Err(format!("{at}: its type changed from {was} to {is}"));
                        }
                        (Kind::Map, Kind::Named(_)) | (Kind::Named(_), Kind::Map) => {
                            return Err(format!("{at}: changed between a map and a message"));
                        }
                        _ => {}
                    },
                    _ => return Err(format!("{at}: changed between a scalar and a message or map")),
                }
                if (old.label == Label::Required) != (new.label == Label::Required) {
                    return Err(format!("{at}: required was added or removed"));
                }
                if old.oneof.is_some() && new.oneof.is_none() {
                    return Err(format!(
                        "{at}: taken out of the oneof {}",
                        old.oneof.as_deref().unwrap_or_default()
                    ));
                }
            }
            for (number, new) in new_fields {
                if new.label == Label::Required && !old_fields.contains_key(number) {
                    return Err(format!(
                        "{message}, field {number} ({}): a required field was added",
                        new.name
                    ));
                }
            }
            // Fields that were on their own and now share one oneof: only one of them can be set from now on.
            let mut moved: BTreeMap<&str, usize> = BTreeMap::new();
            for (number, new) in new_fields {
                if let (Some(group), Some(old)) = (new.oneof.as_deref(), old_fields.get(number))
                    && old.oneof.is_none()
                {
                    *moved.entry(group).or_default() += 1;
                }
            }
            if let Some((group, _)) = moved.iter().find(|(_, count)| **count > 1) {
                return Err(format!(
                    "{message}: several existing fields were moved into the oneof {group}"
                ));
            }
        }
        Ok(())
    }
}

// --- a schema sent as a descriptor --------------------------------------------------------------------------

/// One field of a protobuf message on the wire.
enum Wire<'a> {
    Number(u64),
    Bytes(&'a [u8]),
}

/// The fields of a protobuf message, in order: (field number, value). `None` when the bytes are not one.
fn fields(mut bytes: &[u8]) -> Option<Vec<(u32, Wire<'_>)>> {
    fn varint(bytes: &mut &[u8]) -> Option<u64> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let (first, rest) = bytes.split_first()?;
            *bytes = rest;
            out |= u64::from(first & 0x7f) << shift;
            if first & 0x80 == 0 {
                return Some(out);
            }
        }
        None
    }
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let tag = varint(&mut bytes)?;
        let number = u32::try_from(tag >> 3).ok().filter(|n| *n > 0)?;
        match tag & 7 {
            0 => out.push((number, Wire::Number(varint(&mut bytes)?))),
            1 => bytes = bytes.get(8..)?,
            2 => {
                let len = usize::try_from(varint(&mut bytes)?).ok()?;
                let (value, rest) = bytes.split_at_checked(len)?;
                bytes = rest;
                out.push((number, Wire::Bytes(value)));
            }
            5 => bytes = bytes.get(4..)?,
            _ => return None,
        }
    }
    Some(out)
}

fn text(value: &Wire<'_>) -> Option<String> {
    match value {
        Wire::Bytes(bytes) => String::from_utf8(bytes.to_vec()).ok(),
        Wire::Number(_) => None,
    }
}

fn number(value: &Wire<'_>) -> Option<i64> {
    match value {
        Wire::Number(n) => i64::try_from(*n).ok().or(Some(*n as i64)),
        Wire::Bytes(_) => None,
    }
}

/// One field of a message, as a descriptor holds it.
#[derive(Default)]
struct DescribedField {
    name: String,
    number: i64,
    label: i64,
    kind: i64,
    type_name: String,
    oneof: Option<usize>,
    proto3_optional: bool,
}

/// A message of a descriptor, written as `.proto` text into `out`.
fn print_message(bytes: &[u8], proto3: bool, indent: usize, depth: usize, out: &mut String) -> Option<()> {
    if depth > MAX_DEPTH {
        return None;
    }
    let pad = "  ".repeat(indent);
    let (mut name, mut described, mut nested, mut enums, mut oneofs) =
        (String::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (number_, value) in fields(bytes)? {
        match (number_, &value) {
            (1, _) => name = text(&value)?,
            (2, Wire::Bytes(field)) => {
                let mut f = DescribedField::default();
                for (n, v) in fields(field)? {
                    match n {
                        1 => f.name = text(&v)?,
                        3 => f.number = number(&v)?,
                        4 => f.label = number(&v)?,
                        5 => f.kind = number(&v)?,
                        6 => f.type_name = text(&v)?,
                        9 => f.oneof = usize::try_from(number(&v)?).ok(),
                        17 => f.proto3_optional = number(&v)? != 0,
                        _ => {}
                    }
                }
                described.push(f);
            }
            (3, Wire::Bytes(message)) => nested.push(*message),
            (4, Wire::Bytes(e)) => enums.push(*e),
            (8, Wire::Bytes(oneof)) => {
                oneofs.push(
                    fields(oneof)?
                        .iter()
                        .find(|(n, _)| *n == 1)
                        .and_then(|(_, v)| text(v))?,
                );
            }
            _ => {}
        }
    }
    // A nested type that is a map's entry: its name, and its key and value types.
    let mut entries: Vec<(String, String, String)> = Vec::new();
    let mut plain = Vec::new();
    for message in nested {
        let parts = fields(message)?;
        let is_entry = parts.iter().any(|(n, v)| {
            *n == 7
                && matches!(v, Wire::Bytes(options) if fields(options).is_some_and(|o| {
                    o.iter().any(|(n, v)| *n == 7 && matches!(v, Wire::Number(1)))
                }))
        });
        if !is_entry {
            plain.push(message);
            continue;
        }
        let entry = parts.iter().find(|(n, _)| *n == 1).and_then(|(_, v)| text(v))?;
        let mut key_value = [String::new(), String::new()];
        for (n, v) in &parts {
            if let (2, Wire::Bytes(field)) = (*n, v) {
                let mut f = DescribedField::default();
                for (n, v) in fields(field)? {
                    match n {
                        3 => f.number = number(&v)?,
                        5 => f.kind = number(&v)?,
                        6 => f.type_name = text(&v)?,
                        _ => {}
                    }
                }
                let slot = usize::try_from(f.number - 1)
                    .ok()
                    .and_then(|i| key_value.get_mut(i))?;
                *slot = type_of(&f)?;
            }
        }
        let [key, value] = key_value;
        entries.push((entry, key, value));
    }
    out.push_str(&format!("{pad}message {name} {{\n"));
    let line = |f: &DescribedField, pad: &str, in_oneof: bool| -> Option<String> {
        if let Some((_, key, value)) = entries
            .iter()
            .find(|(entry, _, _)| f.type_name.rsplit('.').next() == Some(entry.as_str()) && f.label == 3)
        {
            return Some(format!("{pad}  map<{key}, {value}> {} = {};\n", f.name, f.number));
        }
        let label = match f.label {
            _ if in_oneof => "",
            3 => "repeated ",
            2 => "required ",
            _ if !proto3 || f.proto3_optional => "optional ",
            _ => "",
        };
        Some(format!(
            "{pad}  {label}{} {} = {};\n",
            type_of(f)?,
            f.name,
            f.number
        ))
    };
    for f in described
        .iter()
        .filter(|f| f.oneof.is_none() || f.proto3_optional)
    {
        out.push_str(&line(f, &pad, false)?);
    }
    for (i, oneof) in oneofs.iter().enumerate() {
        let members: Vec<&DescribedField> = described
            .iter()
            .filter(|f| f.oneof == Some(i) && !f.proto3_optional)
            .collect();
        if members.is_empty() {
            continue;
        }
        out.push_str(&format!("{pad}  oneof {oneof} {{\n"));
        for f in members {
            out.push_str(&line(f, &format!("{pad}  "), true)?);
        }
        out.push_str(&format!("{pad}  }}\n"));
    }
    for message in plain {
        print_message(message, proto3, indent + 1, depth + 1, out)?;
    }
    for e in enums {
        print_enum(e, indent + 1, out)?;
    }
    out.push_str(&format!("{pad}}}\n"));
    Some(())
}

fn type_of(f: &DescribedField) -> Option<String> {
    Some(
        match f.kind {
            1 => "double",
            2 => "float",
            3 => "int64",
            4 => "uint64",
            5 => "int32",
            6 => "fixed64",
            7 => "fixed32",
            8 => "bool",
            9 => "string",
            12 => "bytes",
            13 => "uint32",
            15 => "sfixed32",
            16 => "sfixed64",
            17 => "sint32",
            18 => "sint64",
            11 | 14 if !f.type_name.is_empty() => return Some(f.type_name.clone()),
            _ => return None,
        }
        .to_owned(),
    )
}

fn print_enum(bytes: &[u8], indent: usize, out: &mut String) -> Option<()> {
    let pad = "  ".repeat(indent);
    let parts = fields(bytes)?;
    let name = parts.iter().find(|(n, _)| *n == 1).and_then(|(_, v)| text(v))?;
    out.push_str(&format!("{pad}enum {name} {{\n"));
    for (n, v) in &parts {
        if let (2, Wire::Bytes(value)) = (*n, v) {
            let value = fields(value)?;
            let name = value.iter().find(|(n, _)| *n == 1).and_then(|(_, v)| text(v))?;
            let at = value
                .iter()
                .find(|(n, _)| *n == 2)
                .and_then(|(_, v)| number(v))
                .unwrap_or(0);
            out.push_str(&format!("{pad}  {name} = {at};\n"));
        }
    }
    out.push_str(&format!("{pad}}}\n"));
    Some(())
}

fn base64(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let (mut bits, mut held) = (0u32, 0u32);
    for c in text.bytes().filter(|c| *c != b'=') {
        let value = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        };
        held = (held << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(u8::try_from((held >> bits) & 0xff).ok()?);
        }
    }
    Some(out)
}

/// A schema as a client may send it: the text of a `.proto` file, or - as Confluent's serializers send it - a
/// file descriptor (`FileDescriptorProto`) in base64. Either way, the text of the `.proto` file: what the
/// registry keeps and gives back. `None` when it is not a descriptor, and so is taken as text.
pub fn text_of_descriptor(schema: &str) -> Option<String> {
    let schema = schema.trim();
    // A .proto file has spaces and punctuation that base64 does not.
    if schema.is_empty()
        || schema
            .bytes()
            .any(|c| !(c.is_ascii_alphanumeric() || matches!(c, b'+' | b'/' | b'=')))
    {
        return None;
    }
    let bytes = base64(schema)?;
    let parts = fields(&bytes)?;
    let syntax = parts.iter().find(|(n, _)| *n == 12).and_then(|(_, v)| text(v));
    let proto3 = syntax.as_deref() == Some("proto3");
    let mut out = format!("syntax = \"{}\";\n", if proto3 { "proto3" } else { "proto2" });
    if let Some(package) = parts.iter().find(|(n, _)| *n == 2).and_then(|(_, v)| text(v))
        && !package.is_empty()
    {
        out.push_str(&format!("package {package};\n"));
    }
    let mut defined = false;
    for (n, v) in &parts {
        match (*n, v) {
            (3, _) => out.push_str(&format!("import \"{}\";\n", text(v)?)),
            (4, Wire::Bytes(message)) => {
                defined = true;
                print_message(message, proto3, 0, 0, &mut out)?;
            }
            (5, Wire::Bytes(e)) => {
                defined = true;
                print_enum(e, 0, &mut out)?;
            }
            _ => {}
        }
    }
    defined.then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER: &str = r#"
        syntax = "proto3";
        package shop;
        import "google/protobuf/timestamp.proto";
        // An order.
        message Order {
            int64 id = 1;
            string note = 2; /* free text */
            repeated Line lines = 3;
            Status status = 4;
            google.protobuf.Timestamp placed = 5;
            map<string, string> tags = 6;
            oneof payment { string card = 7; string voucher = 8; }
            message Line { string sku = 1; int32 quantity = 2 [deprecated = true]; }
            enum Status { UNKNOWN = 0; PAID = 1; }
            reserved 20, 21;
        }
        service Orders { rpc Get (Order) returns (Order); }
    "#;

    fn s(text: &str) -> Schema {
        Schema::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    /// `ORDER` with one of its lines replaced.
    fn changed(from: &str, to: &str) -> String {
        assert!(ORDER.contains(from), "{from}");
        ORDER.replace(from, to)
    }

    fn reads(new: &str, old: &str) -> Result<(), String> {
        s(new).can_read(&s(old))
    }

    #[test]
    fn a_proto_file_is_read_with_its_nested_types_resolved() {
        let order = s(ORDER);
        assert_eq!(order.enums, vec!["shop.Order.Status".to_owned()]);
        let fields = &order.messages["shop.Order"];
        assert_eq!(fields[&1].kind, Kind::Scalar("int64"));
        assert_eq!(
            (fields[&3].label, &fields[&3].kind),
            (Label::Repeated, &Kind::Named("shop.Order.Line".into()))
        );
        assert_eq!(fields[&4].kind, Kind::Named("shop.Order.Status".into()));
        assert_eq!(fields[&5].kind, Kind::Named("google.protobuf.Timestamp".into()));
        assert_eq!(fields[&6].kind, Kind::Map);
        assert_eq!(fields[&7].oneof.as_deref(), Some("payment"));
        assert_eq!(order.messages["shop.Order.Line"].len(), 2);
        // Laid out differently, with other comments: the same schema.
        let compact = ORDER
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .replace("// An order.", "");
        assert_eq!(
            s(&compact.replace("/* free text */", "")).canonical,
            order.canonical
        );
    }

    #[test]
    fn what_is_not_a_proto_file_is_refused_with_the_reason() {
        for (bad, why) in [
            ("", "empty"),
            ("syntax = \"proto3\";", "no message"),
            ("syntax = \"proto4\"; message A {}", "not proto2 or proto3"),
            ("message A { int32 a = 1; int32 b = 1; }", "number 1 twice"),
            ("message A { int32 a = 1; string a = 2; }", "two fields named a"),
            ("message A { Missing m = 1; }", "not a type this schema defines"),
            ("message A { int32 a = 0; }", "not a field number"),
            ("message A { int32 a = 19000; }", "not a field number"),
            ("message A { map<float, string> m = 1; }", "cannot be the key"),
            ("message A { int32 a = 1 }", "\";\" was expected"),
            ("message A {", "ends in the middle"),
            ("message A {} message A {}", "defined twice"),
            (
                "import \"other.proto\"; message A {}",
                "not among the schema's references",
            ),
            ("message A { /* never closed", "never closed"),
            ("message A { string s = 1; } §", "not part of a .proto file"),
            ("banana A {}", "does not start anything"),
        ] {
            let e = Schema::parse(bad).unwrap_err();
            assert!(e.0.contains(why), "{bad}: {e}");
        }
        let deep = format!("{} int32 a = 1; {}", "message M {".repeat(40), "}".repeat(40));
        assert!(Schema::parse(&deep).unwrap_err().0.contains("deeper"));
    }

    #[test]
    fn fields_may_come_go_and_be_renamed() {
        assert_eq!(
            reads(
                &changed("string note = 2;", "string note = 2; double total = 9;"),
                ORDER
            ),
            Ok(())
        );
        assert_eq!(reads(&changed("string note = 2;", ""), ORDER), Ok(()));
        assert_eq!(
            reads(&changed("string note = 2;", "string comment = 2;"), ORDER),
            Ok(())
        );
        assert_eq!(
            reads(&changed("PAID = 1;", "PAID = 1; SHIPPED = 2;"), ORDER),
            Ok(())
        );
        assert_eq!(reads(ORDER, ORDER), Ok(()));
    }

    #[test]
    fn what_changes_how_a_field_is_written_is_refused() {
        for (from, to, why) in [
            (
                "int64 id = 1;",
                "string id = 1;",
                "int64 is not written as string is",
            ),
            ("int64 id = 1;", "sint64 id = 1;", "not written as"),
            ("int64 id = 1;", "repeated int64 id = 1;", "single and repeated"),
            ("int64 id = 1;", "Line id = 1;", "scalar and a message"),
            (
                "repeated Line lines = 3;",
                "repeated Status lines = 3;",
                "scalar and a message",
            ),
            (
                "map<string, string> tags = 6;",
                "Line tags = 6;",
                "a map and a message",
            ),
            (
                "oneof payment { string card = 7; string voucher = 8; }",
                "string card = 7; string voucher = 8;",
                "taken out of the oneof",
            ),
            (
                "string note = 2;",
                "oneof extra { string note = 2; int64 other = 1; }\n",
                "does not start",
            ),
        ] {
            let new = changed(from, to);
            match Schema::parse(&new) {
                Ok(new) => {
                    let e = new.can_read(&s(ORDER)).expect_err(&format!("{from} -> {to}"));
                    assert!(e.contains(why), "{from} -> {to}: {e}");
                }
                // The last one is not a valid file at all (two fields numbered 1).
                Err(e) => assert!(to.contains("oneof extra"), "{to}: {e}"),
            }
        }
        // Written alike: taken.
        assert_eq!(reads(&changed("int64 id = 1;", "uint32 id = 1;"), ORDER), Ok(()));
        assert_eq!(
            reads(&changed("string note = 2;", "bytes note = 2;"), ORDER),
            Ok(())
        );
        assert_eq!(
            reads(&changed("Status status = 4;", "int32 status = 4;"), ORDER),
            Ok(())
        );
        assert_eq!(
            reads(&changed("string note = 2;", "repeated string note = 2;"), ORDER),
            Ok(())
        );
        // A message removed; a type swapped for another message.
        let without_line = changed(
            "message Line { string sku = 1; int32 quantity = 2 [deprecated = true]; }",
            "",
        )
        .replace("repeated Line lines = 3;", "");
        assert!(
            reads(&without_line, ORDER)
                .unwrap_err()
                .contains("shop.Order.Line was removed")
        );
        let other = changed("repeated Line lines = 3;", "repeated Other lines = 3;")
            .replace("reserved 20, 21;", "message Other { string sku = 1; }");
        assert!(
            reads(&other, ORDER)
                .unwrap_err()
                .contains("type changed from shop.Order.Line to shop.Order.Other")
        );
        // Existing fields gathered into a oneof: one is fine, two are not.
        let one = changed("string note = 2;", "oneof extra { string note = 2; }");
        assert_eq!(reads(&one, ORDER), Ok(()));
        let two = changed(
            "int64 id = 1;\n            string note = 2;",
            "oneof extra { int64 id = 1; string note = 2; }",
        );
        assert!(
            reads(&two, ORDER)
                .unwrap_err()
                .contains("moved into the oneof extra")
        );
    }

    #[test]
    fn a_file_may_import_the_schemas_it_refers_to() {
        let common = s(
            "syntax = \"proto3\"; package common; message Money { int64 amount = 1; } enum Unit { EACH = 0; }",
        );
        let order = "syntax = \"proto3\"; package shop; import \"common.proto\";
            message Order { int64 id = 1; common.Money total = 2; common.Unit unit = 3; }";
        assert!(Schema::parse(order).unwrap_err().0.contains("references"));
        let read = Schema::parse_with(order, &[("common.proto".to_owned(), &common)]).unwrap();
        assert_eq!(
            read.messages["shop.Order"][&2].kind,
            Kind::Named("common.Money".into())
        );
        assert_eq!(read.messages.len(), 1, "its own messages only");
        // An imported enum is written as a number is: the field may become an int32, not a string.
        let as_int = order.replace("common.Unit unit = 3;", "int32 unit = 3;");
        let as_text = order.replace("common.Unit unit = 3;", "string unit = 3;");
        let imports = [("common.proto".to_owned(), &common)];
        assert_eq!(
            Schema::parse_with(&as_int, &imports).unwrap().can_read(&read),
            Ok(())
        );
        assert!(
            Schema::parse_with(&as_text, &imports)
                .unwrap()
                .can_read(&read)
                .is_err()
        );
        // A type the import does not have is still unknown.
        let wrong = order.replace("common.Money", "common.Cash");
        assert!(
            Schema::parse_with(&wrong, &imports)
                .unwrap_err()
                .0
                .contains("common.Cash")
        );
    }

    /// A protobuf message from its fields, for building a descriptor by hand.
    fn message(parts: &[(u32, &[u8], bool)]) -> Vec<u8> {
        let mut out = Vec::new();
        for (number, value, is_number) in parts {
            if *is_number {
                out.push((number << 3) as u8);
                out.extend_from_slice(value);
            } else {
                out.push(((number << 3) | 2) as u8);
                // The length, as a varint: seven bits at a time.
                let mut len = value.len();
                while len >= 0x80 {
                    out.push((len & 0x7f) as u8 | 0x80);
                    len >>= 7;
                }
                out.push(len as u8);
                out.extend_from_slice(value);
            }
        }
        out
    }

    fn base64_of(bytes: &[u8]) -> String {
        const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let n = u32::from(chunk[0]) << 16
                | u32::from(*chunk.get(1).unwrap_or(&0)) << 8
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for i in 0..4 {
                out.push(if i <= chunk.len() {
                    TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char
                } else {
                    '='
                });
            }
        }
        out
    }

    #[test]
    fn a_schema_sent_as_a_descriptor_is_read_as_the_proto_file_it_describes() {
        // What Confluent's Python serializer sent for `message View { string page = 1; int64 at = 2; }`.
        let sent = "ChN2aWV3XzA2ZDNlY2M2LnByb3RvEg1wcm9vZjA2ZDNlY2M2IiAKBFZpZXcSDAoEcGFnZRgBIAEoCRIKCgJhdBgCIAEoA2IGcHJvdG8z";
        let text = text_of_descriptor(sent).unwrap();
        assert_eq!(
            text,
            "syntax = \"proto3\";\npackage proof06d3ecc6;\nmessage View {\n  string page = 1;\n  int64 at = 2;\n}\n"
        );
        let read = s(&text);
        assert_eq!(
            read.messages["proof06d3ecc6.View"][&2].kind,
            Kind::Scalar("int64")
        );

        // One built here with what else a descriptor may hold: a repeated message, an enum, a map, a oneof.
        let field = |name: &str, number: u8, label: u8, kind: u8, type_name: &str, oneof: Option<u8>| {
            let mut parts = vec![
                (1u32, name.as_bytes().to_vec(), false),
                (3, vec![number], true),
                (4, vec![label], true),
                (5, vec![kind], true),
            ];
            if !type_name.is_empty() {
                parts.push((6, type_name.as_bytes().to_vec(), false));
            }
            if let Some(index) = oneof {
                parts.push((9, vec![index], true));
            }
            let parts: Vec<(u32, &[u8], bool)> =
                parts.iter().map(|(n, v, is)| (*n, v.as_slice(), *is)).collect();
            message(&parts)
        };
        let entry = message(&[
            (1, b"TagsEntry", false),
            (2, &field("key", 1, 1, 9, "", None), false),
            (2, &field("value", 2, 1, 5, "", None), false),
            (7, &message(&[(7, &[1], true)]), false),
        ]);
        let line = message(&[(1, b"Line", false), (2, &field("sku", 1, 1, 9, "", None), false)]);
        let status = message(&[
            (1, b"Status", false),
            (2, &message(&[(1, b"UNKNOWN", false), (2, &[0], true)]), false),
            (2, &message(&[(1, b"PAID", false), (2, &[1], true)]), false),
        ]);
        let order = message(&[
            (1, b"Order", false),
            (2, &field("id", 1, 1, 3, "", None), false),
            (2, &field("lines", 2, 3, 11, ".shop.Order.Line", None), false),
            (2, &field("status", 3, 1, 14, ".shop.Order.Status", None), false),
            (2, &field("tags", 4, 3, 11, ".shop.Order.TagsEntry", None), false),
            (2, &field("card", 5, 1, 9, "", Some(0)), false),
            (2, &field("voucher", 6, 1, 9, "", Some(0)), false),
            (3, &line, false),
            (3, &entry, false),
            (4, &status, false),
            (8, &message(&[(1, b"payment", false)]), false),
        ]);
        let file = message(&[
            (1, b"order.proto", false),
            (2, b"shop", false),
            (4, &order, false),
            (12, b"proto3", false),
        ]);
        let text = text_of_descriptor(&base64_of(&file)).unwrap();
        let read = s(&text);
        let fields = &read.messages["shop.Order"];
        assert_eq!(
            (fields[&2].label, &fields[&2].kind),
            (Label::Repeated, &Kind::Named("shop.Order.Line".into()))
        );
        assert_eq!(fields[&3].kind, Kind::Named("shop.Order.Status".into()));
        assert_eq!(fields[&4].kind, Kind::Map, "{text}");
        assert_eq!(fields[&5].oneof.as_deref(), Some("payment"));
        assert!(
            !read.messages.contains_key("shop.Order.TagsEntry"),
            "a map's entry is not a message of its own"
        );
        // The same schema as the text a person would write is the same shape, and compatible both ways.
        let written = s("syntax = \"proto3\"; package shop; message Order { int64 id = 1; repeated Line lines = 2;
            Status status = 3; map<string, int32> tags = 4; oneof payment { string card = 5; string voucher = 6; }
            message Line { string sku = 1; } enum Status { UNKNOWN = 0; PAID = 1; } }");
        assert_eq!(read.messages, written.messages);

        // Text is never mistaken for a descriptor, and rubbish in base64 is not one.
        assert_eq!(
            text_of_descriptor("syntax = \"proto3\"; message A { int32 a = 1; }"),
            None
        );
        assert_eq!(text_of_descriptor("AAAA"), None);
        assert_eq!(text_of_descriptor("////////"), None);
        assert_eq!(text_of_descriptor(""), None);
    }

    #[test]
    fn required_fields_of_proto2_may_not_come_or_go() {
        let v1 = "syntax = \"proto2\"; message A { required int32 id = 1; optional string note = 2; }";
        let added = "syntax = \"proto2\"; message A { required int32 id = 1; optional string note = 2; required int32 n = 3; }";
        let removed = "syntax = \"proto2\"; message A { optional string note = 2; }";
        let relaxed = "syntax = \"proto2\"; message A { optional int32 id = 1; optional string note = 2; }";
        assert!(reads(added, v1).unwrap_err().contains("required field was added"));
        assert!(
            reads(removed, v1)
                .unwrap_err()
                .contains("required field was removed")
        );
        assert!(
            reads(relaxed, v1)
                .unwrap_err()
                .contains("required was added or removed")
        );
    }
}
