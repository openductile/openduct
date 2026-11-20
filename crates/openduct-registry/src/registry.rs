// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the registry keeps and the rules it keeps it by.
//!
//! - A **subject** is a name (by convention `<topic>-value` or `<topic>-key`) with numbered **versions**, each a
//!   schema. A schema has one **id** across the whole registry: the same schema registered under two subjects is
//!   one id, which is what a serializer writes in front of each message.
//! - A new version must be **compatible** with the ones before it, by the level in force for the subject (its
//!   own, or the registry's): BACKWARD (the new schema reads data written with the last one - the default),
//!   FORWARD (the last one reads data written with the new), FULL (both), each also `_TRANSITIVE` (against every
//!   version, not only the last), or NONE.
//! - Deleting is **soft** first: a version or subject marked deleted is not listed and takes no part in
//!   compatibility, but its id still resolves, so messages already written stay readable. Only then can it be
//!   deleted for good.
//!
//! Every change is first a [`Change`] - a record for the registry's topic (`log`) - and the state is what
//! applying the changes in order gives ([`Registry::apply`]). A request is answered from the state
//! ([`Registry::plan`]-style methods return the changes to write); nothing here reads a clock or picks a random
//! number, so every broker that applies the same changes holds the same registry.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;

use crate::{avro, json_schema, protobuf};

/// Confluent's registry error codes, as its clients expect them.
pub mod code {
    pub const SUBJECT_NOT_FOUND: u32 = 40401;
    pub const VERSION_NOT_FOUND: u32 = 40402;
    pub const SCHEMA_NOT_FOUND: u32 = 40403;
    pub const SUBJECT_SOFT_DELETED: u32 = 40404;
    pub const SUBJECT_NOT_SOFT_DELETED: u32 = 40405;
    pub const VERSION_SOFT_DELETED: u32 = 40406;
    pub const VERSION_NOT_SOFT_DELETED: u32 = 40407;
    pub const SUBJECT_COMPATIBILITY_NOT_SET: u32 = 40408;
    pub const INCOMPATIBLE: u32 = 409;
    pub const INVALID_SCHEMA: u32 = 42201;
    pub const INVALID_VERSION: u32 = 42202;
    pub const INVALID_COMPATIBILITY: u32 = 42203;
    pub const INVALID_MODE: u32 = 42204;
    pub const OPERATION_NOT_PERMITTED: u32 = 42205;
    pub const REFERENCE_EXISTS: u32 = 42206;
    pub const UNSUPPORTED: u32 = 42299;
}

/// A request the registry refuses: the HTTP status, Confluent's error code, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub status: u16,
    pub code: u32,
    pub message: String,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

pub(crate) fn refused<T>(code: u32, message: impl Into<String>) -> Result<T, Refused> {
    let status = if code == code::INCOMPATIBLE {
        409
    } else {
        u16::try_from(code / 100).unwrap_or(500)
    };
    Err(Refused {
        status,
        code,
        message: message.into(),
    })
}

/// How a new version must relate to the ones before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compatibility {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

impl Compatibility {
    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "BACKWARD" => Self::Backward,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            "NONE" => Self::None,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
            Self::None => "NONE",
        }
    }

    fn transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }

    fn backward(self) -> bool {
        matches!(
            self,
            Self::Backward | Self::BackwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    fn forward(self) -> bool {
        matches!(
            self,
            Self::Forward | Self::ForwardTransitive | Self::Full | Self::FullTransitive
        )
    }
}

/// Whether the registry takes changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ReadWrite,
    ReadOnly,
}

impl Mode {
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "READWRITE" => Some(Self::ReadWrite),
            "READONLY" => Some(Self::ReadOnly),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::ReadWrite => "READWRITE",
            Self::ReadOnly => "READONLY",
        }
    }
}

/// One change to the registry: what its topic holds, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// A version of a subject: registered, or (with `deleted`) soft-deleted.
    Schema {
        subject: String,
        version: i32,
        id: i32,
        schema: String,
        kind: SchemaType,
        references: Vec<Reference>,
        deleted: bool,
    },
    /// A version deleted for good.
    Forget {
        subject: String,
        version: i32,
    },
    /// The compatibility level of a subject (`None`: the registry's own), set or (`level: None`) cleared.
    Config {
        subject: Option<String>,
        level: Option<Compatibility>,
    },
    Mode(Mode),
    /// Every version of a subject up to this one marked deleted: how Confluent's registry writes a subject's
    /// soft delete. Read, never written here.
    DeleteSubject {
        subject: String,
        upto: i32,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct Version {
    id: i32,
    deleted: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct Stored {
    /// As it was registered, character for character: what a client is given back.
    text: String,
    /// The same schema whatever its spacing and the order of its attributes.
    identity: String,
    kind: SchemaType,
    /// The schemas of other subjects it uses.
    references: Vec<Reference>,
    parsed: Parsed,
}

/// A schema's use of another subject's: under `name` - an Avro type's full name, the target of a JSON `$ref`, the
/// file of a Protobuf `import` - it means that subject's version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// The kinds of schema the registry takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    pub const ALL: [Self; 3] = [Self::Avro, Self::Json, Self::Protobuf];

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == text)
    }

    /// Its name in the API and in the registry's topic.
    pub fn name(self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
}

/// A schema read, whichever kind it is.
#[derive(Debug, Clone, PartialEq)]
enum Parsed {
    Avro(avro::Schema),
    Json(json_schema::Schema),
    Protobuf(protobuf::Schema),
}

impl Parsed {
    /// Whether what was written with `writer` can be read with this schema: each kind by its own rules, and
    /// never across kinds.
    fn can_read(&self, writer: &Parsed) -> Result<(), String> {
        match (self, writer) {
            (Parsed::Avro(new), Parsed::Avro(old)) => new.can_read(old),
            (Parsed::Json(new), Parsed::Json(old)) => new.can_read(old),
            (Parsed::Protobuf(new), Parsed::Protobuf(old)) => new.can_read(old),
            _ => Err("the schema's type changed (between Avro, JSON Schema and Protobuf)".to_owned()),
        }
    }
}

/// A version of a subject, as the API answers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    pub schema: String,
    pub kind: SchemaType,
    pub references: Vec<Reference>,
}

/// The registry's state.
#[derive(Debug, Clone, PartialEq)]
pub struct Registry {
    subjects: BTreeMap<String, BTreeMap<i32, Version>>,
    schemas: BTreeMap<i32, Stored>,
    levels: BTreeMap<String, Compatibility>,
    level: Compatibility,
    mode: Mode,
    /// Schemas read from the topic before what they refer to: applied once that arrives.
    waiting: Vec<Change>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            subjects: BTreeMap::new(),
            schemas: BTreeMap::new(),
            levels: BTreeMap::new(),
            level: Compatibility::Backward,
            mode: Mode::ReadWrite,
            waiting: Vec::new(),
        }
    }
}

/// A JSON value with every object's attributes in order by name: the same schema written two ways is the same
/// text.
fn ordered(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<&String, Value> = map.iter().map(|(k, v)| (k, ordered(v))).collect();
            Value::Object(sorted.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(ordered).collect()),
        other => other.clone(),
    }
}

/// A version asked for by number or as `latest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wanted {
    Latest,
    Number(i32),
}

impl Wanted {
    pub fn parse(text: &str) -> Result<Self, Refused> {
        if text == "latest" || text == "-1" {
            return Ok(Self::Latest);
        }
        match text.parse::<i32>() {
            Ok(n) if n >= 1 => Ok(Self::Number(n)),
            _ => refused(
                code::INVALID_VERSION,
                format!(
                    "The specified version '{text}' is not a valid version id. Allowed values are between [1, \
                     2^31-1] and the string \"latest\""
                ),
            ),
        }
    }
}

impl Registry {
    /// The schemas `references` name, each with the name it goes by: looked for among the versions not deleted,
    /// or (`with_deleted`, for what the topic already holds) among all of them. `None` when one is not there.
    fn referenced<'a>(
        &'a self,
        references: &'a [Reference],
        with_deleted: bool,
    ) -> Option<Vec<(&'a str, &'a Stored)>> {
        references
            .iter()
            .map(|r| {
                let version = self.subjects.get(&r.subject)?.get(&r.version)?;
                let stored = self.schemas.get(&version.id)?;
                (with_deleted || !version.deleted).then_some((r.name.as_str(), stored))
            })
            .collect()
    }

    /// Reads a schema of this kind, with the schemas it refers to.
    fn read(
        &self,
        kind: SchemaType,
        schema: &str,
        references: &[Reference],
        with_deleted: bool,
    ) -> Result<Stored, Refused> {
        let bad = |why: String| refused(code::INVALID_SCHEMA, format!("Invalid schema: {why}"));
        let mut text = schema.to_owned();
        let Some(others) = self.referenced(references, with_deleted) else {
            return bad("it refers to a subject and version that is not registered".to_owned());
        };
        if let Some((name, _)) = others.iter().find(|(_, other)| other.kind != kind) {
            return bad(format!("the reference {name} is to a schema of another kind"));
        }
        if let Some((name, _)) = others
            .iter()
            .enumerate()
            .find(|(i, (name, _))| others.iter().skip(i + 1).any(|(later, _)| later == name))
            .map(|(_, found)| found)
        {
            return bad(format!("the reference {name} is given twice"));
        }
        let (identity, parsed) = match kind {
            SchemaType::Avro | SchemaType::Json => {
                let json: Value = match serde_json::from_str(schema) {
                    Ok(json) => json,
                    Err(e) => return bad(format!("not JSON: {e}")),
                };
                let identity = ordered(&json).to_string();
                let parsed = match kind {
                    SchemaType::Avro => {
                        // Every named type of the schemas referred to, and of those they refer to.
                        let mut known = BTreeMap::new();
                        for (name, other) in &others {
                            let Parsed::Avro(other) = &other.parsed else {
                                continue;
                            };
                            for (full, definition) in &other.named {
                                if known
                                    .insert(full.clone(), definition.clone())
                                    .is_some_and(|had| had != *definition)
                                {
                                    return bad(format!(
                                        "{full} is defined differently by two references ({name})"
                                    ));
                                }
                            }
                        }
                        avro::Schema::from_json_with(&json, &known)
                            .map(Parsed::Avro)
                            .map_err(|e| e.0)
                    }
                    _ => {
                        let named: Vec<(String, &json_schema::Schema)> = others
                            .iter()
                            .filter_map(|(name, other)| match &other.parsed {
                                Parsed::Json(other) => Some(((*name).to_owned(), other)),
                                _ => None,
                            })
                            .collect();
                        json_schema::Schema::from_json_with(json, &named)
                            .map(Parsed::Json)
                            .map_err(|e| e.0)
                    }
                };
                match parsed {
                    Ok(parsed) => (identity, parsed),
                    Err(why) => return bad(why),
                }
            }
            SchemaType::Protobuf => {
                let imports: Vec<(String, &protobuf::Schema)> = others
                    .iter()
                    .filter_map(|(name, other)| match &other.parsed {
                        Parsed::Protobuf(other) => Some(((*name).to_owned(), other)),
                        _ => None,
                    })
                    .collect();
                // Confluent's serializers send the file's descriptor, in base64, where a person sends its text:
                // either way the registry keeps, and gives back, the text.
                if let Some(described) = protobuf::text_of_descriptor(schema) {
                    text = described;
                }
                match protobuf::Schema::parse_with(&text, &imports) {
                    Ok(parsed) => (parsed.canonical.clone(), Parsed::Protobuf(parsed)),
                    Err(e) => return bad(e.0),
                }
            }
        };
        // The same text may be a schema of two kinds, or mean two things by what it refers to: each its own.
        let uses: String = references
            .iter()
            .map(|r| format!("|{}={}@{}", r.name, r.subject, r.version))
            .collect();
        Ok(Stored {
            text,
            identity: format!("{}:{identity}{uses}", kind.name()),
            kind,
            references: references.to_vec(),
            parsed,
        })
    }

    /// The ids of the schemas that refer to this version of this subject.
    pub fn referenced_by(&self, subject: &str, version: i32) -> Vec<i32> {
        self.schemas
            .iter()
            .filter(|(_, s)| {
                s.references
                    .iter()
                    .any(|r| r.subject == subject && r.version == version)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn unreferenced(&self, subject: &str, version: i32) -> Result<(), Refused> {
        match self.referenced_by(subject, version).first() {
            Some(id) => refused(
                code::REFERENCE_EXISTS,
                format!(
                    "One or more references exist to the schema {{subject={subject},version={version}}}: schema {id} refers to it"
                ),
            ),
            None => Ok(()),
        }
    }

    /// The references of the schema with this id.
    pub fn references(&self, id: i32) -> &[Reference] {
        self.schemas.get(&id).map_or(&[], |s| s.references.as_slice())
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The registry's own compatibility level.
    pub fn level(&self) -> Compatibility {
        self.level
    }

    /// A subject's own level, when it has one.
    pub fn own_level(&self, subject: &str) -> Option<Compatibility> {
        self.levels.get(subject).copied()
    }

    /// The level in force for a subject.
    pub fn level_for(&self, subject: &str) -> Compatibility {
        self.own_level(subject).unwrap_or(self.level)
    }

    /// Subjects with a version that is not deleted - or with any version at all.
    pub fn subjects(&self, with_deleted: bool) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, versions)| with_deleted || versions.values().any(|v| !v.deleted))
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn live<'a>(&'a self, subject: &str, with_deleted: bool) -> Result<Vec<(i32, &'a Version)>, Refused> {
        let found: Vec<(i32, &Version)> = self
            .subjects
            .get(subject)
            .map(|versions| {
                versions
                    .iter()
                    .filter(|(_, v)| with_deleted || !v.deleted)
                    .map(|(n, v)| (*n, v))
                    .collect()
            })
            .unwrap_or_default();
        if found.is_empty() {
            return refused(code::SUBJECT_NOT_FOUND, format!("Subject '{subject}' not found."));
        }
        Ok(found)
    }

    /// A subject's version numbers.
    pub fn versions(&self, subject: &str, with_deleted: bool) -> Result<Vec<i32>, Refused> {
        Ok(self
            .live(subject, with_deleted)?
            .into_iter()
            .map(|(n, _)| n)
            .collect())
    }

    /// One version of a subject.
    pub fn version(&self, subject: &str, wanted: Wanted, with_deleted: bool) -> Result<Registered, Refused> {
        let versions = self.live(subject, with_deleted)?;
        let found = match wanted {
            Wanted::Latest => versions.last().copied(),
            Wanted::Number(n) => versions.iter().find(|(v, _)| *v == n).copied(),
        };
        let Some((version, v)) = found else {
            return refused(
                code::VERSION_NOT_FOUND,
                format!("Version {wanted:?} not found.")
                    .replace("Number(", "")
                    .replace(')', ""),
            );
        };
        Ok(Registered {
            subject: subject.to_owned(),
            version,
            id: v.id,
            kind: self.schemas.get(&v.id).map_or(SchemaType::Avro, |s| s.kind),
            references: self.references(v.id).to_vec(),
            schema: self
                .schemas
                .get(&v.id)
                .map(|s| s.text.clone())
                .unwrap_or_default(),
        })
    }

    /// A schema by its id.
    pub fn schema(&self, id: i32) -> Result<&str, Refused> {
        match self.schemas.get(&id) {
            Some(s) => Ok(&s.text),
            None => refused(code::SCHEMA_NOT_FOUND, format!("Schema {id} not found")),
        }
    }

    /// The kind of the schema with this id.
    pub fn kind(&self, id: i32) -> Option<SchemaType> {
        self.schemas.get(&id).map(|s| s.kind)
    }

    /// Where a schema id is used: each subject and version, deleted ones left out.
    pub fn uses(&self, id: i32) -> Result<Vec<(String, i32)>, Refused> {
        self.schema(id)?;
        Ok(self
            .subjects
            .iter()
            .flat_map(|(subject, versions)| {
                versions
                    .iter()
                    .filter(move |(_, v)| v.id == id && !v.deleted)
                    .map(move |(n, _)| (subject.clone(), *n))
            })
            .collect())
    }

    /// Whether this schema is registered under the subject, and as what.
    pub fn lookup(&self, subject: &str, schema: &str, with_deleted: bool) -> Result<Registered, Refused> {
        self.lookup_as(subject, schema, SchemaType::Avro, with_deleted)
    }

    /// [`Registry::lookup`], for a schema of this kind.
    pub fn lookup_as(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
        with_deleted: bool,
    ) -> Result<Registered, Refused> {
        self.lookup_with(subject, schema, kind, &[], with_deleted)
    }

    /// [`Registry::lookup_as`], for a schema that refers to others.
    pub fn lookup_with(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
        references: &[Reference],
        with_deleted: bool,
    ) -> Result<Registered, Refused> {
        let versions = self.live(subject, with_deleted)?;
        let wanted = self.read(kind, schema, references, false)?;
        let found = versions.iter().rev().find(|(_, v)| {
            self.schemas
                .get(&v.id)
                .is_some_and(|s| s.identity == wanted.identity)
        });
        match found {
            Some((version, v)) => Ok(Registered {
                subject: subject.to_owned(),
                version: *version,
                id: v.id,
                kind,
                references: references.to_vec(),
                schema: self
                    .schemas
                    .get(&v.id)
                    .map(|s| s.text.clone())
                    .unwrap_or_default(),
            }),
            None => refused(code::SCHEMA_NOT_FOUND, "Schema not found"),
        }
    }

    /// Why `schema` may not follow the versions it is checked against, one line for each that it does not fit;
    /// none when it may. `against`: one version, or (`None`) whichever the subject's level says.
    pub fn incompatibilities(
        &self,
        subject: &str,
        schema: &str,
        against: Option<Wanted>,
    ) -> Result<Vec<String>, Refused> {
        self.incompatibilities_as(subject, schema, SchemaType::Avro, against)
    }

    /// [`Registry::incompatibilities`], for a schema of this kind.
    pub fn incompatibilities_as(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
        against: Option<Wanted>,
    ) -> Result<Vec<String>, Refused> {
        self.incompatibilities_with(subject, schema, kind, &[], against)
    }

    /// [`Registry::incompatibilities_as`], for a schema that refers to others.
    pub fn incompatibilities_with(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
        references: &[Reference],
        against: Option<Wanted>,
    ) -> Result<Vec<String>, Refused> {
        let new = self.read(kind, schema, references, false)?;
        let level = self.level_for(subject);
        let existing: Vec<(i32, &Stored)> = self
            .subjects
            .get(subject)
            .into_iter()
            .flatten()
            .filter(|(_, v)| !v.deleted)
            .filter_map(|(n, v)| self.schemas.get(&v.id).map(|s| (*n, s)))
            .collect();
        let checked: Vec<(i32, &Stored)> = match against {
            Some(wanted) => {
                let version = self.version(subject, wanted, false)?.version;
                existing.into_iter().filter(|(n, _)| *n == version).collect()
            }
            None if level.transitive() => existing,
            None => existing.into_iter().next_back().into_iter().collect(),
        };
        let mut problems = Vec::new();
        for (version, old) in checked {
            if level.backward()
                && let Err(why) = new.parsed.can_read(&old.parsed)
            {
                problems.push(format!(
                    "The new schema cannot read data written with version {version}: {why}"
                ));
            }
            if level.forward()
                && let Err(why) = old.parsed.can_read(&new.parsed)
            {
                problems.push(format!(
                    "Version {version} cannot read data written with the new schema: {why}"
                ));
            }
        }
        Ok(problems)
    }

    fn writable(&self) -> Result<(), Refused> {
        match self.mode {
            Mode::ReadWrite => Ok(()),
            Mode::ReadOnly => refused(
                code::OPERATION_NOT_PERMITTED,
                "The registry is in READONLY mode: it takes no changes",
            ),
        }
    }

    /// Registers `schema` under `subject`: its id, and the change to write (none, when it is registered there
    /// already).
    pub fn register(&self, subject: &str, schema: &str) -> Result<(i32, Vec<Change>), Refused> {
        self.register_as(subject, schema, SchemaType::Avro)
    }

    /// [`Registry::register`], for a schema of this kind.
    pub fn register_as(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
    ) -> Result<(i32, Vec<Change>), Refused> {
        self.register_with(subject, schema, kind, &[])
    }

    /// [`Registry::register_as`], for a schema that refers to others: each reference a name the schema uses and
    /// the subject and version it stands for.
    pub fn register_with(
        &self,
        subject: &str,
        schema: &str,
        kind: SchemaType,
        references: &[Reference],
    ) -> Result<(i32, Vec<Change>), Refused> {
        self.writable()?;
        let new = self.read(kind, schema, references, false)?;
        let versions = self.subjects.get(subject);
        // Registered here already: the same id again.
        if let Some(v) = versions.into_iter().flatten().map(|(_, v)| v).find(|v| {
            !v.deleted
                && self
                    .schemas
                    .get(&v.id)
                    .is_some_and(|s| s.identity == new.identity)
        }) {
            return Ok((v.id, Vec::new()));
        }
        let problems = self.incompatibilities_with(subject, schema, kind, references, None)?;
        if !problems.is_empty() {
            return refused(
                code::INCOMPATIBLE,
                format!(
                    "Schema being registered is incompatible with an earlier schema for subject \"{subject}\" \
                     ({}): {}",
                    self.level_for(subject).name(),
                    problems.join("; ")
                ),
            );
        }
        // The same schema anywhere in the registry is the same id.
        let id = self
            .schemas
            .iter()
            .find(|(_, s)| s.identity == new.identity)
            .map(|(id, _)| *id)
            .or_else(|| {
                self.schemas
                    .keys()
                    .next_back()
                    .map_or(Some(1), |last| last.checked_add(1))
            });
        let Some(id) = id else {
            return refused(code::UNSUPPORTED, "The registry has no schema id left");
        };
        let version = versions
            .and_then(|v| v.keys().next_back())
            .map_or(Some(1), |last| last.checked_add(1));
        let Some(version) = version else {
            return refused(
                code::UNSUPPORTED,
                format!("Subject '{subject}' has no version number left"),
            );
        };
        Ok((
            id,
            vec![Change::Schema {
                subject: subject.to_owned(),
                version,
                id,
                schema: new.text.clone(),
                kind,
                references: references.to_vec(),
                deleted: false,
            }],
        ))
    }

    fn marked(&self, subject: &str, version: i32, v: &Version, deleted: bool) -> Change {
        Change::Schema {
            subject: subject.to_owned(),
            version,
            id: v.id,
            kind: self.schemas.get(&v.id).map_or(SchemaType::Avro, |s| s.kind),
            references: self.references(v.id).to_vec(),
            schema: self
                .schemas
                .get(&v.id)
                .map(|s| s.text.clone())
                .unwrap_or_default(),
            deleted,
        }
    }

    /// Deletes a subject: softly (its versions marked), or for good - which needs the soft delete first. The
    /// versions it deleted.
    pub fn delete_subject(&self, subject: &str, permanent: bool) -> Result<(Vec<i32>, Vec<Change>), Refused> {
        self.writable()?;
        let all = self.live(subject, true)?;
        for (version, _) in &all {
            self.unreferenced(subject, *version)?;
        }
        let live: Vec<(i32, &Version)> = all.iter().filter(|(_, v)| !v.deleted).copied().collect();
        if permanent {
            if !live.is_empty() {
                return refused(
                    code::SUBJECT_NOT_SOFT_DELETED,
                    format!("Subject '{subject}' was not deleted first before being permanently deleted"),
                );
            }
            let changes = all
                .iter()
                .map(|(n, _)| Change::Forget {
                    subject: subject.to_owned(),
                    version: *n,
                })
                .collect();
            return Ok((all.iter().map(|(n, _)| *n).collect(), changes));
        }
        if live.is_empty() {
            return refused(
                code::SUBJECT_SOFT_DELETED,
                format!("Subject '{subject}' was soft deleted. Set permanent=true to delete permanently"),
            );
        }
        let changes = live
            .iter()
            .map(|(n, v)| self.marked(subject, *n, v, true))
            .collect();
        Ok((live.iter().map(|(n, _)| *n).collect(), changes))
    }

    /// Deletes one version: softly, or for good - which needs the soft delete first. The version it deleted.
    pub fn delete_version(
        &self,
        subject: &str,
        wanted: Wanted,
        permanent: bool,
    ) -> Result<(i32, Vec<Change>), Refused> {
        self.writable()?;
        let found = self.version(subject, wanted, true)?;
        let version = found.version;
        self.unreferenced(subject, version)?;
        let deleted = self
            .subjects
            .get(subject)
            .and_then(|v| v.get(&version))
            .is_some_and(|v| v.deleted);
        match (permanent, deleted) {
            (true, false) => refused(
                code::VERSION_NOT_SOFT_DELETED,
                format!(
                    "Subject '{subject}' Version {version} was not deleted first before being permanently deleted"
                ),
            ),
            (true, true) => Ok((
                version,
                vec![Change::Forget {
                    subject: subject.to_owned(),
                    version,
                }],
            )),
            (false, true) => refused(
                code::VERSION_SOFT_DELETED,
                format!(
                    "Subject '{subject}' Version {version} was soft deleted. Set permanent=true to delete \
                     permanently"
                ),
            ),
            (false, false) => Ok((
                version,
                vec![Change::Schema {
                    subject: subject.to_owned(),
                    version,
                    id: found.id,
                    schema: found.schema,
                    kind: found.kind,
                    references: found.references,
                    deleted: true,
                }],
            )),
        }
    }

    /// Sets a subject's level (`None`: the registry's own), or clears a subject's (`level: None`).
    pub fn set_level(
        &self,
        subject: Option<&str>,
        level: Option<Compatibility>,
    ) -> Result<Vec<Change>, Refused> {
        self.writable()?;
        Ok(vec![Change::Config {
            subject: subject.map(str::to_owned),
            level,
        }])
    }

    /// Sets the mode. Allowed whatever the mode is: it is how a read-only registry is opened again.
    pub fn set_mode(&self, mode: Mode) -> Vec<Change> {
        vec![Change::Mode(mode)]
    }

    /// Whether the registry is as this change would leave it: asked after a change was written and read back, to
    /// learn whether it took or lost to another broker's (see [`Registry::apply`]).
    pub fn holds(&self, change: &Change) -> bool {
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
                let at = self.subjects.get(subject).and_then(|v| v.get(version));
                let same_schema = self
                    .read(*kind, schema, references, true)
                    .ok()
                    .zip(self.schemas.get(id))
                    .is_some_and(|(wanted, held)| wanted.identity == held.identity);
                at.is_some_and(|v| v.id == *id && v.deleted == *deleted) && same_schema
            }
            Change::Forget { subject, version } => !self
                .subjects
                .get(subject)
                .is_some_and(|v| v.contains_key(version)),
            Change::Config {
                subject: Some(subject),
                level,
            } => self.own_level(subject) == *level,
            Change::Config { subject: None, level } => self.level == level.unwrap_or(Compatibility::Backward),
            Change::Mode(mode) => self.mode == *mode,
            Change::DeleteSubject { .. } => true,
        }
    }

    /// Applies one change. A change that does not read (a schema that is not one) is skipped, with the reason:
    /// the topic may hold what another registry wrote.
    pub fn apply(&mut self, change: &Change) -> Result<(), String> {
        let applied = self.apply_one(change);
        // Whatever was waiting for this one, and whatever was waiting for those.
        while !self.waiting.is_empty() {
            let waiting = std::mem::take(&mut self.waiting);
            let before = waiting.len();
            for change in &waiting {
                if let Err(why) = self.apply_one(change) {
                    return applied.and(Err(why));
                }
            }
            if self.waiting.len() >= before {
                break;
            }
        }
        applied
    }

    fn apply_one(&mut self, change: &Change) -> Result<(), String> {
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
                // Two brokers may each have taken a registration before seeing the other's: the first record in
                // the topic stands, and a later one that would give its id to another schema, or its version
                // number to another id, is skipped - by every broker alike, since all read the same order.
                // What it refers to has not been read yet (the topic may give a record back after one that uses
                // it, once compaction has moved things): kept until it has.
                if self.referenced(references, true).is_none() {
                    self.waiting.push(change.clone());
                    return Ok(());
                }
                let stored = self
                    .read(*kind, schema, references, true)
                    .map_err(|e| format!("schema {id} of {subject:?}: {e}"))?;
                if let Some(held) = self.schemas.get(id)
                    && held.identity != stored.identity
                {
                    return Err(format!(
                        "id {id} is another schema's already: {subject:?} version {version} skipped"
                    ));
                }
                if let Some(held) = self.subjects.get(subject).and_then(|v| v.get(version))
                    && held.id != *id
                {
                    return Err(format!(
                        "{subject:?} version {version} is id {} already: id {id} skipped",
                        held.id
                    ));
                }
                if let Some((other, _)) = self
                    .schemas
                    .iter()
                    .find(|(other, s)| *other != id && s.identity == stored.identity)
                {
                    return Err(format!(
                        "the schema has id {other} already: id {id} for {subject:?} skipped"
                    ));
                }
                self.schemas.entry(*id).or_insert(stored);
                self.subjects.entry(subject.clone()).or_default().insert(
                    *version,
                    Version {
                        id: *id,
                        deleted: *deleted,
                    },
                );
            }
            Change::Forget { subject, version } => {
                let gone = self.subjects.get_mut(subject).and_then(|v| v.remove(version));
                if self.subjects.get(subject).is_some_and(BTreeMap::is_empty) {
                    self.subjects.remove(subject);
                    self.levels.remove(subject);
                }
                // A schema no version refers to any more is gone with the last of them.
                if let Some(gone) = gone
                    && !self.subjects.values().flatten().any(|(_, v)| v.id == gone.id)
                {
                    self.schemas.remove(&gone.id);
                }
            }
            Change::Config { subject, level } => match (subject, level) {
                (Some(subject), Some(level)) => {
                    self.levels.insert(subject.clone(), *level);
                }
                (Some(subject), None) => {
                    self.levels.remove(subject);
                }
                (None, Some(level)) => self.level = *level,
                (None, None) => self.level = Compatibility::Backward,
            },
            Change::Mode(mode) => self.mode = *mode,
            Change::DeleteSubject { subject, upto } => {
                for (_, v) in self
                    .subjects
                    .get_mut(subject)
                    .into_iter()
                    .flatten()
                    .filter(|(n, _)| *n <= upto)
                {
                    v.deleted = true;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const V1: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;
    /// V1 and a field with a default: reads old data.
    pub(crate) const V2: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"note","type":"string","default":""}]}"#;
    /// V1 and a field without a default: cannot read old data.
    pub(crate) const BREAKING: &str = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"total","type":"double"}]}"#;

    /// Registers, writing the changes straight into the state.
    pub(crate) fn register(r: &mut Registry, subject: &str, schema: &str) -> Result<i32, Refused> {
        let (id, changes) = r.register(subject, schema)?;
        for c in &changes {
            r.apply(c).unwrap();
        }
        Ok(id)
    }

    fn level(r: &mut Registry, subject: Option<&str>, to: Option<Compatibility>) {
        let changes = r.set_level(subject, to).unwrap();
        run(r, changes);
    }

    fn mode(r: &mut Registry, to: Mode) {
        let changes = r.set_mode(to);
        run(r, changes);
    }

    fn run(r: &mut Registry, changes: Vec<Change>) {
        for c in &changes {
            r.apply(c).unwrap();
        }
    }

    #[test]
    fn a_schema_has_one_id_and_a_subject_numbers_its_versions() {
        let mut r = Registry::default();
        assert_eq!(register(&mut r, "orders-value", V1).unwrap(), 1);
        // The same schema again, spaced and ordered differently: registered already.
        let respaced =
            r#"{ "name": "Order", "type": "record", "fields": [ {"type": "long", "name": "id"} ] }"#;
        assert_eq!(register(&mut r, "orders-value", respaced).unwrap(), 1);
        assert_eq!(r.versions("orders-value", false).unwrap(), vec![1]);
        // Under another subject it is the same id; a new schema is the next one.
        assert_eq!(register(&mut r, "returns-value", V1).unwrap(), 1);
        assert_eq!(register(&mut r, "orders-value", V2).unwrap(), 2);
        assert_eq!(r.versions("orders-value", false).unwrap(), vec![1, 2]);
        assert_eq!(
            r.subjects(false),
            vec!["orders-value".to_owned(), "returns-value".into()]
        );
        let latest = r.version("orders-value", Wanted::Latest, false).unwrap();
        assert_eq!((latest.version, latest.id, latest.schema.as_str()), (2, 2, V2));
        assert_eq!(r.schema(1).unwrap(), V1, "given back as it was registered");
        assert_eq!(
            r.uses(1).unwrap(),
            vec![("orders-value".to_owned(), 1), ("returns-value".into(), 1)]
        );
        assert_eq!(r.lookup("orders-value", respaced, false).unwrap().version, 1);
        for (found, code) in [
            (r.versions("nowhere", false).map(|_| ()), code::SUBJECT_NOT_FOUND),
            (
                r.version("orders-value", Wanted::Number(9), false).map(|_| ()),
                code::VERSION_NOT_FOUND,
            ),
            (r.schema(9).map(|_| ()), code::SCHEMA_NOT_FOUND),
            (
                r.lookup("orders-value", BREAKING, false).map(|_| ()),
                code::SCHEMA_NOT_FOUND,
            ),
            (r.register("orders-value", "{").map(|_| ()), code::INVALID_SCHEMA),
            (Wanted::parse("0").map(|_| ()), code::INVALID_VERSION),
        ] {
            assert_eq!(found.unwrap_err().code, code);
        }
    }

    #[test]
    fn a_new_version_must_fit_the_level_in_force() {
        let mut r = Registry::default();
        register(&mut r, "orders-value", V1).unwrap();
        // BACKWARD, by default: the new schema must read the old data.
        let e = r.register("orders-value", BREAKING).unwrap_err();
        assert_eq!((e.status, e.code), (409, code::INCOMPATIBLE));
        assert!(
            e.message.contains("field total") && e.message.contains("BACKWARD"),
            "{e}"
        );
        assert_eq!(
            r.versions("orders-value", false).unwrap(),
            vec![1],
            "nothing was registered"
        );
        assert_eq!(
            r.incompatibilities("orders-value", V2, None).unwrap(),
            Vec::<String>::new()
        );
        // FORWARD: the old schema must read the new data - an added field is skipped, so both fit.
        level(&mut r, Some("orders-value"), Some(Compatibility::Forward));
        assert_eq!(register(&mut r, "orders-value", BREAKING).unwrap(), 2);
        // FULL would have refused it; NONE takes anything that is a schema.
        level(&mut r, Some("orders-value"), Some(Compatibility::Full));
        assert!(r.register("orders-value", r#""string""#).is_err());
        level(&mut r, Some("orders-value"), Some(Compatibility::None));
        assert_eq!(register(&mut r, "orders-value", r#""string""#).unwrap(), 3);
        // A subject's own level, or else the registry's.
        assert_eq!(r.level_for("orders-value"), Compatibility::None);
        assert_eq!(r.level_for("other"), Compatibility::Backward);
        level(&mut r, None, Some(Compatibility::Full));
        level(&mut r, Some("orders-value"), None);
        assert_eq!(
            (r.level_for("orders-value"), r.own_level("orders-value")),
            (Compatibility::Full, None)
        );
    }

    #[test]
    fn transitive_levels_check_every_version_not_only_the_last() {
        let optional = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"total","type":"double","default":0}]}"#;
        let mut r = Registry::default();
        level(&mut r, None, Some(Compatibility::Backward));
        register(&mut r, "s", V1).unwrap();
        register(&mut r, "s", optional).unwrap();
        // total without a default reads version 2 (which has total) but not version 1.
        assert_eq!(
            register(&mut r, "s", BREAKING).unwrap(),
            3,
            "BACKWARD looks at the last version only"
        );
        let mut r = Registry::default();
        level(&mut r, None, Some(Compatibility::BackwardTransitive));
        register(&mut r, "s", V1).unwrap();
        register(&mut r, "s", optional).unwrap();
        let e = r.register("s", BREAKING).unwrap_err();
        assert!(e.message.contains("version 1"), "{e}");
        // Against one named version.
        assert!(
            r.incompatibilities("s", BREAKING, Some(Wanted::Number(2)))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            r.incompatibilities("s", BREAKING, Some(Wanted::Number(1)))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn deleting_is_soft_first_and_an_id_outlives_it() {
        let mut r = Registry::default();
        register(&mut r, "s", V1).unwrap();
        register(&mut r, "s", V2).unwrap();
        assert_eq!(
            r.delete_version("s", Wanted::Number(1), true).unwrap_err().code,
            code::VERSION_NOT_SOFT_DELETED
        );
        let (version, changes) = r.delete_version("s", Wanted::Number(1), false).unwrap();
        run(&mut r, changes);
        assert_eq!(version, 1);
        assert_eq!(r.versions("s", false).unwrap(), vec![2]);
        assert_eq!(r.versions("s", true).unwrap(), vec![1, 2]);
        assert_eq!(
            r.version("s", Wanted::Number(1), false).unwrap_err().code,
            code::VERSION_NOT_FOUND
        );
        assert_eq!(
            r.schema(1).unwrap(),
            V1,
            "messages written with it can still be read"
        );
        assert_eq!(
            r.delete_version("s", Wanted::Number(1), false).unwrap_err().code,
            code::VERSION_SOFT_DELETED
        );
        // A deleted version takes no part in compatibility.
        assert!(r.incompatibilities("s", BREAKING, None).unwrap().len() == 1);

        assert_eq!(
            r.delete_subject("s", true).unwrap_err().code,
            code::SUBJECT_NOT_SOFT_DELETED
        );
        let (versions, changes) = r.delete_subject("s", false).unwrap();
        run(&mut r, changes);
        assert_eq!(versions, vec![2]);
        assert!(r.subjects(false).is_empty());
        assert_eq!(r.subjects(true), vec!["s".to_owned()]);
        assert_eq!(
            r.delete_subject("s", false).unwrap_err().code,
            code::SUBJECT_SOFT_DELETED
        );
        // Registered again after a soft delete: the next version number, the same id.
        assert_eq!(register(&mut r, "s", V1).unwrap(), 1);
        assert_eq!(r.versions("s", false).unwrap(), vec![3]);
        // For good: the subject and, with its last version, each schema.
        let (_, changes) = r.delete_subject("s", false).unwrap();
        run(&mut r, changes);
        let (versions, changes) = r.delete_subject("s", true).unwrap();
        run(&mut r, changes);
        assert_eq!(versions, vec![1, 2, 3]);
        assert!(r.subjects(true).is_empty());
        assert_eq!(r.schema(1).unwrap_err().code, code::SCHEMA_NOT_FOUND);
        assert_eq!(r, Registry::default());
    }

    #[test]
    fn of_two_registrations_made_at_once_the_first_in_the_topic_stands() {
        // Two brokers, each from the same empty state, each taking a different schema for the same subject.
        let (a, b) = (Registry::default(), Registry::default());
        let (_, from_a) = a.register("s", V1).unwrap();
        let (_, from_b) = b.register("s", r#""string""#).unwrap();
        // Both plans say version 1, id 1. The topic holds A's first; every broker applies both in that order.
        let mut r = Registry::default();
        r.apply(&from_a[0]).unwrap();
        let lost = r.apply(&from_b[0]).unwrap_err();
        assert!(lost.contains("another schema's already"), "{lost}");
        assert!(r.holds(&from_a[0]) && !r.holds(&from_b[0]));
        // B plans again from what the topic gave it, and gets the next id.
        let (id, again) = r.register("s2", r#""string""#).unwrap();
        r.apply(&again[0]).unwrap();
        assert_eq!(id, 2);
        assert!(r.holds(&again[0]));
        // The same schema registered on both at once: one id, whichever came first.
        let (_, one) = r.register("x", r#""long""#).unwrap();
        let mut copy = r.clone();
        copy.apply(&one[0]).unwrap();
        let (_, two) = r.register("y", r#""long""#).unwrap();
        assert!(
            copy.apply(&two[0]).is_ok(),
            "the same id for the same schema is no conflict"
        );
        assert_eq!(copy.uses(3).unwrap().len(), 2);
    }

    #[test]
    fn a_schema_read_before_the_one_it_refers_to_waits_for_it() {
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"amount","type":"long"}]}"#;
        let order = r#"{"type":"record","name":"Order","fields":[{"name":"total","type":"Money"}]}"#;
        let basket = r#"{"type":"record","name":"Basket","fields":[{"name":"first","type":"Order"}]}"#;
        let uses = |name: &str, subject: &str| {
            vec![Reference {
                name: name.to_owned(),
                subject: subject.to_owned(),
                version: 1,
            }]
        };
        // As it was registered, in order.
        let mut live = Registry::default();
        let mut changes = Vec::new();
        for (subject, schema, references) in [
            ("money", money, Vec::new()),
            ("orders", order, uses("Money", "money")),
            ("baskets", basket, uses("Order", "orders")),
        ] {
            let (_, made) = live
                .register_with(subject, schema, SchemaType::Avro, &references)
                .unwrap();
            run(&mut live, made.clone());
            changes.extend(made);
        }
        assert_eq!(live.referenced_by("money", 1), vec![2]);
        assert_eq!(live.referenced_by("orders", 1), vec![3]);
        // The same records the other way round, as a topic might give them back: each waits for what it uses,
        // and the registry ends the same.
        let mut replayed = Registry::default();
        for change in changes.iter().rev() {
            replayed.apply(change).unwrap();
        }
        assert_eq!(replayed, live);
        assert_eq!(replayed.version("baskets", Wanted::Latest, false).unwrap().id, 3);
        // One whose reference never comes is not in the registry, and nothing else is held up by it.
        let mut alone = Registry::default();
        alone.apply(&changes[1]).unwrap();
        assert!(alone.subjects(true).is_empty());
        register(&mut alone, "other", V1).unwrap();
        assert_eq!(alone.subjects(false), vec!["other".to_owned()]);
    }

    #[test]
    fn a_read_only_registry_takes_no_change_but_the_one_that_opens_it() {
        let mut r = Registry::default();
        register(&mut r, "s", V1).unwrap();
        mode(&mut r, Mode::ReadOnly);
        for refused in [
            r.register("s", V2).map(|_| ()),
            r.delete_subject("s", false).map(|_| ()),
            r.set_level(None, Some(Compatibility::None)).map(|_| ()),
        ] {
            assert_eq!(refused.unwrap_err().code, code::OPERATION_NOT_PERMITTED);
        }
        assert_eq!(
            r.version("s", Wanted::Latest, false).unwrap().id,
            1,
            "reading goes on"
        );
        mode(&mut r, Mode::ReadWrite);
        assert!(r.register("s", V2).is_ok());
    }
}
