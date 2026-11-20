// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The registry's REST API: the paths, bodies, statuses and error codes of Confluent's Schema Registry, which is
//! what Kafka serializers and tools speak.
//!
//! [`handle`] turns one request into an [`Outcome`] without touching anything: an answer, or changes to write
//! and the answer to give once they are written. Whoever serves HTTP writes the changes to the registry's topic,
//! applies them, and only then answers - so a client told "registered" can read it back from any broker.
//!
//! | | |
//! |---|---|
//! | `GET /subjects` | subjects (`?deleted=true`: soft-deleted ones too) |
//! | `POST /subjects/{s}/versions` | register a schema: `{"id":…}` |
//! | `GET /subjects/{s}/versions` | its version numbers |
//! | `GET /subjects/{s}/versions/{n or latest}` | one version; `…/schema`: the schema alone |
//! | `POST /subjects/{s}` | is this schema registered here? |
//! | `DELETE /subjects/{s}`, `DELETE /subjects/{s}/versions/{n}` | soft delete; `?permanent=true` after it, for good |
//! | `GET /schemas/ids/{id}` | a schema by id; `…/schema`, `…/versions`, `…/subjects` |
//! | `GET /schemas/types` | `["AVRO","JSON","PROTOBUF"]` |
//! | `POST /compatibility/subjects/{s}/versions[/{n or latest}]` | would this schema be taken? `?verbose=true`: why not |
//! | `GET`, `PUT /config`; `GET`, `PUT`, `DELETE /config/{s}` | compatibility levels |
//! | `GET`, `PUT /mode` | `READWRITE` or `READONLY` |
//!
//! A schema is Avro unless its `schemaType` says `JSON` (JSON Schema) or `PROTOBUF`. It may use the schemas of
//! other subjects: `references` lists each as the name the schema uses (an Avro type's full name, the target of
//! a JSON `$ref`, the file of a Protobuf `import`) with the subject and version it stands for.
//! `GET /subjects/{s}/versions/{n}/referencedby` lists the ids of the schemas that refer to one, and a schema
//! others refer to cannot be deleted.

use serde_json::{Value, json};

use crate::registry::{
    Change, Compatibility, Mode, Reference, Refused, Registered, Registry, SchemaType, Wanted, code, refused,
};

/// The content type of every answer.
pub const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// One HTTP request, as far as the registry needs it.
#[derive(Debug, Clone, Copy)]
pub struct Request<'a> {
    pub method: &'a str,
    /// The path alone, still percent-encoded.
    pub path: &'a str,
    /// What follows `?`, if anything.
    pub query: &'a str,
    pub body: &'a [u8],
}

/// An answer: a status and a JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub body: String,
}

/// What a request comes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Answer(Response),
    /// Write these changes, apply them, then give this answer.
    Write {
        changes: Vec<Change>,
        then: Response,
    },
}

fn ok(body: Value) -> Response {
    Response {
        status: 200,
        body: body.to_string(),
    }
}

fn error(status: u16, code: u32, message: &str) -> Response {
    Response {
        status,
        body: json!({ "error_code": code, "message": message }).to_string(),
    }
}

impl From<Refused> for Response {
    fn from(r: Refused) -> Self {
        error(r.status, r.code, &r.message)
    }
}

/// `%41` as `A`; what is not valid percent-encoding, or not UTF-8 once decoded, is no path this API has.
fn decoded(part: &str) -> Option<String> {
    let bytes = part.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while let Some(b) = bytes.get(at) {
        if *b == b'%' {
            let hex = part.get(at + 1..at + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            at += 3;
        } else {
            out.push(*b);
            at += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn flag(query: &str, name: &str) -> bool {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .any(|(k, v)| k == name && v.eq_ignore_ascii_case("true"))
}

fn version_json(v: &Registered) -> Value {
    let mut out = json!({ "subject": v.subject, "version": v.version, "id": v.id, "schema": v.schema });
    typed(&mut out, v.kind);
    referring(&mut out, &v.references);
    out
}

/// `references` beside a schema that has any, as Confluent's registry says them.
fn referring(out: &mut Value, references: &[Reference]) {
    if !references.is_empty()
        && let Some(map) = out.as_object_mut()
    {
        let list: Vec<Value> = references
            .iter()
            .map(|r| json!({ "name": r.name, "subject": r.subject, "version": r.version }))
            .collect();
        map.insert("references".to_owned(), json!(list));
    }
}

/// `schemaType` beside a schema that is not Avro, as Confluent's registry says it.
fn typed(out: &mut Value, kind: SchemaType) {
    if kind != SchemaType::Avro
        && let Some(map) = out.as_object_mut()
    {
        map.insert("schemaType".to_owned(), json!(kind.name()));
    }
}

/// What a request's body says of a schema: its text, its kind (Avro, when it does not say), and the schemas of
/// other subjects it refers to.
struct Asked {
    schema: String,
    kind: SchemaType,
    references: Vec<Reference>,
}

fn schema_of(body: &[u8]) -> Result<Asked, Refused> {
    let Ok(json) = serde_json::from_slice::<Value>(body) else {
        return refused(code::INVALID_SCHEMA, "The request body is not JSON");
    };
    let kind = match json.get("schemaType").and_then(Value::as_str) {
        None => SchemaType::Avro,
        Some(name) => match SchemaType::parse(name) {
            Some(kind) => kind,
            None => {
                return refused(
                    code::UNSUPPORTED,
                    format!("Schemas of type {name} are not taken: AVRO, JSON and PROTOBUF are"),
                );
            }
        },
    };
    let mut references = Vec::new();
    for r in json
        .get("references")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let text = |name: &str| r.get(name).and_then(Value::as_str);
        let version = r
            .get("version")
            .and_then(Value::as_i64)
            .and_then(|v| i32::try_from(v).ok());
        match (text("name"), text("subject"), version) {
            (Some(name), Some(subject), Some(version)) => references.push(Reference {
                name: name.to_owned(),
                subject: subject.to_owned(),
                version,
            }),
            _ => {
                return refused(
                    code::INVALID_SCHEMA,
                    "A reference needs a \"name\", a \"subject\" and a \"version\" number",
                );
            }
        }
    }
    match json.get("schema").and_then(Value::as_str) {
        Some(schema) => Ok(Asked {
            schema: schema.to_owned(),
            kind,
            references,
        }),
        None => refused(code::INVALID_SCHEMA, "The request body has no \"schema\""),
    }
}

fn level_of(body: &[u8]) -> Result<Compatibility, Refused> {
    let json: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let name = json
        .get("compatibility")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match Compatibility::parse(name) {
        Some(level) => Ok(level),
        None => refused(
            code::INVALID_COMPATIBILITY,
            "Invalid compatibility level. Valid values are none, backward, forward, full, \
             backward_transitive, forward_transitive, and full_transitive",
        ),
    }
}

fn written(changes: Vec<Change>, then: Response) -> Outcome {
    if changes.is_empty() {
        Outcome::Answer(then)
    } else {
        Outcome::Write { changes, then }
    }
}

/// One request, answered from `registry` as it stands.
pub fn handle(registry: &Registry, request: &Request<'_>) -> Outcome {
    handle_as(registry, request, &Everything)
}

/// What a caller may do with a subject, or (no subject) with the registry as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    /// See versions and schemas, look one up, test compatibility. Of the registry: schemas no subject lists.
    Read,
    /// Register a version.
    Write,
    Delete,
    /// See a compatibility level; of the registry, its own level and its mode.
    ReadLevel,
    /// Set or clear one; of the registry, its own level and its mode.
    WriteLevel,
}

/// What the caller of a request may do.
pub trait Access {
    fn may(&self, subject: Option<&str>, act: Act) -> bool;
}

/// A caller who may do everything: one with the registry's token, or a registry that asks for none.
#[derive(Debug, Clone, Copy)]
pub struct Everything;

impl Access for Everything {
    fn may(&self, _: Option<&str>, _: Act) -> bool {
        true
    }
}

fn denied<T>(subject: Option<&str>, act: Act) -> Result<T, Refused> {
    let what = match act {
        Act::Read => "read",
        Act::Write => "register under",
        Act::Delete => "delete",
        Act::ReadLevel => "see the compatibility level or mode of",
        Act::WriteLevel => "change the compatibility level or mode of",
    };
    Err(Refused {
        status: 403,
        code: 40301,
        message: match subject {
            Some(subject) => format!("User is denied operation: may not {what} subject '{subject}'"),
            None => format!("User is denied operation: may not {what} the registry"),
        },
    })
}

/// One request, answered from `registry` as it stands, for a caller who may do what `access` says: a subject
/// the caller may not read is as if it were not there in what is listed, and refused (`403`) when asked for.
pub fn handle_as(registry: &Registry, request: &Request<'_>, access: &dyn Access) -> Outcome {
    match route(registry, request, access) {
        Ok(outcome) => outcome,
        Err(refusal) => Outcome::Answer(refusal.into()),
    }
}

fn route(registry: &Registry, request: &Request<'_>, access: &dyn Access) -> Result<Outcome, Refused> {
    let Some(parts) = request
        .path
        .split('/')
        .filter(|p| !p.is_empty())
        .map(decoded)
        .collect::<Option<Vec<String>>>()
    else {
        return Ok(Outcome::Answer(error(404, 404, "HTTP 404 Not Found")));
    };
    let parts: Vec<&str> = parts.iter().map(String::as_str).collect();
    let (method, query, body) = (request.method, request.query, request.body);
    let deleted = flag(query, "deleted");
    let permanent = flag(query, "permanent");
    let answer = |body: Value| Ok(Outcome::Answer(ok(body)));
    // What the request needs of its caller, where its path says the subject.
    let need = match (method, parts.as_slice()) {
        ("POST", ["subjects", subject, "versions"]) => Some((Some(*subject), Act::Write)),
        ("GET", ["subjects", subject, ..])
        | ("POST", ["subjects", subject])
        | ("POST", ["compatibility", "subjects", subject, ..]) => Some((Some(*subject), Act::Read)),
        ("DELETE", ["subjects", subject, ..]) => Some((Some(*subject), Act::Delete)),
        ("GET", ["config" | "mode"]) => Some((None, Act::ReadLevel)),
        ("PUT", ["config" | "mode"]) => Some((None, Act::WriteLevel)),
        ("GET", ["config", subject]) => Some((Some(*subject), Act::ReadLevel)),
        ("PUT" | "DELETE", ["config", subject]) => Some((Some(*subject), Act::WriteLevel)),
        _ => None,
    };
    if let Some((subject, act)) = need
        && !access.may(subject, act)
    {
        return denied(subject, act);
    }
    let readable = |subject: &str| access.may(Some(subject), Act::Read);
    match (method, parts.as_slice()) {
        ("GET", []) => answer(json!({})),
        ("GET", ["subjects"]) => answer(json!(
            registry
                .subjects(deleted)
                .into_iter()
                .filter(|subject| readable(subject))
                .collect::<Vec<String>>()
        )),
        ("POST", ["subjects", subject, "versions"]) => {
            let asked = schema_of(body)?;
            // A schema may only take in what its registrant may read.
            if let Some(hidden) = asked.references.iter().find(|r| !readable(&r.subject)) {
                return denied(Some(&hidden.subject), Act::Read);
            }
            let (id, changes) =
                registry.register_with(subject, &asked.schema, asked.kind, &asked.references)?;
            Ok(written(changes, ok(json!({ "id": id }))))
        }
        ("GET", ["subjects", subject, "versions"]) => answer(json!(registry.versions(subject, deleted)?)),
        ("GET", ["subjects", subject, "versions", version]) => {
            let found = registry.version(subject, Wanted::parse(version)?, deleted)?;
            answer(version_json(&found))
        }
        ("GET", ["subjects", subject, "versions", version, "schema"]) => {
            let found = registry.version(subject, Wanted::parse(version)?, deleted)?;
            Ok(Outcome::Answer(Response {
                status: 200,
                body: found.schema,
            }))
        }
        ("GET", ["subjects", subject, "versions", version, "referencedby"]) => {
            let found = registry.version(subject, Wanted::parse(version)?, deleted)?;
            answer(json!(registry.referenced_by(subject, found.version)))
        }
        ("POST", ["subjects", subject]) => {
            let asked = schema_of(body)?;
            answer(version_json(&registry.lookup_with(
                subject,
                &asked.schema,
                asked.kind,
                &asked.references,
                deleted,
            )?))
        }
        ("DELETE", ["subjects", subject]) => {
            let (versions, changes) = registry.delete_subject(subject, permanent)?;
            Ok(written(changes, ok(json!(versions))))
        }
        ("DELETE", ["subjects", subject, "versions", version]) => {
            let (version, changes) = registry.delete_version(subject, Wanted::parse(version)?, permanent)?;
            Ok(written(changes, ok(json!(version))))
        }
        ("GET", ["schemas", "types"]) => answer(json!(SchemaType::ALL.map(SchemaType::name))),
        ("GET", ["schemas", "ids", id])
        | ("GET", ["schemas", "ids", id, "schema" | "versions" | "subjects"]) => {
            let Ok(id) = id.parse::<i32>() else {
                return refused(code::SCHEMA_NOT_FOUND, format!("Schema {id} not found"));
            };
            let schema = registry.schema(id)?;
            // A schema is its subjects': seen by whoever may read one of them (or, where no subject lists it
            // any more, by whoever may read the registry as a whole).
            let uses: Vec<(String, i32)> = registry.uses(id)?;
            let seen = if uses.is_empty() {
                access.may(None, Act::Read)
            } else {
                uses.iter().any(|(subject, _)| readable(subject))
            };
            if !seen {
                return denied(None, Act::Read);
            }
            let uses = uses.into_iter().filter(|(subject, _)| readable(subject));
            match parts.get(3).copied() {
                None => {
                    let mut out = json!({ "schema": schema });
                    typed(&mut out, registry.kind(id).unwrap_or(SchemaType::Avro));
                    referring(&mut out, registry.references(id));
                    answer(out)
                }
                Some("schema") => Ok(Outcome::Answer(Response {
                    status: 200,
                    body: schema.to_owned(),
                })),
                Some("versions") => answer(json!(
                    uses.map(|(subject, version)| json!({ "subject": subject, "version": version }))
                        .collect::<Vec<Value>>()
                )),
                _ => {
                    let mut subjects: Vec<String> = uses.map(|(s, _)| s).collect();
                    subjects.dedup();
                    answer(json!(subjects))
                }
            }
        }
        ("POST", ["compatibility", "subjects", subject, "versions"])
        | ("POST", ["compatibility", "subjects", subject, "versions", _]) => {
            let against = parts.get(4).map(|v| Wanted::parse(v)).transpose()?;
            let asked = schema_of(body)?;
            let problems = registry.incompatibilities_with(
                subject,
                &asked.schema,
                asked.kind,
                &asked.references,
                against,
            )?;
            let mut out = json!({ "is_compatible": problems.is_empty() });
            if flag(query, "verbose")
                && let Some(map) = out.as_object_mut()
            {
                map.insert("messages".to_owned(), json!(problems));
            }
            answer(out)
        }
        ("GET", ["config"]) => answer(json!({ "compatibilityLevel": registry.level().name() })),
        ("PUT", ["config"]) => {
            let level = level_of(body)?;
            let changes = registry.set_level(None, Some(level))?;
            Ok(written(changes, ok(json!({ "compatibility": level.name() }))))
        }
        ("GET", ["config", subject]) => match registry.own_level(subject) {
            Some(level) => answer(json!({ "compatibilityLevel": level.name() })),
            None if flag(query, "defaultToGlobal") => {
                answer(json!({ "compatibilityLevel": registry.level().name() }))
            }
            None => not_set(subject),
        },
        ("PUT", ["config", subject]) => {
            let level = level_of(body)?;
            let changes = registry.set_level(Some(subject), Some(level))?;
            Ok(written(changes, ok(json!({ "compatibility": level.name() }))))
        }
        ("DELETE", ["config", subject]) => match registry.own_level(subject) {
            Some(level) => {
                let changes = registry.set_level(Some(subject), None)?;
                Ok(written(
                    changes,
                    ok(json!({ "compatibilityLevel": level.name() })),
                ))
            }
            None => not_set(subject),
        },
        ("GET", ["mode"]) => answer(json!({ "mode": registry.mode().name() })),
        ("PUT", ["mode"]) => {
            let json: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let name = json.get("mode").and_then(Value::as_str).unwrap_or_default();
            let Some(mode) = Mode::parse(name) else {
                return refused(
                    code::INVALID_MODE,
                    format!("Invalid mode {name:?}. Valid values are READWRITE and READONLY"),
                );
            };
            Ok(written(
                registry.set_mode(mode),
                ok(json!({ "mode": mode.name() })),
            ))
        }
        (_, ["subjects" | "schemas" | "compatibility" | "config" | "mode", ..]) | (_, []) => {
            Ok(Outcome::Answer(error(405, 405, "HTTP 405 Method Not Allowed")))
        }
        _ => Ok(Outcome::Answer(error(404, 404, "HTTP 404 Not Found"))),
    }
}

fn not_set(subject: &str) -> Result<Outcome, Refused> {
    refused(
        code::SUBJECT_COMPATIBILITY_NOT_SET,
        format!("Subject '{subject}' does not have subject-level compatibility configured"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::tests::{BREAKING, V1, V2};

    /// One request against a registry held in memory: what is to be written is applied at once.
    fn call(r: &mut Registry, method: &str, target: &str, body: Value) -> (u16, Value) {
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let body = if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        };
        let request = Request {
            method,
            path,
            query,
            body: &body,
        };
        let response = match handle(r, &request) {
            Outcome::Answer(response) => response,
            Outcome::Write { changes, then } => {
                for change in &changes {
                    r.apply(change).unwrap();
                }
                then
            }
        };
        (response.status, serde_json::from_str(&response.body).unwrap())
    }

    fn schema(text: &str) -> Value {
        json!({ "schema": text })
    }

    #[test]
    fn a_serializers_day_register_read_back_by_id_and_evolve() {
        let mut r = Registry::default();
        assert_eq!(call(&mut r, "GET", "/subjects", Value::Null), (200, json!([])));
        // The first message of a topic: the serializer registers its schema and gets the id to write.
        assert_eq!(
            call(&mut r, "POST", "/subjects/orders-value/versions", schema(V1)),
            (200, json!({ "id": 1 }))
        );
        // A consumer meets id 1 in a message and asks what it is.
        assert_eq!(
            call(&mut r, "GET", "/schemas/ids/1", Value::Null),
            (200, schema(V1))
        );
        // The schema grows a field with a default: taken. One without: refused, with the reason.
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/compatibility/subjects/orders-value/versions/latest",
                schema(V2)
            ),
            (200, json!({ "is_compatible": true }))
        );
        assert_eq!(
            call(&mut r, "POST", "/subjects/orders-value/versions", schema(V2)),
            (200, json!({ "id": 2 }))
        );
        let (status, body) = call(
            &mut r,
            "POST",
            "/subjects/orders-value/versions",
            schema(BREAKING),
        );
        assert_eq!((status, &body["error_code"]), (409, &json!(409)));
        assert!(
            body["message"].as_str().unwrap().contains("field total"),
            "{body}"
        );
        let (_, verbose) = call(
            &mut r,
            "POST",
            "/compatibility/subjects/orders-value/versions?verbose=true",
            schema(BREAKING),
        );
        assert_eq!(verbose["is_compatible"], json!(false));
        assert_eq!(verbose["messages"].as_array().unwrap().len(), 1);

        assert_eq!(
            call(&mut r, "GET", "/subjects", Value::Null).1,
            json!(["orders-value"])
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects/orders-value/versions", Value::Null).1,
            json!([1, 2])
        );
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/orders-value/versions/latest",
                Value::Null
            )
            .1,
            json!({ "subject": "orders-value", "version": 2, "id": 2, "schema": V2 })
        );
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/orders-value/versions/1/schema",
                Value::Null
            )
            .1,
            serde_json::from_str::<Value>(V1).unwrap(),
            "the schema itself"
        );
        assert_eq!(
            call(&mut r, "POST", "/subjects/orders-value", schema(V1)).1["version"],
            json!(1)
        );
        assert_eq!(
            call(&mut r, "GET", "/schemas/ids/2/versions", Value::Null).1,
            json!([{ "subject": "orders-value", "version": 2 }])
        );
        assert_eq!(
            call(&mut r, "GET", "/schemas/ids/2/subjects", Value::Null).1,
            json!(["orders-value"])
        );
        assert_eq!(
            call(&mut r, "GET", "/schemas/types", Value::Null).1,
            json!(["AVRO", "JSON", "PROTOBUF"])
        );
    }

    #[test]
    fn refusals_carry_the_status_and_code_a_client_expects() {
        let mut r = Registry::default();
        call(&mut r, "POST", "/subjects/s/versions", schema(V1));
        for (method, target, body, status, error_code) in [
            ("GET", "/subjects/nowhere/versions", Value::Null, 404, 40401),
            ("GET", "/subjects/s/versions/7", Value::Null, 404, 40402),
            ("GET", "/subjects/s/versions/zero", Value::Null, 422, 42202),
            ("GET", "/schemas/ids/9", Value::Null, 404, 40403),
            ("POST", "/subjects/s", schema(V2), 404, 40403),
            (
                "POST",
                "/subjects/s/versions",
                schema("{\"type\":\"recrd\"}"),
                422,
                42201,
            ),
            ("POST", "/subjects/s/versions", json!({}), 422, 42201),
            (
                "POST",
                "/subjects/s/versions",
                json!({ "schema": "<a/>", "schemaType": "XSD" }),
                422,
                42299,
            ),
            (
                "POST",
                "/subjects/p/versions",
                json!({ "schema": "syntax = \"proto3\";", "schemaType": "PROTOBUF" }),
                422,
                42201,
            ),
            (
                "POST",
                "/subjects/j/versions",
                json!({ "schema": "{\"type\":\"text\"}", "schemaType": "JSON" }),
                422,
                42201,
            ),
            (
                "POST",
                "/subjects/s/versions",
                json!({ "schema": V1, "references": [{ "name": "a", "subject": "b", "version": 1 }] }),
                422,
                42201,
            ),
            (
                "PUT",
                "/config",
                json!({ "compatibility": "SOMEWHAT" }),
                422,
                42203,
            ),
            ("GET", "/config/s", Value::Null, 404, 40408),
            ("PUT", "/mode", json!({ "mode": "IMPORT" }), 422, 42204),
            ("DELETE", "/subjects/s?permanent=true", Value::Null, 404, 40405),
            ("GET", "/nothing/here", Value::Null, 404, 404),
            ("GET", "/subjects/%zz/versions", Value::Null, 404, 404),
            ("PATCH", "/subjects", Value::Null, 405, 405),
        ] {
            let (got, answer) = call(&mut r, method, target, body);
            assert_eq!(
                (got, &answer["error_code"]),
                (status, &json!(error_code)),
                "{method} {target}: {answer}"
            );
            assert!(answer["message"].as_str().is_some_and(|m| !m.is_empty()));
        }
    }

    #[test]
    fn levels_modes_deletes_and_names_that_need_encoding() {
        let mut r = Registry::default();
        assert_eq!(
            call(&mut r, "GET", "/config", Value::Null).1,
            json!({ "compatibilityLevel": "BACKWARD" })
        );
        assert_eq!(
            call(&mut r, "PUT", "/config/s", json!({ "compatibility": "NONE" })),
            (200, json!({ "compatibility": "NONE" }))
        );
        assert_eq!(
            call(&mut r, "GET", "/config/s", Value::Null).1,
            json!({ "compatibilityLevel": "NONE" })
        );
        call(&mut r, "POST", "/subjects/s/versions", schema(V1));
        assert_eq!(
            call(&mut r, "POST", "/subjects/s/versions", schema(BREAKING)).0,
            200,
            "NONE takes it"
        );
        assert_eq!(
            call(&mut r, "DELETE", "/config/s", Value::Null).1,
            json!({ "compatibilityLevel": "NONE" })
        );
        assert_eq!(
            call(&mut r, "GET", "/config/s?defaultToGlobal=true", Value::Null).1,
            json!({ "compatibilityLevel": "BACKWARD" })
        );

        // Soft, then for good.
        assert_eq!(
            call(&mut r, "DELETE", "/subjects/s/versions/1", Value::Null),
            (200, json!(1))
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects/s/versions", Value::Null).1,
            json!([2])
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects/s/versions?deleted=true", Value::Null).1,
            json!([1, 2])
        );
        assert_eq!(
            call(&mut r, "DELETE", "/subjects/s", Value::Null),
            (200, json!([2]))
        );
        assert_eq!(call(&mut r, "GET", "/subjects", Value::Null).1, json!([]));
        assert_eq!(
            call(&mut r, "GET", "/subjects?deleted=true", Value::Null).1,
            json!(["s"])
        );
        assert_eq!(
            call(&mut r, "DELETE", "/subjects/s?permanent=true", Value::Null),
            (200, json!([1, 2]))
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects?deleted=true", Value::Null).1,
            json!([])
        );

        // A read-only registry answers and takes nothing, until it is opened again.
        assert_eq!(
            call(&mut r, "PUT", "/mode", json!({ "mode": "READONLY" })).1,
            json!({ "mode": "READONLY" })
        );
        assert_eq!(
            call(&mut r, "POST", "/subjects/s/versions", schema(V1)).1["error_code"],
            json!(42205)
        );
        assert_eq!(
            call(&mut r, "GET", "/mode", Value::Null).1,
            json!({ "mode": "READONLY" })
        );
        call(&mut r, "PUT", "/mode", json!({ "mode": "READWRITE" }));

        // A subject with a slash and a space in its name, as a client encodes it.
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/team%2Forders%20v2/versions",
                schema(V1)
            )
            .0,
            200
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects", Value::Null).1,
            json!(["team/orders v2"])
        );
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/team%2Forders%20v2/versions/1",
                Value::Null
            )
            .1["subject"],
            json!("team/orders v2")
        );
    }

    #[test]
    fn json_schema_and_protobuf_subjects_are_registered_checked_and_given_back_with_their_type() {
        let mut r = Registry::default();
        // JSON Schema: a closed object gains an optional property; then tries to require one.
        let json_v1 =
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}"#;
        let json_v2 = r#"{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}},"additionalProperties":false}"#;
        let json_bad = r#"{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}},"required":["id"],"additionalProperties":false}"#;
        let typed = |schema: &str, kind: &str| json!({ "schema": schema, "schemaType": kind });
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/clicks-value/versions",
                typed(json_v1, "JSON")
            ),
            (200, json!({ "id": 1 }))
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/clicks-value/versions",
                typed(json_v2, "JSON")
            ),
            (200, json!({ "id": 2 }))
        );
        let (status, why) = call(
            &mut r,
            "POST",
            "/subjects/clicks-value/versions",
            typed(json_bad, "JSON"),
        );
        assert_eq!(status, 409, "{why}");
        assert!(why["message"].as_str().unwrap().contains("now required"), "{why}");
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/clicks-value/versions/latest",
                Value::Null
            )
            .1,
            json!({ "subject": "clicks-value", "version": 2, "id": 2, "schema": json_v2, "schemaType": "JSON" })
        );
        assert_eq!(
            call(&mut r, "GET", "/schemas/ids/1", Value::Null).1,
            json!({ "schema": json_v1, "schemaType": "JSON" })
        );
        assert_eq!(
            call(&mut r, "POST", "/subjects/clicks-value", typed(json_v1, "JSON")).1["version"],
            json!(1)
        );

        // Protobuf: a field added is taken; one whose type changes on the wire is refused.
        let proto_v1 = "syntax = \"proto3\"; message Click { int64 id = 1; string page = 2; }";
        let proto_v2 =
            "syntax = \"proto3\"; message Click { int64 id = 1; string page = 2; string user = 3; }";
        let proto_bad = "syntax = \"proto3\"; message Click { string id = 1; string page = 2; }";
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/views-value/versions",
                typed(proto_v1, "PROTOBUF")
            ),
            (200, json!({ "id": 3 }))
        );
        // The same file laid out differently is the same schema.
        let respaced =
            "syntax = \"proto3\";\n// clicks\nmessage Click {\n  int64 id = 1;\n  string page = 2;\n}\n";
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/views-value/versions",
                typed(respaced, "PROTOBUF")
            ),
            (200, json!({ "id": 3 }))
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/views-value/versions",
                typed(proto_v2, "PROTOBUF")
            ),
            (200, json!({ "id": 4 }))
        );
        let (status, why) = call(
            &mut r,
            "POST",
            "/subjects/views-value/versions",
            typed(proto_bad, "PROTOBUF"),
        );
        assert_eq!(status, 409, "{why}");
        assert!(
            why["message"]
                .as_str()
                .unwrap()
                .contains("int64 is not written as string is"),
            "{why}"
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/compatibility/subjects/views-value/versions/latest",
                typed(proto_bad, "PROTOBUF")
            )
            .1,
            json!({ "is_compatible": false })
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects/views-value/versions/1", Value::Null).1["schemaType"],
            json!("PROTOBUF")
        );

        // A subject does not change kind under a level that checks; and an Avro answer says no type, as before.
        let (status, why) = call(&mut r, "POST", "/subjects/views-value/versions", schema(V1));
        assert_eq!(status, 409, "{why}");
        assert!(why["message"].as_str().unwrap().contains("type changed"), "{why}");
        call(&mut r, "POST", "/subjects/orders-value/versions", schema(V1));
        assert!(
            call(&mut r, "GET", "/subjects/orders-value/versions/1", Value::Null)
                .1
                .get("schemaType")
                .is_none()
        );
        // The same text as two kinds is two schemas.
        call(&mut r, "PUT", "/config", json!({ "compatibility": "NONE" }));
        let (_, as_json) = call(
            &mut r,
            "POST",
            "/subjects/a/versions",
            typed("{\"type\":\"string\"}", "JSON"),
        );
        let (_, as_avro) = call(
            &mut r,
            "POST",
            "/subjects/b/versions",
            schema("{\"type\":\"string\"}"),
        );
        assert_ne!(as_json["id"], as_avro["id"]);
    }

    #[test]
    fn a_schema_may_use_another_subjects_which_then_cannot_be_deleted() {
        let mut r = Registry::default();
        let with = |text: &str, kind: &str, name: &str, subject: &str| {
            json!({
                "schema": text,
                "schemaType": kind,
                "references": [{ "name": name, "subject": subject, "version": 1 }]
            })
        };
        // Avro: an order priced in the Money another subject defines.
        let money = r#"{"type":"record","name":"Money","namespace":"shop","fields":[{"name":"amount","type":"long"}]}"#;
        let order = r#"{"type":"record","name":"Order","namespace":"shop","fields":[{"name":"total","type":"shop.Money"}]}"#;
        assert_eq!(
            call(&mut r, "POST", "/subjects/money/versions", schema(money)),
            (200, json!({ "id": 1 }))
        );
        let (status, why) = call(&mut r, "POST", "/subjects/orders/versions", schema(order));
        assert_eq!(
            (status, &why["error_code"]),
            (422, &json!(42201)),
            "without the reference: {why}"
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/orders/versions",
                with(order, "AVRO", "shop.Money", "money")
            ),
            (200, json!({ "id": 2 }))
        );
        assert_eq!(
            call(&mut r, "GET", "/subjects/orders/versions/1", Value::Null).1["references"],
            json!([{ "name": "shop.Money", "subject": "money", "version": 1 }])
        );
        assert_eq!(
            call(&mut r, "GET", "/schemas/ids/2", Value::Null).1["references"][0]["subject"],
            json!("money")
        );
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/money/versions/1/referencedby",
                Value::Null
            )
            .1,
            json!([2])
        );
        assert_eq!(
            call(
                &mut r,
                "GET",
                "/subjects/orders/versions/1/referencedby",
                Value::Null
            )
            .1,
            json!([])
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/orders",
                with(order, "AVRO", "shop.Money", "money")
            )
            .1["version"],
            json!(1)
        );
        // What others use stays: neither the version nor its subject can be deleted.
        for target in ["/subjects/money/versions/1", "/subjects/money"] {
            let (status, why) = call(&mut r, "DELETE", target, Value::Null);
            assert_eq!(
                (status, &why["error_code"]),
                (422, &json!(42206)),
                "{target}: {why}"
            );
        }
        // A reference to nothing, or to a schema of another kind, is refused.
        for (subject, kind) in [("nowhere", "AVRO"), ("money", "JSON")] {
            let (status, why) = call(
                &mut r,
                "POST",
                "/subjects/x/versions",
                with("{}", kind, "a", subject),
            );
            assert_eq!(
                (status, &why["error_code"]),
                (422, &json!(42201)),
                "{subject} {kind}: {why}"
            );
        }

        // JSON Schema: a $ref to another subject's schema, by the name the reference gives it.
        let address =
            r#"{"type":"object","properties":{"city":{"type":"string"}},"additionalProperties":false}"#;
        let person =
            r#"{"type":"object","properties":{"home":{"$ref":"address.json"}},"additionalProperties":false}"#;
        call(
            &mut r,
            "POST",
            "/subjects/address/versions",
            json!({ "schema": address, "schemaType": "JSON" }),
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/people/versions",
                with(person, "JSON", "address.json", "address")
            )
            .0,
            200
        );
        // Protobuf: an import of another subject's file.
        let common = "syntax = \"proto3\"; package common; message Money { int64 amount = 1; }";
        let invoice =
            "syntax = \"proto3\"; import \"common.proto\"; message Invoice { common.Money total = 1; }";
        call(
            &mut r,
            "POST",
            "/subjects/common/versions",
            json!({ "schema": common, "schemaType": "PROTOBUF" }),
        );
        assert_eq!(
            call(
                &mut r,
                "POST",
                "/subjects/invoices/versions",
                with(invoice, "PROTOBUF", "common.proto", "common")
            )
            .0,
            200
        );
        let (status, why) = call(
            &mut r,
            "POST",
            "/subjects/invoices2/versions",
            json!({ "schema": invoice, "schemaType": "PROTOBUF" }),
        );
        assert_eq!(status, 422, "an import that is not among the references: {why}");

        // Once nothing refers to it any more, it can go.
        call(&mut r, "DELETE", "/subjects/orders", Value::Null);
        assert_eq!(
            call(&mut r, "DELETE", "/subjects/money", Value::Null).0,
            422,
            "a soft-deleted schema still uses it"
        );
        call(&mut r, "DELETE", "/subjects/orders?permanent=true", Value::Null);
        assert_eq!(
            call(&mut r, "DELETE", "/subjects/money", Value::Null),
            (200, json!([1]))
        );
    }

    /// A caller who may read every subject starting `team-a.`, and register under those too.
    struct TeamA;

    impl Access for TeamA {
        fn may(&self, subject: Option<&str>, act: Act) -> bool {
            subject.is_some_and(|s| s.starts_with("team-a.")) && matches!(act, Act::Read | Act::Write)
        }
    }

    #[test]
    fn a_caller_sees_and_changes_only_what_it_may() {
        let mut r = Registry::default();
        for subject in ["team-a.orders", "team-b.orders"] {
            assert_eq!(
                call(
                    &mut r,
                    "POST",
                    &format!("/subjects/{subject}/versions"),
                    schema(V1)
                )
                .0,
                200
            );
        }
        call(
            &mut r,
            "POST",
            "/subjects/team-b.only/versions",
            schema(r#""long""#),
        );
        let as_team_a = |r: &Registry, method: &str, target: &str, body: Value| -> (u16, Value) {
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            let body = if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            };
            let request = Request {
                method,
                path,
                query,
                body: &body,
            };
            let response = match handle_as(r, &request, &TeamA) {
                Outcome::Answer(response) | Outcome::Write { then: response, .. } => response,
            };
            (response.status, serde_json::from_str(&response.body).unwrap())
        };
        // What it may not read is not listed, and refused when asked for.
        assert_eq!(
            as_team_a(&r, "GET", "/subjects", Value::Null).1,
            json!(["team-a.orders"])
        );
        assert_eq!(
            as_team_a(&r, "GET", "/subjects/team-a.orders/versions", Value::Null).1,
            json!([1])
        );
        for (method, target, body) in [
            ("GET", "/subjects/team-b.orders/versions", Value::Null),
            ("GET", "/subjects/team-b.orders/versions/1", Value::Null),
            ("POST", "/subjects/team-b.orders/versions", schema(V2)),
            ("POST", "/subjects/team-b.orders", schema(V1)),
            (
                "POST",
                "/compatibility/subjects/team-b.orders/versions",
                schema(V2),
            ),
            ("DELETE", "/subjects/team-a.orders", Value::Null),
            ("GET", "/config", Value::Null),
            ("PUT", "/config", json!({ "compatibility": "NONE" })),
            ("PUT", "/config/team-a.orders", json!({ "compatibility": "NONE" })),
            ("PUT", "/mode", json!({ "mode": "READONLY" })),
            // A schema only another team's subject holds.
            ("GET", "/schemas/ids/2", Value::Null),
        ] {
            let (status, why) = as_team_a(&r, method, target, body);
            assert_eq!(
                (status, &why["error_code"]),
                (403, &json!(40301)),
                "{method} {target}: {why}"
            );
        }
        // A schema both teams hold is seen through the one it may read - and only that use of it is listed.
        assert_eq!(as_team_a(&r, "GET", "/schemas/ids/1", Value::Null).0, 200);
        assert_eq!(
            as_team_a(&r, "GET", "/schemas/ids/1/versions", Value::Null).1,
            json!([{ "subject": "team-a.orders", "version": 1 }])
        );
        assert_eq!(
            as_team_a(&r, "GET", "/schemas/ids/1/subjects", Value::Null).1,
            json!(["team-a.orders"])
        );
        // It registers under its own subjects, and may not take in a schema it cannot read.
        assert_eq!(
            as_team_a(&r, "POST", "/subjects/team-a.orders/versions", schema(V2)).0,
            200
        );
        let borrowing = json!({
            "schema": V2,
            "references": [{ "name": "x", "subject": "team-b.only", "version": 1 }]
        });
        let (status, why) = as_team_a(&r, "POST", "/subjects/team-a.orders/versions", borrowing);
        assert_eq!(status, 403, "{why}");
        assert!(why["message"].as_str().unwrap().contains("team-b.only"), "{why}");
        // What needs no subject is open to any caller.
        assert_eq!(as_team_a(&r, "GET", "/schemas/types", Value::Null).0, 200);
    }

    #[test]
    fn registering_what_is_there_already_writes_nothing() {
        let mut r = Registry::default();
        let body = schema(V1).to_string().into_bytes();
        let request = Request {
            method: "POST",
            path: "/subjects/s/versions",
            query: "",
            body: &body,
        };
        let Outcome::Write { changes, then } = handle(&r, &request) else {
            panic!("the first registration is a change to write");
        };
        assert_eq!((changes.len(), then.status), (1, 200));
        for change in &changes {
            r.apply(change).unwrap();
        }
        assert!(matches!(
            handle(&r, &request),
            Outcome::Answer(Response { status: 200, .. })
        ));
    }
}
