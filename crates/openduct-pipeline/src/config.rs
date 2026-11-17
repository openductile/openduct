// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The pipeline file: read, checked, and every expression compiled, so a mistake is found before the first message.

use openduct_expr::Compiled;
use yaml_rust2::{Yaml, YamlLoader};

/// Where a new pipeline starts reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    Earliest,
    Latest,
}

/// How a message's value is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// The value is JSON.
    Json,
    /// The value is text, read as `value`.
    Text,
}

/// A connector's settings, as written: text, lists and nested settings. The connector reads and checks them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Setting {
    Text(String),
    List(Vec<Setting>),
    Map(Vec<(String, Setting)>),
}

/// A system a pipeline reads from (`from: http: ...`) or writes to (`to: elasticsearch: ...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connector {
    /// Which connector: `http`, `webhook`, `elasticsearch`, `s3` ...
    pub kind: String,
    /// Where it is in the file, for messages: `from.http`.
    pub path: String,
    pub settings: Vec<(String, Setting)>,
    /// The language of the expressions among its settings: the pipeline's.
    pub language: String,
}

/// A pipeline, ready to run. One side is always a topic: a pipeline reads a topic, or writes one, or both.
#[derive(Debug, Clone)]
pub struct Pipeline {
    pub name: String,
    /// The topic read - or, with a source connector, the connector's name (what `message.topic` says).
    pub from_topic: String,
    /// Messages come from this connector instead of a topic.
    pub source: Option<Connector>,
    pub group: String,
    pub start: Start,
    pub format: Format,
    /// The language its expressions are written in (`language:`; CEL unless it says otherwise).
    pub language: String,
    pub lets: Vec<(String, Compiled)>,
    pub filter: Option<Compiled>,
    pub each: Option<Compiled>,
    pub map: Option<Vec<(String, Compiled)>>,
    pub key: Option<Compiled>,
    pub headers: Vec<(String, Compiled)>,
    /// The topic written - or, with a sink connector, the connector's name.
    pub to_topic: String,
    /// Results go to this connector instead of a topic.
    pub sink: Option<Connector>,
    pub error_topic: Option<String>,
}

const SETTINGS: &[&str] = &[
    "name", "language", "from", "let", "where", "each", "map", "key", "headers", "to", "on_error",
];

/// Words CEL keeps for itself: a name worked out with `let` cannot be one of them, or no expression could read it.
const RESERVED: &[&str] = &[
    "true",
    "false",
    "null",
    "in",
    "as",
    "break",
    "const",
    "continue",
    "else",
    "for",
    "function",
    "if",
    "import",
    "let",
    "loop",
    "package",
    "namespace",
    "return",
    "var",
    "void",
    "while",
];

impl Pipeline {
    /// Read a pipeline file's text. The error says which setting is wrong and why, and shows an expression's mistake.
    pub fn parse(text: &str) -> Result<Self, String> {
        let documents =
            YamlLoader::load_from_str(text).map_err(|e| format!("the file is not valid YAML: {e}"))?;
        let [document] = documents.as_slice() else {
            return Err("a pipeline file holds one pipeline".to_owned());
        };
        let Yaml::Hash(top) = document else {
            return Err("a pipeline file is a set of settings (name:, from:, to: ...)".to_owned());
        };
        for key in top.keys() {
            let key = scalar(key);
            if !SETTINGS.contains(&key.as_str()) {
                return Err(unknown("setting", &key, SETTINGS));
            }
        }
        let get = |name: &str| top.get(&Yaml::String(name.to_owned()));

        let name = required_text(get("name"), "name")?;
        if !is_name(&name) {
            return Err(format!(
                "name: {name:?} must be letters, digits, '.', '_' and '-'"
            ));
        }
        let fail = |path: &str, why: String| format!("pipeline {name}: {path}: {why}");
        let language = match get("language") {
            None => openduct_expr::DEFAULT.to_owned(),
            Some(v) => {
                let l = scalar(v);
                openduct_expr::language(&l).map_err(|e| fail("language", e))?;
                l
            }
        };

        let (from, mut source) =
            side(get("from"), "from", &["topic", "group", "start", "format"]).map_err(|e| fail("from", e))?;
        if let Some(c) = &mut source {
            c.language.clone_from(&language);
        }
        let from_topic = match &source {
            Some(c) => c.kind.clone(),
            None => topic(from.get("topic"), "from.topic").map_err(|e| fail("from.topic", e))?,
        };
        let group = match from.get("group") {
            None => name.clone(),
            Some(g) => {
                let g = scalar(g);
                if !is_name(&g) {
                    return Err(fail(
                        "from.group",
                        "must be letters, digits, '.', '_' and '-'".to_owned(),
                    ));
                }
                g
            }
        };
        let start = match from.get("start").map(|y| scalar(y)).as_deref() {
            None | Some("latest") => Start::Latest,
            Some("earliest") => Start::Earliest,
            Some(other) => {
                return Err(fail(
                    "from.start",
                    format!("is earliest or latest, not {other:?}"),
                ));
            }
        };
        let format = match from.get("format").map(|y| scalar(y)).as_deref() {
            None | Some("json") => Format::Json,
            Some("text") => Format::Text,
            Some(other) => return Err(fail("from.format", format!("is json or text, not {other:?}"))),
        };

        let compile = |path: &str, source: String| {
            openduct_expr::compile(&language, &source).map_err(|e| fail(path, e))
        };
        let named = |setting: &str| -> Result<Vec<(String, Compiled)>, String> {
            match get(setting) {
                None => Ok(Vec::new()),
                Some(Yaml::Hash(fields)) => fields
                    .iter()
                    .map(|(k, v)| {
                        let field = scalar(k);
                        compile(&format!("{setting}.{field}"), scalar(v)).map(|f| (field, f))
                    })
                    .collect(),
                Some(_) => Err(fail(
                    setting,
                    "is a set of names and expressions, one per line".to_owned(),
                )),
            }
        };
        let lets = named("let")?;
        for (n, _) in &lets {
            if matches!(n.as_str(), "value" | "message" | "item")
                || RESERVED.contains(&n.as_str())
                || n.chars().next().is_none_or(|c| c.is_ascii_digit())
                || !n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(fail(
                    &format!("let.{n}"),
                    "is not a name an expression can use: letters, digits and '_', not a word CEL keeps (if, in, \
                     null ...)"
                        .to_owned(),
                ));
            }
        }
        let map = match get("map") {
            None => None,
            Some(_) => Some(named("map")?),
        };
        let headers = named("headers")?;
        let one = |setting: &str| get(setting).map(|v| compile(setting, scalar(v))).transpose();
        let filter = one("where")?;
        let each = one("each")?;
        let key = one("key")?;

        let (to, mut sink) = side(get("to"), "to", &["topic"]).map_err(|e| fail("to", e))?;
        if let Some(c) = &mut sink {
            c.language.clone_from(&language);
        }
        let to_topic = match &sink {
            Some(c) => c.kind.clone(),
            None => topic(to.get("topic"), "to.topic").map_err(|e| fail("to.topic", e))?,
        };
        if source.is_some() && sink.is_some() {
            return Err(fail(
                "to",
                "a pipeline reads or writes a topic: connect two systems with two pipelines and a topic between them"
                    .to_owned(),
            ));
        }
        let error_topic = match get("on_error") {
            None => None,
            Some(v) => {
                let on_error = section(Some(v), "on_error", &["topic"]).map_err(|e| fail("on_error", e))?;
                Some(topic(on_error.get("topic"), "on_error.topic").map_err(|e| fail("on_error.topic", e))?)
            }
        };
        if source.is_none() && sink.is_none() && to_topic == from_topic {
            return Err(fail(
                "to.topic",
                "is the topic it reads from, which would feed the pipeline its own output".to_owned(),
            ));
        }
        Ok(Self {
            name,
            from_topic,
            source,
            group,
            start,
            format,
            language,
            lets,
            filter,
            each,
            map,
            key,
            headers,
            to_topic,
            sink,
            error_topic,
        })
    }
}

/// A YAML scalar as text, as an expression reads it: `42` stays `42`, `true` stays `true`.
fn scalar(yaml: &Yaml) -> String {
    match yaml {
        Yaml::String(s) | Yaml::Real(s) => s.clone(),
        Yaml::Integer(i) => i.to_string(),
        Yaml::Boolean(b) => b.to_string(),
        Yaml::Null => "null".to_owned(),
        _ => String::new(),
    }
}

fn required_text(yaml: Option<&Yaml>, setting: &str) -> Result<String, String> {
    match yaml {
        Some(v @ (Yaml::String(_) | Yaml::Integer(_) | Yaml::Real(_))) => Ok(scalar(v)),
        Some(_) => Err(format!("{setting}: must be a single value")),
        None => Err(format!("{setting}: is required")),
    }
}

fn section<'a>(
    yaml: Option<&'a Yaml>,
    setting: &str,
    allowed: &[&str],
) -> Result<std::collections::BTreeMap<String, &'a Yaml>, String> {
    let Some(Yaml::Hash(fields)) = yaml else {
        return Err(if yaml.is_none() {
            "is required".to_owned()
        } else {
            format!("is a set of settings ({})", allowed.join(", "))
        });
    };
    let mut out = std::collections::BTreeMap::new();
    for (k, v) in fields {
        let k = scalar(k);
        if !allowed.contains(&k.as_str()) {
            return Err(unknown(&format!("setting of {setting}"), &k, allowed));
        }
        out.insert(k, v);
    }
    Ok(out)
}

/// `from:` or `to:`: its own settings, and the connector named there if it is not a topic.
fn side<'a>(
    yaml: Option<&'a Yaml>,
    setting: &str,
    own: &[&str],
) -> Result<(std::collections::BTreeMap<String, &'a Yaml>, Option<Connector>), String> {
    let Some(Yaml::Hash(fields)) = yaml else {
        return Err(if yaml.is_none() {
            "is required".to_owned()
        } else {
            "is a set of settings".to_owned()
        });
    };
    let mut mine = std::collections::BTreeMap::new();
    let mut connector = None;
    for (k, v) in fields {
        let k = scalar(k);
        if own.contains(&k.as_str()) {
            mine.insert(k, v);
        } else if connector.is_some() {
            return Err("names two connectors; a pipeline reads from one and writes to one".to_owned());
        } else {
            let path = format!("{setting}.{k}");
            let settings = match v {
                Yaml::Hash(_) => match to_setting(v) {
                    Setting::Map(m) => m,
                    _ => Vec::new(),
                },
                Yaml::Null => Vec::new(),
                _ => return Err(format!("{k} is a set of the connector's settings")),
            };
            connector = Some(Connector {
                kind: k,
                path,
                settings,
                language: openduct_expr::DEFAULT.to_owned(),
            });
        }
    }
    if connector.is_some() && mine.contains_key("topic") {
        return Err("names a topic and a connector; it is one or the other".to_owned());
    }
    if connector.is_none() && !mine.contains_key("topic") {
        return Err("needs a topic, or a connector".to_owned());
    }
    Ok((mine, connector))
}

fn to_setting(yaml: &Yaml) -> Setting {
    match yaml {
        Yaml::Array(items) => Setting::List(items.iter().map(to_setting).collect()),
        Yaml::Hash(fields) => Setting::Map(fields.iter().map(|(k, v)| (scalar(k), to_setting(v))).collect()),
        other => Setting::Text(scalar(other)),
    }
}

fn topic(yaml: Option<&&Yaml>, setting: &str) -> Result<String, String> {
    let name = required_text(yaml.copied(), setting)
        .map_err(|e| e.trim_start_matches(&format!("{setting}: ")).to_owned())?;
    if !is_name(&name) || name.len() > 249 || name == "." || name == ".." {
        return Err(format!(
            "{name:?} is not a valid topic name: 1 to 249 letters, digits, '.', '_' and '-'"
        ));
    }
    Ok(name)
}

fn is_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

fn unknown(what: &str, given: &str, allowed: &[&str]) -> String {
    let near = allowed
        .iter()
        .find(|a| a.starts_with(given.get(..2).unwrap_or(given)) || given.starts_with(**a));
    match near {
        Some(n) => format!("{given:?} is not a {what} (did you mean {n:?}?)"),
        None => format!(
            "{given:?} is not a {what}: the {what}s are {}",
            allowed.join(", ")
        ),
    }
}
