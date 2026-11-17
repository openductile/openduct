// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workflow file's YAML as JSON, the one form a workflow has inside OpenDuct (the studio keeps JSON).

use serde_json::{Map, Number, Value as Json};
use yaml_rust2::{Yaml, YamlLoader};

/// The deepest a workflow file nests.
const MAX_DEPTH: usize = 64;

pub(crate) fn to_json(text: &str) -> Result<Json, String> {
    let documents =
        YamlLoader::load_from_str(text).map_err(|e| format!("the file is not valid YAML: {e}"))?;
    let [document] = documents.as_slice() else {
        return Err("a workflow file holds one workflow".to_owned());
    };
    convert(document, 0)
}

fn convert(yaml: &Yaml, depth: usize) -> Result<Json, String> {
    if depth > MAX_DEPTH {
        return Err(format!("nested more than {MAX_DEPTH} deep"));
    }
    Ok(match yaml {
        Yaml::Null => Json::Null,
        Yaml::Boolean(b) => Json::Bool(*b),
        Yaml::Integer(i) => Json::Number((*i).into()),
        Yaml::Real(r) => r
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map_or_else(|| Json::String(r.clone()), Json::Number),
        Yaml::String(s) => Json::String(s.clone()),
        Yaml::Array(items) => Json::Array(
            items
                .iter()
                .map(|i| convert(i, depth + 1))
                .collect::<Result<_, _>>()?,
        ),
        Yaml::Hash(fields) => {
            let mut out = Map::new();
            for (k, v) in fields {
                let key = match k {
                    Yaml::String(s) | Yaml::Real(s) => s.clone(),
                    Yaml::Integer(i) => i.to_string(),
                    Yaml::Boolean(b) => b.to_string(),
                    _ => return Err("a setting's name is text".to_owned()),
                };
                out.insert(key, convert(v, depth + 1)?);
            }
            Json::Object(out)
        }
        Yaml::Alias(_) | Yaml::BadValue => {
            return Err("YAML aliases are not used in a workflow file".to_owned());
        }
    })
}
