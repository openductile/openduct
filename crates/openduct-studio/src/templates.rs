// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The templates the studio offers: the workflows of `templates/`, built into the program. Using one makes a new
//! workflow from it, off until it is turned on.

use hyper::StatusCode;
use serde_json::{Value, json};

use crate::State;
use crate::api::signed_in;
use crate::http::{Answer, Call, json};

// `TEMPLATES`: (id, YAML) for every file of templates/, made by build.rs.
include!(concat!(env!("OUT_DIR"), "/templates.rs"));

/// Every template, as `{id, name, description, category, definition}`.
pub(crate) fn all() -> Vec<Value> {
    TEMPLATES
        .iter()
        .filter_map(|(id, yaml)| {
            let definition = openduct_flow::yaml_to_json(yaml).ok()?;
            Some(json!({
                "id": id,
                "name": definition.get("name"),
                "description": definition.get("description"),
                "category": definition.get("category"),
                "definition": definition,
            }))
        })
        .collect()
}

pub(crate) async fn list(state: &State, call: &Call) -> Answer {
    if let Err(answer) = signed_in(state, call).await {
        return answer;
    }
    json(StatusCode::OK, &json!({ "templates": all() }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_template_is_a_workflow_that_reads_whole() {
        assert!(!super::TEMPLATES.is_empty(), "the templates are missing");
        for (id, yaml) in super::TEMPLATES {
            let workflow =
                openduct_flow::Workflow::parse(yaml).unwrap_or_else(|e| panic!("templates/{id}.yaml: {e}"));
            let definition = openduct_flow::yaml_to_json(yaml).unwrap_or_default();
            for field in ["description", "category"] {
                assert!(
                    definition
                        .get(field)
                        .and_then(|d| d.as_str())
                        .is_some_and(|d| !d.is_empty()),
                    "templates/{id}.yaml: no {field}"
                );
            }
            assert!(!workflow.steps.is_empty(), "templates/{id}.yaml: no steps");
        }
        assert_eq!(super::all().len(), super::TEMPLATES.len());
    }
}
