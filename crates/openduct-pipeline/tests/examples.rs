// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The pipeline examples (examples/pipelines/*): each pipeline file over its sample input must give exactly the
//! expected output, line for line - what the examples promise is what `openduct pipeline test` prints.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::Path;

use openduct_pipeline::{Pipeline, test_lines};

#[test]
fn every_example_gives_its_expected_output() {
    let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/pipelines"));
    let mut examples: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_dir())
        .collect();
    examples.sort();
    assert!(examples.len() >= 6, "the examples are missing");
    let mut differences = Vec::new();
    for dir in &examples {
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let pipeline = Pipeline::parse(&std::fs::read_to_string(dir.join("pipeline.yaml")).unwrap())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let input = ["input.jsonl", "input.txt"]
            .iter()
            .map(|f| dir.join(f))
            .find(|p| p.exists())
            .unwrap_or_else(|| panic!("{name} has no input"));
        let got = test_lines(&pipeline, &std::fs::read_to_string(input).unwrap());
        let expected: Vec<String> = std::fs::read_to_string(dir.join("expected.jsonl"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        if got != expected {
            differences.push(format!(
                "{name}:\n  got:\n    {}\n  expected:\n    {}",
                got.join("\n    "),
                expected.join("\n    ")
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n\n"));
}
