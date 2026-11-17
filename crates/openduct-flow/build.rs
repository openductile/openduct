// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Embeds the apps OpenDuct comes with: every `apps/<id>/app.yaml` of the repository.

use std::fmt::Write as _;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let apps = manifest.join("../../apps");
    println!("cargo:rerun-if-changed={}", apps.display());
    let mut found = Vec::new();
    if apps.is_dir() {
        for entry in std::fs::read_dir(&apps)? {
            let dir = entry?.path();
            let file = dir.join("app.yaml");
            if file.is_file() {
                println!("cargo:rerun-if-changed={}", file.display());
                found.push(file);
            }
        }
    }
    found.sort();
    let mut out = String::from("/// The apps OpenDuct comes with: where each is, and its definition.\n");
    out.push_str("pub(crate) static BUILT_IN: &[(&str, &str)] = &[\n");
    let root = manifest.join("../..").canonicalize()?;
    for file in &found {
        let full = file.canonicalize()?;
        let shown = full
            .strip_prefix(&root)
            .unwrap_or(&full)
            .to_string_lossy()
            .into_owned();
        writeln!(out, "    ({shown:?}, include_str!({full:?})),")?;
    }
    out.push_str("];\n");
    std::fs::write(PathBuf::from(std::env::var("OUT_DIR")?).join("apps.rs"), out)?;
    Ok(())
}
