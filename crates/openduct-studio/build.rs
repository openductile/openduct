// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Builds the studio's page into the program: every file of `ui/dist` (what `npm run build` writes), listed for
//! `src/assets.rs`. Without `ui/dist` the list is empty, and the program still builds and runs. The templates of
//! `templates/` are built in too, for `src/templates.rs`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let dist = manifest.join("../../ui/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let mut files = Vec::new();
    if dist.is_dir() {
        walk(&dist, &mut files)?;
    }
    files.sort();
    let mut out = String::from(
        "/// Every file of the studio's page: its path as the browser asks for it, and its bytes.\n",
    );
    out.push_str("pub(crate) static FILES: &[(&str, &[u8])] = &[\n");
    for file in &files {
        let relative = file.strip_prefix(&dist)?.to_string_lossy().replace('\\', "/");
        let absolute = file.canonicalize()?;
        writeln!(
            out,
            "    ({:?}, include_bytes!({:?})),",
            format!("/{relative}"),
            absolute
        )?;
    }
    out.push_str("];\n");
    std::fs::write(PathBuf::from(std::env::var("OUT_DIR")?).join("assets.rs"), out)?;

    // The templates the studio offers: every .yaml file of templates/.
    let templates = manifest.join("../../templates");
    println!("cargo:rerun-if-changed={}", templates.display());
    let mut found = Vec::new();
    if templates.is_dir() {
        walk(&templates, &mut found)?;
    }
    found.retain(|f| f.extension().is_some_and(|e| e == "yaml"));
    found.sort();
    let mut out = String::from("/// Every template: its id (the file's name) and its YAML.\n");
    out.push_str("pub(crate) static TEMPLATES: &[(&str, &str)] = &[\n");
    for file in &found {
        let id = file
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        writeln!(out, "    ({id:?}, include_str!({:?})),", file.canonicalize()?)?;
    }
    out.push_str("];\n");
    std::fs::write(PathBuf::from(std::env::var("OUT_DIR")?).join("templates.rs"), out)?;
    Ok(())
}

fn walk(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            walk(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}
