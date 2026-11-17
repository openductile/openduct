// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A secrets file for `openduct pipeline run --secrets-file`: one `NAME=value` a line (blank lines and `#` comments
//! skipped), what a pipeline names as `secret:NAME`. A value in double quotes may hold `\n` (a new line, as in a
//! private key), `\"` and `\\`. Only its owner may read it - OpenDuct refuses one that others
//! can, as ssh does a key - so a secret is never in the environment, the shell's history, or a file anyone can read.

use std::collections::BTreeMap;
use std::path::Path;

/// The secrets in `path`, by name.
pub(crate) fn read(path: &Path) -> Result<BTreeMap<String, String>, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|e| format!("cannot read the secrets file {}: {e}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "the secrets file {} can be read by others: make it yours alone (chmod 600 {})",
                path.display(),
                path.display()
            ));
        }
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read the secrets file {}: {e}", path.display()))?;
    parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn parse(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            // The line is not shown: it may hold a secret.
            return Err(format!("line {}: is not NAME=value", n + 1));
        };
        let name = name.trim();
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return Err(format!(
                "line {}: a name is letters, digits, '_', '-' and '.'",
                n + 1
            ));
        }
        let value = value.trim();
        let value = match value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
            Some(quoted) => {
                unquote(quoted).ok_or_else(|| format!("line {}: a quoted value ends in a lone \\", n + 1))?
            }
            None => value.to_owned(),
        };
        if out.insert(name.to_owned(), value).is_some() {
            return Err(format!("line {}: {name} is given twice", n + 1));
        }
    }
    Ok(out)
}

fn unquote(quoted: &str) -> Option<String> {
    let mut out = String::with_capacity(quoted.len());
    let mut chars = quoted.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            'n' => out.push('\n'),
            other => out.push(other),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn a_secrets_file_is_names_and_values_and_its_owners_alone() {
        let got = parse("# the shop\nSHOP_TOKEN = abc=def\n\nwarehouse.password=p4ss\n").unwrap();
        assert_eq!(got.get("SHOP_TOKEN").map(String::as_str), Some("abc=def"));
        assert_eq!(got.get("warehouse.password").map(String::as_str), Some("p4ss"));
        let wrong = parse("SHOP_TOKEN abc").unwrap_err();
        assert!(wrong.contains("line 1") && !wrong.contains("abc"), "{wrong}");
        assert!(parse("A=1\nA=2").unwrap_err().contains("given twice"));
        let key = parse("KEY=\"-----BEGIN-----\\nabc\\n-----END-----\"").unwrap();
        assert_eq!(
            key.get("KEY").map(String::as_str),
            Some("-----BEGIN-----\nabc\n-----END-----")
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.env");
        std::fs::write(&path, "A=1\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(read(&path).unwrap_err().contains("chmod 600"));
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(read(&path).unwrap().get("A").map(String::as_str), Some("1"));
    }
}
