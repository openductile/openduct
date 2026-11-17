// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A studio pipeline's secrets: what it names as `secret:NAME` (or `secret:NAME.field`), from its own workspace's
//! credentials - never the environment, never a file on the server. Looked up each time it starts, and again on every
//! sync, so a credential added or replaced in Credentials takes effect without a restart of OpenDuct.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use openduct_pipeline::{Pipeline, secrets};

use crate::credentials::Vault;
use crate::model::PRODUCTION;

/// The secrets `source` names, from `workspace`'s credentials; why it cannot have them.
pub(crate) fn of(vault: &Vault, workspace: &str, source: &str) -> Result<BTreeMap<String, String>, String> {
    let p = Pipeline::parse(source)?;
    if let Some(why) = secrets::first_refused(&p, false) {
        return Err(why);
    }
    let mut out = BTreeMap::new();
    for name in secrets::named(&p) {
        let (credential, field) = match name.split_once('.') {
            Some((c, f)) => (c, Some(f)),
            None => (name.as_str(), None),
        };
        let found = vault.get(workspace, PRODUCTION, credential).ok_or_else(|| {
            format!("secret:{name}: no credential {credential} in this workspace: add it in Credentials")
        })?;
        let fields = || found.fields.keys().cloned().collect::<Vec<_>>().join(", ");
        let value = match field {
            Some(f) => found.fields.get(f).cloned().ok_or_else(|| {
                format!(
                    "secret:{name}: the credential {credential} has no field {f}: it has {}",
                    fields()
                )
            })?,
            None => {
                let mut values = found.fields.values();
                match (values.next(), values.next()) {
                    (Some(v), None) => v.clone(),
                    _ => {
                        return Err(format!(
                            "secret:{name}: the credential {credential} has several fields: write secret:{credential}.<field>, one of {}",
                            fields()
                        ));
                    }
                }
            }
        };
        out.insert(name, value);
    }
    Ok(out)
}

/// The credentials `source` names that `workspace` does not have (or a reason it may not name them at all).
pub(crate) fn missing(vault: &Vault, workspace: &str, source: &str) -> Vec<String> {
    let Ok(p) = Pipeline::parse(source) else {
        return Vec::new();
    };
    secrets::named(&p)
        .into_iter()
        .filter(|name| {
            let credential = name.split_once('.').map_or(name.as_str(), |(c, _)| c);
            vault.get(workspace, PRODUCTION, credential).is_none()
        })
        .collect()
}

/// A mark of what was looked up - the secrets, or why they could not be - that changes when they do; never the
/// secrets themselves.
pub(crate) fn mark(looked_up: &Result<BTreeMap<String, String>, String>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match looked_up {
        Ok(found) => found.hash(&mut h),
        Err(why) => why.hash(&mut h),
    }
    h.finish()
}
