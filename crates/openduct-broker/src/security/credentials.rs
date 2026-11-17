// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The SCRAM credentials file: per name and mechanism, a salt, an iteration count and two derived keys - never a
//! password. Written by `openduct user add` (the password read from standard input, never an argument), readable by
//! the broker's user only, re-read when it changes so a name can be added or revoked without a restart.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::scram::{ScramCredential, ScramMechanism};

const FORMAT: u32 = 1;
const RECHECK: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Users {
    pub format: u32,
    pub users: BTreeMap<String, BTreeMap<ScramMechanism, ScramCredential>>,
}

/// Names may hold letters, digits and `. _ - @ / : +`, up to 128 characters.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-@/:+".contains(c))
}

/// The credentials a broker checks, from a file re-read when it changes (or fixed, for tests and embedding).
#[derive(Debug)]
pub struct Credentials {
    file: Option<PathBuf>,
    users: Mutex<(Instant, Option<SystemTime>, Arc<Users>)>,
}

impl Credentials {
    pub fn fixed(users: Users) -> Self {
        Self {
            file: None,
            users: Mutex::new((Instant::now(), None, Arc::new(users))),
        }
    }

    pub fn from_file(path: &Path) -> Result<Self, String> {
        let (users, modified) = read(path)?;
        Ok(Self {
            file: Some(path.to_owned()),
            users: Mutex::new((Instant::now(), Some(modified), Arc::new(users))),
        })
    }

    pub(crate) fn lookup(&self, mechanism: ScramMechanism, name: &str) -> Option<ScramCredential> {
        let mut held = self.users.lock();
        if let Some(path) = &self.file
            && held.0.elapsed() >= RECHECK
        {
            held.0 = Instant::now();
            let modified = std::fs::metadata(path).and_then(|m| m.modified()).ok();
            if modified.is_some() && modified != held.1 {
                match read(path) {
                    Ok((users, m)) => {
                        held.1 = Some(m);
                        held.2 = Arc::new(users);
                        tracing::info!(file = %path.display(), "credentials re-read");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "the changed credentials file is refused; the one before stays")
                    }
                }
            }
        }
        held.2.users.get(name)?.get(&mechanism).cloned()
    }
}

fn read(path: &Path) -> Result<(Users, SystemTime), String> {
    let raw = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let modified = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let users: Users = serde_json::from_slice(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
    if users.format != FORMAT {
        return Err(format!(
            "{} is format {}; this build reads format {FORMAT}",
            path.display(),
            users.format
        ));
    }
    Ok((users, modified))
}

/// Adds or replaces `name` in the file (created if missing), with credentials for every SCRAM mechanism.
pub fn add_user(path: &Path, name: &str, password: &str, iterations: u32) -> Result<(), String> {
    if !valid_name(name) {
        return Err(format!(
            "{name:?} is not a valid name (letters, digits and . _ - @ / : +, up to 128)"
        ));
    }
    if password.len() < 16 {
        return Err("a password needs at least 16 characters".to_owned());
    }
    let mut users = match read(path) {
        Ok((u, _)) => u,
        Err(_) if !path.exists() => Users {
            format: FORMAT,
            users: BTreeMap::new(),
        },
        Err(e) => return Err(e),
    };
    let mut creds = BTreeMap::new();
    for m in ScramMechanism::ALL {
        creds.insert(m, ScramCredential::new(m, password, iterations)?);
    }
    users.users.insert(name.to_owned(), creds);
    write(path, &users)
}

/// Removes `name`; its sessions end when they next sign in.
pub fn remove_user(path: &Path, name: &str) -> Result<bool, String> {
    let (mut users, _) = read(path)?;
    let removed = users.users.remove(name).is_some();
    write(path, &users)?;
    Ok(removed)
}

fn write(path: &Path, users: &Users) -> Result<(), String> {
    let json = serde_json::to_vec_pretty(users).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut f = options
        .open(&tmp)
        .map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::io::Write::write_all(&mut f, &json).map_err(|e| e.to_string())?;
    f.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn users_are_added_and_removed_without_passwords_in_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        add_user(&path, "gateway", "correct horse battery staple", 4096).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("horse"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let c = Credentials::from_file(&path).unwrap();
        let cred = c.lookup(ScramMechanism::Sha512, "gateway").unwrap();
        assert!(cred.matches(ScramMechanism::Sha512, "correct horse battery staple"));
        assert!(add_user(&path, "bad name", "correct horse battery staple", 4096).is_err());
        assert!(add_user(&path, "short", "tiny", 4096).is_err());
        assert!(remove_user(&path, "gateway").unwrap());
    }
}
