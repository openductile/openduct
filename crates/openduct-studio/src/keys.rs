// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The studio's keys when OpenDuct runs as a cluster: the one that signs sessions and the one that seals credentials.
//! Every node must hold the same ones - a session made by one node is checked by another, a credential sealed by one
//! is opened by another - so they are kept in a topic of the cluster's, each sealed with a key derived from the
//! cluster secret (HKDF-SHA256): the brokers' disks never hold them in the clear. The first one written is the one
//! every node uses, however many start at once. A node that ran on its own before brings its keys (its key files), so
//! what it sealed still opens.

use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::hkdf::{HKDF_SHA256, Salt};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};

use crate::records::Records;

/// The topic the sealed keys are kept in.
pub(crate) const TOPIC: &str = "__openduct.model.keys";

/// The keys every node of the cluster uses.
pub(crate) struct Shared {
    pub(crate) session: Vec<u8>,
    pub(crate) credentials: Vec<u8>,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Shared(..)")
    }
}

/// The key that seals the others, from the cluster secret.
fn wrapping(secret: &[u8]) -> Result<LessSafeKey, String> {
    let prk = Salt::new(HKDF_SHA256, b"openduct studio keys").extract(secret);
    let okm = prk
        .expand(&[b"sealing the studio's keys"], &AES_256_GCM)
        .map_err(|_| "cannot derive a key from the cluster secret")?;
    Ok(LessSafeKey::new(UnboundKey::from(okm)))
}

fn seal(wrap: &LessSafeKey, name: &str, key: &[u8]) -> Result<Value, String> {
    let mut nonce = [0u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| "the system gave no random bytes")?;
    let mut data = key.to_vec();
    wrap.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(format!("openduct studio key/{name}")),
        &mut data,
    )
    .map_err(|_| "cannot seal a key")?;
    let mut sealed = nonce.to_vec();
    sealed.extend_from_slice(&data);
    Ok(json!({ "schema": 1, "sealed": B64.encode(sealed) }))
}

fn open(wrap: &LessSafeKey, name: &str, record: &Value) -> Result<Vec<u8>, String> {
    let sealed = record
        .get("sealed")
        .and_then(Value::as_str)
        .and_then(|s| B64.decode(s).ok())
        .filter(|s| s.len() > NONCE_LEN)
        .ok_or_else(|| format!("the cluster's {name} key is not one OpenDuct sealed"))?;
    let (nonce, data) = sealed.split_at(NONCE_LEN);
    let nonce = Nonce::try_assume_unique_for_key(nonce).map_err(|_| "not a nonce")?;
    let mut data = data.to_vec();
    let key = wrap
        .open_in_place(nonce, Aad::from(format!("openduct studio key/{name}")), &mut data)
        .map_err(|_| {
            format!(
                "the cluster's {name} key was sealed with another cluster secret: give this node the secret the \
                 others have"
            )
        })?;
    if key.len() != 32 {
        return Err(format!("the cluster's {name} key is not 32 bytes"));
    }
    Ok(key.to_vec())
}

/// The cluster's keys: read, or made (this node's own key files offered first) and read back.
pub(crate) async fn shared(broker: &str, secret: &[u8], local: Option<&Path>) -> Result<Shared, String> {
    let wrap = wrapping(secret)?;
    let records = Records::open_keeping_first(broker, TOPIC).await?;
    let mut keys = Vec::new();
    for (name, file) in [("session", "session.key"), ("credentials", "credentials.key")] {
        if records.first(name).await.is_none() {
            let offered = match local.and_then(|dir| crate::auth::existing_key(dir, file).transpose()) {
                Some(found) => found?,
                None => crate::auth::key(None, name)?,
            };
            records.put(name, Some(&seal(&wrap, name, &offered)?)).await?;
        }
        // Whoever wrote first, every node reads the same first record.
        let first = records
            .first(name)
            .await
            .ok_or_else(|| format!("the cluster's {name} key was written but cannot be read back"))?;
        keys.push(open(&wrap, name, &first)?);
    }
    let credentials = keys.pop().unwrap_or_default();
    let session = keys.pop().unwrap_or_default();
    Ok(Shared { session, credentials })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_key_sealed_with_one_secret_opens_with_it_and_not_with_another() {
        let key = [7u8; 32];
        let sealed = seal(&wrapping(b"one cluster secret").unwrap(), "session", &key).unwrap();
        assert_eq!(
            open(&wrapping(b"one cluster secret").unwrap(), "session", &sealed).unwrap(),
            key
        );
        let other = open(&wrapping(b"another cluster secret").unwrap(), "session", &sealed).unwrap_err();
        assert!(other.contains("another cluster secret"), "{other}");
        let swapped = open(&wrapping(b"one cluster secret").unwrap(), "credentials", &sealed);
        assert!(swapped.is_err(), "a key is bound to its name");
    }
}
