// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Credentials: the keys and tokens a workspace's workflows use, kept as records of the data model sealed with
//! AES-256-GCM under a key of the studio's own (made the first time, kept in the data directory). A sealed value is
//! bound to its workspace, its environment and its name, so it opens nowhere else. The API shows a credential's
//! name, kind and the names of its fields - never a value; a field can be replaced, never read back. Runs read them
//! through [`Vault`], which keeps them opened in memory, and only their own workspace's.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use hyper::{Method, StatusCode};
use openduct_flow::Credential;
use openduct_flow::apps::{credential_kind, credential_kinds};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};

use crate::State;
use crate::api::{access, signed_in};
use crate::http::{Answer, Call, json, problem};
use crate::model::{CredentialRecord, Model, PRODUCTION, Role, SCHEMA, credential_key, now};

/// The longest value a field may have.
const MAX_FIELD: usize = 16 * 1024;

/// The credentials, opened, for runs; and the key they are sealed with.
pub(crate) struct Vault {
    key: LessSafeKey,
    /// By `workspace/environment/name`.
    opened: RwLock<BTreeMap<String, Credential>>,
}

impl std::fmt::Debug for Vault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vault").finish_non_exhaustive()
    }
}

impl Vault {
    /// The vault with its key from `dir` (made the first time), or one for this run alone.
    pub(crate) fn load(dir: Option<&Path>) -> Result<Self, String> {
        Self::from_key(&crate::auth::key(dir, "credentials.key")?)
    }

    /// Credentials sealed with `bytes`: a cluster's shared key.
    pub(crate) fn from_key(bytes: &[u8]) -> Result<Self, String> {
        let key = UnboundKey::new(&AES_256_GCM, bytes).map_err(|_| "not a key for AES-256-GCM")?;
        Ok(Self {
            key: LessSafeKey::new(key),
            opened: RwLock::new(BTreeMap::new()),
        })
    }

    /// `fields` sealed, bound to `bound` (the credential's workspace, environment and name).
    pub(crate) fn seal(&self, bound: &str, fields: &BTreeMap<String, String>) -> Result<String, String> {
        let mut nonce = [0u8; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| "the system gave no random bytes")?;
        let mut data = serde_json::to_vec(fields).map_err(|e| e.to_string())?;
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(bound.as_bytes()),
                &mut data,
            )
            .map_err(|_| "cannot seal the credential")?;
        let mut sealed = nonce.to_vec();
        sealed.extend_from_slice(&data);
        Ok(format!("v1:{}", B64.encode(sealed)))
    }

    pub(crate) fn open(&self, bound: &str, sealed: &str) -> Option<BTreeMap<String, String>> {
        let bytes = B64.decode(sealed.strip_prefix("v1:")?).ok()?;
        let (nonce, data) = bytes.split_at_checked(12)?;
        let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
        let mut data = data.to_vec();
        let plain = self
            .key
            .open_in_place(nonce, Aad::from(bound.as_bytes()), &mut data)
            .ok()?;
        serde_json::from_slice(plain).ok()
    }

    /// Opens every credential kept, for runs; one that cannot be opened (sealed under another key) is left out.
    pub(crate) async fn refresh(&self, model: &Model) {
        let mut opened = BTreeMap::new();
        for (key, record) in model.credentials.all().await {
            if record.deleted.is_some() {
                continue;
            }
            match self.open(&key, &record.sealed) {
                Some(fields) => {
                    opened.insert(
                        key,
                        Credential {
                            kind: record.kind,
                            fields,
                        },
                    );
                }
                None => tracing::warn!(
                    credential = key,
                    "a credential cannot be opened with this studio's key"
                ),
            }
        }
        if let Ok(mut held) = self.opened.write() {
            *held = opened;
        }
    }

    pub(crate) fn get(&self, workspace: &str, environment: &str, name: &str) -> Option<Credential> {
        self.opened
            .read()
            .ok()?
            .get(&credential_key(workspace, environment, name))
            .cloned()
    }

    /// A workspace's credentials by name, their secret fields as •••: what a test run needs, no secret in it.
    pub(crate) fn masked(&self, workspace: &str, environment: &str) -> BTreeMap<String, Credential> {
        let prefix = format!("{workspace}/{environment}/");
        self.opened
            .read()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, c)| k.strip_prefix(&prefix).map(|name| (name.to_owned(), c.masked())))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A workspace's credentials, as its runs see them: no other workspace's.
#[derive(Debug)]
pub(crate) struct Secrets {
    pub(crate) vault: Arc<Vault>,
    pub(crate) model: Arc<Model>,
    pub(crate) workspace: String,
    pub(crate) environment: String,
}

/// Where the install's own registrations with vendors are kept: beside the credentials, in no workspace.
pub(crate) const INSTALL: &str = "__install";

/// The install's registration with app `app`'s vendor (its OAuth client id and secret).
pub(crate) fn registration(vault: &Vault, app: &str) -> Option<Credential> {
    vault.get(INSTALL, PRODUCTION, app)
}

impl openduct_flow::Secrets for Secrets {
    fn get(&self, name: &str) -> Option<Credential> {
        self.vault.get(&self.workspace, &self.environment, name)
    }

    fn registration(&self, app: &str) -> Option<Credential> {
        registration(&self.vault, app)
    }

    fn keep<'a>(
        &'a self,
        name: &'a str,
        fields: BTreeMap<String, String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<openduct_expr::Value, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let key = credential_key(&self.workspace, &self.environment, name);
            let mut record = self
                .model
                .credentials
                .get(&key)
                .await
                .filter(|r| r.deleted.is_none())
                .ok_or_else(|| format!("there is no credential called {name:?}"))?;
            let mut all = self
                .vault
                .get(&self.workspace, &self.environment, name)
                .map(|c| c.fields)
                .unwrap_or_default();
            all.extend(fields);
            record.sealed = self.vault.seal(&key, &all)?;
            record.fields = all.keys().cloned().collect();
            record.updated = now();
            self.model.credentials.put(&key, &record).await?;
            self.vault.refresh(&self.model).await;
            Ok(openduct_expr::Value::Null)
        })
    }
}

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "credential-kinds"]) => kinds(state, call).await,
        (&Method::GET, ["api", "credentials"]) => list(state, call).await,
        (&Method::GET, ["api", "credentials", name, "models"]) => models(state, call, name).await,
        (&Method::POST, ["api", "credentials"]) => create(state, call).await,
        (&Method::PUT, ["api", "credentials", name]) => update(state, call, name).await,
        (&Method::DELETE, ["api", "credentials", name]) => delete(state, call, name).await,
        _ => return None,
    })
}

async fn kinds(state: &State, call: &Call) -> Answer {
    if let Err(answer) = signed_in(state, call).await {
        return answer;
    }
    let kinds: Vec<Value> = credential_kinds()
        .iter()
        .map(|k| {
            let fields: Vec<Value> = k
                .fields
                .iter()
                .map(|f| {
                    let name = f.trim_end_matches('?');
                    json!({ "name": name, "optional": f.ends_with('?'), "secret": k.secrets.contains(&name) })
                })
                .collect();
            // An app a person signs in to at the vendor is connected (Connect), not typed in: its token fields are
            // OpenDuct's to keep. `registered`: whether the install has its registration with the vendor yet.
            let app = openduct_flow::app_files::by_credential(k.kind);
            let connect = app.is_some_and(|(_, s)| {
                s.oauth2
                    .as_ref()
                    .is_some_and(|o| o.grant == openduct_flow::app_files::Grant::AuthorizationCode)
            });
            let fields: Vec<Value> = if connect { Vec::new() } else { fields };
            let registered = connect && app.is_some_and(|(a, _)| registration(&state.vault, &a.id).is_some());
            json!({
                "kind": k.kind, "label": k.label, "fields": fields,
                "app": app.map(|(a, _)| a.id.clone()), "connect": connect, "registered": registered,
            })
        })
        .collect();
    json(StatusCode::OK, &json!({ "kinds": kinds }))
}

/// What is shown of a credential: never a value.
fn shown(record: &CredentialRecord) -> Value {
    json!({ "name": record.name, "kind": record.kind, "fields": record.fields, "updated": record.updated })
}

/// The models an ai credential's service offers, asked of the service itself, through the workspace's effects (so
/// as guarded as a step's own request).
async fn models(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let workspace = access.workspace.id.clone();
    let Some(credential) = state.vault.get(&workspace, PRODUCTION, name) else {
        return problem(StatusCode::NOT_FOUND, "no such credential");
    };
    if credential.kind != "ai" {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "only an ai credential lists models",
        );
    }
    let request = match openduct_flow::models_request(&credential) {
        Ok(r) => r,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let effects = match state.effects_for(&workspace) {
        Ok(e) => e,
        Err(e) => return problem(StatusCode::SERVICE_UNAVAILABLE, &e),
    };
    match effects
        .http(request)
        .await
        .and_then(|answer| openduct_flow::models_in(&answer))
    {
        Ok(found) => {
            let models: Vec<Value> = found
                .into_iter()
                .map(|(id, name)| json!({ "id": id, "name": name }))
                .collect();
            json(StatusCode::OK, &json!({ "models": models }))
        }
        Err(e) => problem(StatusCode::BAD_GATEWAY, &format!("cannot list the models: {e}")),
    }
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let _ = state.model.credentials.refresh().await;
    let out: Vec<Value> = state
        .model
        .credentials
        .all()
        .await
        .iter()
        .filter(|(_, c)| {
            c.workspace == access.workspace.id && c.environment == PRODUCTION && c.deleted.is_none()
        })
        .map(|(_, c)| shown(c))
        .collect();
    json(StatusCode::OK, &json!({ "credentials": out }))
}

/// The fields a request sends, checked against the kind's: known names, values of text, the required ones there.
fn fields_of(
    kind: &str,
    body: &Value,
    old: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, String> {
    let spec = credential_kind(kind).ok_or_else(|| format!("{kind:?} is not a kind of credential"))?;
    let known: Vec<&str> = spec.fields.iter().map(|f| f.trim_end_matches('?')).collect();
    let mut fields = old.cloned().unwrap_or_default();
    let given = body
        .get("fields")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (k, v) in given {
        if !known.contains(&k.as_str()) {
            return Err(format!(
                "{k}: not a field of a {kind} credential ({})",
                known.join(", ")
            ));
        }
        let text = v.as_str().ok_or_else(|| format!("{k}: text"))?;
        if text.len() > MAX_FIELD {
            return Err(format!("{k}: at most {MAX_FIELD} bytes"));
        }
        if text.is_empty() {
            fields.remove(&k);
        } else {
            fields.insert(k, text.to_owned());
        }
    }
    for required in spec.fields.iter().filter(|f| !f.ends_with('?')) {
        if !fields.contains_key(*required) {
            return Err(format!("{required}: a {kind} credential needs it"));
        }
    }
    Ok(fields)
}

pub(crate) async fn keep(
    state: &State,
    record: &mut CredentialRecord,
    fields: &BTreeMap<String, String>,
    status: StatusCode,
) -> Answer {
    let key = credential_key(&record.workspace, &record.environment, &record.name);
    record.sealed = match state.vault.seal(&key, fields) {
        Ok(s) => s,
        Err(e) => return problem(StatusCode::INTERNAL_SERVER_ERROR, &e),
    };
    record.fields = fields.keys().cloned().collect();
    record.updated = now();
    if let Err(e) = state.model.credentials.put(&key, record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    state.vault.refresh(&state.model).await;
    json(status, &shown(record))
}

async fn create(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let name = call
        .body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "name: letters, digits, '_' and '-', at most 64",
        );
    }
    let _ = state.model.credentials.refresh().await;
    let key = credential_key(&access.workspace.id, PRODUCTION, &name);
    if state
        .model
        .credentials
        .get(&key)
        .await
        .is_some_and(|c| c.deleted.is_none())
    {
        return problem(StatusCode::CONFLICT, "a credential has this name already");
    }
    let kind = call
        .body
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let fields = match fields_of(&kind, &call.body, None) {
        Ok(f) => f,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let mut record = CredentialRecord {
        schema: SCHEMA,
        workspace: access.workspace.id,
        environment: PRODUCTION.to_owned(),
        name,
        kind,
        fields: Vec::new(),
        sealed: String::new(),
        updated: now(),
        by: access.user.id,
        deleted: None,
    };
    keep(state, &mut record, &fields, StatusCode::CREATED).await
}

async fn update(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let key = credential_key(&access.workspace.id, PRODUCTION, name);
    let Some(mut record) = state
        .model
        .credentials
        .get(&key)
        .await
        .filter(|c| c.deleted.is_none())
    else {
        return problem(StatusCode::NOT_FOUND, "no such credential");
    };
    let Some(old) = state.vault.open(&key, &record.sealed) else {
        return problem(
            StatusCode::CONFLICT,
            "this credential was sealed with another key: delete it and add it again",
        );
    };
    let fields = match fields_of(&record.kind, &call.body, Some(&old)) {
        Ok(f) => f,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    record.by = access.user.id;
    keep(state, &mut record, &fields, StatusCode::OK).await
}

async fn delete(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let key = credential_key(&access.workspace.id, PRODUCTION, name);
    let Some(mut record) = state
        .model
        .credentials
        .get(&key)
        .await
        .filter(|c| c.deleted.is_none())
    else {
        return problem(StatusCode::NOT_FOUND, "no such credential");
    };
    // Its sealed value goes with it: a credential removed opens nowhere.
    record.deleted = Some(now());
    record.sealed = String::new();
    record.updated = now();
    if let Err(e) = state.model.credentials.put(&key, &record).await {
        return problem(StatusCode::SERVICE_UNAVAILABLE, &e);
    }
    state.vault.refresh(&state.model).await;
    json(StatusCode::OK, &json!({ "deleted": name }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credential_is_sealed_to_its_workspace_environment_and_name_and_to_its_key() {
        let vault = Vault::load(None).unwrap();
        let fields: BTreeMap<String, String> = [("bot_token".to_owned(), "123:abc".to_owned())].into();
        let sealed = vault.seal("w1/production/bot", &fields).unwrap();
        assert!(!sealed.contains("123:abc"));
        assert_eq!(vault.open("w1/production/bot", &sealed), Some(fields.clone()));
        assert_eq!(
            vault.open("w2/production/bot", &sealed),
            None,
            "not in another workspace"
        );
        assert_eq!(
            vault.open("w1/production/other", &sealed),
            None,
            "not under another name"
        );
        assert_eq!(
            Vault::load(None).unwrap().open("w1/production/bot", &sealed),
            None,
            "not with another key"
        );
        assert_ne!(
            sealed,
            vault.seal("w1/production/bot", &fields).unwrap(),
            "a nonce each time"
        );
    }

    #[test]
    fn fields_are_those_of_the_kind_and_a_change_keeps_the_others() {
        let body = json!({ "fields": { "base_url": "https://models.example.com/v1", "api_key": "key-1" } });
        let fields = fields_of("ai", &body, None).unwrap();
        assert_eq!(fields.get("api_key").map(String::as_str), Some("key-1"));
        assert!(
            fields_of("ai", &json!({ "fields": {} }), None)
                .unwrap_err()
                .contains("base_url")
        );
        assert!(
            fields_of("ai", &json!({ "fields": { "password": "x" } }), None)
                .unwrap_err()
                .contains("not a field")
        );
        let changed = fields_of("ai", &json!({ "fields": { "api": "messages" } }), Some(&fields)).unwrap();
        assert_eq!(changed.len(), 3, "the address and the key are kept");
        assert!(fields_of("nosuch", &body, None).is_err());
        let masked = Credential {
            kind: "ai".to_owned(),
            fields: changed,
        }
        .masked();
        assert_eq!(
            masked.fields.get("api_key").map(String::as_str),
            Some(openduct_flow::MASK)
        );
        assert_eq!(
            masked.fields.get("base_url").map(String::as_str),
            Some("https://models.example.com/v1"),
            "not a secret"
        );
    }
}
