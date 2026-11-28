// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the studio keeps, as typed records: each kind in a topic of its own (one partition, kept for ever, the last
//! record of a key its value), each record carrying the schema it was written in. The same records map one to one
//! onto tables, should a deployment keep them in a database instead.
//!
//! - **users**: who signs in (by e-mail); an installation's admin manages every workspace.
//! - **workspaces**: what an agency keeps for each of its clients - workflows, credentials, files, runs - apart.
//! - **members**: a user's role in a workspace: `admin` (members too), `editor` (build and run), `viewer` (look).
//! - **credentials**: sealed, bound to their workspace, environment and name.
//! - **workflows**, their **versions** (one for each change saved), and **definitions**, kept once per workspace by
//!   their content's SHA-256 - a version points at one.
//! - **triggers**: how each workflow that is on starts - its webhook's address, its schedule - so all of them can be
//!   listed and a schedule fires once whatever the number of nodes.
//! - **files**: lookup files by path (`/rates.csv`), their bytes beside them.
//!
//! Nothing is deleted outright: a record that is removed keeps its key, marked with when it was.

use std::time::{SystemTime, UNIX_EPOCH};

use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::records::Records;

/// The schema records are written in.
pub(crate) const SCHEMA: u32 = 1;
/// The environment a workspace's workflows and credentials are in until promotion between environments comes.
pub(crate) const PRODUCTION: &str = "production";

pub(crate) const USERS: &str = "__openduct.model.users";
pub(crate) const WORKSPACES: &str = "__openduct.model.workspaces";
pub(crate) const MEMBERS: &str = "__openduct.model.members";
pub(crate) const ALERTS_SENT: &str = "__openduct.model.alerts-sent";
pub(crate) const PIPELINES: &str = "__openduct.model.pipelines";
pub(crate) const CREDENTIALS: &str = "__openduct.model.credentials";
pub(crate) const WORKFLOWS: &str = "__openduct.model.workflows";
pub(crate) const VERSIONS: &str = "__openduct.model.versions";
pub(crate) const DEFINITIONS: &str = "__openduct.model.definitions";
pub(crate) const TRIGGERS: &str = "__openduct.model.triggers";
pub(crate) const FILES: &str = "__openduct.model.files";
pub(crate) const FILE_BODIES: &str = "__openduct.model.file-bodies";

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A new id: a UUID version 7 (its time first, so ids sort by when they were made; 74 random bits).
pub(crate) fn new_id() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(0));
    let mut random = [0u8; 10];
    if SystemRandom::new().fill(&mut random).is_err() {
        // No randomness from the system: the time and a counter keep ids unique in this process.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        random = [0u8; 10];
        for (to, from) in random.iter_mut().skip(2).zip(n.to_be_bytes()) {
            *to = from;
        }
    }
    let mut bytes: Vec<u8> = ms.to_be_bytes().get(2..).unwrap_or_default().to_vec();
    bytes.extend_from_slice(&random);
    if let Some(b) = bytes.get_mut(6) {
        *b = (*b & 0x0f) | 0x70;
    }
    if let Some(b) = bytes.get_mut(8) {
        *b = (*b & 0x3f) | 0x80;
    }
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        hex.get(0..8).unwrap_or_default(),
        hex.get(8..12).unwrap_or_default(),
        hex.get(12..16).unwrap_or_default(),
        hex.get(16..20).unwrap_or_default(),
        hex.get(20..32).unwrap_or_default()
    )
}

/// The SHA-256 of a definition's JSON, in hex: what versions point at.
pub(crate) fn content_hash(definition: &Value) -> String {
    let d = digest::digest(&digest::SHA256, definition.to_string().as_bytes());
    d.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct User {
    pub schema: u32,
    pub id: String,
    pub email: String,
    pub name: String,
    pub password: String,
    /// Bumped to end every session of the user.
    pub generation: u64,
    /// The installation's admin: every workspace, and its members.
    pub admin: bool,
    pub created: u64,
    pub updated: u64,
    #[serde(default)]
    pub deleted: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Workspace {
    pub schema: u32,
    pub id: String,
    pub name: String,
    pub created: u64,
    pub updated: u64,
    #[serde(default)]
    pub deleted: Option<u64>,
    /// Where the workspace's important alerts go: its own messenger accounts, each named by a credential.
    #[serde(default)]
    pub alerts: Vec<AlertChannel>,
}

/// One place a workspace's alerts go: a messenger step's kind, the credential it sends with, and where in it
/// (a Telegram chat, a Slack channel, a WhatsApp number; none for a webhook, which is its own place).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AlertChannel {
    pub kind: String,
    pub credential: String,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub on: bool,
}

/// A pipeline of a workspace's: its file as written, and whether it runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PipelineRecord {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    pub name: String,
    /// The pipeline file (YAML), checked before it was kept.
    pub source: String,
    pub active: bool,
    pub created: u64,
    pub updated: u64,
    pub by: String,
    #[serde(default)]
    pub deleted: Option<u64>,
}

/// An alert sent: what it said, and which node sent it - so each is sent once, across the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AlertSent {
    pub schema: u32,
    pub fingerprint: String,
    pub by: String,
    pub at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Role {
    Viewer,
    Editor,
    Admin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Member {
    pub schema: u32,
    pub workspace: String,
    pub user: String,
    pub role: Role,
    pub added: u64,
    pub by: String,
    #[serde(default)]
    pub deleted: Option<u64>,
}

pub(crate) fn member_key(workspace: &str, user: &str) -> String {
    format!("{workspace}/{user}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CredentialRecord {
    pub schema: u32,
    pub workspace: String,
    pub environment: String,
    pub name: String,
    pub kind: String,
    /// The names of its fields; their values are in `sealed`.
    pub fields: Vec<String>,
    pub sealed: String,
    pub updated: u64,
    pub by: String,
    #[serde(default)]
    pub deleted: Option<u64>,
}

pub(crate) fn credential_key(workspace: &str, environment: &str, name: &str) -> String {
    format!("{workspace}/{environment}/{name}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowRecord {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    pub environment: String,
    pub name: String,
    /// The newest version saved.
    pub version: u64,
    /// The hash of the newest version's definition.
    pub hash: String,
    /// Whether it runs (the newest version, then).
    pub active: bool,
    /// Its webhook's address is `/hooks/<hook>`: random, so an address cannot be guessed.
    pub hook: String,
    /// Where the set-aside events not yet run again start.
    #[serde(default)]
    pub failed_from: i64,
    pub created: u64,
    pub updated: u64,
    pub by: String,
    #[serde(default)]
    pub deleted: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Version {
    pub schema: u32,
    pub workflow: String,
    pub number: u64,
    pub hash: String,
    pub created: u64,
    pub by: String,
}

pub(crate) fn version_key(workflow: &str, number: u64) -> String {
    format!("{workflow}/{number:010}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Definition {
    pub schema: u32,
    pub workspace: String,
    pub hash: String,
    pub body: Value,
}

pub(crate) fn definition_key(workspace: &str, hash: &str) -> String {
    format!("{workspace}/{hash}")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TriggerRecord {
    pub schema: u32,
    /// The workflow's id: a workflow has one trigger.
    pub workflow: String,
    pub workspace: String,
    /// `webhook`, `schedule`, `topic` or `manual`.
    pub kind: String,
    pub version: u64,
    /// For a webhook: its address.
    #[serde(default)]
    pub hook: Option<String>,
    /// For a schedule: its interval, in milliseconds, and the last tick queued.
    #[serde(default)]
    pub every_ms: Option<u64>,
    #[serde(default)]
    pub last_tick: Option<i64>,
    /// For a topic: its name.
    #[serde(default)]
    pub topic: Option<String>,
    pub active: bool,
    pub updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FileRecord {
    pub schema: u32,
    pub id: String,
    pub workspace: String,
    /// `/rates.csv`: what steps name it by.
    pub path: String,
    pub content_type: String,
    pub size: u64,
    pub sha256: String,
    /// Where its bytes are, in parts, in the topic of file bodies.
    pub parts: Vec<i64>,
    pub updated: u64,
    pub by: String,
    #[serde(default)]
    pub deleted: Option<u64>,
}

pub(crate) fn file_key(workspace: &str, path: &str) -> String {
    format!("{workspace}{path}")
}

/// A kind of record in its topic.
#[derive(Debug)]
pub(crate) struct Table<T> {
    records: Records,
    kind: std::marker::PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Table<T> {
    pub(crate) async fn open(broker: &str, topic: &str) -> Result<Self, String> {
        Ok(Self {
            records: Records::open(broker, topic).await?,
            kind: std::marker::PhantomData,
        })
    }

    pub(crate) async fn refresh(&self) -> Result<(), String> {
        self.records.catch_up().await
    }

    pub(crate) async fn get(&self, key: &str) -> Option<T> {
        self.records
            .get(key)
            .await
            .and_then(|v| serde_json::from_value(v).ok())
    }

    pub(crate) async fn put(&self, key: &str, value: &T) -> Result<(), String> {
        let json = serde_json::to_value(value).map_err(|e| e.to_string())?;
        self.records.put(key, Some(&json)).await
    }

    pub(crate) async fn remove(&self, key: &str) -> Result<(), String> {
        self.records.put(key, None).await
    }

    /// Every record (a key whose record does not read in this schema is left out).
    pub(crate) async fn all(&self) -> Vec<(String, T)> {
        self.records
            .all()
            .await
            .into_iter()
            .filter_map(|(k, v)| serde_json::from_value(v).ok().map(|t| (k, t)))
            .collect()
    }

    pub(crate) async fn is_empty(&self) -> bool {
        self.records.is_empty().await
    }
}

/// Every kind of record.
#[derive(Debug)]
pub(crate) struct Model {
    pub users: Table<User>,
    pub workspaces: Table<Workspace>,
    pub members: Table<Member>,
    pub credentials: Table<CredentialRecord>,
    pub workflows: Table<WorkflowRecord>,
    pub versions: Table<Version>,
    pub definitions: Table<Definition>,
    pub triggers: Table<TriggerRecord>,
    pub files: Table<FileRecord>,
    pub alerts_sent: Table<AlertSent>,
    pub pipelines: Table<PipelineRecord>,
    /// MCP: the tokens AI assistants sign in to a workspace's MCP server with (their hashes).
    pub mcp_tokens: Table<crate::mcp::McpToken>,
    pub custom_apps: Table<crate::custom_apps::CustomApp>, // app generator: each workspace's own apps
}

impl Model {
    pub(crate) async fn open(broker: &str) -> Result<Self, String> {
        Ok(Self {
            users: Table::open(broker, USERS).await?,
            workspaces: Table::open(broker, WORKSPACES).await?,
            members: Table::open(broker, MEMBERS).await?,
            credentials: Table::open(broker, CREDENTIALS).await?,
            workflows: Table::open(broker, WORKFLOWS).await?,
            versions: Table::open(broker, VERSIONS).await?,
            definitions: Table::open(broker, DEFINITIONS).await?,
            triggers: Table::open(broker, TRIGGERS).await?,
            files: Table::open(broker, FILES).await?,
            alerts_sent: Table::open(broker, ALERTS_SENT).await?,
            pipelines: Table::open(broker, PIPELINES).await?,
            mcp_tokens: Table::open(broker, crate::mcp::MCP_TOKENS).await?, // MCP
            custom_apps: Table::open(broker, crate::custom_apps::CUSTOM_APPS).await?, // app generator
        })
    }

    /// Reads what other nodes wrote.
    pub(crate) async fn refresh(&self) -> Result<(), String> {
        self.users.refresh().await?;
        self.workspaces.refresh().await?;
        self.members.refresh().await?;
        self.credentials.refresh().await?;
        self.workflows.refresh().await?;
        self.versions.refresh().await?;
        self.definitions.refresh().await?;
        self.triggers.refresh().await?;
        self.files.refresh().await
    }

    /// The workspaces `user` may see, with its role in each (an admin sees all, as admin).
    pub(crate) async fn workspaces_of(&self, user: &User) -> Vec<(Workspace, Role)> {
        let members = self.members.all().await;
        let mut out: Vec<(Workspace, Role)> = self
            .workspaces
            .all()
            .await
            .into_iter()
            .filter(|(_, w)| w.deleted.is_none())
            .filter_map(|(_, w)| {
                if user.admin {
                    return Some((w, Role::Admin));
                }
                members
                    .iter()
                    .find(|(_, m)| m.workspace == w.id && m.user == user.id && m.deleted.is_none())
                    .map(|(_, m)| (w.clone(), m.role))
            })
            .collect();
        out.sort_by_key(|a| a.0.created);
        out
    }

    /// A definition, kept once per workspace by its hash.
    pub(crate) async fn keep_definition(&self, workspace: &str, body: &Value) -> Result<String, String> {
        let hash = content_hash(body);
        let key = definition_key(workspace, &hash);
        if self.definitions.get(&key).await.is_none() {
            self.definitions
                .put(
                    &key,
                    &Definition {
                        schema: SCHEMA,
                        workspace: workspace.to_owned(),
                        hash: hash.clone(),
                        body: body.clone(),
                    },
                )
                .await?;
        }
        Ok(hash)
    }

    pub(crate) async fn definition(&self, workspace: &str, hash: &str) -> Option<Value> {
        self.definitions
            .get(&definition_key(workspace, hash))
            .await
            .map(|d| d.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_uuid_version_7_and_sort_by_time() {
        let a = new_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = new_id();
        assert_eq!(a.len(), 36);
        assert_eq!(a.chars().nth(14), Some('7'), "version 7");
        assert!(
            matches!(a.chars().nth(19), Some('8' | '9' | 'a' | 'b')),
            "the variant"
        );
        assert!(a < b, "later ids sort later");
        assert_ne!(new_id(), new_id());
    }

    #[test]
    fn a_definition_is_named_by_its_content() {
        let one = serde_json::json!({ "name": "x", "steps": [] });
        assert_eq!(content_hash(&one), content_hash(&one.clone()));
        assert_ne!(
            content_hash(&one),
            content_hash(&serde_json::json!({ "name": "y", "steps": [] }))
        );
        assert_eq!(content_hash(&one).len(), 64);
    }

    #[test]
    fn roles_order_by_what_they_allow() {
        assert!(Role::Admin > Role::Editor && Role::Editor > Role::Viewer);
        assert_eq!(serde_json::to_string(&Role::Editor).unwrap(), r#""editor""#);
    }
}
