// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Steps that talk to an app: a model (`ai`, `embed`; see [`crate::ai`]) and the messengers (`telegram`, `slack`, `discord`, `teams`,
//! `google_chat`, `whatsapp`). Each names a credential; the secret in it is added to the request here and nowhere else, so no
//! expression can read it and no run's history holds it. Every request goes through the run's HTTP effect: a test
//! run shows it (the secrets as •••) instead of sending it, and the guard against this machine's network applies.

use std::collections::BTreeMap;

use base64::Engine as _;
use openduct_expr::Value;
use serde_json::{Value as Json, json};

use crate::run::HttpRequest;

/// What a credential holds: its kind and its fields.
#[derive(Debug, Clone, Default)]
pub struct Credential {
    pub kind: String,
    pub fields: BTreeMap<String, String>,
}

impl Credential {
    /// The credential as a test run may see it: its secret fields as •••.
    #[must_use]
    pub fn masked(&self) -> Self {
        let secrets = credential_kind(&self.kind).map_or(&[][..], |k| k.secrets);
        let fields = self
            .fields
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    if secrets.contains(&k.as_str()) {
                        MASK.to_owned()
                    } else {
                        v.clone()
                    },
                )
            })
            .collect();
        Self {
            kind: self.kind.clone(),
            fields,
        }
    }

    /// A field's value; in a test run, where no secret is read, `•••`.
    pub fn field(&self, name: &str) -> String {
        self.fields.get(name).cloned().unwrap_or_else(|| MASK.to_owned())
    }
}

/// What a test run shows for a secret.
pub const MASK: &str = "•••";

/// A kind of credential: its name, what it is for, its fields (`?` after an optional one) and which of them are
/// secret (kept sealed, never shown; a test run shows them as •••).
#[derive(Debug, Clone, Copy)]
pub struct CredentialKind {
    pub kind: &'static str,
    pub label: &'static str,
    pub fields: &'static [&'static str],
    pub secrets: &'static [&'static str],
}

pub const CREDENTIALS: &[CredentialKind] = &[
    CredentialKind {
        kind: "bearer",
        label: "A bearer token (Authorization: Bearer)",
        fields: &["token"],
        secrets: &["token"],
    },
    CredentialKind {
        kind: "header",
        label: "A header with a secret value",
        fields: &["name", "value"],
        secrets: &["value"],
    },
    CredentialKind {
        kind: "basic",
        label: "A user name and password (HTTP basic)",
        fields: &["username", "password"],
        secrets: &["password"],
    },
    CredentialKind {
        kind: "ai",
        label: "A model's API: its address, its key, and its shape (chat_completions, the standard, or messages)",
        fields: &[
            "base_url",
            "api_key?",
            "api?",
            "key_header?",
            "headers?",
            "provider?",
            // The model a step asks when it names none, and the one embeddings use.
            "model?",
            "embedding_model?",
        ],
        secrets: &["api_key"],
    },
    CredentialKind {
        kind: "postgres",
        label: "A Postgres database: postgres://host:port/database, who signs in, and tls (require, the default, or \
                disable)",
        fields: &["url", "username", "password?", "tls?", "ca?"],
        secrets: &["password"],
    },
    // sql databases
    CredentialKind {
        kind: "mysql",
        label: "A MySQL or MariaDB database: its host, port (3306), database, who signs in, and tls (require, the \
                default, or disable)",
        fields: &[
            "host",
            "username",
            "password?",
            "port?",
            "database?",
            "tls?",
            "ca?",
            "certificate?",
        ],
        secrets: &["password"],
    },
    // sql databases
    CredentialKind {
        kind: "sqlserver",
        label: "A SQL Server database: its host, port (1433), database, who signs in (a SQL Server user), and tls \
                (require, the default, strict, or disable)",
        fields: &[
            "host",
            "username",
            "password?",
            "port?",
            "database?",
            "tls?",
            "ca?",
            "certificate?",
        ],
        secrets: &["password"],
    },
    CredentialKind {
        kind: "smtp",
        label: "A mail server: its host, port, security (starttls, the default, tls or none), who signs in, and the \
                address mail is from",
        fields: &["host", "from", "port?", "security?", "username?", "password?"],
        secrets: &["password"],
    },
    // storage
    CredentialKind {
        kind: "sftp",
        label: "An SFTP server: its host, port (22), who signs in with a password or a private key, its host key (kept \
                the first time when not given), and a folder paths are inside",
        fields: &[
            "host",
            "username",
            "port?",
            "password?",
            "private_key?",
            "host_key?",
            "folder?",
        ],
        secrets: &["password", "private_key"],
    },
    CredentialKind {
        kind: "ftp",
        label: "An FTP server: its host, port, security (starttls, the default, tls or none), who signs in, a CA, and \
                a folder paths are inside",
        fields: &[
            "host",
            "port?",
            "security?",
            "username?",
            "password?",
            "ca?",
            "folder?",
        ],
        secrets: &["password"],
    },
    CredentialKind {
        kind: "s3",
        label: "S3, or a store that speaks its API (MinIO, R2, Wasabi, B2): keys, region, endpoint for a store other \
                than AWS, path_style, and a bucket and prefix",
        fields: &[
            "access_key",
            "secret_key",
            "region?",
            "endpoint?",
            "path_style?",
            "bucket?",
            "prefix?",
            "session_token?",
        ],
        secrets: &["secret_key", "session_token"],
    },
    // storage places
    CredentialKind {
        kind: "azure_blob",
        label: "Azure Blob Storage: the storage account, its shared key or a SAS token, a container, an endpoint for a \
                store other than Azure's (Azurite), and a prefix every name is inside",
        fields: &[
            "account",
            "shared_key?",
            "sas_token?",
            "container?",
            "endpoint?",
            "prefix?",
        ],
        secrets: &["shared_key", "sas_token"],
    },
    // storage places
    CredentialKind {
        kind: "gcs",
        label: "Google Cloud Storage: a service account's key file (JSON), a bucket and a prefix every name is inside",
        fields: &["key_file", "bucket?", "prefix?"],
        secrets: &["key_file"],
    },
    // storage places
    CredentialKind {
        kind: "box_token",
        label: "Box, with a developer token from Box's console (it lasts an hour: for trying a workflow; connect Box \
                for one that keeps running)",
        fields: &["token"],
        secrets: &["token"],
    },
    CredentialKind {
        kind: "redis",
        label: "A Redis server: its url (redis://host:6379/0, or rediss:// for TLS), who signs in, and a CA",
        fields: &["url", "username?", "password?", "ca?"],
        secrets: &["password"],
    },
    // vector stores
    CredentialKind {
        kind: "qdrant",
        label: "Qdrant, for vector stores: its address (https://<cluster>:6333) and an API key",
        fields: &["url", "api_key?"],
        secrets: &["api_key"],
    },
    CredentialKind {
        kind: "weaviate",
        label: "Weaviate, for vector stores: its address and an API key",
        fields: &["url", "api_key?"],
        secrets: &["api_key"],
    },
    CredentialKind {
        kind: "milvus",
        label: "Milvus or Zilliz Cloud, for vector stores: its address (https://<host>:19530), a token (user:password, \
                or an API key), and a database other than the default",
        fields: &["url", "token?", "database?"],
        secrets: &["token"],
    },
    CredentialKind {
        kind: "chroma",
        label: "Chroma, for vector stores: its address (https://<host>:8000), an API key, a tenant and a database",
        fields: &["url", "api_key?", "tenant?", "database?"],
        secrets: &["api_key"],
    },
    CredentialKind {
        kind: "pinecone",
        label: "Pinecone, for vector stores: an API key, and the cloud and region new indexes are made in",
        fields: &["api_key", "cloud?", "region?", "url?"],
        secrets: &["api_key"],
    },
    CredentialKind {
        kind: "mongodb",
        label: "MongoDB Atlas, for vector stores: its url (mongodb+srv://cluster0.abcde.mongodb.net), who signs in, \
                where users are kept (admin), TLS, and a CA",
        fields: &["url", "username?", "password?", "auth_source?", "tls?", "ca?"],
        secrets: &["password"],
    },
    // messaging
    CredentialKind {
        kind: "imap",
        label: "A mailbox, over IMAP: its host, port (993), security (tls, the default, starttls or none), who signs \
                in with a password or an OAuth access token (XOAUTH2; with a refresh token, the token address and the \
                client's id and secret, it is kept fresh), and a CA",
        fields: &[
            "host",
            "username",
            "password?",
            "port?",
            "security?",
            "access_token?",
            "refresh_token?",
            "token_url?",
            "client_id?",
            "client_secret?",
            "ca?",
        ],
        secrets: &["password", "access_token", "refresh_token", "client_secret"],
    },
];

/// A step that talks to an app: its name, the credential kinds it takes, and its fields (`?` for optional ones).
#[derive(Debug, Clone, Copy)]
pub struct AppKind {
    pub kind: &'static str,
    pub credentials: &'static [&'static str],
    pub fields: &'static [&'static str],
}

pub const APPS: &[AppKind] = &[
    AppKind {
        kind: "ai",
        credentials: &["ai"],
        fields: &[
            "model?",
            "messages?",
            "prompt?",
            "system?",
            "response_format?",
            "json?",
            "temperature?",
            "top_p?",
            "max_tokens?",
            "stop?",
        ],
    },
    AppKind {
        kind: "embed",
        credentials: &["ai"],
        fields: &["model?", "input"],
    },
];

pub fn app(kind: &str) -> Option<&'static AppKind> {
    APPS.iter().find(|a| a.kind == kind)
}

/// A kind of credential: one of OpenDuct's own, or an app's sign-in.
pub fn credential_kind(kind: &str) -> Option<&'static CredentialKind> {
    CREDENTIALS
        .iter()
        .find(|c| c.kind == kind)
        .or_else(|| crate::app_files::credential_kind(kind))
}

/// Every kind of credential: OpenDuct's own, then every app's sign-ins.
pub fn credential_kinds() -> Vec<&'static CredentialKind> {
    let mut all: Vec<&'static CredentialKind> = CREDENTIALS.iter().collect();
    for app in crate::app_files::all() {
        all.extend(
            app.sign_ins
                .iter()
                .filter_map(|s| crate::app_files::credential_kind(&s.id)),
        );
    }
    all
}

/// The request a model's step makes, from its credential and its fields (worked out). Other apps are defined by
/// files (see [`crate::app_files`]).
pub fn request(
    app: &str,
    credential: &Credential,
    fields: &BTreeMap<String, Value>,
) -> Result<HttpRequest, String> {
    match app {
        "ai" => crate::ai::chat_request(credential, fields),
        "embed" => crate::ai::embed_request(credential, fields),
        other => Err(format!("{other} is not an app OpenDuct talks to")),
    }
}

/// A step's result from the app's answer (`{status, headers, body}`), or why the app refused.
pub fn answer(
    app: &str,
    credential: &Credential,
    fields: &BTreeMap<String, Value>,
    answer: Value,
    ms: u64,
) -> Result<Value, String> {
    let json: Json = serde_json::from_str(&answer.to_json()).unwrap_or(Json::Null);
    // A test run answers with what it would have sent - and, for a model, the fields a real answer gives, saying so,
    // so the steps after it read as they will.
    if json.get("test").is_some() {
        if app == "ai"
            && let Json::Object(mut shown) = json
        {
            shown.insert(
                "text".to_owned(),
                Json::String("(the model's answer, in a real run)".to_owned()),
            );
            let asked_json = fields.get("json").is_some_and(|v| matches!(v, Value::Bool(true)))
                || fields.contains_key("response_format");
            if asked_json {
                shown.insert("json".to_owned(), Json::Object(serde_json::Map::new()));
            }
            return Ok(Value::from_json(&Json::Object(shown)));
        }
        return Ok(answer);
    }
    let status = json.get("status").and_then(Json::as_u64).unwrap_or(0);
    let body = json.get("body").cloned().unwrap_or(Json::Null);
    let said = |b: &Json| -> String {
        let text = match b {
            Json::String(s) => s.clone(),
            other => other.to_string(),
        };
        text.chars().take(300).collect()
    };
    if status >= 400 {
        let reason = body
            .pointer("/error/message")
            .or_else(|| body.get("description"))
            .or_else(|| body.get("error"))
            .map_or_else(|| said(&body), said);
        return Err(format!("{app} answered {status}: {reason}"));
    }
    match app {
        "ai" => crate::ai::chat_answer(credential, fields, &body, ms),
        "embed" => crate::ai::embed_answer(fields, &body, ms),
        _ => Ok(Value::from_json(&json!({ "sent": true, "answer": body }))),
    }
}

/// Adds an HTTP step's credential to its request: a bearer token, a header, or a user name and password.
pub fn authorize(request: &mut HttpRequest, credential: &Credential) -> Result<(), String> {
    match credential.kind.as_str() {
        "bearer" => request.headers.push((
            "authorization".to_owned(),
            format!("Bearer {}", credential.field("token")),
        )),
        "header" => request
            .headers
            .push((credential.field("name"), credential.field("value"))),
        "basic" => {
            let pair = format!(
                "{}:{}",
                credential.field("username"),
                credential.field("password")
            );
            request.headers.push((
                "authorization".to_owned(),
                format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(pair)),
            ));
        }
        // A test run does not read secrets: the request shows where they would go.
        "test" => request
            .headers
            .push(("authorization".to_owned(), MASK.to_owned())),
        other => {
            return Err(format!(
                "a {other} credential is not for an HTTP step: use bearer, header or basic"
            ));
        }
    }
    Ok(())
}

/// A field's value as text.
pub(crate) fn field(fields: &BTreeMap<String, Value>, name: &str) -> String {
    fields.get(name).map(crate::template::as_text).unwrap_or_default()
}

/// Sets a field of a JSON record.
pub(crate) fn put(record: &mut Json, name: &str, value: Json) {
    if let Some(fields) = record.as_object_mut() {
        fields.insert(name.to_owned(), value);
    }
}
