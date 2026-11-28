// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// How each kind of credential, and each of its fields, is shown: a plain name, a hint, an example, and the choices
// a field takes. The API knows the fields by their own names; people see these.

import type { Family } from "./flow/look";

export type KindLook = { name: string; icon: string; family: Family; blurb: string };

export const KIND_LOOK: Record<string, KindLook> = {
  ai: { name: "AI model", icon: "ai", family: "ai", blurb: "Any model with a chat-completions or messages API" },
  bearer: { name: "Bearer token", icon: "http", family: "call", blurb: "An API that takes Authorization: Bearer" },
  header: { name: "Secret header", icon: "http", family: "call", blurb: "An API that takes a key in a header" },
  basic: { name: "User and password", icon: "http", family: "call", blurb: "An API that takes HTTP basic sign-in" },
  postgres: { name: "Postgres", icon: "postgres", family: "data", blurb: "A database for postgres steps" },
  mysql: { name: "MySQL or MariaDB", icon: "mysql", family: "data", blurb: "A database for mysql steps and triggers" }, // sql databases
  sqlserver: { name: "SQL Server", icon: "sqlserver", family: "data", blurb: "A database for sqlserver steps and triggers" }, // sql databases
  smtp: { name: "Mail server", icon: "email", family: "message", blurb: "Sends the e-mail steps' mail" },
  telegram: { name: "Telegram bot", icon: "telegram", family: "message", blurb: "A bot's token" },
  slack: { name: "Slack app", icon: "slack", family: "message", blurb: "A Slack app's bot token" },
  slack_webhook: { name: "Slack webhook", icon: "slack", family: "message", blurb: "A channel's incoming webhook" },
  discord_webhook: { name: "Discord webhook", icon: "discord", family: "message", blurb: "A channel's webhook" },
  teams_webhook: { name: "Teams webhook", icon: "teams", family: "message", blurb: "A channel's webhook" },
  google_chat_webhook: { name: "Google Chat webhook", icon: "google_chat", family: "message", blurb: "A space's webhook" },
  whatsapp: { name: "WhatsApp Business", icon: "whatsapp", family: "message", blurb: "The Cloud API's token and number" },
  // storage
  sftp: { name: "SFTP server", icon: "storage", family: "data", blurb: "Files on a server reached over SSH" },
  ftp: { name: "FTP server", icon: "storage", family: "data", blurb: "Files on an FTP server, over TLS unless you say" },
  s3: { name: "S3 storage", icon: "storage", family: "data", blurb: "AWS S3, MinIO, Cloudflare R2, Wasabi, Backblaze B2" },
  redis: { name: "Redis", icon: "redis", family: "data", blurb: "Keys, values, lists and channels" },
  // storage places
  azure_blob: { name: "Azure Blob Storage", icon: "storage", family: "data", blurb: "A storage account's container, by its key or a SAS token" },
  gcs: { name: "Google Cloud Storage", icon: "storage", family: "data", blurb: "A bucket, by a service account's key file" },
  box_token: { name: "Box (developer token)", icon: "storage", family: "data", blurb: "For trying a workflow: it lasts an hour" },
  // vector stores
  qdrant: { name: "Qdrant", icon: "vector_store", family: "data", blurb: "A vector database for vector stores" },
  weaviate: { name: "Weaviate", icon: "vector_store", family: "data", blurb: "A vector database for vector stores" },
  milvus: { name: "Milvus or Zilliz", icon: "vector_store", family: "data", blurb: "A vector database for vector stores" },
  chroma: { name: "Chroma", icon: "vector_store", family: "data", blurb: "A vector database for vector stores" },
  pinecone: { name: "Pinecone", icon: "vector_store", family: "data", blurb: "A vector database for vector stores" },
  mongodb: { name: "MongoDB Atlas", icon: "vector_store", family: "data", blurb: "Atlas Vector Search, for vector stores" },
  imap: { name: "Mailbox (IMAP)", icon: "mailbox", family: "message", blurb: "Any mailbox: Gmail, Outlook, your host's" }, // messaging
};

// vector stores: the fields of the vector databases' credentials.
const VECTOR_FIELDS: Record<string, Record<string, FieldLook>> = {
  qdrant: { url: { label: "Address", placeholder: "https://<cluster>.cloud.qdrant.io:6333", wide: true }, api_key: { label: "API key", hint: "if the server asks for one" } },
  weaviate: { url: { label: "Address", placeholder: "https://<cluster>.weaviate.cloud", wide: true }, api_key: { label: "API key", hint: "if the server asks for one" } },
  milvus: {
    url: { label: "Address", placeholder: "https://<host>:19530, or Zilliz Cloud's endpoint", wide: true },
    token: { label: "Token", hint: "user:password, or an API key" },
    database: { label: "Database", placeholder: "default" },
  },
  chroma: {
    url: { label: "Address", placeholder: "https://<host>:8000", wide: true },
    api_key: { label: "API key", hint: "Chroma Cloud's, or your server's token" },
    tenant: { label: "Tenant", placeholder: "default_tenant" },
    database: { label: "Database", placeholder: "default_database" },
  },
  pinecone: {
    api_key: { label: "API key" },
    cloud: { label: "Cloud", options: ["aws", "gcp", "azure"], hint: "where new indexes are made" },
    region: { label: "Region", placeholder: "us-east-1" },
    url: { label: "Control address", placeholder: "https://api.pinecone.io", hint: "only to reach another", wide: true },
  },
  mongodb: {
    url: { label: "Cluster", placeholder: "mongodb+srv://cluster0.abcde.mongodb.net", wide: true },
    auth_source: { label: "Users kept in", placeholder: "admin" },
    tls: { label: "TLS", options: ["true", "false"], hint: "on for mongodb+srv" },
  },
};

/** How a kind of credential looks: OpenDuct's own as above; an app's sign-in with its app's icon. */
export function kindLook(kind: string, label?: string, app?: string | null): KindLook {
  return KIND_LOOK[kind] ?? { name: kind, icon: app ?? "http", family: "call", blurb: label ?? "" };
}

export type FieldLook = { label: string; hint?: string; placeholder?: string; options?: string[]; multiline?: boolean; wide?: boolean };

const FIELDS: Record<string, FieldLook> = {
  base_url: { label: "Address", placeholder: "https://models.example.com/v1", wide: true },
  api_key: { label: "Key" },
  model: { label: "Model", hint: "when a step names none" },
  embedding_model: { label: "Embedding model", hint: "for Embed text" },
  api: { label: "API shape", options: ["chat_completions", "messages"], hint: "chat_completions is the standard" },
  key_header: { label: "Key header", placeholder: "Authorization", hint: "if the service wants another" },
  headers: { label: "Extra headers", placeholder: '{"name": "value"}', multiline: true, wide: true, hint: "JSON" },
  token: { label: "Token" },
  name: { label: "Header name", placeholder: "X-Api-Key" },
  value: { label: "Header value" },
  username: { label: "User" },
  password: { label: "Password" },
  tls: { label: "TLS", options: ["require", "disable"] },
  ca: { label: "CA certificate", placeholder: "-----BEGIN CERTIFICATE-----", multiline: true, wide: true, hint: "PEM, if not a public one" },
  host: { label: "Server", placeholder: "smtp.example.com" },
  from: { label: "From", placeholder: "Shop <shop@example.com>" },
  port: { label: "Port", placeholder: "587" },
  security: { label: "Security", options: ["starttls", "tls", "none"] },
  bot_token: { label: "Bot token" },
  access_token: { label: "Access token" },
  phone_number_id: { label: "Phone number id" },
  api_version: { label: "API version", placeholder: "v21.0" },
  // storage
  private_key: { label: "Private key", placeholder: "The whole key file, as ssh-keygen makes it", multiline: true, wide: true, hint: "instead of a password; not protected by a passphrase" },
  host_key: { label: "Server's key", placeholder: "ssh-ed25519 AAAA... or SHA256:...", wide: true, hint: "kept the first time when empty, then required" },
  folder: { label: "Folder", placeholder: "/upload", hint: "paths are inside it" },
  access_key: { label: "Access key" },
  secret_key: { label: "Secret key" },
  session_token: { label: "Session token", hint: "for temporary keys" },
  region: { label: "Region", placeholder: "us-east-1" },
  endpoint: { label: "Address", placeholder: "https://<account>.r2.cloudflarestorage.com", wide: true, hint: "for a store other than AWS" },
  path_style: { label: "Bucket in the path", options: ["true", "false"], hint: "true by default with an address" },
  bucket: { label: "Bucket", placeholder: "orders" },
  prefix: { label: "Prefix", placeholder: "exports/", hint: "every key is inside it" },
  // storage places
  account: { label: "Storage account", placeholder: "mystorage" },
  shared_key: { label: "Account key", hint: "or a SAS token" },
  sas_token: { label: "SAS token", placeholder: "sv=...&sig=...", wide: true, hint: "instead of the key; no addresses of its own" },
  container: { label: "Container", placeholder: "orders" },
  key_file: { label: "Service account key file", placeholder: '{"type": "service_account", ...}', multiline: true, wide: true, hint: "the JSON Google gives" },
  // messaging: an imap credential's own fields (host, port, security, username and password are shared)
  refresh_token: { label: "Refresh token", hint: "with the token address and client id, the access token is kept fresh" },
  token_url: { label: "Token address", placeholder: "https://oauth2.googleapis.com/token", wide: true },
  client_id: { label: "Client id" },
  client_secret: { label: "Client secret" },
};

// sql databases: the fields of a MySQL or SQL Server credential.
const DATABASE_FIELDS: Record<string, FieldLook> = {
  host: { label: "Server", placeholder: "db.example.com" },
  port: { label: "Port", placeholder: "3306 or 1433" },
  database: { label: "Database", placeholder: "shop" },
  tls: { label: "TLS", options: ["require", "strict", "disable"], hint: "strict: SQL Server 2022 and later" },
  certificate: { label: "Server certificate", placeholder: "-----BEGIN CERTIFICATE-----", multiline: true, wide: true, hint: "PEM: trusted as it is, for one of its own making" },
};

/** How field `name` of a `kind` credential is shown. */
export function fieldLook(kind: string, name: string): FieldLook {
  if ((kind === "mysql" || kind === "sqlserver") && DATABASE_FIELDS[name]) return DATABASE_FIELDS[name]!; // sql databases
  // storage places: an Azure account's own address, for a store other than Azure's.
  if (kind === "azure_blob" && name === "endpoint") return { label: "Address", placeholder: "http://127.0.0.1:10000/devstoreaccount1", wide: true, hint: "Azurite, or a store other than Azure's" };
  if (kind === "azure_blob" && name === "prefix") return { label: "Prefix", placeholder: "exports/", hint: "every name is inside it" };
  if (VECTOR_FIELDS[kind]?.[name]) return VECTOR_FIELDS[kind]![name]!; // vector stores
  if (name === "url") {
    if (kind === "redis") return { label: "Server", placeholder: "redis://host:6379/0 (rediss:// for TLS)", wide: true }; // storage
    return kind === "postgres"
      ? { label: "Database", placeholder: "postgres://host:5432/database", wide: true }
      : { label: "Webhook address", placeholder: "https://...", wide: true };
  }
  if (name === "base_url" && kind !== "ai") return { label: "API address", hint: "only for a server of your own", wide: true };
  return FIELDS[name] ?? { label: name };
}
