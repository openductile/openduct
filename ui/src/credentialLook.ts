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
  smtp: { name: "Mail server", icon: "email", family: "message", blurb: "Sends the e-mail steps' mail" },
  telegram: { name: "Telegram bot", icon: "telegram", family: "message", blurb: "A bot's token" },
  slack: { name: "Slack app", icon: "slack", family: "message", blurb: "A Slack app's bot token" },
  slack_webhook: { name: "Slack webhook", icon: "slack", family: "message", blurb: "A channel's incoming webhook" },
  discord_webhook: { name: "Discord webhook", icon: "discord", family: "message", blurb: "A channel's webhook" },
  teams_webhook: { name: "Teams webhook", icon: "teams", family: "message", blurb: "A channel's webhook" },
  google_chat_webhook: { name: "Google Chat webhook", icon: "google_chat", family: "message", blurb: "A space's webhook" },
  whatsapp: { name: "WhatsApp Business", icon: "whatsapp", family: "message", blurb: "The Cloud API's token and number" },
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
};

/** How field `name` of a `kind` credential is shown. */
export function fieldLook(kind: string, name: string): FieldLook {
  if (name === "url") {
    return kind === "postgres"
      ? { label: "Database", placeholder: "postgres://host:5432/database", wide: true }
      : { label: "Webhook address", placeholder: "https://...", wide: true };
  }
  if (name === "base_url" && kind !== "ai") return { label: "API address", hint: "only for a server of your own", wide: true };
  return FIELDS[name] ?? { label: name };
}
