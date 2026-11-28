// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// How each kind of step looks on the canvas: its family (a colour), its icon, and the one line that says what it
// does. Messengers get plain icons with their names, not their makers' marks.

import type { Step } from "../api";
import { scheduleLine } from "../scheduleWords"; // cron-schedules
import { catalogApp } from "../StepsApps"; // G3 app triggers
import { storageSummary, storageTriggerLine } from "../storageWords"; // storage
import { feedTriggerLine, webSummary } from "../webWords"; // web
import { mailboxSummary, mailboxTriggerLine } from "../mailboxWords"; // messaging

export type Family = "start" | "logic" | "data" | "ai" | "call" | "message" | "code" | "control";

export const FAMILY: Record<string, Family> = {
  set: "data",
  lookup: "data",
  postgres: "data",
  mysql: "data", // sql databases
  sqlserver: "data", // sql databases
  where: "logic",
  if: "logic",
  for_each: "logic",
  switch: "logic",
  list: "data",
  group: "data",
  merge: "data",
  datetime: "data",
  respond: "call",
  run_workflow: "control",
  ai: "ai",
  embed: "ai",
  classify: "ai", // G7 more AI steps
  sentiment: "ai",
  extract: "ai",
  summarise: "ai",
  agent: "ai", // G2 AI agents
  split_text: "data", // retrieval
  vector_store: "data", // retrieval
  ask_documents: "ai", // retrieval
  http: "call",
  topic: "call",
  email: "message",
  telegram: "message",
  slack: "message",
  discord: "message",
  teams: "message",
  google_chat: "message",
  whatsapp: "message",
  javascript: "code",
  python: "code",
  wait: "control",
  stop: "control",
  mcp: "call", // MCP
  // forms-and-resume
  form: "control",
  approval: "control",
  table: "data", // data tables
  state: "data", // data tables
  // G6 files
  extract_file: "data",
  convert: "data",
  file_details: "data",
  // storage
  storage: "data",
  compress: "data",
  redis: "data",
  web: "call", // web
  mailbox: "message", // messaging
};

/** Each icon is SVG path data on a 24x24 grid, drawn with a stroke. */
export const ICON: Record<string, string> = {
  start: "M13 2 4 14h7l-1 8 9-12h-7z",
  webhook: "M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1",
  schedule: "M12 7v5l3 2M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z",
  topic: "M4 6h16M4 12h10M4 18h13",
  manual: "M8 5v14l11-7z",
  failed: "M12 3 2 20h20zM12 9v5M12 17h.01",
  chat: "M4 5h16v11H9l-5 4z", // chat trigger (G8)
  database: "M4 6c0-2 16-2 16 0s-16 2-16 0zM4 6v12c0 2 16 2 16 0V6M4 12c0 2 16 2 16 0", // sql databases
  mysql: "M4 6c0-2 16-2 16 0s-16 2-16 0zM4 6v12c0 2 16 2 16 0V6M4 12c0 2 16 2 16 0", // sql databases
  sqlserver: "M4 6c0-2 16-2 16 0s-16 2-16 0zM4 6v12c0 2 16 2 16 0V6M4 12c0 2 16 2 16 0", // sql databases
  switch: "M12 3v6M12 9l-7 6v6M12 9v12M12 9l7 6v6",
  list: "M8 6h12M8 12h12M8 18h12M4 6h.01M4 12h.01M4 18h.01",
  group: "M4 4h7v7H4zM13 4h7v7h-7zM8.5 15v5M15.5 15v5M5 20h14",
  merge: "M6 3v5a4 4 0 0 0 4 4h4M18 3v5a4 4 0 0 1-4 4M12 12v9M9 18l3 3 3-3",
  datetime: "M4 6h16v14H4zM4 10h16M8 3v4M16 3v4M8 14h3",
  respond: "M9 14 4 9l5-5M4 9h11a5 5 0 0 1 5 5v6",
  run_workflow: "M4 6h6v4H4zM14 14h6v4h-6zM10 8h2a2 2 0 0 1 2 2v4M17 9l2-2-2-2",
  set: "M4 7h10M4 12h16M4 17h7M17 15l3 2-3 2",
  lookup: "M4 5h16v14H4zM4 10h16M10 5v14",
  postgres: "M4 6c0-2 16-2 16 0s-16 2-16 0zM4 6v12c0 2 16 2 16 0V6M4 12c0 2 16 2 16 0",
  where: "M4 5h16l-6 8v6l-4-2v-4z",
  if: "M6 3v6a4 4 0 0 0 4 4h4a4 4 0 0 1 4 4v4M6 21v-8",
  for_each: "M17 2l3 3-3 3M4 11V9a4 4 0 0 1 4-4h12M7 22l-3-3 3-3M20 13v2a4 4 0 0 1-4 4H4",
  ai: "M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8zM19 16l.8 2.2L22 19l-2.2.8L19 22l-.8-2.2L16 19l2.2-.8z",
  embed: "M4 12h4M16 12h4M12 4v4M12 16v4M8 8l8 8M16 8l-8 8",
  // G7 more AI steps
  classify: "M4 5h7v6H4zM13 13h7v6h-7zM4 15h5M15 7h5M11 8h2a2 2 0 0 1 2 2v3",
  sentiment: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM8 14s1.5 2 4 2 4-2 4-2M9 9h.01M15 9h.01",
  extract: "M4 4h10l6 6v10H4zM14 4v6h6M8 13h8M8 17h5",
  summarise: "M4 5h16M4 10h16M4 15h10M4 20h6",
  agent: "M8 4h8v6H8zM12 10v3M5 20v-3a3 3 0 0 1 3-3h8a3 3 0 0 1 3 3v3M10 7h.01M14 7h.01", // G2 AI agents
  split_text: "M6 4h9l3 3v13H6zM4 12h16M9 9h4M9 16h6", // retrieval
  vector_store: "M4 6c0-2 16-2 16 0s-16 2-16 0zM4 6v12c0 2 16 2 16 0V6M9 12h.01M12 14h.01M15 11h.01", // retrieval
  ask_documents: "M5 3h10l4 4v14H5zM10 10a2 2 0 1 1 3 1.7c-.6.4-1 .8-1 1.5M12 17h.01", // retrieval
  http: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM3 12h18M12 3c3 3 3 15 0 18M12 3c-3 3-3 15 0 18",
  email: "M3 6h18v12H3zM3 7l9 6 9-6",
  // The apps whose makers asked for no logo: a plain icon each, so none looks like another.
  slack: "M9 3 7 21M17 3l-2 18M4 8h17M3 16h17",
  teams: "M9 11a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM3 20a6 6 0 0 1 12 0M16 11a2.5 2.5 0 1 0 0-5M17 14a5 5 0 0 1 4 5",
  microsoft_365: "M4 8h16v11H4zM9 8V5h6v3M4 13h16",
  twilio: "M5 4h4l2 5-2.5 1.5a11 11 0 0 0 5 5L15 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 6a2 2 0 0 1 2-2",
  message: "M4 5h16v11H9l-5 4z",
  javascript: "M8 8l-4 4 4 4M16 8l4 4-4 4M14 5l-4 14",
  python: "M8 8l-4 4 4 4M16 8l4 4-4 4M14 5l-4 14",
  wait: "M7 3h10M7 21h10M8 3c0 5 8 5 8 9s-8 4-8 9M16 3c0 5-8 5-8 9s8 4 8 9",
  stop: "M6 6h12v12H6z",
  end: "M6 6h12v12H6z",
  // MCP: a plug into a socket.
  mcp: "M9 3v5M15 3v5M6 8h12v3a6 6 0 0 1-12 0zM12 17v4",
  // forms-and-resume
  form: "M5 3h14v18H5zM8 8h8M8 12h8M8 16h5",
  approval: "M9 11l3 3 8-8M20 12v7H4V5h11",
  table: "M3 5h18v14H3zM3 10h18M3 15h18M9 10v9M15 10v9", // data tables
  state: "M12 3a9 9 0 1 0 9 9M12 7v5l3 2M17 3h4v4", // data tables
  // G6 files: a page with rows coming out, a page made of rows, a page with a tag.
  extract_file: "M6 3h8l4 4v14H6zM14 3v4h4M9 12h6M9 16h6",
  convert: "M6 3h8l4 4v14H6zM14 3v4h4M12 10v8M9 15l3 3 3-3",
  file_details: "M6 3h8l4 4v14H6zM14 3v4h4M12 11h.01M12 14v4",
  // storage: a folder, a box with a band, a stack of discs - and the triggers' own.
  storage: "M3 6h6l2 2h10v11H3zM3 10h18",
  compress: "M5 4h14v16H5zM12 4v3M12 9v2M12 13v2M10 17h4v-2h-4z",
  redis: "M4 7c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 7v5c0 1.7 3.6 3 8 3s8-1.3 8-3V7M4 12v5c0 1.7 3.6 3 8 3s8-1.3 8-3v-5",
  // web: a globe for a page; a feed's waves.
  web: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM3 12h18M12 3c2.5 2.5 3.5 5.5 3.5 9s-1 6.5-3.5 9M12 3c-2.5 2.5-3.5 5.5-3.5 9s1 6.5 3.5 9",
  feed: "M5 19h.01M5 12a7 7 0 0 1 7 7M5 5a14 14 0 0 1 14 14",
  // messaging: a tray with a letter in it
  mailbox: "M3 13h5l1 2h6l1-2h5M3 13l2-8h14l2 8v6H3zM8 8h8M9 11h6",
};

export function iconOf(kind: string): string {
  return ICON[kind] ?? (FAMILY[kind] === "message" ? ICON.message! : ICON.set!);
}

function short(text: unknown, max = 42): string {
  const s = typeof text === "string" ? text : text == null ? "" : JSON.stringify(text);
  const one = s.replace(/\s+/g, " ").trim();
  return one.length > max ? `${one.slice(0, max - 1)}…` : one;
}

/** The one line a step's card shows under its name. */
export function summaryOf(kind: string, step: Step): string {
  const body = step[kind];
  const f = (body && typeof body === "object" ? body : {}) as Record<string, unknown>;
  switch (kind) {
    case "set": {
      const n = Object.keys(f).length;
      return n === 1 ? `1 field: ${Object.keys(f)[0]}` : `${n} fields`;
    }
    case "where":
      return short(body);
    case "if":
      return short(f.when);
    case "for_each":
      return `each of ${short(f.items, 30)}`;
    case "switch": {
      const n = Array.isArray(f.cases) ? f.cases.length : 0;
      return `${n} case${n === 1 ? "" : "s"} and a default`;
    }
    case "list": {
      const did = [f.filter && "keep", f.sort && "sort", f.unique && "each once", typeof f.limit === "number" && `first ${f.limit}`].filter(Boolean);
      return `${short(f.items, 22)}${did.length ? `: ${did.join(", ")}` : ""}`;
    }
    case "group":
      return `${short(f.items, 18)} by ${short(f.by, 18)}`;
    case "merge": {
      const n = Array.isArray(f.of) ? f.of.length : 0;
      return f.mode === "key" ? `${n} lists joined on ${short(f.key, 14)}` : f.mode === "fields" ? `${n} records as one` : `${n} lists as one`;
    }
    case "datetime":
      return [f.value ? short(f.value, 18) : "now", f.add && `+ ${short(f.add)}`, f.subtract && `- ${short(f.subtract)}`, f.timezone && `in ${short(f.timezone, 16)}`]
        .filter(Boolean)
        .join(" ");
    case "respond":
      return `answer ${typeof f.status === "number" ? f.status : 200}`;
    case "run_workflow":
      return f.workflow ? "another workflow" : "choose a workflow";
    case "http":
      return `${String(f.method ?? "GET")} ${short(f.url, 34)}`;
    case "ai":
      return short(f.model) || "a model's answer";
    case "embed":
      return short(f.model) || "vectors";
    // G7 more AI steps
    case "classify": {
      const n = Array.isArray(f.categories) ? f.categories.length : 0;
      return `${n} categor${n === 1 ? "y" : "ies"}${f.other === false ? "" : " or other"}`;
    }
    case "sentiment":
      return Array.isArray(f.labels) ? short(f.labels.map((l: unknown) => (typeof l === "string" ? l : (l as { name?: string }).name)).join(", ")) : "positive, neutral, negative";
    case "extract": {
      const n = Array.isArray(f.fields) ? f.fields.length : 0;
      return f.schema ? "fields to a JSON schema" : `${n} field${n === 1 ? "" : "s"}`;
    }
    case "summarise":
      return f.length ? `a summary, ${short(f.length, 30)}` : "a summary";
    case "agent": { // G2 AI agents
      const n = Array.isArray(f.tools) ? f.tools.length : 0;
      return `${n} tool${n === 1 ? "" : "s"}${f.memory ? ", with memory" : ""}`;
    }
    case "split_text": // retrieval
      return `by ${String(f.by ?? "paragraphs")}, ${typeof f.size === "number" ? f.size : 1000} characters`;
    case "vector_store": // retrieval
      return `${f.action === "search" ? "find in" : f.action === "delete" ? "delete from" : "add to"} ${short(f.table ?? f.collection, 28)}`; // vector stores
    case "ask_documents": // retrieval
      return `from ${short(f.table ?? f.collection, 30)}`; // vector stores
    case "lookup":
      return short(f.file);
    case "mysql": // sql databases
    case "sqlserver": // sql databases
    case "postgres":
      return Array.isArray(f.statements) ? `${f.statements.length} statements${f.transaction ? ", in one transaction" : ""}` : short(f.sql);
    case "email":
      return `to ${short(f.to, 36)}`;
    case "telegram":
    case "slack":
    case "discord":
    case "teams":
    case "google_chat":
    case "whatsapp":
      return short(f.text);
    case "topic":
      return `to ${short(f.name, 36)}`;
    case "javascript":
    case "python": {
      const code = typeof body === "string" ? body : String(f.code ?? "");
      const lines = code.split("\n").filter((l) => l.trim()).length;
      return `${lines} line${lines === 1 ? "" : "s"} of code`;
    }
    case "wait":
      // forms-and-resume
      if (f.until) return `until ${short(f.until, 36)}`;
      if (f.webhook) return "for a call to its address";
      return `for ${short(typeof body === "string" ? body : f.for)}`;
    case "form":
    case "approval":
      return short(f.title);
    case "stop":
      return f.error ? `fail: ${short(f.error, 34)}` : "end the run";
    case "mcp": // MCP
      return f.tool ? `call ${short(f.tool, 34)}` : "list the server's tools";
    case "table": // data tables
      return `${String(f.action ?? "get")} ${short(f.name, 30) || "a table"}`;
    case "state": // data tables
      return "set" in f ? `keep ${Object.keys((f.set ?? {}) as object).join(", ")}` : "delete" in f ? "forget values" : "clear" in f ? "forget all" : "read what it keeps";
    // G6 files
    case "extract_file":
      return `${f.format ? `${short(f.format, 8)} ` : ""}from ${short(f.file, 30)}`;
    case "convert":
      return `${short(f.format, 8)} file${f.name ? ` ${short(f.name, 28)}` : ""}`;
    case "file_details":
      return `of ${short(typeof body === "string" ? body : f.file, 34)}`;
    case "storage": // storage
    case "compress":
    case "redis":
      return storageSummary(kind, f);
    case "web": // web
      return webSummary(f);
    case "mailbox": // messaging
      return mailboxSummary(f);
    default:
      return "";
  }
}

/** What starts the workflow, in a few words. */
export function triggerSummary(trigger: unknown): { kind: string; line: string } {
  const [kind, settings] =
    typeof trigger === "string"
      ? [trigger, {}]
      : trigger && typeof trigger === "object"
        ? (Object.entries(trigger as Record<string, unknown>)[0] ?? ["manual", {}])
        : ["manual", {}];
  const s = (settings ?? {}) as Record<string, unknown>;
  switch (kind) {
    case "webhook":
      return { kind, line: "a webhook is called" };
    case "schedule":
      return { kind, line: scheduleLine(s) }; // cron-schedules
    case "topic":
      return { kind, line: `a message reaches ${String(s.name ?? "?")}` };
    case "failed":
      return { kind, line: "another workflow's run fails" };
    case "chat": // chat trigger (G8)
      return { kind, line: "someone writes in its chat" };
    case "storage": // storage
    case "redis":
      return { kind, line: storageTriggerLine(kind, s) };
    case "feed": // web
      return { kind, line: feedTriggerLine(s) };
    case "mailbox": // messaging
      return { kind, line: mailboxTriggerLine(s) };
    // forms-and-resume
    case "form":
      return { kind, line: "someone fills in a form" };
    case "mysql": // sql databases
    case "sqlserver":
    case "postgres":
      return { kind, line: `a row ${s.key || !/^id$/i.test(String(s.cursor ?? "")) ? "is added or changed in" : "is added to"} ${String(s.table ?? "?")}` };
    default: {
      // G3 app triggers: an app's own, in its trigger's words.
      const app = catalogApp(kind);
      const t = app?.triggers?.find((x) => x.name === s.when) ?? app?.triggers?.[0];
      if (app && t) return { kind, line: t.label.charAt(0).toLowerCase() + t.label.slice(1) };
      return { kind: "manual", line: "someone presses Run" };
    }
  }
}
