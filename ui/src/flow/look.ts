// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// How each kind of step looks on the canvas: its family (a colour), its icon, and the one line that says what it
// does. Messengers get plain icons with their names, not their makers' marks.

import type { Step } from "../api";

export type Family = "start" | "logic" | "data" | "ai" | "call" | "message" | "code" | "control";

export const FAMILY: Record<string, Family> = {
  set: "data",
  lookup: "data",
  postgres: "data",
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
};

/** Each icon is SVG path data on a 24x24 grid, drawn with a stroke. */
export const ICON: Record<string, string> = {
  start: "M13 2 4 14h7l-1 8 9-12h-7z",
  webhook: "M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1",
  schedule: "M12 7v5l3 2M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z",
  topic: "M4 6h16M4 12h10M4 18h13",
  manual: "M8 5v14l11-7z",
  failed: "M12 3 2 20h20zM12 9v5M12 17h.01",
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
    case "lookup":
      return short(f.file);
    case "postgres":
      return short(f.sql);
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
      return `for ${short(typeof body === "string" ? body : f.for)}`;
    case "stop":
      return f.error ? `fail: ${short(f.error, 34)}` : "end the run";
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
      return { kind, line: `every ${String(s.every ?? "?")}` };
    case "topic":
      return { kind, line: `a message reaches ${String(s.name ?? "?")}` };
    case "failed":
      return { kind, line: "another workflow's run fails" };
    default:
      return { kind: "manual", line: "someone presses Run" };
  }
}
