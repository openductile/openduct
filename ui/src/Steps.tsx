// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The steps of a workflow, as cards: each kind with its own fields; branches and loops hold steps of their own.

import { createContext, useContext, useEffect, useState } from "react";
import { Grow } from "./Grow";
import { api, type CredentialSummary, type Step } from "./api";
import { Icon } from "./flow/Icon";
import { FAMILY } from "./flow/look";
import { MORE_KINDS, MoreBody } from "./StepsMore";
import { AppBody, appStep, catalogApp, remember } from "./StepsApps";
import { AI_STEP_KINDS, AiStepBody, SchemaField, freshAi } from "./StepsAi"; // G7 more AI steps
import { aiInnerLists } from "./flow/aiBranches"; // G7 more AI steps
import { McpBody } from "./StepsMcp"; // MCP
import { RETRIEVAL_KINDS, RetrievalBody, retrievalStep } from "./StepsRetrieval"; // retrieval
import { TABLE_KINDS, TableStepBody } from "./StepsTables"; // data tables
import { Attachments, FILE_KINDS, FilesBody, HttpFiles, freshFile } from "./StepsFiles"; // G6 files
import { STORAGE_KINDS, StorageBody, freshStorage } from "./StepsStorage"; // storage
import { WebBody, freshWeb } from "./StepsWeb"; // web
import { MailboxBody, freshMailbox } from "./StepsMailbox"; // messaging
import type { AppInfo } from "./api";
import { AgentBody, agentStep } from "./StepsAgent"; // G2 AI agents
import { DATABASE_KINDS, DatabaseBody, DatabaseLimits, freshDatabase } from "./StepsDatabases"; // sql databases
// forms-and-resume
import { FormStepBody, WaitBody } from "./FormBuilder";

/** The workspace's credentials, for the steps that name one. */
export const CredentialsContext = createContext<CredentialSummary[]>([]);

/** The credential kinds each kind of step takes. */
export const TAKES: Record<string, string[]> = {
  ai: ["ai"],
  embed: ["ai"],
  classify: ["ai"], // G7 more AI steps
  sentiment: ["ai"],
  extract: ["ai"],
  summarise: ["ai"],
  agent: ["ai"], // G2 AI agents
  telegram: ["telegram"],
  slack: ["slack", "slack_webhook"],
  discord: ["discord_webhook"],
  teams: ["teams_webhook"],
  google_chat: ["google_chat_webhook"],
  whatsapp: ["whatsapp"],
  http: ["bearer", "header", "basic"],
  postgres: ["postgres"],
  mysql: ["mysql"], // sql databases
  sqlserver: ["sqlserver"], // sql databases
  email: ["smtp"],
  mcp: ["bearer", "header", "basic"], // MCP
  storage: ["sftp", "ftp", "s3", "azure_blob", "gcs", "dropbox", "box", "box_token", "microsoft_365", "google_drive"], // storage, storage places
  redis: ["redis"], // storage
  mailbox: ["imap"], // messaging
};

export const KINDS: { kind: string; label: string; hint: string }[] = [
  { kind: "set", label: "Set fields", hint: "Work out fields from what came in and from earlier steps" },
  { kind: "where", label: "Filter (only if)", hint: "Go on only when a condition is true" },
  { kind: "if", label: "Branch (if / else)", hint: "Take one branch or the other" },
  { kind: "switch", label: "Switch (several ways)", hint: "Take the first case whose condition holds, or the default" },
  { kind: "for_each", label: "Loop (for each)", hint: "Run steps for every item of a list" },
  { kind: "list", label: "List tools", hint: "Keep, sort, de-duplicate and limit a list's items" },
  { kind: "group", label: "Group and summarise", hint: "Count, sum, average, least and greatest per group" },
  { kind: "merge", label: "Merge", hint: "Make several lists, or records, one" },
  { kind: "datetime", label: "Date and time", hint: "Read, move and write a time, in a time zone" },
  { kind: "ai", label: "Ask AI", hint: "A model's answer, the same form whatever the model" },
  { kind: "embed", label: "Embed text", hint: "Vectors for search and retrieval" },
  // G7 more AI steps
  { kind: "classify", label: "Classify text", hint: "Sort a text into your categories; each category's steps run" },
  { kind: "sentiment", label: "Sentiment", hint: "Positive, neutral or negative (or your labels), with a score" },
  { kind: "extract", label: "Extract fields", hint: "Named fields with types pulled out of a text, checked" },
  { kind: "summarise", label: "Summarise", hint: "A text, or many, to a length and a style; long ones in pieces" },
  { kind: "agent", label: "AI agent", hint: "A model that uses tools over several turns to reach a goal, with memory" }, // G2 AI agents
  { kind: "split_text", label: "Split text", hint: "Cut documents into pieces for search" }, // retrieval
  { kind: "vector_store", label: "Vector store", hint: "Keep pieces with their vectors in Postgres or a vector database; find the nearest" }, // retrieval, vector stores
  { kind: "ask_documents", label: "Ask your documents", hint: "An answer from your documents, with its sources" }, // retrieval
  { kind: "lookup", label: "Look up a file", hint: "Find rows in a lookup file" },
  { kind: "table", label: "Data table", hint: "Get, add, update or delete rows of a table" }, // data tables
  { kind: "state", label: "Keep between runs", hint: "Values this workflow keeps from one run to the next" }, // data tables
  { kind: "postgres", label: "Query a database", hint: "SQL on Postgres; with pgvector, the texts nearest a question" },
  { kind: "mysql", label: "MySQL", hint: "SQL on MySQL or MariaDB, its values beside it" }, // sql databases
  { kind: "sqlserver", label: "SQL Server", hint: "SQL on SQL Server or Azure SQL, its values beside it" }, // sql databases
  { kind: "email", label: "Send an e-mail", hint: "Through your own mail server" },
  { kind: "javascript", label: "Code: JavaScript", hint: "A snippet of your own: return the result" },
  { kind: "python", label: "Code: Python", hint: "A snippet of your own: set result" },
  { kind: "http", label: "HTTP request", hint: "Call any API" },
  { kind: "mcp", label: "MCP tool", hint: "Call a tool of an MCP server, or list its tools" }, // MCP
  { kind: "telegram", label: "Telegram", hint: "Send a Telegram message" },
  { kind: "slack", label: "Slack", hint: "Post to a Slack channel" },
  { kind: "discord", label: "Discord", hint: "Post to a Discord channel" },
  { kind: "teams", label: "Teams", hint: "Post to a Microsoft Teams channel" },
  { kind: "google_chat", label: "Google Chat", hint: "Post to a Google Chat space" },
  { kind: "whatsapp", label: "WhatsApp", hint: "Send a WhatsApp message" },
  { kind: "topic", label: "Write to a topic", hint: "Keep a message for other workflows and apps" },
  { kind: "respond", label: "Respond to webhook", hint: "Answer the webhook's caller with a status and a body" },
  { kind: "run_workflow", label: "Run a workflow", hint: "Run another workflow of this workspace and use its result" },
  { kind: "wait", label: "Wait", hint: "Pause the run for a while, until a time, or for a call to its address" },
  // forms-and-resume
  { kind: "form", label: "Ask with a form", hint: "The run waits for a form to be filled in: its link goes out in a step before" },
  { kind: "approval", label: "Ask for approval", hint: "The run waits for someone to approve or reject" },
  { kind: "stop", label: "Stop", hint: "End the run, or fail it with a reason" },
  // G6 files
  { kind: "extract_file", label: "Extract from a file", hint: "Rows, a value or text from CSV, JSON, a spreadsheet, a web page or a PDF" },
  { kind: "convert", label: "Convert to a file", hint: "A CSV, JSON, XLSX or text file made of rows or fields" },
  { kind: "file_details", label: "File details", hint: "A file's name, type and size; rename it" },
  // storage
  { kind: "storage", label: "Files on a server or a folder", hint: "List, read, write, move, copy or delete files: SFTP, FTP, S3, Azure, Google Cloud, Dropbox, Box, OneDrive, Google Drive, or a folder here" }, // storage places
  { kind: "compress", label: "Zip and unzip", hint: "Pack files into a ZIP, gzip or tar archive, or unpack one" },
  { kind: "redis", label: "Redis", hint: "Get and set values, count, use lists as queues, publish to a channel" },
  { kind: "web", label: "Read a web page", hint: "A page's text, fields picked by CSS selectors, a site crawled, a sitemap or a feed" }, // web
  { kind: "mailbox", label: "Mailbox (IMAP)", hint: "List, search and read emails of any mailbox, their attachments as files; mark, move or delete them" }, // messaging
];

/**
 * The apps OpenDuct has (`/api/apps`): those this page has no form of its own for get one made from their definitions,
 * and join the steps a workflow may add.
 */
export function registerApps(apps: AppInfo[]) {
  for (const app of apps) {
    const own = KINDS.some((k) => k.kind === app.id) && !catalogApp(app.id);
    if (own) continue;
    remember(app);
    if (!KINDS.some((k) => k.kind === app.id)) {
      KINDS.push({ kind: app.id, label: app.name, hint: app.description || app.actions.map((a) => a.label).join(", ") });
    }
    TAKES[app.id] = app.sign_ins.map((s) => s.id);
    if (!FAMILY[app.id]) FAMILY[app.id] = app.category === "messaging" ? "message" : "call";
  }
}

export function kindOf(step: Step): string {
  return Object.keys(step).find((k) => KINDS.some((x) => x.kind === k)) ?? "set";
}

/** A new step of `kind`, named so no other step has its name. */
export function fresh(kind: string, taken: Set<string>): Step {
  let n = 1;
  while (taken.has(`${kind}_${n}`)) n++;
  const name = `${kind}_${n}`;
  if (AI_STEP_KINDS.includes(kind)) return freshAi(kind, name); // G7 more AI steps
  if (FILE_KINDS.includes(kind)) return freshFile(kind, name); // G6 files
  if (DATABASE_KINDS.includes(kind)) return freshDatabase(kind, name); // sql databases
  if (STORAGE_KINDS.includes(kind)) return freshStorage(kind, name); // storage
  if (kind === "web") return freshWeb(name); // web
  if (kind === "mailbox") return freshMailbox(name); // messaging
  switch (kind) {
    case "set":
      return { name, set: { field: "value.id" } };
    case "where":
      return { name, where: "value != null" };
    case "email":
      return { name, email: { credential: "", to: "{{ value.email }}", subject: "Order {{ value.id }}", text: "Hello {{ value.name }}," } };
    case "javascript":
      return { name, javascript: "// value: what came in; steps: earlier steps' results\nreturn { total: value.lines.reduce((t, l) => t + l.qty * l.price, 0) };" };
    case "python":
      return { name, python: "# value: what came in; steps: earlier steps' results\nresult = {\"total\": sum(l[\"qty\"] * l[\"price\"] for l in value[\"lines\"])}" };
    case "if":
      return { name, if: { when: "value.total > 100", then: [], else: [] } };
    case "for_each":
      return { name, for_each: { items: "value.items", steps: [] } };
    case "switch":
      return {
        name,
        switch: {
          cases: [
            { label: "big", when: "value.total > 500", steps: [] },
            { label: "middling", when: "value.total > 100", steps: [] },
          ],
          default: [],
        },
      };
    case "list":
      return { name, list: { items: "value.items", filter: "item.total > 0", sort: "item.total", order: "desc", limit: 10 } };
    case "group":
      return { name, group: { items: "value.items", by: "item.category", sum: { total: "item.total" } } };
    case "merge":
      return { name, merge: { of: ["value.items", "value.more"] } };
    case "datetime":
      return { name, datetime: { value: "value.at", timezone: "UTC", format: "%Y-%m-%d %H:%M" } };
    case "respond":
      return { name, respond: { status: 200, body: { ok: true, id: "{{ value.id }}" } } };
    case "run_workflow":
      return { name, run_workflow: { workflow: "" } };
    case "http":
      return { name, http: { method: "POST", url: "https://example.com/hook", json: { text: "{{ value }}" } } };
    case "ai":
      return { name, ai: { credential: "", model: "", system: "You are a helpful assistant.", prompt: "{{ value.text }}" } };
    case "embed":
      return { name, embed: { credential: "", model: "", input: "{{ value.text }}" } };
    case "agent": // G2 AI agents
      return agentStep(name);
    case "lookup":
      return { name, lookup: { file: "/rates.csv", where: { currency: "{{ value.currency }}" } } };
    case "table": // data tables
      return { name, table: { name: "", action: "get", where: { email: "{{ value.email }}" }, limit: 10 } };
    case "state": // data tables
      return { name, state: { set: { last_seen: "{{ value.id }}" } } };
    case "postgres":
      return { name, postgres: { credential: "", sql: "SELECT * FROM orders WHERE id = $1", params: ["value.id"] } };
    case "telegram":
      return { name, telegram: { credential: "", chat_id: "", text: "{{ value.text }}" } };
    case "slack":
      return { name, slack: { credential: "", channel: "#general", text: "{{ value.text }}" } };
    case "discord":
    case "teams":
    case "google_chat":
      return { name, [kind]: { credential: "", text: "{{ value.text }}" } };
    case "whatsapp":
      return { name, whatsapp: { credential: "", to: "", text: "{{ value.text }}" } };
    case "topic":
      return { name, topic: { name: "results", value: "{{ value }}" } };
    case "wait":
      return { name, wait: "10s" };
    case "mcp": // MCP
      return { name, mcp: { url: "https://tools.example.com/mcp" } };
    case "split_text": // retrieval
    case "vector_store": // retrieval
    case "ask_documents": // retrieval
      return retrievalStep(kind, name);
    // forms-and-resume
    case "form":
      return { name, form: { title: "A few details", fields: [{ name: "answer", label: "Your answer", type: "text", required: true }], timeout: "7d" } };
    case "approval":
      return { name, approval: { title: "Approve {{ value.id }}?", comment: true, timeout: "3d", on_timeout: "continue" } };
    default: {
      const app = catalogApp(kind);
      return app ? appStep(app, name) : { name, stop: {} };
    }
  }
}

/** The lists of steps a step holds: a branch's two, a switch's cases and default, a loop's own. */
export function innerLists(step: Step): Step[][] {
  const branch = step.if as { then?: Step[]; else?: Step[] } | undefined;
  const loop = step.for_each as { steps?: Step[] } | undefined;
  const choice = step.switch as { cases?: { steps?: Step[] }[]; default?: Step[] } | undefined;
  return [branch?.then, branch?.else, ...(choice?.cases ?? []).map((c) => c.steps), choice?.default, loop?.steps, ...aiInnerLists(step) /* G7 */].filter(
    (l): l is Step[] => Array.isArray(l),
  );
}

/** Every step name in a list, branches and loops included. */
export function names(steps: Step[], into = new Set<string>()): Set<string> {
  for (const step of steps) {
    if (typeof step.name === "string") into.add(step.name);
    for (const inner of innerLists(step)) names(inner, into);
  }
  return into;
}

type CardProps = {
  step: Step;
  taken: Set<string>;
  onChange: (s: Step) => void;
  onRemove: () => void;
  onUp: () => void;
  onDown: () => void;
};

export function StepCard(props: CardProps) {
  const kind = kindOf(props.step);
  const label = KINDS.find((k) => k.kind === kind)?.label ?? kind;
  const body = props.step[kind];
  const setBody = (value: unknown) => props.onChange({ ...props.step, [kind]: value });
  const retry = (props.step.retry as { attempts?: number; wait?: string } | undefined) ?? {};
  return (
    <section className={`card step family-${FAMILY[kind] ?? "data"}`}>
      <header>
        <span className="kind-chip">
          <span className="icon">
            <Icon kind={kind} />
          </span>
          {label}
        </span>
        <input
          className="name"
          aria-label="Step name"
          data-plain
          value={props.step.name ?? ""}
          onChange={(e) => props.onChange({ ...props.step, name: e.target.value.replace(/[^A-Za-z0-9_]/g, "_") })}
        />
        <span className="tools">
          <button className="icon-button" title="Move up" aria-label="Move up" onClick={props.onUp}>
            ↑
          </button>
          <button className="icon-button" title="Move down" aria-label="Move down" onClick={props.onDown}>
            ↓
          </button>
          <button className="icon-button remove" title="Remove the step" aria-label="Remove the step" onClick={props.onRemove}>
            ✕
          </button>
        </span>
      </header>
      <div className="step-body">
        <Body kind={kind} body={body} setBody={setBody} />
      </div>
      <details className="more">
        <summary>When it fails</summary>
        <div className="row">
          <label>
            Tries
            <input
              type="number"
              min={1}
              max={10}
              value={retry.attempts ?? 1}
              onChange={(e) => props.onChange({ ...props.step, retry: { ...retry, attempts: Number(e.target.value) } })}
            />
          </label>
          <label>
            Wait between
            <input
              value={retry.wait ?? "1s"}
              onChange={(e) => props.onChange({ ...props.step, retry: { ...retry, wait: e.target.value } })}
            />
          </label>
          <label>
            Then
            <select
              value={props.step.on_error === "continue" ? "continue" : "stop"}
              onChange={(e) => props.onChange({ ...props.step, on_error: e.target.value })}
            >
              <option value="stop">stop - to CatchErr</option>
              <option value="continue">go on to the next step</option>
            </select>
          </label>
        </div>
      </details>
    </section>
  );
}

type BodyProps = { kind: string; body: unknown; setBody: (v: unknown) => void };

function Body({ kind, body, setBody }: BodyProps) {
  const fields = (body ?? {}) as Record<string, unknown>;
  const set = (name: string, value: unknown) => setBody({ ...fields, [name]: value });
  if (MORE_KINDS.includes(kind)) return <MoreBody kind={kind} fields={fields} set={set} setBody={setBody} />;
  if (AI_STEP_KINDS.includes(kind)) return <AiStepBody kind={kind} fields={fields} set={set} setBody={setBody} />; // G7 more AI steps
  if (RETRIEVAL_KINDS.includes(kind)) return <RetrievalBody kind={kind} fields={fields} set={set} setBody={setBody} />; // retrieval
  if (TABLE_KINDS.includes(kind)) return <TableStepBody kind={kind} fields={fields} set={set} setBody={setBody} />; // data tables
  if (FILE_KINDS.includes(kind)) return <FilesBody kind={kind} fields={fields} set={set} setBody={setBody} />; // G6 files
  if (DATABASE_KINDS.includes(kind)) return <DatabaseBody kind={kind} fields={fields} set={set} setBody={setBody} />; // sql databases
  if (STORAGE_KINDS.includes(kind)) return <StorageBody kind={kind} fields={fields} set={set} setBody={setBody} />; // storage
  if (kind === "web") return <WebBody fields={fields} set={set} setBody={setBody} />; // web
  if (kind === "mailbox") return <MailboxBody fields={fields} set={set} setBody={setBody} />; // messaging
  const app = catalogApp(kind);
  if (app) return <AppBody app={app} fields={fields} set={set} setBody={setBody} />;
  switch (kind) {
    case "set":
      return <Pairs value={fields as Record<string, string>} onChange={setBody} keyLabel="Field" valueLabel="Expression" expressions />;
    case "where":
      return <Expr label="Go on only when" value={String(body ?? "")} onChange={setBody} />;
    case "javascript":
    case "python":
      return <ScriptBody kind={kind} body={body} setBody={setBody} />;
    case "if":
      return (
        <>
          <Expr label="When" value={String(fields.when ?? "")} onChange={(v) => set("when", v)} />
          <p className="hint">Its then and else steps are on the canvas: the + on each branch adds one.</p>
        </>
      );
    case "for_each":
      return (
        <>
          <Expr label="For each item of" value={String(fields.items ?? "")} onChange={(v) => set("items", v)} />
          <p className="hint">Inside, the item is item and its place is index. Its steps are in its box on the canvas.</p>
        </>
      );
    case "http":
      return (
        <>
          <CredentialPicker kind="http" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v || undefined)} optional />
          <HttpBody fields={fields} setBody={setBody} />
          <JsonField label="Sample answer (optional: a test run answers with it, sending nothing)" value={fields.sample} onChange={(v) => set("sample", v ?? undefined)} /> {/* test runs */}
        </>
      );
    case "ai":
      return <AiBody fields={fields} set={set} />;
    case "agent": // G2 AI agents
      return <AgentBody fields={fields} set={set} setBody={setBody} />;
    case "embed":
      return (
        <>
          <CredentialPicker kind="embed" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
          <ModelPicker credential={String(fields.credential ?? "")} value={String(fields.model ?? "")} onChange={(v) => set("model", v)} />
          <Text label="Text" value={String(fields.input ?? "")} onChange={(v) => set("input", v)} />
        </>
      );
    case "lookup":
      return (
        <>
          <Text label="File" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} placeholder="/rates.csv" />
          <p className="hint">Rows whose columns hold this text:</p>
          <Pairs value={(fields.where ?? {}) as Record<string, string>} onChange={(v) => set("where", v)} keyLabel="Column" valueLabel="Text" />
          <label className="check">
            <input type="checkbox" checked={fields.all === true} onChange={(e) => set("all", e.target.checked || undefined)} />
            Every matching row (not only the first)
          </label>
        </>
      );
    case "postgres":
      return <PostgresBody fields={fields} set={set} />;
    case "email":
      return (
        <>
          <CredentialPicker kind="email" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
          <Text label="To" value={String(fields.to ?? "")} onChange={(v) => set("to", v)} placeholder="addresses, separated by commas" wide />
          <div className="row">
            <Text label="Cc" value={String(fields.cc ?? "")} onChange={(v) => set("cc", v || undefined)} />
            <Text label="Bcc" value={String(fields.bcc ?? "")} onChange={(v) => set("bcc", v || undefined)} />
            <Text label="Reply to" value={String(fields.reply_to ?? "")} onChange={(v) => set("reply_to", v || undefined)} />
          </div>
          <Text label="Subject" value={String(fields.subject ?? "")} onChange={(v) => set("subject", v)} wide />
          <Area label="Text" hint="with {{ }} for values" value={String(fields.text ?? "")} onChange={(v) => set("text", v || undefined)} />
          <Area label="HTML" hint="optional: sent beside the text" value={String(fields.html ?? "")} onChange={(v) => set("html", v || undefined)} />
          <Attachments value={fields.attachments} onChange={(v) => set("attachments", v)} /> {/* G6 files */}
        </>
      );
    case "telegram":
    case "slack":
    case "discord":
    case "teams":
    case "google_chat":
    case "whatsapp":
      return <MessengerBody kind={kind} fields={fields} set={set} />;
    case "topic":
      return (
        <>
          <div className="row">
            <Text label="Topic" value={String(fields.name ?? "")} onChange={(v) => set("name", v)} />
            <Text label="Key" value={String(fields.key ?? "")} onChange={(v) => set("key", v || undefined)} />
          </div>
          <JsonField label="Message" value={fields.value ?? "{{ value }}"} onChange={(v) => set("value", v)} />
        </>
      );
    // forms-and-resume: a wait for a while, until a time, or for a call; a form; an approval.
    case "wait":
      return <Text label="Wait for" value={String(body ?? "")} onChange={setBody} placeholder="10s, 5m, 1h" />;
    case "mcp": // MCP
      return <McpBody fields={fields} set={set} setBody={setBody} />;
      return <WaitBody body={body} setBody={setBody} />;
    case "form":
    case "approval":
      return <FormStepBody kind={kind === "form" ? "form" : "approval"} fields={fields} setBody={setBody} />;
    default:
      return (
        <Text
          label="Fail with (empty: end well)"
          value={String(fields.error ?? "")}
          onChange={(v) => setBody(v ? { error: v } : {})}
        />
      );
  }
}

/** The credentials a step of `kind` may name. */
export function CredentialPicker(props: { kind: string; value: string; onChange: (v: string) => void; optional?: boolean }) {
  const credentials = useContext(CredentialsContext).filter((c) => TAKES[props.kind]?.includes(c.kind));
  return (
    <label>
      Credential {props.optional && <span className="hint">optional</span>}
      <select value={props.value} onChange={(e) => props.onChange(e.target.value)}>
        <option value="">{credentials.length ? "choose one" : "none yet: add one"}</option>
        {credentials.map((c) => (
          <option key={c.name} value={c.name}>
            {c.name} ({c.kind})
          </option>
        ))}
      </select>
    </label>
  );
}

/** The models the credential's service offers, asked of it; or a name typed, when it lists none. */
export function ModelPicker(props: { credential: string; value: string; onChange: (v: string) => void }) {
  const [models, setModels] = useState<{ id: string; name: string | null }[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [typing, setTyping] = useState(false);
  const [asked, setAsked] = useState(0);
  useEffect(() => {
    setModels(null);
    setTrouble("");
    if (!props.credential) return;
    let current = true;
    api
      .models(props.credential)
      .then((m) => current && setModels(m))
      .catch((e: Error) => current && setTrouble(e.message));
    return () => {
      current = false;
    };
  }, [props.credential, asked]);
  const listed = models && models.length > 0 && !typing;
  return (
    <label>
      <span className="field-label">
        Model
        {props.credential && (
          <button type="button" className="link" onClick={() => setAsked((n) => n + 1)} title="Ask the service again">
            refresh
          </button>
        )}
        {listed && (
          <button type="button" className="link" onClick={() => setTyping(true)}>
            type another
          </button>
        )}
      </span>
      {listed ? (
        <select value={props.value} onChange={(e) => props.onChange(e.target.value)}>
          <option value="">the credential's model</option>
          {props.value && !models!.some((m) => m.id === props.value) && <option value={props.value}>{props.value}</option>}
          {models!.map((m) => (
            <option key={m.id} value={m.id}>
              {m.name && m.name !== m.id ? `${m.name} (${m.id})` : m.id}
            </option>
          ))}
        </select>
      ) : (
        <input className="code" value={props.value} onChange={(e) => props.onChange(e.target.value)} placeholder={props.credential ? "the model's name (the credential's when empty)" : "choose a credential first"} spellCheck={false} />
      )}
      {trouble && <span className="hint">{trouble}: type its name</span>}
      {props.credential && !models && !trouble && <span className="hint">asking the service for its models...</span>}
    </label>
  );
}

function AiBody({ fields, set }: { fields: Record<string, unknown>; set: (name: string, value: unknown) => void }) {
  const json = fields.response_format === "json" || fields.json === true;
  return (
    <>
      <div className="row top">
        <CredentialPicker kind="ai" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
        <ModelPicker credential={String(fields.credential ?? "")} value={String(fields.model ?? "")} onChange={(v) => set("model", v)} />
      </div>
      <Area label="System prompt" hint="how the model should behave; {{ steps.writer.text }} uses another step's answer" value={String(fields.system ?? "")} onChange={(v) => set("system", v || undefined)} />
      <Area label="Prompt" hint="what to ask, with {{ }} for values" value={String(fields.prompt ?? "")} onChange={(v) => set("prompt", v)} />
      <div className="row">
        <label className="narrow">
          Temperature
          <input type="number" step="0.1" min={0} max={2} value={String(fields.temperature ?? "")} onChange={(e) => set("temperature", e.target.value === "" ? undefined : Number(e.target.value))} />
        </label>
        <label className="narrow">
          Max tokens
          <input type="number" min={1} value={String(fields.max_tokens ?? "")} onChange={(e) => set("max_tokens", e.target.value === "" ? undefined : Number(e.target.value))} />
        </label>
        <label className="check">
          <input type="checkbox" checked={json} onChange={(e) => set("response_format", e.target.checked ? "json" : undefined)} />
          Answer as JSON
        </label>
      </div>
      <SchemaField fields={fields} set={set} /> {/* G7 more AI steps */}
    </>
  );
}

const MESSENGER_FIELDS: Record<string, { name: string; label: string }[]> = {
  telegram: [{ name: "chat_id", label: "Chat id" }],
  slack: [{ name: "channel", label: "Channel (for a bot token)" }],
  discord: [],
  teams: [],
  google_chat: [],
  whatsapp: [{ name: "to", label: "To (phone number)" }],
};

function MessengerBody(props: { kind: string; fields: Record<string, unknown>; set: (name: string, value: unknown) => void }) {
  return (
    <>
      <CredentialPicker kind={props.kind} value={String(props.fields.credential ?? "")} onChange={(v) => props.set("credential", v)} />
      {(MESSENGER_FIELDS[props.kind] ?? []).map((f) => (
        <Text key={f.name} label={f.label} value={String(props.fields[f.name] ?? "")} onChange={(v) => props.set(f.name, v)} />
      ))}
      <Area label="Message" hint="with {{ }} for values" value={String(props.fields.text ?? "")} onChange={(v) => props.set("text", v)} />
    </>
  );
}

/** A script: its code, kept as the step's body - or with a time limit, as {code, timeout}. */
function ScriptBody(props: { kind: string; body: unknown; setBody: (b: unknown) => void }) {
  const fields = typeof props.body === "object" && props.body !== null ? (props.body as Record<string, unknown>) : { code: props.body };
  const code = String(fields.code ?? "");
  const timeout = String(fields.timeout ?? "");
  const state = fields.state === true; // data tables
  const keep = (next: { code: string; timeout: string; state?: boolean }) => {
    const kept = next.state ?? state;
    props.setBody(next.timeout || kept ? { code: next.code, ...(next.timeout ? { timeout: next.timeout } : {}), ...(kept ? { state: true } : {}) } : next.code);
  };
  return (
    <>
      <label>
        Code{" "}
        <span className="hint">
          {props.kind === "javascript"
            ? "given value, steps and trigger; return the result; console.log for the logs"
            : "given value, steps and trigger; set result; print for the logs"}
        </span>
        <Grow className="code script" rows={6} max={560} value={code} onChange={(v) => keep({ code: v, timeout })} />
      </label>
      <div className="row">
        <label className="narrow">
          Time limit <span className="hint">up to 60s</span>
          <input className="code" placeholder="10s" value={timeout} onChange={(e) => keep({ code, timeout: e.target.value })} />
        </label>
        <p className="hint">It runs on its own, with no files, network or programs: it only works out its answer.</p>
      </div>
      {/* data tables */}
      <label className="check">
        <input type="checkbox" checked={state} onChange={(e) => keep({ code, timeout, state: e.target.checked })} />
        Give it the workflow's state (state): what it leaves there is kept for the next run
      </label>
    </>
  );
}

function PostgresBody({ fields, set }: { fields: Record<string, unknown>; set: (name: string, value: unknown) => void }) {
  const params = Array.isArray(fields.params) ? (fields.params as unknown[]).map(String) : [];
  const change = (next: string[]) => set("params", next.length ? next : undefined);
  return (
    <>
      <CredentialPicker kind="postgres" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
      <DatabaseLimits fields={fields} set={set} /> {/* sql databases */}
      <Area
        label="SQL"
        hint="values go in as $1, $2 ... - never written into the SQL"
        value={String(fields.sql ?? "")}
        onChange={(v) => set("sql", v)}
      />
      <p className="hint">Values, each an expression: the first for $1, the next for $2 ...</p>
      {params.map((p, i) => (
        <div className="row" key={i}>
          <label className="wide">
            {`$${i + 1}`}
            <input className="code" data-expr value={p} onChange={(e) => change(params.map((q, j) => (j === i ? e.target.value : q)))} />
          </label>
          <button type="button" className="quiet" onClick={() => change(params.filter((_, j) => j !== i))} aria-label={`Remove $${i + 1}`}>
            Remove
          </button>
        </div>
      ))}
      <button type="button" className="quiet" onClick={() => change([...params, "value."])}>
        Add a value
      </button>
    </>
  );
}

export function Area(props: { label: string; hint?: string; value: string; onChange: (v: string) => void }) {
  return (
    <label>
      {props.label} {props.hint && <span className="hint">{props.hint}</span>}
      <Grow className="code" value={props.value} onChange={props.onChange} />
    </label>
  );
}

function HttpBody({ fields, setBody }: { fields: Record<string, unknown>; setBody: (v: unknown) => void }) {
  const set = (name: string, value: unknown) => {
    const next: Record<string, unknown> = { ...fields, [name]: value };
    if (value === undefined) delete next[name];
    setBody(next);
  };
  const bodyKind = "json" in fields ? "json" : "body" in fields ? "text" : "form" in fields ? "form" : "none";
  // G6 files: a file, or a form with files, as the body.
  const kind = "file" in fields ? "file" : "multipart" in fields ? "multipart" : bodyKind;
  const setBodyKind = (k: string) => {
    const next: Record<string, unknown> = { ...fields };
    delete next.json;
    delete next.body;
    delete next.form;
    delete next.file; // G6 files
    delete next.multipart; // G6 files
    if (k === "json") next.json = {};
    if (k === "text") next.body = "";
    if (k === "form") next.form = {};
    if (k === "file") next.file = ""; // G6 files
    if (k === "multipart") next.multipart = {}; // G6 files
    setBody(next);
  };
  return (
    <>
      <div className="row">
        <label className="narrow">
          Method
          <select value={String(fields.method ?? "GET")} onChange={(e) => set("method", e.target.value)}>
            {["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"].map((m) => (
              <option key={m}>{m}</option>
            ))}
          </select>
        </label>
        <Text label="URL" value={String(fields.url ?? "")} onChange={(v) => set("url", v)} wide />
      </div>
      <details>
        <summary>Headers and query</summary>
        <Pairs value={(fields.headers ?? {}) as Record<string, string>} onChange={(v) => set("headers", v)} keyLabel="Header" valueLabel="Text" />
        <Pairs value={(fields.query ?? {}) as Record<string, string>} onChange={(v) => set("query", v)} keyLabel="Query" valueLabel="Text" />
      </details>
      <label>
        Body
        <select value={kind} onChange={(e) => setBodyKind(e.target.value)}>
          <option value="none">none</option>
          <option value="json">JSON</option>
          <option value="text">text</option>
          <option value="form">form</option>
          <option value="file">a file</option> {/* G6 files */}
          <option value="multipart">a form with files</option> {/* G6 files */}
        </select>
      </label>
      <HttpFiles fields={fields} set={set} bodyKind={kind} /> {/* G6 files */}
      {bodyKind === "json" && <JsonField label="JSON" value={fields.json} onChange={(v) => set("json", v)} />}
      {bodyKind === "text" && <Area label="Text" value={String(fields.body ?? "")} onChange={(v) => set("body", v)} />}
      {bodyKind === "form" && (
        <Pairs value={(fields.form ?? {}) as Record<string, string>} onChange={(v) => set("form", v)} keyLabel="Field" valueLabel="Text" />
      )}
    </>
  );
}

export function Expr(props: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <label>
      {props.label} <span className="hint">an expression, as value.total &gt; 100</span>
      <Grow className="code" oneLine data-expr="" value={props.value} onChange={props.onChange} />
    </label>
  );
}

export function Text(props: { label: string; value: string; onChange: (v: string) => void; placeholder?: string; wide?: boolean }) {
  return (
    <label className={props.wide ? "wide" : undefined}>
      {props.label}
      <Grow className="code" oneLine value={props.value} placeholder={props.placeholder} onChange={props.onChange} />
    </label>
  );
}

/** Names and their values, kept in the order written. */
export function Pairs(props: {
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  keyLabel: string;
  valueLabel: string;
  /** Its values are expressions (value.total), not text with {{ }}. */
  expressions?: boolean;
}) {
  const rows = Object.entries(props.value ?? {});
  const write = (next: [string, string][]) => props.onChange(Object.fromEntries(next));
  return (
    <div className="pairs">
      {rows.map(([k, v], i) => (
        <div className="pair" key={i}>
          <input
            aria-label={props.keyLabel}
            placeholder={props.keyLabel}
            data-plain
            value={k}
            onChange={(e) => write(rows.map((r, j) => (j === i ? [e.target.value, r[1]] : r)))}
          />
          <Grow
            aria-label={props.valueLabel}
            className="code"
            oneLine
            data-expr={props.expressions ? "" : undefined}
            placeholder={props.valueLabel}
            value={String(v)}
            onChange={(next) => write(rows.map((r, j) => (j === i ? [r[0], next] : r)))}
          />
          <button className="quiet" title="Remove" onClick={() => write(rows.filter((_, j) => j !== i))}>
            ✕
          </button>
        </div>
      ))}
      <button className="quiet" onClick={() => write([...rows, [`${props.keyLabel.toLowerCase()}${rows.length + 1}`, ""]])}>
        + {props.keyLabel}
      </button>
    </div>
  );
}

/** JSON written as text, with {{ }} in its strings; kept as it is written until it reads as JSON. */
export function JsonField(props: { label: string; value: unknown; onChange: (v: unknown) => void }) {
  const [text, setText] = useState(() => JSON.stringify(props.value ?? null, null, 2));
  const [bad, setBad] = useState("");
  return (
    <label>
      {props.label}
      <Grow
        className="code"
        value={text}
        onChange={(v) => {
          setText(v);
          try {
            props.onChange(JSON.parse(v));
            setBad("");
          } catch {
            setBad("not JSON yet");
          }
        }}
      />
      {bad && <span className="error">{bad}</span>}
    </label>
  );
}
