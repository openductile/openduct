// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The AI agent step: its model, instructions and goal; its tools, each a name, what it does, the step it runs and the
// arguments the model fills, and whether it may run; its memory, the shape of its answer, and its limits. And what a
// run of it did: every turn and every tool call.

import { Area, CredentialPicker, Expr, JsonField, KINDS, ModelPicker, Text } from "./Steps";
import { catalogApp } from "./StepsApps";
import { WorkflowPicker } from "./StepsMore";
import { MemoryStoreFields } from "./memoryStores"; // vector stores
import type { StepRun } from "./api";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;
type Tool = Record<string, unknown> & { name?: string; description?: string; parameters?: unknown };

/** What a tool may run, beside the apps' actions: each with the body a new one starts with. */
const TOOL_KINDS: { kind: string; label: string; body: () => unknown }[] = [
  { kind: "http", label: "HTTP request", body: () => ({ method: "GET", url: "https://example.com/items/{{ args.id }}" }) },
  { kind: "run_workflow", label: "Another workflow", body: () => ({ workflow: "" }) },
  { kind: "javascript", label: "JavaScript", body: () => "return { answer: args };" },
  { kind: "python", label: "Python", body: () => "result = args" },
  { kind: "agent", label: "Another agent", body: () => ({ credential: "", instructions: "" }) },
];
const NOT_KIND = ["name", "description", "parameters"];

/** A new agent step. */
export function agentStep(name: string) {
  return {
    name,
    agent: {
      credential: "",
      instructions: "You are a helpful assistant. Use the tools when they help.",
      input: "{{ value.text }}",
      tools: [],
    },
  };
}

/** The kind of step a tool runs. */
export function toolKind(tool: Tool): string {
  return Object.keys(tool).find((k) => !NOT_KIND.includes(k)) ?? "http";
}

function kindLabel(kind: string): string {
  return TOOL_KINDS.find((k) => k.kind === kind)?.label ?? catalogApp(kind)?.name ?? kind;
}

/** The apps whose actions a tool may run. */
function appKinds(): { kind: string; label: string }[] {
  return KINDS.filter((k) => catalogApp(k.kind)).map((k) => ({ kind: k.kind, label: k.label }));
}

function freshTool(kind: string, taken: string[]): Tool {
  let n = 1;
  while (taken.includes(`tool_${n}`)) n++;
  const app = catalogApp(kind);
  const body = app ? { credential: "", ...(app.default_action ? {} : { action: app.actions[0]?.name }) } : TOOL_KINDS.find((k) => k.kind === kind)?.body();
  const parameters =
    kind === "http"
      ? { type: "object", properties: { id: { type: "string", description: "The item's id" } }, required: ["id"] }
      : kind === "javascript" || kind === "python" || kind === "run_workflow"
        ? { type: "object", properties: {} }
        : undefined;
  return { name: `tool_${n}`, description: "", ...(parameters ? { parameters } : {}), [kind]: body };
}

export function AgentBody({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (b: unknown) => void }) {
  const tools = (Array.isArray(fields.tools) ? fields.tools : []) as Tool[];
  const allow = Array.isArray(fields.allow) ? (fields.allow as string[]) : null;
  const names = tools.map((t) => String(t.name ?? ""));
  // The tools and which may run, in one change: every tool may run unless some may not.
  const writeTools = (next: Tool[], nextAllow: string[] | null = allow) => {
    const kept = nextAllow?.filter((n) => next.some((t) => t.name === n)) ?? null;
    const body: Fields = { ...fields, tools: next };
    if (kept && kept.length < next.length) body.allow = kept;
    else delete body.allow;
    setBody(body);
  };
  const mayRun = (name: string) => allow === null || allow.includes(name);
  const setMayRun = (name: string, on: boolean) => {
    const now = allow ?? names;
    writeTools(tools, on ? [...new Set([...now, name])] : now.filter((n) => n !== name));
  };
  const memory = fields.memory && typeof fields.memory === "object" ? (fields.memory as Fields) : null;
  const schema = fields.output && typeof fields.output === "object" ? (fields.output as Fields).schema : undefined;
  const outputKind = schema !== undefined ? "schema" : fields.output === "json" ? "json" : "text";
  const number = (name: string) => (
    <label className="narrow" key={name}>
      {name.replace(/_/g, " ")}
      <input
        type="number"
        min={name === "max_tool_calls" ? 0 : 1}
        value={String(fields[name] ?? "")}
        onChange={(e) => set(name, e.target.value === "" ? undefined : Number(e.target.value))}
      />
    </label>
  );
  return (
    <>
      <div className="row top">
        <CredentialPicker kind="agent" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
        <ModelPicker credential={String(fields.credential ?? "")} value={String(fields.model ?? "")} onChange={(v) => set("model", v || undefined)} />
      </div>
      <Area label="Instructions" hint="how the agent behaves and when to use which tool" value={String(fields.instructions ?? "")} onChange={(v) => set("instructions", v || undefined)} />
      <Area label="Input" hint="its goal in each run, with {{ }} for values" value={String(fields.input ?? "")} onChange={(v) => set("input", v)} />
      <p className="field-label">Tools</p>
      <p className="hint">Each turn the model chooses tools to call, or answers. A tool reads what the model gives it as args (args.id); untick one it may not run yet.</p>
      <ol className="tools-list">
        {tools.map((tool, i) => (
          <ToolCard
            key={i}
            tool={tool}
            mayRun={mayRun(String(tool.name ?? ""))}
            onMayRun={(on) => setMayRun(String(tool.name ?? ""), on)}
            onChange={(next) => {
              const renamed = next.name !== tool.name && allow ? allow.map((n) => (n === tool.name ? String(next.name) : n)) : allow;
              writeTools(
                tools.map((t, j) => (j === i ? next : t)),
                renamed,
              );
            }}
            onRemove={() => writeTools(tools.filter((_, j) => j !== i))}
          />
        ))}
      </ol>
      <label className="narrow">
        Add a tool
        <select
          aria-label="Add a tool"
          value=""
          onChange={(e) => {
            if (!e.target.value) return;
            // A new tool may run, until it is unticked.
            const added = freshTool(e.target.value, names);
            writeTools([...tools, added], allow && [...allow, String(added.name)]);
          }}
        >
          <option value="">choose what it runs</option>
          {TOOL_KINDS.map((k) => (
            <option key={k.kind} value={k.kind}>
              {k.label}
            </option>
          ))}
          {appKinds().map((k) => (
            <option key={k.kind} value={k.kind}>
              {k.label} (an action)
            </option>
          ))}
        </select>
      </label>
      <details className="more" open={memory !== null}>
        <summary>Memory</summary>
        <label className="check">
          <input
            type="checkbox"
            checked={memory !== null}
            onChange={(e) => set("memory", e.target.checked ? { key: "{{ value.chat_id }}", window: 10 } : undefined)}
          />
          Remember each conversation between runs
        </label>
        {memory && (
          <>
            <div className="row">
              <Text label="Whose conversation" value={String(memory.key ?? "")} onChange={(v) => set("memory", { ...memory, key: v })} placeholder="{{ value.chat_id }}" />
              <label className="narrow">
                Last turns kept
                <input type="number" min={1} max={50} value={String(memory.window ?? 10)} onChange={(e) => set("memory", { ...memory, window: Number(e.target.value) })} />
              </label>
              <Text label="Forget after" value={String(memory.forget_after ?? "")} onChange={(v) => set("memory", { ...memory, forget_after: v || undefined })} placeholder="30d" />
            </div>
            <Expr label="Start afresh when" value={String(memory.clear ?? "")} onChange={(v) => set("memory", { ...memory, clear: v || undefined })} />
            <MemoryStoreFields memory={memory} put={(m) => set("memory", m)} /> {/* vector stores */}
          </>
        )}
      </details>
      <details className="more" open={outputKind !== "text"}>
        <summary>Answer</summary>
        <label className="narrow">
          Answer as
          <select
            value={outputKind}
            onChange={(e) =>
              set("output", e.target.value === "text" ? undefined : e.target.value === "json" ? "json" : { schema: { type: "object", properties: {} } })
            }
          >
            <option value="text">text</option>
            <option value="json">JSON</option>
            <option value="schema">JSON matching a schema</option>
          </select>
        </label>
        {outputKind === "schema" && <JsonField label="Schema" value={schema} onChange={(v) => set("output", { schema: v })} />}
        {outputKind !== "text" && <p className="hint">Checked when it answers; one more turn to put a wrong answer right.</p>}
      </details>
      <details className="more">
        <summary>Limits</summary>
        <div className="row">
          {["max_turns", "max_tool_calls", "max_tokens", "max_total_tokens"].map(number)}
          <Text label="Time" value={String(fields.timeout ?? "")} onChange={(v) => set("timeout", v || undefined)} placeholder="5m" />
        </div>
        <p className="hint">By default 10 turns, 20 tool calls, 1,024 tokens an answer, 200,000 in all, and 5 minutes.</p>
      </details>
    </>
  );
}

function ToolCard(props: { tool: Tool; mayRun: boolean; onMayRun: (on: boolean) => void; onChange: (t: Tool) => void; onRemove: () => void }) {
  const { tool } = props;
  const kind = toolKind(tool);
  const body = tool[kind];
  const fields = (body && typeof body === "object" ? body : {}) as Fields;
  const setBody = (value: unknown) => props.onChange({ ...tool, [kind]: value });
  const setField = (name: string, value: unknown) => {
    const next: Fields = { ...fields, [name]: value };
    if (value === undefined) delete next[name];
    setBody(next);
  };
  const app = catalogApp(kind);
  return (
    <li className="tool">
      <div className="row">
        <input
          className="name"
          aria-label="Tool name"
          data-plain
          value={String(tool.name ?? "")}
          onChange={(e) => props.onChange({ ...tool, name: e.target.value.replace(/[^A-Za-z0-9_-]/g, "_") })}
        />
        <span className="kind-chip">{kindLabel(kind)}</span>
        <label className="check">
          <input type="checkbox" checked={props.mayRun} onChange={(e) => props.onMayRun(e.target.checked)} />
          may run
        </label>
        <button className="icon-button remove" title="Remove the tool" aria-label="Remove the tool" onClick={props.onRemove}>
          ✕
        </button>
      </div>
      <Text label="What it does" value={String(tool.description ?? "")} onChange={(v) => props.onChange({ ...tool, description: v })} placeholder="for the model to choose it by" wide />
      {kind === "http" && (
        <div className="row">
          <label className="narrow">
            Method
            <select value={String(fields.method ?? "GET")} onChange={(e) => setField("method", e.target.value)}>
              {["GET", "POST", "PUT", "PATCH", "DELETE"].map((m) => (
                <option key={m}>{m}</option>
              ))}
            </select>
          </label>
          <Text label="URL" value={String(fields.url ?? "")} onChange={(v) => setField("url", v)} wide />
        </div>
      )}
      {kind === "run_workflow" && <WorkflowPicker label="Workflow" value={String(fields.workflow ?? "")} onChange={(v) => setField("workflow", v)} />}
      {(kind === "javascript" || kind === "python") && (
        <Area label="Code" hint={kind === "javascript" ? "given args; return the result" : "given args; set result"} value={typeof body === "string" ? body : String(fields.code ?? "")} onChange={setBody} />
      )}
      {kind === "agent" && (
        <>
          <CredentialPicker kind="agent" value={String(fields.credential ?? "")} onChange={(v) => setField("credential", v)} />
          <Area label="Its instructions" value={String(fields.instructions ?? "")} onChange={(v) => setField("instructions", v || undefined)} />
          <p className="hint">The model gives it its task as args.input.</p>
        </>
      )}
      {app && (
        <>
          <div className="row">
            <CredentialPicker kind={kind} value={String(fields.credential ?? "")} onChange={(v) => setField("credential", v)} />
            {app.actions.length > 1 && (
              <label>
                Action
                <select value={String(fields.action ?? app.default_action ?? "")} onChange={(e) => setField("action", e.target.value)}>
                  {app.actions.map((a) => (
                    <option key={a.name} value={a.name}>
                      {a.label}
                      {a.write ? "" : " (reads)"}
                    </option>
                  ))}
                </select>
              </label>
            )}
          </div>
          <p className="hint">The model fills the action's inputs this step does not give.</p>
        </>
      )}
      <details>
        <summary>Arguments the model fills</summary>
        <JsonField
          label="JSON schema"
          value={tool.parameters ?? { type: "object", properties: {} }}
          onChange={(v) => props.onChange({ ...tool, parameters: v })}
        />
      </details>
    </li>
  );
}

type Turn = { turn: number; ms: number; prompt_tokens?: number | null; completion_tokens?: number | null; tool_calls?: string[] };
type Call = { turn: number; tool: string; arguments: unknown; result: unknown; ms: number; error?: string };

/** An agent's run: each tool call - what it was given, what came back, how long it took - and each turn's tokens. */
export function AgentCalls({ step }: { step: StepRun }) {
  const r = (step.result ?? {}) as { turns?: Turn[]; calls?: Call[]; memory?: { recalled?: number; kept?: number; cleared?: boolean; error?: string } };
  const calls = r.calls ?? [];
  const turns = r.turns ?? [];
  return (
    <div className="agent-calls">
      {calls.length > 0 && (
        <table className="runs">
          <thead>
            <tr>
              <th>Turn</th>
              <th>Tool</th>
              <th>Given</th>
              <th>Gave back</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {calls.map((c, i) => (
              <tr key={i} className={c.error ? "bad" : undefined}>
                <td>{c.turn}</td>
                <td>{c.tool}</td>
                <td>
                  <code>{JSON.stringify(c.arguments)}</code>
                </td>
                <td>{c.error ? <span className="error">{c.error}</span> : <code>{typeof c.result === "string" ? c.result : JSON.stringify(c.result)}</code>}</td>
                <td>{c.ms} ms</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <p className="chips">
        <span>
          {turns.length} turn{turns.length === 1 ? "" : "s"}
        </span>
        <span>
          {calls.length} tool call{calls.length === 1 ? "" : "s"}
        </span>
        {r.memory && (
          <span>
            {r.memory.cleared ? "memory cleared" : `remembered ${r.memory.recalled ?? 0}`}, kept {r.memory.kept ?? 0}
          </span>
        )}
        {r.memory?.error && <span className="warn">memory not kept: {r.memory.error}</span>}
      </p>
    </div>
  );
}
