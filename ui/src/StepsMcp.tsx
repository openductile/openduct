// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The MCP step's form: the server's address and credential, its tools listed with a click (asked the way a run asks:
// with the credential, and not on this machine's own network unless allowed), the tool chosen and its arguments -
// one field each, from its definition, with {{ }} for values from the run.

import { useState } from "react";
import { api, type McpToolInfo } from "./api";
import { CredentialPicker, JsonField, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The arguments a tool's schema names: each with its type and whether it is needed. */
function argumentsOf(tool: McpToolInfo | undefined): { name: string; type: string; required: boolean; description: string }[] {
  const schema = tool?.input_schema as { properties?: Record<string, { type?: string; description?: string }>; required?: string[] } | undefined;
  return Object.entries(schema?.properties ?? {}).map(([name, p]) => ({
    name,
    type: String(p?.type ?? "string"),
    required: schema?.required?.includes(name) ?? false,
    description: String(p?.description ?? ""),
  }));
}

export function McpBody({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const [tools, setTools] = useState<McpToolInfo[] | null>(null);
  const [asking, setAsking] = useState(false);
  const [trouble, setTrouble] = useState("");
  const url = String(fields.url ?? "");
  const credential = String(fields.credential ?? "");
  const tool = typeof fields.tool === "string" ? fields.tool : "";
  const args = (fields.arguments && typeof fields.arguments === "object" ? fields.arguments : {}) as Record<string, unknown>;
  const chosen = tools?.find((t) => t.name === tool);
  const list = async () => {
    setAsking(true);
    setTrouble("");
    try {
      setTools((await api.mcpTools(url, credential)).tools);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot list its tools");
    } finally {
      setAsking(false);
    }
  };
  const choose = (name: string) => {
    const next: Fields = { ...fields, tool: name || undefined };
    if (!name) delete next.arguments;
    setBody(next);
  };
  return (
    <>
      <Text label="Server address" value={url} onChange={(v) => set("url", v)} placeholder="https://tools.example.com/mcp" wide />
      <div className="row top">
        <CredentialPicker kind="mcp" value={credential} onChange={(v) => set("credential", v || undefined)} optional />
        <label>
          <span className="field-label">
            Tool
            <button type="button" className="link" aria-label="List the server's tools" disabled={!url || asking} onClick={list}>
              {asking ? "asking…" : "list its tools"}
            </button>
          </span>
          {tools ? (
            <select aria-label="Tool" value={tool} onChange={(e) => choose(e.target.value)}>
              <option value="">none: list the server's tools</option>
              {tool && !chosen && <option value={tool}>{tool}</option>}
              {tools.map((t) => (
                <option key={t.name} value={t.name} title={t.description}>
                  {t.title && t.title !== t.name ? `${t.title} (${t.name})` : t.name}
                </option>
              ))}
            </select>
          ) : (
            <input aria-label="Tool" className="code" value={tool} placeholder="its name; empty: list the server's tools" onChange={(e) => choose(e.target.value)} spellCheck={false} />
          )}
        </label>
      </div>
      {trouble && <p className="error">{trouble}</p>}
      {chosen?.description && <p className="hint">{chosen.description}</p>}
      {tool && chosen && argumentsOf(chosen).length > 0 ? (
        argumentsOf(chosen).map((a) => (
          <Text
            key={a.name}
            label={`${a.name}${a.required ? "" : " (optional)"}${a.description ? ` - ${a.description}` : ""}`}
            value={typeof args[a.name] === "string" ? String(args[a.name]) : args[a.name] === undefined ? "" : JSON.stringify(args[a.name])}
            placeholder={a.type === "string" ? "text, with {{ }} for values" : `a ${a.type}, or {{ }}`}
            onChange={(v) => {
              const next = { ...args };
              if (v === "") delete next[a.name];
              else if (a.type !== "string" && /^-?\d+(\.\d+)?$|^(true|false)$/.test(v)) next[a.name] = JSON.parse(v);
              else next[a.name] = v;
              set("arguments", Object.keys(next).length ? next : undefined);
            }}
          />
        ))
      ) : tool ? (
        <JsonField key={tool} label="Arguments" value={fields.arguments ?? {}} onChange={(v) => set("arguments", v)} />
      ) : null}
      <div className="row">
        <label className="check">
          <input type="checkbox" checked={fields.read_only === true} onChange={(e) => set("read_only", e.target.checked || undefined)} />
          It only reads: a preview may call it
        </label>
        <label className="check">
          <input type="checkbox" checked={fields.fail_on_error !== false} onChange={(e) => set("fail_on_error", e.target.checked ? undefined : false)} />
          Its error fails the step
        </label>
        <label className="narrow">
          Time limit
          <input className="code" placeholder="30s" value={String(fields.timeout ?? "")} onChange={(e) => set("timeout", e.target.value || undefined)} />
        </label>
      </div>
      <p className="hint">The tool's answer is steps.&lt;this step&gt;.answer (its text in .text); without a tool, its tools are .tools.</p>
    </>
  );
}
