// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The workspace's MCP server, on its settings page: the address AI assistants reach its tools at, the workflows it
// offers as tools (and why one is not offered), and the tokens assistants sign in with - each made by an admin,
// shown once, limited to some tools and so many requests a minute, and revoked with a click.

import { useEffect, useState, type FormEvent } from "react";
import { api, type McpServerInfo, type McpToken } from "./api";
import { Sym } from "./Sym";
import { Copy } from "./Trigger";
import "./styles-mcp.css";

export function McpSettings(props: { workspace: string; onGo: (path: string) => void }) {
  const [server, setServer] = useState<McpServerInfo | null>(null);
  const [trouble, setTrouble] = useState("");
  const [made, setMade] = useState<McpToken | null>(null);
  const load = () =>
    api
      .mcpServer()
      .then(setServer)
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    setServer(null);
    setMade(null);
    load();
  }, [props.workspace]);
  if (!server) return <p className="hint set-loading">{trouble || "…"}</p>;
  const address = `${location.origin}${server.path}`;
  const named = (id: string) => server.tools.find((t) => t.workflow === id)?.name ?? id;
  return (
    <div className="mcp-settings">
      <div className="set-row">
        <div className="set-name">
          <span>Address</span>
          <span className="hint">AI assistants that speak MCP reach this workspace's tools here, signed in with a token</span>
        </div>
        <div className="set-control tg-copyable">
          <code title={address}>{address}</code>
          <Copy text={address} label="Copy the address" />
        </div>
      </div>
      <div className="set-row">
        <div className="set-name">
          <span>Tools</span>
          <span className="hint">Workflows offered as tools: set it in a workflow's "Starts when" window</span>
        </div>
        <div className="set-control">
          {server.tools.length === 0 ? (
            <span className="set-value">None yet</span>
          ) : (
            <ul className="mcp-tools" aria-label="Tools offered">
              {server.tools.map((t) => (
                <li key={t.workflow}>
                  <button className="link" onClick={() => props.onGo(`/workflows/${t.workflow}`)}>
                    <code>{t.name}</code>
                  </button>{" "}
                  <span className="hint">
                    {t.workflow_name}
                    {t.taken_by ? ` - not offered: ${t.taken_by} has this name` : t.active ? "" : " - off: turn it on to offer it"}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
      <div className="set-row">
        <div className="set-name">
          <span>Tokens</span>
          <span className="hint">{server.manage ? "Each assistant its own: revoke one and it no longer signs in" : "Its admins make and revoke them"}</span>
        </div>
        <div className="set-control">
          {server.tokens.length === 0 ? (
            <span className="set-value">None yet</span>
          ) : (
            <ul className="mcp-tokens" aria-label="Tokens">
              {server.tokens.map((t) => (
                <li key={t.id}>
                  <span>{t.name}</span> <code className="hint">…{t.hint}</code>{" "}
                  <span className="hint">
                    {t.tools ? t.tools.map(named).join(", ") : "every tool"}, {t.per_minute} a minute
                  </span>
                  {server.manage && (
                    <button
                      className="quiet"
                      onClick={() =>
                        api
                          .revokeMcpToken(t.id)
                          .then(load)
                          .catch((e: Error) => setTrouble(e.message))
                      }
                    >
                      <Sym name="close" />
                      Revoke
                    </button>
                  )}
                </li>
              ))}
            </ul>
          )}
          {made?.token && (
            <div className="mcp-made" role="status">
              <span>The token for {made.name}, shown this once:</span>
              <div className="tg-copyable">
                <code>{made.token}</code>
                <Copy text={made.token} label="Copy the token" />
              </div>
              <span className="hint">The assistant sends it as Authorization: Bearer &lt;token&gt;.</span>
            </div>
          )}
          {server.manage && (
            <NewToken
              tools={server.tools.filter((t) => !t.taken_by)}
              onMade={(t) => {
                setMade(t);
                load();
              }}
            />
          )}
          {trouble && <span className="error set-note">{trouble}</span>}
        </div>
      </div>
    </div>
  );
}

function NewToken(props: { tools: McpServerInfo["tools"]; onMade: (t: McpToken) => void }) {
  const [name, setName] = useState("");
  const [every, setEvery] = useState(true);
  const [chosen, setChosen] = useState<string[]>([]);
  const [perMinute, setPerMinute] = useState(60);
  const [note, setNote] = useState("");
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setNote("");
    try {
      const t = await api.issueMcpToken({ name: name.trim(), tools: every ? null : chosen, per_minute: perMinute });
      setName("");
      setChosen([]);
      props.onMade(t);
    } catch (err) {
      setNote(err instanceof Error ? err.message : "cannot make the token");
    }
  };
  return (
    <form className="mcp-new set-inline" onSubmit={submit} aria-label="A new token">
      <input aria-label="What the token is for" placeholder="For which assistant" value={name} maxLength={100} onChange={(e) => setName(e.target.value)} required />
      <label className="check">
        <input type="checkbox" checked={every} onChange={(e) => setEvery(e.target.checked)} />
        Every tool
      </label>
      {!every &&
        props.tools.map((t) => (
          <label className="check" key={t.workflow}>
            <input
              type="checkbox"
              checked={chosen.includes(t.workflow)}
              onChange={(e) => setChosen(e.target.checked ? [...chosen, t.workflow] : chosen.filter((c) => c !== t.workflow))}
            />
            {t.name}
          </label>
        ))}
      <label className="narrow">
        A minute
        <input type="number" min={1} max={1000} aria-label="Requests a minute" value={perMinute} onChange={(e) => setPerMinute(Number(e.target.value))} />
      </label>
      <button disabled={!name.trim() || (!every && chosen.length === 0)}>
        <Sym name="key" />
        Make a token
      </button>
      {note && <span className="error set-note">{note}</span>}
    </form>
  );
}
