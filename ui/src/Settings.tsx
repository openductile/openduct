// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Settings, as rows in groups - each setting's name and what it does on the left, its control on the right - in two
// columns on a wide window: your account and its password; how the studio looks and opens; the workspace, its name
// and what is in it; this browser's open tabs; the installation (for its admin); the keyboard; the version.

import { AppSignIns } from "./AppSignIns";
import { Sym } from "./Sym";
import { ApiTokens, EnvironmentSettings, GitSync } from "./Delivery"; // developer CI/CD
import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { api, type CredentialSummary, type FileSummary, type Session, type WorkflowSummary } from "./api";
import { Glyph, useFolded, useTheme, type Theme } from "./Sidebar";
import { AlertSettings } from "./AlertSettings";
import { McpSettings } from "./McpSettings"; // MCP

const THEMES: { theme: Theme; label: string }[] = [
  { theme: "light", label: "Light" },
  { theme: "dark", label: "Dark" },
  { theme: "system", label: "Same as this computer" },
];

function Row(props: { name: string; about?: string; children: ReactNode }) {
  return (
    <div className="set-row">
      <div className="set-name">
        <span>{props.name}</span>
        {props.about && <span className="hint">{props.about}</span>}
      </div>
      <div className="set-control">{props.children}</div>
    </div>
  );
}

/** What a workspace holds, counted: shown with a way to each page. */
type Counts = { workflows: number; on: number; credentials: number; files: number; members: number | null };

const SHORTCUTS: [string, string][] = [
  ["⌘K / Ctrl+K", "Find a page or a workflow"],
  ["Esc", "Close a window"],
  ["Middle click on a tab", "Close it"],
  ["↑ ↓ then Enter", "Pick in the find and add-a-step windows"],
];

export function Settings(props: { session: Session; workspace: string; onRenamed: () => void; onGo: (path: string) => void; tabs: number; onCloseTabs: () => void }) {
  const [theme, setTheme] = useTheme();
  const [folded, setFolded] = useFolded();
  const [version, setVersion] = useState("");
  const [counts, setCounts] = useState<Counts | null>(null);
  const [logDir, setLogDir] = useState<string | null>(null);
  const account = props.session.account;
  const mine = props.session.workspaces.find((w) => w.id === props.workspace);
  useEffect(() => {
    api
      .status()
      .then((s) => setVersion(s.version ?? ""))
      .catch(() => undefined);
    Promise.all([
      api.workflows().catch((): WorkflowSummary[] => []),
      api.credentials().catch((): CredentialSummary[] => []),
      api.files().catch((): FileSummary[] => []),
      mine?.role === "admin" ? api.members(props.workspace).catch(() => null) : Promise.resolve(null),
    ]).then(([w, c, f, m]) => setCounts({ workflows: w.length, on: w.filter((x) => x.active).length, credentials: c.length, files: f.length, members: m ? m.length : null }));
    if (account.admin)
      api
        .logs({ limit: 1 })
        .then((l) => setLogDir(l.kept ? (l.dir ?? "") : ""))
        .catch(() => setLogDir(null));
  }, [props.workspace]);
  const go = (path: string, label: string) => (
    <button className="quiet set-go" onClick={() => props.onGo(path)}>
      {label} ›
    </button>
  );
  return (
    <main className="page list-page settings-page">
      <h1 className="visually-hidden">Settings</h1>
      <div className="set-columns">
        <div className="set-column">
          <section className="set-group" aria-label="Account">
            <h2>Account</h2>
            <Row name="You" about={account.admin ? "You run this installation" : undefined}>
              <span className="set-who">
                <span className="avatar-lg" aria-hidden="true">
                  {account.name.trim().charAt(0).toUpperCase() || "?"}
                </span>
                <span className="set-who-text">
                  <span>{account.name}</span>
                  <span className="hint">{account.email}</span>
                </span>
              </span>
            </Row>
            <Row name="Password" about="At least 10 characters">
              <Password />
            </Row>
          </section>
          <section className="set-group" aria-label="Appearance">
            <h2>Appearance</h2>
            <Row name="Theme" about="How the studio looks in this browser">
              <div className="segments" role="radiogroup" aria-label="Theme">
                {THEMES.map((t) => (
                  <button key={t.theme} role="radio" aria-checked={theme === t.theme} className={theme === t.theme ? "here" : ""} onClick={() => setTheme(t.theme)}>
                    <Glyph name={t.theme} size={14} />
                    {t.label}
                  </button>
                ))}
              </div>
            </Row>
            <Row name="The side" about="How the side opens in this browser">
              <div className="segments" role="radiogroup" aria-label="The side">
                {[
                  [false, "Open"],
                  [true, "Folded to icons"],
                ].map(([f, label]) => (
                  <button key={String(f)} role="radio" aria-checked={folded === f} className={folded === f ? "here" : ""} onClick={() => setFolded(f as boolean)}>
                    {label as string}
                  </button>
                ))}
              </div>
            </Row>
            <Row name="Open tabs" about="Each page and workflow opened stays a tab until closed">
              <span className="set-value">{props.tabs === 1 ? "Dashboard only" : `${props.tabs} open`}</span>
              <button className="quiet" disabled={props.tabs <= 1} onClick={props.onCloseTabs}>
                <Sym name="close" />
                Close all but Dashboard
              </button>
            </Row>
          </section>
          <section className="set-group" aria-label="Keyboard">
            <h2>Keyboard</h2>
            {SHORTCUTS.map(([keys, what]) => (
              <Row key={keys} name={what}>
                <kbd className="set-keys">{keys}</kbd>
              </Row>
            ))}
          </section>
        </div>
        <div className="set-column">
          {mine && (
            <section className="set-group" aria-label="Workspace">
              <h2>Workspace</h2>
              <Row name="Name" about={mine.role === "admin" ? "What everyone in it sees" : "Only its admins rename it"}>
                <WorkspaceName id={mine.id} name={mine.name} admin={mine.role === "admin"} onRenamed={props.onRenamed} />
              </Row>
              <Row name="Topics" about="Its own: no other workspace reads or writes them">
            <span className="set-value">
              Outside OpenDuct (Kafka clients, the command line) <code className="set-path">orders</code> is <code className="set-path">w.{mine.id}.orders</code>
            </span>
          </Row>
          <Row name="Your role" about="What you may do in it">
                <span className="set-value">{{ admin: "Admin: everything, members too", editor: "Editor: builds and runs", viewer: "Viewer: looks only" }[mine.role]}</span>
              </Row>
              <Row name="Workflows">
                <span className="set-value">{counts ? `${counts.workflows}, ${counts.on} on` : "…"}</span>
                {go("/workflows", "Open")}
              </Row>
              <Row name="Credentials">
                <span className="set-value">{counts ? counts.credentials : "…"}</span>
                {go("/credentials", "Open")}
              </Row>
              <Row name="Files">
                <span className="set-value">{counts ? counts.files : "…"}</span>
                {go("/files", "Open")}
              </Row>
              {counts?.members != null && (
                <Row name="Members">
                  <span className="set-value">{counts.members}</span>
                  {go("/members", "Open")}
                </Row>
              )}
            </section>
          )}
          {mine && mine.role !== "viewer" && ( // MCP
            <section className="set-group" aria-label="Tools for AI assistants">
              <h2>Tools for AI assistants (MCP)</h2>
              <McpSettings workspace={mine.id} onGo={props.onGo} />
            </section>
          )}
          {mine?.role === "admin" && (
            <section className="set-group" aria-label="Alerts">
              <h2>Alerts</h2>
              <AlertSettings workspace={mine.id} onGo={props.onGo} />
            </section>
          )}
          {/* developer CI/CD: API tokens, the workspace's environment, and Git sync */}
          {mine?.role === "admin" && (
            <section className="set-group" aria-label="API tokens">
              <h2>API tokens</h2>
              <ApiTokens workspace={mine.id} role={mine.role} />
            </section>
          )}
          {mine && (
            <section className="set-group" aria-label="Environment">
              <h2>Environment</h2>
              <EnvironmentSettings workspace={mine.id} admin={mine.role === "admin"} />
            </section>
          )}
          {mine && mine.role !== "viewer" && (
            <section className="set-group" aria-label="Git">
              <h2>Git</h2>
              <GitSync workspace={mine.id} admin={mine.role === "admin"} />
            </section>
          )}
          {account.admin && (
            <section className="set-group" aria-label="Installation">
              <h2>Installation</h2>
              <Row name="Its admin" about="Makes workspaces, reads the log">
                <span className="set-value">You</span>
              </Row>
              <Row name="Log" about="OpenTelemetry records, a file a day">
                {logDir === null ? (
                  <span className="set-value">…</span>
                ) : logDir ? (
                  <code className="set-path" title={logDir}>
                    {logDir}
                  </code>
                ) : (
                  <span className="set-value">Not kept on disk</span>
                )}
                {go("/logs", "Logs")}
              </Row>
            </section>
          )}
          {account.admin && (
            <section className="set-group" aria-label="App sign-ins">
              <h2>App sign-ins</h2>
              <AppSignIns />
            </section>
          )}
          <section className="set-group" aria-label="About">
            <h2>About</h2>
            <Row name="Version">
              <span className="set-value">OpenDuct {version || "…"}</span>
            </Row>
          </section>
        </div>
      </div>
    </main>
  );
}

function Password() {
  const [present, setPresent] = useState("");
  const [next, setNext] = useState("");
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setNote(null);
    try {
      await api.changePassword(present, next);
      setPresent("");
      setNext("");
      setNote({ ok: true, text: "Password changed." });
    } catch (err) {
      setNote({ ok: false, text: err instanceof Error ? err.message : "cannot change it" });
    } finally {
      setBusy(false);
    }
  };
  return (
    <form className="set-inline" onSubmit={submit}>
      <input
        type="password"
        aria-label="Present password"
        placeholder="Present password"
        autoComplete="current-password"
        value={present}
        onChange={(e) => setPresent(e.target.value)}
        required
      />
      <input
        type="password"
        aria-label="New password"
        placeholder="New password"
        autoComplete="new-password"
        minLength={10}
        value={next}
        onChange={(e) => setNext(e.target.value)}
        required
      />
      <button disabled={busy || !present || next.length < 10}>
        <Sym name="key" />
        {busy ? "Changing…" : "Change"}
      </button>
      {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
    </form>
  );
}

function WorkspaceName(props: { id: string; name: string; admin: boolean; onRenamed: () => void }) {
  const [name, setName] = useState(props.name);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  useEffect(() => setName(props.name), [props.name]);
  if (!props.admin) return <span className="set-value">{props.name}</span>;
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setNote(null);
    try {
      await api.renameWorkspace(props.id, name.trim());
      setNote({ ok: true, text: "Renamed." });
      props.onRenamed();
    } catch (err) {
      setNote({ ok: false, text: err instanceof Error ? err.message : "cannot rename it" });
    }
  };
  return (
    <form className="set-inline" onSubmit={submit}>
      <input aria-label="Workspace name" value={name} onChange={(e) => setName(e.target.value)} maxLength={100} required />
      <button disabled={name.trim() === props.name || !name.trim()}>
        <Sym name="pencil" />
        Rename
      </button>
      {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
    </form>
  );
}
