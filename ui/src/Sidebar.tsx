// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The studio's side: its pages by what they are for, a quick find (Ctrl+K / Cmd+K) for pages and workflows, the
// workspaces (scrolling when they are many), and at its foot, on one row, who is signed in, light or dark, settings
// and signing out; a button folds it to a rail of icons - each choice remembered in this browser.

import { useEffect, useMemo, useRef, useState } from "react";
import { api, type WorkflowSummary } from "./api";

const PATHS: Record<string, string> = {
  workflows: "M4 6h6v4H4zM14 14h6v4h-6zM10 8h2a2 2 0 0 1 2 2v4",
  dashboard: "M3.5 17a8.5 8.5 0 1 1 17 0M12 17l4.2-5.2M7 17h.01M17 17h.01",
  plus: "M12 5v14M5 12h14",
  templates: "M12 3 3 8l9 5 9-5zM3 12.5l9 5 9-5M3 17l9 5 9-5",
  workflow: "M5 4h6v5H5zM13 15h6v5h-6zM8 9v3a2 2 0 0 0 2 2h4a2 2 0 0 1 2 2",
  pipelines: "M3 8h12l-3-3M15 8l-3 3M21 16H9l3-3M9 16l3 3M19 8h2M3 16h2",
  runs: "M3 12h4l3-8 4 16 3-8h4",
  logs: "M4 4h16v16H4zM8 9l3 3-3 3M13 15h4",
  settings:
    "M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z",
  credentials: "M15 7a4 4 0 1 1-3.9 4.9L4 19v-3h2v-2h2l1.1-1.1A4 4 0 0 1 15 7zM16 10h.01",
  files: "M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",
  tables: "M3 5h18v14H3zM3 10h18M3 15h18M9 10v9", // data tables
  apps: "M4 4h7v7H4zM13 4h7v7h-7zM4 13h7v7H4zM16.5 13v7M13 16.5h7", // app generator
  members: "M16 19v-1a4 4 0 0 0-4-4H7a4 4 0 0 0-4 4v1M9.5 10a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7zM21 19v-1a4 4 0 0 0-3-3.9M16 3.1a3.5 3.5 0 0 1 0 6.8",
  find: "M11 18a7 7 0 1 0 0-14 7 7 0 0 0 0 14zM21 21l-4.3-4.3",
  system: "M4 5h16v11H4zM9 20h6M12 16v4",
  light: "M12 16a4 4 0 1 0 0-8 4 4 0 0 0 0 8zM12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4",
  dark: "M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z",
  signout: "M15 17l5-5-5-5M20 12H9M9 4H5a1 1 0 0 0-1 1v14a1 1 0 0 0 1 1h4",
  fold: "M12 7l-5 5 5 5M18 7l-5 5 5 5",
  unfold: "M6 7l5 5-5 5M12 7l5 5-5 5",
};

export function Glyph({ name, size = 18 }: { name: string; size?: number }) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={PATHS[name]} />
    </svg>
  );
}

export type Theme = "system" | "light" | "dark";

function kept<T extends string>(key: string, fallback: T): T {
  try {
    return (localStorage.getItem(key) as T | null) ?? fallback;
  } catch {
    return fallback;
  }
}

function keep(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    // A browser that keeps nothing: the choice lasts this visit.
  }
}

export function applyTheme(theme: Theme) {
  if (theme === "system") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
}

/** The theme, as chosen in this browser: shared by the side's switch and the settings page. */
export function useTheme(): [Theme, (t: Theme) => void] {
  const [theme, setTheme] = useState<Theme>(() => kept<Theme>("openduct.theme", "system"));
  useEffect(() => {
    const changed = () => setTheme(kept<Theme>("openduct.theme", "system"));
    addEventListener("openduct-theme", changed);
    return () => removeEventListener("openduct-theme", changed);
  }, []);
  return [
    theme,
    (t) => {
      keep("openduct.theme", t);
      applyTheme(t);
      setTheme(t);
      dispatchEvent(new Event("openduct-theme"));
    },
  ];
}

/** Whether the page is dark now: chosen so, or following a computer set to dark. */
function isDark(theme: Theme): boolean {
  if (theme !== "system") return theme === "dark";
  try {
    return matchMedia("(prefers-color-scheme: dark)").matches;
  } catch {
    return false;
  }
}

/** Whether the window is narrow: the side then folds to its rail by itself. */
export function useNarrow(): boolean {
  const query = "(max-width: 52rem)";
  const [narrow, setNarrow] = useState(() => (typeof matchMedia === "function" ? matchMedia(query).matches : false));
  useEffect(() => {
    if (typeof matchMedia !== "function") return;
    const list = matchMedia(query);
    const changed = () => setNarrow(list.matches);
    list.addEventListener?.("change", changed);
    return () => list.removeEventListener?.("change", changed);
  }, []);
  return narrow;
}

export function useFolded(): [boolean, (f: boolean) => void] {
  const [folded, setFolded] = useState(() => kept<string>("openduct.side", "open") === "folded");
  return [
    folded,
    (f) => {
      setFolded(f);
      keep("openduct.side", f ? "folded" : "open");
    },
  ];
}

export function Sidebar(props: {
  workspaces: { id: string; name: string }[];
  workspace: string;
  onChoose: (id: string) => void;
  /** The installation's admin makes workspaces. */
  canMake: boolean;
  onMade: (id: string) => void;
  /** The installation's admin: the log is theirs to read. */
  installationAdmin: boolean;
  account: string;
  onSignOut: () => void;
  page: string;
  inWorkflow: boolean;
  admin: boolean;
  canEdit: boolean;
  folded: boolean;
  /** The window is narrow: folded by itself. */
  narrow?: boolean;
  onFold: (folded: boolean) => void;
  onGo: (path: string) => void;
  onNewWorkflow: () => void;
}) {
  const [theme, choose] = useTheme();
  const [finding, setFinding] = useState(false);
  useEffect(() => applyTheme(theme), [theme]);
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setFinding(true);
      }
    };
    addEventListener("keydown", key);
    return () => removeEventListener("keydown", key);
  }, []);
  const item = (page: string, label: string, glyph = page) => (
    <a
      className={props.page === page && !(props.inWorkflow && page !== "workflows") ? "here" : ""}
      href={`/${page}`}
      title={props.folded ? label : undefined}
      onClick={(e) => {
        e.preventDefault();
        props.onGo(`/${page}`);
      }}
    >
      <Glyph name={glyph} />
      <span className="label">{label}</span>
    </a>
  );
  return (
    <nav className={`side${props.folded ? " folded" : ""}`} aria-label="Studio">
      <div className="brand-row">
        <img src="/favicon.svg" alt="" width={24} height={24} />
        <span className="label">OpenDuct</span>
        {!props.narrow && (
          <button
            className="fold"
            onClick={() => props.onFold(!props.folded)}
            aria-label={props.folded ? "Show the side" : "Hide the side"}
            title={props.folded ? "Show the side" : "Hide the side"}
          >
            <Glyph name={props.folded ? "unfold" : "fold"} size={14} />
          </button>
        )}
      </div>
      <div className="top">
        <button className="find" onClick={() => setFinding(true)} title={props.folded ? "Find (Ctrl+K)" : undefined}>
          <Glyph name="find" />
          <span className="label">Find</span>
          <kbd className="label">⌘K</kbd>
        </button>
      </div>
      <div className="group">{item("dashboard", "Dashboard")}</div>
      <div className="group">
        <span className="heading label">Build</span>
        {item("workflows", "Workflows")}
        {item("pipelines", "Pipelines")}
        {item("templates", "Templates")}
        {props.canEdit && (
          <button className="item" title={props.folded ? "New workflow" : undefined} onClick={props.onNewWorkflow}>
            <Glyph name="plus" />
            <span className="label">New workflow</span>
          </button>
        )}
      </div>
      <div className="group">
        <span className="heading label">Watch</span>
        {item("runs", "Runs")}
        {props.installationAdmin && item("logs", "Logs")}
      </div>
      <div className="group">
        <span className="heading label">Connect</span>
        {item("credentials", "Credentials")}
        {item("files", "Files")}
        {item("tables", "Tables") /* data tables */}
        {item("apps", "Apps") /* app generator */}
      </div>
      {props.admin && (
        <div className="group">
          <span className="heading label">Team</span>
          {item("members", "Members")}
        </div>
      )}
      <Workspaces {...props} />
      <div className="foot">
        <div className="account" title={props.folded ? props.account : undefined}>
          <span className="avatar" aria-hidden="true">
            {props.account.trim().charAt(0).toUpperCase() || "?"}
          </span>
          <span className="label name" title={props.account}>
            {props.account}
          </span>
          <button
            className="foot-button"
            aria-label={isDark(theme) ? "Light" : "Dark"}
            title={isDark(theme) ? "Make it light" : "Make it dark"}
            onClick={() => choose(isDark(theme) ? "light" : "dark")}
          >
            <Glyph name={isDark(theme) ? "light" : "dark"} size={16} />
          </button>
          <button className={`foot-button${props.page === "settings" ? " here" : ""}`} aria-label="Settings" title="Settings" onClick={() => props.onGo("/settings")}>
            <Glyph name="settings" size={16} />
          </button>
          <button className="foot-button signout" aria-label="Sign out" title="Sign out" onClick={props.onSignOut}>
            <Glyph name="signout" size={16} />
          </button>
        </div>
      </div>
      {finding && <Find admin={props.admin} onGo={props.onGo} onClose={() => setFinding(false)} />}
    </nav>
  );
}

/** A workspace's colour, from its name: the same each time. */
function hueOf(name: string): number {
  let h = 0;
  for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360;
  return h;
}

/** The workspaces, filling the side's room below its pages and scrolling when they are many; the current one marked. */
function Workspaces(props: {
  workspaces: { id: string; name: string }[];
  workspace: string;
  onChoose: (id: string) => void;
  canMake: boolean;
  onMade: (id: string) => void;
  folded: boolean;
  page: string;
  onGo: (path: string) => void;
}) {
  const [making, setMaking] = useState(false);
  const [name, setName] = useState("");
  const [trouble, setTrouble] = useState("");
  const make = async () => {
    if (!name.trim()) return;
    try {
      const made = await api.addWorkspace(name.trim());
      setName("");
      setMaking(false);
      setTrouble("");
      props.onMade(made.id);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot make it");
    }
  };
  return (
    <div className="group workspaces">
      <div className="heading-row">
        <span className="heading label">Workspaces</span>
        {props.canMake && !props.folded && (
          <button className="heading-add" aria-label="New workspace" title="New workspace" onClick={() => setMaking(!making)}>
            <Glyph name="plus" size={14} />
          </button>
        )}
      </div>
      <div className="ws-list" role="list" aria-label="Workspaces">
        {props.workspaces.length > 1 && (
          <button
            role="listitem"
            className={`ws-item ws-all${props.page === "overview" ? " here" : ""}`}
            title="Every workspace at a glance"
            onClick={() => props.onGo("/overview")}
          >
            <span className="ws-mark ws-mark-all" aria-hidden="true">
              <Glyph name="dashboard" size={12} />
            </span>
            <span className="label">All workspaces</span>
          </button>
        )}
        {props.workspaces.map((w) => (
          <button
            key={w.id}
            role="listitem"
            className={`ws-item${w.id === props.workspace && props.page !== "overview" ? " here" : ""}`}
            aria-current={w.id === props.workspace ? "true" : undefined}
            title={w.name}
            onClick={() => w.id !== props.workspace && props.onChoose(w.id)}
          >
            <span className="ws-mark" style={{ background: `hsl(${hueOf(w.name)} 62% 46%)` }} aria-hidden="true">
              {w.name.trim().charAt(0).toUpperCase() || "?"}
            </span>
            <span className="label">{w.name}</span>
          </button>
        ))}
        {making && !props.folded && (
          <form
            className="ws-new"
            onSubmit={(e) => {
              e.preventDefault();
              make();
            }}
          >
            <div className="ws-new-row">
              <input
                autoFocus
                aria-label="New workspace's name"
                placeholder="Its name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") setMaking(false);
                  else if (e.key === "Enter") {
                    e.preventDefault();
                    make();
                  }
                }}
              />
              <button type="submit" className="ws-new-add" disabled={!name.trim()}>
                Add
              </button>
              <button type="button" className="ws-new-cancel" aria-label="Cancel" title="Cancel (Esc)" onClick={() => setMaking(false)}>
                ×
              </button>
            </div>
            {trouble ? <span className="error">{trouble}</span> : <span className="hint ws-new-hint">Enter to add · Esc to cancel</span>}
          </form>
        )}
      </div>
    </div>
  );
}

/** Find a page or a workflow by name: arrow keys to move, Enter to go, Escape to close. */
function Find(props: { admin: boolean; onGo: (path: string) => void; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => {
    input.current?.focus();
    api
      .workflows()
      .then(setWorkflows)
      .catch(() => undefined);
  }, []);
  const all = useMemo(() => {
    const pages = [
      { label: "Dashboard", hint: "page", path: "/dashboard" },
      { label: "Workflows", hint: "page", path: "/workflows" },
      { label: "Pipelines", hint: "page", path: "/pipelines" },
      { label: "Templates", hint: "page", path: "/templates" },
      { label: "Runs", hint: "page", path: "/runs" },
      { label: "Credentials", hint: "page", path: "/credentials" },
      { label: "Files", hint: "page", path: "/files" },
      { label: "Tables", hint: "page", path: "/tables" }, // data tables
      { label: "Apps", hint: "page", path: "/apps" }, // app generator
      ...(props.admin ? [{ label: "Members", hint: "page", path: "/members" }] : []),
      { label: "Settings", hint: "page", path: "/settings" },
    ];
    return [...pages, ...workflows.map((w) => ({ label: w.name, hint: w.active ? "workflow, on" : "workflow", path: `/workflows/${w.id}` }))];
  }, [workflows, props.admin]);
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  const found = all.filter((i) => words.every((w) => `${i.label} ${i.hint}`.toLowerCase().includes(w))).slice(0, 12);
  useEffect(() => setActive(0), [query]);
  const go = (path: string) => {
    props.onGo(path);
    props.onClose();
  };
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <div
        className="modal find-window"
        role="dialog"
        aria-modal="true"
        aria-label="Find"
        onKeyDown={(e) => {
          if (e.key === "Escape") props.onClose();
          else if (e.key === "ArrowDown") {
            e.preventDefault();
            setActive((a) => Math.min(a + 1, found.length - 1));
          } else if (e.key === "ArrowUp") {
            e.preventDefault();
            setActive((a) => Math.max(a - 1, 0));
          } else if (e.key === "Enter" && found[active]) go(found[active].path);
        }}
      >
        <input ref={input} className="search" type="search" placeholder="Find a page or a workflow" aria-label="Find" value={query} onChange={(e) => setQuery(e.target.value)} />
        <ul>
          {found.map((i, n) => (
            <li key={i.path}>
              <button className={n === active ? "active" : ""} onMouseEnter={() => setActive(n)} onClick={() => go(i.path)}>
                <span>{i.label}</span>
                <span className="hint">{i.hint}</span>
              </button>
            </li>
          ))}
          {found.length === 0 && <li className="hint">Nothing matches.</li>}
        </ul>
      </div>
    </div>
  );
}
