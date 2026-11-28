// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Watching what happens: Runs - every workflow's newest runs in this workspace, in one list, each opened to its steps
// - and Logs - OpenDuct's own log, read back from its OpenTelemetry files, followed as it grows.

import { Fragment, useEffect, useMemo, useState, type ReactNode } from "react";
import { api, type LogLine, type WorkspaceRun } from "./api";
import { RunView } from "./Editor";

// forms-and-resume: "waiting"
const STATUSES = ["all", "done", "failed", "skipped", "waiting"] as const;

/** Runs shown on a page. */
const PAGE = 25;

function ago(ms?: number): string {
  if (!ms) return "";
  const s = Math.max(0, (Date.now() - ms) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return new Date(ms).toLocaleString();
}

function triggerOf(run: WorkspaceRun): string {
  const t = run.trigger?.trigger;
  return typeof t === "string" ? t : "";
}

export function Runs(props: { onOpen: (id: string) => void }) {
  const [runs, setRuns] = useState<WorkspaceRun[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [status, setStatus] = useState<(typeof STATUSES)[number]>("all");
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  useEffect(() => setPage(0), [status, query]);
  useEffect(() => {
    let live = true;
    const load = () =>
      api
        .allRuns()
        .then((r) => live && (setRuns(r), setTrouble("")))
        .catch((e: Error) => live && setTrouble(e.message));
    load();
    const every = setInterval(load, 5000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, []);
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  const shown = (runs ?? []).filter(
    (r) => (status === "all" || r.status === status) && words.every((w) => `${r.workflow.name} ${triggerOf(r)} ${r.error ?? ""}`.toLowerCase().includes(w)),
  );
  const count = (s: string) => (runs ?? []).filter((r) => s === "all" || r.status === s).length;
  const pages = Math.max(1, Math.ceil(shown.length / PAGE));
  const at = Math.min(page, pages - 1);
  const onPage = shown.slice(at * PAGE, at * PAGE + PAGE);
  return (
    <main className="page list-page watch-page">
      <div className="head-line">
        <h1 className="visually-hidden">Runs</h1>
        <div className="segments" role="group" aria-label="Status">
          {STATUSES.map((s) => (
            <button key={s} className={s === status ? `here ${s}` : s} onClick={() => setStatus(s)}>
              {s !== "all" && <span className={`dot ${s}`} aria-hidden="true" />}
              {s === "all" ? "All" : s[0]!.toUpperCase() + s.slice(1)} <span className="count">{count(s)}</span>
            </button>
          ))}
        </div>
        <input className="search" type="search" placeholder="Search runs" aria-label="Search runs" value={query} onChange={(e) => setQuery(e.target.value)} />
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {runs === null && !trouble && <p className="hint empty-note">Reading the runs…</p>}
      {runs && runs.length === 0 && <p className="hint empty-note">No run yet: a workflow's runs show here once it has run.</p>}
      {shown.length > 0 && (
        <table className="compact runs-table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Workflow</th>
              <th>Started by</th>
              <th>When</th>
              <th className="num">Took</th>
              <th>What happened</th>
            </tr>
          </thead>
          <tbody>
            {onPage.map((r) => {
              const key = `${r.workflow.id}/${r.run ?? r.at}`;
              return (
                <Fragment key={key}>
                  <tr className={open === key ? "open" : ""} onClick={() => setOpen(open === key ? null : key)}>
                    <td>
                      <span className={`status ${r.status}`}>{r.status}</span>
                    </td>
                    <td>
                      <a
                        href={`/workflows/${r.workflow.id}`}
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          props.onOpen(r.workflow.id);
                        }}
                      >
                        {r.workflow.name}
                      </a>
                    </td>
                    <td className="muted">{triggerOf(r) || "-"}</td>
                    <td className="muted" title={r.at ? new Date(r.at).toLocaleString() : ""}>
                      {ago(r.at)}
                    </td>
                    <td className="num muted">{r.ms} ms</td>
                    <td className={r.error ? "error-text one-line" : "muted one-line"}>{r.error ?? `${r.steps.length} step${r.steps.length === 1 ? "" : "s"}`}</td>
                  </tr>
                  {open === key && (
                    <tr className="detail">
                      <td colSpan={6}>
                        <RunView run={r} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      )}
      {shown.length > PAGE && (
        <nav className="pager" aria-label="Pages">
          <span className="hint">
            {at * PAGE + 1}-{Math.min(shown.length, at * PAGE + PAGE)} of {shown.length}
          </span>
          <button className="quiet" disabled={at === 0} onClick={() => setPage(at - 1)} aria-label="Newer runs">
            ‹ Newer
          </button>
          <span className="pager-at">
            {at + 1} / {pages}
          </span>
          <button className="quiet" disabled={at >= pages - 1} onClick={() => setPage(at + 1)} aria-label="Older runs">
            Older ›
          </button>
        </nav>
      )}
      {runs && runs.length > 0 && shown.length === 0 && <p className="hint empty-note">No run matches.</p>}
    </main>
  );
}

const LEVELS = [
  { level: "", label: "Everything" },
  { level: "info", label: "Info and above" },
  { level: "warn", label: "Warnings and errors" },
  { level: "error", label: "Errors" },
];

/** Log lines shown on a page. */
const LOG_PAGE = 100;

/** `text` with each place `needle` is found in it marked, whatever its case. */
function Marked({ text, needle }: { text: string; needle: string }) {
  const n = needle.trim().toLowerCase();
  if (!n) return <>{text}</>;
  const parts: ReactNode[] = [];
  const lower = text.toLowerCase();
  let from = 0;
  for (let at = lower.indexOf(n); at >= 0; at = lower.indexOf(n, from)) {
    parts.push(text.slice(from, at), <mark key={at}>{text.slice(at, at + n.length)}</mark>);
    from = at + n.length;
  }
  parts.push(text.slice(from));
  return <>{parts}</>;
}

export function Logs() {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [level, setLevel] = useState("info");
  const [query, setQuery] = useState("");
  const [follow, setFollow] = useState(true);
  const [where, setWhere] = useState<{ kept: boolean; dir?: string; why?: string } | null>(null);
  const [older, setOlder] = useState(false);
  // Where each page starts: page 0 is the newest; each older one is "before" the oldest record of the one before it.
  const [cursors, setCursors] = useState<string[]>([""]);
  // Long lines are folded to three; a click opens one in full.
  const [opened, setOpened] = useState<Set<string>>(new Set());
  const [trouble, setTrouble] = useState("");
  const at = cursors.length - 1;
  useEffect(() => setCursors([""]), [level, query]);
  useEffect(() => {
    let live = true;
    const load = async () => {
      try {
        const before = cursors[at] || undefined;
        const got = await api.logs({ level, q: query, before, limit: LOG_PAGE });
        if (!live) return;
        setWhere({ kept: got.kept, dir: got.dir, why: got.why });
        setOlder(!!got.older);
        setTrouble("");
        // Newest first on a page.
        setLines([...got.lines].reverse());
      } catch (e) {
        if (live) setTrouble(e instanceof Error ? e.message : "cannot read the log");
      }
    };
    const wait = setTimeout(load, query ? 250 : 0);
    // Following keeps the newest page fresh; an older page stays as it is.
    const every = follow && at === 0 ? setInterval(load, 2000) : undefined;
    return () => {
      live = false;
      clearTimeout(wait);
      if (every) clearInterval(every);
    };
  }, [level, query, follow, cursors]);
  const shownDir = useMemo(() => where?.dir ?? "", [where]);
  const oldest = lines.at(-1);
  const time = (l?: LogLine) => (l ? new Date(l.ms).toLocaleTimeString() : "");
  return (
    <main className="page list-page logs-page">
      <div className="head-line">
        <h1 className="visually-hidden">Logs</h1>
        {shownDir && (
          <code className="head-where" title={`Where the log's files are: ${shownDir}`}>
            {shownDir}
          </code>
        )}
        <select aria-label="Level" value={level} onChange={(e) => setLevel(e.target.value)}>
          {LEVELS.map((l) => (
            <option key={l.level} value={l.level}>
              {l.label}
            </option>
          ))}
        </select>
        <input className="search" type="search" placeholder="Search the log" aria-label="Search the log" value={query} onChange={(e) => setQuery(e.target.value)} />
        <label className={`follow${follow && at === 0 ? " on" : ""}`} title={at === 0 ? "Keep the newest page fresh" : "Following resumes on the newest page"}>
          <input type="checkbox" checked={follow} onChange={(e) => setFollow(e.target.checked)} />
          <span className="dot" aria-hidden="true" />
          Follow
        </label>
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {where && !where.kept && <p className="hint empty-note">{where.why}</p>}
      <div className="log-view" role="log" aria-label="Log">
        {lines.map((l, i) => (
          <div
            key={`${l.at}-${i}`}
            className={`log-line ${l.level.toLowerCase()}${opened.has(l.at) ? " open" : ""}`}
            title={opened.has(l.at) ? "Click to fold" : "Click to open in full"}
            onClick={() => {
              if (getSelection()?.toString()) return;
              const next = new Set(opened);
              if (next.has(l.at)) next.delete(l.at);
              else next.add(l.at);
              setOpened(next);
            }}
          >
            <time dateTime={new Date(l.ms).toISOString()}>{time(l)}</time>
            <span className="log-level">{l.level}</span>
            <span className="log-target" title={l.target}>
              {l.target.split("::").at(-1)}
            </span>
            <span className="log-text">
              <Marked text={l.text} needle={query} />
              {Object.entries(l.fields).map(([k, v]) => (
                <span key={k} className="log-field">
                  <Marked text={`${k}=`} needle={query} />
                  <b>
                    <Marked text={typeof v === "string" ? v : JSON.stringify(v)} needle={query} />
                  </b>
                </span>
              ))}
            </span>
          </div>
        ))}
        {where?.kept && lines.length === 0 && <p className="hint log-empty">Nothing in the log matches.</p>}
      </div>
      {(at > 0 || older) && (
        <nav className="pager" aria-label="Pages">
          <span className="hint">
            {lines.length > 0 ? `${time(lines[0])} back to ${time(oldest)}` : ""} · page {at + 1}
          </span>
          <button className="quiet" disabled={at === 0} onClick={() => setCursors([""])} aria-label="Newest lines" title="Newest">
            «
          </button>
          <button className="quiet" disabled={at === 0} onClick={() => setCursors(cursors.slice(0, -1))} aria-label="Newer lines">
            ‹ Newer
          </button>
          <button className="quiet" disabled={!older || !oldest} onClick={() => oldest && setCursors([...cursors, oldest.at])} aria-label="Older lines">
            Older ›
          </button>
        </nav>
      )}
    </main>
  );
}
