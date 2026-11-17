// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The dashboard: is everything working, what needs me, what do I do next. For one workspace - a business, or one of
// an agency's clients - its workflows, its runs over a period, its success, the records waiting in CatchErr and
// what needs someone, each with where it is fixed; for someone who looks after several, every workspace at a glance,
// those needing attention first. A first visit gets a short list of what to do, ticked off as it is done.

import { Sym } from "./Sym";
import { useEffect, useState, type ReactNode } from "react";
import { api, type Alert, type ClientSummary, type Dashboard as Board, type Health, type Range, type Session } from "./api";
import { Glyph } from "./Sidebar";

const RANGES: { range: Range; label: string }[] = [
  { range: "1d", label: "Today" },
  { range: "7d", label: "7 days" },
  { range: "30d", label: "30 days" },
];

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

function ago(ms?: number | null): string {
  if (!ms) return "never";
  const s = Math.max(0, (Date.now() - ms) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} d ago`;
}

function percent(p: number | null | undefined): string {
  return p == null ? "-" : `${p % 1 === 0 ? p : p.toFixed(1)}%`;
}

export function Dashboard(props: {
  session: Session;
  workspace: string;
  canEdit: boolean;
  onGo: (path: string) => void;
  onOpenClient: (id: string, path: string) => void;
  onNewWorkflow: () => void;
}) {
  const [range, choose] = useRange();
  const current = props.session.workspaces.find((w) => w.id === props.workspace);
  return (
    <main className="page list-page dashboard-page">
      <h1 className="visually-hidden">Dashboard</h1>
      <div className="head-line">
        <span className="dash-workspace" title="The workspace picked in the side">
          <span className="ws-dot" aria-hidden="true">
            {(current?.name ?? "?").trim().charAt(0).toUpperCase()}
          </span>
          {current?.name ?? "This workspace"}
        </span>
        <div className="segments" role="radiogroup" aria-label="Period">
          {RANGES.map((r) => (
            <button key={r.range} role="radio" aria-checked={range === r.range} className={range === r.range ? "here" : ""} onClick={() => choose(r.range)}>
              {r.label}
            </button>
          ))}
        </div>
        <div className="dash-actions">
          {props.canEdit && (
            <button onClick={props.onNewWorkflow}>
              <Glyph name="plus" size={14} /> New workflow
            </button>
          )}
          <button className="quiet" onClick={() => props.onGo("/templates")}>
            <Sym name="template" />
            Templates
          </button>
          {props.canEdit && (
            <button className="quiet" onClick={() => props.onGo("/credentials#add")}>
              <Sym name="plug" />
              Connect an app
            </button>
          )}
        </div>
      </div>
      <Client range={range} workspace={props.workspace} canEdit={props.canEdit} onGo={props.onGo} onNewWorkflow={props.onNewWorkflow} />
    </main>
  );
}

/** The period shown, kept in this browser. */
function useRange(): [Range, (r: Range) => void] {
  const [range, setRange] = useState<Range>(() => kept<Range>("openduct.dashboard.range", "7d"));
  return [
    range,
    (r) => {
      setRange(r);
      keep("openduct.dashboard.range", r);
    },
  ];
}

function Period(props: { range: Range; onChoose: (r: Range) => void }) {
  return (
    <div className="segments" role="radiogroup" aria-label="Period">
      {RANGES.map((r) => (
        <button key={r.range} role="radio" aria-checked={props.range === r.range} className={props.range === r.range ? "here" : ""} onClick={() => props.onChoose(r.range)}>
          {r.label}
        </button>
      ))}
    </div>
  );
}

/** Every workspace of the person's at a glance - an agency's clients each - those needing attention first. */
export function AllWorkspaces(props: { current: string; onOpenAt: (id: string, path: string) => void }) {
  const [range, choose] = useRange();
  return (
    <main className="page list-page dashboard-page">
      <h1 className="visually-hidden">All workspaces</h1>
      <div className="head-line">
        <Period range={range} onChoose={choose} />
      </div>
      <Clients range={range} current={props.current} onOpen={(id) => props.onOpenAt(id, "/dashboard")} onOpenAt={props.onOpenAt} />
    </main>
  );
}

function Tile(props: { label: string; value: ReactNode; note?: ReactNode; tone?: "good" | "bad" | "warn"; children?: ReactNode }) {
  return (
    <div className={`tile${props.tone ? ` ${props.tone}` : ""}`}>
      <span className="tile-label">{props.label}</span>
      <span className="tile-value">{props.value}</span>
      {props.note && <span className="tile-note">{props.note}</span>}
      {props.children}
    </div>
  );
}

/** A ring showing a share, from 0 to 100. */
function Ring({ value }: { value: number | null }) {
  const r = 16;
  const c = 2 * Math.PI * r;
  const share = value == null ? 0 : Math.max(0, Math.min(100, value)) / 100;
  return (
    <svg className="ring" viewBox="0 0 40 40" width="40" height="40" aria-hidden="true">
      <circle cx="20" cy="20" r={r} fill="none" strokeWidth="5" className="ring-track" />
      <circle cx="20" cy="20" r={r} fill="none" strokeWidth="5" className="ring-value" strokeDasharray={`${c * share} ${c}`} transform="rotate(-90 20 20)" strokeLinecap="round" />
    </svg>
  );
}

/** Runs over the period, a bar each hour (a day) or day, done / failed / stopped stacked. */
function RunsChart({ board }: { board: Board }) {
  const buckets = board.runs.buckets;
  const most = Math.max(1, ...buckets.map((b) => b.done + b.failed + b.skipped));
  const w = 100 / Math.max(1, buckets.length);
  const label = (i: number) => {
    const at = new Date(board.from + i * board.bucket_ms);
    return board.range === "1d" ? `${at.getHours()}:00` : at.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  };
  const every = board.range === "1d" ? 6 : board.range === "7d" ? 1 : 5;
  return (
    <div className="chart">
      <svg viewBox="0 0 100 50" preserveAspectRatio="none" className="chart-bars" role="img" aria-label="Runs over time">
        {[0.25, 0.5, 0.75].map((y) => (
          <line key={y} x1="0" x2="100" y1={50 * y} y2={50 * y} className="chart-grid" />
        ))}
        {buckets.map((b, i) => {
          const total = b.done + b.failed + b.skipped;
          let y = 50;
          const part = (n: number, cls: string) => {
            if (!n) return null;
            const h = (n / most) * 46;
            y -= h;
            return <rect key={cls} x={i * w + w * 0.18} width={w * 0.64} y={y} height={h} className={cls} />;
          };
          return (
            <g key={i}>
              <title>{`${label(i)}: ${total} run${total === 1 ? "" : "s"} - ${b.done} done, ${b.failed} failed, ${b.skipped} stopped`}</title>
              {part(b.done, "bar-done")}
              {part(b.failed, "bar-failed")}
              {part(b.skipped, "bar-skipped")}
            </g>
          );
        })}
      </svg>
      <div className="chart-axis">
        {buckets.map((_, i) => (
          <span key={i} style={{ width: `${w}%` }}>
            {i % every === 0 ? label(i) : ""}
          </span>
        ))}
      </div>
      <div className="chart-key">
        <span>
          <i className="bar-done" /> Done
        </span>
        <span>
          <i className="bar-failed" /> Failed
        </span>
        <span>
          <i className="bar-skipped" /> Stopped
        </span>
        <span className="most">most in one: {most === 1 && buckets.every((b) => b.done + b.failed + b.skipped === 0) ? 0 : most}</span>
      </div>
    </div>
  );
}

/** How many alerts show before "Show all". */
const ALERTS_SHOWN = 5;

/** What needs someone, a line each - the most pressing first; a reason many share is said once, under them. */
function Alerts(props: { alerts: Alert[]; onGo: (path: string) => void }) {
  const [all, setAll] = useState(false);
  if (props.alerts.length === 0)
    return (
      <p className="all-clear">
        <span className="dot done" /> All clear: nothing needs you.
      </p>
    );
  const ordered = [...props.alerts].sort((a, b) => Number(b.level === "critical") - Number(a.level === "critical"));
  const shown = all ? ordered : ordered.slice(0, ALERTS_SHOWN);
  const shared = new Set(props.alerts.map((a) => a.detail)).size === 1 ? props.alerts[0]!.detail : null;
  return (
    <>
      <ul className="alert-list">
        {shown.map((a) => (
          <li key={a.key} className={`alert ${a.level}`} title={a.detail}>
            <span className={`alert-mark ${a.level}`} aria-hidden="true">
              {a.level === "critical" ? "!" : "i"}
            </span>
            <span className="alert-text">
              <span className="alert-title">{a.title}</span>
              {!shared && <span className="hint">{a.detail}</span>}
            </span>
            <button className="quiet" onClick={() => props.onGo(a.action.path)}>
              <Sym name="arrow" />
              {a.action.label}
            </button>
          </li>
        ))}
      </ul>
      {(shared || ordered.length > ALERTS_SHOWN) && (
        <p className="alert-foot">
          {shared && <span className="hint">{shared}</span>}
          {ordered.length > ALERTS_SHOWN && (
            <button className="quiet link" onClick={() => setAll(!all)}>
              {all ? "Show fewer" : `Show all ${ordered.length}`}
            </button>
          )}
        </p>
      )}
    </>
  );
}

/** The first steps, ticked off as they are done; hidden once all are, or when put away. */
function GettingStarted(props: { board: Board; canEdit: boolean; onGo: (path: string) => void; onNewWorkflow: () => void }) {
  const [hidden, setHidden] = useState(() => kept<string>(`openduct.started.${props.board.workspace.id}`, "") === "hidden");
  const ran = props.board.per_workflow.some((w) => w.last_at);
  const steps = [
    { done: props.board.credentials > 0, label: "Connect an app", hint: "A model, a messenger, a database: its key, kept sealed", action: () => props.onGo("/credentials#add") },
    { done: props.board.workflows.total > 0, label: "Make a workflow", hint: "From a template, or from nothing", action: () => props.onGo("/templates") },
    { done: props.board.workflows.on > 0, label: "Turn it on", hint: "Its trigger starts runs from then on", action: () => props.onGo("/workflows") },
    { done: ran, label: "See it run", hint: "Each run, step by step, in Runs", action: () => props.onGo("/runs") },
  ];
  const left = steps.filter((s) => !s.done).length;
  if (hidden || left === 0 || !props.canEdit) return null;
  return (
    <section className="dash-card started" aria-label="Getting started">
      <div className="card-head">
        <span>Getting started</span>
        <span className="hint">
          {steps.length - left} of {steps.length} done
        </span>
        <button
          className="quiet put-away"
          onClick={() => {
            keep(`openduct.started.${props.board.workspace.id}`, "hidden");
            setHidden(true);
          }}
        >
          <Sym name="close" />
          Put away
        </button>
      </div>
      <div className="progress" aria-hidden="true">
        <span style={{ width: `${((steps.length - left) / steps.length) * 100}%` }} />
      </div>
      <ol className="steps-todo">
        {steps.map((s, i) => (
          <li key={s.label} className={s.done ? "done" : ""}>
            <span className="todo-mark">{s.done ? "✓" : i + 1}</span>
            <span className="todo-text">
              <span>{s.label}</span>
              <span className="hint">{s.hint}</span>
            </span>
            {!s.done && (
              <button className="quiet" onClick={s.action}>
                <Sym name="arrow" />
                Start
              </button>
            )}
          </li>
        ))}
      </ol>
    </section>
  );
}

function Client(props: { range: Range; workspace: string; canEdit: boolean; onGo: (path: string) => void; onNewWorkflow: () => void }) {
  const [board, setBoard] = useState<Board | null>(null);
  const [trouble, setTrouble] = useState("");
  useEffect(() => {
    let live = true;
    const load = () =>
      api
        .dashboard(props.range)
        .then((b) => live && (setBoard(b), setTrouble("")))
        .catch((e: Error) => live && setTrouble(e.message));
    setBoard(null);
    load();
    const every = setInterval(load, 30000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, [props.range, props.workspace]);
  if (trouble) return <p className="error banner">{trouble}</p>;
  if (!board) return <p className="hint empty-note">Reading the dashboard…</p>;
  const busiest = [...board.per_workflow].sort((a, b) => b.waiting - a.waiting || b.failed - a.failed || b.runs - a.runs).slice(0, 8);
  return (
    <div className="dash">
      <GettingStarted board={board} canEdit={props.canEdit} onGo={props.onGo} onNewWorkflow={props.onNewWorkflow} />
      <div className="tiles">
        <Tile label="Workflows on" value={`${board.workflows.on} / ${board.workflows.total}`} note={board.workflows.never_ran ? `${board.workflows.never_ran} never ran` : "each has run"} />
        <Tile label="Runs" value={board.runs.total} note={`${board.runs.done} done · ${board.runs.failed} failed · ${board.runs.skipped} stopped`} />
        <Tile label="Success" value={percent(board.success_percent)} tone={board.success_percent == null ? undefined : board.success_percent >= 95 ? "good" : board.success_percent >= 80 ? "warn" : "bad"} note="of runs that finished">
          <Ring value={board.success_percent} />
        </Tile>
        <Tile label="Waiting in CatchErr" value={board.waiting} tone={board.waiting ? "bad" : "good"} note={board.waiting ? "records to run again once fixed" : "none waiting"} />
      </div>
      {/* Two columns that each flow on their own: a long list on one side leaves no hole on the other. */}
      <div className="dash-grid">
        <div className="dash-col">
          <section className="dash-card wide" aria-label="Runs over time">
            <div className="card-head">
              <span>Runs over time</span>
              <span className="hint">{board.range === "1d" ? "by hour, today" : "by day"}</span>
            </div>
            <RunsChart board={board} />
          </section>
          <section className="dash-card wide" aria-label="Workflows">
            <div className="card-head">
              <span>Workflows</span>
              <button className="quiet link" onClick={() => props.onGo("/workflows")}>
                All workflows ›
              </button>
            </div>
            {busiest.length === 0 ? (
              <p className="hint">No workflow yet.</p>
            ) : (
              <table className="compact dash-table">
                <thead>
                  <tr>
                    <th>Workflow</th>
                    <th className="num">Runs</th>
                    <th className="num">Failed</th>
                    <th className="num">Waiting</th>
                    <th>Last run</th>
                  </tr>
                </thead>
                <tbody>
                  {busiest.map((w) => (
                    <tr key={w.id} onClick={() => props.onGo(`/workflows/${w.id}`)}>
                      <td>
                        <span className={`dot ${w.active ? "done" : ""}`} title={w.active ? "On" : "Off"} /> {w.name}
                      </td>
                      <td className="num">{w.runs}</td>
                      <td className={`num${w.failed ? " bad" : ""}`}>{w.failed}</td>
                      <td className={`num${w.waiting ? " bad" : ""}`}>{w.waiting}</td>
                      <td className="muted">
                        {w.last_status && <span className={`dot ${w.last_status}`} />} {ago(w.last_at)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </div>
        <div className="dash-col">
          <section className="dash-card" aria-label="Needs attention">
            <div className="card-head">
              <span>Needs attention</span>
              {board.alerts.length > 0 && <span className={`count-pill ${board.health}`}>{board.alerts.length}</span>}
            </div>
            <Alerts alerts={board.alerts} onGo={props.onGo} />
          </section>
          <section className="dash-card" aria-label="Recent activity">
            <div className="card-head">
              <span>Recent activity</span>
              <button className="quiet link" onClick={() => props.onGo("/runs")}>
                All runs ›
              </button>
            </div>
            {board.recent.length === 0 ? (
              <p className="hint">No run in this period.</p>
            ) : (
              <ul className="activity">
                {board.recent.map((r, i) => (
                  <li key={`${r.workflow.id}/${r.run ?? i}`} onClick={() => props.onGo(`/workflows/${r.workflow.id}`)}>
                    <span className={`dot ${r.status}`} />
                    <span className="act-name">{r.workflow.name}</span>
                    <span className="act-what">{r.status === "failed" ? (r.error ?? "failed") : r.status === "done" ? "done" : "stopped"}</span>
                    <span className="act-when">{ago(r.at)}</span>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

const HEALTH_WORD: Record<Health, string> = { green: "All good", amber: "Needs a look", red: "Needs you" };

function Clients(props: { range: Range; current: string; onOpen: (id: string) => void; onOpenAt: (id: string, path: string) => void }) {
  const [clients, setClients] = useState<ClientSummary[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [query, setQuery] = useState("");
  useEffect(() => {
    let live = true;
    const load = () =>
      api
        .overview(props.range)
        .then((c) => live && (setClients(c), setTrouble("")))
        .catch((e: Error) => live && setTrouble(e.message));
    setClients(null);
    load();
    const every = setInterval(load, 30000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, [props.range]);
  if (trouble) return <p className="error banner">{trouble}</p>;
  if (!clients) return <p className="hint empty-note">Reading every workspace…</p>;
  const sum = (f: (c: ClientSummary) => number) => clients.reduce((t, c) => t + f(c), 0);
  const red = clients.filter((c) => c.health === "red").length;
  const amber = clients.filter((c) => c.health === "amber").length;
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  const shown = clients.filter((c) => words.every((w) => c.name.toLowerCase().includes(w)));
  return (
    <div className="dash">
      <div className="tiles">
        <Tile label="Workspaces" value={clients.length} note={red || amber ? `${red} need you · ${amber} need a look` : "all good"} tone={red ? "bad" : amber ? "warn" : "good"} />
        <Tile label="Runs" value={sum((c) => c.runs.total)} note="across every workspace" />
        <Tile label="Failed runs" value={sum((c) => c.runs.failed)} tone={sum((c) => c.runs.failed) ? "bad" : "good"} note="in this period" />
        <Tile label="Waiting in CatchErr" value={sum((c) => c.waiting)} tone={sum((c) => c.waiting) ? "bad" : "good"} note="records to run again" />
      </div>
      <section className="dash-card" aria-label="Workspaces">
        <div className="card-head">
          <span>Workspaces</span>
          <span className="hint">those needing you first</span>
          <input className="search" type="search" placeholder="Find a workspace" aria-label="Find a workspace" value={query} onChange={(e) => setQuery(e.target.value)} />
        </div>
        <table className="compact dash-table clients-table">
          <thead>
            <tr>
              <th>Workspace</th>
              <th>Health</th>
              <th className="num">Workflows on</th>
              <th className="num">Runs</th>
              <th className="num">Failed</th>
              <th className="num">Success</th>
              <th className="num">Waiting</th>
              <th>Last failure</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {shown.map((c) => (
              <tr key={c.id} className={c.id === props.current ? "current" : ""}>
                <td>
                  <span className="client-name">{c.name}</span>
                  {c.alerts[0] && (
                    <button className="quiet link client-alert" onClick={() => props.onOpenAt(c.id, c.alerts[0]!.action.path)} title={c.alerts[0].detail}>
                      {c.alerts[0].title}
                      {c.alerts.length > 1 ? ` · +${c.alerts.length - 1} more` : ""}
                    </button>
                  )}
                </td>
                <td>
                  <span className={`health ${c.health}`}>
                    <span className={`dot ${c.health}`} /> {HEALTH_WORD[c.health]}
                  </span>
                </td>
                <td className="num">
                  {c.workflows.on} / {c.workflows.total}
                </td>
                <td className="num">{c.runs.total}</td>
                <td className={`num${c.runs.failed ? " bad" : ""}`}>{c.runs.failed}</td>
                <td className="num">{percent(c.success_percent)}</td>
                <td className={`num${c.waiting ? " bad" : ""}`}>{c.waiting}</td>
                <td className="muted">{c.last_failure ? ago(c.last_failure) : "none"}</td>
                <td className="num">
                  <button className="quiet" onClick={() => props.onOpen(c.id)}>
                    <Sym name="arrow" />
                    Open
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {shown.length === 0 && <p className="hint">No workspace matches.</p>}
      </section>
    </div>
  );
}
