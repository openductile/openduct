// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// One workflow: its trigger and steps, tried out on sample input, turned on and off, and watched as it runs.

import { Sym } from "./Sym";
import { useEffect, useMemo, useRef, useState } from "react";
import { TriggerSettings } from "./Trigger";
import {
  api,
  type CredentialSummary,
  type Definition,
  type Metrics,
  type Run,
  type SetAside,
  type StepRun,
  type VersionRecord,
  type WorkflowRecord,
} from "./api";
import { AddStepDialog } from "./flow/AddStep";
import { Canvas, type Selection } from "./flow/Canvas";
import { StepOutput } from "./flow/Output";
import { InputPane, writable, type Box } from "./flow/Input";
import { Popup } from "./flow/Popup";
import { Icon } from "./flow/Icon";
import { triggerSummary } from "./flow/look";
import { insertAt, moveAt, removeAt, replaceAt, stepAt, type StepPath } from "./flow/paths";
import { CredentialsContext, JsonField, StepCard, fresh, names, registerApps } from "./Steps";

type Tab = "build" | "history" | "aside" | "versions";


/** What a test runs on when a workflow has no sample of its own. */
const DEFAULT_SAMPLE = { id: "A-100", total: 120, email: "ada@example.com" };

export function Editor(props: {
  id: string;
  /** Its tab is the open one. */
  active: boolean;
  onTitle: (title: string, dirty: boolean) => void;
  onGone: () => void;
}) {
  const [record, setRecord] = useState<WorkflowRecord | null>(null);
  const [definition, setDefinition] = useState<Definition | null>(null);
  const [dirty, setDirty] = useState(false);
  const [tab, setTab] = useState<Tab>("build");
  const [message, setMessage] = useState<{ kind: "error" | "ok"; text: string } | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [credentials, setCredentials] = useState<CredentialSummary[]>([]);
  const [selection, setSelection] = useState<Selection>(null);
  const [adding, setAdding] = useState<StepPath | null>(null);
  const [run, setRun] = useState<Run | null>(null);
  const [flash, setFlash] = useState("");
  const flashTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  // The box last written in, in a step's window: where a field picked from its input goes.
  const box = useRef<Box | null>(null);

  const [, setAppsSeen] = useState(0);
  useEffect(() => {
    api
      .workflow(props.id)
      .then((r) => {
        setRecord(r);
        setDefinition(r.definition);
      })
      .catch((e: Error) => setMessage({ kind: "error", text: e.message }));
    api.credentials().then(setCredentials).catch(() => undefined);
    // The apps' steps and forms come from their definitions: drawn again once they are here.
    api
      .apps()
      .then((apps) => {
        registerApps(apps);
        setAppsSeen((n) => n + 1);
      })
      .catch(() => undefined);
  }, [props.id]);

  const taken = useMemo(() => names(definition?.steps ?? []), [definition]);
  useEffect(() => {
    if (definition) props.onTitle(definition.name || "Untitled", dirty);
  }, [definition?.name, dirty]);
  if (!record || !definition) return <main className="page">{message?.text ?? ""}</main>;

  const edit = (next: Definition) => {
    setDefinition(next);
    setDirty(true);
    setMessage(null);
  };
  // The event a test or a preview runs on, kept with the workflow (a template brings its own): the same for the
  // header's bar and each step's window.
  const sample = definition.sample ?? DEFAULT_SAMPLE;
  const setSample = (next: unknown) => edit({ ...definition, sample: next });
  /** A word beside the name for a moment: what was just done. */
  const say = (text: string) => {
    setMessage(null);
    setFlash(text);
    clearTimeout(flashTimer.current);
    flashTimer.current = setTimeout(() => setFlash(""), 2600);
  };
  const save = async () => {
    try {
      const renamed = definition.name !== record.definition.name;
      const saved = await api.save(record.id, definition);
      setRecord(saved);
      setDirty(false);
      say(renamed ? "Renamed and saved" : "Saved");
      return true;
    } catch (e) {
      setMessage({ kind: "error", text: e instanceof Error ? e.message : "cannot save" });
      return false;
    }
  };
  const toggle = async () => {
    if (dirty && !(await save())) return;
    try {
      setRecord(await api.setActive(record.id, !record.active));
      say(record.active ? "Turned off" : "Turned on: it runs from now on");
    } catch (e) {
      setMessage({ kind: "error", text: e instanceof Error ? e.message : "cannot change it" });
    }
  };
  const remove = async () => {
    await api.remove(record.id).catch(() => undefined);
    props.onGone();
  };
  const steps = definition.steps ?? [];
  const setSteps = (next: typeof steps) => edit({ ...definition, steps: next });
  const selected = selection?.kind === "step" ? stepAt(steps, selection.at) : undefined;
  const starts = triggerSummary(definition.trigger);
  // A step's or the trigger's window: whether its changes are saved, Save itself, and Done.
  const windowFoot = (
    <>
      <span className={`foot-state${dirty ? " unsaved" : ""}`}>
        {dirty ? (
          "Not saved yet"
        ) : flash ? (
          <>
            <Mark d="M5 12l5 5L20 7" /> {flash}
          </>
        ) : (
          "All saved"
        )}
      </span>
      <button className="quiet" onClick={() => setSelection(null)}>
        Done
      </button>
      <button onClick={save} disabled={!dirty}>
        <Mark d="M5 12l5 5L20 7" />
        Save
      </button>
    </>
  );
  // The workflow's own buttons: on or off, save, delete.
  const actions = (
    <div className="editor-actions">
      <label className={`on-off${record.active ? " on" : ""}`} title={record.active ? "It runs: click to turn it off" : "It does not run: click to turn it on"}>
        <span className="switch small">
          <input type="checkbox" checked={record.active} onChange={toggle} aria-label={record.active ? "Turn off" : "Turn on"} />
          <span />
        </span>
        {record.active ? "On" : "Off"}
      </label>
      <button className="save" onClick={save} disabled={!dirty}>
        Save
      </button>
      {deleting ? (
        <span className="confirm">
          <button className="danger" onClick={remove}>
            <Sym name="trash" />
            Delete it
          </button>
          <button className="quiet" onClick={() => setDeleting(false)}>
            <Sym name="check" />
            Keep it
          </button>
        </span>
      ) : (
        <button className="icon-only" aria-label="Delete the workflow" title="Delete the workflow" onClick={() => setDeleting(true)}>
          <Mark d={VIEW_MARKS.trash} />
        </button>
      )}
    </div>
  );

  return (
    <main className="page editor">
      <div className="editor-head">
        <label className="title-wrap" title="Rename the workflow">
          <input
            className="title"
            aria-label="Workflow name"
            value={definition.name}
            onChange={(e) => edit({ ...definition, name: e.target.value })}
            onKeyDown={(e) => {
              // Enter keeps the new name (the workflow saved); Escape puts back the one saved.
              if (e.key === "Enter") {
                e.preventDefault();
                e.currentTarget.blur();
                save();
              } else if (e.key === "Escape") {
                edit({ ...definition, name: record.definition.name });
                e.currentTarget.blur();
              }
            }}
            title="Rename it: Enter saves, Escape puts back"
          />
          <Mark d={VIEW_MARKS.rename} />
        </label>
        {flash && (
          <span className="saved-chip" role="status">
            <Mark d="M5 12l5 5L20 7" />
            {flash}
          </span>
        )}
        <button className="head-trigger" onClick={() => setSelection({ kind: "trigger" })} title="What starts it: click to change">
          <Icon kind={starts.kind} />
          <span>{starts.line}</span>
        </button>
        <nav className="views" role="tablist" aria-label="Views">
          {VIEWS.map((v) => (
            <button key={v.tab} role="tab" aria-selected={v.tab === tab} className={v.tab === tab ? "here" : ""} onClick={() => setTab(v.tab)}>
              <Mark d={VIEW_MARKS[v.tab]} />
              <span className="view-label">{v.label}</span>
              {v.tab === "versions" && <span className="view-count">v{record.version}</span>}
            </button>
          ))}
        </nav>
        {tab === "build" && <TryBar id={record.id} definition={definition} active={record.active} onRun={setRun} input={sample} setInput={setSample} />}
        {actions}
      </div>
      {message && <p className={message.kind === "error" ? "error banner" : "ok banner"}>{message.text}</p>}
      {tab === "build" && (
        <div className="build-canvas">
          <Canvas
            definition={definition}
            run={run}
            selection={selection}
            onSelect={setSelection}
            onAdd={(at) => setAdding(at)}
          />
          {selection?.kind === "trigger" && (
            <Popup label="Starts when" title={<h3>Starts when</h3>} onClose={() => setSelection(null)} footer={windowFoot}>
              <TriggerSettings definition={definition} record={record} onChange={edit} />
            </Popup>
          )}
          {selection?.kind === "step" && selected && (
            <Popup
              label={selected.name ?? "A step"}
              wide
              className="with-input"
              footer={windowFoot}
              title={<h3>{selected.name ?? "A step"}</h3>}
              onClose={() => setSelection(null)}
            >
              <InputPane definition={definition} at={selection.at} step={selected} sample={sample} setSample={setSample} run={run} box={box} />
              <div
                className="popup-settings"
                onFocusCapture={(e) => {
                  if (writable(e.target)) box.current = e.target;
                }}
              >
                <CredentialsContext.Provider value={credentials}>
                  <StepCard
                    key={JSON.stringify(selection.at)}
                    step={selected}
                    taken={taken}
                    onChange={(s) => setSteps(replaceAt(steps, selection.at, s))}
                    onRemove={() => {
                      setSteps(removeAt(steps, selection.at));
                      setSelection(null);
                    }}
                    onUp={() => {
                      setSteps(moveAt(steps, selection.at, -1));
                      if (selection.at.index > 0) setSelection({ kind: "step", at: { ...selection.at, index: selection.at.index - 1 } });
                    }}
                    onDown={() => {
                      const next = moveAt(steps, selection.at, 1);
                      setSteps(next);
                      if (stepAt(next, { ...selection.at, index: selection.at.index + 1 }) === selected)
                        setSelection({ kind: "step", at: { ...selection.at, index: selection.at.index + 1 } });
                    }}
                  />
                </CredentialsContext.Provider>
              </div>
              <section className="popup-output">
                <div className="output-head">
                  <h4>Output</h4>
                  <TryHere id={record.id} definition={definition} input={sample} onRun={setRun} />
                </div>
                <StepOutput run={run} name={selected.name} />
              </section>
            </Popup>
          )}
          {adding && (
            <AddStepDialog
              where={adding.list.length === 0 ? undefined : "inside a branch or loop"}
              onClose={() => setAdding(null)}
              onPick={(kind) => {
                setSteps(insertAt(steps, adding, fresh(kind, taken)));
                setSelection({ kind: "step", at: adding });
                setAdding(null);
              }}
            />
          )}
        </div>
      )}
      {tab !== "build" && (
        <div className="tab-body">
          {tab === "history" && <History id={record.id} />}
          {tab === "aside" && <Aside id={record.id} />}
          {tab === "versions" && (
            <Versions
              id={record.id}
              onOpen={(d) => {
                edit(d);
                setTab("build");
              }}
            />
          )}
        </div>
      )}
    </main>
  );
}

/** The editor's views, as its switcher shows them. */
const VIEWS: { tab: Tab; label: string }[] = [
  { tab: "build", label: "Build" },
  { tab: "history", label: "Runs" },
  { tab: "aside", label: "CatchErr" },
  { tab: "versions", label: "Versions" },
];

const VIEW_MARKS: Record<Tab | "rename" | "trash", string> = {
  build: "M4 6h6v4H4zM14 14h6v4h-6zM10 8h2a2 2 0 0 1 2 2v4",
  history: "M3 12h4l3-8 4 16 3-8h4",
  aside: "M12 3 2 20h20zM12 9v5M12 17h.01",
  versions: "M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 8v4l3 2",
  rename: "M4 20h4L19 9l-4-4L4 16zM14 6l4 4",
  trash: "M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3",
};

/** A small mark for the try bar's buttons, drawn with a stroke on a 24x24 grid. */
function Mark({ d }: { d: string }) {
  return (
    <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={d} />
    </svg>
  );
}

const MARKS = {
  test: "M7 5v14l11-7z",
  preview: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z",
  sample: "M8 4H7a2 2 0 0 0-2 2v4l-2 2 2 2v4a2 2 0 0 0 2 2h1M16 4h1a2 2 0 0 1 2 2v4l2 2-2 2v4a2 2 0 0 1-2 2h-1",
  more: "M5 12h.01M12 12h.01M19 12h.01",
};

/**
 * Trying the workflow, in the header beside its views: Test (sends nothing) and Preview (does what only reads) on a sample
 * input kept in its own window; the last real record previewed, or a real run, under more. What came of it is one
 * line under the bar; each step's own output is in its window.
 */
/** Test or Preview from a step's own window: the whole workflow on the sample input, this step's output shown. */
function TryHere(props: { id: string; definition: Definition; input: unknown; onRun: (run: Run | null) => void }) {
  const [busy, setBusy] = useState<"" | "test" | "preview">("");
  const [trouble, setTrouble] = useState("");
  const go = async (how: "test" | "preview") => {
    setBusy(how);
    setTrouble("");
    try {
      props.onRun(how === "test" ? await api.test(props.id, props.definition, props.input) : (await api.preview(props.id, props.definition, props.input, false)).run);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot run it");
    } finally {
      setBusy("");
    }
  };
  return (
    <span className="try-here">
      <button className="try-test" disabled={!!busy} onClick={() => go("test")} title="Run on the sample input; nothing is sent">
        <Mark d={MARKS.test} />
        {busy === "test" ? "Testing…" : "Test"}
      </button>
      <button className="quiet" disabled={!!busy} onClick={() => go("preview")} title="Reads for real, only shows what it would write">
        <Mark d={MARKS.preview} />
        {busy === "preview" ? "Previewing…" : "Preview"}
      </button>
      {trouble && <span className="error">{trouble}</span>}
    </span>
  );
}

function TryBar(props: {
  id: string;
  definition: Definition;
  active: boolean;
  onRun: (run: Run | null) => void;
  input: unknown;
  setInput: (v: unknown) => void;
}) {
  const { input, setInput } = props;
  const [editing, setEditing] = useState(false);
  const [menu, setMenu] = useState(false);
  const [busy, setBusy] = useState(false);
  const [note, setNote] = useState<{ kind: "ok" | "error" | "failed"; text: string } | null>(null);
  // Whether there is a real record to preview: asked each time the menu opens.
  const [real, setReal] = useState<boolean | null>(null);
  useEffect(() => {
    if (!menu) return;
    api
      .runs(props.id)
      .then((r) => setReal(r.length > 0))
      .catch(() => setReal(null));
    const away = (e: MouseEvent) => !(e.target as HTMLElement).closest(".try-more") && setMenu(false);
    addEventListener("mousedown", away);
    return () => removeEventListener("mousedown", away);
  }, [menu]);
  const said = (run: Run) => {
    props.onRun(run);
    const what = run.status === "done" ? "It ran through" : run.status === "skipped" ? "It stopped at a condition" : `It failed at ${run.steps.at(-1)?.name ?? "a step"}`;
    setNote({ kind: run.status === "failed" ? "failed" : "ok", text: `${what} in ${run.ms} ms. Click a step to see its output.` });
  };
  const attempt = async (work: () => Promise<void>, failing: string) => {
    setMenu(false);
    setBusy(true);
    setNote(null);
    try {
      await work();
    } catch (e) {
      props.onRun(null);
      setNote({ kind: "error", text: e instanceof Error ? e.message : failing });
    } finally {
      setBusy(false);
    }
  };
  const test = () => attempt(async () => said(await api.test(props.id, props.definition, input)), "cannot test it");
  const preview = (fromLast: boolean) =>
    attempt(async () => {
      const shown = await api.preview(props.id, props.definition, input, fromLast);
      if (fromLast) setInput(shown.input);
      said(shown.run);
    }, "cannot preview it");
  const runNow = () =>
    attempt(async () => {
      await api.runNow(props.id, input);
      setNote({ kind: "ok", text: "Queued for a real run: see Runs." });
    }, "cannot run it");
  return (
    <div className="try">
      <div className="try-bar" role="toolbar" aria-label="Try it">
        <button className="try-test" onClick={test} disabled={busy} title="Runs on the sample input and sends nothing">
          <Mark d={MARKS.test} />
          Test
        </button>
        <button className="try-quiet" onClick={() => preview(false)} disabled={busy} title="Reads for real - AI answers, GETs, lookups - and only shows what it would write">
          <Mark d={MARKS.preview} />
          Preview
        </button>
        <span className="try-split" aria-hidden="true" />
        <button className="try-quiet" onClick={() => setEditing(true)} title="The record a test or a preview runs on">
          <Mark d={MARKS.sample} />
          Sample input
        </button>
        <span className="try-more">
          <button className="try-icon" aria-label="More ways to try it" aria-expanded={menu} title="More" onClick={() => setMenu(!menu)}>
            <Mark d={MARKS.more} />
          </button>
          {menu && (
            <div className="try-menu" role="menu">
              <button role="menuitem" onClick={() => preview(true)} disabled={real === false}>
                <strong>Preview the last record</strong>
                <span>{real === false ? "No real record yet: it has not run" : "The most recent real record, previewed"}</span>
              </button>
              <button role="menuitem" onClick={runNow} disabled={!props.active}>
                <strong>Run for real</strong>
                <span>{props.active ? "The sample input, run as a real record" : "Turn the workflow on first"}</span>
              </button>
            </div>
          )}
        </span>
      </div>
      {busy && <p className="try-note">Working…</p>}
      {note && (
        <p className={`try-note ${note.kind}`}>
          <span>{note.text}</span>
          <button aria-label="Dismiss" onClick={() => setNote(null)}>
            ×
          </button>
        </p>
      )}
      {editing && (
        <Popup label="Sample input" title={<h3>Sample input</h3>} onClose={() => setEditing(false)}>
          <JsonField label="Sample input" value={input} onChange={setInput} />
          <p className="hint">The record a test or a preview runs on, as JSON, kept with the workflow when it is saved. Preview the last record takes a real one instead.</p>
        </Popup>
      )}
    </div>
  );
}

export function RunView({ run }: { run: Run }) {
  return (
    <div className="run">
      <p>
        <span className={`status ${run.status}`}>{run.status}</span> in {run.ms} ms
        {run.error && <span className="error"> - {run.error}</span>}
      </p>
      <ol className="results">
        {run.steps.map((s, i) => (
          <li key={i} className={s.error ? "failed" : ""}>
            <details open={Boolean(s.error) || run.steps.length <= 3}>
              <summary>
                <strong>{s.name}</strong> <span className="hint">{s.kind}</span>
                {s.attempts > 1 && <span className="hint"> - {s.attempts} tries</span>}
                {s.error && <span className="error"> - {s.error}</span>}
              </summary>
              {isAi(s) ? <AiResult step={s} /> : <pre>{JSON.stringify(s.result, null, 2)}</pre>}
              {s.logs && s.logs.length > 0 && (
                <>
                  <p className="hint">What it logged</p>
                  <pre className="logs">{s.logs.join("\n")}</pre>
                </>
              )}
            </details>
          </li>
        ))}
      </ol>
    </div>
  );
}

function when(ms?: number) {
  return ms ? new Date(ms).toLocaleString() : "";
}

function History({ id }: { id: string }) {
  const [runs, setRuns] = useState<Run[]>([]);
  const [open, setOpen] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    const load = () => api.runs(id).then((r) => live && setRuns(r)).catch(() => undefined);
    load();
    const every = setInterval(load, 3000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, [id]);
  if (runs.length === 0) return <div className="empty">No runs yet. They appear here as they happen.</div>;
  return (
    <>
    <MetricsView id={id} />
    <table className="runs">
      <thead>
        <tr>
          <th>When</th>
          <th>Result</th>
          <th>Started by</th>
          <th>Took</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((r) => (
          <RunRow key={r.run} run={r} open={open === r.run} onToggle={() => setOpen(open === r.run ? null : r.run ?? null)} />
        ))}
      </tbody>
    </table>
    </>
  );
}

function RunRow(props: { run: Run; open: boolean; onToggle: () => void }) {
  const r = props.run;
  return (
    <>
      <tr onClick={props.onToggle} className="clickable">
        <td>{when(r.at)}</td>
        <td>
          <span className={`status ${r.status}`}>{r.status}</span> {r.error && <span className="hint">{r.error}</span>}
        </td>
        <td>{String(r.trigger?.trigger ?? "")}</td>
        <td>{r.ms} ms</td>
      </tr>
      {props.open && (
        <tr>
          <td colSpan={4}>
            <RunView run={r} />
          </td>
        </tr>
      )}
    </>
  );
}

function Aside({ id }: { id: string }) {
  const [events, setEvents] = useState<SetAside[] | null>(null);
  const [note, setNote] = useState("");
  const load = () => api.failed(id).then(setEvents).catch((e: Error) => setNote(e.message));
  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);
  const again = async () => {
    const { queued } = await api.runFailed(id);
    setNote(`${queued} queued to run again: see Runs.`);
    load();
  };
  if (!events) return <p className="hint">{note}</p>;
  return (
    <div>
      <p className="hint">
        CatchErr holds each record whose run failed, with the reason. Fix the cause - the workflow, or what it calls - then
        run them again.
      </p>
      {note && <p className="ok">{note}</p>}
      {events.length === 0 ? (
        <div className="empty">Nothing in CatchErr: no run has failed.</div>
      ) : (
        <>
          <button onClick={again}>Run all {events.length} again</button>
          <ul className="aside">
            {events.map((e, i) => (
              <li key={i}>
                <p className="error">{e.error}</p>
                <pre>{JSON.stringify(e.event, null, 2)}</pre>
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

type AiShown = {
  text?: string;
  model?: string;
  finish_reason?: string;
  transcript?: { role: string; content: string }[];
  usage?: { prompt_tokens?: number | null; completion_tokens?: number | null; total_tokens?: number | null };
  metrics?: { latency_ms?: number; tokens_per_second?: number | null; ignored_settings?: string[] };
  json?: unknown;
};

export function isAi(step: StepRun) {
  return step.kind === "ai" && Array.isArray((step.result as AiShown | null)?.transcript);
}

/** An AI step's result: its transcript as a conversation, and what it cost. */
export function AiResult({ step }: { step: StepRun }) {
  const r = step.result as AiShown;
  return (
    <div className="ai">
      <ol className="transcript">
        {(r.transcript ?? []).map((m, i) => (
          <li key={i} className={`said ${m.role}`}>
            <span className="who">{m.role}</span>
            <span className="what">{m.content}</span>
          </li>
        ))}
      </ol>
      <p className="chips">
        {r.model && <span>{r.model}</span>}
        {r.usage?.total_tokens != null && (
          <span>
            {r.usage.prompt_tokens} in / {r.usage.completion_tokens} out
          </span>
        )}
        {r.metrics?.latency_ms != null && <span>{r.metrics.latency_ms} ms</span>}
        {r.metrics?.tokens_per_second != null && <span>{r.metrics.tokens_per_second} tokens/s</span>}
        {r.finish_reason && r.finish_reason !== "stop" && <span className="warn">{r.finish_reason}</span>}
        {r.metrics?.ignored_settings?.length ? <span className="warn">ignored: {r.metrics.ignored_settings.join(", ")}</span> : null}
      </p>
      {r.json !== undefined && <pre>{JSON.stringify(r.json, null, 2)}</pre>}
    </div>
  );
}

function MetricsView({ id }: { id: string }) {
  const [m, setM] = useState<Metrics | null>(null);
  useEffect(() => {
    api.metrics(id).then(setM).catch(() => undefined);
  }, [id]);
  if (!m || m.runs === 0) return null;
  return (
    <div className="metrics">
      <div className="stat">
        <strong>{m.runs}</strong>
        <span>runs</span>
      </div>
      <div className="stat">
        <strong>{m.success_percent ?? "-"}%</strong>
        <span>succeeded</span>
      </div>
      <div className="stat">
        <strong>{m.duration_ms.median} ms</strong>
        <span>median, {m.duration_ms.p95} ms p95</span>
      </div>
      <div className="stat">
        <strong>{m.tokens.total_tokens}</strong>
        <span>AI tokens</span>
      </div>
      <table className="steps-metrics">
        <thead>
          <tr>
            <th>Step</th>
            <th>Average</th>
            <th>Failures</th>
            <th>Tokens</th>
          </tr>
        </thead>
        <tbody>
          {m.steps.map((s) => (
            <tr key={s.name}>
              <td>{s.name}</td>
              <td>{s.average_ms} ms</td>
              <td>{s.failures}</td>
              <td>{s.tokens || ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Versions(props: { id: string; onOpen: (d: Definition) => void }) {
  const [versions, setVersions] = useState<VersionRecord[] | null>(null);
  useEffect(() => {
    api.versions(props.id).then(setVersions).catch(() => undefined);
  }, [props.id]);
  if (!versions) return null;
  return (
    <table className="list">
      <thead>
        <tr>
          <th>Version</th>
          <th>Saved</th>
          <th>Steps</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {versions.map((v) => (
          <tr key={v.number}>
            <td>{v.number}</td>
            <td>{new Date(v.created * 1000).toLocaleString()}</td>
            <td>{v.definition?.steps?.length ?? 0}</td>
            <td className="actions">
              <button className="quiet" onClick={() => props.onOpen(v.definition)}>
                Open in the editor
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
