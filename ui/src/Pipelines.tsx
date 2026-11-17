// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Pipelines: read a topic, change each message, write another - continuously. The workspace's pipelines on the left,
// each with how its run is going; the one chosen on the right: its file, checked as it is written, tried over sample
// messages, kept, turned on or off. Turned on, OpenDuct runs it - its topics made if they are not there yet.

import { useEffect, useState } from "react";
import { api, type PipelineCheck, type PipelineSummary } from "./api";
import { Sym } from "./Sym";
import { Grow } from "./Grow";

function starter(name: string): string {
  return `name: ${name}
from:
  topic: orders
  start: earliest
where: status != 'test'
map:
  order_id: id
  total: qty * price
key: order_id
to:
  topic: orders-clean
on_error:
  topic: orders-rejected
`;
}

const SAMPLE = `{"id": "A-100", "status": "paid", "qty": 2, "price": 5}
{"id": "T-1", "status": "test", "qty": 1, "price": 1}
{"id": "A-101", "status": "paid", "qty": "lots", "price": 4}`;

function stateOf(p: PipelineSummary): "error" | "running" | "starting" | "off" {
  if (!p.active) return "off";
  if (p.error) return "error";
  return p.running ? "running" : "starting";
}

const WORD = { error: "Stopped, trying again", running: "Running", starting: "Starting", off: "Off" };

function count(n: number): string {
  return n.toLocaleString();
}

export function Pipelines(props: { canEdit: boolean }) {
  const [list, setList] = useState<PipelineSummary[] | null>(null);
  const [chosen, setChosen] = useState<string | null>(null);
  const [trouble, setTrouble] = useState("");
  const load = () =>
    api
      .pipelines()
      .then((l) => {
        setList(l);
        setTrouble("");
        if (!chosen && l[0]) setChosen(l[0].id);
      })
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
    // Counts move while a pipeline runs.
    const every = setInterval(load, 3000);
    return () => clearInterval(every);
  }, []);
  const make = async () => {
    const taken = new Set((list ?? []).map((p) => p.name));
    let n = 1;
    while (taken.has(`pipeline-${n}`)) n++;
    try {
      const made = await api.addPipeline(starter(`pipeline-${n}`));
      setChosen(made.id);
      load();
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot make it");
    }
  };
  const current = list?.find((p) => p.id === chosen) ?? null;
  return (
    <main className="page list-page pipelines-page">
      <h1 className="visually-hidden">Pipelines</h1>
      <div className="files-head">
        <p className="hint">Read a topic, change each message with expressions, write another - continuously, as messages arrive. Turned on, OpenDuct runs it. The topics are this workspace's own.</p>
        {props.canEdit && (
          <button onClick={make}>
            <Sym name="plus" />
            New pipeline
          </button>
        )}
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {list && list.length === 0 && (
        <div className="empty-note">No pipeline yet: a new one starts from an example that cleans orders - change it to yours.</div>
      )}
      {list && list.length > 0 && (
        <div className="pipes">
          <ul className="pipe-list" aria-label="Pipelines">
            {list.map((p) => {
              const s = stateOf(p);
              return (
                <li key={p.id}>
                  <button className={`pipe-item${p.id === chosen ? " here" : ""}`} onClick={() => setChosen(p.id)}>
                    <span className={`dot ${s === "running" ? "done" : s === "error" ? "failed" : s === "starting" ? "skipped" : ""}`} title={WORD[s]} />
                    <span className="pipe-words">
                      <span className="pipe-name">{p.name}</span>
                      <span className="pipe-route">
                        {p.from} → {p.to}
                      </span>
                      {p.active && (
                        <span className="pipe-counts">
                          {count(p.read)} in · {count(p.written)} out{p.set_aside ? ` · ${count(p.set_aside)} rejected` : ""}
                        </span>
                      )}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
          {current && <Editor key={current.id} pipeline={current} canEdit={props.canEdit} onChanged={load} onGone={() => (setChosen(null), load())} />}
        </div>
      )}
    </main>
  );
}

function Editor(props: { pipeline: PipelineSummary; canEdit: boolean; onChanged: () => void; onGone: () => void }) {
  const p = props.pipeline;
  const [source, setSource] = useState(p.source);
  const [checked, setChecked] = useState<{ ok: PipelineCheck } | { error: string } | null>(null);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [input, setInput] = useState(SAMPLE);
  const [output, setOutput] = useState<unknown[] | null>(null);
  const [deleting, setDeleting] = useState(false);
  const dirty = source !== p.source;
  // Checked as it is written, a moment after the last key.
  useEffect(() => {
    const wait = setTimeout(() => {
      api
        .checkPipeline(source)
        .then((ok) => setChecked({ ok }))
        .catch((e: Error) => setChecked({ error: e.message }));
    }, 400);
    return () => clearTimeout(wait);
  }, [source]);
  const act = async (work: () => Promise<unknown>, done: string) => {
    setNote(null);
    try {
      await work();
      setNote({ ok: true, text: done });
      props.onChanged();
    } catch (e) {
      setNote({ ok: false, text: e instanceof Error ? e.message : "cannot do it" });
    }
  };
  const test = async () => {
    setOutput(null);
    try {
      setOutput(await api.testPipeline(source, input));
    } catch (e) {
      setOutput([{ error: e instanceof Error ? e.message : "cannot test it" }]);
    }
  };
  const s = stateOf(p);
  return (
    <section className="pipe-editor" aria-label={p.name}>
      <div className="pipe-head">
        <span className={`dot ${s === "running" ? "done" : s === "error" ? "failed" : s === "starting" ? "skipped" : ""}`} />
        <span className="pipe-title">{p.name}</span>
        <span className="hint">{WORD[s]}</span>
        {props.canEdit && (
          <span className="pipe-tools">
            <label className={`on-off${p.active ? " on" : ""}`} title={p.active ? "It runs: click to turn it off" : "Turn it on: OpenDuct runs it, its topics made if missing"}>
              <span className="switch small">
                <input type="checkbox" checked={p.active} disabled={dirty && !p.active} aria-label={p.active ? "Turn off" : "Turn on"} onChange={() => act(() => api.setPipelineActive(p.id, !p.active), p.active ? "Turned off" : "Turned on: it runs from now on")} />
                <span />
              </span>
              {p.active ? "On" : "Off"}
            </label>
            <button disabled={!dirty || "error" in (checked ?? {})} onClick={() => act(() => api.savePipeline(p.id, source), "Saved")}>
              <Sym name="check" />
              Save
            </button>
            {deleting ? (
              <>
                <button className="danger" onClick={() => act(() => api.removePipeline(p.id), "Removed").then(props.onGone)}>
                  <Sym name="trash" />
                  Delete it
                </button>
                <button className="quiet" onClick={() => setDeleting(false)}>
                  Keep it
                </button>
              </>
            ) : (
              <button className="icon-only quiet" aria-label="Delete the pipeline" title="Delete the pipeline" onClick={() => setDeleting(true)}>
                <Sym name="trash" />
              </button>
            )}
          </span>
        )}
      </div>
      {note && <p className={note.ok ? "ok pipe-note" : "error pipe-note"}>{note.text}</p>}
      {p.error && p.active && <p className="error banner">It stopped: {p.error}. It is tried again by itself; fix the file or the topics it names.</p>}
      {p.active && (
        <div className="tiles pipe-tiles">
          <div className="tile">
            <span className="tile-label">Read</span>
            <span className="tile-value">{count(p.read)}</span>
          </div>
          <div className="tile good">
            <span className="tile-label">Written</span>
            <span className="tile-value">{count(p.written)}</span>
          </div>
          <div className={`tile${p.set_aside ? " bad" : ""}`}>
            <span className="tile-label">Rejected</span>
            <span className="tile-value">{count(p.set_aside)}</span>
            {p.error_topic && <span className="tile-note">to {p.error_topic}</span>}
          </div>
          <div className="tile">
            <span className="tile-label">Since</span>
            <span className="tile-value small">{p.since ? new Date(p.since * 1000).toLocaleTimeString() : "-"}</span>
            <span className="tile-note">on this node</span>
          </div>
        </div>
      )}
      <div className="pipe-body">
        <div className="pipe-file">
          <div className="card-head">
            <span>The pipeline file</span>
            <span className={`check-line ${checked && "ok" in checked ? (checked.ok.missing?.length ? "warn" : "good") : checked ? "bad" : ""}`}>
              {checked === null
                ? "Checking…"
                : "ok" in checked
                  ? checked.ok.missing?.length
                    ? `⚠ add in Credentials: ${checked.ok.missing.join(", ")}`
                    : `✓ reads ${checked.ok.from}, writes ${checked.ok.to}`
                  : `✕ ${checked.error}`}
            </span>
          </div>
          <Grow className="code pipe-source" aria-label="The pipeline file" rows={14} max={720} value={source} readOnly={!props.canEdit} onChange={setSource} />
          <p className="hint">
            Steps run in order: let, where, each, map, then key and headers. Text in an expression goes in single quotes: <code>"'USD'"</code>. A password or key:{" "}
            <code>secret:NAME</code>, the workspace's credential of that name.
          </p>
        </div>
        <div className="pipe-test">
          <div className="card-head">
            <span>Try it</span>
            <span className="hint">one message per line, nothing sent</span>
            <button className="quiet" onClick={test}>
              <Sym name="arrow" />
              Test
            </button>
          </div>
          <Grow className="code pipe-input" aria-label="Sample messages" rows={6} max={320} value={input} onChange={setInput} />
          {output && (
            <ol className="pipe-output" aria-label="What it would write">
              {output.length === 0 && <li className="hint">Nothing would be written: every message was left out.</li>}
              {output.map((o, i) => {
                const line = o as { key?: unknown; value?: unknown; error?: string; line?: number };
                return line.error ? (
                  <li key={i} className="bad">
                    {line.line ? `line ${line.line}: ` : ""}
                    {line.error}
                  </li>
                ) : (
                  <li key={i}>
                    {line.key != null && <span className="out-key">{String(line.key)}</span>}
                    <code>{JSON.stringify(line.value)}</code>
                  </li>
                );
              })}
            </ol>
          )}
        </div>
      </div>
    </section>
  );
}
