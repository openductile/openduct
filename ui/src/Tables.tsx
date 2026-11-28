// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A workspace's data tables: made (with typed columns, or from a CSV file), opened to browse and edit their rows,
// deleted - and the values its workflows keep between runs, shown and cleared. Table steps read and write them.

import { useEffect, useState } from "react";
import { ColumnsEditor, TableView, size } from "./TableView";
import { tablesApi, type TableColumn, type TableLimits, type TableSummary, type WorkflowState } from "./tablesApi";
import "./tables.css";

/** A name a table or column may have: letters, digits and _, starting with a letter. */
export function nameOf(text: string): string {
  const cleaned = text.trim().toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 60);
  return /^[a-z]/.test(cleaned) ? cleaned : cleaned ? `t_${cleaned}` : "";
}

function ago(s: number) {
  const d = Math.max(0, Date.now() / 1000 - s);
  if (d < 60) return "just now";
  if (d < 3600) return `${Math.floor(d / 60)} min ago`;
  if (d < 86400) return `${Math.floor(d / 3600)} h ago`;
  return new Date(s * 1000).toLocaleDateString();
}

function NewTable(props: { onMade: (name: string) => void; onCancel: () => void }) {
  const [name, setName] = useState("");
  const [columns, setColumns] = useState<TableColumn[]>([{ name: "name", type: "text" }]);
  const [trouble, setTrouble] = useState("");
  const make = async () => {
    setTrouble("");
    try {
      const made = await tablesApi.create(name, columns);
      props.onMade(made.name);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot make it");
    }
  };
  return (
    <section className="card table-new" aria-label="New table">
      <label>
        Name <span className="hint">letters, digits and _: what steps name it by</span>
        <input value={name} data-plain onChange={(e) => setName(e.target.value.replace(/[^A-Za-z0-9_]/g, "_"))} placeholder="leads" />
      </label>
      <p className="hint">Its columns, each with what it holds. Every row also has an id, and when it was added and last changed.</p>
      <ColumnsEditor columns={columns} onChange={setColumns} />
      {trouble && <p className="error">{trouble}</p>}
      <div className="row">
        <button onClick={make} disabled={!name}>
          Make the table
        </button>
        <button className="quiet" onClick={props.onCancel}>
          Cancel
        </button>
      </div>
    </section>
  );
}

function States(props: { canEdit: boolean }) {
  const [states, setStates] = useState<WorkflowState[] | null>(null);
  const [shown, setShown] = useState("");
  const load = () => tablesApi.states().then(setStates).catch(() => setStates([]));
  useEffect(() => {
    load();
  }, []);
  if (!states || states.length === 0) return null;
  return (
    <section className="table-states" aria-label="Kept by workflows">
      <h2>Kept by workflows</h2>
      <p className="hint">What each workflow keeps between its runs (its state steps, and scripts given the state).</p>
      <table className="compact">
        <thead>
          <tr>
            <th>Workflow</th>
            <th className="num">Values</th>
            <th className="num">Size</th>
            <th>Changed</th>
            <th className="num" />
          </tr>
        </thead>
        <tbody>
          {states.map((s) => (
            <tr key={s.workflow}>
              <td>{s.name ?? s.workflow}</td>
              <td className="num">{Object.keys(s.values).length}</td>
              <td className="num muted">{size(s.bytes)}</td>
              <td className="muted">{ago(s.updated / 1000)}</td>
              <td className="num">
                <span className="row-tools">
                  <button className="quiet" onClick={() => setShown(shown === s.workflow ? "" : s.workflow)}>
                    {shown === s.workflow ? "Hide" : "Show"}
                  </button>
                  {props.canEdit && (
                    <button className="quiet remove" onClick={() => tablesApi.clearState(s.workflow).then(load)} aria-label={`Clear what ${s.name ?? s.workflow} keeps`}>
                      Clear
                    </button>
                  )}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {shown && <pre className="code table-state">{JSON.stringify(states.find((s) => s.workflow === shown)?.values ?? {}, null, 2)}</pre>}
    </section>
  );
}

export function Tables(props: { canEdit: boolean }) {
  const [list, setList] = useState<TableSummary[] | null>(null);
  const [limits, setLimits] = useState<TableLimits | null>(null);
  const [making, setMaking] = useState(false);
  const [open, setOpen] = useState("");
  const [trouble, setTrouble] = useState("");
  const load = () =>
    tablesApi
      .list()
      .then((l) => {
        setList(l.tables);
        setLimits(l.limits);
      })
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
  }, []);
  const fromCsv = async (file: File) => {
    setTrouble("");
    if (file.size > 1024 * 1024) return setTrouble(`${file.name} is over 1 MiB: import it in parts`);
    try {
      const made = await tablesApi.createFromCsv(nameOf(file.name.replace(/\.[^.]*$/, "")), await file.text());
      await load();
      setOpen(made.name);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot read it");
    }
  };
  const remove = async (name: string) => {
    if (!window.confirm(`Delete the table ${name} and every row in it?`)) return;
    await tablesApi.remove(name).catch((e: Error) => setTrouble(e.message));
    if (open === name) setOpen("");
    load();
  };
  if (open) {
    return (
      <main className="page list-page tables-page">
        <TableView
          name={open}
          canEdit={props.canEdit}
          limits={limits}
          onBack={() => {
            setOpen("");
            load();
          }}
          onRenamed={(name) => setOpen(name)}
        />
      </main>
    );
  }
  return (
    <main className="page list-page tables-page">
      <h1 className="visually-hidden">Tables</h1>
      <div className="files-head">
        <p className="hint">
          Data your workflows keep between runs: rows with typed columns, read and written by table steps.
          {limits && ` Up to ${limits.tables} tables, ${limits.rows.toLocaleString()} rows each, ${size(limits.workspace_bytes)} in all.`}
        </p>
        {props.canEdit && (
          <span className="row-tools">
            <button onClick={() => setMaking(true)}>New table</button>
            <label className="button upload quiet">
              Make one from a CSV file
              <input type="file" accept=".csv,text/csv" hidden onChange={(e) => e.target.files?.[0] && fromCsv(e.target.files[0])} />
            </label>
          </span>
        )}
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {making && (
        <NewTable
          onCancel={() => setMaking(false)}
          onMade={(name) => {
            setMaking(false);
            load();
            setOpen(name);
          }}
        />
      )}
      {list && list.length === 0 && !making && <div className="empty-note">No tables yet: make one, or make one from a CSV file.</div>}
      {list && list.length > 0 && (
        <table className="compact files-table">
          <thead>
            <tr>
              <th>Table</th>
              <th>Columns</th>
              <th className="num">Rows</th>
              <th className="num">Size</th>
              <th>Changed</th>
              <th className="num" />
            </tr>
          </thead>
          <tbody>
            {list.map((t) => (
              <tr key={t.id}>
                <td>
                  <button className="link" onClick={() => setOpen(t.name)}>
                    <code>{t.name}</code>
                  </button>
                </td>
                <td className="muted">{t.columns.map((c) => c.name).join(", ") || "none yet"}</td>
                <td className="num">{t.rows.toLocaleString()}</td>
                <td className="num muted">{size(t.bytes)}</td>
                <td className="muted">{ago(t.updated)}</td>
                <td className="num">
                  {props.canEdit && (
                    <button className="icon-button remove" title="Delete the table" aria-label={`Delete ${t.name}`} onClick={() => remove(t.name)}>
                      ✕
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <States canEdit={props.canEdit} />
    </main>
  );
}
