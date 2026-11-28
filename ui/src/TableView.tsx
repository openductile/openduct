// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// One data table: its rows a page at a time - sorted by a column, filtered by an expression - each cell edited in
// place, rows added and deleted; its columns changed; CSV imported into it and exported from it.

import { useEffect, useState } from "react";
import { tablesApi, type ColumnEdit, type ColumnType, type RowPage, type TableColumn, type TableLimits, type TableSummary } from "./tablesApi";

const PAGE = 50;

export const TYPES: { type: ColumnType; label: string }[] = [
  { type: "text", label: "Text" },
  { type: "number", label: "Number" },
  { type: "boolean", label: "True / false" },
  { type: "datetime", label: "Date and time" },
  { type: "json", label: "JSON" },
];

export function size(bytes: number) {
  return bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
}

export function ColumnsEditor(props: { columns: (TableColumn & { from?: string })[]; onChange: (c: (TableColumn & { from?: string })[]) => void }) {
  const set = (i: number, next: Partial<TableColumn>) => props.onChange(props.columns.map((c, j) => (j === i ? { ...c, ...next } : c)));
  return (
    <div className="table-columns">
      {props.columns.map((c, i) => (
        <div className="table-column" key={i}>
          <input aria-label="Column name" placeholder="column" data-plain value={c.name} onChange={(e) => set(i, { name: e.target.value.replace(/[^A-Za-z0-9_]/g, "_") })} />
          <select aria-label={`Type of ${c.name || "the column"}`} value={c.type} onChange={(e) => set(i, { type: e.target.value as ColumnType })}>
            {TYPES.map((t) => (
              <option key={t.type} value={t.type}>
                {t.label}
              </option>
            ))}
          </select>
          {c.from && c.from !== c.name && <span className="hint">was {c.from}</span>}
          <button type="button" className="quiet" title="Remove the column" aria-label={`Remove ${c.name || "the column"}`} onClick={() => props.onChange(props.columns.filter((_, j) => j !== i))}>
            ✕
          </button>
        </div>
      ))}
      <button type="button" className="quiet" onClick={() => props.onChange([...props.columns, { name: `column${props.columns.length + 1}`, type: "text" }])}>
        + Column
      </button>
    </div>
  );
}


/** What a cell's text is sent as: JSON parsed for a JSON column, nothing for an empty one that is not text. */
function valueOf(text: string, type: string): unknown {
  if (text === "" && type !== "text") return null;
  if (type === "json") {
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  return text;
}

function shown(value: unknown): string {
  if (value === null || value === undefined) return "";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}

function Cell(props: { value: unknown; type: string; canEdit: boolean; onSave: (v: unknown) => Promise<void> }) {
  const [editing, setEditing] = useState(false);
  const [text, setText] = useState("");
  if (!editing) {
    return (
      <td
        className={props.canEdit ? "cell editable" : "cell"}
        title={props.canEdit ? "Click to change it" : undefined}
        onClick={() => {
          if (!props.canEdit) return;
          setText(shown(props.value));
          setEditing(true);
        }}
      >
        {shown(props.value)}
      </td>
    );
  }
  const save = async () => {
    setEditing(false);
    if (text !== shown(props.value)) await props.onSave(valueOf(text, props.type));
  };
  return (
    <td className="cell">
      <input
        autoFocus
        aria-label="The cell's value"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onBlur={save}
        onKeyDown={(e) => {
          if (e.key === "Enter") save();
          if (e.key === "Escape") setEditing(false);
        }}
      />
    </td>
  );
}

export function TableView(props: { name: string; canEdit: boolean; limits: TableLimits | null; onBack: () => void; onRenamed: (name: string) => void }) {
  const [table, setTable] = useState<TableSummary | null>(null);
  const [page, setPage] = useState<RowPage | null>(null);
  const [offset, setOffset] = useState(0);
  const [sort, setSort] = useState<{ column: string; desc: boolean }>({ column: "id", desc: false });
  const [filter, setFilter] = useState("");
  const [applied, setApplied] = useState("");
  const [picked, setPicked] = useState<Set<number>>(new Set());
  const [adding, setAdding] = useState<Record<string, string> | null>(null);
  const [editing, setEditing] = useState<{ name: string; columns: ColumnEdit[] } | null>(null);
  const [trouble, setTrouble] = useState("");
  const [note, setNote] = useState("");
  const fail = (e: unknown) => setTrouble(e instanceof Error ? e.message : "it did not work");
  const load = async () => {
    try {
      const list = await tablesApi.list();
      setTable(list.tables.find((t) => t.name === props.name) ?? null);
      setPage(await tablesApi.rows(props.name, { offset, limit: PAGE, sort: sort.column, desc: sort.desc, where: applied }));
      setTrouble("");
    } catch (e) {
      fail(e);
    }
  };
  useEffect(() => {
    load();
  }, [props.name, offset, sort.column, sort.desc, applied]);
  const columns = table?.columns ?? [];
  const typeOf = (c: string) => columns.find((x) => x.name === c)?.type ?? "text";
  const by = (column: string) => setSort((s) => ({ column, desc: s.column === column ? !s.desc : false }));
  const edit = async (id: number, column: string, value: unknown) => {
    await tablesApi.editRow(props.name, id, { [column]: value }).then(load, fail);
  };
  const add = async () => {
    if (!adding) return;
    const row = Object.fromEntries(Object.entries(adding).map(([k, v]) => [k, valueOf(v, typeOf(k))]));
    await tablesApi.addRows(props.name, [row]).then(() => {
      setAdding(null);
      return load();
    }, fail);
  };
  const removePicked = async () => {
    await tablesApi.removeRows(props.name, [...picked]).then(() => {
      setPicked(new Set());
      return load();
    }, fail);
  };
  const importCsv = async (file: File) => {
    if (file.size > 1024 * 1024) return setTrouble(`${file.name} is over 1 MiB: import it in parts`);
    await tablesApi.importCsv(props.name, await file.text()).then((r) => {
      setNote(`${r.added} rows added`);
      return load();
    }, fail);
  };
  const exportCsv = async () => {
    try {
      const { csv } = await tablesApi.exportCsv(props.name);
      const link = document.createElement("a");
      link.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
      link.download = `${props.name}.csv`;
      link.click();
      URL.revokeObjectURL(link.href);
    } catch (e) {
      fail(e);
    }
  };
  const saveColumns = async () => {
    if (!editing) return;
    try {
      const next = await tablesApi.change(props.name, { name: editing.name, columns: editing.columns });
      setEditing(null);
      if (next.name !== props.name) props.onRenamed(next.name);
      else load();
    } catch (e) {
      fail(e);
    }
  };
  const header = ["id", ...columns.map((c) => c.name), "updated_at"];
  return (
    <section className="table-view" aria-label={`Table ${props.name}`}>
      <div className="files-head">
        <span className="row-tools">
          <button className="quiet" onClick={props.onBack}>
            ← Tables
          </button>
          <h1>
            <code>{props.name}</code>
          </h1>
          {table && (
            <span className="hint">
              {table.rows.toLocaleString()} row{table.rows === 1 ? "" : "s"}, {size(table.bytes)}
              {props.limits && ` (at most ${props.limits.rows.toLocaleString()})`}
            </span>
          )}
        </span>
        <span className="row-tools">
          {props.canEdit && (
            <>
              <button className="quiet" onClick={() => setAdding(Object.fromEntries(columns.map((c) => [c.name, ""])))}>
                Add a row
              </button>
              <button className="quiet" onClick={() => setEditing({ name: props.name, columns: columns.map((c) => ({ ...c, from: c.name })) })}>
                Columns
              </button>
              <label className="button upload quiet">
                Import CSV
                <input type="file" accept=".csv,text/csv" hidden onChange={(e) => e.target.files?.[0] && importCsv(e.target.files[0])} />
              </label>
            </>
          )}
          <button className="quiet" onClick={exportCsv}>
            Export CSV
          </button>
        </span>
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {note && <p className="hint">{note}</p>}
      {editing && (
        <section className="card table-new" aria-label="The table's columns">
          <label>
            Name
            <input value={editing.name} data-plain onChange={(e) => setEditing({ ...editing, name: e.target.value.replace(/[^A-Za-z0-9_]/g, "_") })} />
          </label>
          <p className="hint">A column left out is deleted with its values; a type changed reads every value again as the new type.</p>
          <ColumnsEditor columns={editing.columns} onChange={(c) => setEditing({ ...editing, columns: c })} />
          <div className="row">
            <button onClick={saveColumns}>Save the columns</button>
            <button className="quiet" onClick={() => setEditing(null)}>
              Cancel
            </button>
          </div>
        </section>
      )}
      <div className="row table-filter">
        <input
          className="code"
          data-expr=""
          aria-label="Only the rows where"
          placeholder="only the rows where... as row.score > 10"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              setOffset(0);
              setApplied(filter);
            }
          }}
        />
        {picked.size > 0 && props.canEdit && (
          <button className="quiet remove" onClick={removePicked}>
            Delete {picked.size} row{picked.size === 1 ? "" : "s"}
          </button>
        )}
      </div>
      <div className="scroll">
        <table className="compact table-rows">
          <thead>
            <tr>
              {props.canEdit && <th />}
              {header.map((c) => (
                <th key={c} onClick={() => by(c)} className="sortable" aria-sort={sort.column === c ? (sort.desc ? "descending" : "ascending") : "none"}>
                  {c}
                  {sort.column === c ? (sort.desc ? " ↓" : " ↑") : ""}
                  {c !== "id" && c !== "updated_at" && <span className="muted"> {typeOf(c)}</span>}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {adding && (
              <tr className="adding">
                {props.canEdit && <td />}
                <td className="muted">new</td>
                {columns.map((c) => (
                  <td key={c.name}>
                    <input aria-label={`New row's ${c.name}`} value={adding[c.name] ?? ""} onChange={(e) => setAdding({ ...adding, [c.name]: e.target.value })} />
                  </td>
                ))}
                <td>
                  <button onClick={add}>Add</button>
                  <button className="quiet" onClick={() => setAdding(null)}>
                    ✕
                  </button>
                </td>
              </tr>
            )}
            {page?.rows.map((row) => (
              <tr key={row.id}>
                {props.canEdit && (
                  <td>
                    <input
                      type="checkbox"
                      aria-label={`Pick row ${row.id}`}
                      checked={picked.has(row.id)}
                      onChange={(e) => {
                        const next = new Set(picked);
                        if (e.target.checked) next.add(row.id);
                        else next.delete(row.id);
                        setPicked(next);
                      }}
                    />
                  </td>
                )}
                <td className="muted">{row.id}</td>
                {columns.map((c) => (
                  <Cell key={c.name} value={row[c.name]} type={c.type} canEdit={props.canEdit} onSave={(v) => edit(row.id, c.name, v)} />
                ))}
                <td className="muted" title={row.updated_at}>
                  {new Date(row.updated_at).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {page && page.rows.length === 0 && <div className="empty-note">{applied ? "No row matches." : "No rows yet: add one, or import a CSV file."}</div>}
      {page && page.matched > PAGE && (
        <div className="row pager">
          <button className="quiet" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>
            ← Earlier
          </button>
          <span className="hint">
            {offset + 1}-{Math.min(offset + PAGE, page.matched)} of {page.matched.toLocaleString()}
          </span>
          <button className="quiet" disabled={offset + PAGE >= page.matched} onClick={() => setOffset(offset + PAGE)}>
            Later →
          </button>
        </div>
      )}
    </section>
  );
}
