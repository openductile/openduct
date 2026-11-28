// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The forms of the steps that keep data between runs: a data table's rows (get, count, add, update, add or update,
// delete), and what the workflow keeps from one run to the next (state). Each writes the step as the engine reads it.

import { useEffect, useState } from "react";
import { Expr, Pairs, Text } from "./Steps";
import { tablesApi, type TableSummary } from "./tablesApi";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const TABLE_KINDS = ["table", "state"];

const ACTIONS: { action: string; label: string }[] = [
  { action: "get", label: "Get rows" },
  { action: "count", label: "Count rows" },
  { action: "insert", label: "Add rows" },
  { action: "update", label: "Update rows" },
  { action: "upsert", label: "Add or update a row (by key)" },
  { action: "delete", label: "Delete rows" },
];

const OPS = ["=", "!=", "<", "<=", ">", ">=", "contains", "starts_with", "ends_with", "empty", "not_empty"];

/** A table step's settings each action takes. */
const SETTINGS: Record<string, string[]> = {
  get: ["where", "match", "sort", "limit", "offset"],
  count: ["where", "match"],
  insert: ["row", "rows"],
  update: ["where", "match", "set"],
  upsert: ["key", "row"],
  delete: ["where", "match"],
};

export function TableStepBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  return props.kind === "state" ? <StateBody fields={props.fields} setBody={props.setBody} /> : <TableBody fields={props.fields} setBody={props.setBody} />;
}

type Test = { column?: string; op?: string; value?: string };

/** Which rows: every row, columns equal to values, comparisons, or an expression over `row`. */
function Which(props: { fields: Fields; required: boolean; columns: string[]; put: Put }) {
  const where = props.fields.where;
  const mode = where === undefined ? "all" : typeof where === "string" ? "expression" : Array.isArray(where) ? "tests" : "equal";
  const tests = (Array.isArray(where) ? where : []) as Test[];
  const choose = (m: string) =>
    props.put("where", m === "all" ? undefined : m === "expression" ? "row.id > 0" : m === "tests" ? [{ column: props.columns[0] ?? "", op: "=", value: "" }] : {});
  return (
    <>
      <label>
        Which rows
        <select value={mode} onChange={(e) => choose(e.target.value)}>
          {!props.required && <option value="all">every row</option>}
          {props.required && mode === "all" && <option value="all">choose which rows</option>}
          <option value="equal">where columns hold these values</option>
          <option value="tests">where columns compare with values</option>
          <option value="expression">where an expression is true</option>
        </select>
      </label>
      {mode === "equal" && <Pairs value={(where ?? {}) as Record<string, string>} onChange={(v) => props.put("where", v)} keyLabel="Column" valueLabel="Value" />}
      {mode === "expression" && <Expr label="Rows where" value={String(where ?? "")} onChange={(v) => props.put("where", v)} />}
      {mode === "tests" && (
        <>
          {tests.map((t, i) => (
            <div className="table-test" key={i}>
              <input aria-label="Column" placeholder="column" list="table-columns" data-plain value={t.column ?? ""} onChange={(e) => props.put("where", tests.map((x, j) => (j === i ? { ...x, column: e.target.value } : x)))} />
              <select aria-label="Compared" value={t.op ?? "="} onChange={(e) => props.put("where", tests.map((x, j) => (j === i ? (e.target.value.includes("empty") ? { column: x.column, op: e.target.value } : { ...x, op: e.target.value, value: x.value ?? "" }) : x)))}>
                {OPS.map((o) => (
                  <option key={o}>{o}</option>
                ))}
              </select>
              {!String(t.op ?? "").includes("empty") && (
                <input className="code" aria-label="Value" placeholder="value, with {{ }}" value={t.value ?? ""} onChange={(e) => props.put("where", tests.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))} />
              )}
              <button type="button" className="quiet" title="Remove" onClick={() => props.put("where", tests.filter((_, j) => j !== i))}>
                ✕
              </button>
            </div>
          ))}
          <div className="row">
            <button type="button" className="quiet" onClick={() => props.put("where", [...tests, { column: "", op: "=", value: "" }])}>
              + Comparison
            </button>
            <label className="narrow">
              Rows where
              <select value={props.fields.match === "any" ? "any" : "all"} onChange={(e) => props.put("match", e.target.value === "any" ? "any" : undefined)}>
                <option value="all">every comparison holds</option>
                <option value="any">any one holds</option>
              </select>
            </label>
          </div>
        </>
      )}
    </>
  );
}

function TableBody(props: { fields: Fields; setBody: (v: unknown) => void }) {
  const { fields } = props;
  const [tables, setTables] = useState<TableSummary[]>([]);
  useEffect(() => {
    tablesApi
      .list()
      .then((l) => setTables(l.tables))
      .catch(() => undefined);
  }, []);
  const action = String(fields.action ?? "get");
  const put: Put = (name, value) => {
    const next: Fields = { ...fields, [name]: value };
    if (value === undefined) delete next[name];
    props.setBody(next);
  };
  const setAction = (a: string) => {
    const kept: Fields = { name: fields.name, action: a };
    for (const k of SETTINGS[a] ?? []) if (fields[k] !== undefined) kept[k] = fields[k];
    if (a === "update" && kept.where === undefined) kept.where = {};
    if (a === "delete" && kept.where === undefined) kept.where = {};
    if (a === "update" && kept.set === undefined) kept.set = {};
    if ((a === "insert" || a === "upsert") && kept.row === undefined && kept.rows === undefined) kept.row = {};
    if (a === "upsert" && kept.key === undefined) kept.key = "";
    props.setBody(kept);
  };
  const columns = tables.find((t) => t.name === fields.name)?.columns.map((c) => c.name) ?? [];
  const named = String(fields.name ?? "");
  return (
    <>
      <datalist id="table-columns">
        {columns.map((c) => (
          <option key={c} value={c} />
        ))}
      </datalist>
      <div className="row top">
        <label>
          Table
          <select value={named} onChange={(e) => put("name", e.target.value)}>
            <option value="">{tables.length ? "choose one" : "none yet: make one under Tables"}</option>
            {named && !tables.some((t) => t.name === named) && <option value={named}>{named}</option>}
            {tables.map((t) => (
              <option key={t.id} value={t.name}>
                {t.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Do
          <select value={action} onChange={(e) => setAction(e.target.value)}>
            {ACTIONS.map((a) => (
              <option key={a.action} value={a.action}>
                {a.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      {columns.length > 0 && <p className="hint">Its columns: {columns.join(", ")}.</p>}
      {["get", "count", "update", "delete"].includes(action) && <Which fields={fields} required={action === "update" || action === "delete"} columns={columns} put={put} />}
      {action === "get" && (
        <div className="row">
          <Text label="Sorted by" value={Array.isArray(fields.sort) ? (fields.sort as string[]).join(", ") : String(fields.sort ?? "")} onChange={(v) => put("sort", v ? v.split(",").map((s) => s.trim()).filter(Boolean) : undefined)} placeholder="-score, name (- for highest first)" />
          <label className="narrow">
            At most
            <input type="number" min={1} max={10000} value={String(fields.limit ?? "")} placeholder="100" onChange={(e) => put("limit", e.target.value === "" ? undefined : Number(e.target.value))} />
          </label>
          <label className="narrow">
            Skipping
            <input type="number" min={0} value={String(fields.offset ?? "")} placeholder="0" onChange={(e) => put("offset", e.target.value === "" ? undefined : Number(e.target.value))} />
          </label>
        </div>
      )}
      {action === "insert" && (
        <>
          <label className="check">
            <input type="checkbox" checked={fields.rows !== undefined} onChange={(e) => props.setBody({ name: fields.name, action, ...(e.target.checked ? { rows: "value.items" } : { row: {} }) })} />
            Each record of a list (not one row)
          </label>
          {fields.rows !== undefined ? (
            <Expr label="The list of records" value={String(fields.rows ?? "")} onChange={(v) => put("rows", v)} />
          ) : (
            <Pairs value={(fields.row ?? {}) as Record<string, string>} onChange={(v) => put("row", v)} keyLabel="Column" valueLabel="Value" />
          )}
        </>
      )}
      {action === "upsert" && (
        <>
          <Text label="Found by (its key column, or several, with commas)" value={Array.isArray(fields.key) ? (fields.key as string[]).join(", ") : String(fields.key ?? "")} onChange={(v) => put("key", v.includes(",") ? v.split(",").map((s) => s.trim()).filter(Boolean) : v.trim())} placeholder="email" />
          <p className="hint">The rows whose key columns hold these values are set to them; when none does, the row is added.</p>
          <Pairs value={(fields.row ?? {}) as Record<string, string>} onChange={(v) => put("row", v)} keyLabel="Column" valueLabel="Value" />
        </>
      )}
      {action === "update" && (
        <>
          <p className="hint">Set, in each of those rows - with row as the row as it is: {"{{ row.count + 1 }}"}</p>
          <Pairs value={(fields.set ?? {}) as Record<string, string>} onChange={(v) => put("set", v)} keyLabel="Column" valueLabel="Value" />
        </>
      )}
    </>
  );
}

const STATE_ACTIONS = [
  { action: "get", label: "Read what it keeps" },
  { action: "set", label: "Keep values" },
  { action: "delete", label: "Forget values" },
  { action: "clear", label: "Forget everything it keeps" },
];

function StateBody(props: { fields: Fields; setBody: (v: unknown) => void }) {
  const { fields } = props;
  const action = "set" in fields ? "set" : "delete" in fields ? "delete" : "clear" in fields ? "clear" : "get";
  const names = (v: unknown) => (Array.isArray(v) ? (v as string[]).join(", ") : String(v ?? ""));
  const list = (text: string) => {
    const all = text.split(",").map((s) => s.trim()).filter(Boolean);
    return all.length === 1 ? all[0] : all;
  };
  const choose = (a: string) => props.setBody(a === "get" ? {} : a === "set" ? { set: { last_seen: "{{ value.id }}" } } : a === "delete" ? { delete: "last_seen" } : { clear: true });
  return (
    <>
      <label>
        Do
        <select value={action} onChange={(e) => choose(e.target.value)}>
          {STATE_ACTIONS.map((a) => (
            <option key={a.action} value={a.action}>
              {a.label}
            </option>
          ))}
        </select>
      </label>
      {action === "get" && (
        <Text label="Which values (empty: all of them, as a record)" value={names(fields.get)} onChange={(v) => props.setBody(v.trim() ? { get: list(v) } : {})} placeholder="last_seen" />
      )}
      {action === "set" && (
        <>
          <p className="hint">Each value with {"{{ }}"}; state is what it keeps now: {"{{ has(state.runs) ? state.runs + 1 : 1 }}"}. Null forgets it.</p>
          <Pairs value={(fields.set ?? {}) as Record<string, string>} onChange={(v) => props.setBody({ set: v })} keyLabel="Name" valueLabel="Value" />
        </>
      )}
      {action === "delete" && <Text label="Which values, with commas" value={names(fields.delete)} onChange={(v) => props.setBody({ delete: list(v) })} />}
      <p className="hint">Each workflow keeps its own values between its runs: see them, and clear them, under Tables.</p>
    </>
  );
}
