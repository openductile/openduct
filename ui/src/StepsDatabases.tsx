// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The SQL steps' forms - MySQL (and MariaDB), SQL Server, and the settings a PostgreSQL step has beside its SQL -
// and the trigger a table's new or changed rows start. Each writes the step as the engine reads it.

import type { Step } from "./api";
import { Area, CredentialPicker, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const DATABASE_KINDS = ["mysql", "sqlserver"];
/** The triggers of a table's rows, by database. */
export const DATABASE_TRIGGERS = ["mysql", "sqlserver", "postgres"];

const NAMES: Record<string, string> = { mysql: "MySQL or MariaDB", sqlserver: "SQL Server", postgres: "PostgreSQL" };

/** Where the `n`th value goes in a database's SQL. */
function place(kind: string, n: number): string {
  if (kind === "mysql") return "?";
  if (kind === "sqlserver") return `@p${n + 1}`;
  return `$${n + 1}`;
}

/** A new step of `kind`. */
export function freshDatabase(kind: string, name: string): Step {
  return { name, [kind]: { credential: "", sql: `SELECT * FROM orders WHERE id = ${place(kind, 0)}`, params: ["value.id"] } };
}

/** A new trigger: a table's new rows, by its id. */
export function newDatabaseTrigger(kind = "mysql"): Record<string, unknown> {
  return { [kind]: { credential: "", table: "orders", cursor: "id", every: "1m" } };
}

function Values(props: { kind: string; params: unknown; onChange: (v: string[] | undefined) => void }) {
  const params = Array.isArray(props.params) ? (props.params as unknown[]).map(String) : [];
  const change = (next: string[]) => props.onChange(next.length ? next : undefined);
  return (
    <>
      <p className="hint">Values, each an expression: {props.kind === "mysql" ? "one for each ?, in order" : `the first for ${place(props.kind, 0)}, the next for ${place(props.kind, 1)} ...`}</p>
      {params.map((p, i) => (
        <div className="row" key={i}>
          <label className="wide">
            {props.kind === "mysql" ? `? ${i + 1}` : place(props.kind, i)}
            <input className="code" data-expr value={p} onChange={(e) => change(params.map((q, j) => (j === i ? e.target.value : q)))} />
          </label>
          <button type="button" className="quiet" onClick={() => change(params.filter((_, j) => j !== i))} aria-label={`Remove value ${i + 1}`}>
            Remove
          </button>
        </div>
      ))}
      <button type="button" className="quiet" onClick={() => change([...params, "value."])}>
        Add a value
      </button>
    </>
  );
}

/** The settings every SQL step has beside its SQL: read only, how many rows, how long. */
export function DatabaseLimits({ fields, set }: { fields: Fields; set: Put }) {
  return (
    <>
      <label className="check">
        <input type="checkbox" checked={fields.read_only === true} onChange={(e) => set("read_only", e.target.checked || undefined)} />
        Only reads: SELECT alone, run where the database keeps it from writing (for an agent's tool)
      </label>
      <div className="row">
        <Text label="Most rows" value={fields.max_rows === undefined ? "" : String(fields.max_rows)} placeholder="1000" onChange={(v) => set("max_rows", v.trim() ? Number(v) : undefined)} />
        <Text label="Time" value={String(fields.timeout ?? "")} placeholder="30s" onChange={(v) => set("timeout", v.trim() || undefined)} />
      </div>
    </>
  );
}

export function DatabaseBody({ kind, fields, set, setBody }: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const statements = Array.isArray(fields.statements) ? (fields.statements as Fields[]) : null;
  const putStatement = (i: number, name: string, value: unknown) => {
    const next = (statements ?? []).map((s, j) => (j === i ? { ...s, [name]: value } : s));
    for (const s of next) if (s.params === undefined) delete s.params;
    set("statements", next);
  };
  const several = () => {
    const { sql, params, ...rest } = fields;
    setBody({ ...rest, transaction: true, statements: [{ sql: sql ?? "", ...(params ? { params } : {}) }, { sql: "" }] });
  };
  const one = () => {
    const { statements: list, transaction: _, ...rest } = fields;
    const first = (Array.isArray(list) ? (list as Fields[])[0] : undefined) ?? {};
    setBody({ ...rest, sql: first.sql ?? "", ...(first.params ? { params: first.params } : {}) });
  };
  return (
    <>
      <CredentialPicker kind={kind} value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
      {statements === null ? (
        <>
          <Area label="SQL" hint={`values go in as ${place(kind, 0)} - never written into the SQL`} value={String(fields.sql ?? "")} onChange={(v) => set("sql", v)} />
          <Values kind={kind} params={fields.params} onChange={(v) => set("params", v)} />
          <button type="button" className="quiet" onClick={several}>
            Run several statements
          </button>
        </>
      ) : (
        <>
          {statements.map((s, i) => (
            <fieldset key={i} className="statement">
              <legend>Statement {i + 1}</legend>
              <Area label="SQL" value={String(s.sql ?? "")} onChange={(v) => putStatement(i, "sql", v)} />
              <Values kind={kind} params={s.params} onChange={(v) => putStatement(i, "params", v)} />
              {statements.length > 1 && (
                <button type="button" className="quiet" onClick={() => set("statements", statements.filter((_, j) => j !== i))}>
                  Remove the statement
                </button>
              )}
            </fieldset>
          ))}
          <button type="button" className="quiet" onClick={() => set("statements", [...statements, { sql: "" }])}>
            Add a statement
          </button>
          <label className="check">
            <input type="checkbox" checked={fields.transaction === true} onChange={(e) => set("transaction", e.target.checked || undefined)} />
            In one transaction: all of them, or (one failing) none
          </label>
          <button type="button" className="quiet" onClick={one}>
            One statement
          </button>
        </>
      )}
      <DatabaseLimits fields={fields} set={set} />
    </>
  );
}

const EVERY = ["30s", "1m", "5m", "15m", "1h"];

/** A trigger of a table's new or changed rows. */
export function DatabaseTrigger(props: { kind: string; settings: Fields; onChange: (trigger: unknown) => void }) {
  const kind = DATABASE_TRIGGERS.includes(props.kind) ? props.kind : "mysql";
  const s = props.settings;
  const put = (name: string, value: unknown) => {
    const next: Fields = { ...s, [name]: value };
    if (value === "" || value === undefined) delete next[name];
    props.onChange({ [kind]: next });
  };
  return (
    <div className="tg-section">
      <label className="tg-field">
        <span className="tg-name">Database</span>
        <select value={kind} onChange={(e) => props.onChange({ [e.target.value]: { ...s, credential: "" } })}>
          {DATABASE_TRIGGERS.map((k) => (
            <option key={k} value={k}>
              {NAMES[k]}
            </option>
          ))}
        </select>
      </label>
      <CredentialPicker kind={kind} value={String(s.credential ?? "")} onChange={(v) => put("credential", v)} />
      <div className="row">
        <Text label="Table" value={String(s.table ?? "")} placeholder="orders, or schema.orders" onChange={(v) => put("table", v.trim())} />
        <Text label="Column rows are taken after" value={String(s.cursor ?? "")} placeholder="id, or updated_at" onChange={(v) => put("cursor", v.trim())} />
        <Text label="Key (for a time column)" value={String(s.key ?? "")} placeholder="id" onChange={(v) => put("key", v.trim())} />
      </div>
      <Text label="Only rows where (SQL)" value={String(s.where ?? "")} placeholder="status = 'paid'" onChange={(v) => put("where", v)} wide />
      <div className="row">
        <label>
          Ask every
          <select value={String(s.every ?? "1m")} onChange={(e) => put("every", e.target.value)}>
            {EVERY.map((e) => (
              <option key={e} value={e}>
                {e}
              </option>
            ))}
          </select>
        </label>
        <Text label="Most rows each time" value={s.max_rows === undefined ? "" : String(s.max_rows)} placeholder="50" onChange={(v) => put("max_rows", v.trim() ? Number(v) : undefined)} />
      </div>
      <p className="tg-note">
        Each row after the last one seen runs once, oldest first: an id that grows takes new rows; a time a row is changed at takes changed rows too (name the rows&apos; key
        beside it). The first time, it notes where the table is and takes nothing.
      </p>
    </div>
  );
}
