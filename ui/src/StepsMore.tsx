// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The forms of the steps that shape a run and its data: switch, merge, list tools, group and summarise, respond to
// a webhook, run another workflow, date and time. Each writes the step as the engine reads it.

import { useEffect, useMemo, useState } from "react";
import { api, type WorkflowSummary } from "./api";
import { Expr, JsonField, Pairs, Text } from "./Steps";
import { Grow } from "./Grow";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const MORE_KINDS = ["switch", "merge", "list", "group", "respond", "run_workflow", "datetime"];

export function MoreBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  switch (props.kind) {
    case "switch":
      return <SwitchBody fields={fields} set={set} />;
    case "merge":
      return <MergeBody fields={fields} set={set} />;
    case "list":
      return <ListBody fields={fields} set={set} />;
    case "group":
      return <GroupBody fields={fields} setBody={props.setBody} />;
    case "respond":
      return <RespondBody fields={fields} set={set} />;
    case "run_workflow":
      return (
        <>
          <WorkflowPicker label="Workflow" value={String(fields.workflow ?? "")} onChange={(v) => set("workflow", v)} />
          <Expr label="Its input" value={String(fields.input ?? "")} onChange={(v) => set("input", v || undefined)} />
          <p className="hint">Empty: the record as it came in. Its result is steps.&lt;this step&gt;.result; each of its steps' under .steps.</p>
        </>
      );
    default:
      return <DateTimeBody fields={fields} set={set} />;
  }
}

type Case = { label?: string; when?: string; steps?: unknown[] };

function SwitchBody({ fields, set }: { fields: Fields; set: Put }) {
  const cases = (Array.isArray(fields.cases) ? fields.cases : []) as Case[];
  const write = (next: Case[]) => set("cases", next);
  return (
    <>
      <div className="rows">
        {cases.map((c, i) => (
          <div className="case-row" key={i}>
            <span className="case-n">{i + 1}</span>
            <input
              aria-label="Case name"
              placeholder="name"
              value={c.label ?? ""}
              onChange={(e) => write(cases.map((x, j) => (j === i ? { ...x, label: e.target.value || undefined } : x)))}
            />
            <input
              aria-label="Case condition"
              className="code"
              placeholder="when, as value.total > 500"
              value={c.when ?? ""}
              spellCheck={false}
              onChange={(e) => write(cases.map((x, j) => (j === i ? { ...x, when: e.target.value } : x)))}
            />
            <button className="icon-button remove" title="Remove the case" aria-label="Remove the case" disabled={cases.length < 2} onClick={() => write(cases.filter((_, j) => j !== i))}>
              ✕
            </button>
          </div>
        ))}
      </div>
      <button className="quiet add-row" onClick={() => write([...cases, { label: `case ${cases.length + 1}`, when: "true", steps: [] }])}>
        + Case
      </button>
      <p className="hint">The first case whose condition holds runs; when none does, the default. Each case's steps are on the canvas.</p>
    </>
  );
}

function MergeBody({ fields, set }: { fields: Fields; set: Put }) {
  const of = (Array.isArray(fields.of) ? fields.of : []) as string[];
  const mode = String(fields.mode ?? "append");
  return (
    <>
      <div className="rows">
        {of.map((expr, i) => (
          <div className="case-row" key={i}>
            <span className="case-n">{i + 1}</span>
            <input
              aria-label="What to merge"
              className="code"
              placeholder="as steps.orders.list"
              value={expr}
              spellCheck={false}
              onChange={(e) => set("of", of.map((x, j) => (j === i ? e.target.value : x)))}
            />
            <button className="icon-button remove" title="Remove" aria-label="Remove" disabled={of.length < 2} onClick={() => set("of", of.filter((_, j) => j !== i))}>
              ✕
            </button>
          </div>
        ))}
      </div>
      <button className="quiet add-row" onClick={() => set("of", [...of, ""])}>
        + Another
      </button>
      <div className="row">
        <label>
          How
          <select value={mode} onChange={(e) => set("mode", e.target.value === "append" ? undefined : e.target.value)}>
            <option value="append">one list after another</option>
            <option value="key">join records with the same key</option>
            <option value="fields">one record, later fields winning</option>
          </select>
        </label>
        {mode === "key" && <Text label="Key" value={String(fields.key ?? "")} onChange={(v) => set("key", v)} placeholder="id" />}
      </div>
    </>
  );
}

function ListBody({ fields, set }: { fields: Fields; set: Put }) {
  return (
    <>
      <Expr label="The list" value={String(fields.items ?? "")} onChange={(v) => set("items", v)} />
      <p className="hint">Each step below is optional and works on item (and its place, index), in this order.</p>
      <Opt label="Keep only" placeholder="item.total > 0" value={fields.filter} onChange={(v) => set("filter", v)} />
      <div className="row">
        <Opt label="Sort by" placeholder="item.total" value={fields.sort} onChange={(v) => set("sort", v)} />
        <label className="narrow">
          Order
          <select value={String(fields.order ?? "asc")} onChange={(e) => set("order", e.target.value === "asc" ? undefined : e.target.value)}>
            <option value="asc">smallest first</option>
            <option value="desc">greatest first</option>
          </select>
        </label>
      </div>
      <div className="row">
        <Opt label="Each once, by" placeholder="item.id" value={fields.unique} onChange={(v) => set("unique", v)} />
        <label className="narrow">
          At most
          <input
            type="number"
            min={1}
            value={typeof fields.limit === "number" ? fields.limit : ""}
            placeholder="all"
            onChange={(e) => set("limit", e.target.value ? Number(e.target.value) : undefined)}
          />
        </label>
      </div>
    </>
  );
}

/** An expression that may be left out. */
function Opt(props: { label: string; placeholder: string; value: unknown; onChange: (v: string | undefined) => void }) {
  return (
    <label>
      {props.label}
      <Grow
        className="code"
        oneLine
        data-expr=""
        placeholder={props.placeholder}
        value={typeof props.value === "string" ? props.value : ""}
        onChange={(v) => props.onChange(v || undefined)}
      />
    </label>
  );
}

const HOW = ["sum", "average", "min", "max"] as const;
const HOW_LABEL: Record<(typeof HOW)[number], string> = { sum: "sum of", average: "average of", min: "least", max: "greatest" };
type Row = { name: string; how: (typeof HOW)[number]; expr: string };

function GroupBody({ fields, setBody }: { fields: Fields; setBody: (v: unknown) => void }) {
  const rows: Row[] = HOW.flatMap((how) => Object.entries((fields[how] ?? {}) as Record<string, string>).map(([name, expr]) => ({ name, how, expr })));
  const write = (next: Row[]) => {
    const body: Fields = { items: fields.items, by: fields.by };
    for (const how of HOW) {
      const mine = next.filter((r) => r.how === how);
      if (mine.length) body[how] = Object.fromEntries(mine.map((r) => [r.name, r.expr]));
    }
    setBody(body);
  };
  return (
    <>
      <div className="row">
        <Text label="The list" value={String(fields.items ?? "")} onChange={(v) => setBody({ ...fields, items: v })} placeholder="value.orders" />
        <Text label="Grouped by" value={String(fields.by ?? "")} onChange={(v) => setBody({ ...fields, by: v })} placeholder="item.customer" />
      </div>
      <p className="hint">Each group gives its key and count; and these, each worked out over its items:</p>
      <div className="rows">
        {rows.map((r, i) => (
          <div className="agg-row" key={i}>
            <input aria-label="Field" placeholder="field" value={r.name} onChange={(e) => write(rows.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))} />
            <select aria-label="Worked out as" value={r.how} onChange={(e) => write(rows.map((x, j) => (j === i ? { ...x, how: e.target.value as Row["how"] } : x)))}>
              {HOW.map((h) => (
                <option key={h} value={h}>
                  {HOW_LABEL[h]}
                </option>
              ))}
            </select>
            <input
              aria-label="Of"
              className="code"
              placeholder="item.total"
              value={r.expr}
              spellCheck={false}
              onChange={(e) => write(rows.map((x, j) => (j === i ? { ...x, expr: e.target.value } : x)))}
            />
            <button className="icon-button remove" title="Remove" aria-label="Remove" onClick={() => write(rows.filter((_, j) => j !== i))}>
              ✕
            </button>
          </div>
        ))}
      </div>
      <button className="quiet add-row" onClick={() => write([...rows, { name: `field${rows.length + 1}`, how: "sum", expr: "item.total" }])}>
        + Field
      </button>
    </>
  );
}

function RespondBody({ fields, set }: { fields: Fields; set: Put }) {
  return (
    <>
      <div className="row">
        <label className="narrow">
          Status
          <input type="number" min={100} max={599} value={typeof fields.status === "number" ? fields.status : 200} onChange={(e) => set("status", Number(e.target.value))} />
        </label>
        <p className="hint grow">The webhook's caller waits for this answer (up to 30 seconds). Without a respond step, it gets 202 at once.</p>
      </div>
      <p className="hint">Headers</p>
      <Pairs value={(fields.headers ?? {}) as Record<string, string>} onChange={(v) => set("headers", Object.keys(v).length ? v : undefined)} keyLabel="Header" valueLabel="Text" />
      <JsonField label="Body" value={fields.body ?? { ok: true }} onChange={(v) => set("body", v)} />
    </>
  );
}

const FORMATS = ["%Y-%m-%d", "%Y-%m-%d %H:%M", "%d %b %Y", "%A, %d %B %Y", "%H:%M", "%d/%m/%Y", "%m/%d/%Y"];

function zones(): string[] {
  try {
    return (Intl as unknown as { supportedValuesOf: (k: string) => string[] }).supportedValuesOf("timeZone");
  } catch {
    return ["UTC", "Europe/London", "America/New_York", "Asia/Kolkata"];
  }
}

function DateTimeBody({ fields, set }: { fields: Fields; set: Put }) {
  const all = useMemo(zones, []);
  return (
    <>
      <Opt label="The time (empty: now)" placeholder="value.created_at" value={fields.value} onChange={(v) => set("value", v)} />
      <div className="row">
        <Opt label="Add" placeholder="1d, 2h, 30m" value={fields.add} onChange={(v) => set("add", v)} />
        <Opt label="Subtract" placeholder="7d" value={fields.subtract} onChange={(v) => set("subtract", v)} />
      </div>
      <div className="row">
        <label>
          Time zone
          <input className="code" list="openduct-zones" placeholder="UTC" value={String(fields.timezone ?? "")} onChange={(e) => set("timezone", e.target.value || undefined)} />
          <datalist id="openduct-zones">
            {all.map((z) => (
              <option key={z} value={z} />
            ))}
          </datalist>
        </label>
        <label>
          Written as
          <input className="code" list="openduct-formats" placeholder="%Y-%m-%d %H:%M" value={String(fields.format ?? "")} onChange={(e) => set("format", e.target.value || undefined)} />
          <datalist id="openduct-formats">
            {FORMATS.map((f) => (
              <option key={f} value={f} />
            ))}
          </datalist>
        </label>
      </div>
      <p className="hint">It gives iso, utc, unix, date, time, weekday and, with a form, formatted.</p>
    </>
  );
}

/** Another workflow of this workspace, by name. */
export function WorkflowPicker(props: { label: string; value: string; onChange: (id: string) => void; except?: string }) {
  const [all, setAll] = useState<WorkflowSummary[] | null>(null);
  useEffect(() => {
    api
      .workflows()
      .then(setAll)
      .catch(() => setAll([]));
  }, []);
  const others = (all ?? []).filter((w) => w.id !== props.except);
  return (
    <label>
      {props.label}
      <select value={props.value} onChange={(e) => props.onChange(e.target.value)}>
        <option value="">{all === null ? "…" : others.length ? "Choose a workflow" : "No other workflow yet"}</option>
        {others.map((w) => (
          <option key={w.id} value={w.id}>
            {w.name}
          </option>
        ))}
        {props.value && !others.some((w) => w.id === props.value) && <option value={props.value}>{props.value}</option>}
      </select>
    </label>
  );
}
