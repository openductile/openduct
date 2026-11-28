// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Forms in the studio: a form's fields as a list to build (each its name, label, kind, whether it must be filled in,
// help, and its kind's own settings), a preview of the page as people see it, the form trigger's settings, the form
// and approval steps', a wait's (a while, a time, a call to its address), and the runs that wait now.

import { useEffect, useState } from "react";
import { api, type WaitingRun } from "./api";
import "./forms.css";

export type FormField = {
  name: string;
  label?: string;
  type?: string;
  required?: boolean;
  help?: string;
  placeholder?: string;
  options?: string[] | string;
  min?: number | string;
  max?: number | string;
  max_length?: number;
  max_size?: string | number;
  accept?: string;
  default?: string;
};

export const FIELD_TYPES: [string, string][] = [
  ["text", "Text"],
  ["long_text", "Long text"],
  ["number", "Number"],
  ["email", "E-mail"],
  ["date", "Date"],
  ["dropdown", "Dropdown"],
  ["checkbox", "Checkbox"],
  ["file", "File upload"],
];

type Fields = Record<string, unknown>;

/** A field name as the engine takes it: letters, digits and `_`, starting with a letter. */
function asName(text: string): string {
  const cleaned = text.replace(/[^A-Za-z0-9_]/g, "_").slice(0, 64);
  return /^[A-Za-z]/.test(cleaned) ? cleaned : `f_${cleaned}`.slice(0, 64);
}

/** Choices as written in one box: `S, M, L` - or a `{{ }}` that works them out from the run. */
function optionsText(options: FormField["options"]): string {
  return Array.isArray(options) ? options.join(", ") : options ?? "";
}

function optionsOf(text: string): string[] | string | undefined {
  if (text.includes("{{")) return text;
  const list = text
    .split(",")
    .map((o) => o.trim())
    .filter(Boolean);
  return list.length ? list : undefined;
}

/** A field with its empty settings left out. */
function tidy(field: FormField): FormField {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(field)) {
    if (v === undefined || v === "" || v === false) continue;
    out[k] = v;
  }
  return out as FormField;
}

export function FormFields(props: { fields: FormField[]; onChange: (f: FormField[]) => void; stepForm?: boolean }) {
  const { fields } = props;
  const write = (next: FormField[]) => props.onChange(next.map(tidy));
  const put = (i: number, patch: Partial<FormField>) => write(fields.map((f, j) => (j === i ? { ...f, ...patch } : f)));
  const move = (i: number, by: number) => {
    const j = i + by;
    if (j < 0 || j >= fields.length) return;
    const next = [...fields];
    [next[i], next[j]] = [next[j]!, next[i]!];
    write(next);
  };
  const add = () => {
    let n = fields.length + 1;
    while (fields.some((f) => f.name === `field_${n}`)) n++;
    write([...fields, { name: `field_${n}`, label: `Field ${n}`, type: "text" }]);
  };
  return (
    <div className="form-fields">
      {fields.length === 0 && <p className="hint">No fields yet.</p>}
      {fields.map((f, i) => {
        const type = f.type ?? "text";
        return (
          <div className="form-field" key={i}>
            <div className="ff-row">
              <input aria-label="Field label" placeholder="Label" value={f.label ?? ""} onChange={(e) => put(i, { label: e.target.value })} />
              <input
                aria-label="Field name"
                className="code"
                placeholder="name"
                value={f.name}
                spellCheck={false}
                onChange={(e) => put(i, { name: asName(e.target.value) })}
              />
              <select aria-label="Field type" value={type} onChange={(e) => put(i, { type: e.target.value })}>
                {FIELD_TYPES.map(([v, l]) => (
                  <option key={v} value={v}>
                    {l}
                  </option>
                ))}
              </select>
              <label className="check">
                <input type="checkbox" aria-label="Required" checked={!!f.required} onChange={(e) => put(i, { required: e.target.checked })} />
                Required
              </label>
              <span className="tools">
                <button type="button" className="icon-button" aria-label="Move the field up" title="Move up" onClick={() => move(i, -1)}>
                  ↑
                </button>
                <button type="button" className="icon-button" aria-label="Move the field down" title="Move down" onClick={() => move(i, 1)}>
                  ↓
                </button>
                <button type="button" className="icon-button remove" aria-label="Remove the field" title="Remove" onClick={() => write(fields.filter((_, j) => j !== i))}>
                  ✕
                </button>
              </span>
            </div>
            <div className="ff-row ff-more">
              <input aria-label="Help text" placeholder="Help text (optional)" value={f.help ?? ""} onChange={(e) => put(i, { help: e.target.value })} />
              {(type === "dropdown" || type === "checkbox") && (
                <input
                  aria-label="Choices"
                  placeholder={type === "dropdown" ? "Choices: S, M, L" : "Choices (none: one box, yes or no)"}
                  value={optionsText(f.options)}
                  onChange={(e) => put(i, { options: optionsOf(e.target.value) })}
                />
              )}
              {(type === "number" || type === "date") && (
                <>
                  <input aria-label="Least" placeholder={type === "date" ? "Earliest, 2026-01-31" : "Least"} value={String(f.min ?? "")} onChange={(e) => put(i, { min: e.target.value })} />
                  <input aria-label="Greatest" placeholder={type === "date" ? "Latest" : "Greatest"} value={String(f.max ?? "")} onChange={(e) => put(i, { max: e.target.value })} />
                </>
              )}
              {type === "file" && (
                <>
                  <input aria-label="Largest file" placeholder="Largest: 1MB (at most 2MB)" value={String(f.max_size ?? "")} onChange={(e) => put(i, { max_size: e.target.value })} />
                  <input aria-label="File kinds" placeholder="Kinds: .pdf,image/*" value={f.accept ?? ""} onChange={(e) => put(i, { accept: e.target.value })} />
                </>
              )}
              {(type === "text" || type === "long_text" || type === "email") && (
                <input aria-label="Placeholder" placeholder="Placeholder (optional)" value={f.placeholder ?? ""} onChange={(e) => put(i, { placeholder: e.target.value })} />
              )}
              {type !== "file" && (
                <input
                  aria-label="Filled in with"
                  placeholder={props.stepForm ? "Filled in with: {{ value.email }}" : "Filled in with (optional)"}
                  value={f.default ?? ""}
                  onChange={(e) => put(i, { default: e.target.value })}
                />
              )}
            </div>
          </div>
        );
      })}
      <button type="button" className="quiet add-row" onClick={add}>
        + Field
      </button>
    </div>
  );
}

/** The page as people see it - its controls not live. */
export function FormPreview(props: {
  title?: string;
  description?: string;
  fields: FormField[];
  button?: string;
  approval?: { approve?: string; reject?: string; comment?: boolean };
}) {
  return (
    <div className="form-preview" aria-label="Preview of the form">
      <h3>{props.title || "Untitled form"}</h3>
      {props.description && <p className="hint">{props.description}</p>}
      {props.fields.map((f, i) => {
        const type = f.type ?? "text";
        const label = (
          <span className="fp-label">
            {f.label || f.name}
            {f.required && <span className="fp-req"> *</span>}
          </span>
        );
        const options = Array.isArray(f.options) ? f.options : [];
        let control;
        if (type === "long_text") control = <textarea disabled placeholder={f.placeholder} defaultValue={f.default} />;
        else if (type === "dropdown")
          control = (
            <select disabled>
              <option>Choose</option>
              {options.map((o) => (
                <option key={o}>{o}</option>
              ))}
            </select>
          );
        else if (type === "checkbox" && options.length)
          control = options.map((o) => (
            <label className="check" key={o}>
              <input type="checkbox" disabled /> {o}
            </label>
          ));
        else if (type === "checkbox")
          return (
            <div className="fp-field" key={i}>
              <label className="check">
                <input type="checkbox" disabled /> {label}
              </label>
            </div>
          );
        else control = <input disabled type={type === "file" ? "file" : type === "email" ? "email" : type === "date" ? "date" : type === "number" ? "number" : "text"} placeholder={f.placeholder} />;
        return (
          <div className="fp-field" key={i}>
            {label}
            {control}
            {f.help && <span className="hint">{f.help}</span>}
          </div>
        );
      })}
      {props.approval?.comment && (
        <div className="fp-field">
          <span className="fp-label">Comment (optional)</span>
          <textarea disabled />
        </div>
      )}
      <div className="fp-actions">
        {props.approval ? (
          <>
            <button type="button" disabled className="fp-approve">
              {props.approval.approve || "Approve"}
            </button>
            <button type="button" disabled className="fp-reject">
              {props.approval.reject || "Reject"}
            </button>
          </>
        ) : (
          <button type="button" disabled>
            {props.button || "Send"}
          </button>
        )}
      </div>
    </div>
  );
}

function fieldsOf(settings: Fields): FormField[] {
  return Array.isArray(settings.fields) ? (settings.fields as FormField[]) : [];
}

function Line(props: { label: string; value: unknown; onChange: (v: string | undefined) => void; placeholder?: string; long?: boolean }) {
  const value = typeof props.value === "string" || typeof props.value === "number" ? String(props.value) : "";
  return (
    <label className="ff-line">
      <span>{props.label}</span>
      {props.long ? (
        <textarea value={value} placeholder={props.placeholder} onChange={(e) => props.onChange(e.target.value || undefined)} />
      ) : (
        <input value={value} placeholder={props.placeholder} onChange={(e) => props.onChange(e.target.value || undefined)} />
      )}
    </label>
  );
}

/** What the page does once it has its answer: thank, or send people on. */
function AfterSending(props: { settings: Fields; set: (k: string, v: unknown) => void }) {
  return (
    <div className="ff-pair">
      <Line label="Thank-you text" value={props.settings.thanks} placeholder="Thank you: your answer is in." long onChange={(v) => props.set("thanks", v)} />
      <Line label="Or go on to" value={props.settings.redirect} placeholder="https://example.com/thanks" onChange={(v) => props.set("redirect", v)} />
    </div>
  );
}

/** A form trigger's settings: its page's texts, fields and protection, its address once saved, and a preview. */
export function FormTriggerSettings(props: { settings: Fields; address?: string | null; onChange: (s: Fields) => void }) {
  const s = props.settings;
  const set = (k: string, v: unknown) => {
    const next = { ...s, [k]: v };
    if (v === undefined || v === "") delete next[k];
    props.onChange(next);
  };
  const address = props.address ? `${location.origin}${props.address}` : "";
  return (
    <div className="tg-section form-builder">
      {address ? (
        <p className="tg-note">
          The form is at <a href={address} target="_blank" rel="noreferrer">{address}</a> once the workflow is on. Each answer starts a run: <code>value.&lt;field&gt;</code>.
        </p>
      ) : (
        <p className="tg-note">Save the workflow to get its form's address.</p>
      )}
      <Line label="Title" value={s.title} placeholder="Contact us" onChange={(v) => set("title", v)} />
      <Line label="Description" value={s.description} long onChange={(v) => set("description", v)} />
      <FormFields fields={fieldsOf(s)} onChange={(f) => set("fields", f)} />
      <div className="ff-pair">
        <Line label="Button" value={s.button} placeholder="Send" onChange={(v) => set("button", v)} />
        <Line label="Password (optional)" value={s.password} placeholder="none: anyone with the address" onChange={(v) => set("password", v)} />
        <Line
          label="Answers a minute from one address"
          value={s.rate_limit}
          placeholder="10 (0: no limit)"
          onChange={(v) => set("rate_limit", v === undefined ? undefined : Number(v))}
        />
      </div>
      <AfterSending settings={s} set={set} />
      <FormPreview title={String(s.title ?? "")} description={String(s.description ?? "")} fields={fieldsOf(s)} button={String(s.button ?? "")} />
    </div>
  );
}

/** How long a step waits for its answer, and what then. */
function Timeout(props: { fields: Fields; set: (k: string, v: unknown) => void }) {
  return (
    <div className="ff-pair">
      <Line label="Wait at most" value={props.fields.timeout} placeholder="7d (at most 90d)" onChange={(v) => props.set("timeout", v)} />
      <label className="ff-line">
        <span>If no answer comes</span>
        <select value={props.fields.on_timeout === "continue" ? "continue" : "fail"} onChange={(e) => props.set("on_timeout", e.target.value === "fail" ? undefined : e.target.value)}>
          <option value="fail">the step fails</option>
          <option value="continue">go on: timed_out is true</option>
        </select>
      </label>
    </div>
  );
}

/** A form step's or an approval's settings. */
export function FormStepBody(props: { kind: "form" | "approval"; fields: Fields; setBody: (v: unknown) => void }) {
  const f = props.fields;
  const set = (k: string, v: unknown) => {
    const next = { ...f, [k]: v };
    if (v === undefined || v === "" || v === false) delete next[k];
    props.setBody(next);
  };
  const approval = props.kind === "approval";
  return (
    <div className="form-builder">
      <Line label="Title" value={f.title} placeholder={approval ? "Refund {{ value.amount }}?" : "A few details"} onChange={(v) => set("title", v)} />
      <Line label="Description" value={f.description} long onChange={(v) => set("description", v)} />
      {approval && (
        <div className="ff-pair">
          <Line label="Approve button" value={f.approve} placeholder="Approve" onChange={(v) => set("approve", v)} />
          <Line label="Reject button" value={f.reject} placeholder="Reject" onChange={(v) => set("reject", v)} />
          <label className="check">
            <input type="checkbox" checked={!!f.comment} onChange={(e) => set("comment", e.target.checked)} /> Ask for a comment
          </label>
        </div>
      )}
      <FormFields fields={fieldsOf(f)} onChange={(x) => set("fields", x.length ? x : undefined)} stepForm />
      <AfterSending settings={f} set={set} />
      <Timeout fields={f} set={set} />
      <p className="hint">
        The run waits here. Its form's one-time address is <code>{"{{ links.<this step> }}"}</code>: send it in a step before this one (an e-mail, a
        message). Its answers are <code>steps.&lt;this step&gt;.&lt;field&gt;</code>
        {approval ? (
          <>
            , and <code>.approved</code>, <code>.decision</code>, <code>.comment</code>
          </>
        ) : null}
        ; <code>.timed_out</code> says no answer came. A test run or a preview does not wait: it takes a sample answer.
      </p>
      <FormPreview
        title={String(f.title ?? "")}
        description={String(f.description ?? "")}
        fields={fieldsOf(f)}
        approval={approval ? { approve: f.approve as string, reject: f.reject as string, comment: !!f.comment } : undefined}
      />
    </div>
  );
}

type WaitMode = "while" | "until" | "webhook";

function modeOf(body: unknown): WaitMode {
  if (body && typeof body === "object") {
    if ("until" in body) return "until";
    if ("webhook" in body) return "webhook";
  }
  return "while";
}

/** A wait's settings: a while (up to an hour within the run, longer kept on the log), until a time, or for a call. */
export function WaitBody(props: { body: unknown; setBody: (v: unknown) => void }) {
  const mode = modeOf(props.body);
  const f = (props.body && typeof props.body === "object" ? props.body : {}) as Fields;
  const set = (k: string, v: unknown) => {
    const next = { ...f, [k]: v };
    if (v === undefined || v === "") delete next[k];
    props.setBody(next);
  };
  const choose = (m: WaitMode) => {
    if (m === mode) return;
    props.setBody(m === "while" ? "10s" : m === "until" ? { until: "value.at" } : { webhook: true, timeout: "1d" });
  };
  const forText = typeof props.body === "string" ? props.body : String(f.for ?? "");
  return (
    <div className="form-builder">
      <div className="tg-segments" role="radiogroup" aria-label="Wait for">
        {(
          [
            ["while", "A while"],
            ["until", "Until a time"],
            ["webhook", "A call to its address"],
          ] as [WaitMode, string][]
        ).map(([m, label]) => (
          <button key={m} type="button" role="radio" aria-checked={m === mode} className={m === mode ? "here" : ""} onClick={() => choose(m)}>
            {label}
          </button>
        ))}
      </div>
      {mode === "while" && (
        <>
          <Line label="Wait for" value={forText} placeholder="10s, 5m, 2d" onChange={(v) => props.setBody(v ?? "")} />
          <p className="hint">Up to an hour, the run is held; longer (up to 90d), it is kept on the log and goes on after a restart too.</p>
        </>
      )}
      {mode === "until" && (
        <>
          <label className="ff-line">
            <span>Until (an expression)</span>
            <input className="code" spellCheck={false} value={String(f.until ?? "")} placeholder="value.deliver_at" onChange={(e) => props.setBody({ until: e.target.value })} />
          </label>
          <p className="hint">A time as 2026-12-01T09:00:00Z, a day as 2026-12-01, or milliseconds since 1970. A time already past goes on at once.</p>
        </>
      )}
      {mode === "webhook" && (
        <>
          <Timeout fields={f} set={set} />
          <p className="hint">
            The run waits for a call to its one-time address, <code>{"{{ links.<this step> }}"}</code>: hand it over in a step before this one (an HTTP call's
            callback). The call's method, body, query and headers are <code>steps.&lt;this step&gt;</code>. The address takes one call.
          </p>
        </>
      )}
    </div>
  );
}

function when(ms: number): string {
  return new Date(ms).toLocaleString();
}

/** The runs of a workflow that wait now: what for, until when, and where to answer. */
export function WaitingRuns(props: { id: string }) {
  const [waiting, setWaiting] = useState<WaitingRun[]>([]);
  useEffect(() => {
    let live = true;
    const load = () =>
      api
        .waiting(props.id)
        .then((w) => live && setWaiting(w))
        .catch(() => undefined);
    load();
    const every = setInterval(load, 3000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, [props.id]);
  if (waiting.length === 0) return null;
  return (
    <section className="waiting-runs" aria-label="Runs waiting now">
      <h3>Waiting now ({waiting.length})</h3>
      <table className="runs">
        <thead>
          <tr>
            <th>Since</th>
            <th>At</th>
            <th>For</th>
            <th>Until</th>
            <th>Address</th>
          </tr>
        </thead>
        <tbody>
          {waiting.map((w) => (
            <tr key={`${w.run}-${w.step}`}>
              <td>{when(w.since)}</td>
              <td>
                <span className="status waiting">waiting</span> {w.step}
              </td>
              <td>{w.title || w.kind}</td>
              <td>
                {when(w.until)}
                {w.kind !== "time" && <span className="hint"> then {w.on_timeout === "continue" ? "goes on" : "fails"}</span>}
              </td>
              <td>
                {w.answered ? (
                  <span className="hint">answered: going on</span>
                ) : w.url && w.kind !== "webhook" ? (
                  <a href={w.url} target="_blank" rel="noreferrer">
                    Open the form
                  </a>
                ) : w.url ? (
                  <code title={w.url}>{w.url}</code>
                ) : (
                  <span className="hint">goes on by itself</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
