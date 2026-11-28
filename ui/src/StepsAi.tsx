// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The forms of the AI steps built on "Ask AI": classify, sentiment, extract and summarise, and the JSON schema an
// "Ask AI" answer can be held to. Each writes the step as the engine reads it; a category's (or a label's) steps
// are its branch on the canvas.

import type { Step } from "./api";
import { Area, CredentialPicker, JsonField, ModelPicker, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The AI steps with a form here. */
export const AI_STEP_KINDS = ["classify", "sentiment", "extract", "summarise"];

/** A new step of one of them. */
export function freshAi(kind: string, name: string): Step {
  const model = { credential: "", model: "", text: "{{ value.text }}" };
  switch (kind) {
    case "classify":
      return {
        name,
        classify: {
          ...model,
          categories: [
            { name: "question", description: "Asks how to do something", steps: [] },
            { name: "problem", description: "Something does not work", steps: [] },
          ],
          default: [],
        },
      };
    case "sentiment":
      return { name, sentiment: { ...model, labels: ["positive", "neutral", "negative"].map((l) => ({ name: l, steps: [] })) } };
    case "extract":
      return {
        name,
        extract: {
          ...model,
          fields: [
            { name: "name", type: "string", description: "the person's name" },
            { name: "email", type: "string", description: "their e-mail address" },
          ],
        },
      };
    default:
      return { name, summarise: { ...model, length: "short" } };
  }
}

type Choice = string | { name?: string; description?: string; steps?: unknown[] };
const choiceOf = (c: Choice) => (typeof c === "string" ? { name: c } : c);

/** Categories or labels: a name and what it means each; their steps are on the canvas. */
function Choices(props: { label: string; value: Choice[]; onChange: (v: Choice[]) => void; least: number }) {
  const rows = props.value.map(choiceOf);
  const write = (next: typeof rows) => props.onChange(next);
  return (
    <>
      <div className="rows">
        {rows.map((c, i) => (
          <div className="case-row" key={i}>
            <span className="case-n">{i + 1}</span>
            <input
              aria-label={`${props.label} name`}
              placeholder="name"
              value={c.name ?? ""}
              onChange={(e) => write(rows.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))}
            />
            <input
              aria-label={`${props.label} description`}
              placeholder="what it means (optional)"
              value={c.description ?? ""}
              onChange={(e) => write(rows.map((x, j) => (j === i ? { ...x, description: e.target.value || undefined } : x)))}
            />
            <button
              className="icon-button remove"
              title={`Remove the ${props.label.toLowerCase()}`}
              aria-label={`Remove the ${props.label.toLowerCase()}`}
              disabled={rows.length <= props.least}
              onClick={() => write(rows.filter((_, j) => j !== i))}
            >
              ✕
            </button>
          </div>
        ))}
      </div>
      <button className="quiet add-row" onClick={() => write([...rows, { name: `${props.label.toLowerCase()} ${rows.length + 1}`, steps: [] }])}>
        + {props.label}
      </button>
    </>
  );
}

const TYPES = ["string", "number", "integer", "boolean", "date", "list"];
type Field = string | { name?: string; type?: string; description?: string; required?: boolean };

/** The fields an extract pulls out: a name, a type, what it is, and whether it must be there. */
function FieldRows(props: { value: Field[]; onChange: (v: Field[]) => void }) {
  const rows = props.value.map((f) => (typeof f === "string" ? { name: f } : f));
  const write = (next: typeof rows) => props.onChange(next);
  const change = (i: number, part: object) => write(rows.map((x, j) => (j === i ? { ...x, ...part } : x)));
  return (
    <>
      <div className="rows">
        {rows.map((f, i) => (
          <div className="case-row" key={i}>
            <input aria-label="Field name" placeholder="name" value={f.name ?? ""} onChange={(e) => change(i, { name: e.target.value.replace(/[^A-Za-z0-9_]/g, "_") })} />
            <select aria-label="Field type" value={f.type ?? "string"} onChange={(e) => change(i, { type: e.target.value })}>
              {TYPES.map((t) => (
                <option key={t}>{t}</option>
              ))}
            </select>
            <input aria-label="Field description" placeholder="what it is (optional)" value={f.description ?? ""} onChange={(e) => change(i, { description: e.target.value || undefined })} />
            <label className="check" title="The step fails when the text does not give it">
              <input type="checkbox" checked={f.required === true} onChange={(e) => change(i, { required: e.target.checked || undefined })} />
              must be there
            </label>
            <button className="icon-button remove" title="Remove the field" aria-label="Remove the field" disabled={rows.length < 2} onClick={() => write(rows.filter((_, j) => j !== i))}>
              ✕
            </button>
          </div>
        ))}
      </div>
      <button className="quiet add-row" onClick={() => write([...rows, { name: `field_${rows.length + 1}`, type: "string" }])}>
        + Field
      </button>
    </>
  );
}

/** Its credential, its model, what it works on, and the model's settings. */
function ModelFields(props: { kind: string; fields: Fields; set: Put; textHint: string }) {
  const { fields, set } = props;
  return (
    <>
      <div className="row top">
        <CredentialPicker kind={props.kind} value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
        <ModelPicker credential={String(fields.credential ?? "")} value={String(fields.model ?? "")} onChange={(v) => set("model", v || undefined)} />
      </div>
      <Area label="Text" hint={props.textHint} value={typeof fields.text === "string" ? fields.text : JSON.stringify(fields.text ?? "")} onChange={(v) => set("text", v)} />
    </>
  );
}

function MoreForTheModel({ fields, set }: { fields: Fields; set: Put }) {
  return (
    <details className="more">
      <summary>More for the model</summary>
      <Area label="Instructions" hint="optional: beside the step's own, as how to tell two categories apart" value={String(fields.instructions ?? "")} onChange={(v) => set("instructions", v || undefined)} />
      <div className="row">
        <label className="narrow">
          Temperature
          <input type="number" step="0.1" min={0} max={2} value={String(fields.temperature ?? "")} onChange={(e) => set("temperature", e.target.value === "" ? undefined : Number(e.target.value))} />
        </label>
        <label className="narrow">
          Max tokens
          <input type="number" min={1} value={String(fields.max_tokens ?? "")} onChange={(e) => set("max_tokens", e.target.value === "" ? undefined : Number(e.target.value))} />
        </label>
      </div>
    </details>
  );
}

/** The step's settings with some changed at once: one left undefined is taken out. */
function changed(fields: Fields, part: Fields): Fields {
  const next: Fields = { ...fields, ...part };
  for (const [k, v] of Object.entries(part)) if (v === undefined) delete next[k];
  return next;
}

export function AiStepBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { kind, fields, set, setBody } = props;
  switch (kind) {
    case "classify":
      return (
        <>
          <ModelFields kind={kind} fields={fields} set={set} textHint="what to sort, with {{ }} for values" />
          <Choices label="Category" least={1} value={(Array.isArray(fields.categories) ? fields.categories : []) as Choice[]} onChange={(v) => set("categories", v)} />
          <div className="row">
            <label className="check">
              <input type="checkbox" checked={fields.several === true} onChange={(e) => set("several", e.target.checked || undefined)} />
              More than one may fit (each one's steps run)
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={fields.other !== false}
                onChange={(e) => setBody(changed(fields, e.target.checked ? { other: undefined, default: [] } : { other: false, default: undefined }))}
              />
              Other, when none fits
            </label>
          </div>
          <p className="hint">The model chooses; each category's steps are its branch on the canvas, other's the last one. Its choice is steps.&lt;this step&gt;.category.</p>
          <MoreForTheModel fields={fields} set={set} />
        </>
      );
    case "sentiment":
      return (
        <>
          <ModelFields kind={kind} fields={fields} set={set} textHint="what to read, with {{ }} for values" />
          <Choices label="Label" least={2} value={(Array.isArray(fields.labels) ? fields.labels : ["positive", "neutral", "negative"]) as Choice[]} onChange={(v) => set("labels", v)} />
          <p className="hint">Gives steps.&lt;this step&gt;.label and .score (0 to 1: how strongly the text carries it); each label's steps are its branch on the canvas.</p>
          <MoreForTheModel fields={fields} set={set} />
        </>
      );
    case "extract": {
      const bySchema = fields.schema !== undefined;
      const mode = (schema: boolean) =>
        setBody(
          changed(
            fields,
            schema
              ? { fields: undefined, schema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] } }
              : { schema: undefined, fields: [{ name: "name", type: "string" }] },
          ),
        );
      return (
        <>
          <ModelFields kind={kind} fields={fields} set={set} textHint="what to pull the fields out of" />
          <div className="row">
            <label className="check">
              <input type="radio" name="extract-by" checked={!bySchema} onChange={() => mode(false)} />
              Fields
            </label>
            <label className="check">
              <input type="radio" name="extract-by" checked={bySchema} onChange={() => mode(true)} />
              A JSON schema
            </label>
          </div>
          {bySchema ? (
            <JsonField label="JSON schema" value={fields.schema} onChange={(v) => set("schema", v)} />
          ) : (
            <FieldRows value={(Array.isArray(fields.fields) ? fields.fields : []) as Field[]} onChange={(v) => set("fields", v)} />
          )}
          <p className="hint">The answer is held to them: a mismatch is asked once more, then fails the step. It is steps.&lt;this step&gt;.json; a field the text does not give is null.</p>
          <MoreForTheModel fields={fields} set={set} />
        </>
      );
    }
    default:
      return (
        <>
          <ModelFields kind={kind} fields={fields} set={set} textHint="a text, or a list of texts, as {{ value.messages }}" />
          <div className="row">
            <Text label="Length" value={String(fields.length ?? "")} onChange={(v) => set("length", v || undefined)} placeholder="short, medium, long, or 3 sentences" />
            <Text label="Style" value={String(fields.style ?? "")} onChange={(v) => set("style", v || undefined)} placeholder="bullets, prose, or for a customer" />
            <label className="narrow">
              Piece size
              <input type="number" min={1000} max={100000} placeholder="12000" value={String(fields.piece_size ?? "")} onChange={(e) => set("piece_size", e.target.value === "" ? undefined : Number(e.target.value))} />
            </label>
          </div>
          <p className="hint">A long text is summarised in pieces of this many characters (at most 40), then as a whole. The summary is steps.&lt;this step&gt;.summary.</p>
          <MoreForTheModel fields={fields} set={set} />
        </>
      );
  }
}

/** "Ask AI"'s answer held to a JSON schema: checked, asked once more on a mismatch. */
export function SchemaField({ fields, set }: { fields: Fields; set: Put }) {
  const on = fields.schema !== undefined;
  return (
    <details className="more" open={on}>
      <summary>Answer to a JSON schema</summary>
      <label className="check">
        <input type="checkbox" checked={on} onChange={(e) => set("schema", e.target.checked ? { type: "object", properties: { answer: { type: "string" } }, required: ["answer"] } : undefined)} />
        Hold the answer to a schema: a mismatch is asked once more, then fails the step
      </label>
      {on && <JsonField label="JSON schema" value={fields.schema} onChange={(v) => set("schema", v)} />}
    </details>
  );
}
