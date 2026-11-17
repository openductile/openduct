// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// What a step can read, beside its settings: the record the run started with (`value`), in a loop its `item` and
// `index`, what each earlier step gave in the last test (`steps.<name>`), and how the run started (`trigger`). A field
// clicked goes into the box last written in - as {{ its path }}, or as the bare path in a box that takes an
// expression - and a field the step names that its input does not have is said, before a test runs it empty.

import { useState, type RefObject } from "react";
import type { Definition, Run, Step } from "../api";
import { JsonField, names } from "../Steps";
import { partsOf } from "./Output";
import { listAt, type StepPath } from "./paths";

/** A box a field may be put in: one of text. */
export type Box = HTMLInputElement | HTMLTextAreaElement;

const TEXTY = new Set(["text", "search", "url", "email", "tel"]);

/** Whether `el` is a box a field may go in: text, and not one marked as its own (a step's name). */
export function writable(el: EventTarget | null): el is Box {
  const box = el instanceof HTMLTextAreaElement || (el instanceof HTMLInputElement && TEXTY.has(el.type));
  return box && (el as Box).dataset.plain === undefined;
}

/** Puts `path` at the box's cursor - bare in a box that takes an expression - as typing would; what went in. */
export function putIn(box: Box, path: string): string {
  const text = box.dataset.expr !== undefined ? path : `{{ ${path} }}`;
  const start = box.selectionStart ?? box.value.length;
  const end = box.selectionEnd ?? start;
  const next = box.value.slice(0, start) + text + box.value.slice(end);
  // The value set as the page's own boxes set it, so the form hears it as typing.
  const proto = box instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
  Object.getOwnPropertyDescriptor(proto, "value")?.set?.call(box, next);
  box.dispatchEvent(new Event("input", { bubbles: true }));
  box.focus();
  const at = start + text.length;
  requestAnimationFrame(() => box.isConnected && box.setSelectionRange(at, at));
  return text;
}

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** A path one level down: `.name`, `["odd name"]` or `[0]`. */
export function down(path: string, key: string | number): string {
  if (typeof key === "number") return `${path}[${key}]`;
  return IDENT.test(key) ? `${path}.${key}` : `${path}[${JSON.stringify(key)}]`;
}

const SEGMENT = /\.([A-Za-z_][A-Za-z0-9_]*)|\[(\d+)\]|\["([^"]*)"\]|\['([^']*)'\]/g;

function segments(rest: string): (string | number)[] {
  return [...rest.matchAll(SEGMENT)].map((m) => (m[2] !== undefined ? Number(m[2]) : (m[1] ?? m[3] ?? m[4])!));
}

/** What is at `path` in `data`; `found` false when something on the way is not there. */
function lookup(data: unknown, path: (string | number)[]): { found: boolean; value?: unknown } {
  let here = data;
  for (const key of path) {
    if (Array.isArray(here) && typeof key === "number" && key < here.length) here = here[key];
    else if (here && typeof here === "object" && !Array.isArray(here) && typeof key === "string" && key in here)
      here = (here as Record<string, unknown>)[key];
    else return { found: false };
  }
  return { found: true, value: here };
}

const REF = /\b(value|steps|trigger|item)((?:\.[A-Za-z_][A-Za-z0-9_]*|\[\d+\]|\["[^"]*"\]|\['[^']*'\])*)/g;

/** The paths an expression or a template names: `value.total`, `steps.order.id` (a method called on it left off). */
export function namedIn(text: string): { base: string; path: (string | number)[]; shown: string }[] {
  const parts = text.includes("{{") ? [...text.matchAll(/\{\{([\s\S]*?)\}\}/g)].map((m) => m[1] ?? "") : [text];
  const out: { base: string; path: (string | number)[]; shown: string }[] = [];
  for (const part of parts) {
    for (const m of part.matchAll(REF)) {
      const at = m.index ?? 0;
      const before = part.slice(0, at);
      // A field of something else (x.value), or one only asked about (has(value.x)), is not one it needs.
      if (before.endsWith(".") || /has\(\s*$/.test(before)) continue;
      let path = segments(m[2] ?? "");
      if (part[at + m[0].length] === "(") path = path.slice(0, -1);
      if (path.length === 0) continue;
      const base = m[1]!;
      out.push({ base, path, shown: path.reduce<string>((p, k) => down(p, k), base) });
    }
  }
  return out;
}

/** Every text the step itself holds - not the steps inside a branch or loop of it, which are steps of their own. */
function textsOf(value: unknown, into: string[] = [], key = ""): string[] {
  if (typeof value === "string") into.push(value);
  else if (Array.isArray(value)) {
    if (["then", "else", "steps", "default"].includes(key)) return into;
    for (const v of value) textsOf(v, into);
  } else if (value && typeof value === "object") {
    for (const [k, v] of Object.entries(value)) if (k !== "name") textsOf(v, into, k);
  }
  return into;
}

/** The steps that come before the one at `at`: each list's earlier steps (and theirs inside), and the step it is in. */
export function earlierNames(steps: Step[], at: StepPath): string[] {
  const out: string[] = [];
  const all = (list: Step[]) => list.forEach((s) => out.push(...names([s])));
  for (let i = 0; i < at.list.length; i += 2) {
    const list = listAt(steps, at.list.slice(0, i));
    const index = at.list[i] as number;
    all(list.slice(0, index));
    const holder = list[index];
    if (typeof holder?.name === "string") out.push(holder.name);
  }
  all(listAt(steps, at.list).slice(0, at.index));
  return [...new Set(out)];
}

/** How the workflow starts, as a real run's `trigger` gives it (a test's is { trigger: "test", at }). */
export function triggerShape(trigger: unknown): Record<string, unknown> {
  const kind = typeof trigger === "string" ? trigger : trigger && typeof trigger === "object" ? Object.keys(trigger)[0] : "";
  const at = "2026-11-16T09:30:00Z";
  switch (kind) {
    case "webhook":
      return { trigger: "webhook", method: "POST", path: "/", query: {}, headers: { "content-type": "application/json" }, at };
    case "schedule":
      return { trigger: "schedule", at, slot: at };
    case "topic":
      return { trigger: "topic", topic: "orders", partition: 0, offset: 42, key: "A-100" };
    default:
      return { trigger: "manual", by: "you@example.com", at };
  }
}

/** The innermost loop the step is in: the sample's first item of its list, when the list is a plain path. */
function loopOf(definition: Definition, at: StepPath, known: Record<string, unknown>): { in: boolean; item?: unknown } {
  const i = at.list.lastIndexOf("steps");
  if (i < 1) return { in: false };
  const loop = listAt(definition.steps ?? [], at.list.slice(0, i - 1))[at.list[i - 1] as number];
  const items = (loop?.for_each as { items?: unknown } | undefined)?.items;
  const named = typeof items === "string" ? namedIn(items)[0] : undefined;
  if (!named || typeof items !== "string" || items.trim() !== named.shown) return { in: true };
  const list = lookup(known[named.base], named.path);
  return { in: true, item: list.found && Array.isArray(list.value) && list.value.length > 0 ? list.value[0] : undefined };
}

function shortly(v: unknown): string {
  if (Array.isArray(v)) return `${v.length} item${v.length === 1 ? "" : "s"}`;
  if (v && typeof v === "object") return `${Object.keys(v).length} field${Object.keys(v).length === 1 ? "" : "s"}`;
  const text = JSON.stringify(v) ?? "";
  return text.length > 36 ? `${text.slice(0, 35)}…` : text;
}

/** A value's fields, each a button that puts its path where one is writing; a list by its first item. */
function Fields(props: { data: unknown; path: string; pick: (path: string) => void; depth?: number }) {
  const depth = props.depth ?? 0;
  const { data } = props;
  const entries: [string | number, unknown][] = Array.isArray(data)
    ? data.length > 0
      ? [[0, data[0]]]
      : []
    : data && typeof data === "object"
      ? Object.entries(data)
      : [];
  if (entries.length === 0 || depth > 4) return null;
  return (
    <ul className="input-fields">
      {entries.map(([key, v]) => {
        const path = down(props.path, key);
        return (
          <li key={String(key)}>
            <button
              type="button"
              className="input-field"
              title={`Put ${path} where you are writing`}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => props.pick(path)}
            >
              <span className="input-name">{typeof key === "number" ? `[${key}]` : key}</span>
              <span className="input-value">{shortly(v)}</span>
            </button>
            <Fields data={v} path={path} pick={props.pick} depth={depth + 1} />
          </li>
        );
      })}
    </ul>
  );
}

function Root(props: { name: string; what: string; pick: (path: string) => void }) {
  return (
    <button
      type="button"
      className="input-root"
      title={`Put ${props.name} where you are writing`}
      onMouseDown={(e) => e.preventDefault()}
      onClick={() => props.pick(props.name)}
    >
      <code>{props.name}</code> <span>{props.what}</span>
    </button>
  );
}

export function InputPane(props: {
  definition: Definition;
  at: StepPath;
  step: Step;
  sample: unknown;
  setSample: (v: unknown) => void;
  run: Run | null;
  /** The box last written in, in the step's settings. */
  box: RefObject<Box | null>;
}) {
  const [editing, setEditing] = useState(false);
  const [said, setSaid] = useState("");
  const steps = props.definition.steps ?? [];
  const earlier = earlierNames(steps, props.at);
  const parts = partsOf(props.run);
  const results: Record<string, unknown> = {};
  for (const name of earlier) if (parts.has(name)) results[name] = parts.get(name)?.result;
  const loop = loopOf(props.definition, props.at, { value: props.sample, steps: results });
  const trigger = triggerShape(props.definition.trigger);

  const pick = (path: string) => {
    const box = props.box.current;
    if (box && box.isConnected) {
      setSaid(`Put ${putIn(box, path)} in.`);
      return;
    }
    const text = `{{ ${path} }}`;
    void navigator.clipboard?.writeText(text).catch(() => undefined);
    setSaid(`Copied ${text}: click in a box first to put it there.`);
  };

  // What the step names that its input does not have, said before a test runs it empty.
  const own = typeof props.step.name === "string" ? props.step.name : "";
  const everyName = names(steps);
  const missing: string[] = [];
  for (const ref of textsOf(props.step).flatMap(namedIn)) {
    let why = "";
    if (ref.base === "value" && !lookup(props.sample, ref.path).found) why = "is not in the sample input";
    if (ref.base === "item" && loop.item !== undefined && !lookup(loop.item, ref.path).found) why = "is not in the list's first item";
    if (ref.base === "steps") {
      const [name, ...rest] = ref.path;
      if (typeof name !== "string") continue;
      if (name === own) why = "is this step's own result: it is not there yet";
      else if (!earlier.includes(name)) why = everyName.has(name) ? `- ${name} comes after this step` : `- there is no step called ${name}`;
      else if (name in results && rest.length > 0 && !lookup(results[name], rest).found) why = `is not in what ${name} gave in the last test`;
    }
    const line = `${ref.shown} ${why}`;
    if (why && !missing.includes(line)) missing.push(line);
  }

  return (
    <section className="popup-input" aria-label="Input">
      <div className="output-head">
        <h4>Input</h4>
      </div>
      <p className="hint">What this step can read. Click a field to put it in the box you are writing in.</p>
      {missing.length > 0 && (
        <ul className="input-missing" aria-label="Not in the input">
          {missing.map((m) => (
            <li key={m}>{m}</li>
          ))}
        </ul>
      )}
      {said && <p className="input-said">{said}</p>}

      <div className="input-part">
        <div className="input-part-head">
          <Root name="value" what="the record it started with" pick={pick} />
          <button type="button" className="quiet input-edit" onClick={() => setEditing(!editing)} aria-expanded={editing}>
            {editing ? "Done" : "Edit"}
          </button>
        </div>
        {editing ? (
          <>
            <JsonField label="Sample input" value={props.sample} onChange={props.setSample} />
            <p className="hint">A test or a preview runs on it; a real run on what arrives.</p>
          </>
        ) : (
          <Fields data={props.sample} path="value" pick={pick} />
        )}
      </div>

      {loop.in && (
        <div className="input-part">
          <Root name="item" what="this time round the loop" pick={pick} />
          {loop.item === undefined ? (
            <p className="hint">Each item of the loop's list, one at a time.</p>
          ) : (
            <Fields data={loop.item} path="item" pick={pick} />
          )}
          <Root name="index" what="its place in the list, from 0" pick={pick} />
        </div>
      )}

      <div className="input-part">
        <Root name="steps" what="what earlier steps gave" pick={pick} />
        {earlier.length === 0 && <p className="hint">No step comes before this one.</p>}
        {earlier.map((name) => (
          <div key={name} className="input-step">
            <Root name={down("steps", name)} what={name in results ? "" : props.run ? "did not run in the last test" : "- Test to see what it gives"} pick={pick} />
            {name in results && <Fields data={results[name]} path={down("steps", name)} pick={pick} />}
          </div>
        ))}
      </div>

      <div className="input-part">
        <Root name="trigger" what="how the run started" pick={pick} />
        <Fields data={trigger} path="trigger" pick={pick} />
        <p className="hint">As a real run gives it; a test gives trigger "test".</p>
      </div>
    </section>
  );
}
