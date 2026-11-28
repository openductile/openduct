// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Offering a workflow as a tool, in its "Starts when" window: a switch, then what a model reads to choose it and the
// arguments it takes - each a name, a type, a few words and whether it is needed. The workspace's MCP server lists it
// for AI assistants once the workflow is on; a call runs it with the arguments as what came in.

import type { Definition, ToolOffer as Offer } from "./api";

const TYPES: { type: string; label: string }[] = [
  { type: "string", label: "Text" },
  { type: "number", label: "Number" },
  { type: "integer", label: "Whole number" },
  { type: "boolean", label: "Yes or no" },
  { type: "array", label: "List" },
  { type: "object", label: "Record" },
];

type Input = { type?: string; description?: string; required?: boolean; choices?: unknown[] };

function inputOf(value: Input | string | undefined): Input {
  return typeof value === "string" ? { type: value } : (value ?? {});
}

export function ToolOffer(props: { definition: Definition; onChange: (d: Definition) => void }) {
  const offer = props.definition.tool;
  const starts = props.definition.trigger;
  const kind = typeof starts === "string" ? starts : starts && typeof starts === "object" ? Object.keys(starts)[0] : "manual";
  if (kind === "topic" || kind === "failed") return null;
  const write = (next: Offer | undefined) => {
    const d = { ...props.definition };
    if (next) d.tool = next;
    else delete d.tool;
    props.onChange(d);
  };
  const inputs = Object.entries(offer?.input ?? {}).map(([name, v]) => [name, inputOf(v)] as [string, Input]);
  const putInputs = (next: [string, Input][]) => {
    if (!offer) return;
    const input = Object.fromEntries(next);
    const changed: Offer = { ...offer, input };
    if (next.length === 0) delete changed.input;
    write(changed);
  };
  return (
    <div className="tg-section tool-offer">
      <label className="check">
        <input
          type="checkbox"
          checked={!!offer}
          onChange={(e) => write(e.target.checked ? { description: "" } : undefined)}
        />
        Offer it as a tool to AI assistants
      </label>
      {offer && (
        <>
          <label className="tg-field">
            <span className="tg-name">What it does</span>
            <input
              value={offer.description}
              placeholder="the price of an item, for a quantity"
              aria-label="What the tool does"
              onChange={(e) => write({ ...offer, description: e.target.value })}
            />
          </label>
          <label className="tg-field">
            <span className="tg-name">
              Its name <span className="tg-optional">optional</span>
            </span>
            <input className="code" value={offer.name ?? ""} placeholder="the workflow's name, made one word" aria-label="The tool's name" onChange={(e) => write({ ...offer, name: e.target.value.replace(/[^A-Za-z0-9_.-]/g, "_") || undefined })} />
          </label>
          <div className="tg-field">
            <span className="tg-name">What it takes</span>
            {inputs.map(([name, input], i) => (
              <div className="row tool-input" key={i}>
                <input aria-label="Argument name" className="code" value={name} onChange={(e) => putInputs(inputs.map((x, j) => (j === i ? [e.target.value.replace(/[^A-Za-z0-9_]/g, "_"), x[1]] : x)))} />
                <select aria-label="Argument type" value={input.type ?? "string"} onChange={(e) => putInputs(inputs.map((x, j) => (j === i ? [x[0], { ...x[1], type: e.target.value }] : x)))}>
                  {TYPES.map((t) => (
                    <option key={t.type} value={t.type}>
                      {t.label}
                    </option>
                  ))}
                </select>
                <input aria-label="Argument description" placeholder="what it is, for the model" value={input.description ?? ""} onChange={(e) => putInputs(inputs.map((x, j) => (j === i ? [x[0], { ...x[1], description: e.target.value || undefined }] : x)))} />
                <label className="check">
                  <input type="checkbox" checked={input.required === true} onChange={(e) => putInputs(inputs.map((x, j) => (j === i ? [x[0], { ...x[1], required: e.target.checked || undefined }] : x)))} />
                  needed
                </label>
                <button type="button" className="quiet" aria-label={`Remove ${name}`} onClick={() => putInputs(inputs.filter((_, j) => j !== i))}>
                  ✕
                </button>
              </div>
            ))}
            <button type="button" className="quiet" onClick={() => putInputs([...inputs, [`argument${inputs.length + 1}`, { type: "string" }]])}>
              + An argument
            </button>
          </div>
          <label className="tg-field">
            <span className="tg-name">
              Waits up to <span className="tg-optional">optional</span>
            </span>
            <input className="code" value={offer.timeout ?? ""} placeholder="60s, at most 5m" aria-label="How long a call waits" onChange={(e) => write({ ...offer, timeout: e.target.value || undefined })} />
          </label>
          <p className="tg-note">
            A call runs this workflow with its arguments as <code>value</code>; its answer is what its last step (or a respond step) gives. Turn it on to offer it: the address and tokens are
            on the Settings page.
          </p>
        </>
      )}
    </div>
  );
}
