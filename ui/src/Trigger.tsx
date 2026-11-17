// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// What starts a workflow, in its own window: the five kinds as tiles with their symbols, then each one's few
// settings - a webhook's address and token, each copied with one click, and a ready `curl` to try it; a schedule's
// interval with the usual ones a click away; a topic and where to read it from; another workflow whose failures
// start this one.

import { useState } from "react";
import type { Definition, WorkflowRecord } from "./api";
import { Icon } from "./flow/Icon";
import { WorkflowPicker } from "./StepsMore";

export type TriggerKind = "webhook" | "schedule" | "topic" | "manual" | "failed";

const KINDS: { kind: TriggerKind; label: string; hint: string }[] = [
  { kind: "webhook", label: "Webhook", hint: "Another system calls its address" },
  { kind: "schedule", label: "Schedule", hint: "Every so often" },
  { kind: "topic", label: "Topic", hint: "Each message of a topic" },
  { kind: "manual", label: "By hand", hint: "Someone presses Run" },
  { kind: "failed", label: "When one fails", hint: "Another workflow's failed runs" },
];

const EVERY = ["1m", "5m", "15m", "1h", "6h", "1d"];

export function triggerOf(trigger: unknown): { kind: TriggerKind; settings: Record<string, unknown> } {
  if (typeof trigger === "string") return { kind: trigger as TriggerKind, settings: {} };
  if (trigger && typeof trigger === "object") {
    const [kind, settings] = Object.entries(trigger as Record<string, unknown>)[0] ?? ["manual", {}];
    return { kind: kind as TriggerKind, settings: (settings ?? {}) as Record<string, unknown> };
  }
  return { kind: "manual", settings: {} };
}

/** A random token, as callers send it: 32 letters and digits. */
function newToken(): string {
  const bytes = new Uint8Array(24);
  crypto.getRandomValues(bytes);
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, "x").replace(/\//g, "y").replace(/=+$/, "");
}

function Symbol({ d }: { d: string }) {
  return (
    <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={d} />
    </svg>
  );
}

const SYMBOLS = {
  copy: "M9 9h11v11H9zM5 15H4V4h11v1",
  done: "M5 12l5 5L20 7",
  key: "M15 7a4 4 0 1 1-3.9 4.9L4 19v-3h2v-2h2l1.1-1.1A4 4 0 0 1 15 7zM16 10h.01",
  shuffle: "M16 3h5v5M4 20 21 3M21 16v5h-5M15 15l6 6M4 4l5 5",
  eye: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z",
  hide: "M3 3l18 18M10.6 5.1A10.5 10.5 0 0 1 12 5c6.5 0 10 7 10 7a17 17 0 0 1-3.2 4M6.6 6.6C3.8 8.4 2 12 2 12s3.5 7 10 7a9.7 9.7 0 0 0 5.4-1.6",
  info: "M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 16v-4M12 8h.01",
};

/** A button that copies `text`, and says so for a moment. */
export function Copy(props: { text: string; label: string }) {
  const [done, setDone] = useState(false);
  return (
    <button
      type="button"
      className="tg-icon"
      aria-label={props.label}
      title={done ? "Copied" : props.label}
      onClick={() => {
        navigator.clipboard?.writeText(props.text).then(
          () => {
            setDone(true);
            setTimeout(() => setDone(false), 1500);
          },
          () => undefined,
        );
      }}
    >
      <Symbol d={done ? SYMBOLS.done : SYMBOLS.copy} />
    </button>
  );
}

export function TriggerSettings(props: { definition: Definition; record: WorkflowRecord; onChange: (d: Definition) => void }) {
  const { kind, settings } = triggerOf(props.definition.trigger);
  const set = (k: TriggerKind, s: Record<string, unknown>) => props.onChange({ ...props.definition, trigger: { [k]: s } });
  const choose = (k: TriggerKind) => k !== kind && set(k, k === "schedule" ? { every: "1h" } : {});
  return (
    <div className="trigger-settings">
      <div className="tg-kinds" role="radiogroup" aria-label="What starts it">
        {KINDS.map((k) => (
          <button key={k.kind} type="button" role="radio" aria-checked={k.kind === kind} className={k.kind === kind ? "here" : ""} title={k.hint} onClick={() => choose(k.kind)}>
            <span className="tg-symbol">
              <Icon kind={k.kind} />
            </span>
            <span className="tg-label">{k.label}</span>
          </button>
        ))}
      </div>
      <p className="tg-lead">{KINDS.find((k) => k.kind === kind)?.hint}.</p>
      {kind === "webhook" && (
        <Webhook
          record={props.record}
          token={typeof settings.token === "string" ? settings.token : ""}
          sample={props.definition.sample}
          onToken={(t) => set("webhook", t ? { token: t } : {})}
        />
      )}
      {kind === "schedule" && (
        <div className="tg-section">
          <label className="tg-field">
            <span className="tg-name">Every</span>
            <input className="code" value={String(settings.every ?? "")} placeholder="15m" onChange={(e) => set("schedule", { every: e.target.value })} />
          </label>
          <div className="tg-chips" role="group" aria-label="Usual intervals">
            {EVERY.map((e) => (
              <button key={e} type="button" className={settings.every === e ? "here" : ""} onClick={() => set("schedule", { every: e })}>
                {e}
              </button>
            ))}
          </div>
          <p className="tg-note">
            <Symbol d={SYMBOLS.info} /> Counted from midnight UTC: every 15m runs at :00, :15, :30 and :45. 30s, 15m, 1h and 1d are all written so.
          </p>
        </div>
      )}
      {kind === "topic" && (
        <div className="tg-section">
          <label className="tg-field">
            <span className="tg-name">Topic</span>
            <input className="code" value={String(settings.name ?? "")} placeholder="orders" onChange={(e) => set("topic", { ...settings, name: e.target.value })} />
          </label>
          <div className="tg-field">
            <span className="tg-name">Read</span>
            <div className="tg-segments" role="radiogroup" aria-label="Read">
              {[
                ["latest", "New messages"],
                ["earliest", "Every message kept"],
              ].map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  role="radio"
                  aria-checked={String(settings.start ?? "latest") === value}
                  className={String(settings.start ?? "latest") === value ? "here" : ""}
                  onClick={() => set("topic", { ...settings, start: value })}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}
      {kind === "manual" && (
        <p className="tg-note">
          <Symbol d={SYMBOLS.info} /> Turn it on, then press Run for real under the try bar's more menu - or call <code>POST /api/workflows/{props.record.id}/run</code>.
        </p>
      )}
      {kind === "failed" && (
        <div className="tg-section">
          <WorkflowPicker label="Workflow" value={String(settings.workflow ?? "")} except={props.record.id} onChange={(v) => set("failed", { workflow: v })} />
          <p className="tg-note">
            <Symbol d={SYMBOLS.info} /> It runs for each record that workflow sends to CatchErr, from when this one is on: <code>value</code> is the record, <code>trigger.error</code>{" "}
            why its run failed, <code>trigger.run</code> which run.
          </p>
        </div>
      )}
      <RunsAtOnce definition={props.definition} onChange={props.onChange} />
    </div>
  );
}

const AT_ONCE = [1, 5, 10, 25, 50];

/** How many of its runs go at once on each node: 10 unless it says. */
function RunsAtOnce(props: { definition: Definition; onChange: (d: Definition) => void }) {
  const now = props.definition.runs_at_once ?? 10;
  const set = (n: number) => {
    const next = { ...props.definition };
    if (n === 10) delete next.runs_at_once;
    else next.runs_at_once = n;
    props.onChange(next);
  };
  return (
    <div className="tg-section">
      <div className="tg-field">
        <span className="tg-name">Runs at once</span>
        <div className="tg-chips" role="radiogroup" aria-label="Runs at once">
          {AT_ONCE.map((n) => (
            <button key={n} type="button" role="radio" aria-checked={now === n} className={now === n ? "here" : ""} onClick={() => set(n)}>
              {n}
            </button>
          ))}
        </div>
      </div>
      <p className="tg-note">
        <Symbol d={SYMBOLS.info} /> {now === 1 ? "One at a time: each record runs after the one before." : `Up to ${now} runs go at once on each node, so a slow step does not hold up the rest.`} Records with the same key always
        run in order.
      </p>
    </div>
  );
}

function Webhook(props: { record: WorkflowRecord; token: string; sample?: unknown; onToken: (t: string) => void }) {
  const [shown, setShown] = useState(false);
  // A webhook's own address, made at random - not the workflow's id. A new webhook gets one when it is saved.
  const address = props.record.hook ? `${location.origin}${props.record.hook}` : "";
  // The workflow's own sample, as the shell takes it inside single quotes.
  const body = JSON.stringify(props.sample ?? { id: "A-100", total: 120 }).replace(/'/g, "'\\''");
  const curl = address
    ? `curl -X POST ${address} \\\n  -H 'content-type: application/json'${props.token ? ` \\\n  -H 'x-openduct-token: ${props.token}'` : ""} \\\n  -d '${body}'`
    : "";
  return (
    <div className="tg-section">
      <div className="tg-field">
        <span className="tg-name">Address</span>
        {address ? (
          <div className="tg-copyable">
            <span className="tg-method">POST</span>
            <code title={address}>{address}</code>
            <Copy text={address} label="Copy the address" />
          </div>
        ) : (
          <p className="tg-note">
            <Symbol d={SYMBOLS.info} /> Save the workflow to get its webhook's address.
          </p>
        )}
      </div>
      <div className="tg-field">
        <span className="tg-name">
          Token <span className="tg-optional">optional</span>
        </span>
        <div className="tg-copyable tg-token">
          <Symbol d={SYMBOLS.key} />
          <input
            className="code"
            type={shown || !props.token ? "text" : "password"}
            value={props.token}
            placeholder="none: anyone with the address may call it"
            aria-label="Token"
            autoComplete="off"
            spellCheck={false}
            onChange={(e) => props.onToken(e.target.value)}
          />
          {props.token && (
            <button type="button" className="tg-icon" aria-label={shown ? "Hide the token" : "Show the token"} title={shown ? "Hide" : "Show"} onClick={() => setShown(!shown)}>
              <Symbol d={shown ? SYMBOLS.hide : SYMBOLS.eye} />
            </button>
          )}
          {props.token && <Copy text={props.token} label="Copy the token" />}
          <button type="button" className="tg-icon" aria-label="Make a new token" title="Make a new token" onClick={() => props.onToken(newToken())}>
            <Symbol d={SYMBOLS.shuffle} />
          </button>
        </div>
        <p className="tg-hint">Callers send it as the x-openduct-token header, a bearer token, or ?token=.</p>
      </div>
      {curl && (
        <div className="tg-field">
          <span className="tg-name">Try it from a terminal</span>
          <div className="tg-curl">
            <pre>{curl}</pre>
            <Copy text={curl.replace(/\\\n\s+/g, "")} label="Copy the command" />
          </div>
        </div>
      )}
    </div>
  );
}
