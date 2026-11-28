// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// An app's own trigger: which app, which of its triggers, the credential and the trigger's settings - all made from
// the app's definition. One that asks the app has how often and how many at most each time; one the app calls has
// the address it sends to, and says whether turning the workflow on gives the app that address or what to do in the
// app's own pages.

import type { AppInfo, AppTriggerInfo, WorkflowRecord } from "./api";
import { CredentialPicker, Text } from "./Steps";
import { catalogApps } from "./StepsApps";

const EVERY = ["1m", "5m", "15m", "1h", "1d"];
const MOST = [10, 50, 100, 500];
/** A trigger's own settings: not inputs of the app's. */
const OWN = ["when", "credential", "every", "max_items"];

/** The apps that start workflows, in the order they were given. */
export function appsWithTriggers(): AppInfo[] {
  return catalogApps().filter((a) => (a.triggers ?? []).length > 0);
}

/** A new app trigger: Gmail's new email when there is Gmail, else the first app's first trigger. */
export function newAppTrigger(): Record<string, unknown> {
  const apps = appsWithTriggers();
  const app = apps.find((a) => a.id === "gmail") ?? apps[0];
  const first = app?.triggers?.[0];
  if (!app || !first) return { manual: {} };
  return { [app.id]: fresh(first) };
}

/** A trigger's settings when it is chosen: the inputs it needs, left to fill in. */
function fresh(t: AppTriggerInfo): Record<string, unknown> {
  const s: Record<string, unknown> = { when: t.name, credential: "" };
  for (const i of t.inputs.filter((i) => i.required)) s[i.name] = "";
  return s;
}

function everyOf(ms: number | null): string {
  if (!ms) return "5m";
  if (ms % 86_400_000 === 0) return `${ms / 86_400_000}d`;
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000}h`;
  return `${Math.round(ms / 60_000)}m`;
}

export function AppTrigger(props: { app: string; settings: Record<string, unknown>; record: WorkflowRecord; onChange: (trigger: unknown) => void }) {
  const apps = appsWithTriggers();
  const app = apps.find((a) => a.id === props.app);
  const triggers = app?.triggers ?? [];
  const chosen = triggers.find((t) => t.name === props.settings.when) ?? triggers[0];
  const set = (s: Record<string, unknown>) => props.onChange({ [props.app]: s });
  const put = (name: string, value: unknown) => {
    const next: Record<string, unknown> = { ...props.settings, when: chosen?.name ?? props.settings.when };
    if (value === "" || value === undefined) delete next[name];
    else next[name] = value;
    set(next);
  };
  const pickApp = (id: string) => {
    const other = apps.find((a) => a.id === id);
    const first = other?.triggers?.[0];
    if (other && first) props.onChange({ [other.id]: { ...fresh(first), credential: "" } });
  };
  const pickTrigger = (name: string) => {
    const next = triggers.find((t) => t.name === name);
    if (!next) return;
    // The credential stays; the settings of another trigger go.
    set({ ...fresh(next), credential: props.settings.credential ?? "" });
  };
  if (!app) {
    return (
      <div className="tg-section">
        <p className="tg-note">No app here starts workflows yet: apps with triggers appear once the studio has read them.</p>
      </div>
    );
  }
  const address = props.record.hook ? `${location.origin}${props.record.hook}` : "";
  return (
    <div className="tg-section">
      <div className="row">
        <label>
          App
          <select value={app.id} onChange={(e) => pickApp(e.target.value)} aria-label="App">
            {apps.map((a) => (
              <option key={a.id} value={a.id}>
                {a.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          When
          <select value={chosen?.name ?? ""} onChange={(e) => pickTrigger(e.target.value)} aria-label="When">
            {triggers.map((t) => (
              <option key={t.name} value={t.name}>
                {t.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      {chosen?.description && <p className="hint">{chosen.description}</p>}
      <div className="row">
        <CredentialPicker kind={app.id} value={String(props.settings.credential ?? "")} onChange={(v) => put("credential", v)} />
      </div>
      {chosen?.inputs.map((input) => (
        <Text
          key={input.name}
          label={`${input.label}${input.required ? "" : " (optional)"}`}
          value={String(props.settings[input.name] ?? "")}
          onChange={(v) => put(input.name, v)}
          placeholder={input.description || undefined}
          wide
        />
      ))}
      {chosen?.kind === "poll" && <Asking settings={props.settings} every={everyOf(chosen.every_ms)} put={put} />}
      {chosen?.kind === "webhook" && (
        <div className="tg-field">
          <span className="tg-name">Address</span>
          {address ? (
            <div className="tg-copyable">
              <span className="tg-method">POST</span>
              <code title={address}>{address}</code>
            </div>
          ) : (
            <p className="tg-note">Save the workflow to get the address {app.name} sends to.</p>
          )}
          <p className="tg-hint">
            {chosen.registers
              ? `Turning the workflow on gives ${app.name} this address, and turning it off takes it away. Each delivery is checked against ${app.name}'s signature before it runs.`
              : `${chosen.setup || `Give ${app.name} this address in its own settings.`} Each delivery is checked against ${app.name}'s signature before it runs.`}
          </p>
        </div>
      )}
      <p className="tg-note">
        A run's <code>value</code> is what {app.name} gave; <code>trigger.id</code> tells it from the others.
      </p>
      {Object.keys(props.settings).some((k) => !OWN.includes(k) && !chosen?.inputs.some((i) => i.name === k)) && (
        <p className="tg-note">Some settings here are not this trigger's: choose the trigger again to clear them.</p>
      )}
    </div>
  );
}

/** How often a poll asks, and how many items at most each time. */
function Asking(props: { settings: Record<string, unknown>; every: string; put: (name: string, value: unknown) => void }) {
  const every = String(props.settings.every ?? props.every);
  const most = Number(props.settings.max_items ?? 50);
  return (
    <>
      <div className="tg-field">
        <span className="tg-name">Every</span>
        <div className="tg-chips" role="radiogroup" aria-label="How often it asks">
          {EVERY.map((e) => (
            <button key={e} type="button" role="radio" aria-checked={every === e} className={every === e ? "here" : ""} onClick={() => props.put("every", e === props.every ? undefined : e)}>
              {e}
            </button>
          ))}
        </div>
      </div>
      <div className="tg-field">
        <span className="tg-name">At most</span>
        <div className="tg-chips" role="radiogroup" aria-label="How many at most each time">
          {MOST.map((n) => (
            <button key={n} type="button" role="radio" aria-checked={most === n} className={most === n ? "here" : ""} onClick={() => props.put("max_items", n === 50 ? undefined : n)}>
              {n}
            </button>
          ))}
        </div>
      </div>
      <p className="tg-note">
        The first time it asks, it notes what is there and runs nothing: from then on, each new item runs once, the oldest first - up to {most} each time, the rest the next.
      </p>
    </>
  );
}
