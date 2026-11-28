// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Steps of apps defined by files (apps/<id>/app.yaml): each app's step and form are made from its definition - its
// actions and their inputs - so a new app needs nothing here.

import type { AppInfo, Step } from "./api";
import { Area, CredentialPicker, JsonField, Text } from "./Steps";

/** The apps this page makes forms for from their definitions. */
const CATALOG: Record<string, AppInfo> = {};

export function remember(app: AppInfo) {
  CATALOG[app.id] = app;
}

/** Every app this page makes forms for. */ // G3 app triggers
export function catalogApps(): AppInfo[] {
  return Object.values(CATALOG);
}

/** The app a kind of step is, when its form is made from its definition. */
export function catalogApp(kind: string): AppInfo | undefined {
  return CATALOG[kind];
}

/** A new step of an app: its default action (or its first), the inputs it needs left to fill in. */
export function appStep(app: AppInfo, name: string): Step {
  const action = app.actions.find((a) => a.name === app.default_action) ?? app.actions[0];
  const body: Record<string, unknown> = { credential: "" };
  if (!app.default_action && action) body.action = action.name;
  for (const input of action?.inputs.filter((i) => i.required) ?? []) body[input.name] = "";
  return { name, [app.id]: body };
}

export function AppBody(props: { app: AppInfo; fields: Record<string, unknown>; set: (name: string, value: unknown) => void; setBody: (b: unknown) => void }) {
  const { app, fields } = props;
  const chosen = String(fields.action ?? app.default_action ?? app.actions[0]?.name ?? "");
  const action = app.actions.find((a) => a.name === chosen);
  // Another action: its inputs, the ones it shares kept.
  const pick = (name: string) => {
    const next = app.actions.find((a) => a.name === name);
    const kept: Record<string, unknown> = { credential: fields.credential ?? "" };
    if (name !== app.default_action) kept.action = name;
    for (const input of next?.inputs ?? []) {
      if (fields[input.name] !== undefined) kept[input.name] = fields[input.name];
      else if (input.required) kept[input.name] = "";
    }
    props.setBody(kept);
  };
  return (
    <>
      <div className="row">
        <CredentialPicker kind={app.id} value={String(fields.credential ?? "")} onChange={(v) => props.set("credential", v)} />
        {app.actions.length > 1 && (
          <label>
            Action
            <select value={chosen} onChange={(e) => pick(e.target.value)}>
              {app.actions.map((a) => (
                <option key={a.name} value={a.name}>
                  {a.label}
                  {a.write ? "" : " (reads)"}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>
      {action?.description && <p className="hint">{action.description}</p>}
      {action?.inputs.map((input) => {
        const label = `${input.label}${input.required ? "" : " (optional)"}`;
        const value = fields[input.name];
        const set = (v: unknown) => props.set(input.name, v === "" && !input.required ? undefined : v);
        if (input.kind === "record" || input.kind === "list") {
          return <JsonField key={input.name} label={label} value={value ?? (input.kind === "list" ? [] : {})} onChange={set} />;
        }
        if (input.name === "text" || input.name === "body" || input.name === "message" || input.name === "description") {
          return <Area key={input.name} label={label} hint="with {{ }} for values" value={String(value ?? "")} onChange={set} />;
        }
        return <Text key={input.name} label={label} value={String(value ?? "")} onChange={set} placeholder={input.description || undefined} wide />;
      })}
    </>
  );
}
