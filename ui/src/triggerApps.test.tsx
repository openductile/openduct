// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// An app's trigger in the trigger window: made from the app's definition, its settings written as the engine reads
// them.
// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { AppInfo, Definition, WorkflowRecord } from "./api";
import { registerApps } from "./Steps";
import { TriggerSettings } from "./Trigger";
import { triggerSummary } from "./flow/look";

const gmail: AppInfo = {
  id: "gmail",
  name: "Gmail",
  category: "email",
  description: "Mail",
  docs: null,
  default_action: null,
  sign_ins: [{ id: "gmail", label: "Sign in with Google", connect: true }],
  actions: [{ name: "send_email", label: "Send an email", description: "", write: true, list: false, inputs: [] }],
  registered: true,
  triggers: [
    {
      name: "new_email",
      label: "A new email",
      description: "Each email that arrives.",
      inputs: [{ name: "q", label: "Only mail like this", required: false, description: "in:inbox", kind: "text" }],
      kind: "poll",
      every_ms: 300_000,
      registers: false,
      setup: "",
    },
  ],
};
const stripe: AppInfo = {
  ...gmail,
  id: "stripe",
  name: "Stripe",
  sign_ins: [{ id: "stripe", label: "A secret key", connect: false }],
  triggers: [
    {
      name: "event",
      label: "An event",
      description: "",
      inputs: [{ name: "events", label: "Which events", required: true, description: "", kind: "text" }],
      kind: "webhook",
      every_ms: null,
      registers: true,
      setup: "",
    },
  ],
};

const record: WorkflowRecord = { id: "w1", name: "Mail", active: false, version: 1, definition: { name: "Mail" }, created: 0, updated: 0, hook: "/hooks/h1" };

function shown(definition: Definition) {
  const changed: Definition[] = [];
  render(<TriggerSettings definition={definition} record={record} onChange={(d) => changed.push(d)} />);
  return changed;
}

afterEach(cleanup);

describe("an app's trigger", () => {
  registerApps([gmail, stripe]);

  it("is chosen from the tiles, starting with Gmail's new email", () => {
    const changed = shown({ name: "Mail", trigger: { manual: {} } });
    fireEvent.click(screen.getByRole("radio", { name: /An app/ }));
    expect(changed.at(-1)?.trigger).toEqual({ gmail: { when: "new_email", credential: "" } });
  });

  it("shows a poll's settings and writes them as the engine reads them", () => {
    const changed = shown({ name: "Mail", trigger: { gmail: { when: "new_email", credential: "" } } });
    expect(screen.getByRole("radio", { name: /An app/ }).getAttribute("aria-checked")).toBe("true");
    expect((screen.getByLabelText("When") as HTMLSelectElement).value).toBe("new_email");
    fireEvent.change(screen.getByPlaceholderText("in:inbox"), { target: { value: "from:shop@example.com" } });
    expect(changed.at(-1)?.trigger).toEqual({ gmail: { when: "new_email", credential: "", q: "from:shop@example.com" } });
    fireEvent.click(screen.getByRole("radio", { name: "15m" }));
    expect((changed.at(-1)?.trigger as { gmail: { every: string } }).gmail.every).toBe("15m");
    fireEvent.click(screen.getByRole("radio", { name: "100" }));
    expect((changed.at(-1)?.trigger as { gmail: { max_items: number } }).gmail.max_items).toBe(100);
    expect(screen.getByText(/notes what is there and runs nothing/)).toBeTruthy();
  });

  it("shows a webhook's address, and that turning the workflow on gives it to the app", () => {
    const changed = shown({ name: "Pay", trigger: { stripe: { when: "event", credential: "", events: "" } } });
    expect(screen.getByText(`${location.origin}/hooks/h1`)).toBeTruthy();
    expect(screen.getByText(/Turning the workflow on gives Stripe this address/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("App"), { target: { value: "gmail" } });
    expect(changed.at(-1)?.trigger).toEqual({ gmail: { when: "new_email", credential: "" } });
  });

  it("is said on the canvas in its trigger's words", () => {
    expect(triggerSummary({ gmail: { when: "new_email" } })).toEqual({ kind: "gmail", line: "a new email" });
    expect(triggerSummary({ nothing_known: {} }).kind).toBe("manual");
  });
});
