// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The page against a stand-in API: what each view shows, and what a person's clicks send.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { App } from "./App";

// The canvas measures what it draws; jsdom has nothing to measure with.
class Unmeasured {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(globalThis as { ResizeObserver?: unknown }).ResizeObserver ??= Unmeasured;

type Answer = { status?: number; body: unknown };
type Routes = Record<string, Answer | ((body: unknown) => Answer)>;

/** A fetch that answers from `routes` ("GET /api/status") and keeps what was sent. */
function stand_in(routes: Routes) {
  const sent: { route: string; body: unknown; workspace?: string }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    const route = `${init.method ?? "GET"} ${path}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    sent.push({ route, body, workspace: (init.headers as Record<string, string> | undefined)?.["x-openduct-workspace"] });
    // A request with a query is answered by its address alone, unless the query is named.
    const found = routes[route] ?? routes[route.split("?")[0]!];
    const answer = typeof found === "function" ? found(body) : found ?? { status: 404, body: { error: "no such request" } };
    return new Response(JSON.stringify(answer.body), { status: answer.status ?? 200 });
  });
  return sent;
}

const session = {
  account: { id: "u1", email: "ada@example.com", name: "Ada", admin: true },
  workspaces: [
    { id: "w1", name: "My workspace", role: "admin" },
    { id: "w2", name: "Client B", role: "viewer" },
  ],
  csrf: "tok",
};
const workflow = {
  id: "big-orders",
  active: false,
  created: 1,
  updated: 1,
  hook: "/hooks/big-orders",
  definition: {
    name: "Big orders",
    trigger: { webhook: { token: "t0k" } },
    steps: [
      { name: "order", set: { total: "value.qty * value.price" } },
      { name: "big", if: { when: "steps.order.total > 100", then: [{ name: "tell", http: { method: "POST", url: "https://x", json: { a: 1 } } }], else: [] } },
      { name: "each", for_each: { items: "value.lines", steps: [{ name: "w", wait: "1s" }] } },
      { name: "keep", topic: { name: "out", value: "{{ value }}" } },
      { name: "end", stop: { error: "no" } },
      { name: "only", where: "true" },
    ],
  },
};

/** A workspace with nothing in it yet, as the dashboard reads it. */
const emptyBoard = {
  range: "7d",
  from: Date.now() - 7 * 86400000,
  bucket_ms: 86400000,
  health: "green",
  workspace: { id: "w1", name: "My workspace" },
  workflows: { total: 0, on: 0, never_ran: 0 },
  credentials: 0,
  runs: { total: 0, done: 0, failed: 0, skipped: 0, buckets: Array.from({ length: 7 }, () => ({ done: 0, failed: 0, skipped: 0 })) },
  success_percent: null,
  waiting: 0,
  per_workflow: [],
  recent: [],
  alerts: [],
};

afterEach(() => {
  cleanup();
  localStorage.clear();
  vi.unstubAllGlobals();
  history.replaceState(null, "", "/");
});

describe("the studio", () => {
  it("asks for the owner's account on the first visit and opens the studio once it is made", async () => {
    const sent = stand_in({
      "GET /api/status": { body: { setup: true, version: "0.1.0" } },
      "POST /api/setup": { status: 201, body: session },
      "GET /api/workflows": { body: { workflows: [] } },
      "GET /api/templates": { body: { templates: [] } },
      "GET /api/dashboard": { body: emptyBoard },
      "GET /api/overview": { body: { workspaces: [] } },
    });
    render(<App />);
    fireEvent.change(await screen.findByLabelText("Your name"), { target: { value: "Ada" } });
    fireEvent.change(screen.getByLabelText("E-mail address"), { target: { value: "ada@example.com" } });
    fireEvent.change(screen.getByLabelText(/Password/), { target: { value: "correct horse battery" } });
    fireEvent.click(screen.getByText("Make the account"));
    // The studio opens on the dashboard of the workspace in use: for a new one, its first steps.
    expect(await screen.findByText("Getting started")).toBeTruthy();
    expect(screen.getByText("0 of 4 done")).toBeTruthy();
    expect(sent.find((s) => s.route === "POST /api/setup")?.body).toEqual({
      name: "Ada",
      email: "ada@example.com",
      password: "correct horse battery",
    });
  });

  it("lists workflows and templates, and makes one from a template", async () => {
    history.replaceState(null, "", "/workflows");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/workflows": { body: { workflows: [{ id: "big-orders", name: "Big orders", active: true, trigger: "webhook", updated: 1 }] } },
      "GET /api/templates": { body: { templates: [{ id: "t", name: "Alert on big orders", description: "d", category: "E-commerce", definition: workflow.definition }] } },
      "POST /api/workflows": { status: 201, body: workflow },
      "GET /api/workflows/big-orders": { body: workflow },
    });
    render(<App />);
    expect(await screen.findByText("Big orders")).toBeTruthy();
    expect(screen.getByText("Webhook")).toBeTruthy();
    fireEvent.click(screen.getByRole("link", { name: "Templates" }));
    expect(location.pathname).toBe("/templates");
    fireEvent.click(await screen.findByText("Alert on big orders"));
    await screen.findByDisplayValue("Big orders");
    expect(location.pathname).toBe("/workflows/big-orders");
    const made = sent.find((s) => s.route === "POST /api/workflows");
    expect((made?.body as { definition: { name: string } }).definition.name).toBe("Big orders");
  });

  it("shows each step on the canvas, its settings in a window, and tests on sample input", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
      "POST /api/workflows/big-orders/test": {
        body: { run: { status: "done", error: null, ms: 3, steps: [{ name: "order", kind: "set", attempts: 1, ms: 0, result: { total: 150 }, error: null }] } },
      },
      "PUT /api/workflows/big-orders": (body) => ({ body: { ...workflow, definition: (body as { definition: unknown }).definition } }),
    });
    render(<App />);
    fireEvent.click(await screen.findByText("Starts when"));
    expect(screen.getByRole("dialog", { name: "Starts when" }).textContent).toMatch(/\/hooks\/big-orders/);
    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    const shown: [string, string][] = [
      ["order", "Set fields"],
      ["big", "Branch (if / else)"],
      ["each", "Loop (for each)"],
      ["keep", "Write to a topic"],
      ["end", "Stop"],
      ["only", "Filter (only if)"],
    ];
    for (const [name, chip] of shown) {
      fireEvent.click(screen.getByText(name));
      expect(screen.getByRole("dialog", { name }).textContent).toContain(chip);
    }
    // The window's changes are the workflow's: a new name, saved.
    fireEvent.click(screen.getByText("order"));
    fireEvent.change(screen.getByLabelText("Step name"), { target: { value: "totals" } });
    fireEvent.click(screen.getByLabelText("Close"));
    expect(screen.getByText("totals")).toBeTruthy();
    fireEvent.click(screen.getByText("Test"));
    expect(await screen.findByText(/It ran through/)).toBeTruthy();
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(sent.some((s) => s.route === "PUT /api/workflows/big-orders")).toBe(true));
    const saved = sent.find((s) => s.route === "PUT /api/workflows/big-orders")?.body as { definition: { steps: { name: string }[] } };
    expect(saved.definition.steps[0]?.name).toBe("totals");
  });
});

describe("credentials, workspaces and AI", () => {
  it("says which workspace each request is for, and adds a credential without showing it back", async () => {
    history.replaceState(null, "", "/credentials");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": {
        body: {
          kinds: [
            {
              kind: "ai",
              label: "A model's API",
              fields: [
                { name: "base_url", optional: false, secret: false },
                { name: "api_key", optional: true, secret: true },
              ],
            },
          ],
        },
      },
      "POST /api/credentials": { status: 201, body: { name: "model", kind: "ai", fields: ["base_url", "api_key"], updated: 1 } },
    });
    render(<App />);
    fireEvent.click(await screen.findByText("Add a credential"));
    // Its kind first, from the tiles, then that kind's fields by their plain names.
    fireEvent.click(screen.getByText("AI model"));
    // A service from the list fills in its address; "Any other" leaves it to be written.
    fireEvent.click(screen.getByText("Any other"));
    fireEvent.change(screen.getByLabelText(/Name/), { target: { value: "model" } });
    fireEvent.change(screen.getByLabelText("Address"), { target: { value: "https://models.example.com/v1" } });
    const key = screen.getByLabelText("Key") as HTMLInputElement;
    expect(key.type).toBe("password");
    expect(key.autocomplete).toBe("new-password");
    fireEvent.change(key, { target: { value: "key-1" } });
    fireEvent.click(screen.getByText("Keep it"));
    await waitFor(() => expect(sent.some((s) => s.route === "POST /api/credentials")).toBe(true));
    const added = sent.find((s) => s.route === "POST /api/credentials");
    expect(added?.body).toEqual({
      name: "model",
      kind: "ai",
      fields: { provider: "other", base_url: "https://models.example.com/v1", api: "chat_completions", api_key: "key-1" },
    });
    expect(added?.workspace).toBe("w1");
    expect(screen.getByRole("list", { name: "Workspaces" })).toBeTruthy();
  });

  it("edits a script step's code and shows what a run of it logged", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const code = "result = {'n': len(value['lines'])}";
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/big-orders": {
        body: { ...workflow, definition: { name: "Big orders", steps: [{ name: "count", python: code }] } },
      },
      "POST /api/workflows/big-orders/test": {
        body: {
          run: {
            status: "done",
            error: null,
            ms: 90,
            steps: [{ name: "count", kind: "python", attempts: 1, ms: 80, error: null, result: { n: 2 }, logs: ["counting 2 lines"] }],
          },
        },
      },
    });
    render(<App />);
    fireEvent.click(await screen.findByText("Test"));
    await screen.findByText(/It ran through/);
    fireEvent.click(screen.getByText("count"));
    expect(screen.getByDisplayValue(code).tagName).toBe("TEXTAREA");
    expect(screen.getByText("counting 2 lines")).toBeTruthy();
  });

  it("shows an AI step's transcript and metrics, and previews with the reads done", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const aiRun = {
      status: "done",
      error: null,
      ms: 900,
      steps: [
        {
          name: "sum",
          kind: "ai",
          attempts: 1,
          ms: 880,
          error: null,
          result: {
            text: "A short summary.",
            model: "some-model",
            finish_reason: "stop",
            transcript: [
              { role: "system", content: "You summarise." },
              { role: "user", content: "Summarise this" },
              { role: "assistant", content: "A short summary." },
            ],
            usage: { prompt_tokens: 20, completion_tokens: 6, total_tokens: 26 },
            metrics: { latency_ms: 880, tokens_per_second: 6.8, ignored_settings: ["temperature"] },
          },
        },
      ],
    };
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [{ name: "model", kind: "ai", fields: [], updated: 1 }] } },
      "GET /api/workflows/big-orders": {
        body: { ...workflow, definition: { name: "Big orders", steps: [{ name: "sum", ai: { credential: "model", model: "some-model", prompt: "x" } }] } },
      },
      "POST /api/workflows/big-orders/preview": { body: { run: aiRun, input: { id: 1 } } },
      "GET /api/credentials/model/models": { body: { models: [{ id: "some-model", name: "Some model" }, { id: "other-model", name: null }] } },
    });
    render(<App />);
    fireEvent.click(await screen.findByText("sum"));
    expect(screen.getByRole("dialog", { name: "sum" }).textContent).toContain("Ask AI");
    expect(screen.getByText("model (ai)")).toBeTruthy();
    // The model is chosen from what the credential's service offers.
    expect(await screen.findByText("Some model (some-model)")).toBeTruthy();
    expect(screen.getByText("other-model")).toBeTruthy();
    // Previewed from the step's own window: its output shows there.
    fireEvent.click(within(screen.getByRole("dialog", { name: "sum" })).getByText("Preview"));
    expect(await screen.findByText("A short summary.", { selector: ".what" })).toBeTruthy();
    expect(screen.getByText("20 in / 6 out")).toBeTruthy();
    expect(screen.getByText("ignored: temperature")).toBeTruthy();
    expect(sent.find((s) => s.route === "POST /api/workflows/big-orders/preview")?.body).toMatchObject({ input: expect.anything() });
  });
});

describe("dashboard and alerts", () => {
  const busy = {
    ...emptyBoard,
    health: "red",
    workflows: { total: 2, on: 1, never_ran: 0 },
    credentials: 1,
    runs: { total: 12, done: 9, failed: 3, skipped: 0, buckets: emptyBoard.runs.buckets.map((b, i) => (i === 6 ? { done: 9, failed: 3, skipped: 0 } : b)) },
    success_percent: 75,
    waiting: 2,
    per_workflow: [
      { id: "big-orders", name: "Big orders", active: true, runs: 12, failed: 3, waiting: 2, last_at: Date.now() - 60000, last_status: "failed" },
      { id: "digest", name: "Daily digest", active: false, runs: 0, failed: 0, waiting: 0, last_at: Date.now() - 9 * 86400000, last_status: "done" },
    ],
    recent: [{ run: "0-9", at: Date.now() - 60000, status: "failed", error: "tell: connection refused", ms: 4, steps: [], workflow: { id: "big-orders", name: "Big orders" } }],
    alerts: [
      { key: "waiting/big-orders", level: "critical", title: "2 records waiting in CatchErr", detail: "Big orders: runs failed.", action: { label: "Review", path: "/workflows/big-orders" } },
      { key: "credential/big-orders/team", level: "warning", title: "Big orders needs the credential team", detail: "A step uses it.", action: { label: "Add it", path: "/credentials" } },
    ],
  };
  const clients = [
    { id: "w2", name: "Acme Bakery", role: "admin", health: "red", workflows: { total: 3, on: 2, never_ran: 0 }, runs: { total: 40, failed: 5 }, success_percent: 87.5, waiting: 4, alerts: [busy.alerts[0]], last_failure: Date.now() - 3600000 },
    { id: "w1", name: "My workspace", role: "admin", health: "green", workflows: { total: 1, on: 1, never_ran: 0 }, runs: { total: 8, failed: 0 }, success_percent: 100, waiting: 0, alerts: [], last_failure: null },
  ];

  it("shows a workspace at a glance, what needs it with a way to fix each, and its runs over time", async () => {
    history.replaceState(null, "", "/dashboard");
    localStorage.setItem("openduct.dashboard.view", "client");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/dashboard": { body: busy },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": { body: { kinds: [] } },
    });
    render(<App />);
    expect(await screen.findByText("1 / 2")).toBeTruthy();
    expect(screen.getByText("75%")).toBeTruthy();
    expect(screen.getByText("2 records waiting in CatchErr")).toBeTruthy();
    expect(screen.getByRole("img", { name: "Runs over time" })).toBeTruthy();
    expect(screen.getByText("tell: connection refused")).toBeTruthy();
    // Every step done but none - nothing to start: no first-steps list.
    expect(screen.queryByText("Getting started")).toBeNull();
    fireEvent.click(screen.getByText("Add it"));
    expect(location.pathname).toBe("/credentials");
  });

  it("connects an app from the dashboard: the credential window is open on arrival", async () => {
    history.replaceState(null, "", "/dashboard");
    localStorage.setItem("openduct.dashboard.view", "client");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/dashboard": { body: emptyBoard },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": { body: { kinds: [] } },
    });
    render(<App />);
    fireEvent.click(await screen.findByText("Connect an app", { selector: ".dash-actions button" }));
    expect(location.pathname).toBe("/credentials");
    expect(await screen.findByRole("dialog")).toBeTruthy();
    // The address is the page's again: a reload does not open it a second time.
    expect(location.hash).toBe("");
  });

  it("shows every workspace on a page of its own, those needing attention first, and opens one", async () => {
    history.replaceState(null, "", "/dashboard");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/overview": { body: { workspaces: clients } },
      "GET /api/dashboard": { body: busy },
    });
    render(<App />);
    // The dashboard is the workspace in use's alone; every workspace is a page of its own, first in the side's list.
    expect(await screen.findByText("Needs attention")).toBeTruthy();
    expect(screen.queryByText("Acme Bakery")).toBeNull();
    fireEvent.click(screen.getByText("All workspaces"));
    expect(location.pathname).toBe("/overview");
    expect((await screen.findAllByText("Acme Bakery")).length).toBeGreaterThan(0);
    expect(screen.getByText("Needs you")).toBeTruthy();
    expect(screen.getByText("All good")).toBeTruthy();
    // The bell counts what needs someone, in every client.
    expect(await screen.findByLabelText("1 alert")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("1 alert"));
    expect(screen.getByRole("dialog", { name: "Alerts" }).textContent).toContain("Acme Bakery");
    fireEvent.click(screen.getByLabelText("Dismiss: 2 records waiting in CatchErr"));
    expect(screen.getByLabelText("No alerts")).toBeTruthy();
    fireEvent.click(screen.getAllByText("Open")[0]!);
    expect(localStorage.getItem("openduct.workspace")).toBe("w2");
  });

  it("lets a workspace's admin choose where its alerts go: each messenger ready, turned on once set up", async () => {
    history.replaceState(null, "", "/settings");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false, version: "0.1.0" } },
      "GET /api/session": { body: session },
      "GET /api/workflows": { body: { workflows: [] } },
      "GET /api/credentials": { body: { credentials: [{ name: "space", kind: "google_chat_webhook", fields: ["url"], updated: 1 }] } },
      "GET /api/files": { body: { files: [] } },
      "GET /api/workspaces/w1/members": { body: { members: [] } },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/workspaces/w1/alerts": { body: { channels: [] } },
      "PUT /api/workspaces/w1/alerts": (body) => ({ body }),
    });
    render(<App />);
    for (const name of ["Slack", "WhatsApp", "Telegram", "Discord", "Microsoft Teams", "Google Chat"])
      expect((await screen.findAllByText(name)).length).toBeGreaterThan(0);
    const on = screen.getByLabelText("Google Chat: on") as HTMLInputElement;
    expect(on.disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Google Chat: credential"), { target: { value: "space" } });
    fireEvent.click(screen.getByLabelText("Google Chat: on"));
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(sent.some((x) => x.route === "PUT /api/workspaces/w1/alerts")).toBe(true));
    expect(sent.find((x) => x.route === "PUT /api/workspaces/w1/alerts")?.body).toEqual({
      channels: [{ kind: "google_chat", credential: "space", target: "", on: true }],
    });
  });
});

describe("workspaces", () => {
  it("makes a workspace from the side - Add or Enter - and says why when it cannot", async () => {
    history.replaceState(null, "", "/workflows");
    let made = false;
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: { ...session, account: { ...session.account, admin: true } } },
      "GET /api/workflows": { body: { workflows: [] } },
      "GET /api/templates": { body: { templates: [] } },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/dashboard": { body: emptyBoard },
      "POST /api/workspaces": (body) => {
        if ((body as { name: string }).name === "Taken") return { status: 422, body: { error: "a workspace by that name is there already" } };
        made = true;
        return { status: 201, body: { id: "w9", name: "Acme Bakery", role: "admin" } };
      },
    });
    render(<App />);
    fireEvent.click(await screen.findByLabelText("New workspace"));
    const name = screen.getByLabelText("New workspace's name");
    fireEvent.change(name, { target: { value: "Taken" } });
    fireEvent.click(screen.getByText("Add"));
    expect(await screen.findByText("a workspace by that name is there already")).toBeTruthy();
    fireEvent.change(name, { target: { value: "Acme Bakery" } });
    fireEvent.keyDown(name, { key: "Enter" });
    await waitFor(() => expect(made).toBe(true));
    expect(sent.filter((s) => s.route === "POST /api/workspaces").map((s) => s.body)).toEqual([{ name: "Taken" }, { name: "Acme Bakery" }]);
    await waitFor(() => expect(localStorage.getItem("openduct.workspace")).toBe("w9"));
  });
});

describe("pipelines", () => {
  const pipe = {
    id: "p1",
    name: "orders-clean",
    active: false,
    updated: 1,
    source: "name: orders-clean\nfrom:\n  topic: orders\nto:\n  topic: orders-clean\n",
    from: "orders",
    to: "orders-clean",
    error_topic: null,
    running: false,
    since: null,
    read: 0,
    written: 0,
    set_aside: 0,
    error: null,
  };

  it("lists a workspace's pipelines, checks a file as it is written, tries it, and turns it on", async () => {
    history.replaceState(null, "", "/pipelines");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/pipelines": { body: { pipelines: [pipe] } },
      "POST /api/pipelines/check": { body: { ok: true, name: "orders-clean", from: "orders", to: "orders-clean", error_topic: null } },
      "POST /api/pipelines/test": { body: { lines: [{ key: "A-1", value: { total: 10 } }, { line: 3, error: "map.total: a number" }] } },
      "POST /api/pipelines/p1/active": { body: { ...pipe, active: true, running: true, since: 1 } },
    });
    render(<App />);
    expect((await screen.findAllByText("orders-clean")).length).toBeGreaterThan(0);
    expect(await screen.findByText("✓ reads orders, writes orders-clean")).toBeTruthy();
    fireEvent.click(screen.getByText("Test"));
    expect(await screen.findByText("A-1")).toBeTruthy();
    expect(screen.getByText(/map.total: a number/)).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Turn on"));
    await waitFor(() => expect(sent.some((x) => x.route === "POST /api/pipelines/p1/active")).toBe(true));
    expect(sent.find((x) => x.route === "POST /api/pipelines/p1/active")?.body).toEqual({ active: true });
  });
});

describe("apps", () => {
  const shop = {
    id: "shop",
    name: "Shop",
    category: "commerce",
    description: "Customers and orders",
    docs: null,
    default_action: null,
    registered: false,
    sign_ins: [{ id: "shop_key", label: "An API key", connect: false }],
    actions: [
      { name: "orders", label: "List orders", description: "", write: false, list: true, inputs: [{ name: "status", label: "Status", required: false, description: "paid or open", kind: "text" }] },
      { name: "add_customer", label: "Add a customer", description: "", write: true, list: false, inputs: [{ name: "email", label: "Email", required: true, description: "", kind: "text" }] },
    ],
  };

  it("makes an app's step form from its definition: its credentials, its actions and their inputs", async () => {
    history.replaceState(null, "", "/workflows/w-shop");
    const defined = { ...workflow, id: "w-shop", definition: { name: "Shop", trigger: { webhook: {} }, steps: [{ name: "orders", shop: { credential: "", action: "orders", status: "paid" } }] } };
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [{ name: "my-shop", kind: "shop_key", fields: ["key"], updated: 1 }] } },
      "GET /api/apps": { body: { apps: [shop] } },
      "GET /api/workflows/w-shop": { body: defined },
    });
    render(<App />);
    fireEvent.click(await screen.findByText("orders"));
    const window = await screen.findByRole("dialog", { name: "orders" });
    await waitFor(() => expect(within(window).getByLabelText("Status (optional)")).toBeTruthy());
    expect((within(window).getByLabelText("Status (optional)") as HTMLInputElement).value).toBe("paid");
    expect(within(window).getByText("my-shop (shop_key)")).toBeTruthy();
    // Another action: its inputs.
    fireEvent.change(within(window).getByLabelText("Action"), { target: { value: "add_customer" } });
    expect(within(window).getByLabelText("Email")).toBeTruthy();
    expect(within(window).queryByLabelText("Status (optional)")).toBeNull();
  });

  it("connects an app signed in at its service, once the installation is registered with it", async () => {
    history.replaceState(null, "", "/credentials?connected=books");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": {
        body: {
          kinds: [
            { kind: "books", label: "Books: Sign in with Books", fields: [], app: "books", connect: true, registered: false },
            { kind: "sheets", label: "Sheets: Sign in with Sheets", fields: [], app: "sheets", connect: true, registered: true },
          ],
        },
      },
      "POST /api/oauth/start": { status: 409, body: { error: "not registered" } },
    });
    render(<App />);
    expect(await screen.findByText("Connected: books")).toBeTruthy();
    fireEvent.click(screen.getByText("Add a credential"));
    fireEvent.click(await screen.findByText(/Sign in with Books/));
    const window = screen.getByRole("dialog");
    expect(within(window).getByText(/not registered with the service yet/)).toBeTruthy();
    expect((within(window).getByRole("button", { name: "Connect" }) as HTMLButtonElement).disabled).toBe(true);
    expect(sent.some((x) => x.route === "POST /api/oauth/start")).toBe(false);
  });

  it("lets the installation's admin keep its registration with each service, and shows the address to give it", async () => {
    history.replaceState(null, "", "/settings");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false, version: "0.1.0" } },
      "GET /api/session": { body: session },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/workflows": { body: { workflows: [] } },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/files": { body: { files: [] } },
      "GET /api/oauth/apps": { body: { callback: "https://flows.example.com/oauth/callback", apps: [{ app: "books", name: "Books", sign_in: "books", docs: null, registered: false, client_id: null }] } },
      "PUT /api/oauth/apps/books": { body: { app: "books", registered: true } },
    });
    render(<App />);
    expect(await screen.findByText("https://flows.example.com/oauth/callback")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Books client id"), { target: { value: "cid-1" } });
    fireEvent.change(screen.getByLabelText("Books client secret"), { target: { value: "s3cret" } });
    fireEvent.click(screen.getByText("Keep"));
    await waitFor(() => expect(sent.some((x) => x.route === "PUT /api/oauth/apps/books")).toBe(true));
    expect(sent.find((x) => x.route === "PUT /api/oauth/apps/books")?.body).toEqual({ client_id: "cid-1", client_secret: "s3cret" });
  });
});

describe("settings", () => {
  it("shows the account, the workspace and what is in it, the installation's log, and the keys", async () => {
    history.replaceState(null, "", "/settings");
    stand_in({
      "GET /api/status": { body: { setup: false, version: "0.1.0" } },
      "GET /api/session": { body: { ...session, account: { ...session.account, admin: true } } },
      "GET /api/workflows": { body: { workflows: [{ id: "a", name: "A", active: true, trigger: "webhook", updated: 1 }, { id: "b", name: "B", active: false, trigger: "manual", updated: 1 }] } },
      "GET /api/credentials": { body: { credentials: [{ name: "model", kind: "ai", fields: [], updated: 1 }] } },
      "GET /api/files": { body: { files: [] } },
      "GET /api/workspaces/w1/members": { body: { members: [{ user: "u", email: "a@b.c", name: "Ada", role: "admin" }] } },
      "GET /api/logs": { body: { lines: [], kept: true, dir: "/data/logs" } },
    });
    render(<App />);
    expect(await screen.findByText("2, 1 on")).toBeTruthy();
    expect(screen.getByText("OpenDuct 0.1.0")).toBeTruthy();
    expect(await screen.findByText("/data/logs")).toBeTruthy();
    expect(screen.getByText("Find a page or a workflow")).toBeTruthy();
    expect(screen.getByRole("radiogroup", { name: "Theme" })).toBeTruthy();
  });
});

describe("runs and logs", () => {
  it("lists every workflow's runs, filters them by status, and opens one to its steps", async () => {
    history.replaceState(null, "", "/runs");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/runs": {
        body: {
          runs: [
            { run: "0-2", at: Date.now() - 5000, status: "failed", error: "tell: connection refused", ms: 12, trigger: { trigger: "webhook" }, workflow: { id: "a", name: "Big orders" }, steps: [{ name: "tell", kind: "http", attempts: 1, ms: 9, result: null, error: "connection refused" }] },
            { run: "0-1", at: Date.now() - 60000, status: "done", error: null, ms: 3, trigger: { trigger: "schedule" }, workflow: { id: "b", name: "Daily digest" }, steps: [{ name: "sum", kind: "set", attempts: 1, ms: 1, result: { n: 2 }, error: null }] },
          ],
        },
      },
    });
    render(<App />);
    expect(await screen.findByText("Big orders")).toBeTruthy();
    expect(screen.getByText("Daily digest")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /^Failed/ }));
    expect(screen.queryByText("Daily digest")).toBeNull();
    fireEvent.click(screen.getByText("tell: connection refused"));
    expect(await screen.findByText("tell")).toBeTruthy();
  });

  it("pages the runs, 25 at a time", async () => {
    history.replaceState(null, "", "/runs");
    const many = Array.from({ length: 30 }, (_, i) => ({
      run: `0-${i}`,
      at: Date.now() - i * 1000,
      status: "done",
      error: null,
      ms: 1,
      workflow: { id: "a", name: `Flow ${i}` },
      steps: [],
    }));
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/runs": { body: { runs: many } },
    });
    render(<App />);
    expect(await screen.findByText("1-25 of 30")).toBeTruthy();
    expect(screen.getByText("Flow 0")).toBeTruthy();
    expect(screen.queryByText("Flow 29")).toBeNull();
    fireEvent.click(screen.getByLabelText("Older runs"));
    expect(screen.getByText("26-30 of 30")).toBeTruthy();
    expect(screen.getByText("Flow 29")).toBeTruthy();
  });

  it("shows OpenDuct's log, line by line, to the installation's admin", async () => {
    history.replaceState(null, "", "/logs");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: { ...session, account: { ...session.account, admin: true } } },
      "GET /api/logs": {
        body: {
          kept: true,
          dir: "/data/logs",
          older: true,
          lines: [{ at: "1", ms: 1700000000000, level: "WARN", target: "openduct_studio::manager", text: "cannot set up the workflow's effects", fields: { workflow: "w1" }, node: "node-1" }],
        },
      },
    });
    render(<App />);
    expect(await screen.findByText("cannot set up the workflow's effects")).toBeTruthy();
    expect(screen.getByText("manager")).toBeTruthy();
    expect(screen.getByText("w1")).toBeTruthy();
    expect(screen.getByText("/data/logs")).toBeTruthy();
    // An older page: the lines before the oldest shown.
    fireEvent.click(screen.getByLabelText("Older lines"));
    await waitFor(() => expect(sent.some((s) => s.route.includes("before=1"))).toBe(true));
    // What a search finds is marked in the line, as typed, whatever its case.
    fireEvent.change(screen.getByLabelText("Search the log"), { target: { value: "SET UP" } });
    await waitFor(() => expect(document.querySelector(".log-view mark")?.textContent).toBe("set up"));
    fireEvent.change(screen.getByLabelText("Level"), { target: { value: "error" } });
    await waitFor(() => expect(sent.some((s) => s.route.startsWith("GET /api/logs") && String(s.route).includes("error"))).toBe(true));
  });
});

describe("the canvas", () => {
  const ran = {
    status: "failed",
    error: "connection refused",
    ms: 12,
    steps: [
      { name: "order", kind: "set", attempts: 1, ms: 1, result: { total: 150 }, error: null },
      { name: "big", kind: "if", attempts: 1, ms: 0, result: { branch: "then" }, error: null },
      { name: "tell", kind: "http", attempts: 3, ms: 9, result: { error: "connection refused" }, error: "connection refused" },
    ],
  };

  it("draws the workflow, and adds a step where it is asked for, found in the search window", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
      "PUT /api/workflows/big-orders": (body) => ({ body: { ...workflow, definition: (body as { definition: unknown }).definition } }),
    });
    render(<App />);
    expect(await screen.findByText("Starts when")).toBeTruthy();
    for (const name of ["order", "big", "tell", "each", "w", "keep"]) expect(screen.getByText(name)).toBeTruthy();
    expect(screen.getByText("POST https://x")).toBeTruthy();
    expect(screen.getByText("for each")).toBeTruthy();

    // The else branch is empty: a step goes there.
    fireEvent.click(screen.getByText("+ add a step"));
    const search = screen.getByLabelText("Search steps and apps");
    fireEvent.change(search, { target: { value: "notify channel" } });
    expect(screen.queryByText("Ask AI")).toBeNull();
    expect(screen.getByText("Post to a Slack channel")).toBeTruthy();
    fireEvent.change(search, { target: { value: "mail" } });
    fireEvent.keyDown(search, { key: "Enter" });
    // The search window closes, and the new step's settings open in their own window.
    expect(screen.queryByRole("dialog", { name: "Add a step" })).toBeNull();
    const settings = await screen.findByRole("dialog", { name: "email_1" });
    expect(settings.querySelector("input[value='email_1']")).toBeTruthy();
    expect(screen.getByText("Output")).toBeTruthy();
    fireEvent.keyDown(settings, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.getByText("email_1")).toBeTruthy();
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(sent.some((s) => s.route === "PUT /api/workflows/big-orders")).toBe(true));
    const saved = sent.find((s) => s.route === "PUT /api/workflows/big-orders")?.body as {
      definition: { steps: { if?: { else: { name: string }[] } }[] };
    };
    expect(saved.definition.steps[1]?.if?.else.map((s) => s.name)).toEqual(["email_1"]);
  });

  it("adds a switch whose cases stand on the canvas, each named, and whose form edits them", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
    });
    render(<App />);
    expect(await screen.findByText("Starts when")).toBeTruthy();
    fireEvent.click(screen.getByText("+ add a step"));
    const search = screen.getByLabelText("Search steps and apps");
    fireEvent.change(search, { target: { value: "router several ways" } });
    fireEvent.keyDown(search, { key: "Enter" });
    const settings = await screen.findByRole("dialog", { name: "switch_1" });
    expect(settings.querySelectorAll("input[aria-label='Case condition']")).toHaveLength(2);
    fireEvent.click(screen.getByText("+ Case"));
    expect(settings.querySelectorAll("input[aria-label='Case condition']")).toHaveLength(3);
    fireEvent.keyDown(settings, { key: "Escape" });
    // Each case, and the default, is a branch of its own on the canvas: each empty, ready for a step.
    expect(screen.getAllByText("+ add a step")).toHaveLength(4);
    expect(screen.getByText("3 cases and a default")).toBeTruthy();
  });

  it("keeps each page and workflow opened as a tab, a workflow's changes kept while another tab is open", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": { body: { kinds: [] } },
      "GET /api/workflows": { body: { workflows: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
    });
    render(<App />);
    expect(await screen.findByText("Starts when")).toBeTruthy();
    // Its own buttons are in its header, beside its views.
    expect(document.querySelector(".editor-head")?.textContent).toContain("Save");
    fireEvent.change(screen.getByLabelText("Workflow name"), { target: { value: "Bigger orders" } });
    const tabs = screen.getByRole("tablist", { name: "Open tabs" });
    expect(tabs.textContent).toContain("Bigger orders");
    expect(tabs.querySelector(".open-tab.dirty")).toBeTruthy();
    fireEvent.click(screen.getByRole("link", { name: "Credentials" }));
    expect(location.pathname).toBe("/credentials");
    expect(tabs.textContent).toContain("Credentials");
    // Back on its tab, the change not yet saved is still there.
    fireEvent.click(screen.getByText("Bigger orders"));
    expect(location.pathname).toBe("/workflows/big-orders");
    expect((screen.getByLabelText("Workflow name") as HTMLInputElement).value).toBe("Bigger orders");
    fireEvent.click(screen.getByLabelText("Close Bigger orders"));
    expect(location.pathname).toBe("/credentials");
    expect(tabs.textContent).not.toContain("Bigger orders");
  });

  it("saves from a window's own foot: what starts it changed, Save there, and it says so", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
      "PUT /api/workflows/big-orders": (body) => ({ body: { ...workflow, definition: (body as { definition: unknown }).definition } }),
    });
    render(<App />);
    fireEvent.click(await screen.findByText("Starts when"));
    const window = screen.getByRole("dialog", { name: "Starts when" });
    expect(within(window).getByText("All saved")).toBeTruthy();
    fireEvent.click(within(window).getByText("Topic"));
    expect(within(window).getByText("Not saved yet")).toBeTruthy();
    // One at a time, rather than the ten at once it does unless it says.
    const atOnce = within(window).getByRole("radiogroup", { name: "Runs at once" });
    expect(within(atOnce).getByRole("radio", { name: "10" }).getAttribute("aria-checked")).toBe("true");
    fireEvent.click(within(atOnce).getByRole("radio", { name: "1" }));
    expect(within(window).getByText(/One at a time/)).toBeTruthy();
    fireEvent.click(within(window).getByText("Save"));
    await waitFor(() => expect(sent.some((x) => x.route === "PUT /api/workflows/big-orders")).toBe(true));
    const saved = sent.find((x) => x.route === "PUT /api/workflows/big-orders")?.body as { definition: { trigger: Record<string, unknown>; runs_at_once?: number } };
    expect(Object.keys(saved.definition.trigger)).toEqual(["topic"]);
    expect(saved.definition.runs_at_once).toBe(1);
    expect(await within(window).findByText("Saved")).toBeTruthy();
  });

  it("saves a new name on Enter, and puts the saved one back on Escape", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/overview": { body: { workspaces: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
      "PUT /api/workflows/big-orders": (body) => ({ body: { ...workflow, definition: (body as { definition: unknown }).definition } }),
    });
    render(<App />);
    expect(await screen.findByText("Starts when")).toBeTruthy();
    const name = screen.getByLabelText("Workflow name") as HTMLInputElement;
    fireEvent.change(name, { target: { value: "Nothing to keep" } });
    fireEvent.keyDown(name, { key: "Escape" });
    expect(name.value).toBe(workflow.definition.name);
    fireEvent.change(name, { target: { value: "TestFlow" } });
    fireEvent.keyDown(name, { key: "Enter" });
    await waitFor(() => expect(sent.some((x) => x.route === "PUT /api/workflows/big-orders")).toBe(true));
    const saved = sent.find((x) => x.route === "PUT /api/workflows/big-orders")?.body as { definition: { name: string } };
    expect(saved.definition.name).toBe("TestFlow");
    // It says so, beside the name.
    expect(await screen.findByText("Renamed and saved")).toBeTruthy();
  });

  it("shows what a step can read beside its settings, puts a field where one writes, and says what is not there", async () => {
    const down = {
      ...workflow,
      id: "down",
      definition: {
        name: "Service down",
        trigger: { webhook: { token: "t0k" } },
        steps: [
          { name: "check", set: { total: "value.total * 2" } },
          { name: "keep", topic: { name: "status", key: "{{ value.service }}", value: "{{ value }}" } },
          { name: "only", where: "true" },
        ],
      },
    };
    history.replaceState(null, "", "/workflows/down");
    const sent = stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/down": { body: down },
      "POST /api/workflows/down/test": {
        body: { run: { status: "done", error: null, ms: 2, steps: [{ name: "check", kind: "set", attempts: 1, ms: 0, result: { total: 240 }, error: null }] } },
      },
      "PUT /api/workflows/down": (body) => ({ body: { ...down, definition: (body as { definition: unknown }).definition } }),
    });
    render(<App />);
    fireEvent.click(await screen.findByText("keep"));
    const dialog = screen.getByRole("dialog", { name: "keep" });
    const input = within(dialog).getByRole("region", { name: "Input" });
    // The sample input's fields, the earlier step not yet tested, and how a webhook starts it.
    for (const field of ["id", "total", "email", "method", "headers"]) expect(within(input).getByText(field)).toBeTruthy();
    expect(input.textContent).toContain("Test to see what it gives");
    // The key names a field the sample input does not have: said before a test runs it empty.
    expect(within(input).getByRole("list", { name: "Not in the input" }).textContent).toContain("value.service is not in the sample input");

    // A field clicked goes into the box being written in, as a template.
    const key = within(dialog).getByLabelText("Key") as HTMLInputElement;
    key.focus();
    key.setSelectionRange(0, key.value.length);
    fireEvent.click(within(input).getByText("email"));
    expect(key.value).toBe("{{ value.email }}");
    expect(within(input).queryByRole("list", { name: "Not in the input" })).toBeNull();

    // After a test, what the earlier step gave is there too; in a box that takes an expression it goes in bare.
    fireEvent.click(within(dialog).getByText("Test"));
    expect(await within(input).findByText("240")).toBeTruthy();
    fireEvent.click(within(dialog).getByLabelText("Close"));
    fireEvent.click(screen.getByText("only"));
    const only = screen.getByRole("dialog", { name: "only" });
    const when = within(only).getByLabelText(/Go on only when/) as HTMLInputElement;
    when.focus();
    when.setSelectionRange(0, when.value.length);
    fireEvent.click(within(within(only).getByRole("region", { name: "Input" })).getByTitle("Put steps.check.total where you are writing"));
    expect(when.value).toBe("steps.check.total");
    fireEvent.click(within(only).getByText("Save"));
    await waitFor(() => expect(sent.some((x) => x.route === "PUT /api/workflows/down")).toBe(true));
    const saved = sent.find((x) => x.route === "PUT /api/workflows/down")?.body as { definition: { steps: Record<string, unknown>[] } };
    expect((saved.definition.steps[1]?.topic as { key: string }).key).toBe("{{ value.email }}");
  });

  it("marks each step after a test and shows what each gave back", async () => {
    history.replaceState(null, "", "/workflows/big-orders");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/workflows/big-orders": { body: workflow },
      "POST /api/workflows/big-orders/test": { body: { run: ran } },
    });
    const { container } = render(<App />);
    await screen.findByText("Starts when");
    fireEvent.click(screen.getByText("Test"));
    expect(await screen.findByText(/It failed at tell/)).toBeTruthy();
    // The flow's nodes are marked a render after the message: wait for them as for it.
    await waitFor(() => expect(container.querySelectorAll(".flow-node.status-done").length).toBe(2));
    expect(container.querySelectorAll(".flow-node.status-failed").length).toBe(1);
    expect(container.querySelectorAll(".flow-node.status-unreached").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByText("order"));
    expect(await screen.findByText(/"total": 150/)).toBeTruthy();
    fireEvent.click(screen.getByText("tell"));
    expect(await screen.findByText("3 tries")).toBeTruthy();
    expect(screen.getAllByText("connection refused").length).toBeGreaterThan(0);
    fireEvent.click(screen.getByText("keep"));
    // Said in its output; its input says the same of an earlier step that did not run.
    expect((await screen.findAllByText(/did not run in the last test/)).some((e) => e.closest(".popup-output"))).toBe(true);
  });
});


describe("templates", () => {
  it("gives a workflow made from a template its own webhook token, never the template's", async () => {
    const { withOwnToken } = await import("./Workflows");
    const template = { name: "Bot", trigger: { webhook: { token: "" } }, steps: [] };
    const a = withOwnToken(template).trigger as { webhook: { token: string } };
    const b = withOwnToken(template).trigger as { webhook: { token: string } };
    expect(a.webhook.token).toMatch(/^[A-Za-z0-9_-]{32}$/);
    expect(a.webhook.token).not.toBe(b.webhook.token);
    // A token a definition already has is its own to keep.
    const kept = { name: "Mine", trigger: { webhook: { token: "t0k" } }, steps: [] };
    expect(withOwnToken(kept)).toBe(kept);
  });
});

describe("the side", () => {
  it("folds to icons and back, and finds a page or a workflow with Ctrl+K", async () => {
    history.replaceState(null, "", "/workflows");
    stand_in({
      "GET /api/status": { body: { setup: false } },
      "GET /api/session": { body: session },
      "GET /api/workflows": { body: { workflows: [{ id: "big-orders", name: "Big orders", active: true, trigger: "webhook", updated: 1 }] } },
      "GET /api/templates": { body: { templates: [] } },
      "GET /api/credentials": { body: { credentials: [] } },
      "GET /api/credential-kinds": { body: { kinds: [] } },
    });
    const { container } = render(<App />);
    await screen.findByText("1 in this workspace, 1 on.");
    fireEvent.click(screen.getByLabelText("Hide the side"));
    expect(container.querySelector("nav.side.folded")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Show the side"));
    expect(container.querySelector("nav.side.folded")).toBeNull();

    fireEvent.keyDown(window, { key: "k", ctrlKey: true });
    const find = screen.getByLabelText("Find", { selector: "input" });
    fireEvent.change(find, { target: { value: "cred" } });
    fireEvent.keyDown(find, { key: "Enter" });
    expect(location.pathname).toBe("/credentials");
    expect(await screen.findByText("Add a credential")).toBeTruthy();
  });
});
