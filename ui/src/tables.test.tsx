// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The Tables page and the steps that keep data, against a stand-in API: what they show, and what clicks send.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Tables } from "./Tables";
import { StepCard } from "./Steps";
import type { Step } from "./api";

type Answer = { status?: number; body: unknown };
type Routes = Record<string, Answer | ((body: unknown) => Answer)>;

/** A fetch that answers from `routes` ("GET /api/tables") and keeps what was sent. */
function stand_in(routes: Routes) {
  const sent: { route: string; body: unknown }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    const route = `${init.method ?? "GET"} ${path}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    sent.push({ route, body });
    const found = routes[route] ?? routes[route.split("?")[0]!];
    const answer = typeof found === "function" ? found(body) : found ?? { status: 404, body: { error: "no such request" } };
    return new Response(JSON.stringify(answer.body), { status: answer.status ?? 200 });
  });
  return sent;
}

const limits = { tables: 100, columns: 50, rows: 50000, row_bytes: 65536, workspace_bytes: 52428800, state_bytes: 262144, rows_at_once: 10000, types: ["text", "number", "boolean", "datetime", "json"] };
const leads = {
  id: "t1",
  name: "leads",
  columns: [
    { name: "email", type: "text" },
    { name: "score", type: "number" },
  ],
  rows: 2,
  bytes: 300,
  created: 1,
  updated: 1,
};
const rows = {
  rows: [
    { id: 1, email: "ada@example.com", score: 12, created_at: "2026-11-27T09:00:00Z", updated_at: "2026-11-27T09:00:00Z" },
    { id: 2, email: "sam@example.com", score: null, created_at: "2026-11-27T09:00:00Z", updated_at: "2026-11-27T09:00:00Z" },
  ],
  matched: 2,
  total: 2,
  offset: 0,
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("tables", () => {
  it("lists a workspace's tables and what its workflows keep, and makes a table with typed columns", async () => {
    const sent = stand_in({
      "GET /api/tables": { body: { tables: [leads], limits } },
      "GET /api/workflow-state": { body: { states: [{ workflow: "wf1", name: "Seen", bytes: 40, updated: 1, values: { last_id: "A-7" } }] } },
      "POST /api/tables": (body) => ({ status: 201, body: { ...leads, id: "t2", name: (body as { name: string }).name, rows: 0 } }),
      "GET /api/tables/visits/rows": { body: { rows: [], matched: 0, total: 0, offset: 0 } },
    });
    render(<Tables canEdit />);
    expect(await screen.findByText("leads")).toBeTruthy();
    expect(screen.getByText("email, score")).toBeTruthy();
    expect(await screen.findByText("Seen")).toBeTruthy();
    fireEvent.click(screen.getByText("Show"));
    expect(screen.getByText(/"last_id": "A-7"/)).toBeTruthy();
    fireEvent.click(screen.getByText("New table"));
    fireEvent.change(screen.getByPlaceholderText("leads"), { target: { value: "visits" } });
    fireEvent.click(screen.getByText("+ Column"));
    const names = screen.getAllByLabelText("Column name");
    fireEvent.change(names[1]!, { target: { value: "count" } });
    fireEvent.change(screen.getByLabelText("Type of count"), { target: { value: "number" } });
    fireEvent.click(screen.getByText("Make the table"));
    await waitFor(() => expect(sent.some((s) => s.route === "POST /api/tables")).toBe(true));
    expect(sent.find((s) => s.route === "POST /api/tables")!.body).toEqual({
      name: "visits",
      columns: [
        { name: "name", type: "text" },
        { name: "count", type: "number" },
      ],
    });
  });

  it("opens a table to its rows: sorts them, filters them, edits a cell in place, and adds and deletes rows", async () => {
    const sent = stand_in({
      "GET /api/tables": { body: { tables: [leads], limits } },
      "GET /api/workflow-state": { body: { states: [] } },
      "GET /api/tables/leads/rows": { body: rows },
      "PUT /api/tables/leads/rows/2": { body: rows.rows[1] },
      "POST /api/tables/leads/rows": { body: { added: 1 } },
      "POST /api/tables/leads/rows/delete": { body: { deleted: 1 } },
    });
    render(<Tables canEdit />);
    fireEvent.click(await screen.findByText("leads"));
    expect(await screen.findByText("ada@example.com")).toBeTruthy();
    fireEvent.click(screen.getByText(/^score/));
    await waitFor(() => expect(sent.some((s) => s.route.includes("rows?offset=0&limit=50&sort=score"))).toBe(true));
    const filter = screen.getByLabelText("Only the rows where");
    fireEvent.change(filter, { target: { value: "row.score > 10" } });
    fireEvent.keyDown(filter, { key: "Enter" });
    await waitFor(() => expect(sent.some((s) => s.route.includes("where=row.score+%3E+10"))).toBe(true));
    // An empty number cell, given a value: sent as text, read by the table as its number.
    const cells = document.querySelectorAll("td.cell");
    fireEvent.click(cells[3]!);
    const input = screen.getByLabelText("The cell's value");
    fireEvent.change(input, { target: { value: "7" } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() => expect(sent.some((s) => s.route === "PUT /api/tables/leads/rows/2")).toBe(true));
    expect(sent.find((s) => s.route === "PUT /api/tables/leads/rows/2")!.body).toEqual({ values: { score: "7" } });
    fireEvent.click(screen.getByText("Add a row"));
    fireEvent.change(screen.getByLabelText("New row's email"), { target: { value: "kim@example.com" } });
    fireEvent.click(screen.getByText("Add"));
    await waitFor(() => expect(sent.some((s) => s.route === "POST /api/tables/leads/rows")).toBe(true));
    expect(sent.find((s) => s.route === "POST /api/tables/leads/rows")!.body).toEqual({ rows: [{ email: "kim@example.com", score: null }] });
    fireEvent.click(screen.getByLabelText("Pick row 1"));
    fireEvent.click(screen.getByText("Delete 1 row"));
    await waitFor(() => expect(sent.some((s) => s.route === "POST /api/tables/leads/rows/delete")).toBe(true));
    expect(sent.find((s) => s.route === "POST /api/tables/leads/rows/delete")!.body).toEqual({ ids: [1] });
  });

  it("shows a viewer the rows, with nothing to change them", async () => {
    stand_in({
      "GET /api/tables": { body: { tables: [leads], limits } },
      "GET /api/workflow-state": { body: { states: [] } },
      "GET /api/tables/leads/rows": { body: rows },
    });
    render(<Tables canEdit={false} />);
    fireEvent.click(await screen.findByText("leads"));
    expect(await screen.findByText("ada@example.com")).toBeTruthy();
    expect(screen.queryByText("Add a row")).toBeNull();
    expect(screen.queryByText("Columns")).toBeNull();
    expect(screen.getByText("Export CSV")).toBeTruthy();
  });
});

function card(step: Step) {
  const changed: Step[] = [];
  const noop = () => undefined;
  render(<StepCard step={step} taken={new Set()} onChange={(s) => changed.push(s)} onRemove={noop} onUp={noop} onDown={noop} />);
  return changed;
}

describe("the steps that keep data", () => {
  it("writes a table step as the engine reads it: its table, what it does, which rows", async () => {
    stand_in({ "GET /api/tables": { body: { tables: [leads], limits } } });
    const changed = card({ name: "find", table: { name: "", action: "get", where: { email: "{{ value.email }}" }, limit: 10 } });
    expect(await screen.findByText("leads")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Table"), { target: { value: "leads" } });
    expect(changed.at(-1)!.table).toMatchObject({ name: "leads", action: "get" });
    fireEvent.change(screen.getByLabelText("Do"), { target: { value: "update" } });
    expect(changed.at(-1)!.table).toEqual({ name: "", action: "update", where: { email: "{{ value.email }}" }, set: {} });
  });

  it("chooses comparisons for which rows", async () => {
    stand_in({ "GET /api/tables": { body: { tables: [leads], limits } } });
    const changed = card({ name: "few", table: { name: "leads", action: "count" } });
    fireEvent.change(await screen.findByLabelText("Which rows"), { target: { value: "tests" } });
    expect(changed.at(-1)!.table).toEqual({ name: "leads", action: "count", where: [{ column: "email", op: "=", value: "" }] });
  });

  it("keeps values between runs with a state step, and gives a script the state when asked", async () => {
    stand_in({});
    const changed = card({ name: "seen", state: {} });
    fireEvent.change(screen.getByLabelText("Do"), { target: { value: "set" } });
    expect(changed.at(-1)!.state).toEqual({ set: { last_seen: "{{ value.id }}" } });
    cleanup();
    const script = card({ name: "count", javascript: "state.n = (state.n || 0) + 1;" });
    fireEvent.click(screen.getByLabelText(/Give it the workflow's state/));
    expect(script.at(-1)!.javascript).toEqual({ code: "state.n = (state.n || 0) + 1;", state: true });
  });
});
