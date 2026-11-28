// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The SQL steps and the database trigger in the builder: each written as the engine reads it.
// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { Definition, Step, WorkflowRecord } from "./api";
import { StepCard, fresh } from "./Steps";
import { TriggerSettings } from "./Trigger";
import { summaryOf, triggerSummary } from "./flow/look";

function card(step: Step) {
  const changed: Step[] = [];
  const noop = () => undefined;
  render(<StepCard step={step} taken={new Set()} onChange={(s) => changed.push(s)} onRemove={noop} onUp={noop} onDown={noop} />);
  return changed;
}

const record: WorkflowRecord = { id: "w1", name: "Orders", active: false, version: 1, definition: { name: "Orders" }, created: 0, updated: 0, hook: "/hooks/h1" };

afterEach(cleanup);

describe("the SQL steps", () => {
  it("start with SQL in each database's own placeholders", () => {
    expect(fresh("mysql", new Set())).toEqual({ name: "mysql_1", mysql: { credential: "", sql: "SELECT * FROM orders WHERE id = ?", params: ["value.id"] } });
    expect((fresh("sqlserver", new Set()).sqlserver as { sql: string }).sql).toBe("SELECT * FROM orders WHERE id = @p1");
  });

  it("write read only, several statements in a transaction, and the limits as the engine reads them", () => {
    const step = fresh("mysql", new Set());
    const changed = card(step);
    fireEvent.click(screen.getByLabelText(/Only reads/));
    expect((changed.at(-1)!.mysql as Record<string, unknown>).read_only).toBe(true);
    fireEvent.click(screen.getByText("Run several statements"));
    const several = changed.at(-1)!.mysql as Record<string, unknown>;
    expect(several.transaction).toBe(true);
    expect(several.sql).toBeUndefined();
    expect(several.statements).toEqual([{ sql: "SELECT * FROM orders WHERE id = ?", params: ["value.id"] }, { sql: "" }]);
    cleanup();
    const changed2 = card({ name: "q", sqlserver: { credential: "", sql: "SELECT 1" } });
    fireEvent.change(screen.getByPlaceholderText("1000"), { target: { value: "50" } });
    expect((changed2.at(-1)!.sqlserver as Record<string, unknown>).max_rows).toBe(50);
    expect(summaryOf("mysql", { name: "q", mysql: { statements: [{ sql: "a" }, { sql: "b" }], transaction: true } })).toBe("2 statements, in one transaction");
  });

  it("gives a postgres step the same limits beside its SQL", () => {
    const changed = card({ name: "q", postgres: { credential: "", sql: "SELECT 1" } });
    fireEvent.click(screen.getByLabelText(/Only reads/));
    expect(changed.at(-1)!.postgres).toEqual({ credential: "", sql: "SELECT 1", read_only: true });
  });
});

describe("the database trigger", () => {
  it("is chosen from the tiles and written as the engine reads it", () => {
    const changed: Definition[] = [];
    render(<TriggerSettings definition={{ name: "Orders", trigger: { manual: {} } }} record={record} onChange={(d) => changed.push(d)} />);
    fireEvent.click(screen.getByRole("radio", { name: /A database/ }));
    expect(changed.at(-1)?.trigger).toEqual({ mysql: { credential: "", table: "orders", cursor: "id", every: "1m" } });
    cleanup();
    const next: Definition[] = [];
    render(<TriggerSettings definition={{ name: "Orders", trigger: { mysql: { credential: "", table: "orders", cursor: "id" } } }} record={record} onChange={(d) => next.push(d)} />);
    expect(screen.getByRole("radio", { name: /A database/ }).getAttribute("aria-checked")).toBe("true");
    fireEvent.change(screen.getByLabelText("Database"), { target: { value: "sqlserver" } });
    expect(Object.keys(next.at(-1)!.trigger as object)).toEqual(["sqlserver"]);
    fireEvent.change(screen.getByPlaceholderText("status = 'paid'"), { target: { value: "status = 'paid'" } });
    expect((next.at(-1)!.trigger as Record<string, Record<string, unknown>>).mysql!.where).toBe("status = 'paid'");
    expect(triggerSummary({ mysql: { table: "orders", cursor: "id" } }).line).toBe("a row is added to orders");
    expect(triggerSummary({ sqlserver: { table: "invoices", cursor: "updated_at", key: "id" } }).line).toBe("a row is added or changed in invoices");
  });
});
