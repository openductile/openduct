// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The Tables page's calls: a workspace's data tables, their rows, CSV in and out, and its workflows' state.

import { call } from "./api";

export type ColumnType = "text" | "number" | "boolean" | "datetime" | "json";
export type TableColumn = { name: string; type: ColumnType };
export type TableSummary = { id: string; name: string; columns: TableColumn[]; rows: number; bytes: number; created: number; updated: number };
export type TableLimits = {
  tables: number;
  columns: number;
  rows: number;
  row_bytes: number;
  workspace_bytes: number;
  state_bytes: number;
  rows_at_once: number;
  types: ColumnType[];
};
export type TableRow = Record<string, unknown> & { id: number; created_at: string; updated_at: string };
export type RowPage = { rows: TableRow[]; matched: number; total: number; offset: number };
export type WorkflowState = { workflow: string; name: string | null; bytes: number; updated: number; values: Record<string, unknown> };
/** A column as the page edits it: `from`, the name it had, when it is renamed. */
export type ColumnEdit = TableColumn & { from?: string };

const at = (name: string) => `/api/tables/${encodeURIComponent(name)}`;

export const tablesApi = {
  list: () => call<{ tables: TableSummary[]; limits: TableLimits }>("GET", "/api/tables"),
  create: (name: string, columns: TableColumn[]) => call<TableSummary>("POST", "/api/tables", { name, columns }),
  createFromCsv: (name: string, csv: string) => call<TableSummary>("POST", "/api/tables", { name, csv }),
  change: (name: string, next: { name?: string; columns: ColumnEdit[] }) => call<TableSummary>("PUT", at(name), next),
  remove: (name: string) => call("DELETE", at(name)),
  rows: (name: string, asked: { offset: number; limit: number; sort?: string; desc?: boolean; where?: string }) => {
    const query = new URLSearchParams({ offset: String(asked.offset), limit: String(asked.limit) });
    if (asked.sort) query.set("sort", asked.sort);
    if (asked.desc) query.set("desc", "1");
    if (asked.where) query.set("where", asked.where);
    return call<RowPage>("GET", `${at(name)}/rows?${query}`);
  },
  addRows: (name: string, rows: Record<string, unknown>[]) => call<{ added: number }>("POST", `${at(name)}/rows`, { rows }),
  editRow: (name: string, id: number, values: Record<string, unknown>) => call<TableRow>("PUT", `${at(name)}/rows/${id}`, { values }),
  removeRows: (name: string, ids: number[]) => call<{ deleted: number }>("POST", `${at(name)}/rows/delete`, { ids }),
  importCsv: (name: string, csv: string) => call<{ added: number }>("POST", `${at(name)}/import`, { csv }),
  exportCsv: (name: string) => call<{ name: string; csv: string }>("GET", `${at(name)}/export?as=json`),
  states: async () => (await call<{ states: WorkflowState[] }>("GET", "/api/workflow-state")).states,
  clearState: (workflow: string) => call("DELETE", `/api/workflow-state/${encodeURIComponent(workflow)}`),
};
