// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The Apps page's calls: a workspace's own apps, made from an API description or an MCP server, previewed, saved,
// changed and removed.

import { call } from "./api";

export type CustomAppSummary = { id: string; name: string; actions: number; score: number | null; source: string; updated: number };
export type Quality = {
  score: number;
  bar: number;
  meets: boolean;
  actions: number;
  described: number;
  with_output: number;
  recorded: number;
  refusal: boolean;
  sign_ins: { id: string; kind: string }[];
  hosts: string[];
  missing: string[];
  failing?: string[];
};
export type Proposed = { name: string; label: string; method: string; path: string; score: number; chosen: boolean; left_out: string | null };
export type ProposedSignIn = { id: string; label: string; kind: string; chosen: boolean; allowed: boolean };
export type Made = {
  id: string;
  name: string;
  app_yaml: string;
  readme: string;
  tests: { file: string; text: string }[];
  proposed: Proposed[];
  sign_ins: ProposedSignIn[];
  not_done: string[];
};
export type Preview = { made: Made; quality: Quality | null; problems: string[]; saved: boolean };
export type CustomApp = {
  id: string;
  name: string;
  definition: string;
  readme: string;
  tests: { file: string; text: string }[];
  source: string;
  created: number;
  updated: number;
  quality: Quality | null;
};
/** Where an app is made from, and with what choices. */
export type Asked = {
  from: "openapi" | "mcp" | "definition";
  name?: string;
  title?: string;
  url?: string;
  text?: string;
  file?: string;
  token?: string;
  chosen?: string[];
  sign_ins?: string[];
  sign_in?: string;
  save?: boolean;
};

const at = (id: string) => `/api/apps/custom/${encodeURIComponent(id)}`;

export const customAppsApi = {
  list: () => call<{ apps: CustomAppSummary[]; most: number }>("GET", "/api/apps/custom"),
  one: (id: string) => call<CustomApp>("GET", at(id)),
  preview: (asked: Asked) => call<Preview>("POST", "/api/apps/custom", { ...asked, save: false }),
  save: (asked: Asked) => call<{ app: CustomApp; saved: boolean }>("POST", "/api/apps/custom", { ...asked, save: true }),
  change: (id: string, definition: string) => call<{ app: CustomApp }>("PUT", at(id), { definition }),
  remove: (id: string) => call("DELETE", at(id)),
};
