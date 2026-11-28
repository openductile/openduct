// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// CI/CD's calls: the workspace's API tokens, its place among the environments and its Git sync, and promotion.

import { call, type Role } from "./api";

export type ApiToken = {
  id: string;
  name: string;
  hint: string;
  role: Role;
  workflows: string[] | null;
  per_minute: number;
  created: number;
  expires: number;
  by: string;
  expired: boolean;
  /** Only in the answer that made it: shown once. */
  token?: string;
};
export type Environment = "development" | "test" | "production";
export type GitSettings = {
  provider: "github" | "gitlab" | "gitea";
  url: string;
  branch: string;
  folder: string;
  credential: string;
  changes: "commit" | "pull_request";
  hook?: string;
  secret?: boolean;
  signing_token?: boolean | string;
};
export type Delivery = {
  environment: Environment | null;
  order: number;
  locked: boolean;
  git: GitSettings | null;
  last_deploy: { at: number; commit: string; ok: boolean; error: string | null; by: string } | null;
  manage: boolean;
};
export type EnvironmentWorkspace = { id: string; name: string; environment: Environment; order: number; role: Role; here: boolean };
/** Where a version came from: the studio's editor, or a push, a promotion, a rollback, Git. */
export type VersionSource = { source: "studio" | "push" | "promotion" | "rollback" | "git"; token?: string | null; by?: string; detail?: string | null };
export type PlanItem = { name: string; action: "create" | "update" | "unchanged" | "turn_off"; from_version?: number; to_version?: number; was_on: boolean; on: boolean };
export type PromoteAnswer = { workflows: PlanItem[]; summary: Record<string, number | boolean>; dry_run: boolean };

export const deliveryApi = {
  tokens: async () => (await call<{ tokens: ApiToken[]; default_days: number; max_days: number }>("GET", "/api/tokens")).tokens,
  makeToken: (asked: { name: string; role: Role; days: number; workflows: string[] | null; per_minute?: number }) => call<ApiToken>("POST", "/api/tokens", asked),
  revokeToken: (id: string) => call("DELETE", `/api/tokens/${encodeURIComponent(id)}`),
  settings: () => call<Delivery>("GET", "/api/cicd/settings"),
  saveSettings: (changes: { environment?: Environment | null; order?: number; locked?: boolean; git?: Partial<GitSettings> | null }) =>
    call<Delivery>("PUT", "/api/cicd/settings", changes),
  newSecret: () => call<{ secret: string; hook: string }>("POST", "/api/cicd/git/secret", {}),
  saveToGit: (message: string) =>
    call<{ saved: number; unchanged: number; commit: string | null; pull_request?: string | null }>("POST", "/api/cicd/git/save", message ? { message } : {}),
  deployFromGit: () => call<PromoteAnswer & { at: string }>("POST", "/api/cicd/git/deploy", {}),
  environments: async () => (await call<{ environments: EnvironmentWorkspace[] }>("GET", "/api/cicd/environments")).environments,
  promote: (to: string, workflows: string[], dryRun: boolean) =>
    call<PromoteAnswer>("POST", "/api/cicd/promote", { to, workflows, dry_run: dryRun }),
};
