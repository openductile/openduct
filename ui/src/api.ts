// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The studio's API, as the page calls it. A change carries the session's token; the server refuses one without.

export type Account = { id: string; email: string; name: string; admin: boolean };
export type Role = "admin" | "editor" | "viewer";
export type WorkspaceSummary = { id: string; name: string; role: Role };
export type Session = { account: Account; workspaces: WorkspaceSummary[]; csrf: string };
export type Status = { setup: boolean; version: string };

/** A workflow as the studio keeps it: `definition` is what the editor edits. */
export type Definition = {
  name: string;
  description?: string;
  category?: string;
  trigger?: unknown;
  steps?: Step[];
  runs_at_once?: number;
  /** The event a test or a preview runs on, kept with the workflow. */
  sample?: unknown;
};
export type Step = Record<string, unknown> & { name?: string };
export type WorkflowRecord = {
  id: string;
  name: string;
  active: boolean;
  version: number;
  definition: Definition;
  created: number;
  updated: number;
  hook?: string | null;
};
export type VersionRecord = { number: number; hash: string; created: number; by: string; definition: Definition };
export type Metrics = {
  runs: number;
  by_status: Record<string, number>;
  success_percent: number | null;
  duration_ms: { median: number; p95: number; max: number };
  tokens: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  steps: { name: string; runs: number; average_ms: number; failures: number; tokens: number }[];
};
export type CredentialSummary = { name: string; kind: string; fields: string[]; updated: number };
export type CredentialKind = {
  kind: string;
  label: string;
  fields: { name: string; optional: boolean; secret: boolean }[];
  /** The app it is a sign-in of, and whether it is connected by signing in at the vendor (and the install is registered). */
  app?: string | null;
  connect?: boolean;
  registered?: boolean;
};
export type AppInput = { name: string; label: string; required: boolean; description: string; kind: string };
export type AppAction = { name: string; label: string; description: string; write: boolean; list: boolean; inputs: AppInput[] };
/** An app workflows may use, as its definition says (apps/<id>/app.yaml). */
export type AppInfo = {
  id: string;
  name: string;
  category: string;
  description: string;
  docs: string | null;
  default_action: string | null;
  sign_ins: { id: string; label: string; connect: boolean }[];
  actions: AppAction[];
  registered: boolean;
};
/** An app a person signs in to at the vendor, and the install's registration with it. */
export type OauthApp = { app: string; name: string; sign_in: string; sign_ins?: string[]; docs: string | null; registered: boolean; client_id: string | null };
export type FileSummary = { path: string; content_type: string; size: number; sha256: string; updated: number };
export type FilePreview = { rows: number; columns: string[]; first: Record<string, unknown>[] };
export type Member = { user: string; email: string; name: string; role: Role };
export type TriggerSummary = { workflow: string; name: string | null; kind: string; hook: string | null; every_ms: number | null; topic: string | null };
export type WorkflowSummary = { id: string; name: string; active: boolean; trigger: string; updated: number };
export type StepRun = {
  name: string;
  kind: string;
  attempts: number;
  ms: number;
  result: unknown;
  error: string | null;
  /** What a script step logged. */
  logs?: string[];
};
export type Run = {
  run?: string;
  at?: number;
  status: "done" | "skipped" | "failed";
  error: string | null;
  ms: number;
  steps: StepRun[];
  event?: unknown;
  trigger?: Record<string, unknown>;
};
/** A run on the Runs page: with the workflow it is of. */
export type WorkspaceRun = Run & { workflow: { id: string; name: string } };
/** A record of OpenDuct's own log. */
export type LogLine = { at: string; ms: number; level: string; target: string; text: string; fields: Record<string, unknown>; node: unknown };
export type LogPage = { lines: LogLine[]; kept: boolean; dir?: string; why?: string; older?: boolean };
/** Something that needs someone, and where it is fixed. */
export type Alert = { key: string; level: "critical" | "warning" | "info"; title: string; detail: string; action: { label: string; path: string } };
export type Health = "green" | "amber" | "red";
export type Range = "1d" | "7d" | "30d";
export type Dashboard = {
  range: Range;
  from: number;
  bucket_ms: number;
  health: Health;
  workspace: { id: string; name: string };
  workflows: { total: number; on: number; never_ran: number };
  credentials: number;
  runs: { total: number; done: number; failed: number; skipped: number; buckets: { done: number; failed: number; skipped: number }[] };
  success_percent: number | null;
  waiting: number;
  per_workflow: { id: string; name: string; active: boolean; runs: number; failed: number; waiting: number; last_at: number | null; last_status: string | null }[];
  recent: WorkspaceRun[];
  alerts: Alert[];
};
export type ClientSummary = {
  id: string;
  name: string;
  role: Role;
  health: Health;
  workflows: { total: number; on: number; never_ran: number };
  runs: { total: number; failed: number };
  success_percent: number | null;
  waiting: number;
  alerts: Alert[];
  last_failure: number | null;
};
/** A pipeline of the workspace's, with how its run on this node is going. */
export type PipelineSummary = {
  id: string;
  name: string;
  active: boolean;
  updated: number;
  source: string;
  from: string;
  to: string;
  error_topic: string | null;
  running: boolean;
  since: number | null;
  read: number;
  written: number;
  set_aside: number;
  error: string | null;
};
export type PipelineCheck = { ok: boolean; name: string; from: string; to: string; error_topic: string | null; missing?: string[] };
export type AlertChannel = { kind: string; credential: string; target: string; on: boolean };
export type SetAside = { run: string | null; error: string | null; event: unknown };
export type Template = { id: string; name: string; description: string; category: string; definition: Definition };

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

let csrf = "";
/** The workspace every request is for: the one the studio shows. */
let workspace = "";

export function setWorkspace(id: string) {
  workspace = id;
}

async function call<T>(method: "GET" | "POST" | "PUT" | "DELETE", path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = {};
  if (workspace) headers["x-openduct-workspace"] = workspace;
  if (method !== "GET") {
    headers["content-type"] = "application/json";
    if (csrf) headers["x-openduct-csrf"] = csrf;
  }
  let response: Response;
  try {
    response = await fetch(path, {
      method,
      headers,
      body: method === "GET" ? undefined : JSON.stringify(body ?? {}),
      credentials: "same-origin",
    });
  } catch {
    // No answer at all: the studio is restarting, stopped, or out of reach - not a refusal.
    throw new Error("the studio did not answer: try again in a moment");
  }
  const data: unknown = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = (data as { error?: string }).error;
    throw new ApiError(response.status, error ?? `the studio answered ${response.status}`);
  }
  return data as T;
}

function signedIn(session: Session): Session {
  csrf = session.csrf;
  return session;
}

export const api = {
  status: () => call<Status>("GET", "/api/status"),
  session: async (): Promise<Session | null> => {
    try {
      return signedIn(await call<Session>("GET", "/api/session"));
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) return null;
      throw e;
    }
  },
  setup: async (name: string, email: string, password: string) =>
    signedIn(await call<Session>("POST", "/api/setup", { name, email, password })),
  signIn: async (email: string, password: string) =>
    signedIn(await call<Session>("POST", "/api/sign-in", { email, password })),
  signOut: async () => {
    await call("POST", "/api/sign-out");
    csrf = "";
  },
  workflows: async () => (await call<{ workflows: WorkflowSummary[] }>("GET", "/api/workflows")).workflows,
  workflow: (id: string) => call<WorkflowRecord>("GET", `/api/workflows/${encodeURIComponent(id)}`),
  create: (definition: Definition) => call<WorkflowRecord>("POST", "/api/workflows", { definition }),
  save: (id: string, definition: Definition) =>
    call<WorkflowRecord>("PUT", `/api/workflows/${encodeURIComponent(id)}`, { definition }),
  remove: (id: string) => call("DELETE", `/api/workflows/${encodeURIComponent(id)}`),
  setActive: (id: string, active: boolean) =>
    call<WorkflowRecord>("POST", `/api/workflows/${encodeURIComponent(id)}/active`, { active }),
  test: async (id: string, definition: Definition, input: unknown) =>
    (await call<{ run: Run }>("POST", `/api/workflows/${encodeURIComponent(id)}/test`, { definition, input })).run,
  runNow: (id: string, input: unknown) => call("POST", `/api/workflows/${encodeURIComponent(id)}/run`, { input }),
  dashboard: (range: Range) => call<Dashboard>("GET", `/api/dashboard?range=${range}`),
  overview: async (range: Range) => (await call<{ workspaces: ClientSummary[] }>("GET", `/api/overview?range=${range}`)).workspaces,
  alertChannels: async (id: string) => (await call<{ channels: AlertChannel[] }>("GET", `/api/workspaces/${encodeURIComponent(id)}/alerts`)).channels,
  saveAlertChannels: async (id: string, channels: AlertChannel[]) =>
    (await call<{ channels: AlertChannel[] }>("PUT", `/api/workspaces/${encodeURIComponent(id)}/alerts`, { channels })).channels,
  testAlert: (id: string, channel: AlertChannel) => call("POST", `/api/workspaces/${encodeURIComponent(id)}/alerts/test`, channel),
  pipelines: async () => (await call<{ pipelines: PipelineSummary[] }>("GET", "/api/pipelines")).pipelines,
  pipeline: (id: string) => call<PipelineSummary>("GET", `/api/pipelines/${encodeURIComponent(id)}`),
  addPipeline: (source: string) => call<PipelineSummary>("POST", "/api/pipelines", { source }),
  savePipeline: (id: string, source: string) => call<PipelineSummary>("PUT", `/api/pipelines/${encodeURIComponent(id)}`, { source }),
  removePipeline: (id: string) => call("DELETE", `/api/pipelines/${encodeURIComponent(id)}`),
  setPipelineActive: (id: string, active: boolean) => call<PipelineSummary>("POST", `/api/pipelines/${encodeURIComponent(id)}/active`, { active }),
  checkPipeline: (source: string) => call<PipelineCheck>("POST", "/api/pipelines/check", { source }),
  testPipeline: async (source: string, input: string) => (await call<{ lines: unknown[] }>("POST", "/api/pipelines/test", { source, input })).lines,
  allRuns: async () => (await call<{ runs: WorkspaceRun[] }>("GET", "/api/runs")).runs,
  logs: (asked: { level?: string; q?: string; after?: string; before?: string; limit?: number }) => {
    const query = new URLSearchParams();
    if (asked.level) query.set("level", asked.level);
    if (asked.q) query.set("q", asked.q);
    if (asked.after) query.set("after", asked.after);
    if (asked.before) query.set("before", asked.before);
    if (asked.limit) query.set("limit", String(asked.limit));
    return call<LogPage>("GET", `/api/logs?${query}`);
  },
  runs: async (id: string) => (await call<{ runs: Run[] }>("GET", `/api/workflows/${encodeURIComponent(id)}/runs`)).runs,
  failed: async (id: string) =>
    (await call<{ failed: SetAside[] }>("GET", `/api/workflows/${encodeURIComponent(id)}/failed`)).failed,
  runFailed: (id: string) =>
    call<{ queued: number }>("POST", `/api/workflows/${encodeURIComponent(id)}/failed/run`, {}),
  templates: async () => (await call<{ templates: Template[] }>("GET", "/api/templates")).templates,
  preview: async (id: string, definition: Definition, input: unknown, fromLast: boolean) =>
    call<{ run: Run; input: unknown }>(
      "POST",
      `/api/workflows/${encodeURIComponent(id)}/preview`,
      fromLast ? { definition, from: "last" } : { definition, input },
    ),
  metrics: async (id: string) =>
    (await call<{ metrics: Metrics }>("GET", `/api/workflows/${encodeURIComponent(id)}/metrics`)).metrics,
  versions: async (id: string) =>
    (await call<{ versions: VersionRecord[] }>("GET", `/api/workflows/${encodeURIComponent(id)}/versions`)).versions,
  triggers: async () => (await call<{ triggers: TriggerSummary[] }>("GET", "/api/triggers")).triggers,
  models: async (credential: string) =>
    (await call<{ models: { id: string; name: string | null }[] }>("GET", `/api/credentials/${encodeURIComponent(credential)}/models`)).models,
  credentials: async () => (await call<{ credentials: CredentialSummary[] }>("GET", "/api/credentials")).credentials,
  apps: async () => (await call<{ apps: AppInfo[] }>("GET", "/api/apps")).apps,
  oauthApps: () => call<{ apps: OauthApp[]; callback: string }>("GET", "/api/oauth/apps"),
  registerOauth: (app: string, client_id: string, client_secret: string) =>
    call("PUT", `/api/oauth/apps/${encodeURIComponent(app)}`, client_secret ? { client_id, client_secret } : { client_id }),
  unregisterOauth: (app: string) => call("DELETE", `/api/oauth/apps/${encodeURIComponent(app)}`),
  oauthStart: async (sign_in: string, name: string) => (await call<{ url: string }>("POST", "/api/oauth/start", { sign_in, name })).url,
  credentialKinds: async () => (await call<{ kinds: CredentialKind[] }>("GET", "/api/credential-kinds")).kinds,
  addCredential: (name: string, kind: string, fields: Record<string, string>) =>
    call<CredentialSummary>("POST", "/api/credentials", { name, kind, fields }),
  changeCredential: (name: string, fields: Record<string, string>) =>
    call<CredentialSummary>("PUT", `/api/credentials/${encodeURIComponent(name)}`, { fields }),
  removeCredential: (name: string) => call("DELETE", `/api/credentials/${encodeURIComponent(name)}`),
  files: async () => (await call<{ files: FileSummary[] }>("GET", "/api/files")).files,
  uploadFile: (path: string, content: { text?: string; base64?: string }, contentType = "") =>
    call<FileSummary>("POST", "/api/files", {
      path,
      content: content.text,
      content_base64: content.base64,
      content_type: contentType,
    }),
  filePreview: (path: string) => call<FilePreview>("GET", `/api/files/preview?path=${encodeURIComponent(path)}`),
  removeFile: (path: string) => call("DELETE", `/api/files?path=${encodeURIComponent(path)}`),
  workspaces: async () => (await call<{ workspaces: WorkspaceSummary[] }>("GET", "/api/workspaces")).workspaces,
  addWorkspace: (name: string) => call<WorkspaceSummary>("POST", "/api/workspaces", { name }),
  members: async (id: string) =>
    (await call<{ members: Member[] }>("GET", `/api/workspaces/${encodeURIComponent(id)}/members`)).members,
  addMember: (id: string, member: { email: string; name: string; password: string; role: Role }) =>
    call<Member>("POST", `/api/workspaces/${encodeURIComponent(id)}/members`, member),
  setRole: (id: string, user: string, role: Role) =>
    call("PUT", `/api/workspaces/${encodeURIComponent(id)}/members/${encodeURIComponent(user)}`, { role }),
  changePassword: (password: string, newPassword: string) => call("POST", "/api/password", { password, new_password: newPassword }),
  renameWorkspace: (id: string, name: string) => call("PUT", `/api/workspaces/${encodeURIComponent(id)}`, { name }),
  removeMember: (id: string, user: string) =>
    call("DELETE", `/api/workspaces/${encodeURIComponent(id)}/members/${encodeURIComponent(user)}`),
};
