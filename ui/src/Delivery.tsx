// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// CI/CD on the settings page: the workspace's API tokens (each with a role, an expiry and optional workflows, shown
// once and revoked with a click), its place among the environments with the switch that keeps people's edits out
// of production, and its Git sync - the repository, Save to Git, Deploy from Git, and the webhook a push calls.

import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { api, type CredentialSummary, type Role } from "./api";
import { deliveryApi, type ApiToken, type Delivery, type Environment, type GitSettings } from "./deliveryApi";
import { Sym } from "./Sym";
import { Copy } from "./Trigger";

function Row(props: { name: string; about?: string; children: ReactNode }) {
  return (
    <div className="set-row">
      <div className="set-name">
        <span>{props.name}</span>
        {props.about && <span className="hint">{props.about}</span>}
      </div>
      <div className="set-control">{props.children}</div>
    </div>
  );
}

const day = (secs: number) => new Date(secs * 1000).toLocaleDateString();

/** The workspace's API tokens: for CI and other programs, made by its admins. */
export function ApiTokens(props: { workspace: string; role: Role }) {
  const [tokens, setTokens] = useState<ApiToken[] | null>(null);
  const [made, setMade] = useState<ApiToken | null>(null);
  const [trouble, setTrouble] = useState("");
  const load = () =>
    deliveryApi
      .tokens()
      .then(setTokens)
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    setTokens(null);
    setMade(null);
    load();
  }, [props.workspace]);
  if (!tokens) return <p className="hint set-loading">{trouble || "…"}</p>;
  return (
    <div className="api-tokens">
      <Row name="Tokens" about="Sent as Authorization: Bearer odapi_… by the command line and CI">
        {tokens.length === 0 ? (
          <span className="set-value">None yet</span>
        ) : (
          <ul className="mcp-tokens" aria-label="API tokens">
            {tokens.map((t) => (
              <li key={t.id}>
                <span>{t.name}</span> <code className="hint">…{t.hint}</code>{" "}
                <span className="hint">
                  {t.role}, {t.workflows ? t.workflows.join(", ") : "every workflow"}, {t.expired ? "expired" : `until ${day(t.expires)}`}
                </span>
                <button
                  className="quiet"
                  onClick={() =>
                    deliveryApi
                      .revokeToken(t.id)
                      .then(load)
                      .catch((e: Error) => setTrouble(e.message))
                  }
                >
                  <Sym name="close" />
                  Revoke
                </button>
              </li>
            ))}
          </ul>
        )}
        {made?.token && (
          <div className="mcp-made" role="status">
            <span>The token for {made.name}, shown this once:</span>
            <div className="tg-copyable">
              <code>{made.token}</code>
              <Copy text={made.token} label="Copy the token" />
            </div>
            <span className="hint">Keep it as a secret of your CI: OPENDUCT_TOKEN, or a file given with --token-file.</span>
          </div>
        )}
        <NewToken
          role={props.role}
          onMade={(t) => {
            setMade(t);
            load();
          }}
        />
        {trouble && <span className="error set-note">{trouble}</span>}
      </Row>
    </div>
  );
}

const ROLES: Role[] = ["viewer", "editor", "admin"];

function NewToken(props: { role: Role; onMade: (t: ApiToken) => void }) {
  const [name, setName] = useState("");
  const [role, setRole] = useState<Role>("editor");
  const [days, setDays] = useState(90);
  const [scopes, setScopes] = useState("");
  const [note, setNote] = useState("");
  const allowed = ROLES.slice(0, ROLES.indexOf(props.role) + 1);
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setNote("");
    const workflows = scopes
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
    try {
      const t = await deliveryApi.makeToken({ name: name.trim(), role, days, workflows: workflows.length ? workflows : null });
      setName("");
      setScopes("");
      props.onMade(t);
    } catch (err) {
      setNote(err instanceof Error ? err.message : "cannot make the token");
    }
  };
  return (
    <form className="mcp-new set-inline" onSubmit={submit} aria-label="A new API token">
      <input aria-label="What the API token is for" placeholder="For which CI job" value={name} maxLength={100} onChange={(e) => setName(e.target.value)} required />
      <select aria-label="Its role" value={role} onChange={(e) => setRole(e.target.value as Role)}>
        {allowed.map((r) => (
          <option key={r} value={r}>
            {r}
          </option>
        ))}
      </select>
      <label className="narrow">
        Days
        <input type="number" min={1} max={365} aria-label="Days it lasts" value={days} onChange={(e) => setDays(Number(e.target.value))} />
      </label>
      <input aria-label="Only these workflows" placeholder="Only these workflows (billing-*, …): all if empty" value={scopes} onChange={(e) => setScopes(e.target.value)} />
      <button disabled={!name.trim() || days < 1 || days > 365}>
        <Sym name="key" />
        Make an API token
      </button>
      {note && <span className="error set-note">{note}</span>}
    </form>
  );
}

const KINDS: { kind: Environment | null; label: string }[] = [
  { kind: null, label: "None" },
  { kind: "development", label: "Development" },
  { kind: "test", label: "Test" },
  { kind: "production", label: "Production" },
];

/** The workspace's place among the environments, and whether people may edit its workflows. */
export function EnvironmentSettings(props: { workspace: string; admin: boolean; onLoaded?: (d: Delivery) => void }) {
  const [d, setD] = useState<Delivery | null>(null);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  useEffect(() => {
    setD(null);
    deliveryApi
      .settings()
      .then((got) => {
        setD(got);
        props.onLoaded?.(got);
      })
      .catch((e: Error) => setNote({ ok: false, text: e.message }));
  }, [props.workspace]);
  if (!d) return <p className="hint set-loading">{note?.text || "…"}</p>;
  const save = async (changes: Parameters<typeof deliveryApi.saveSettings>[0]) => {
    setNote(null);
    try {
      const got = await deliveryApi.saveSettings(changes);
      setD(got);
      props.onLoaded?.(got);
      setNote({ ok: true, text: "Saved." });
    } catch (err) {
      setNote({ ok: false, text: err instanceof Error ? err.message : "cannot save it" });
    }
  };
  return (
    <div className="environment-settings">
      <Row name="Environment" about="Workflows are promoted from one to the next, in their order">
        {props.admin ? (
          <div className="segments" role="radiogroup" aria-label="Environment">
            {KINDS.map((k) => (
              <button key={k.label} role="radio" aria-checked={d.environment === k.kind} className={d.environment === k.kind ? "here" : ""} onClick={() => save({ environment: k.kind })}>
                {k.label}
              </button>
            ))}
          </div>
        ) : (
          <span className="set-value">{KINDS.find((k) => k.kind === d.environment)?.label}</span>
        )}
      </Row>
      {d.environment && (
        <Row name="Order" about="Lower first: development 1, test 2, production 3">
          {props.admin ? (
            <input type="number" min={0} max={1000} aria-label="Order" defaultValue={d.order} onBlur={(e) => Number(e.target.value) !== d.order && save({ order: Number(e.target.value) })} />
          ) : (
            <span className="set-value">{d.order}</span>
          )}
        </Row>
      )}
      <Row name="Changes only by push or promotion" about="People cannot edit, add or delete workflows here in the studio">
        <label className="switch">
          <input type="checkbox" aria-label="Changes only by push or promotion" checked={d.locked} disabled={!props.admin} onChange={(e) => save({ locked: e.target.checked })} />
          <span />
        </label>
      </Row>
      {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
    </div>
  );
}

const HOSTS: { provider: GitSettings["provider"]; label: string }[] = [
  { provider: "github", label: "GitHub" },
  { provider: "gitlab", label: "GitLab" },
  { provider: "gitea", label: "Gitea" },
];

/** The workspace's repository: where its workflows are saved to and deployed from. */
export function GitSync(props: { workspace: string; admin: boolean }) {
  const [d, setD] = useState<Delivery | null>(null);
  const [credentials, setCredentials] = useState<CredentialSummary[]>([]);
  const [form, setForm] = useState<GitSettings>({ provider: "github", url: "", branch: "main", folder: "workflows", credential: "", changes: "commit" });
  const [secret, setSecret] = useState("");
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const shown = (got: Delivery) => {
    setD(got);
    if (got.git) setForm({ ...got.git, signing_token: undefined });
  };
  useEffect(() => {
    setD(null);
    setSecret("");
    deliveryApi
      .settings()
      .then(shown)
      .catch((e: Error) => setNote({ ok: false, text: e.message }));
    if (props.admin)
      api
        .credentials()
        .then(setCredentials)
        .catch(() => undefined);
  }, [props.workspace]);
  if (!d) return <p className="hint set-loading">{note?.text || "…"}</p>;
  const act = async (what: () => Promise<string>) => {
    setBusy(true);
    setNote(null);
    try {
      setNote({ ok: true, text: await what() });
    } catch (err) {
      setNote({ ok: false, text: err instanceof Error ? err.message : "it did not work" });
    } finally {
      setBusy(false);
    }
  };
  const set = (name: keyof GitSettings, value: string) => setForm({ ...form, [name]: value });
  const bearers = credentials.filter((c) => c.kind === "bearer");
  const hook = d.git?.hook ? `${location.origin}${d.git.hook}` : "";
  return (
    <div className="git-sync">
      {props.admin && (
        <form
          className="set-row git-form"
          aria-label="The repository"
          onSubmit={(e) => {
            e.preventDefault();
            act(async () => {
              shown(await deliveryApi.saveSettings({ git: form }));
              return "Saved.";
            });
          }}
        >
          <div className="set-name">
            <span>Repository</span>
            <span className="hint">Its https address, a branch, a folder, and a bearer credential holding a token that may write to it</span>
          </div>
          <div className="set-control set-inline">
            <select aria-label="Git host" value={form.provider} onChange={(e) => set("provider", e.target.value)}>
              {HOSTS.map((h) => (
                <option key={h.provider} value={h.provider}>
                  {h.label}
                </option>
              ))}
            </select>
            <input aria-label="Repository address" placeholder="https://github.com/owner/repo" value={form.url} onChange={(e) => set("url", e.target.value)} required />
            <input aria-label="Branch" value={form.branch} onChange={(e) => set("branch", e.target.value)} required />
            <input aria-label="Folder" placeholder="The repository's top" value={form.folder} onChange={(e) => set("folder", e.target.value)} />
            <select aria-label="Token credential" value={form.credential} onChange={(e) => set("credential", e.target.value)} required>
              <option value="">A bearer credential…</option>
              {bearers.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name}
                </option>
              ))}
            </select>
            <select aria-label="Changes go" value={form.changes} onChange={(e) => set("changes", e.target.value)}>
              <option value="commit">As a commit to the branch</option>
              <option value="pull_request">As a pull request</option>
            </select>
            {form.provider === "gitlab" && (
              <input aria-label="GitLab signing token" placeholder="GitLab's signing token (whsec_…), if set" value={String(form.signing_token ?? "")} onChange={(e) => set("signing_token", e.target.value)} />
            )}
            <button disabled={busy}>
              <Sym name="check" />
              Save the repository
            </button>
            {d.git && (
              <button type="button" className="quiet" disabled={busy} onClick={() => act(async () => (shown(await deliveryApi.saveSettings({ git: null })), "Git sync is off."))}>
                <Sym name="close" />
                Stop syncing
              </button>
            )}
          </div>
        </form>
      )}
      {!props.admin && (
        <Row name="Repository">
          <span className="set-value">{d.git ? `${d.git.url} (${d.git.branch}${d.git.folder ? `, ${d.git.folder}` : ""})` : "None: its admins set one"}</span>
        </Row>
      )}
      {d.git && (
        <>
          <Row name="Save and deploy" about="Save writes every workflow as a file; Deploy pushes the folder's files here">
            <button
              disabled={busy}
              onClick={() =>
                act(async () => {
                  const r = await deliveryApi.saveToGit("");
                  if (r.saved === 0) return "Nothing to save: the repository has every workflow as it is.";
                  return r.pull_request ? `${r.saved} file(s) proposed: ${r.pull_request}` : `${r.saved} file(s) committed (${(r.commit ?? "").slice(0, 7)}).`;
                })
              }
            >
              <Sym name="check" />
              Save to Git
            </button>
            <button
              className="quiet"
              disabled={busy}
              onClick={() =>
                act(async () => {
                  const r = await deliveryApi.deployFromGit();
                  return `Deployed: ${r.summary.create} new, ${r.summary.update} changed, ${r.summary.unchanged} unchanged.`;
                })
              }
            >
              Deploy from Git
            </button>
          </Row>
          <Row name="Webhook" about="Add it to the repository for pushes, with the secret: each push to the branch is deployed">
            <span className="tg-copyable">
              <code title={hook}>{hook}</code>
              <Copy text={hook} label="Copy the webhook's address" />
            </span>
            {props.admin && (
              <button className="quiet" disabled={busy} onClick={() => act(async () => (setSecret((await deliveryApi.newSecret()).secret), "A new secret: the old one no longer signs."))}>
                <Sym name="key" />
                {d.git.secret ? "New secret" : "Make its secret"}
              </button>
            )}
            {secret && (
              <span className="mcp-made" role="status">
                The webhook's secret, shown this once: <code>{secret}</code> <Copy text={secret} label="Copy the secret" />
              </span>
            )}
          </Row>
          {d.last_deploy && (
            <Row name="Last deploy">
              <span className={d.last_deploy.ok ? "set-value" : "error"}>
                {new Date(d.last_deploy.at * 1000).toLocaleString()}, {d.last_deploy.commit.slice(0, 12)}: {d.last_deploy.ok ? "done" : d.last_deploy.error}
              </span>
            </Row>
          )}
        </>
      )}
      {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
    </div>
  );
}
