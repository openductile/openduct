// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The workspace's own apps ("Add an app"): each listed with how finished it is. An editor makes one from an API
// description (its address, or a file) or an MCP server's address - or pastes an app.yaml - and sees what it would
// be: the actions proposed (the best ticked, as `openduct app from-openapi` ranks them), the sign-ins, the quality
// report and what is not done; unticks what it does not want, looks again, and saves. An app's definition can be
// changed, saved as app.yaml, and removed. Viewers see the list.

import { useEffect, useState } from "react";
import { customAppsApi, type Asked, type CustomApp, type CustomAppSummary, type Preview, type Quality } from "./customAppsApi";
import "./apps.css";

/** The largest description file the page sends. */
const MOST = 12 * 1024 * 1024;

type From = "url" | "file" | "mcp" | "definition";
const FROM: { from: From; label: string }[] = [
  { from: "url", label: "An API description's address" },
  { from: "file", label: "An API description file" },
  { from: "mcp", label: "An MCP server" },
  { from: "definition", label: "An app.yaml" },
];

/** An app's id from what a person types: lowercase letters, digits and _, starting with a letter. */
export function idOf(text: string): string {
  const cleaned = text.trim().toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 40);
  return /^[a-z]/.test(cleaned) ? cleaned : cleaned ? `app_${cleaned}`.slice(0, 40) : "";
}

function QualityReport(props: { quality: Quality | null }) {
  const q = props.quality;
  if (!q) return null;
  return (
    <section className="apps-quality" aria-label="Quality">
      <p>
        <strong className={q.meets ? "ok" : "warn"}>
          {q.score}/100
        </strong>{" "}
        <span className="hint">
          {q.meets ? `meets the bar of ${q.bar}` : `below the bar of ${q.bar}`}: {q.actions} actions, {q.described} described, {q.with_output} with an output,{" "}
          {q.recorded} with a recorded test{q.refusal ? ", a refusal recorded" : ""}
        </span>
      </p>
      <p className="hint">
        Signs in with {q.sign_ins.map((s) => s.kind).join("; ") || "nothing"}; reaches {q.hosts.join(", ") || "the address its credential gives"}.
      </p>
      {q.missing.length > 0 && (
        <ul className="hint apps-missing" aria-label="To do">
          {q.missing.map((m) => (
            <li key={m}>{m}</li>
          ))}
        </ul>
      )}
      {(q.failing ?? []).length > 0 && (
        <ul className="error" aria-label="Recordings that do not hold">
          {(q.failing ?? []).map((f) => (
            <li key={f}>{f}</li>
          ))}
        </ul>
      )}
    </section>
  );
}

function AddApp(props: { onSaved: () => void; onCancel: () => void }) {
  const [from, setFrom] = useState<From>("url");
  const [url, setUrl] = useState("");
  const [file, setFile] = useState<{ name: string; text: string } | null>(null);
  const [definition, setDefinition] = useState("");
  const [token, setToken] = useState("");
  const [name, setName] = useState("");
  const [title, setTitle] = useState("");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [chosen, setChosen] = useState<string[] | null>(null);
  const [signIns, setSignIns] = useState<string[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [busy, setBusy] = useState(false);
  const asked = (): Asked => {
    const base = { name, title: title || undefined, chosen: chosen ?? undefined, sign_ins: signIns ?? undefined };
    if (from === "mcp") return { ...base, from: "mcp", url, token: token || undefined, sign_ins: undefined };
    if (from === "definition") return { from: "definition", text: definition };
    if (from === "file") return { ...base, from: "openapi", text: file?.text ?? "", file: file?.name };
    return { ...base, from: "openapi", url };
  };
  const look = async () => {
    setTrouble("");
    setBusy(true);
    try {
      const p = await customAppsApi.preview(asked());
      setPreview(p);
      setChosen(p.made.proposed.filter((a) => a.chosen).map((a) => a.name));
      setSignIns(p.made.sign_ins.filter((s) => s.chosen).map((s) => s.id));
    } catch (e) {
      setPreview(null);
      setTrouble(e instanceof Error ? e.message : "cannot make it");
    } finally {
      setBusy(false);
    }
  };
  const save = async () => {
    setTrouble("");
    setBusy(true);
    try {
      await customAppsApi.save(asked());
      props.onSaved();
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot save it");
    } finally {
      setBusy(false);
    }
  };
  const pick = async (f: File) => {
    setTrouble("");
    if (f.size > MOST) return setTrouble(`${f.name} is over 12 MB`);
    setFile({ name: f.name, text: await f.text() });
    if (!name) setName(idOf(f.name.replace(/\.[^.]*$/, "")));
  };
  const toggle = (list: string[] | null, item: string) => (list ?? []).includes(item) ? (list ?? []).filter((x) => x !== item) : [...(list ?? []), item];
  const ready = from === "definition" ? definition.trim() !== "" : name !== "" && (from === "file" ? file !== null : url.trim() !== "");
  return (
    <section className="card apps-new" aria-label="Add an app">
      <div className="segments" role="radiogroup" aria-label="Made from">
        {FROM.map((f) => (
          <button
            key={f.from}
            role="radio"
            aria-checked={from === f.from}
            className={from === f.from ? "here" : ""}
            onClick={() => {
              setFrom(f.from);
              setPreview(null);
              setChosen(null);
              setSignIns(null);
            }}
          >
            {f.label}
          </button>
        ))}
      </div>
      {from === "url" && (
        <label>
          Address <span className="hint">of an OpenAPI 3.0 or 3.1, or Swagger 2.0, description: JSON or YAML</span>
          <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://api.example.com/openapi.json" />
        </label>
      )}
      {from === "file" && (
        <label className="button upload quiet">
          {file ? file.name : "Choose the description file"}
          <input type="file" accept=".json,.yaml,.yml" hidden aria-label="The description file" onChange={(e) => e.target.files?.[0] && pick(e.target.files[0])} />
        </label>
      )}
      {from === "mcp" && (
        <>
          <label>
            Address <span className="hint">the vendor's MCP server: its tools become the app's actions</span>
            <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://mcp.example.com/mcp" />
          </label>
          <label>
            Token <span className="hint">to list its tools, if it asks for one: used once, not kept</span>
            <input type="password" value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" />
          </label>
        </>
      )}
      {from === "definition" ? (
        <label>
          app.yaml <span className="hint">an app's definition, as docs/apps.md describes it</span>
          <textarea className="code" rows={14} value={definition} onChange={(e) => setDefinition(e.target.value)} />
        </label>
      ) : (
        <div className="row">
          <label>
            Name <span className="hint">what steps name it by: lowercase letters, digits and _</span>
            <input value={name} data-plain onChange={(e) => setName(idOf(e.target.value) || e.target.value.toLowerCase())} placeholder="my_crm" />
          </label>
          <label>
            Shown as <span className="hint">by default, the description's title</span>
            <input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="My CRM" />
          </label>
        </div>
      )}
      {trouble && <p className="error">{trouble}</p>}
      <div className="row">
        <button onClick={look} disabled={!ready || busy}>
          {preview ? "Look again" : "Look"}
        </button>
        {preview && (
          <button onClick={save} disabled={busy || preview.problems.length > 0 || (chosen !== null && chosen.length === 0 && from !== "definition")}>
            Save the app
          </button>
        )}
        <button className="quiet" onClick={props.onCancel}>
          Cancel
        </button>
      </div>
      {preview && (
        <div className="apps-preview">
          {preview.problems.length > 0 && (
            <ul className="error" aria-label="What stops it">
              {preview.problems.map((p) => (
                <li key={p}>{p}</li>
              ))}
            </ul>
          )}
          <QualityReport quality={preview.quality} />
          {preview.made.sign_ins.length > 0 && (
            <fieldset className="apps-choices" aria-label="Sign-ins">
              <legend>How it signs in</legend>
              {preview.made.sign_ins.map((s) => (
                <label key={s.id} className={s.allowed ? "" : "muted"}>
                  <input type="checkbox" disabled={!s.allowed} checked={(signIns ?? []).includes(s.id)} onChange={() => setSignIns(toggle(signIns, s.id))} />
                  {s.label} <span className="hint">{s.allowed ? s.kind : `${s.kind}: needs the installation's own registration, so is for apps it adds`}</span>
                </label>
              ))}
            </fieldset>
          )}
          {preview.made.proposed.length > 0 && (
            <fieldset className="apps-choices" aria-label="Actions">
              <legend>
                Actions <span className="hint">{(chosen ?? []).length} chosen of {preview.made.proposed.filter((a) => !a.left_out).length}; the best first. Untick any, then look again.</span>
              </legend>
              {preview.made.proposed.map((a) => (
                <label key={a.name} className={a.left_out ? "muted" : ""}>
                  <input type="checkbox" disabled={a.left_out !== null} checked={(chosen ?? []).includes(a.name)} onChange={() => setChosen(toggle(chosen, a.name))} />
                  {a.label} <code className="hint">{a.name}</code>{" "}
                  <span className="hint">
                    {a.method} {a.path}
                    {a.left_out ? ` - not offered: ${a.left_out}` : ""}
                  </span>
                </label>
              ))}
            </fieldset>
          )}
          {preview.made.not_done.length > 0 && (
            <details>
              <summary>Not done ({preview.made.not_done.length})</summary>
              <ul className="hint">
                {preview.made.not_done.map((n) => (
                  <li key={n}>{n}</li>
                ))}
              </ul>
            </details>
          )}
          <details>
            <summary>Its app.yaml</summary>
            <pre className="code">{preview.made.app_yaml}</pre>
          </details>
        </div>
      )}
    </section>
  );
}

function ChangeApp(props: { app: CustomApp; onDone: () => void }) {
  const [text, setText] = useState(props.app.definition);
  const [trouble, setTrouble] = useState("");
  const save = async () => {
    setTrouble("");
    try {
      await customAppsApi.change(props.app.id, text);
      props.onDone();
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot save it");
    }
  };
  return (
    <section className="card apps-new" aria-label={`Change ${props.app.name}`}>
      <QualityReport quality={props.app.quality} />
      <label>
        app.yaml <span className="hint">its name stays {props.app.id}</span>
        <textarea className="code" rows={20} value={text} onChange={(e) => setText(e.target.value)} />
      </label>
      {trouble && <p className="error">{trouble}</p>}
      <div className="row">
        <button onClick={save} disabled={text === props.app.definition}>
          Save
        </button>
        <button className="quiet" onClick={props.onDone}>
          Cancel
        </button>
      </div>
    </section>
  );
}

/** Saves `text` as a file called `name`. */
function download(name: string, text: string) {
  const link = document.createElement("a");
  link.href = URL.createObjectURL(new Blob([text], { type: "application/yaml" }));
  link.download = name;
  link.click();
  URL.revokeObjectURL(link.href);
}

export function Apps(props: { canEdit: boolean }) {
  const [list, setList] = useState<CustomAppSummary[] | null>(null);
  const [most, setMost] = useState(200);
  const [adding, setAdding] = useState(false);
  const [changing, setChanging] = useState<CustomApp | null>(null);
  const [trouble, setTrouble] = useState("");
  const load = () =>
    customAppsApi
      .list()
      .then((l) => {
        setList(l.apps);
        setMost(l.most);
      })
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
  }, []);
  const remove = async (a: CustomAppSummary) => {
    if (!window.confirm(`Remove the app ${a.name}? Workflows that use it stop working.`)) return;
    await customAppsApi.remove(a.id).catch((e: Error) => setTrouble(e.message));
    load();
  };
  const exported = async (a: CustomAppSummary) => {
    try {
      download("app.yaml", (await customAppsApi.one(a.id)).definition);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot read it");
    }
  };
  return (
    <main className="page list-page apps-page">
      <h1 className="visually-hidden">Apps</h1>
      <div className="files-head">
        <p className="hint">
          Apps of this workspace's own, made from an API's description or an MCP server: their steps are offered in this workspace's workflows, and no
          other's. Up to {most}.
        </p>
        {props.canEdit && !adding && <button onClick={() => setAdding(true)}>Add an app</button>}
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {adding && (
        <AddApp
          onCancel={() => setAdding(false)}
          onSaved={() => {
            setAdding(false);
            load();
          }}
        />
      )}
      {changing && (
        <ChangeApp
          app={changing}
          onDone={() => {
            setChanging(null);
            load();
          }}
        />
      )}
      {list && list.length === 0 && !adding && <div className="empty-note">No apps of this workspace's own yet.</div>}
      {list && list.length > 0 && (
        <table className="compact files-table">
          <thead>
            <tr>
              <th>App</th>
              <th className="num">Actions</th>
              <th className="num">Quality</th>
              <th>Made from</th>
              <th className="num" />
            </tr>
          </thead>
          <tbody>
            {list.map((a) => (
              <tr key={a.id}>
                <td>
                  {a.name} <code className="hint">{a.id}</code>
                </td>
                <td className="num">{a.actions}</td>
                <td className="num">{a.score ?? "-"}</td>
                <td className="muted">{a.source || "written by hand"}</td>
                <td className="num">
                  <span className="row-tools">
                    <button className="quiet" onClick={() => exported(a)} aria-label={`Save ${a.name} as app.yaml`}>
                      app.yaml
                    </button>
                    {props.canEdit && (
                      <>
                        <button className="quiet" onClick={() => customAppsApi.one(a.id).then(setChanging).catch((e: Error) => setTrouble(e.message))}>
                          Change
                        </button>
                        <button className="icon-button remove" title="Remove the app" aria-label={`Remove ${a.name}`} onClick={() => remove(a)}>
                          ✕
                        </button>
                      </>
                    )}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
