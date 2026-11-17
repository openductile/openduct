// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A workspace's credentials: added, changed and removed - their values never shown, a secret field never read back.
// Adding one opens a window: its kind, picked from tiles, then that kind's fields by their plain names.

import { Sym } from "./Sym";
import { Grow } from "./Grow";
import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { PROVIDERS, filledBy, type Provider } from "./aiProviders";
import { api, type CredentialKind, type CredentialSummary } from "./api";
import { kindLook, fieldLook } from "./credentialLook";
import { BrandIcon, Icon } from "./flow/Icon";
import { Popup } from "./flow/Popup";

type Editing = { name: string; kind: string | null; fresh: boolean };

export function Credentials(props: { canEdit: boolean; hash?: string }) {
  const [list, setList] = useState<CredentialSummary[] | null>(null);
  const [kinds, setKinds] = useState<CredentialKind[]>([]);
  const [editing, setEditing] = useState<Editing | null>(null);
  const [trouble, setTrouble] = useState("");
  // Back from signing in at a service: how it went.
  const [arrived] = useState(() => {
    const q = new URLSearchParams(location.search);
    return q.get("connected") ? { ok: true, text: `Connected: ${q.get("connected")}` } : q.get("oauth_error") ? { ok: false, text: q.get("oauth_error") ?? "" } : null;
  });
  const load = () => api.credentials().then(setList).catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
    api.credentialKinds().then(setKinds).catch(() => undefined);
  }, []);
  // Arrived to connect an app (#add), or to add a credential a step names (#add=<name>): its window opens at once.
  useEffect(() => {
    if (!props.canEdit || !props.hash?.startsWith("#add")) return;
    const name = decodeURIComponent(props.hash.slice("#add=".length) || "");
    setEditing({ name: props.hash.startsWith("#add=") ? name : "", kind: null, fresh: true });
    history.replaceState(null, "", location.pathname);
  }, [props.hash, props.canEdit]);
  const remove = async (name: string) => {
    await api.removeCredential(name).catch((e: Error) => setTrouble(e.message));
    load();
  };
  return (
    <main className="page list-page">
      <div className="page-head">
        <div>
          <h1 className="visually-hidden">Credentials</h1>
          <p className="hint">Keys and tokens your workflows use. A step names one; its secret is added to the request and kept nowhere else.</p>
        </div>
        {props.canEdit && <button onClick={() => setEditing({ name: "", kind: null, fresh: true })}>
            <Sym name="plus" />
            Add a credential
          </button>}
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {arrived && <p className={arrived.ok ? "ok banner" : "error banner"}>{arrived.text}</p>}
      {list && list.length === 0 && (
        <div className="empty">
          No credentials yet. {props.canEdit && "Add one for each service your workflows call: a model, a database, a chat."}
        </div>
      )}
      {list && list.length > 0 && (
        <table className="list">
          <thead>
            <tr>
              <th>Name</th>
              <th>Kind</th>
              <th>Holds</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {list.map((c) => {
              const spec = kinds.find((k) => k.kind === c.kind);
              const look = kindLook(c.kind, spec?.label, spec?.app);
              return (
                <tr key={c.name}>
                  <td>
                    <code>{c.name}</code>
                  </td>
                  <td>
                    <span className={`kind-chip family-${look.family}`}>
                      <span className="icon">
                        <Icon kind={look.icon} />
                      </span>
                      {look.name}
                    </span>
                  </td>
                  <td className="hint">{c.fields.map((f) => fieldLook(c.kind, f).label).join(", ")}</td>
                  <td className="actions">
                    {props.canEdit && (
                      <>
                        <button className="quiet" onClick={() => setEditing({ name: c.name, kind: c.kind, fresh: false })}>
                          <Sym name="pencil" />
                          Change
                        </button>
                        <button className="quiet danger-quiet" onClick={() => remove(c.name)}>
                          <Sym name="trash" />
                          Remove
                        </button>
                      </>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      {editing && (
        <CredentialWindow
          kinds={kinds}
          editing={editing}
          onDone={() => {
            setEditing(null);
            load();
          }}
          onCancel={() => setEditing(null)}
        />
      )}
    </main>
  );
}

function CredentialWindow(props: { kinds: CredentialKind[]; editing: Editing; onDone: () => void; onCancel: () => void }) {
  const [name, setName] = useState(props.editing.name);
  const [kind, setKind] = useState<string | null>(props.editing.kind);
  const [provider, setProvider] = useState<Provider | null>(null);
  const [fields, setFields] = useState<Record<string, string>>({});
  const [error, setError] = useState("");
  const spec = props.kinds.find((k) => k.kind === kind);
  const look = kind ? kindLook(kind, spec?.label, spec?.app) : null;
  const ai = kind === "ai" && props.editing.fresh;
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!kind) return;
    setError("");
    // Signed in at the vendor: off to its page, and back here once it is done.
    if (spec?.connect) {
      try {
        location.href = await api.oauthStart(kind, name || kind);
      } catch (e) {
        setError(e instanceof Error ? e.message : "cannot connect it");
      }
      return;
    }
    try {
      if (props.editing.fresh) await api.addCredential(name, kind, fields);
      else await api.changeCredential(name, Object.fromEntries(Object.entries(fields).filter(([, v]) => v !== "")));
      props.onDone();
    } catch (e) {
      setError(e instanceof Error ? e.message : "cannot keep it");
    }
  };
  const pick = (p: Provider) => {
    setProvider(p);
    setFields(filledBy(p));
    if (!name) setName(p.id === "other" ? "model" : p.id);
  };
  const title = props.editing.fresh
    ? provider
      ? `A new ${provider.name} credential`
      : look
        ? `A new ${look.name} credential`
        : "A new credential"
    : `Change ${props.editing.name}`;
  const visible = (spec?.fields ?? []).filter((f) => f.name !== "provider");
  const advanced = ["base_url", "api", "key_header", "headers"];
  const field = (f: { name: string; optional: boolean; secret: boolean }) => (
    <Field
      key={f.name}
      kind={kind!}
      spec={f}
      fresh={props.editing.fresh}
      required={props.editing.fresh && (!f.optional || (ai && f.name === "api_key" && !provider?.keyless))}
      value={fields[f.name] ?? ""}
      onChange={(v) => setFields({ ...fields, [f.name]: v })}
    />
  );
  return (
    <Popup label={title} title={<h3>{title}</h3>} onClose={props.onCancel}>
      {!kind ? (
        <Tiles
          question="What is it for?"
          items={props.kinds.map((k) => {
            const l = kindLook(k.kind, k.label, k.app);
            return { id: k.kind, name: l.name, blurb: l.blurb, family: l.family, icon: <Icon kind={l.icon} /> };
          })}
          onPick={setKind}
        />
      ) : ai && !provider ? (
        <Tiles
          question="Which service runs the model?"
          items={PROVIDERS.map((p) => ({
            id: p.id,
            name: p.name,
            blurb: p.id === "other" ? "Its address, and how it takes its key" : p.keyless ? "No key needed" : "Just its key",
            family: "ai" as const,
            icon: p.brand ? <BrandIcon slug={p.brand} /> : <Icon kind="ai" />,
          }))}
          onPick={(id) => pick(PROVIDERS.find((p) => p.id === id)!)}
          back={() => setKind(null)}
        />
      ) : (
        <form className="credential-form" onSubmit={submit} autoComplete="off">
          {props.editing.fresh && (
            <div className="form-grid">
              <label className="wide">
                <span className="field-label">
                  Name <span className="hint">what steps call it</span>
                </span>
                <input
                  className="code"
                  name="credential-name"
                  autoComplete="off"
                  spellCheck={false}
                  placeholder="model"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  required
                />
              </label>
              <p className="chosen wide">
                <span className={`kind-chip family-${look!.family}`}>
                  <span className="icon">{provider?.brand ? <BrandIcon slug={provider.brand} /> : <Icon kind={look!.icon} />}</span>
                  {provider?.name ?? look!.name}
                </span>
                <button type="button" className="link" onClick={() => (ai ? setProvider(null) : setKind(null))}>
                  {ai ? "another service" : "another kind"}
                </button>
              </p>
            </div>
          )}
          {ai ? (
            <>
              <div className="form-grid">{visible.filter((f) => !advanced.includes(f.name) && !(f.name === "api_key" && provider?.keyless)).map(field)}</div>
              <details className="advanced" open={provider?.id === "other"}>
                <summary>Address and API {provider?.id !== "other" && <span className="hint">filled in for {provider?.name}</span>}</summary>
                <div className="form-grid">{visible.filter((f) => advanced.includes(f.name)).map(field)}</div>
              </details>
            </>
          ) : spec?.connect ? (
            spec.registered ? (
              <p className="hint">You sign in at the service itself; OpenDuct keeps what it gives back, sealed, and renews it as it runs out.</p>
            ) : (
              <p className="error">This installation is not registered with the service yet: its admin adds the client id and secret under Settings, App sign-ins.</p>
            )
          ) : (
            <div className="form-grid">{visible.map(field)}</div>
          )}
          {error && <p className="error">{error}</p>}
          <div className="form-actions">
            <button type="button" className="quiet" onClick={props.onCancel}>
              <Sym name="close" />
              Cancel
            </button>
            {spec?.connect ? (
              <button disabled={!spec.registered}>
                <Sym name="arrow" />
                Connect
              </button>
            ) : (
              <button>
                <Sym name="check" />
                Keep it
              </button>
            )}
          </div>
        </form>
      )}
    </Popup>
  );
}

type Tile = { id: string; name: string; blurb: string; family: string; icon: ReactNode };

function Tiles(props: { question: string; items: Tile[]; onPick: (id: string) => void; back?: () => void }) {
  return (
    <div className="kind-tiles">
      <p className="hint">
        {props.question}
        {props.back && (
          <>
            {" "}
            <button type="button" className="link" onClick={props.back}>
              another kind
            </button>
          </>
        )}
      </p>
      <div className="choices">
        {props.items.map((t) => (
          <button key={t.id} type="button" className={`choice family-${t.family}`} onClick={() => props.onPick(t.id)}>
            <span className="icon">{t.icon}</span>
            <span className="words">
              <strong>{t.name}</strong>
              <span>{t.blurb}</span>
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

function Field(props: {
  kind: string;
  spec: { name: string; optional: boolean; secret: boolean };
  fresh: boolean;
  required: boolean;
  value: string;
  onChange: (v: string) => void;
}) {
  const f = props.spec;
  const l = fieldLook(props.kind, f.name);
  return (
    <label className={l.wide || l.multiline ? "wide" : ""}>
      <span className="field-label">
        {l.label}
        {f.optional && !props.required && <span className="tag">optional</span>}
        {!props.fresh && <span className="hint">empty keeps it</span>}
        {l.hint && props.fresh && <span className="hint">{l.hint}</span>}
      </span>
      {l.options ? (
        <select aria-label={l.label} value={props.value} onChange={(e) => props.onChange(e.target.value)} required={props.required}>
          <option value="">{f.optional ? `as it comes (${l.options[0]})` : "choose"}</option>
          {l.options.map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      ) : l.multiline ? (
        <Grow aria-label={l.label} className="code" max={300} placeholder={l.placeholder} value={props.value} onChange={props.onChange} />
      ) : (
        <input
          aria-label={l.label}
          className="code"
          type={f.secret ? "password" : "text"}
          name={`${props.kind}-${f.name}`}
          autoComplete={f.secret ? "new-password" : "off"}
          spellCheck={false}
          placeholder={l.placeholder}
          value={props.value}
          onChange={(e) => props.onChange(e.target.value)}
          required={props.required}
        />
      )}
    </label>
  );
}
