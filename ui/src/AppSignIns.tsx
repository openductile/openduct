// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Settings, App sign-ins (the install's admin): for each app a person signs in to at the service itself, the
// install's own registration with that service - its client id and secret, which the service gives when the admin
// registers this installation there (OpenDuct takes no part). The callback address to give the service is shown.

import { useEffect, useState } from "react";
import { api, type OauthApp } from "./api";
import { Copy } from "./Trigger";

function Registration(props: { app: OauthApp; onChanged: () => void }) {
  const [clientId, setClientId] = useState(props.app.client_id ?? "");
  const [secret, setSecret] = useState("");
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const save = async () => {
    setNote(null);
    try {
      await api.registerOauth(props.app.app, clientId, secret);
      setSecret("");
      setNote({ ok: true, text: "Kept" });
      props.onChanged();
    } catch (e) {
      setNote({ ok: false, text: e instanceof Error ? e.message : "cannot keep it" });
    }
  };
  return (
    <div className="set-row">
      <div className="set-name">
        <span>{props.app.name}</span>
        <span className="hint">
          {props.app.registered ? "registered" : "not registered yet"}
          {props.app.docs && (
            <>
              {" · "}
              <a href={props.app.docs} target="_blank" rel="noreferrer">
                its developer pages
              </a>
            </>
          )}
        </span>
      </div>
      <div className="set-control oauth-reg">
        <input aria-label={`${props.app.name} client id`} placeholder="Client id" value={clientId} spellCheck={false} autoComplete="off" onChange={(e) => setClientId(e.target.value)} />
        <input
          aria-label={`${props.app.name} client secret`}
          type="password"
          placeholder={props.app.registered ? "kept: type a new one to replace it" : "Client secret"}
          value={secret}
          autoComplete="off"
          onChange={(e) => setSecret(e.target.value)}
        />
        <button disabled={!clientId || (!props.app.registered && !secret)} onClick={save}>
          Keep
        </button>
        {note && <span className={note.ok ? "ok" : "error"}>{note.text}</span>}
      </div>
    </div>
  );
}

export function AppSignIns() {
  const [apps, setApps] = useState<OauthApp[] | null>(null);
  const [callback, setCallback] = useState("");
  const [trouble, setTrouble] = useState("");
  const load = () =>
    api
      .oauthApps()
      .then((r) => {
        setApps(r.apps);
        setCallback(r.callback);
      })
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
  }, []);
  if (trouble) return <p className="error">{trouble}</p>;
  if (!apps) return <p className="hint">…</p>;
  if (apps.length === 0) return <p className="hint">No app here asks for a sign-in at its service.</p>;
  return (
    <>
      <p className="hint">
        Some services let people sign in at the service itself. Register this installation with each one (an "OAuth app" in its developer pages), give it the address
        below, and keep the client id and secret it gives here. Each workspace then connects its own account under Credentials.
      </p>
      <div className="set-row">
        <div className="set-name">
          <span>Address to give each service</span>
          <span className="hint">its "redirect URI" or "callback URL"</span>
        </div>
        <div className="set-control">
          <code className="set-path">{callback}</code>
          <Copy text={callback} label="Copy the address" />
        </div>
      </div>
      {apps.map((a) => (
        <Registration key={a.app} app={a} onChanged={load} />
      ))}
    </>
  );
}
