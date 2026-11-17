// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The bell: what needs someone, in every workspace the person is in - an agency's clients each - counted on the bell
// and listed under it, each with where it is fixed. One dismissed stays away until it says something new (worse,
// or another number); one fixed goes by itself.

import { useEffect, useRef, useState } from "react";
import { api, type Alert, type ClientSummary } from "./api";

type Held = { workspace: ClientSummary; alert: Alert };

function readDismissed(): Record<string, string> {
  try {
    return JSON.parse(localStorage.getItem("openduct.dismissed") ?? "{}") as Record<string, string>;
  } catch {
    return {};
  }
}

function fingerprint(a: Alert): string {
  return `${a.level}|${a.title}`;
}

export function Bell(props: { onOpenAt: (workspace: string, path: string) => void }) {
  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [open, setOpen] = useState(false);
  const [dismissed, setDismissed] = useState<Record<string, string>>(readDismissed);
  const box = useRef<HTMLDivElement>(null);
  useEffect(() => {
    let live = true;
    const load = () =>
      api
        .overview("1d")
        .then((c) => live && setClients(c))
        .catch(() => undefined);
    load();
    const every = setInterval(load, 60000);
    return () => {
      live = false;
      clearInterval(every);
    };
  }, []);
  useEffect(() => {
    if (!open) return;
    const away = (e: MouseEvent) => box.current && !box.current.contains(e.target as Node) && setOpen(false);
    const esc = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    addEventListener("mousedown", away);
    addEventListener("keydown", esc);
    return () => {
      removeEventListener("mousedown", away);
      removeEventListener("keydown", esc);
    };
  }, [open]);
  const held: Held[] = clients.flatMap((w) =>
    w.alerts.filter((a) => a.level !== "info" && dismissed[`${w.id}/${a.key}`] !== fingerprint(a)).map((alert) => ({ workspace: w, alert })),
  );
  const critical = held.some((h) => h.alert.level === "critical");
  const dismiss = (h: Held) => {
    const next = { ...dismissed, [`${h.workspace.id}/${h.alert.key}`]: fingerprint(h.alert) };
    setDismissed(next);
    try {
      localStorage.setItem("openduct.dismissed", JSON.stringify(next));
    } catch {
      // A browser that keeps nothing: dismissed for this visit.
    }
  };
  const several = clients.length > 1;
  return (
    <div className="bell" ref={box}>
      <button
        className={`bell-button${held.length ? (critical ? " critical" : " warning") : ""}`}
        aria-label={held.length ? `${held.length} alert${held.length === 1 ? "" : "s"}` : "No alerts"}
        aria-expanded={open}
        title={held.length ? `${held.length} need${held.length === 1 ? "s" : ""} attention` : "Nothing needs you"}
        onClick={() => setOpen(!open)}
      >
        <svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M6 8a6 6 0 1 1 12 0c0 7 3 9 3 9H3s3-2 3-9M10.3 21a1.94 1.94 0 0 0 3.4 0" />
        </svg>
        {held.length > 0 && <span className="bell-count">{held.length > 99 ? "99+" : held.length}</span>}
      </button>
      {open && (
        <div className="bell-panel" role="dialog" aria-label="Alerts">
          <div className="bell-head">
            <span>Alerts</span>
            <span className="hint">{held.length ? `${held.length} need${held.length === 1 ? "s" : ""} attention` : "Nothing needs you"}</span>
          </div>
          {held.length === 0 ? (
            <p className="bell-empty">
              <span className="dot done" /> All clear{several ? " in every client" : ""}.
            </p>
          ) : (
            <ul className="bell-list">
              {held.map((h) => (
                <li key={`${h.workspace.id}/${h.alert.key}`} className={h.alert.level}>
                  <span className={`alert-mark ${h.alert.level}`} aria-hidden="true">
                    {h.alert.level === "critical" ? "!" : "i"}
                  </span>
                  <span className="bell-text">
                    {several && <span className="bell-client">{h.workspace.name}</span>}
                    <span>{h.alert.title}</span>
                    <span className="hint">{h.alert.detail}</span>
                    <button
                      className="quiet link"
                      onClick={() => {
                        setOpen(false);
                        props.onOpenAt(h.workspace.id, h.alert.action.path);
                      }}
                    >
                      {h.alert.action.label} ›
                    </button>
                  </span>
                  <button className="bell-dismiss" aria-label={`Dismiss: ${h.alert.title}`} title="Dismiss until it changes" onClick={() => dismiss(h)}>
                    ×
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
