// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Where a workspace's alerts go: its own messengers - Slack, WhatsApp, Telegram, Discord, Microsoft Teams, Google
// Chat - each ready, each through a credential of the workspace's and, where the messenger needs it, a place in it.
// Its admins turn each on, try it, and keep the lot; OpenDuct sends what needs someone there, once.

import { Sym } from "./Sym";
import { useEffect, useState } from "react";
import { api, type AlertChannel, type CredentialSummary } from "./api";
import { Icon } from "./flow/Icon";
import { TAKES } from "./Steps";

const MESSENGERS: { kind: string; name: string; place?: string; placeholder?: string; needed?: boolean }[] = [
  { kind: "slack", name: "Slack", place: "Channel", placeholder: "#alerts (for a bot token; a webhook has its own)" },
  { kind: "whatsapp", name: "WhatsApp", place: "Number", placeholder: "15551234567", needed: true },
  { kind: "telegram", name: "Telegram", place: "Chat id", placeholder: "-1001234567890", needed: true },
  { kind: "discord", name: "Discord" },
  { kind: "teams", name: "Microsoft Teams" },
  { kind: "google_chat", name: "Google Chat" },
];

function blank(kind: string): AlertChannel {
  return { kind, credential: "", target: "", on: false };
}

export function AlertSettings(props: { workspace: string; onGo: (path: string) => void }) {
  const [channels, setChannels] = useState<AlertChannel[] | null>(null);
  const [credentials, setCredentials] = useState<CredentialSummary[]>([]);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [tried, setTried] = useState<Record<string, { ok: boolean; text: string }>>({});
  const [dirty, setDirty] = useState(false);
  useEffect(() => {
    api
      .alertChannels(props.workspace)
      .then((saved) => setChannels(MESSENGERS.map((m) => saved.find((c) => c.kind === m.kind) ?? blank(m.kind))))
      .catch((e: Error) => setNote({ ok: false, text: e.message }));
    api.credentials().then(setCredentials).catch(() => undefined);
  }, [props.workspace]);
  if (!channels) return <p className="hint set-loading">{note?.text ?? "…"}</p>;
  const change = (kind: string, patch: Partial<AlertChannel>) => {
    setChannels(channels.map((c) => (c.kind === kind ? { ...c, ...patch } : c)));
    setDirty(true);
    setNote(null);
  };
  const save = async () => {
    try {
      // Only those set up are kept: an untouched messenger stays a blank, ready one.
      const kept = await api.saveAlertChannels(props.workspace, channels.filter((c) => c.credential || c.on));
      setChannels(MESSENGERS.map((m) => kept.find((c) => c.kind === m.kind) ?? blank(m.kind)));
      setDirty(false);
      setNote({ ok: true, text: "Saved." });
    } catch (e) {
      setNote({ ok: false, text: e instanceof Error ? e.message : "cannot save" });
    }
  };
  const test = async (c: AlertChannel) => {
    setTried({ ...tried, [c.kind]: { ok: true, text: "Sending…" } });
    try {
      await api.testAlert(props.workspace, { ...c, on: true });
      setTried({ ...tried, [c.kind]: { ok: true, text: "Sent: look there." } });
    } catch (e) {
      setTried({ ...tried, [c.kind]: { ok: false, text: e instanceof Error ? e.message : "not sent" } });
    }
  };
  const on = channels.filter((c) => c.on).length;
  return (
    <>
      <div className="set-row alerts-intro">
        <div className="set-name">
          <span>Send alerts to</span>
          <span className="hint">What needs someone - records waiting, a workflow failing, a missing credential - goes to each one turned on, once.</span>
        </div>
        <div className="set-control">
          <span className="set-value">{on ? `${on} on` : "None on yet"}</span>
          <button disabled={!dirty} onClick={save}>
            <Sym name="check" />
            Save
          </button>
          {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
        </div>
      </div>
      {MESSENGERS.map((m) => {
        const c = channels.find((x) => x.kind === m.kind) ?? blank(m.kind);
        const usable = credentials.filter((cr) => (TAKES[m.kind] ?? []).includes(cr.kind));
        const ready = !!c.credential && (!m.needed || !!c.target);
        return (
          <div key={m.kind} className={`set-row channel-row${c.on ? " on" : ""}`}>
            <div className="set-name channel-name">
              <span className="channel-icon">
                <Icon kind={m.kind} />
              </span>
              <span>{m.name}</span>
            </div>
            <div className="set-control channel-control">
              <select aria-label={`${m.name}: credential`} value={c.credential} onChange={(e) => change(m.kind, { credential: e.target.value })}>
                <option value="">{usable.length ? "Choose its credential" : "No credential yet"}</option>
                {usable.map((cr) => (
                  <option key={cr.name} value={cr.name}>
                    {cr.name}
                  </option>
                ))}
              </select>
              {m.place ? (
                <input aria-label={`${m.name}: ${m.place}`} placeholder={m.placeholder} value={c.target} onChange={(e) => change(m.kind, { target: e.target.value })} />
              ) : (
                <span className="channel-own hint">its webhook's own place</span>
              )}
              <label className="switch small" title={ready ? (c.on ? "On: click to turn off" : "Off: click to turn on") : "Choose its credential first"}>
                <input type="checkbox" aria-label={`${m.name}: on`} checked={c.on} disabled={!ready && !c.on} onChange={(e) => change(m.kind, { on: e.target.checked })} />
                <span />
              </label>
              <button className="quiet" disabled={!ready} onClick={() => test(c)}>
                <Sym name="send" />
                Send a test
              </button>
              {!usable.length && (
                <button className="quiet link" onClick={() => props.onGo("/credentials")}>
                  Add one ›
                </button>
              )}
              {tried[m.kind] && <span className={tried[m.kind]!.ok ? "ok set-note" : "error set-note"}>{tried[m.kind]!.text}</span>}
            </div>
          </div>
        );
      })}
    </>
  );
}
