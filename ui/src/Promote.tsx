// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Promotion in the editor: the workflow copied to the next environment's workspace as a new version there, once
// that workspace is found to have every credential, lookup file and table it names - checked first, then done. And
// where each saved version came from, in the Versions view.

import { useState } from "react";
import { deliveryApi, type EnvironmentWorkspace, type PromoteAnswer, type VersionSource } from "./deliveryApi";
import { Sym } from "./Sym";

const ACTIONS: Record<string, string> = { create: "new there", update: "a new version there", unchanged: "the same there already", turn_off: "turned off" };

/** The Promote button and its window: shown when the workspace is an environment with one after it. */
export function Promote(props: { name: string; dirty: boolean }) {
  const [targets, setTargets] = useState<EnvironmentWorkspace[] | null>(null);
  const [to, setTo] = useState("");
  const [checked, setChecked] = useState<PromoteAnswer | null>(null);
  const [note, setNote] = useState<{ ok: boolean; text: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const open = async () => {
    setNote(null);
    setChecked(null);
    try {
      const all = await deliveryApi.environments();
      const here = all.find((w) => w.here);
      const later = all.filter((w) => !w.here && here && w.order > here.order && w.role !== "viewer");
      setTargets(later);
      setTo(later[0]?.id ?? "");
      if (!here) setNote({ ok: false, text: "This workspace is not an environment: mark it in Settings to promote from it." });
      else if (later.length === 0) setNote({ ok: false, text: "No environment after this one that you may change." });
    } catch (err) {
      setNote({ ok: false, text: err instanceof Error ? err.message : "cannot list the environments" });
    }
  };
  const run = async (dryRun: boolean) => {
    setBusy(true);
    setNote(null);
    try {
      const answer = await deliveryApi.promote(to, [props.name], dryRun);
      if (dryRun) setChecked(answer);
      else {
        setChecked(null);
        const item = answer.workflows[0];
        setNote({ ok: true, text: item ? `Promoted: ${props.name} is ${ACTIONS[item.action] ?? item.action}.` : "Promoted." });
      }
    } catch (err) {
      setChecked(null);
      setNote({ ok: false, text: err instanceof Error ? err.message : "cannot promote it" });
    } finally {
      setBusy(false);
    }
  };
  if (!targets)
    return (
      <button className="quiet" onClick={open} title="Copy it to the next environment">
        <Sym name="check" />
        Promote
      </button>
    );
  return (
    <div className="promote" role="dialog" aria-label="Promote the workflow">
      {targets.length > 0 && (
        <span className="set-inline">
          <label>
            To{" "}
            <select aria-label="Promote to" value={to} onChange={(e) => (setTo(e.target.value), setChecked(null))}>
              {targets.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name} ({t.environment})
                </option>
              ))}
            </select>
          </label>
          <button className="quiet" disabled={busy || !to} onClick={() => run(true)}>
            Check
          </button>
          <button disabled={busy || !to || !checked} onClick={() => run(false)} title={props.dirty ? "What is saved is promoted, not the changes not yet saved" : undefined}>
            <Sym name="check" />
            Promote it
          </button>
        </span>
      )}
      {checked?.workflows[0] && (
        <span className="hint" role="status">
          {props.name} will be {ACTIONS[checked.workflows[0].action] ?? checked.workflows[0].action}
          {checked.workflows[0].to_version ? ` (v${checked.workflows[0].to_version})` : ""}: it needs nothing the target lacks.
        </span>
      )}
      {note && <span className={note.ok ? "ok set-note" : "error set-note"}>{note.text}</span>}
      <button className="quiet" aria-label="Close" onClick={() => setTargets(null)}>
        <Sym name="close" />
      </button>
    </div>
  );
}

/** Where a version came from, in words. */
export function SourceOf(props: { source?: VersionSource }) {
  const s = props.source;
  if (!s || s.source === "studio") return <span>Studio</span>;
  const kind = { push: "Push", promotion: "Promotion", rollback: "Rollback", git: "Git" }[s.source] ?? s.source;
  return (
    <span>
      {kind}
      {s.detail ? ` ${s.detail}` : ""}
      {s.token ? <span className="hint"> (token {s.token})</span> : null}
    </span>
  );
}
