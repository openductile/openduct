// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// vector stores: where an agent's memory is kept - OpenDuct's own (the default), a PostgreSQL table, Redis keys, or a
// data table - written into the step's memory as the engine reads it.

import { useContext } from "react";
import { CredentialsContext, Text } from "./Steps";

type Fields = Record<string, unknown>;

/** The places a memory is kept in, and what each needs. */
export const KEPT_IN: { id: string; name: string; hint: string }[] = [
  { id: "broker", name: "OpenDuct (the default)", hint: "Kept by OpenDuct itself, the same on every node." },
  { id: "postgres", name: "A PostgreSQL table", hint: "One row a conversation; the table is made when asked." },
  { id: "redis", name: "Redis", hint: "One key a conversation (openduct:memory:...), forgotten by its TTL." },
  { id: "table", name: "A data table", hint: "Its columns: key (text), turns (JSON), forget_at (date and time). A row holds 64 KiB: the oldest turns are let go." },
];

/** The memory's store, as a step's `memory` names it, with what that store takes. */
export function MemoryStoreFields({ memory, put }: { memory: Fields; put: (m: Fields) => void }) {
  const credentials = useContext(CredentialsContext);
  const store = typeof memory.store === "string" ? memory.store : "broker";
  const look = KEPT_IN.find((k) => k.id === store) ?? KEPT_IN[0]!;
  const choose = (id: string) => {
    const next: Fields = { ...memory };
    for (const name of ["store", "credential", "table", "create"]) delete next[name];
    if (id !== "broker") next.store = id;
    if (id === "postgres") Object.assign(next, { credential: "", table: "agent_memory", create: true });
    if (id === "redis") next.credential = "";
    if (id === "table") next.table = "agent_memory";
    put(next);
  };
  const usable = credentials.filter((c) => c.kind === store);
  return (
    <>
      <div className="row top">
        <label>
          Kept in
          <select value={store} onChange={(e) => choose(e.target.value)}>
            {KEPT_IN.map((k) => (
              <option key={k.id} value={k.id}>
                {k.name}
              </option>
            ))}
          </select>
        </label>
        {(store === "postgres" || store === "redis") && (
          <label>
            {store === "postgres" ? "Database" : "Server"}
            <select value={String(memory.credential ?? "")} onChange={(e) => put({ ...memory, credential: e.target.value })}>
              <option value="">{usable.length ? "choose one" : "none yet: add one"}</option>
              {usable.map((c) => (
                <option key={c.name} value={c.name}>
                  {c.name} ({c.kind})
                </option>
              ))}
            </select>
          </label>
        )}
        {(store === "postgres" || store === "table") && (
          <Text label="Table" value={String(memory.table ?? "")} onChange={(v) => put({ ...memory, table: v })} placeholder="agent_memory" />
        )}
        {store === "postgres" && (
          <label className="check">
            <input type="checkbox" checked={memory.create === true} onChange={(e) => put({ ...memory, create: e.target.checked || undefined })} />
            Make the table when it is not there
          </label>
        )}
      </div>
      <p className="hint">{look.hint}</p>
    </>
  );
}
