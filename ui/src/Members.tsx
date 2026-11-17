// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A workspace's members and their roles; for the installation's admin, the workspaces themselves.

import { Sym } from "./Sym";
import { useEffect, useState, type FormEvent } from "react";
import { api, type Member, type Role } from "./api";

const ROLES: { role: Role; label: string }[] = [
  { role: "admin", label: "Admin - members too" },
  { role: "editor", label: "Editor - builds and runs" },
  { role: "viewer", label: "Viewer - looks" },
];

export function Members(props: { workspace: string; me: string }) {
  const [members, setMembers] = useState<Member[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const load = () => api.members(props.workspace).then(setMembers).catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [props.workspace]);
  const act = async (done: Promise<unknown>) => {
    setTrouble("");
    await done.catch((e: Error) => setTrouble(e.message));
    load();
  };
  return (
    <main className="page">
      <h1 className="visually-hidden">Members</h1>
      {trouble && <p className="error banner">{trouble}</p>}
      {members && (
        <table className="list">
          <thead>
            <tr>
              <th>Name</th>
              <th>E-mail</th>
              <th>Role</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {members.map((m) => (
              <tr key={m.user}>
                <td>{m.name}</td>
                <td>{m.email}</td>
                <td>
                  <select value={m.role} onChange={(e) => act(api.setRole(props.workspace, m.user, e.target.value as Role))} aria-label={`${m.name}'s role`}>
                    {ROLES.map((r) => (
                      <option key={r.role} value={r.role}>
                        {r.label}
                      </option>
                    ))}
                  </select>
                </td>
                <td className="actions">
                  {m.user !== props.me && (
                    <button className="quiet danger-quiet" onClick={() => act(api.removeMember(props.workspace, m.user))}>
                      <Sym name="trash" />
                      Remove
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <AddMember onAdd={(m) => act(api.addMember(props.workspace, m))} />
    </main>
  );
}

function AddMember(props: { onAdd: (m: { email: string; name: string; password: string; role: Role }) => Promise<void> }) {
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<Role>("editor");
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    await props.onAdd({ email, name, password, role });
    setEmail("");
    setName("");
    setPassword("");
  };
  return (
    <form className="add-member" onSubmit={submit}>
      <div className="add-member-head">
        <span className="add-member-title">Add someone</span>
        <span className="hint">Someone new gets an account: give them a first password, to change once they sign in.</span>
      </div>
      <div className="add-member-row">
        <input type="email" aria-label="E-mail address" placeholder="E-mail address" value={email} onChange={(e) => setEmail(e.target.value)} required />
        <input aria-label="Name" placeholder="Name (if new)" value={name} onChange={(e) => setName(e.target.value)} />
        <input type="password" aria-label="First password" placeholder="First password (if new)" autoComplete="new-password" value={password} onChange={(e) => setPassword(e.target.value)} />
        <select aria-label="Role" value={role} onChange={(e) => setRole(e.target.value as Role)}>
          {ROLES.map((r) => (
            <option key={r.role} value={r.role}>
              {r.label}
            </option>
          ))}
        </select>
        <button>
          <Sym name="person" />
          Add
        </button>
      </div>
    </form>
  );
}
