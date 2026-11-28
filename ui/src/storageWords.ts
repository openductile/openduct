// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Storage in a few words, for the canvas: what a storage, compress or redis step does, and what a storage or redis
// trigger waits for. Kept apart from the forms (StepsStorage.tsx) so the canvas's look needs nothing more.

type Fields = Record<string, unknown>;

function short(text: unknown, max = 32): string {
  const s = String(text ?? "");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

/** Where the files are: a folder's name, or a credential's. */
function where(f: Fields): string {
  if (f.root) return `folder ${short(f.root, 16)}`;
  if (f.credential) return short(f.credential, 16);
  return "a place";
}

const VERBS: Record<string, string> = {
  list: "list",
  read: "read",
  write: "write",
  move: "move",
  copy: "copy",
  delete: "delete",
  make_folder: "make folder",
  presign: "link to",
};

/** A storage step's line: `read in/orders.csv - partner`. */
export function storageSummary(kind: string, f: Fields): string {
  if (kind === "compress") {
    const action = String(f.action ?? "pack");
    if (action === "pack") return `pack ${short(f.format ?? "zip", 8)}${f.name ? ` ${short(f.name, 24)}` : ""}`;
    return `${action} ${short(f.file, 28)}`;
  }
  if (kind === "redis") {
    const action = String(f.action ?? "get");
    const what = action === "publish" ? short(f.channel, 24) : short(f.key, 24);
    return `${action} ${what}`;
  }
  const action = String(f.action ?? "list");
  const path = short(f.path ?? "", 26) || "the top";
  return `${VERBS[action] ?? action} ${path} - ${where(f)}`;
}

/** A storage trigger's line: what starts a run. */
export function storageTriggerLine(kind: string, s: Fields): string {
  if (kind === "redis") return `a message on ${short(s.channel ?? "?", 24)}`;
  const folder = String(s.folder ?? "") || "the top";
  const which = s.pattern ? ` (${short(s.pattern, 16)})` : "";
  return `a new file${which} in ${short(folder, 24)} - ${where(s)}`;
}
