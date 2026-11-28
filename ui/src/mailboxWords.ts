// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A mailbox in a few words, for the canvas: what a mailbox step does, and what a mailbox trigger waits for. Kept apart
// from the forms (StepsMailbox.tsx) so the canvas's look needs nothing more.

type Fields = Record<string, unknown>;

function short(text: unknown, max = 28): string {
  const t = String(text ?? "");
  return t.length > max ? `${t.slice(0, max - 1)}…` : t;
}

/** A mailbox step's line: `list unread from billing@... in INBOX - shop-mail`. */
export function mailboxSummary(f: Fields): string {
  const action = String(f.action ?? "list");
  const who = f.credential ? ` - ${short(f.credential, 16)}` : "";
  switch (action) {
    case "list":
      return `list ${f.unseen ? "unread " : ""}${f.from ? `from ${short(f.from, 20)} ` : ""}in ${short(f.folder ?? "INBOX", 16)}${who}`;
    case "move":
      return `move to ${short(f.to_folder, 20)}${who}`;
    case "folders":
      return `list folders${who}`;
    default:
      return `${action} ${short(f.uid ?? "", 20)}${who}`;
  }
}

/** A mailbox trigger's line: what starts a run. */
export function mailboxTriggerLine(s: Fields): string {
  const from = s.from ? ` from ${short(s.from, 20)}` : "";
  return `a new email${from} in ${short(s.folder ?? "INBOX", 16)}${s.credential ? ` - ${short(s.credential, 16)}` : ""}`;
}
