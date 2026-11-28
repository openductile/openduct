// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Email from any mailbox in the builder: the mailbox step's form (list and search, read, mark, move, delete, folders),
// the mailbox trigger's settings (new messages in a folder), the imap credential's fields, and what each says on the
// canvas. Each form writes only the settings its action takes, as the engine reads them.

import { CredentialPicker, Text } from "./Steps";
import { keepFor } from "./StepsStorage";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** A new mailbox step: the inbox's unread messages. */
export function freshMailbox(name: string): Record<string, unknown> {
  return { name, mailbox: { action: "list", credential: "", unseen: true } };
}

const ACTIONS: { action: string; label: string }[] = [
  { action: "list", label: "List or search messages" },
  { action: "read", label: "Read a message (its text and attachments)" },
  { action: "mark", label: "Mark a message seen or flagged" },
  { action: "move", label: "Move a message to a folder" },
  { action: "delete", label: "Delete a message" },
  { action: "folders", label: "List the folders" },
];

/** The settings each action takes, beside the credential and the folder. */
export const MAILBOX_OWN: Record<string, string[]> = {
  list: ["unseen", "flagged", "from", "to", "subject", "text", "since", "before", "limit"],
  read: ["uid", "files", "raw"],
  mark: ["uid", "seen", "flagged"],
  move: ["uid", "to_folder"],
  delete: ["uid"],
  folders: [],
};
const COMMON = ["action", "credential", "folder"];

function Check(props: { label: string; on: boolean; set: (v: boolean) => void }) {
  return (
    <label className="check">
      <input type="checkbox" checked={props.on} onChange={(e) => props.set(e.target.checked)} />
      {props.label}
    </label>
  );
}

/** A mark's choice: set, take away, or leave as it is. */
function Mark(props: { label: string; value: unknown; set: (v: boolean | undefined) => void }) {
  const now = props.value === true ? "set" : props.value === false ? "clear" : "";
  return (
    <label className="narrow">
      {props.label}
      <select value={now} onChange={(e) => props.set(e.target.value === "set" ? true : e.target.value === "clear" ? false : undefined)}>
        <option value="">leave as it is</option>
        <option value="set">set</option>
        <option value="clear">take away</option>
      </select>
    </label>
  );
}

export function MailboxBody(props: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  const action = String(fields.action ?? "list");
  return (
    <>
      <div className="row">
        <label className="narrow">
          Action
          <select value={action} onChange={(e) => props.setBody(keepFor(e.target.value, fields, MAILBOX_OWN, COMMON))}>
            {ACTIONS.map(({ action: a, label }) => (
              <option key={a} value={a}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <CredentialPicker kind="mailbox" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
      </div>
      {action !== "folders" && (
        <Text label="Folder" value={String(fields.folder ?? "")} onChange={(v) => set("folder", v || undefined)} placeholder="INBOX" />
      )}
      {action === "list" && (
        <>
          <div className="row">
            <Check label="Unread only" on={fields.unseen === true} set={(v) => set("unseen", v || undefined)} />
            <Check label="Flagged only" on={fields.flagged === true} set={(v) => set("flagged", v || undefined)} />
          </div>
          <div className="row">
            <Text label="From" value={String(fields.from ?? "")} onChange={(v) => set("from", v || undefined)} placeholder="billing@example.com" />
            <Text label="Subject has" value={String(fields.subject ?? "")} onChange={(v) => set("subject", v || undefined)} placeholder="Invoice" />
          </div>
          <div className="row">
            <Text label="To" value={String(fields.to ?? "")} onChange={(v) => set("to", v || undefined)} />
            <Text label="Text has" value={String(fields.text ?? "")} onChange={(v) => set("text", v || undefined)} />
          </div>
          <div className="row">
            <Text label="Since" value={String(fields.since ?? "")} onChange={(v) => set("since", v || undefined)} placeholder="2026-11-01" />
            <Text label="Before" value={String(fields.before ?? "")} onChange={(v) => set("before", v || undefined)} placeholder="2026-12-01" />
            <label className="narrow">
              At most
              <input type="number" min={1} max={500} value={Number(fields.limit ?? 50)} onChange={(e) => set("limit", Number(e.target.value) || undefined)} />
            </label>
          </div>
        </>
      )}
      {MAILBOX_OWN[action]?.includes("uid") && (
        <Text label="Message (its UID)" value={String(fields.uid ?? "")} onChange={(v) => set("uid", v)} placeholder="{{ value.uid }}" />
      )}
      {action === "read" && (
        <div className="row">
          <Check label="Attachments as files" on={fields.files !== false} set={(v) => set("files", v ? undefined : false)} />
          <Check label="The whole message as a file too (.eml)" on={fields.raw === true} set={(v) => set("raw", v || undefined)} />
        </div>
      )}
      {action === "mark" && (
        <div className="row">
          <Mark label="Seen" value={fields.seen} set={(v) => set("seen", v)} />
          <Mark label="Flagged" value={fields.flagged} set={(v) => set("flagged", v)} />
        </div>
      )}
      {action === "move" && <Text label="To the folder" value={String(fields.to_folder ?? "")} onChange={(v) => set("to_folder", v)} placeholder="Archive" />}
    </>
  );
}

/** The mailbox trigger's settings: new messages in a folder, polled (and waited for, with IDLE). */
export function MailboxTrigger(props: { settings: Fields; onChange: (s: Fields) => void }) {
  const s = props.settings;
  const set = (k: string, v: unknown) => props.onChange({ ...s, [k]: v });
  return (
    <div className="tg-section">
      <div className="row">
        <CredentialPicker kind="mailbox" value={String(s.credential ?? "")} onChange={(v) => set("credential", v)} />
        <Text label="Folder" value={String(s.folder ?? "")} onChange={(v) => set("folder", v || undefined)} placeholder="INBOX" />
      </div>
      <div className="row">
        <Text label="From" value={String(s.from ?? "")} onChange={(v) => set("from", v || undefined)} placeholder="billing@example.com" />
        <Text label="Subject has" value={String(s.subject ?? "")} onChange={(v) => set("subject", v || undefined)} placeholder="Invoice" />
      </div>
      <div className="row">
        <Text label="Look every" value={String(s.every ?? "")} onChange={(v) => set("every", v || undefined)} placeholder="5m (1m to 1d)" />
        <Check label="Unread only" on={s.unseen === true} set={(v) => set("unseen", v || undefined)} />
      </div>
      <Check label="Also wait for news between looks (IDLE, where the server offers it)" on={s.idle === true} set={(v) => set("idle", v || undefined)} />
      <Check label="The messages already there too, the first time" on={s.start === "existing"} set={(v) => set("start", v ? "existing" : undefined)} />
      <p className="tg-note">
        A run for each new message: its uid, subject, from, to, date, text and HTML, and its attachments&apos; names - read the files with a mailbox step&apos;s
        Read. The first look notes where the folder is; a cluster looks once each time, and a restart neither runs a message twice nor passes one by.
      </p>
    </div>
  );
}
