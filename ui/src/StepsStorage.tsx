// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Storage in the builder: the forms of the storage step (files in a folder of this machine, on an SFTP or FTP server,
// in S3), the compress step (archives) and the redis step (keys and values), and the settings of the storage and
// redis triggers. Each writes only the settings its action takes, as the engine reads them.

import { useContext, useEffect, useState } from "react";
import { call } from "./api";
import { CredentialPicker, CredentialsContext, Expr, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const STORAGE_KINDS = ["storage", "compress", "redis"];

/** A new step of one of these kinds. */
export function freshStorage(kind: string, name: string): Record<string, unknown> {
  switch (kind) {
    case "compress":
      return { name, compress: { action: "pack", format: "zip", files: "[steps.report]", name: "files.zip" } };
    case "redis":
      return { name, redis: { action: "get", credential: "", key: "{{ value.id }}" } };
    default:
      return { name, storage: { action: "list", credential: "", path: "in" } };
  }
}

const ACTIONS: { action: string; label: string; server?: boolean }[] = [
  { action: "list", label: "List a folder" },
  { action: "read", label: "Read a file" },
  { action: "write", label: "Write a file" },
  { action: "move", label: "Move or rename" },
  { action: "copy", label: "Copy a file" },
  { action: "delete", label: "Delete a file or an empty folder" },
  { action: "make_folder", label: "Make a folder" },
  { action: "presign", label: "A link to share (S3, Azure, Google Cloud)", server: true }, // storage places
];

/** The settings each action takes, beside where the files are. */
const OWN: Record<string, string[]> = {
  list: ["pattern", "recursive", "limit"],
  read: ["export"], // storage places: a Google Doc read as
  write: ["file", "overwrite"],
  move: ["to", "overwrite"],
  copy: ["to", "overwrite"],
  delete: [],
  make_folder: [],
  presign: ["expires", "method"],
};
const COMMON = ["action", "root", "credential", "path", "bucket", "folder_id", "drive"]; // storage places: folder_id, drive

/** A body with only what its action takes. */
export function keepFor(action: string, fields: Fields, own: Record<string, string[]>, common: string[]): Fields {
  const allowed = new Set([...common, ...(own[action] ?? [])]);
  const out: Fields = {};
  for (const [k, v] of Object.entries(fields)) if (allowed.has(k)) out[k] = v;
  out.action = action;
  return out;
}

let rootsSeen: string[] | null = null;

/** The folders of this machine the installation allows: asked once. */
function useRoots(): string[] | null {
  const [roots, setRoots] = useState<string[] | null>(rootsSeen);
  useEffect(() => {
    if (rootsSeen) return;
    call<{ roots: string[] }>("GET", "/api/storage/roots")
      .then((r) => {
        rootsSeen = r.roots;
        setRoots(r.roots);
      })
      .catch(() => setRoots([]));
  }, []);
  return roots;
}

/** Where the files are: a folder of this machine (by its name), or a server (by its credential). */
function Where(props: { fields: Fields; setBody: (v: Fields) => void; trigger?: boolean }) {
  const { fields } = props;
  const roots = useRoots();
  const local = fields.root !== undefined;
  const choose = (next: string) => {
    const body = { ...fields };
    delete body.root;
    delete body.credential;
    if (next === "folder") body.root = roots?.[0] ?? "";
    else body.credential = "";
    props.setBody(body);
  };
  return (
    <>
      <div className="row">
        <label className="narrow">
          Where
          <select value={local ? "folder" : "server"} onChange={(e) => choose(e.target.value)}>
            <option value="server">a server or a store (SFTP, FTP, S3, Azure, Google Cloud, Dropbox, Box, OneDrive, Google Drive)</option>
            <option value="folder">a folder of this machine</option>
          </select>
        </label>
        {local ? (
          <label>
            Folder
            <select value={String(fields.root ?? "")} onChange={(e) => props.setBody({ ...fields, root: e.target.value })}>
              {!roots?.length && <option value="">none allowed here</option>}
              {roots?.map((r) => (
                <option key={r}>{r}</option>
              ))}
            </select>
          </label>
        ) : (
          <CredentialPicker kind="storage" value={String(fields.credential ?? "")} onChange={(v) => props.setBody({ ...fields, credential: v })} />
        )}
      </div>
      {local && roots && roots.length === 0 && (
        <p className="hint">This installation allows no folder yet: its admin starts OpenDuct with --files-root name=/a/folder.</p>
      )}
      {!local && <PlaceFields fields={fields} setBody={props.setBody} />}
    </>
  );
}

/** The kind of the credential a step or a trigger names, when the workspace has it. // storage places */
function useKindOf(name: unknown): string {
  const credentials = useContext(CredentialsContext);
  return credentials.find((c) => c.name === name)?.kind ?? "";
}

/** What a place takes beside its credential (storage places): a bucket or a container, a folder's id, a drive. */
function PlaceFields(props: { fields: Fields; setBody: (v: Fields) => void }) {
  const { fields } = props;
  const kind = useKindOf(fields.credential);
  const put = (k: string) => (v: string) => props.setBody({ ...fields, [k]: v || undefined });
  const byId = ["dropbox", "box", "box_token", "microsoft_365", "google_drive"].includes(kind);
  return (
    <>
      {(kind === "" || kind === "s3" || kind === "gcs") && (
        <Text label="Bucket (S3 or Google Cloud Storage, when the credential names none)" value={String(fields.bucket ?? "")} onChange={put("bucket")} />
      )}
      {kind === "azure_blob" && <Text label="Container (when the credential names none)" value={String(fields.bucket ?? "")} onChange={put("bucket")} />}
      {kind === "microsoft_365" && (
        <Text label="Drive" value={String(fields.drive ?? "")} onChange={put("drive")} placeholder="me (your OneDrive), a drive's id, or site:<a SharePoint site's id>" />
      )}
      {byId && (
        <Text
          label="Inside the folder (its id)"
          value={String(fields.folder_id ?? "")}
          onChange={put("folder_id")}
          placeholder={kind === "dropbox" ? "id:... (the top when empty)" : "the end of its address (the top when empty)"}
        />
      )}
    </>
  );
}

function Check(props: { label: string; on: boolean; set: (v: boolean) => void }) {
  return (
    <label className="check">
      <input type="checkbox" checked={props.on} onChange={(e) => props.set(e.target.checked)} />
      {props.label}
    </label>
  );
}

function FilesBody(props: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  const action = String(fields.action ?? "list");
  const local = fields.root !== undefined;
  const kind = useKindOf(fields.credential); // storage places
  return (
    <>
      <Where fields={fields} setBody={props.setBody} />
      <label>
        What it does
        <select value={action} onChange={(e) => props.setBody(keepFor(e.target.value, fields, OWN, COMMON))}>
          {ACTIONS.filter((a) => !a.server || !local).map((a) => (
            <option key={a.action} value={a.action}>
              {a.label}
            </option>
          ))}
        </select>
      </label>
      <Text
        label={action === "list" ? "Folder" : "Path"}
        value={String(fields.path ?? "")}
        onChange={(v) => set("path", v)}
        placeholder={action === "list" ? "in (the top when empty)" : "in/orders.csv - with {{ }} for values"}
      />
      {action === "read" && kind === "google_drive" && (
        <label className="narrow">
          A Google Doc, Sheet or Slides read as
          <select value={String(fields.export ?? "office")} onChange={(e) => set("export", e.target.value === "office" ? undefined : e.target.value)}>
            <option value="office">Word, Excel, PowerPoint</option>
            <option value="pdf">PDF</option>
            <option value="text">text (a sheet as CSV)</option>
            <option value="open">OpenDocument</option>
          </select>
        </label>
      )}
      {(action === "move" || action === "copy") && <Text label="To" value={String(fields.to ?? "")} onChange={(v) => set("to", v)} placeholder="done/orders.csv" />}
      {action === "write" && <Expr label="File" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} />}
      {["write", "move", "copy"].includes(action) && (
        <Check label="Replace a file already there" on={fields.overwrite === true} set={(v) => set("overwrite", v || undefined)} />
      )}
      {action === "list" && (
        <>
          <div className="row">
            <Text label="Names like" value={String(fields.pattern ?? "")} onChange={(v) => set("pattern", v || undefined)} placeholder="*.csv, *.xlsx" />
            <label className="narrow">
              At most
              <input type="number" min={1} max={10000} value={Number(fields.limit ?? 1000)} onChange={(e) => set("limit", Number(e.target.value) === 1000 ? undefined : Number(e.target.value))} />
            </label>
          </div>
          <Check label="The folders inside it too" on={fields.recursive === true} set={(v) => set("recursive", v || undefined)} />
        </>
      )}
      {action === "presign" && (
        <div className="row">
          <Text label="Works for" value={String(fields.expires ?? "")} onChange={(v) => set("expires", v || undefined)} placeholder="1h (at most 7d)" />
          <label className="narrow">
            Lets its holder
            <select value={String(fields.method ?? "get")} onChange={(e) => set("method", e.target.value === "get" ? undefined : e.target.value)}>
              <option value="get">get the file</option>
              <option value="put">put a file there</option>
            </select>
          </label>
        </div>
      )}
      <p className="hint">
        {action === "read"
          ? "Its result is the file: extract its rows, attach it, or send it on."
          : action === "list"
            ? "Its result's files: each one's name, path, kind, size and when it changed."
            : action === "write"
              ? "The file is a step's file (steps.report) or text. A half-written file is never seen there."
              : "A preview shows what it would do; a test run reaches nothing."}
      </p>
    </>
  );
}

const ARCHIVE_OWN: Record<string, string[]> = { pack: ["format", "files", "name"], unpack: ["format", "file", "pattern"], list: ["format", "file"] };

function ArchiveBody(props: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  const action = String(fields.action ?? "pack");
  return (
    <>
      <div className="row">
        <label className="narrow">
          What it does
          <select value={action} onChange={(e) => props.setBody(keepFor(e.target.value, fields, ARCHIVE_OWN, ["action"]))}>
            <option value="pack">pack files into an archive</option>
            <option value="unpack">unpack an archive's files</option>
            <option value="list">list an archive's files</option>
          </select>
        </label>
        <label className="narrow">
          Format
          <select value={String(fields.format ?? (action === "pack" ? "zip" : "auto"))} onChange={(e) => set("format", e.target.value === "auto" ? undefined : e.target.value)}>
            {action !== "pack" && <option value="auto">from the archive</option>}
            {["zip", "gzip", "tar", "tar.gz"].map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
      </div>
      {action === "pack" ? (
        <>
          <Expr label="Files" value={String(fields.files ?? "")} onChange={(v) => set("files", v)} />
          <Text label="Archive's name" value={String(fields.name ?? "")} onChange={(v) => set("name", v || undefined)} placeholder="files.zip" />
        </>
      ) : (
        <>
          <Expr label="Archive" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} />
          {action === "unpack" && <Text label="Only names like" value={String(fields.pattern ?? "")} onChange={(v) => set("pattern", v || undefined)} placeholder="*.csv" />}
        </>
      )}
      <p className="hint">An archive that would unpack to far more than it holds is refused.</p>
    </>
  );
}

const REDIS_OWN: Record<string, string[]> = {
  get: ["key"],
  exists: ["key"],
  delete: ["key"],
  set: ["key", "value", "ttl", "if_missing"],
  increment: ["key", "by"],
  push: ["key", "value", "end"],
  pop: ["key", "end"],
  range: ["key", "start", "stop"],
  publish: ["channel", "value"],
  expire: ["key", "ttl"],
};
const REDIS_ACTIONS: [string, string][] = [
  ["get", "Get a value"],
  ["set", "Set a value"],
  ["delete", "Delete a key"],
  ["exists", "Whether a key is there"],
  ["increment", "Count up"],
  ["push", "Add to a list"],
  ["pop", "Take from a list"],
  ["range", "Read a list"],
  ["expire", "Keep a key for a while"],
  ["publish", "Publish to a channel"],
];

function RedisBody(props: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  const action = String(fields.action ?? "get");
  const own = REDIS_OWN[action] ?? [];
  return (
    <>
      <CredentialPicker kind="redis" value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
      <label>
        What it does
        <select value={action} onChange={(e) => props.setBody(keepFor(e.target.value, fields, REDIS_OWN, ["action", "credential"]))}>
          {REDIS_ACTIONS.map(([a, label]) => (
            <option key={a} value={a}>
              {label}
            </option>
          ))}
        </select>
      </label>
      {own.includes("key") && <Text label="Key" value={String(fields.key ?? "")} onChange={(v) => set("key", v)} placeholder="order:{{ value.id }}" />}
      {own.includes("channel") && <Text label="Channel" value={String(fields.channel ?? "")} onChange={(v) => set("channel", v)} placeholder="orders" />}
      {own.includes("value") && <Text label="Value" value={String(fields.value ?? "")} onChange={(v) => set("value", v)} placeholder="{{ value }} - a record is kept as JSON" />}
      {own.includes("ttl") && <Text label="Kept for" value={String(fields.ttl ?? "")} onChange={(v) => set("ttl", v || undefined)} placeholder={action === "expire" ? "1h" : "always (or 1h, 1d)"} />}
      {own.includes("if_missing") && <Check label="Only when the key has no value yet" on={fields.if_missing === true} set={(v) => set("if_missing", v || undefined)} />}
      {own.includes("by") && <Text label="By" value={String(fields.by ?? "")} onChange={(v) => set("by", v || undefined)} placeholder="1" />}
      {own.includes("end") && (
        <label className="narrow">
          End of the list
          <select value={String(fields.end ?? "")} onChange={(e) => set("end", e.target.value || undefined)}>
            <option value="">{action === "push" ? "the end (a queue)" : "the start (a queue)"}</option>
            <option value="left">the start</option>
            <option value="right">the end</option>
          </select>
        </label>
      )}
      {own.includes("start") && (
        <div className="row">
          <label className="narrow">
            From
            <input type="number" value={Number(fields.start ?? 0)} onChange={(e) => set("start", Number(e.target.value) || undefined)} />
          </label>
          <label className="narrow">
            To (-1: the last)
            <input type="number" value={Number(fields.stop ?? -1)} onChange={(e) => set("stop", Number(e.target.value) === -1 ? undefined : Number(e.target.value))} />
          </label>
        </div>
      )}
    </>
  );
}

export function StorageBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  if (props.kind === "compress") return <ArchiveBody {...props} />;
  if (props.kind === "redis") return <RedisBody {...props} />;
  return <FilesBody {...props} />;
}

/** A storage trigger's settings (new files in a folder, polled) or a redis trigger's (a channel's messages). */
export function StorageTrigger(props: { kind: "storage" | "redis"; settings: Fields; onChange: (s: Fields) => void }) {
  const s = props.settings;
  const set = (k: string, v: unknown) => props.onChange({ ...s, [k]: v });
  if (props.kind === "redis") {
    return (
      <div className="tg-section">
        <CredentialPicker kind="redis" value={String(s.credential ?? "")} onChange={(v) => set("credential", v)} />
        <Text label="Channel" value={String(s.channel ?? "")} onChange={(v) => set("channel", v)} placeholder="orders, or orders.* for several" />
        <p className="tg-note">
          A run for each message, its value the message (a record when it is JSON). One node of a cluster listens; a channel keeps nothing, so what is
          published while none does is not seen - push to a list and pop it on a schedule for messages that must not be lost.
        </p>
      </div>
    );
  }
  return (
    <div className="tg-section">
      <Where fields={s} setBody={props.onChange} trigger />
      <div className="row">
        <Text label="Folder" value={String(s.folder ?? "")} onChange={(v) => set("folder", v)} placeholder="in (the top when empty)" />
        <Text label="Names like" value={String(s.pattern ?? "")} onChange={(v) => set("pattern", v || undefined)} placeholder="*.csv" />
      </div>
      <div className="row">
        <Text label="Look every" value={String(s.every ?? "")} onChange={(v) => set("every", v || undefined)} placeholder="5m (10s to 1d)" />
        <label className="narrow">
          Runs for
          <select value={String(s.when ?? "new")} onChange={(e) => set("when", e.target.value === "new" ? undefined : e.target.value)}>
            <option value="new">new files</option>
            <option value="new_or_changed">new and changed files</option>
          </select>
        </label>
      </div>
      <Check label="The files already there too, the first time" on={s.start === "existing"} set={(v) => set("start", v ? "existing" : undefined)} />
      <p className="tg-note">
        A run for each file, its value the file's name, path, size and when it changed: read it with a storage step. The first look notes what is there;
        a cluster looks once each time, and a restart neither runs a file twice nor passes one by.
      </p>
    </div>
  );
}
