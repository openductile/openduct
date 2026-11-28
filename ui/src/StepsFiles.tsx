// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Files between steps: the forms of the steps that read and make files (extract, convert, file details), the file
// settings of an HTTP request and an e-mail, and the files a run's record names - each with a download for a kept one.

import { useState } from "react";
import { api } from "./api";
import { Area, Expr, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const FILE_KINDS = ["extract_file", "convert", "file_details"];

/** A new step of one of these kinds. */
export function freshFile(kind: string, name: string): Record<string, unknown> {
  switch (kind) {
    case "extract_file":
      return { name, extract_file: { file: "value.upload" } };
    case "convert":
      return { name, convert: { format: "csv", rows: "value.items", name: "items.csv" } };
    default:
      return { name, file_details: { file: "value.upload" } };
  }
}

const READ = ["auto", "csv", "json", "xlsx", "xls", "text", "html", "pdf", "base64"];
const WRITE = ["csv", "json", "xlsx", "text", "html", "base64"];
/** What each format a file is made in is made of. */
const MADE_OF: Record<string, string> = { csv: "rows", xlsx: "rows", json: "data", text: "text", html: "text", base64: "base64" };

export function FilesBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  if (props.kind === "extract_file") {
    const format = String(fields.format ?? "auto");
    return (
      <>
        <Expr label="File" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} />
        <div className="row">
          <label className="narrow">
            Format
            <select value={format} onChange={(e) => set("format", e.target.value === "auto" ? undefined : e.target.value)}>
              {READ.map((f) => (
                <option key={f} value={f}>
                  {f === "auto" ? "from its name or type" : f}
                </option>
              ))}
            </select>
          </label>
          {["auto", "xlsx", "xls"].includes(format) && (
            <Text label="Sheet" value={String(fields.sheet ?? "")} onChange={(v) => set("sheet", v || undefined)} placeholder="the first" />
          )}
          {["auto", "csv"].includes(format) && (
            <Text label="Delimiter" value={String(fields.delimiter ?? "")} onChange={(v) => set("delimiter", v || undefined)} placeholder=", ; or tab" />
          )}
        </div>
        <label className="check">
          <input type="checkbox" checked={fields.header !== false} onChange={(e) => set("header", e.target.checked ? undefined : false)} />
          The first row names the columns
        </label>
        <p className="hint">
          Rows from CSV and spreadsheets (.rows), a value from JSON (.data), text from text, web pages and PDFs (.text), or
          Base64. The file is a step's file (steps.download.body) or an upload (value.photo).
        </p>
      </>
    );
  }
  if (props.kind === "convert") {
    const format = String(fields.format ?? "csv");
    const from = MADE_OF[format] ?? "rows";
    const setFormat = (next: string) => {
      const body: Fields = { ...fields, format: next };
      const was = body[from];
      for (const k of ["rows", "data", "text", "base64"]) delete body[k];
      body[MADE_OF[next] ?? "rows"] = was ?? "";
      props.setBody(body);
    };
    const columns = Array.isArray(fields.columns) ? (fields.columns as string[]).join(", ") : "";
    return (
      <>
        <div className="row">
          <label className="narrow">
            Format
            <select value={format} onChange={(e) => setFormat(e.target.value)}>
              {WRITE.map((f) => (
                <option key={f}>{f}</option>
              ))}
            </select>
          </label>
          <Text label="File name" value={String(fields.name ?? "")} onChange={(v) => set("name", v || undefined)} placeholder="orders.csv" />
        </div>
        {from === "text" ? (
          <Area label="Text" hint="with {{ }} for values" value={String(fields.text ?? "")} onChange={(v) => set("text", v)} />
        ) : (
          <Expr
            label={from === "rows" ? "Rows" : from === "data" ? "Value" : "Base64 text"}
            value={String(fields[from] ?? "")}
            onChange={(v) => set(from, v)}
          />
        )}
        {from === "rows" && (
          <div className="row">
            <Text
              label="Columns"
              value={columns}
              onChange={(v) => {
                const list = v.split(",").map((c) => c.trim()).filter(Boolean);
                set("columns", list.length ? list : undefined);
              }}
              placeholder="every column"
            />
            {format === "xlsx" && <Text label="Sheet" value={String(fields.sheet ?? "")} onChange={(v) => set("sheet", v || undefined)} placeholder="Sheet1" />}
            {format === "csv" && (
              <Text label="Delimiter" value={String(fields.delimiter ?? "")} onChange={(v) => set("delimiter", v || undefined)} placeholder="," />
            )}
          </div>
        )}
        <p className="hint">Its result is the file: send it in an HTTP request, attach it to an e-mail, or read it again.</p>
      </>
    );
  }
  return (
    <>
      <Expr label="File" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} />
      <div className="row">
        <Text label="New name" value={String(fields.name ?? "")} onChange={(v) => set("name", v || undefined)} placeholder="the same" />
        <Text label="New type" value={String(fields.type ?? "")} onChange={(v) => set("type", v || undefined)} placeholder="the same" />
      </div>
      <p className="hint">Its name, type, size, extension and SHA-256 - and the file again, renamed when asked.</p>
    </>
  );
}

/** An HTTP request's file settings: a file or a form with files as its body, and what its answer is made. */
export function HttpFiles(props: { fields: Fields; set: Put; bodyKind: string }) {
  const { fields, set } = props;
  const parts = (fields.multipart ?? {}) as Record<string, unknown>;
  const rows = Object.entries(parts);
  const write = (next: [string, unknown][]) => set("multipart", Object.fromEntries(next));
  return (
    <>
      {props.bodyKind === "file" && (
        <Expr label="The file to send" value={String(fields.file ?? "")} onChange={(v) => set("file", v)} />
      )}
      {props.bodyKind === "multipart" && (
        <div className="pairs">
          {rows.map(([name, value], i) => {
            const file = value && typeof value === "object" ? String((value as { file?: unknown }).file ?? "") : null;
            return (
              <div className="pair" key={i}>
                <input aria-label="Field" placeholder="field" value={name} onChange={(e) => write(rows.map((r, j) => (j === i ? [e.target.value, r[1]] : r)))} />
                <select
                  aria-label="Field kind"
                  value={file === null ? "text" : "file"}
                  onChange={(e) => write(rows.map((r, j) => (j === i ? [r[0], e.target.value === "file" ? { file: "" } : ""] : r)))}
                >
                  <option value="text">text</option>
                  <option value="file">file</option>
                </select>
                <input
                  aria-label={file === null ? "Text" : "File"}
                  className="code"
                  placeholder={file === null ? "text, with {{ }}" : "steps.report"}
                  value={file ?? String(value ?? "")}
                  onChange={(e) => write(rows.map((r, j) => (j === i ? [r[0], file === null ? e.target.value : { file: e.target.value }] : r)))}
                />
                <button className="icon-button remove" title="Remove the field" aria-label="Remove the field" onClick={() => write(rows.filter((_, j) => j !== i))}>
                  ✕
                </button>
              </div>
            );
          })}
          <button className="quiet add-row" onClick={() => write([...rows, [`field_${rows.length + 1}`, ""]])}>
            + Field
          </button>
        </div>
      )}
      <label className="narrow">
        Its answer
        <select value={String(fields.response ?? "auto")} onChange={(e) => set("response", e.target.value === "auto" ? undefined : e.target.value)}>
          <option value="auto">a file when it is one</option>
          <option value="file">always a file</option>
          <option value="text">always text</option>
        </select>
      </label>
    </>
  );
}

/** An e-mail's attachments: an expression giving a file or a list of them. */
export function Attachments(props: { value: unknown; onChange: (v: unknown) => void }) {
  const text = Array.isArray(props.value) ? `[${(props.value as unknown[]).map(String).join(", ")}]` : String(props.value ?? "");
  return <Expr label="Attachments (optional)" value={text} onChange={(v) => props.onChange(v.trim() ? v : undefined)} />;
}

export type FileValue = { file: string; name: string; type: string; size: number };

const REFERENCE = /^(m|\d+\.\d+)\/[0-9a-f]{64}$/;

/** Every file a value names, looked for a few levels deep. */
export function filesIn(value: unknown, depth = 0, found: FileValue[] = []): FileValue[] {
  if (!value || typeof value !== "object" || depth > 6 || found.length >= 50) return found;
  const v = value as Record<string, unknown>;
  if (typeof v.file === "string" && REFERENCE.test(v.file) && typeof v.name === "string") {
    if (!found.some((f) => f.file === v.file && f.name === v.name)) {
      found.push({ file: v.file, name: v.name, type: String(v.type ?? ""), size: Number(v.size ?? 0) });
    }
    return found;
  }
  for (const inner of Array.isArray(value) ? value : Object.values(v)) filesIn(inner, depth + 1, found);
  return found;
}

export function size(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

/** The files a step's result (or a run's event) names: name, type, size, and a download for a kept one. */
export function FilesIn(props: { value: unknown; label?: string }) {
  const files = filesIn(props.value);
  const [trouble, setTrouble] = useState("");
  if (files.length === 0) return null;
  return (
    <div className="files-in">
      <p className="hint">{props.label ?? "Files"}</p>
      <ul>
        {files.map((f) => (
          <li key={`${f.file}-${f.name}`}>
            <strong>{f.name}</strong> <span className="hint">{f.type} - {size(f.size)}</span>{" "}
            {f.file.startsWith("m/") ? (
              <span className="hint">held for this test only</span>
            ) : (
              <button
                className="link"
                onClick={() => {
                  setTrouble("");
                  api.downloadRunFile(f.file, f.name).catch((e: Error) => setTrouble(e.message));
                }}
              >
                download
              </button>
            )}
          </li>
        ))}
      </ul>
      {trouble && <p className="error-text">{trouble}</p>}
    </div>
  );
}
