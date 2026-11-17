// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A workspace's lookup files: uploaded, shown by their first rows, removed. A lookup step finds rows in them.

import { useEffect, useState } from "react";
import { api, type FilePreview, type FileSummary } from "./api";

const MAX = 5 * 1024 * 1024;

function size(bytes: number) {
  return bytes < 1024 ? `${bytes} B` : bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(1)} KiB` : `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
}

export function Files(props: { canEdit: boolean }) {
  const [list, setList] = useState<FileSummary[] | null>(null);
  const [shown, setShown] = useState<{ path: string; preview: FilePreview } | null>(null);
  const [trouble, setTrouble] = useState("");
  const load = () => api.files().then(setList).catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
  }, []);
  const upload = async (file: File) => {
    setTrouble("");
    if (file.size > MAX) return setTrouble(`${file.name} is over 5 MiB`);
    const path = `/${file.name.replace(/[^A-Za-z0-9._ -]/g, "_")}`;
    try {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = "";
      bytes.forEach((b) => (binary += String.fromCharCode(b)));
      await api.uploadFile(path, { base64: btoa(binary) }, file.type);
      load();
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot upload it");
    }
  };
  const show = async (path: string) => {
    try {
      setShown({ path, preview: await api.filePreview(path) });
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot read it");
    }
  };
  const remove = async (path: string) => {
    await api.removeFile(path).catch((e: Error) => setTrouble(e.message));
    if (shown?.path === path) setShown(null);
    load();
  };
  const kind = (path: string) => (path.split(".").pop() ?? "").toUpperCase().slice(0, 5) || "FILE";
  const ago = (s: number) => {
    const d = Math.max(0, Date.now() / 1000 - s);
    if (d < 60) return "just now";
    if (d < 3600) return `${Math.floor(d / 60)} min ago`;
    if (d < 86400) return `${Math.floor(d / 3600)} h ago`;
    return new Date(s * 1000).toLocaleDateString();
  };
  return (
    <main className="page list-page files-page">
      <h1 className="visually-hidden">Files</h1>
      <div className="files-head">
        <p className="hint">Rates, price lists, mappings - CSV with a header row, JSON lists of records, or JSON lines, up to 5 MiB. A lookup step finds rows in them.</p>
        {props.canEdit && (
          <label className="button upload">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M12 16V4M7 9l5-5 5 5M4 20h16" />
            </svg>
            Upload a file
            <input type="file" accept=".csv,.json,.jsonl,.txt" hidden onChange={(e) => e.target.files?.[0] && upload(e.target.files[0])} />
          </label>
        )}
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {list && list.length === 0 && <div className="empty-note">No files yet: upload a CSV or JSON file for a lookup step to read.</div>}
      {list && list.length > 0 && (
        <table className="compact files-table">
          <thead>
            <tr>
              <th>File</th>
              <th className="num">Size</th>
              <th>Changed</th>
              <th className="num" />
            </tr>
          </thead>
          <tbody>
            {list.map((f) => (
              <tr key={f.path} className={shown?.path === f.path ? "open" : ""}>
                <td>
                  <span className="file-name">
                    <span className={`file-kind kind-${kind(f.path).toLowerCase()}`}>{kind(f.path)}</span>
                    <code>{f.path}</code>
                  </span>
                </td>
                <td className="num muted">{size(f.size)}</td>
                <td className="muted" title={new Date(f.updated * 1000).toLocaleString()}>
                  {ago(f.updated)}
                </td>
                <td className="num">
                  <span className="row-tools">
                    <button className="icon-button" title="Its rows" aria-label={`Rows of ${f.path}`} onClick={() => (shown?.path === f.path ? setShown(null) : show(f.path))}>
                      <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                        <path d="M3 5h18v14H3zM3 10h18M3 15h18M9 5v14" />
                      </svg>
                    </button>
                    {props.canEdit && (
                      <button className="icon-button remove" title="Remove it" aria-label={`Remove ${f.path}`} onClick={() => remove(f.path)}>
                        <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                          <path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3" />
                        </svg>
                      </button>
                    )}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {shown && (
        <section className="file-rows" aria-label={`Rows of ${shown.path}`}>
          <div className="file-rows-head">
            <code>{shown.path}</code>
            <span className="hint">
              {shown.preview.rows} row{shown.preview.rows === 1 ? "" : "s"}
              {shown.preview.first.length < shown.preview.rows ? `, the first ${shown.preview.first.length} shown` : ""}
            </span>
            <button className="popup-close" aria-label="Close the rows" title="Close" onClick={() => setShown(null)}>
              <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
                <path d="M6 6l12 12M18 6 6 18" />
              </svg>
            </button>
          </div>
          <div className="scroll">
            <table className="compact">
              <thead>
                <tr>
                  {shown.preview.columns.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {shown.preview.first.map((row, i) => (
                  <tr key={i}>
                    {shown.preview.columns.map((c) => (
                      <td key={c}>{String(row[c] ?? "")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </main>
  );
}
