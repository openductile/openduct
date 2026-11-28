// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The window for adding a step: every step and app, by family, found by name or by what it does. The arrow keys
// move through what is found, Enter adds it, Escape closes.

import { useEffect, useMemo, useRef, useState } from "react";
import { KINDS } from "../Steps";
import { FAMILY, type Family } from "./look";
import { Icon } from "./Icon";

const FAMILIES: { family: Family; label: string }[] = [
  { family: "ai", label: "AI" },
  { family: "data", label: "Data" },
  { family: "logic", label: "Logic" },
  { family: "call", label: "Calls and topics" },
  { family: "message", label: "Messages" },
  { family: "code", label: "Code" },
  { family: "control", label: "Control" },
];

/** More words a step is found by. */
const WORDS: Record<string, string> = {
  ai: "llm model chat prompt summarise summarize classify extract write answer",
  embed: "vector embedding rag search similar meaning",
  // G7 more AI steps
  classify: "classify classifier categorise categorize sort route label intent topic triage",
  sentiment: "sentiment mood tone feeling positive negative review emotion",
  extract: "extract extractor parse fields structured output json schema information entities",
  summarise: "summarise summarize summary digest shorten condense tldr",
  agent: "agent tools tool calling assistant chatbot memory goal autonomous function", // G2 AI agents
  split_text: "split chunk chunks splitter text document loader pieces paragraphs markdown html rag", // retrieval
  vector_store: "vector store pgvector embeddings index similarity search nearest knowledge base rag upsert delete qdrant weaviate milvus zilliz chroma pinecone redis mongodb atlas memory", // retrieval, vector stores
  ask_documents: "rag answer question documents knowledge base sources citations retrieval qa chat docs", // retrieval
  set: "map fields transform compute shape",
  where: "filter condition only when skip keep drop",
  if: "branch condition else decide split route",
  switch: "switch case route router several ways choose decide branch multiple",
  list: "filter sort order unique dedupe duplicates distinct limit top first array items",
  group: "group summarise summarize aggregate count sum total average mean min max per pivot",
  merge: "merge combine join append concat union lists records together",
  datetime: "date time timezone zone format parse add subtract now today tomorrow calendar",
  respond: "respond answer reply webhook response status return caller http",
  run_workflow: "run call workflow subworkflow sub reuse another execute",
  for_each: "loop each list items repeat iterate",
  http: "api rest request call get post webhook",
  mcp: "mcp model context protocol tool tools server assistant agent ai", // MCP
  lookup: "csv file table find row rates",
  table: "data table rows database store save insert update upsert delete count find keep", // data tables
  state: "remember static data state keep between runs counter last seen memory", // data tables
  postgres: "sql database query insert pgvector rag",
  mysql: "sql database query insert select mariadb", // sql databases
  sqlserver: "sql database query insert select mssql microsoft azure t-sql", // sql databases
  email: "mail smtp send",
  telegram: "chat message bot notify",
  slack: "chat message channel notify",
  discord: "chat message channel notify",
  teams: "chat message channel notify microsoft",
  google_chat: "chat message space notify google workspace gchat",
  whatsapp: "chat message phone notify",
  topic: "kafka stream write record event queue",
  javascript: "js code snippet script function",
  python: "py code snippet script",
  wait: "delay sleep pause",
  stop: "end fail finish",
  // G6 files
  extract_file: "file read parse csv json xlsx xls excel spreadsheet sheet pdf text html upload attachment binary import",
  convert: "file write make csv json xlsx excel spreadsheet text export report base64 download attachment binary",
  file_details: "file name rename type size mime extension binary",
  // storage
  storage: "file files folder disk sftp ftp ftps s3 bucket minio r2 wasabi backblaze upload download list move copy delete local on-premise share azure blob container gcs google cloud storage dropbox box onedrive sharepoint drive google drive docs", // storage places
  compress: "zip unzip gzip tar archive pack unpack compress extract",
  redis: "redis cache key value counter queue list publish channel valkey",
  web: "web page site scrape scraping crawl crawler spider fetch html markdown text url link links selector css sitemap rss atom feed news blog read extract", // web
  mailbox: "email mail imap inbox mailbox gmail outlook message attachment invoice read unread folder move archive", // messaging
};

function matches(query: string, kind: { kind: string; label: string; hint: string }): boolean {
  const text = `${kind.kind} ${kind.label} ${kind.hint} ${WORDS[kind.kind] ?? ""} ${FAMILY[kind.kind] ?? ""}`.toLowerCase();
  return query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .every((word) => text.includes(word));
}

export function AddStepDialog(props: { where?: string; onPick: (kind: string) => void; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  const groups = useMemo(
    () =>
      FAMILIES.map((f) => ({ ...f, kinds: KINDS.filter((k) => FAMILY[k.kind] === f.family && matches(query, k)) })).filter(
        (g) => g.kinds.length > 0,
      ),
    [query],
  );
  const flat = groups.flatMap((g) => g.kinds);
  useEffect(() => input.current?.focus(), []);
  useEffect(() => setActive(0), [query]);
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") props.onClose();
    else if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((a) => Math.min(a + 1, flat.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === "Enter" && flat[active]) props.onPick(flat[active].kind);
  };
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <div className="modal add-step" role="dialog" aria-modal="true" aria-label="Add a step" onKeyDown={onKey}>
        <header>
          <h3>Add a step</h3>
          {props.where && <span className="hint">{props.where}</span>}
          <button className="popup-close" aria-label="Close" title="Close (Esc)" onClick={props.onClose}>
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
              <path d="M6 6l12 12M18 6 6 18" />
            </svg>
          </button>
        </header>
        <input
          ref={input}
          className="search"
          type="search"
          placeholder="Search steps and apps: AI, database, Slack, email, loop..."
          aria-label="Search steps and apps"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <div className="found">
          {groups.length === 0 && <p className="hint">Nothing matches "{query}".</p>}
          {groups.map((g) => (
            <section key={g.family} className={`step-family family-${g.family}`}>
              <h4>
                <span className="family-dot" aria-hidden="true" />
                {g.label}
                <span className="family-count">{g.kinds.length}</span>
              </h4>
              <div className="choices">
                {g.kinds.map((k) => {
                  const i = flat.indexOf(k);
                  return (
                    <button
                      key={k.kind}
                      className={`choice family-${g.family}${i === active ? " active" : ""}`}
                      onMouseEnter={() => setActive(i)}
                      onClick={() => props.onPick(k.kind)}
                    >
                      <span className="icon">
                        <Icon kind={k.kind} />
                      </span>
                      <span className="words">
                        <strong>{k.label}</strong>
                        <span>{k.hint}</span>
                      </span>
                    </button>
                  );
                })}
              </div>
            </section>
          ))}
        </div>
      </div>
    </div>
  );
}
