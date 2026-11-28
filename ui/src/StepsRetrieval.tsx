// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The forms of the retrieval steps: split text into pieces, keep them in a vector store (add, search, delete), and
// answer a question from the documents with every passage shown. Each writes the step as the engine reads it.

import { useContext } from "react";
import type { Step } from "./api";
import { Area, CredentialsContext, Expr, ModelPicker, Pairs, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** The steps with a form here. */
export const RETRIEVAL_KINDS = ["split_text", "vector_store", "ask_documents"];

/** A new retrieval step, ready to fill in. */
export function retrievalStep(kind: string, name: string): Step {
  switch (kind) {
    case "split_text":
      return { name, split_text: { text: "{{ value.text }}", source: "{{ value.title }}", by: "paragraphs", size: 1000 } };
    case "vector_store":
      return { name, vector_store: { action: "add", store: "postgres", credential: "", table: "doc_pieces", embed_with: "", create: true, pieces: "steps.split.pieces" } };
    default:
      return { name, ask_documents: { store: "postgres", credential: "", table: "doc_pieces", embed_with: "", question: "{{ value.question }}", top_k: 4 } };
  }
}

export function RetrievalBody(props: { kind: string; fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  switch (props.kind) {
    case "split_text":
      return <SplitBody fields={props.fields} set={props.set} setBody={props.setBody} />;
    case "vector_store":
      return <StoreBody fields={props.fields} set={props.set} setBody={props.setBody} />;
    default:
      return <AskBody fields={props.fields} set={props.set} setBody={props.setBody} />;
  }
}

/** A credential of one of `kinds`, by name, under its own label: a step here names two. */
function Picker(props: { label: string; kinds: string[]; value: string; onChange: (v: string) => void; optional?: string }) {
  const credentials = useContext(CredentialsContext).filter((c) => props.kinds.includes(c.kind));
  return (
    <label>
      {props.label} {props.optional && <span className="hint">{props.optional}</span>}
      <select value={props.value} onChange={(e) => props.onChange(e.target.value)}>
        <option value="">{props.optional ? "the same" : credentials.length ? "choose one" : "none yet: add one"}</option>
        {credentials.map((c) => (
          <option key={c.name} value={c.name}>
            {c.name} ({c.kind})
          </option>
        ))}
      </select>
    </label>
  );
}

function NumberField(props: { label: string; value: unknown; onChange: (v: number | undefined) => void; min?: number; max?: number; step?: string; hint?: string }) {
  return (
    <label className="narrow">
      {props.label} {props.hint && <span className="hint">{props.hint}</span>}
      <input
        type="number"
        min={props.min}
        max={props.max}
        step={props.step}
        value={props.value === undefined || props.value === null ? "" : String(props.value)}
        onChange={(e) => props.onChange(e.target.value === "" ? undefined : Number(e.target.value))}
      />
    </label>
  );
}

// vector stores: where the pieces are kept - PostgreSQL with pgvector, a vector database, or the run's own memory.
export type StoreLook = { id: string; name: string; holder: string; placeholder: string; hint: string };

/** Each store a vector store step keeps its pieces in: its credential is of the kind of its id (none for memory). */
export const STORES: StoreLook[] = [
  { id: "postgres", name: "PostgreSQL (pgvector)", holder: "Table", placeholder: "doc_pieces", hint: "The table's name is small letters, digits and _." },
  { id: "qdrant", name: "Qdrant", holder: "Collection", placeholder: "doc-pieces", hint: "Letters, digits, _ and -." },
  { id: "weaviate", name: "Weaviate", holder: "Collection", placeholder: "DocPieces", hint: "Letters, digits and _; Weaviate makes the first a capital." },
  { id: "milvus", name: "Milvus or Zilliz", holder: "Collection", placeholder: "doc_pieces", hint: "Letters, digits and _, a letter first." },
  { id: "chroma", name: "Chroma", holder: "Collection", placeholder: "doc-pieces", hint: "3 or more letters, digits, _, - and ." },
  { id: "pinecone", name: "Pinecone", holder: "Index", placeholder: "doc-pieces", hint: "Small letters, digits and -, at most 45." },
  { id: "redis", name: "Redis", holder: "Index", placeholder: "doc_pieces", hint: "Redis 8 or Redis Stack: the pieces' keys begin with the index's name." },
  { id: "mongodb", name: "MongoDB Atlas", holder: "Collection", placeholder: "shop.pieces", hint: "database.collection; its vector search index is vector_index unless named." },
  { id: "memory", name: "This run's memory", holder: "Collection", placeholder: "default", hint: "Kept for this run only, for small sets and trying a workflow out: gone when the run ends." },
];

/** The store a step names; with none, its credential's kind (PostgreSQL's when it has none yet). */
export function storeOf(fields: Fields, kindOfCredential: (name: string) => string | undefined): StoreLook {
  const named = typeof fields.store === "string" ? fields.store : kindOfCredential(String(fields.credential ?? ""));
  return STORES.find((s) => s.id === named) ?? STORES[0]!;
}

/** The store a step keeps its pieces in, its credential, its table or collection, and how nearness is measured. */
function TableFields({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const credentials = useContext(CredentialsContext);
  const store = storeOf(fields, (name) => credentials.find((c) => c.name === name)?.kind);
  const holderField = store.id === "postgres" ? "table" : "collection";
  const name = String(fields.table ?? fields.collection ?? "");
  const choose = (id: string) => {
    const next: Fields = { ...fields, store: id };
    const kept = String(fields.table ?? fields.collection ?? "");
    delete next.table;
    delete next.collection;
    delete next.namespace;
    delete next.index;
    if (kept) next[id === "postgres" ? "table" : "collection"] = kept;
    // A credential is of its store's kind: another store's is let go.
    const had = credentials.find((c) => c.name === fields.credential)?.kind;
    if (id === "memory" || (had !== undefined && had !== id)) delete next.credential;
    if (id !== "memory" && next.credential === undefined) next.credential = "";
    setBody(next);
  };
  return (
    <>
      <div className="row top">
        <label>
          Kept in
          <select value={store.id} onChange={(e) => choose(e.target.value)}>
            {STORES.map((s) => (
              <option key={s.id} value={s.id}>
                {s.name}
              </option>
            ))}
          </select>
        </label>
        {store.id !== "memory" && (
          <Picker label={store.id === "postgres" ? "Database" : "Store"} kinds={[store.id]} value={String(fields.credential ?? "")} onChange={(v) => set("credential", v)} />
        )}
        <Text label={store.holder} value={name} onChange={(v) => set(holderField, v)} placeholder={store.placeholder} />
        <label className="narrow">
          Nearness
          <select value={String(fields.distance ?? "cosine")} onChange={(e) => set("distance", e.target.value === "cosine" ? undefined : e.target.value)}>
            <option value="cosine">cosine</option>
            <option value="l2">distance (L2)</option>
            <option value="inner_product">inner product</option>
          </select>
        </label>
      </div>
      {store.id === "pinecone" && (
        <Text label="Namespace" value={String(fields.namespace ?? "")} onChange={(v) => set("namespace", v || undefined)} placeholder="the default" />
      )}
      {store.id === "mongodb" && (
        <Text label="Vector search index" value={String(fields.index ?? "")} onChange={(v) => set("index", v || undefined)} placeholder="vector_index" />
      )}
      <p className="hint">{store.hint}</p>
    </>
  );
}

function EmbedFields({ fields, set, what }: { fields: Fields; set: Put; what: string }) {
  return (
    <div className="row top">
      <Picker label={what} kinds={["ai"]} value={String(fields.embed_with ?? "")} onChange={(v) => set("embed_with", v || undefined)} />
      <Text label="Embedding model" value={String(fields.embedding_model ?? "")} onChange={(v) => set("embedding_model", v || undefined)} placeholder="the credential's" />
    </div>
  );
}

/** How many to find, which metadata they hold, how near they must be, and an optional rerank model. */
function SearchFields({ fields, set, k }: { fields: Fields; set: Put; k: number }) {
  const rerank = (fields.rerank ?? undefined) as { model?: string } | undefined;
  const filter = fields.filter && typeof fields.filter === "object" ? (fields.filter as Record<string, string>) : {};
  return (
    <>
      <div className="row">
        <NumberField label="How many" value={fields.top_k ?? k} min={1} max={50} onChange={(v) => set("top_k", v)} />
        <NumberField label="Least score" value={fields.min_score} step="0.05" hint="optional" onChange={(v) => set("min_score", v)} />
      </div>
      <p className="hint">Only pieces whose metadata holds these (empty: any piece):</p>
      <Pairs value={filter} onChange={(v) => set("filter", Object.keys(v).length ? v : undefined)} keyLabel="Field" valueLabel="Text" />
      <label className="check">
        <input type="checkbox" checked={rerank !== undefined} onChange={(e) => set("rerank", e.target.checked ? { model: "" } : undefined)} />
        Reorder with a rerank model (its service must offer one)
      </label>
      {rerank !== undefined && (
        <Text label="Rerank model" value={String(rerank.model ?? "")} onChange={(v) => set("rerank", { ...rerank, model: v })} placeholder="the model's name" />
      )}
    </>
  );
}

function SplitBody({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const many = "documents" in fields;
  const metadata = (fields.metadata ?? {}) as Record<string, string>;
  const choose = (list: boolean) => {
    const next: Fields = { ...fields };
    delete next.text;
    delete next.documents;
    if (list) next.documents = "value.documents";
    else next.text = "{{ value.text }}";
    setBody(next);
  };
  return (
    <>
      <label>
        Split
        <select value={many ? "documents" : "text"} onChange={(e) => choose(e.target.value === "documents")}>
          <option value="text">one text</option>
          <option value="documents">a list of documents</option>
        </select>
      </label>
      {many ? (
        <>
          <Expr label="Documents" value={String(fields.documents ?? "")} onChange={(v) => set("documents", v)} />
          <p className="hint">Each a text, or a record with text (and source, metadata). In the name below, the document is item.</p>
        </>
      ) : (
        <Area label="Text" hint="with {{ }} for values" value={String(fields.text ?? "")} onChange={(v) => set("text", v)} />
      )}
      <Text label="Document's name" value={String(fields.source ?? "")} onChange={(v) => set("source", v || undefined)} placeholder="{{ value.title }}" wide />
      <div className="row">
        <label>
          By
          <select value={String(fields.by ?? "paragraphs")} onChange={(e) => set("by", e.target.value)}>
            <option value="paragraphs">paragraphs</option>
            <option value="sentences">sentences</option>
            <option value="markdown">Markdown headings</option>
            <option value="characters">characters</option>
          </select>
        </label>
        <NumberField label="Size" hint="characters" value={fields.size ?? 1000} min={50} max={20000} onChange={(v) => set("size", v)} />
        <NumberField label="Overlap" value={fields.overlap} min={0} hint="optional" onChange={(v) => set("overlap", v || undefined)} />
        <label className="check">
          <input type="checkbox" checked={fields.format === "html"} onChange={(e) => set("format", e.target.checked ? "html" : undefined)} />
          It is a web page (HTML)
        </label>
      </div>
      <p className="hint">Fields kept with every piece, to filter by later:</p>
      <Pairs value={metadata} onChange={(v) => set("metadata", Object.keys(v).length ? v : undefined)} keyLabel="Field" valueLabel="Text" />
    </>
  );
}

const ACTION_FIELDS: Record<string, string[]> = {
  add: ["pieces", "embeddings", "create", "dimensions", "replace"],
  search: ["query", "vector", "top_k", "filter", "min_score", "rerank"],
  delete: ["source"],
};

function StoreBody({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const action = String(fields.action ?? "add");
  const act = (next: string) => {
    const kept: Fields = { ...fields, action: next };
    for (const name of Object.values(ACTION_FIELDS).flat()) delete kept[name];
    if (next === "add") Object.assign(kept, { pieces: "steps.split.pieces", create: true });
    if (next === "search") Object.assign(kept, { query: "{{ value.question }}", top_k: 5 });
    if (next === "delete") kept.source = "value.source";
    setBody(kept);
  };
  return (
    <>
      <label>
        Do
        <select value={action} onChange={(e) => act(e.target.value)}>
          <option value="add">add pieces</option>
          <option value="search">find the nearest</option>
          <option value="delete">delete a document's pieces</option>
        </select>
      </label>
      <TableFields fields={fields} set={set} setBody={setBody} />
      {action !== "delete" && <EmbedFields fields={fields} set={set} what="Vectors from" />}
      {action === "add" && (
        <>
          <Expr label="Pieces" value={String(fields.pieces ?? "")} onChange={(v) => set("pieces", v)} />
          <div className="row">
            <label className="check">
              <input type="checkbox" checked={fields.create === true} onChange={(e) => set("create", e.target.checked || undefined)} />
              Make the table when it is not there
            </label>
            <label className="check">
              <input type="checkbox" checked={fields.replace !== false} onChange={(e) => set("replace", e.target.checked ? undefined : false)} />
              A document added again replaces its old pieces
            </label>
          </div>
        </>
      )}
      {action === "search" && (
        <>
          <Text label="Find pieces about" value={String(fields.query ?? "")} onChange={(v) => set("query", v)} wide />
          <SearchFields fields={fields} set={set} k={5} />
        </>
      )}
      {action === "delete" && <Expr label="Document (or a list of them)" value={String(fields.source ?? "")} onChange={(v) => set("source", v)} />}
    </>
  );
}

function AskBody({ fields, set, setBody }: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const answering = String(fields.answer_with ?? fields.embed_with ?? "");
  return (
    <>
      <Area label="Question" hint="with {{ }} for values" value={String(fields.question ?? "")} onChange={(v) => set("question", v)} />
      <TableFields fields={fields} set={set} setBody={setBody} />
      <EmbedFields fields={fields} set={set} what="Vectors from" />
      <div className="row top">
        <Picker label="Answer from" kinds={["ai"]} optional="optional" value={String(fields.answer_with ?? "")} onChange={(v) => set("answer_with", v || undefined)} />
        <ModelPicker credential={answering} value={String(fields.model ?? "")} onChange={(v) => set("model", v || undefined)} />
      </div>
      <SearchFields fields={fields} set={set} k={4} />
      <Area label="Instructions" hint="optional: how to answer, beside using the passages only" value={String(fields.instructions ?? "")} onChange={(v) => set("instructions", v || undefined)} />
      <div className="row">
        <Text label="When nothing is found" value={String(fields.no_answer ?? "")} onChange={(v) => set("no_answer", v || undefined)} placeholder="No passage of the documents answers this question." wide />
        <NumberField label="Max tokens" value={fields.max_tokens} min={1} onChange={(v) => set("max_tokens", v)} />
        <NumberField label="Temperature" value={fields.temperature} min={0} max={2} step="0.1" onChange={(v) => set("temperature", v)} />
      </div>
      <p className="hint">The answer comes with every passage it was given: its document, its score, and whether it was cited.</p>
    </>
  );
}
