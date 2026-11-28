// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The retrieval steps' forms: what a person's choices write into the step, as the engine reads it.
// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import type { CredentialSummary } from "./api";
import { CredentialsContext, KINDS, fresh, kindOf } from "./Steps";
import { RetrievalBody } from "./StepsRetrieval";
import { FAMILY, summaryOf } from "./flow/look";
import { MemoryStoreFields } from "./memoryStores"; // vector stores

type Fields = Record<string, unknown>;

const credentials: CredentialSummary[] = [
  { name: "docs-db", kind: "postgres", fields: ["url"], updated: 1 },
  { name: "model", kind: "ai", fields: ["base_url"], updated: 1 },
  { name: "mail", kind: "smtp", fields: ["host"], updated: 1 },
  { name: "vectors", kind: "qdrant", fields: ["url"], updated: 1 }, // vector stores
];

/** The form of a step of `kind`, its latest body kept in `seen`. */
function show(kind: string, start: Fields) {
  const seen: { body: Fields } = { body: start };
  function Harness() {
    const [body, setBody] = useState<Fields>(start);
    const keep = (next: Fields) => {
      seen.body = next;
      setBody(next);
    };
    return (
      <CredentialsContext.Provider value={credentials}>
        <RetrievalBody kind={kind} fields={body} set={(n, v) => keep({ ...body, [n]: v })} setBody={(v) => keep(v as Fields)} />
      </CredentialsContext.Provider>
    );
  }
  render(<Harness />);
  return seen;
}

function select(label: string): HTMLSelectElement {
  return screen.getByLabelText(label, { exact: false, selector: "select" }) as HTMLSelectElement;
}

afterEach(cleanup);

describe("retrieval steps", () => {
  it("are steps to add, each written as the engine reads it", () => {
    for (const kind of ["split_text", "vector_store", "ask_documents"]) {
      expect(KINDS.some((k) => k.kind === kind)).toBe(true);
      const step = fresh(kind, new Set([`${kind}_1`]));
      expect(step.name).toBe(`${kind}_2`);
      expect(kindOf(step)).toBe(kind);
      expect(FAMILY[kind]).toBeDefined();
    }
    expect(fresh("vector_store", new Set()).vector_store).toMatchObject({ action: "add", pieces: "steps.split.pieces", create: true });
    expect(summaryOf("split_text", { split_text: { by: "markdown", size: 800 } })).toBe("by markdown, 800 characters");
    expect(summaryOf("vector_store", { vector_store: { action: "search", table: "doc_pieces" } })).toBe("find in doc_pieces");
    expect(summaryOf("ask_documents", { ask_documents: { table: "doc_pieces" } })).toBe("from doc_pieces");
  });

  it("split text: one text or a list of documents, and how to cut it", () => {
    const seen = show("split_text", { text: "{{ value.text }}", by: "paragraphs" });
    fireEvent.change(select("Split"), { target: { value: "documents" } });
    expect(seen.body.documents).toBe("value.documents");
    expect("text" in seen.body).toBe(false);
    fireEvent.change(select("By"), { target: { value: "markdown" } });
    expect(seen.body.by).toBe("markdown");
    fireEvent.click(screen.getByLabelText("It is a web page (HTML)"));
    expect(seen.body.format).toBe("html");
  });

  it("vector store: each action keeps only its own settings, and each credential its own kind", () => {
    const seen = show("vector_store", { action: "add", credential: "", table: "doc_pieces", pieces: "steps.split.pieces", create: true });
    const database = within(select("Database")).getAllByRole("option").map((o) => o.textContent);
    expect(database).toEqual(["choose one", "docs-db (postgres)"]);
    const vectors = within(select("Vectors from")).getAllByRole("option").map((o) => o.textContent);
    expect(vectors).toEqual(["choose one", "model (ai)"]);
    fireEvent.change(select("Database"), { target: { value: "docs-db" } });
    fireEvent.change(select("Vectors from"), { target: { value: "model" } });
    fireEvent.change(select("Do"), { target: { value: "search" } });
    expect(seen.body).toEqual({ action: "search", credential: "docs-db", table: "doc_pieces", embed_with: "model", query: "{{ value.question }}", top_k: 5 });
    fireEvent.click(screen.getByLabelText("Reorder with a rerank model", { exact: false }));
    expect(seen.body.rerank).toEqual({ model: "" });
    fireEvent.change(select("Do"), { target: { value: "delete" } });
    expect(seen.body).toEqual({ action: "delete", credential: "docs-db", table: "doc_pieces", embed_with: "model", source: "value.source" });
    fireEvent.change(select("Do"), { target: { value: "add" } });
    fireEvent.click(screen.getByLabelText("A document added again replaces its old pieces"));
    expect(seen.body.replace).toBe(false);
  });

  it("ask your documents: the answer's model may be another credential's", () => {
    const seen = show("ask_documents", { credential: "docs-db", table: "doc_pieces", embed_with: "", question: "{{ value.question }}", top_k: 4 });
    fireEvent.change(select("Answer from"), { target: { value: "model" } });
    expect(seen.body.answer_with).toBe("model");
    fireEvent.change(select("Answer from"), { target: { value: "" } });
    expect(seen.body.answer_with).toBeUndefined();
    fireEvent.change(select("Nearness"), { target: { value: "l2" } });
    expect(seen.body.distance).toBe("l2");
  });

  // vector stores
  it("vector store: a store is chosen, its credential is of its kind, and its pieces are in a collection", () => {
    const seen = show("vector_store", { action: "search", credential: "docs-db", table: "doc_pieces", query: "q", top_k: 5 });
    expect(select("Kept in").value).toBe("postgres");
    fireEvent.change(select("Kept in"), { target: { value: "qdrant" } });
    expect(seen.body).toEqual({ action: "search", store: "qdrant", credential: "", collection: "doc_pieces", query: "q", top_k: 5 });
    const stores = within(select("Store")).getAllByRole("option").map((o) => o.textContent);
    expect(stores).toEqual(["choose one", "vectors (qdrant)"]);
    fireEvent.change(select("Store"), { target: { value: "vectors" } });
    expect(seen.body.credential).toBe("vectors");
    fireEvent.change(select("Kept in"), { target: { value: "pinecone" } });
    expect(screen.getByText("Namespace")).toBeDefined();
    fireEvent.change(select("Kept in"), { target: { value: "memory" } });
    expect("credential" in seen.body).toBe(false);
    expect(seen.body.store).toBe("memory");
    expect(screen.getByText(/Kept for this run only/)).toBeDefined();
    fireEvent.change(select("Kept in"), { target: { value: "postgres" } });
    expect(seen.body).toMatchObject({ store: "postgres", credential: "", table: "doc_pieces" });
    expect("collection" in seen.body).toBe(false);
    expect(summaryOf("vector_store", { vector_store: { action: "add", store: "qdrant", collection: "kb" } })).toBe("add to kb");
  });

  it("an agent's memory: kept by OpenDuct, or in PostgreSQL, Redis or a data table, each with what it needs", () => {
    const seen: { memory: Fields } = { memory: { key: "{{ value.chat }}", window: 10 } };
    function Harness() {
      const [memory, setMemory] = useState<Fields>(seen.memory);
      const put = (m: Fields) => {
        seen.memory = m;
        setMemory(m);
      };
      return (
        <CredentialsContext.Provider value={credentials}>
          <MemoryStoreFields memory={memory} put={put} />
        </CredentialsContext.Provider>
      );
    }
    render(<Harness />);
    fireEvent.change(select("Kept in"), { target: { value: "postgres" } });
    expect(seen.memory).toEqual({ key: "{{ value.chat }}", window: 10, store: "postgres", credential: "", table: "agent_memory", create: true });
    fireEvent.change(select("Database"), { target: { value: "docs-db" } });
    expect(seen.memory.credential).toBe("docs-db");
    fireEvent.change(select("Kept in"), { target: { value: "table" } });
    expect(seen.memory).toEqual({ key: "{{ value.chat }}", window: 10, store: "table", table: "agent_memory" });
    fireEvent.change(select("Kept in"), { target: { value: "broker" } });
    expect(seen.memory).toEqual({ key: "{{ value.chat }}", window: 10 });
  });
});
