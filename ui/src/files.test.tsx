// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Files between steps on the page: the steps' forms write what the engine reads, a run's files are found in its
// record and downloaded with the workspace they are of, and a test run's are said to be held for it alone.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { setWorkspace } from "./api";
import { StepCard, fresh, KINDS } from "./Steps";
import { FilesIn, filesIn, size } from "./StepsFiles";
import { summaryOf } from "./flow/look";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const sha = "a".repeat(64);

/** A step card that keeps what it is changed to, and says it. */
function Card(props: { start: Record<string, unknown>; seen: (s: Record<string, unknown>) => void }) {
  const [step, setStep] = useState(props.start);
  return (
    <StepCard
      step={step}
      taken={new Set()}
      onChange={(s) => {
        setStep(s);
        props.seen(s);
      }}
      onRemove={() => {}}
      onUp={() => {}}
      onDown={() => {}}
    />
  );
}

describe("files between steps", () => {
  it("offers the file steps and makes each one the engine reads", () => {
    for (const kind of ["extract_file", "convert", "file_details"]) {
      expect(KINDS.some((k) => k.kind === kind)).toBe(true);
    }
    expect(fresh("extract_file", new Set())).toEqual({ name: "extract_file_1", extract_file: { file: "value.upload" } });
    expect(fresh("convert", new Set(["convert_1"]))).toEqual({
      name: "convert_2",
      convert: { format: "csv", rows: "value.items", name: "items.csv" },
    });
    expect(summaryOf("extract_file", { extract_file: { file: "steps.get.body", format: "pdf" } })).toBe("pdf from steps.get.body");
    expect(summaryOf("convert", { convert: { format: "xlsx", name: "orders.xlsx" } })).toBe("xlsx file orders.xlsx");
  });

  it("a convert step's format decides what it is made of", () => {
    let last: Record<string, unknown> = {};
    render(<Card start={fresh("convert", new Set())} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("csv"), { target: { value: "text" } });
    expect(last.convert).toEqual({ format: "text", text: "value.items", name: "items.csv" });
    fireEvent.change(screen.getByDisplayValue("text"), { target: { value: "xlsx" } });
    expect(last.convert).toEqual({ format: "xlsx", rows: "value.items", name: "items.csv" });
  });

  it("an HTTP step sends a file or a form with files, and says what its answer is made", () => {
    let last: Record<string, unknown> = {};
    render(<Card start={{ name: "send", http: { method: "POST", url: "https://example.com", json: {} } }} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("JSON"), { target: { value: "multipart" } });
    expect(last.http).toEqual({ method: "POST", url: "https://example.com", multipart: {} });
    fireEvent.click(screen.getByText("+ Field"));
    fireEvent.change(screen.getByLabelText("Field kind"), { target: { value: "file" } });
    fireEvent.change(screen.getByLabelText("File"), { target: { value: "steps.report" } });
    expect((last.http as { multipart: unknown }).multipart).toEqual({ field_1: { file: "steps.report" } });
    fireEvent.change(screen.getByDisplayValue("a file when it is one"), { target: { value: "file" } });
    expect((last.http as { response: unknown }).response).toBe("file");
  });

  it("finds a run's files and downloads a kept one with its workspace", async () => {
    const result = {
      status: 200,
      body: { file: `12.1/${sha}`, name: "invoice.pdf", type: "application/pdf", size: 2048 },
      more: [{ file: `m/${sha}`, name: "draft.csv", type: "text/csv", size: 10 }],
      not: { file: "12.1/xyz", name: "nope" },
    };
    expect(filesIn(result).map((f) => f.name)).toEqual(["invoice.pdf", "draft.csv"]);
    expect(size(2048)).toBe("2.0 KB");
    const asked: { path: string; workspace?: string }[] = [];
    vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
      asked.push({ path, workspace: (init.headers as Record<string, string>)["x-openduct-workspace"] });
      return new Response(new Blob(["%PDF"]), { status: 200 });
    });
    URL.createObjectURL = vi.fn(() => "blob:x");
    URL.revokeObjectURL = vi.fn();
    setWorkspace("w1");
    render(<FilesIn value={result} />);
    expect(screen.getByText("held for this test only")).toBeTruthy();
    fireEvent.click(screen.getByText("download"));
    await waitFor(() => expect(asked).toHaveLength(1));
    expect(asked[0]).toEqual({ path: `/api/runs/file?file=12.1%2F${sha}&name=invoice.pdf`, workspace: "w1" });
  });
});
