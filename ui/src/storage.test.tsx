// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Storage on the page: the storage, compress and redis steps are offered and their forms write only what each
// action takes (the engine refuses the rest); the canvas says what each does; the triggers are offered and described.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { CredentialsContext, StepCard, fresh, KINDS, TAKES } from "./Steps";
import { keepFor } from "./StepsStorage";
import { summaryOf, triggerSummary } from "./flow/look";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

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

describe("storage", () => {
  it("offers the steps, the credentials they take, and says what each does", () => {
    for (const kind of ["storage", "compress", "redis"]) expect(KINDS.some((k) => k.kind === kind)).toBe(true);
    expect(TAKES.storage).toEqual(["sftp", "ftp", "s3", "azure_blob", "gcs", "dropbox", "box", "box_token", "microsoft_365", "google_drive"]); // storage places
    expect(TAKES.redis).toEqual(["redis"]);
    expect(fresh("storage", new Set())).toEqual({ name: "storage_1", storage: { action: "list", credential: "", path: "in" } });
    expect(summaryOf("storage", { storage: { action: "read", credential: "partner", path: "in/orders.csv" } })).toBe("read in/orders.csv - partner");
    expect(summaryOf("storage", { storage: { action: "list", root: "inbox" } })).toBe("list the top - folder inbox");
    expect(summaryOf("compress", { compress: { action: "pack", format: "zip", name: "all.zip" } })).toBe("pack zip all.zip");
    expect(summaryOf("redis", { redis: { action: "publish", channel: "orders" } })).toBe("publish orders");
    expect(triggerSummary({ storage: { credential: "partner", folder: "in", pattern: "*.csv" } }).line).toBe("a new file (*.csv) in in - partner");
    expect(triggerSummary({ redis: { credential: "cache", channel: "orders" } }).line).toBe("a message on orders");
  });

  it("keeps only the settings an action takes", () => {
    const own = { list: ["pattern"], write: ["file", "overwrite"] };
    expect(keepFor("write", { action: "list", root: "inbox", path: "in", pattern: "*.csv" }, own, ["action", "root", "path"])).toEqual({
      action: "write",
      root: "inbox",
      path: "in",
    });
  });

  it("a storage step's form changes its action and where it reaches", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ roots: ["inbox", "outbox"], allow_private_network: false }) })),
    );
    let last: Record<string, unknown> = {};
    render(<Card start={{ name: "s", storage: { action: "list", credential: "partner", path: "in", pattern: "*.csv" } }} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("List a folder"), { target: { value: "write" } });
    expect(last.storage).toEqual({ action: "write", credential: "partner", path: "in" });
    fireEvent.click(screen.getByLabelText("Replace a file already there"));
    expect((last.storage as Record<string, unknown>).overwrite).toBe(true);
    fireEvent.change(screen.getByDisplayValue(/a server or a store/), { target: { value: "folder" } });
    await waitFor(() => expect((last.storage as Record<string, unknown>).root).toBeDefined());
    expect((last.storage as Record<string, unknown>).credential).toBeUndefined();
    await waitFor(() => expect(screen.getByDisplayValue("Write a file")).toBeTruthy());
  });

  // storage places
  it("a place reached over its vendor's API shows what it takes beside its credential", () => {
    let last: Record<string, unknown> = {};
    const credentials = [
      { name: "work", kind: "microsoft_365", fields: [], updated: 0 },
      { name: "mine", kind: "google_drive", fields: [], updated: 0 },
    ];
    render(
      <CredentialsContext.Provider value={credentials}>
        <Card start={{ name: "s", storage: { action: "read", credential: "work", path: "in/a.csv" } }} seen={(s) => (last = s)} />
      </CredentialsContext.Provider>,
    );
    fireEvent.change(screen.getByPlaceholderText(/a SharePoint site's id/), { target: { value: "site:contoso.sharepoint.com,1,2" } });
    expect((last.storage as Record<string, unknown>).drive).toBe("site:contoso.sharepoint.com,1,2");
    expect(screen.queryByText(/A Google Doc, Sheet or Slides read as/)).toBeNull();
    cleanup();
    render(
      <CredentialsContext.Provider value={credentials}>
        <Card start={{ name: "s", storage: { action: "read", credential: "mine", path: "docs/Plan" } }} seen={(s) => (last = s)} />
      </CredentialsContext.Provider>,
    );
    fireEvent.change(screen.getByDisplayValue("Word, Excel, PowerPoint"), { target: { value: "pdf" } });
    expect(last.storage).toEqual({ action: "read", credential: "mine", path: "docs/Plan", export: "pdf" });
    fireEvent.change(screen.getByDisplayValue("Read a file"), { target: { value: "list" } });
    expect((last.storage as Record<string, unknown>).export).toBeUndefined();
  });

  it("a redis step's form shows what its action takes", () => {
    let last: Record<string, unknown> = {};
    render(<Card start={fresh("redis", new Set())} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("Get a value"), { target: { value: "set" } });
    expect(last.redis).toEqual({ action: "set", credential: "", key: "{{ value.id }}" });
    expect(screen.getByLabelText("Only when the key has no value yet")).toBeTruthy();
    fireEvent.change(screen.getByDisplayValue("Set a value"), { target: { value: "publish" } });
    expect(last.redis).toEqual({ action: "publish", credential: "" });
  });
});
