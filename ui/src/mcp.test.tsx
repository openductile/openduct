// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// MCP on the page, against a stand-in API: the workspace's server with its tools and tokens, a token made and shown
// once, a workflow offered as a tool, and the MCP step listing a server's tools to choose from.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { McpSettings } from "./McpSettings";
import { McpBody } from "./StepsMcp";
import { ToolOffer } from "./ToolOffer";
import type { Definition } from "./api";

type Answer = { status?: number; body: unknown };

function stand_in(routes: Record<string, Answer | ((body: unknown) => Answer)>) {
  const sent: { route: string; body: unknown }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    const route = `${init.method ?? "GET"} ${path}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    sent.push({ route, body });
    const found = routes[route];
    const answer = typeof found === "function" ? found(body) : (found ?? { status: 404, body: { error: "no such request" } });
    return new Response(JSON.stringify(answer.body), { status: answer.status ?? 200 });
  });
  return sent;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const server = {
  path: "/mcp/w1",
  versions: ["2026-07-28", "2025-11-25"],
  tools: [
    { workflow: "wf1", workflow_name: "Price an item", name: "price_an_item", description: "A price", inputs: 2, active: true, taken_by: null },
    { workflow: "wf2", workflow_name: "Off one", name: "off_one", description: "Off", inputs: 0, active: false, taken_by: null },
  ],
  tokens: [{ id: "t1", name: "Desk assistant", hint: "a1b2", tools: null, per_minute: 60, created: 1, by: "ada@example.com" }],
  manage: true,
};

describe("the workspace's MCP server", () => {
  it("shows its address, its tools and tokens, makes a token shown once and revokes one", async () => {
    const sent = stand_in({
      "GET /api/mcp": { body: server },
      "POST /api/mcp/tokens": (body) => ({ status: 201, body: { ...(body as object), id: "t2", hint: "zz99", created: 2, by: "ada", token: "odmcp_secret-once" } }),
      "DELETE /api/mcp/tokens/t1": { body: { revoked: "t1" } },
    });
    render(<McpSettings workspace="w1" onGo={() => undefined} />);
    expect(await screen.findByText(`${location.origin}/mcp/w1`)).toBeTruthy();
    expect(screen.getByText("price_an_item")).toBeTruthy();
    expect(screen.getByText(/off: turn it on to offer it/)).toBeTruthy();
    expect(screen.getByText("Desk assistant")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("What the token is for"), { target: { value: "Front desk assistant" } });
    fireEvent.click(screen.getByRole("button", { name: /Make a token/ }));
    expect(await screen.findByText("odmcp_secret-once")).toBeTruthy();
    expect(sent.find((s) => s.route === "POST /api/mcp/tokens")?.body).toEqual({ name: "Front desk assistant", tools: null, per_minute: 60 });
    fireEvent.click(screen.getByRole("button", { name: /Revoke/ }));
    await waitFor(() => expect(sent.some((s) => s.route === "DELETE /api/mcp/tokens/t1")).toBe(true));
  });
});

function Offering(props: { start: Definition; seen: (d: Definition) => void }) {
  const [d, setD] = useState(props.start);
  return (
    <ToolOffer
      definition={d}
      onChange={(next) => {
        setD(next);
        props.seen(next);
      }}
    />
  );
}

describe("a workflow offered as a tool", () => {
  it("writes its description and arguments as the engine reads them", () => {
    let last: Definition | null = null;
    render(<Offering start={{ name: "Price an item", trigger: "manual", steps: [] }} seen={(d) => (last = d)} />);
    fireEvent.click(screen.getByLabelText("Offer it as a tool to AI assistants"));
    fireEvent.change(screen.getByLabelText("What the tool does"), { target: { value: "The price of an item" } });
    fireEvent.click(screen.getByRole("button", { name: "+ An argument" }));
    fireEvent.change(screen.getByLabelText("Argument name"), { target: { value: "sku" } });
    fireEvent.click(screen.getByLabelText("needed"));
    expect(last!.tool).toEqual({ description: "The price of an item", input: { sku: { type: "string", required: true } } });
    fireEvent.click(screen.getByLabelText("Offer it as a tool to AI assistants"));
    expect(last!.tool).toBeUndefined();
  });

  it("is not offered for a workflow a topic starts", () => {
    render(<Offering start={{ name: "w", trigger: { topic: { name: "orders" } } }} seen={() => undefined} />);
    expect(screen.queryByLabelText("Offer it as a tool to AI assistants")).toBeNull();
  });
});

function Step(props: { seen: (f: Record<string, unknown>) => void }) {
  const [fields, setFields] = useState<Record<string, unknown>>({ url: "https://tools.example.com/mcp" });
  const put = (next: Record<string, unknown>) => {
    setFields(next);
    props.seen(next);
  };
  return <McpBody fields={fields} set={(name, value) => put({ ...fields, [name]: value })} setBody={(v) => put(v as Record<string, unknown>)} />;
}

describe("the MCP step", () => {
  it("lists a server's tools, and asks for the chosen one's arguments one by one", async () => {
    const sent = stand_in({
      "POST /api/mcp/discover": {
        body: {
          protocol: "2026-07-28",
          tools: [{ name: "get_forecast", title: "Forecast", description: "The weather for a city", input_schema: { type: "object", properties: { city: { type: "string" }, days: { type: "integer" } }, required: ["city"] } }],
        },
      },
    });
    let last: Record<string, unknown> = {};
    render(<Step seen={(f) => (last = f)} />);
    fireEvent.click(screen.getByRole("button", { name: "List the server's tools" }));
    const pick = await screen.findByRole("combobox", { name: "Tool" });
    expect(sent[0]?.body).toEqual({ url: "https://tools.example.com/mcp" });
    fireEvent.change(pick, { target: { value: "get_forecast" } });
    expect(await screen.findByText("The weather for a city")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("city"), { target: { value: "{{ value.city }}" } });
    fireEvent.change(screen.getByLabelText("days (optional)"), { target: { value: "3" } });
    expect(last).toEqual({ url: "https://tools.example.com/mcp", tool: "get_forecast", arguments: { city: "{{ value.city }}", days: 3 } });
  });
});
