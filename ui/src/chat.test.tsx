// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A chat trigger's window against a stand-in API: where its chat is, what its settings change, and "Try the chat"
// keeping the conversation it sends with each message.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { TriggerSettings } from "./Trigger";
import type { Definition, WorkflowRecord } from "./api";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const definition: Definition = {
  name: "Help desk",
  trigger: { chat: { texts: { title: "Help desk" } } },
  steps: [{ name: "answer", ai: { credential: "model", messages: "{{ value.history }}", prompt: "{{ value.message }}" } }],
};
const record: WorkflowRecord = {
  id: "w-1",
  name: "Help desk",
  active: true,
  version: 1,
  definition,
  created: 1,
  updated: 1,
  hook: null,
  chat: "/chat/0123abcd",
};

function Editing(props: { onDefinition: (d: Definition) => void }) {
  const [now, setNow] = useState(definition);
  return (
    <TriggerSettings
      definition={now}
      record={record}
      onChange={(d) => {
        setNow(d);
        props.onDefinition(d);
      }}
    />
  );
}

describe("the chat trigger", () => {
  it("shows where its chat is, and changes its words, sites and password in the definition", () => {
    const seen: Definition[] = [];
    render(<Editing onDefinition={(d) => seen.push(d)} />);
    expect(screen.getByRole("radio", { name: /Chat/ }).getAttribute("aria-checked")).toBe("true");
    expect(screen.getByText(`${location.origin}/chat/0123abcd`)).toBeTruthy();
    expect(screen.getByText(`<script src="${location.origin}/chat/0123abcd/embed.js" async></script>`)).toBeTruthy();

    fireEvent.change(screen.getByPlaceholderText("Hi! How can I help?"), { target: { value: "Ask about your order." } });
    fireEvent.change(screen.getByLabelText("Sites it may be on"), { target: { value: "https://shop.example.com\n\n" } });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "open sesame" } });
    const chat = (seen.at(-1)!.trigger as { chat: Record<string, unknown> }).chat;
    expect(chat.texts).toEqual({ title: "Help desk", welcome: "Ask about your order." });
    expect(chat.origins).toEqual(["https://shop.example.com"]);
    expect(chat.password).toBe("open sesame");

    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "" } });
    expect("password" in (seen.at(-1)!.trigger as { chat: Record<string, unknown> }).chat).toBe(false);
  });

  it("tries the chat: each message runs the workflow being edited with the conversation so far", async () => {
    const sent: unknown[] = [];
    vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
      expect(`${init.method} ${path}`).toBe("POST /api/workflows/w-1/chat");
      const body = JSON.parse(String(init.body)) as { message: string };
      sent.push(body);
      const run = { status: "done", error: null, ms: 12, steps: [{ name: "answer", kind: "ai", attempts: 1, ms: 11, result: {}, error: null }] };
      return new Response(JSON.stringify({ answer: `You asked: ${body.message}`, failed: false, error_text: "Sorry", session: "try-1", run }));
    });
    render(<Editing onDefinition={() => undefined} />);
    const box = screen.getByLabelText("A message to try");
    fireEvent.change(box, { target: { value: "Do you ship to Canada?" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    expect(await screen.findByText("You asked: Do you ship to Canada?")).toBeTruthy();
    fireEvent.change(box, { target: { value: "How long?" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    expect(await screen.findByText("You asked: How long?")).toBeTruthy();
    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toMatchObject({
      message: "How long?",
      history: [
        { role: "user", content: "Do you ship to Canada?" },
        { role: "assistant", content: "You asked: Do you ship to Canada?" },
      ],
      definition: { name: "Help desk" },
    });
    expect(screen.getByText(/The last run: done/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Start again" }));
    expect(screen.queryByText("You asked: How long?")).toBeNull();
  });
});
