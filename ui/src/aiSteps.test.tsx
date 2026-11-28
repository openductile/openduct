// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The AI steps built on "Ask AI" in the builder: each one's form writes the step as the engine reads it, and a
// classify or sentiment step's choices are branches on the canvas, each with its own steps.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { Definition, Step } from "./api";
import { fresh, innerLists, names, StepCard } from "./Steps";
import { layout } from "./flow/layout";
import { insertAt, listAt } from "./flow/paths";
import { summaryOf } from "./flow/look";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const sorted: Step = {
  name: "sort",
  classify: {
    credential: "model",
    text: "{{ value.text }}",
    categories: [{ name: "billing", steps: [{ name: "to_billing", topic: { name: "billing" } }] }, "bug"],
    default: [{ name: "to_others", topic: { name: "others" } }],
  },
};

describe("a classify step on the canvas", () => {
  it("stands its categories and other side by side, each with its own steps", () => {
    const definition: Definition = { name: "Sort", trigger: { webhook: {} }, steps: [sorted, { name: "after", set: { a: "1" } }] };
    const laid = layout(definition);
    const labels = laid.edges.filter((e) => e.source === "step:0").map((e) => e.label);
    expect(labels).toEqual(["billing", "bug", "other"]);
    const inner = laid.nodes.filter((n) => n.kind === "step" && n.at?.list.length === 2).map((n) => n.step?.name);
    expect(inner).toEqual(["to_billing", "to_others"]);
    expect(laid.nodes.some((n) => n.id === "empty:0/case1"), "an empty category says a step can go there").toBe(true);
    expect(laid.nodes.find((n) => n.id === "join:0")!.y).toBeLessThan(laid.nodes.find((n) => n.id === "step:1")!.y);
  });

  it("takes a step added to a category - one written as a plain name too - or to other", () => {
    const added = insertAt([sorted], { list: [0, "case1"], index: 0 }, { name: "to_bugs", stop: {} });
    const categories = (added[0]!.classify as { categories: unknown[] }).categories;
    expect(categories[1]).toEqual({ name: "bug", steps: [{ name: "to_bugs", stop: {} }] });
    expect(listAt(added, [0, "case1"]).map((s) => s.name)).toEqual(["to_bugs"]);
    const other = insertAt(added, { list: [0, "default"], index: 1 }, { name: "last", stop: {} });
    expect(listAt(other, [0, "default"]).map((s) => s.name)).toEqual(["to_others", "last"]);
    expect([...names(other)]).toEqual(["sort", "to_billing", "to_bugs", "to_others", "last"]);
    expect(innerLists(other[0]!)).toHaveLength(3);
  });

  it("has no other branch when the model must choose, and sentiment's labels are its branches", () => {
    const strict: Step = { name: "s", classify: { ...(sorted.classify as object), other: false } };
    const laid = layout({ name: "x", steps: [strict] });
    expect(laid.edges.filter((e) => e.source === "step:0").map((e) => e.label)).toEqual(["billing", "bug"]);
    const mood = fresh("sentiment", new Set());
    const branches = layout({ name: "x", steps: [mood] }).edges.filter((e) => e.source === "step:0").map((e) => e.label);
    expect(branches).toEqual(["positive", "neutral", "negative"]);
  });

  it("says what it does in a line", () => {
    expect(summaryOf("classify", sorted)).toBe("2 categories or other");
    expect(summaryOf("sentiment", fresh("sentiment", new Set()))).toBe("positive, neutral, negative");
    expect(summaryOf("extract", fresh("extract", new Set()))).toBe("2 fields");
    expect(summaryOf("summarise", fresh("summarise", new Set()))).toBe("a summary, short");
  });
});

describe("the forms", () => {
  // The model picker asks the credential's service for its models.
  const card = (step: Step) => {
    vi.stubGlobal("fetch", async () => new Response(JSON.stringify({ models: [] }), { status: 200 }));
    const changed = vi.fn();
    render(<StepCard step={step} taken={new Set()} onChange={changed} onRemove={() => {}} onUp={() => {}} onDown={() => {}} />);
    return changed;
  };

  it("names a classify step's categories and what each means, keeping their steps", () => {
    const changed = card(sorted);
    fireEvent.change(screen.getAllByLabelText("Category description")[0]!, { target: { value: "Payments" } });
    const step = changed.mock.calls.at(-1)![0] as Step;
    expect((step.classify as { categories: unknown[] }).categories[0]).toEqual({
      name: "billing",
      description: "Payments",
      steps: [{ name: "to_billing", topic: { name: "billing" } }],
    });
    fireEvent.click(screen.getByText("+ Category"));
    const more = changed.mock.calls.at(-1)![0] as Step;
    expect((more.classify as { categories: unknown[] }).categories).toHaveLength(3);
    fireEvent.click(screen.getByLabelText(/More than one may fit/));
    expect(((changed.mock.calls.at(-1)![0] as Step).classify as { several?: boolean }).several).toBe(true);
  });

  it("gives an extract step fields with types, or a JSON schema", () => {
    const changed = card(fresh("extract", new Set()));
    fireEvent.change(screen.getAllByLabelText("Field type")[1]!, { target: { value: "date" } });
    const step = changed.mock.calls.at(-1)![0] as Step;
    expect((step.extract as { fields: { type: string }[] }).fields[1]!.type).toBe("date");
    fireEvent.click(screen.getByLabelText("A JSON schema"));
    const schema = changed.mock.calls.at(-1)![0] as Step;
    expect((schema.extract as { schema?: unknown }).schema).toBeTruthy();
  });

  it("lets Ask AI's answer be held to a schema", () => {
    const changed = card({ name: "ask", ai: { credential: "model", prompt: "hi" } });
    fireEvent.click(screen.getByLabelText(/Hold the answer to a schema/));
    const step = changed.mock.calls.at(-1)![0] as Step;
    expect((step.ai as { schema?: { type?: string } }).schema?.type).toBe("object");
  });
});
