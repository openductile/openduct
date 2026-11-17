// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The canvas's workings without a page: changing a step wherever it is, and the diagram's layout - where each
// step sits, which arrows join them, and where a step added on each arrow goes.

import { describe, expect, it } from "vitest";
import type { Definition, Step } from "../api";
import { layout, NODE_W } from "./layout";
import { insertAt, listAt, moveAt, removeAt, replaceAt, stepAt } from "./paths";
import { summaryOf } from "./look";

const steps: Step[] = [
  { name: "order", set: { total: "value.qty * value.price" } },
  {
    name: "big",
    if: {
      when: "steps.order.total > 100",
      then: [{ name: "tell", http: { method: "POST", url: "https://x" } }],
      else: [],
    },
  },
  { name: "each", for_each: { items: "value.lines", steps: [{ name: "w", wait: "1s" }] } },
  { name: "keep", topic: { name: "out" } },
];

describe("a step's place", () => {
  it("is found, changed, added to and taken away in branches and loops alike", () => {
    expect(stepAt(steps, { list: [1, "then"], index: 0 })?.name).toBe("tell");
    expect(stepAt(steps, { list: [2, "steps"], index: 0 })?.name).toBe("w");
    const added = insertAt(steps, { list: [1, "else"], index: 0 }, { name: "quiet", stop: {} });
    expect(listAt(added, [1, "else"]).map((s) => s.name)).toEqual(["quiet"]);
    // The workflow it came from is not changed.
    expect(listAt(steps, [1, "else"])).toEqual([]);
    const renamed = replaceAt(added, { list: [2, "steps"], index: 0 }, { name: "pause", wait: "2s" });
    expect(stepAt(renamed, { list: [2, "steps"], index: 0 })?.name).toBe("pause");
    const gone = removeAt(renamed, { list: [1, "then"], index: 0 });
    expect(listAt(gone, [1, "then"])).toEqual([]);
    expect(gone.map((s) => s.name)).toEqual(["order", "big", "each", "keep"]);
    expect(moveAt(steps, { list: [], index: 3 }, -1).map((s) => s.name)).toEqual(["order", "big", "keep", "each"]);
    // Nothing moves out of its list.
    expect(moveAt(steps, { list: [], index: 0 }, -1)).toEqual(steps);
  });
});

describe("the diagram", () => {
  const definition: Definition = { name: "Big orders", trigger: { webhook: {} }, steps };
  const laid = layout(definition);
  const node = (id: string) => laid.nodes.find((n) => n.id === id)!;

  it("runs from what starts it, through each step, to its end", () => {
    const top = laid.nodes.filter((n) => n.kind === "step" && n.at?.list.length === 0).map((n) => n.step?.name);
    expect(top).toEqual(["order", "big", "each", "keep"]);
    expect(node("trigger").y).toBeLessThan(node("step:0").y);
    expect(node("step:0").y).toBeLessThan(node("step:1").y);
    expect(node("step:3").y).toBeLessThan(node("end").y);
    expect(laid.edges.find((e) => e.source === "trigger")?.target).toBe("step:0");
  });

  it("stands an if's branches side by side, joined again, and an empty branch says a step can go there", () => {
    const then = node("step:1/then/0");
    const empty = node("empty:1/else");
    expect(then.y).toBe(empty.y);
    expect(then.x + NODE_W).toBeLessThanOrEqual(empty.x);
    expect(laid.edges.filter((e) => e.target === "join:1").map((e) => e.source).sort()).toEqual(["empty:1/else", "step:1/then/0"]);
    expect(laid.edges.find((e) => e.source === "join:1")?.target).toBe("step:2");
    expect(laid.edges.find((e) => e.target === "empty:1/else")?.insert).toEqual({ list: [1, "else"], index: 0 });
  });

  it("boxes a loop's steps, with the way back to the next item", () => {
    const box = node("box:2");
    const inner = node("step:2/steps/0");
    expect(inner.y).toBeGreaterThan(box.y);
    expect(inner.y + inner.height).toBeLessThan(box.y + box.height);
    const back = laid.edges.find((e) => e.back);
    expect(back).toMatchObject({ source: "join:2", target: "step:2" });
    expect(back?.insert).toBeUndefined();
  });

  it("says where a step added on each arrow goes", () => {
    const into = (target: string) => laid.edges.find((e) => e.target === target && !e.back)?.insert;
    expect(into("step:0")).toEqual({ list: [], index: 0 });
    expect(into("step:3")).toEqual({ list: [], index: 3 });
    expect(into("end")).toEqual({ list: [], index: 4 });
    expect(into("step:2/steps/0")).toEqual({ list: [2, "steps"], index: 0 });
  });

  it("is a trigger and an end for a workflow with no steps", () => {
    const bare = layout({ name: "x", steps: [] });
    expect(bare.nodes.map((n) => n.kind)).toEqual(["trigger", "end"]);
    expect(bare.edges[0]?.insert).toEqual({ list: [], index: 0 });
  });
});

describe("a step's card", () => {
  it("says in one line what the step does", () => {
    expect(summaryOf("http", { http: { method: "POST", url: "https://example.com/hook" } })).toBe("POST https://example.com/hook");
    expect(summaryOf("set", { set: { a: "1", b: "2" } })).toBe("2 fields");
    expect(summaryOf("python", { python: "x = 1\n\nresult = x" })).toBe("2 lines of code");
    expect(summaryOf("email", { email: { to: "{{ value.email }}" } })).toBe("to {{ value.email }}");
  });
});

describe("a switch", () => {
  const routed: Step[] = [
    {
      name: "size",
      switch: {
        cases: [
          { label: "big", when: "value.total > 500", steps: [{ name: "tell", stop: {} }] },
          { label: "middling", when: "value.total > 100", steps: [] },
        ],
        default: [{ name: "file", wait: "1s" }],
      },
    },
  ];

  it("keeps each case's steps, and its default's, at their own places", () => {
    expect(stepAt(routed, { list: [0, "case0"], index: 0 })?.name).toBe("tell");
    expect(stepAt(routed, { list: [0, "default"], index: 0 })?.name).toBe("file");
    const added = insertAt(routed, { list: [0, "case1"], index: 0 }, { name: "note", set: {} });
    expect(listAt(added, [0, "case1"]).map((s) => s.name)).toEqual(["note"]);
    // The cases' conditions stay as they were.
    expect((added[0]?.switch as { cases: { when: string }[] }).cases.map((c) => c.when)).toEqual(["value.total > 500", "value.total > 100"]);
  });

  it("lays its cases and default side by side, each labelled, joined again after", () => {
    const laid = layout({ name: "x", steps: routed });
    const labels = laid.edges.filter((e) => e.source === "step:0" && e.label).map((e) => e.label);
    expect(labels).toEqual(["big", "middling", "default"]);
    const tell = laid.nodes.find((n) => n.id === "step:0/case0/0")!;
    const file = laid.nodes.find((n) => n.id === "step:0/default/0")!;
    expect(tell.x).toBeLessThan(file.x);
    expect(laid.width).toBeGreaterThanOrEqual(NODE_W * 3);
    expect(laid.edges.filter((e) => e.target === "join:0")).toHaveLength(3);
    expect(summaryOf("switch", routed[0]!)).toBe("2 cases and a default");
  });
});
