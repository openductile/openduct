// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A workflow laid out as a diagram, top to bottom: what starts it, then its steps in order. An "if" step's two
// branches, and a "switch" step's cases and default, stand side by side and join again; a "for_each" step's own
// steps sit in a box under it. Every arrow knows where a step added on it goes.

import type { Definition, Step } from "../api";
import { kindOf } from "../Steps";
import type { ListPath, Part as Which, StepPath } from "./paths";
import { keyOf } from "./paths";

export const NODE_W = 196;
export const NODE_H = 46;
const SMALL = 16;
const GAP_Y = 32;
const GAP_X = 28;
const PAD = 16;

export type NodeKind = "trigger" | "step" | "join" | "empty" | "end" | "loop-box";

export type LaidNode = {
  id: string;
  kind: NodeKind;
  x: number;
  y: number;
  width: number;
  height: number;
  /** The step's place, for a step (and the list an empty branch belongs to). */
  at?: StepPath;
  step?: Step;
  /** For an empty branch: which one. */
  label?: string;
};

export type LaidEdge = {
  id: string;
  source: string;
  target: string;
  /** Where a step added on this arrow goes; none for the loop's way back. */
  insert?: StepPath;
  label?: string;
  back?: boolean;
};

export type Laid = { nodes: LaidNode[]; edges: LaidEdge[]; width: number; height: number };

type Part = { width: number; height: number };

type Branch = { part: Which; label: string; steps: Step[] };

/** The branches a step splits into, left to right: an if's then and else; a switch's cases, then its default. */
function branchesOf(step: Step): Branch[] {
  const kind = kindOf(step);
  if (kind === "if") {
    const b = (step.if ?? {}) as { then?: Step[]; else?: Step[] };
    return [
      { part: "then", label: "then", steps: b.then ?? [] },
      { part: "else", label: "else", steps: b.else ?? [] },
    ];
  }
  if (kind === "switch") {
    const s = (step.switch ?? {}) as { cases?: { label?: string; steps?: Step[] }[]; default?: Step[] };
    return [
      ...(s.cases ?? []).map((c, i) => ({ part: `case${i}` as Which, label: c.label || `case ${i + 1}`, steps: c.steps ?? [] })),
      { part: "default", label: "default", steps: s.default ?? [] },
    ];
  }
  return [];
}

function loopOf(step: Step): Step[] {
  return ((step.for_each ?? {}) as { steps?: Step[] }).steps ?? [];
}

function measureList(steps: Step[]): Part {
  if (steps.length === 0) return { width: NODE_W, height: SMALL };
  let width = NODE_W;
  let height = 0;
  steps.forEach((s, i) => {
    const p = measureStep(s);
    width = Math.max(width, p.width);
    height += p.height + (i > 0 ? GAP_Y : 0);
  });
  return { width, height };
}

function measureStep(step: Step): Part {
  const kind = kindOf(step);
  if (kind === "if" || kind === "switch") {
    const parts = branchesOf(step).map((b) => measureList(b.steps));
    return {
      width: Math.max(NODE_W, parts.reduce((w, p) => w + p.width, 0) + GAP_X * (parts.length - 1)),
      height: NODE_H + GAP_Y + Math.max(0, ...parts.map((p) => p.height)) + GAP_Y + SMALL,
    };
  }
  if (kind === "for_each") {
    const inner = measureList(loopOf(step));
    return {
      width: Math.max(NODE_W, inner.width + PAD * 2),
      height: NODE_H + GAP_Y / 2 + PAD + inner.height + GAP_Y / 2 + SMALL + PAD,
    };
  }
  return { width: NODE_W, height: NODE_H };
}

class Builder {
  nodes: LaidNode[] = [];
  edges: LaidEdge[] = [];

  node(n: LaidNode): string {
    this.nodes.push(n);
    return n.id;
  }

  edge(source: string, target: string, insert?: StepPath, label?: string, back?: boolean) {
    this.edges.push({ id: `${source}->${target}${back ? "/back" : ""}`, source, target, insert, label, back });
  }

  /**
   * Lays `steps` (the list at `list`) out with its middle at `cx` from `y`, joined from `from` (the arrow into the
   * first step adds at the list's start). Gives back the node the next thing joins from.
   */
  list(steps: Step[], list: ListPath, cx: number, y: number, from: string, firstLabel?: string): string {
    if (steps.length === 0) {
      const id = this.node({
        id: `empty:${list.join("/")}`,
        kind: "empty",
        x: cx - NODE_W / 2,
        y,
        width: NODE_W,
        height: SMALL,
        at: { list, index: 0 },
        label: firstLabel,
      });
      this.edge(from, id, { list, index: 0 }, firstLabel);
      return id;
    }
    let previous = from;
    let top = y;
    steps.forEach((step, index) => {
      const at = { list, index };
      const part = measureStep(step);
      const last = this.step(step, at, cx, top, previous, index === 0 ? firstLabel : undefined);
      previous = last;
      top += part.height + GAP_Y;
    });
    return previous;
  }

  step(step: Step, at: StepPath, cx: number, y: number, from: string, label?: string): string {
    const kind = kindOf(step);
    const id = `step:${keyOf(at)}`;
    this.node({ id, kind: "step", x: cx - NODE_W / 2, y, width: NODE_W, height: NODE_H, at, step });
    this.edge(from, id, at, label);
    const base: ListPath = [...at.list, at.index];
    if (kind === "if" || kind === "switch") {
      const branches = branchesOf(step);
      const parts = branches.map((b) => measureList(b.steps));
      const total = parts.reduce((w, p) => w + p.width, 0) + GAP_X * (branches.length - 1);
      const top = y + NODE_H + GAP_Y;
      let left = cx - total / 2;
      const ends = branches.map((b, i) => {
        const width = parts[i]?.width ?? NODE_W;
        const end = this.list(b.steps, [...base, b.part], left + width / 2, top, id, b.label);
        left += width + GAP_X;
        return end;
      });
      const joinY = top + Math.max(0, ...parts.map((p) => p.height)) + GAP_Y;
      const join = this.node({ id: `join:${keyOf(at)}`, kind: "join", x: cx - SMALL / 2, y: joinY, width: SMALL, height: SMALL });
      branches.forEach((b, i) => this.edge(ends[i] ?? id, join, { list: [...base, b.part], index: b.steps.length }));
      return join;
    }
    if (kind === "for_each") {
      const steps = loopOf(step);
      const inner = measureList(steps);
      const width = Math.max(NODE_W, inner.width + PAD * 2);
      const boxTop = y + NODE_H + GAP_Y / 2;
      const boxHeight = PAD + inner.height + GAP_Y / 2 + SMALL + PAD;
      this.node({ id: `box:${keyOf(at)}`, kind: "loop-box", x: cx - width / 2, y: boxTop, width, height: boxHeight });
      const end = this.list(steps, [...base, "steps"], cx, boxTop + PAD, id, "each");
      const again = this.node({
        id: `join:${keyOf(at)}`,
        kind: "join",
        x: cx - SMALL / 2,
        y: boxTop + PAD + inner.height + GAP_Y / 2,
        width: SMALL,
        height: SMALL,
      });
      this.edge(end, again, { list: [...base, "steps"], index: steps.length });
      this.edge(again, id, undefined, "next item", true);
      return again;
    }
    return id;
  }
}

/** The diagram of `definition`. */
export function layout(definition: Definition): Laid {
  const steps = definition.steps ?? [];
  const main = measureList(steps);
  const width = Math.max(NODE_W, main.width);
  const cx = width / 2;
  const b = new Builder();
  b.node({ id: "trigger", kind: "trigger", x: cx - NODE_W / 2, y: 0, width: NODE_W, height: NODE_H });
  const top = NODE_H + GAP_Y;
  let last = "trigger";
  if (steps.length > 0) last = b.list(steps, [], cx, top, "trigger");
  const endY = steps.length > 0 ? top + main.height + GAP_Y : top;
  b.node({ id: "end", kind: "end", x: cx - 36, y: endY, width: 72, height: 24 });
  b.edge(last, "end", { list: [], index: steps.length });
  return { nodes: b.nodes, edges: b.edges, width, height: endY + 24 };
}
