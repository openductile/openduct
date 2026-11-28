// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The branches of the AI steps that route as a switch does: a "classify" step's categories (case0, case1, ...) and
// its fallback, other (default); a "sentiment" step's labels. Each holds the steps that run when the model chose it.

import type { Step } from "../api";
import type { Part } from "./paths";

/** The AI steps that route. */
export const ROUTING = ["classify", "sentiment"];

type Choice = string | ({ name?: string; description?: string; steps?: Step[] } & Record<string, unknown>);
type Routed = { categories?: Choice[]; labels?: Choice[]; other?: boolean; default?: Step[] } & Record<string, unknown>;

function routedOf(step: Step): { key: "classify" | "sentiment"; body: Routed; list: "categories" | "labels" } | null {
  if (step.classify && typeof step.classify === "object") return { key: "classify", body: step.classify as Routed, list: "categories" };
  if (step.sentiment && typeof step.sentiment === "object") return { key: "sentiment", body: step.sentiment as Routed, list: "labels" };
  return null;
}

const caseOf = (part: Part): number | null => (part.startsWith("case") ? Number(part.slice(4)) : null);
const nameOf = (c: Choice | undefined, i: number) => (typeof c === "string" ? c : c?.name) || `choice ${i + 1}`;
const stepsOf = (c: Choice | undefined): Step[] => (c && typeof c === "object" ? (c.steps ?? []) : []);

/** The branches a routing AI step splits into, left to right: each choice, then (classify) other. */
export function aiBranches(step: Step): { part: Part; label: string; steps: Step[] }[] {
  const routed = routedOf(step);
  if (!routed) return [];
  const choices = routed.body[routed.list] ?? [];
  const out = choices.map((c, i) => ({ part: `case${i}` as Part, label: nameOf(c, i), steps: stepsOf(c) }));
  if (routed.key === "classify" && routed.body.other !== false) out.push({ part: "default", label: "other", steps: routed.body.default ?? [] });
  return out;
}

/** The steps of one of its branches; null for a step that is not a routing AI step. */
export function aiBranchOf(step: Step, part: Part): Step[] | null {
  const routed = routedOf(step);
  if (!routed) return null;
  if (part === "default") return routed.body.default ?? [];
  const n = caseOf(part);
  return n === null ? [] : stepsOf(routed.body[routed.list]?.[n]);
}

/** The step with one of its branches' steps changed; null for a step that is not a routing AI step. */
export function aiWithBranch(step: Step, part: Part, steps: Step[]): Step | null {
  const routed = routedOf(step);
  if (!routed) return null;
  if (part === "default") return { ...step, [routed.key]: { ...routed.body, default: steps } };
  const n = caseOf(part);
  const list = (routed.body[routed.list] ?? []).map((c, i) => (i === n ? { ...(typeof c === "string" ? { name: c } : c), steps } : c));
  return { ...step, [routed.key]: { ...routed.body, [routed.list]: list } };
}

/** The lists of steps a routing AI step holds. */
export function aiInnerLists(step: Step): Step[][] {
  return aiBranches(step).map((b) => b.steps);
}
