// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Where a step is in a workflow, and the changes made there: a path walks from the workflow's steps into an
// "if" step's then or else branch, a "switch" step's cases (case0, case1, ...) or default, or a "for_each" step's
// own steps.

import type { Step } from "../api";
import { aiBranchOf, aiWithBranch } from "./aiBranches"; // G7 more AI steps

/** Which of a step's lists: an if's then or else, a switch's case (by its place) or default, a loop's steps. */
export type Part = "then" | "else" | "steps" | "default" | `case${number}`;
/** A list of steps: the workflow's own ([]), or a branch or loop of the step at a path. */
export type ListPath = (number | Part)[];
/** One step: the list it is in, and its place in it. */
export type StepPath = { list: ListPath; index: number };

type Case = { steps?: Step[] } & Record<string, unknown>;
type Choice = { cases?: Case[]; default?: Step[] } & Record<string, unknown>;

const caseOf = (part: Part): number | null => (part.startsWith("case") ? Number(part.slice(4)) : null);

const branchOf = (step: Step, part: Part): Step[] => {
  const routed = aiBranchOf(step, part); // G7 more AI steps: a category's steps
  if (routed) return routed;
  if (part === "steps") return ((step.for_each as { steps?: Step[] } | undefined)?.steps ?? []) as Step[];
  const n = caseOf(part);
  const choice = step.switch as Choice | undefined;
  if (n !== null) return choice?.cases?.[n]?.steps ?? [];
  if (part === "default") return choice?.default ?? [];
  return ((step.if as Record<string, Step[] | undefined> | undefined)?.[part] ?? []) as Step[];
};

const withBranch = (step: Step, part: Part, steps: Step[]): Step => {
  const routed = aiWithBranch(step, part, steps); // G7 more AI steps
  if (routed) return routed;
  if (part === "steps") return { ...step, for_each: { ...(step.for_each as object), steps } };
  const n = caseOf(part);
  const choice = (step.switch ?? {}) as Choice;
  if (n !== null) return { ...step, switch: { ...choice, cases: (choice.cases ?? []).map((c, i) => (i === n ? { ...c, steps } : c)) } };
  if (part === "default") return { ...step, switch: { ...choice, default: steps } };
  return { ...step, if: { ...(step.if as object), [part]: steps } };
};

/** The steps of the list at `path`. */
export function listAt(steps: Step[], path: ListPath): Step[] {
  let list = steps;
  for (let i = 0; i < path.length; i += 2) {
    const step = list[path[i] as number];
    if (!step) return [];
    list = branchOf(step, path[i + 1] as Part);
  }
  return list;
}

/** `steps` with the list at `path` changed by `change`. */
export function changeList(steps: Step[], path: ListPath, change: (list: Step[]) => Step[]): Step[] {
  if (path.length === 0) return change(steps);
  const [index, part, ...rest] = path as [number, Part, ...ListPath];
  return steps.map((step, i) =>
    i === index ? withBranch(step, part, changeList(branchOf(step, part), rest, change)) : step,
  );
}

export function stepAt(steps: Step[], at: StepPath): Step | undefined {
  return listAt(steps, at.list)[at.index];
}

export function replaceAt(steps: Step[], at: StepPath, step: Step): Step[] {
  return changeList(steps, at.list, (list) => list.map((s, i) => (i === at.index ? step : s)));
}

export function removeAt(steps: Step[], at: StepPath): Step[] {
  return changeList(steps, at.list, (list) => list.filter((_, i) => i !== at.index));
}

export function insertAt(steps: Step[], at: StepPath, step: Step): Step[] {
  return changeList(steps, at.list, (list) => [...list.slice(0, at.index), step, ...list.slice(at.index)]);
}

/** The step moved `by` places within its list; where it would leave the list, nothing changes. */
export function moveAt(steps: Step[], at: StepPath, by: number): Step[] {
  return changeList(steps, at.list, (list) => {
    const to = at.index + by;
    if (to < 0 || to >= list.length) return list;
    const next = [...list];
    const [moved] = next.splice(at.index, 1);
    if (moved) next.splice(to, 0, moved);
    return next;
  });
}

/** A stable key for a path, for node ids. */
export function keyOf(at: StepPath): string {
  return [...at.list, at.index].join("/");
}

export function sameStep(a: StepPath | null, b: StepPath | null): boolean {
  return !!a && !!b && keyOf(a) === keyOf(b);
}
