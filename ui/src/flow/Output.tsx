// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// What one step did in the last test, preview or run: whether it worked, how long it took, its output (an AI
// step's as a conversation), what it logged, and its error.

import type { Run, StepRun } from "../api";
import { AiResult, isAi } from "../Editor";

export type Status = "done" | "failed" | "stopped" | "unreached";

/** Each step's part in `run`, by name (a step in a loop: its last time). */
export function partsOf(run: Run | null): Map<string, StepRun> {
  const parts = new Map<string, StepRun>();
  for (const s of run?.steps ?? []) parts.set(s.name, s);
  return parts;
}

export function statusOf(run: Run | null, name: string | undefined): Status | undefined {
  if (!run || !name) return undefined;
  const part = partsOf(run).get(name);
  if (!part) return "unreached";
  if (part.error) return "failed";
  if (run.status === "skipped" && run.error === null && part.result === false) return "stopped";
  return "done";
}

export function StepOutput(props: { run: Run | null; name: string | undefined; testedWhat?: string }) {
  if (!props.run) {
    return <p className="hint">Press Test to run the workflow on the sample input and see what this step gives back - nothing is sent.</p>;
  }
  const part = props.name ? partsOf(props.run).get(props.name) : undefined;
  if (!part) {
    return <p className="hint">This step did not run in the last {props.testedWhat ?? "test"}: an earlier step ended it.</p>;
  }
  return (
    <div className="output">
      <p className="chips">
        <span className={part.error ? "bad" : "good"}>{part.error ? "failed" : "done"}</span>
        <span>{part.ms} ms</span>
        {part.attempts > 1 && <span>{part.attempts} tries</span>}
      </p>
      {part.error && <pre className="error-text">{part.error}</pre>}
      {isAi(part) ? <AiResult step={part} /> : <pre>{JSON.stringify(part.result, null, 2)}</pre>}
      {part.logs && part.logs.length > 0 && (
        <>
          <p className="hint">What it logged</p>
          <pre className="logs">{part.logs.join("\n")}</pre>
        </>
      )}
    </div>
  );
}
