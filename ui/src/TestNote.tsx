// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// What a test run says beside a step's result: an answer it was given in place of sending - an app's recorded
// example, or the sample answer an HTTP step was given - with the request it would have sent; or that a table is
// not made yet, so the test used an empty one. // test runs

import type { StepRun } from "./api";

export function TestNote({ step }: { step: StepRun }) {
  if (!step.sample && !step.note) return null;
  return (
    <div className="test-note">
      {step.note && <p className="hint">{step.note}</p>}
      {step.sample && (
        <details>
          <summary className="hint">{step.sample[0]!.toUpperCase() + step.sample.slice(1)}: what it would have sent</summary>
          <pre>{JSON.stringify(step.request ?? null, null, 2)}</pre>
        </details>
      )}
    </div>
  );
}
