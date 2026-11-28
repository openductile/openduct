// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// Forms in the studio: building a form's fields, its preview, the form and approval steps, a wait's three ways, and
// the runs that wait shown as waiting.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useState } from "react";
import { FormFields, FormPreview, FormStepBody, FormTriggerSettings, WaitBody, WaitingRuns, type FormField } from "./FormBuilder";
import { summaryOf, triggerSummary } from "./flow/look";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

/** A component that holds what it is given, and says each change. */
function Holding<T>(props: { start: T; seen: T[]; children: (value: T, set: (v: T) => void) => React.ReactNode }) {
  const [value, setValue] = useState(props.start);
  return (
    <>
      {props.children(value, (v) => {
        props.seen.push(v);
        setValue(v);
      })}
    </>
  );
}

describe("a form's fields", () => {
  it("are added, typed, given choices and made required", () => {
    const seen: FormField[][] = [];
    render(<Holding start={[] as FormField[]} seen={seen}>{(fields, set) => <FormFields fields={fields} onChange={set} />}</Holding>);
    fireEvent.click(screen.getByText("+ Field"));
    expect(seen.at(-1)).toEqual([{ name: "field_1", label: "Field 1", type: "text" }]);
    fireEvent.change(screen.getByLabelText("Field name"), { target: { value: "t-shirt size" } });
    fireEvent.change(screen.getByLabelText("Field type"), { target: { value: "dropdown" } });
    fireEvent.change(screen.getByLabelText("Choices"), { target: { value: "S, M ,, L" } });
    fireEvent.click(screen.getByLabelText("Required"));
    expect(seen.at(-1)).toEqual([{ name: "t_shirt_size", label: "Field 1", type: "dropdown", options: ["S", "M", "L"], required: true }]);
    fireEvent.click(screen.getByText("+ Field"));
    fireEvent.click(screen.getAllByLabelText("Move the field up")[1]!);
    expect(seen.at(-1)!.map((f) => f.name)).toEqual(["field_2", "t_shirt_size"]);
    fireEvent.click(screen.getAllByLabelText("Remove the field")[0]!);
    expect(seen.at(-1)!.map((f) => f.name)).toEqual(["t_shirt_size"]);
  });

  it("show a file's limits and a number's bounds, and a preview as people see the page", () => {
    render(
      <FormPreview
        title="Apply"
        fields={[
          { name: "cv", label: "Your CV", type: "file", required: true },
          { name: "age", type: "number" },
          { name: "size", type: "dropdown", options: ["S", "M"] },
        ]}
        approval={{ approve: "Hire", reject: "Not now", comment: true }}
      />,
    );
    const preview = screen.getByLabelText("Preview of the form");
    expect(preview.textContent).toContain("Apply");
    expect(preview.textContent).toContain("Your CV *");
    expect(screen.getByText("Hire")).toBeTruthy();
    expect(screen.getByText("Not now")).toBeTruthy();
    expect(screen.getByText("Comment (optional)")).toBeTruthy();
  });
});

describe("a form trigger", () => {
  it("says its address once saved and keeps its protection", () => {
    const seen: Record<string, unknown>[] = [];
    render(
      <Holding start={{ title: "Contact", fields: [{ name: "email", type: "email" }] } as Record<string, unknown>} seen={seen}>
        {(s, set) => <FormTriggerSettings settings={s} address="/forms/abc123" onChange={set} />}
      </Holding>,
    );
    expect(screen.getByRole("link").getAttribute("href")).toBe(`${location.origin}/forms/abc123`);
    fireEvent.change(screen.getByPlaceholderText("none: anyone with the address"), { target: { value: "s3cret" } });
    fireEvent.change(screen.getByPlaceholderText("10 (0: no limit)"), { target: { value: "3" } });
    expect(seen.at(-1)).toMatchObject({ password: "s3cret", rate_limit: 3 });
  });
});

describe("the steps that wait", () => {
  it("an approval asks for a comment and says what to do when no answer comes", () => {
    const seen: unknown[] = [];
    render(
      <Holding start={{ title: "Refund?" } as unknown} seen={seen}>
        {(body, set) => <FormStepBody kind="approval" fields={body as Record<string, unknown>} setBody={set} />}
      </Holding>,
    );
    fireEvent.click(screen.getByLabelText("Ask for a comment"));
    fireEvent.change(screen.getByDisplayValue("the step fails"), { target: { value: "continue" } });
    fireEvent.change(screen.getByPlaceholderText("7d (at most 90d)"), { target: { value: "2d" } });
    expect(seen.at(-1)).toEqual({ title: "Refund?", comment: true, on_timeout: "continue", timeout: "2d" });
    expect(screen.getAllByText(/links\.<this step>/).length).toBeGreaterThan(0);
  });

  it("a wait is a while, a time or a call to its address", () => {
    const seen: unknown[] = [];
    render(<Holding start={"10s" as unknown} seen={seen}>{(body, set) => <WaitBody body={body} setBody={set} />}</Holding>);
    fireEvent.click(screen.getByRole("radio", { name: "A call to its address" }));
    expect(seen.at(-1)).toEqual({ webhook: true, timeout: "1d" });
    fireEvent.change(screen.getByDisplayValue("the step fails"), { target: { value: "continue" } });
    expect(seen.at(-1)).toEqual({ webhook: true, timeout: "1d", on_timeout: "continue" });
    fireEvent.click(screen.getByRole("radio", { name: "Until a time" }));
    expect(seen.at(-1)).toEqual({ until: "value.at" });
    fireEvent.click(screen.getByRole("radio", { name: "A while" }));
    fireEvent.change(screen.getByPlaceholderText("10s, 5m, 2d"), { target: { value: "2d" } });
    expect(seen.at(-1)).toBe("2d");
    expect(summaryOf("wait", { wait: { webhook: true } })).toBe("for a call to its address");
    expect(summaryOf("approval", { approval: { title: "Refund?" } })).toBe("Refund?");
    expect(triggerSummary({ form: { title: "x" } }).line).toBe("someone fills in a form");
  });
});

describe("the runs that wait", () => {
  it("are shown as waiting, with their form's address", async () => {
    vi.stubGlobal("fetch", async (path: string) => {
      expect(path).toBe("/api/workflows/w1/waiting");
      const waiting = [
        { run: "1-4", step: "ok", kind: "approval", title: "Refund 40?", since: 1, until: 2, on_timeout: "continue", answered: null, url: "http://x/waits/T" },
        { run: "1-5", step: "later", kind: "time", title: "", since: 1, until: 2, on_timeout: "continue", answered: null, url: null },
      ];
      return new Response(JSON.stringify({ waiting }), { status: 200 });
    });
    render(<WaitingRuns id="w1" />);
    await waitFor(() => expect(screen.getByText("Waiting now (2)")).toBeTruthy());
    expect(screen.getAllByText("waiting")).toHaveLength(2);
    expect(screen.getByText("Open the form").getAttribute("href")).toBe("http://x/waits/T");
    expect(screen.getByText("goes on by itself")).toBeTruthy();
  });
});
