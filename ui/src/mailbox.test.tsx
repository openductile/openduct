// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A mailbox on the page: the mailbox step is offered with the credential it takes, its form writes only what each
// action takes (the engine refuses the rest), the canvas says what it does, and the new-email trigger is offered and
// described.
// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { StepCard, fresh, KINDS, TAKES } from "./Steps";
import { summaryOf, triggerSummary } from "./flow/look";
import { kindLook } from "./credentialLook";

afterEach(cleanup);

function Card(props: { start: Record<string, unknown>; seen: (s: Record<string, unknown>) => void }) {
  const [step, setStep] = useState(props.start);
  return (
    <StepCard
      step={step}
      taken={new Set()}
      onChange={(s) => {
        setStep(s);
        props.seen(s);
      }}
      onRemove={() => {}}
      onUp={() => {}}
      onDown={() => {}}
    />
  );
}

describe("mailbox", () => {
  it("offers the step and its credential, and says what each does", () => {
    expect(KINDS.some((k) => k.kind === "mailbox")).toBe(true);
    expect(TAKES.mailbox).toEqual(["imap"]);
    expect(kindLook("imap").name).toBe("Mailbox (IMAP)");
    expect(fresh("mailbox", new Set())).toEqual({ name: "mailbox_1", mailbox: { action: "list", credential: "", unseen: true } });
    expect(summaryOf("mailbox", { mailbox: { action: "list", credential: "shop", unseen: true, from: "billing@example.com" } })).toBe(
      "list unread from billing@example.com in INBOX - shop",
    );
    expect(summaryOf("mailbox", { mailbox: { action: "move", credential: "shop", uid: "{{ value.uid }}", to_folder: "Archive" } })).toBe(
      "move to Archive - shop",
    );
    expect(triggerSummary({ mailbox: { credential: "shop", from: "billing@example.com" } }).line).toBe(
      "a new email from billing@example.com in INBOX - shop",
    );
  });

  it("a mailbox step's form keeps only what its action takes", () => {
    let last: Record<string, unknown> = {};
    render(<Card start={{ name: "m", mailbox: { action: "list", credential: "shop", unseen: true, from: "a@example.com" } }} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("List or search messages"), { target: { value: "read" } });
    expect(last.mailbox).toEqual({ action: "read", credential: "shop" });
    fireEvent.click(screen.getByLabelText("The whole message as a file too (.eml)"));
    expect((last.mailbox as Record<string, unknown>).raw).toBe(true);
    fireEvent.click(screen.getByLabelText("Attachments as files"));
    expect((last.mailbox as Record<string, unknown>).files).toBe(false);
    fireEvent.change(screen.getByDisplayValue("Read a message (its text and attachments)"), { target: { value: "mark" } });
    expect(last.mailbox).toEqual({ action: "mark", credential: "shop" });
    fireEvent.change(screen.getAllByDisplayValue("leave as it is")[0]!, { target: { value: "set" } });
    expect((last.mailbox as Record<string, unknown>).seen).toBe(true);
  });
});
