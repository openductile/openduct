// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The web step and the feed trigger on the page: the step is offered and found by what people call it; its form
// writes only what each action takes (the engine refuses the rest); the canvas says what it does; the feed trigger
// is offered and described.
// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { StepCard, fresh, KINDS } from "./Steps";
import { webFor } from "./StepsWeb";
import { summaryOf, triggerSummary, FAMILY } from "./flow/look";

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

describe("web", () => {
  it("offers the step and says what it and the feed trigger do", () => {
    expect(KINDS.some((k) => k.kind === "web")).toBe(true);
    expect(FAMILY.web).toBe("call");
    expect(fresh("web", new Set())).toEqual({ name: "web_1", web: { action: "fetch", url: "{{ value.url }}" } });
    expect(summaryOf("web", { web: { url: "https://example.com/blog/post" } })).toBe("fetch example.com/blog/post");
    expect(summaryOf("web", { web: { action: "select", url: "https://shop.example/", fields: { price: ".price", title: "h1" } } })).toBe(
      "pick price, title from shop.example/",
    );
    expect(summaryOf("web", { web: { action: "crawl", url: "https://docs.example/", depth: 2 } })).toBe("crawl docs.example/ (depth 2, 10 pages)");
    expect(summaryOf("web", { web: { action: "fetch", from: "steps.get.body" } })).toBe("fetch the page in steps.get.body");
    expect(triggerSummary({ feed: { url: "https://example.com/feed.xml" } }).line).toBe("a new item in the feed example.com/feed.xml");
  });

  it("keeps only the settings an action takes", () => {
    expect(webFor("crawl", { action: "fetch", url: "https://x.example/", html: "file", headers: { a: "b" }, format: "text" })).toEqual({
      action: "crawl",
      url: "https://x.example/",
      format: "text",
    });
    expect(webFor("select", { action: "fetch", url: "u", format: "text" })).toEqual({ action: "select", url: "u", fields: { title: "h1" } });
    expect(webFor("sitemap", { action: "fetch", from: "value.html" })).toEqual({ action: "sitemap", url: "" });
  });

  it("the form changes the action, the fields picked and how a crawl goes", () => {
    let last: Record<string, unknown> = {};
    render(<Card start={fresh("web", new Set())} seen={(s) => (last = s)} />);
    fireEvent.change(screen.getByDisplayValue("Read a page"), { target: { value: "select" } });
    expect(last.web).toEqual({ action: "select", url: "{{ value.url }}", fields: { title: "h1" } });
    fireEvent.change(screen.getByDisplayValue("h1"), { target: { value: "h1.title" } });
    expect((last.web as Record<string, unknown>).fields).toEqual({ title: "h1.title" });
    fireEvent.click(screen.getByText("+ a field"));
    fireEvent.change(screen.getAllByPlaceholderText("its text (or an attribute: href)")[1]!, { target: { value: "href" } });
    expect((last.web as Record<string, unknown>).fields).toEqual({ title: "h1.title", field_2: { selector: "", attribute: "href" } });
    fireEvent.change(screen.getByDisplayValue("Pick fields out of a page"), { target: { value: "crawl" } });
    expect(last.web).toEqual({ action: "crawl", url: "{{ value.url }}" });
    fireEvent.click(screen.getByLabelText("Honour the site's robots.txt"));
    expect((last.web as Record<string, unknown>).robots).toBe(false);
    expect(screen.getByText(/Crawl past it only a site that is yours/)).toBeTruthy();
  });
});
