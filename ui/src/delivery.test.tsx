// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// CI/CD on the page, against a stand-in API: API tokens made (shown once) and revoked, the workspace marked as an
// environment and locked, Git sync set, saved to and deployed from, a webhook secret shown once, a workflow promoted
// after a check, and where each version came from.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ApiTokens, EnvironmentSettings, GitSync } from "./Delivery";
import { Promote, SourceOf } from "./Promote";

type Answer = { status?: number; body: unknown };

function stand_in(routes: Record<string, Answer | ((body: unknown) => Answer)>) {
  const sent: { route: string; body: unknown }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    const route = `${init.method ?? "GET"} ${path}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    sent.push({ route, body });
    const found = routes[route];
    const answer = typeof found === "function" ? found(body) : (found ?? { status: 404, body: { error: "no such request" } });
    return new Response(JSON.stringify(answer.body), { status: answer.status ?? 200 });
  });
  return sent;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const token = { id: "t1", name: "Nightly deploy", hint: "a1b2", role: "editor", workflows: null, per_minute: 120, created: 1, expires: 4102444800, by: "ada@example.com", expired: false };
const delivery = { environment: null, order: 0, locked: false, git: null, last_deploy: null, manage: true };

describe("API tokens", () => {
  it("lists them, makes one shown once with a role, days and workflows, and revokes one", async () => {
    const sent = stand_in({
      "GET /api/tokens": { body: { tokens: [token], default_days: 90, max_days: 365 } },
      "POST /api/tokens": (body) => ({ status: 201, body: { ...token, ...(body as object), id: "t2", token: "odapi_secret-once" } }),
      "DELETE /api/tokens/t1": { body: { revoked: "t1" } },
    });
    render(<ApiTokens workspace="w1" role="admin" />);
    expect(await screen.findByText("Nightly deploy")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("What the API token is for"), { target: { value: "GitHub Actions" } });
    fireEvent.change(screen.getByLabelText("Its role"), { target: { value: "viewer" } });
    fireEvent.change(screen.getByLabelText("Days it lasts"), { target: { value: "30" } });
    fireEvent.change(screen.getByLabelText("Only these workflows"), { target: { value: "billing-*, Orders" } });
    fireEvent.click(screen.getByRole("button", { name: /Make an API token/ }));
    expect(await screen.findByText("odapi_secret-once")).toBeTruthy();
    expect(sent.find((s) => s.route === "POST /api/tokens")?.body).toEqual({ name: "GitHub Actions", role: "viewer", days: 30, workflows: ["billing-*", "Orders"] });
    fireEvent.click(screen.getAllByRole("button", { name: /Revoke/ })[0]!);
    await waitFor(() => expect(sent.some((s) => s.route === "DELETE /api/tokens/t1")).toBe(true));
  });

  it("offers no role above the maker's", async () => {
    stand_in({ "GET /api/tokens": { body: { tokens: [], default_days: 90, max_days: 365 } } });
    render(<ApiTokens workspace="w1" role="editor" />);
    await screen.findByText("None yet");
    const roles = Array.from((screen.getByLabelText("Its role") as HTMLSelectElement).options).map((o) => o.value);
    expect(roles).toEqual(["viewer", "editor"]);
  });
});

describe("the workspace's environment", () => {
  it("is marked, ordered and locked by an admin", async () => {
    const sent = stand_in({
      "GET /api/cicd/settings": { body: delivery },
      "PUT /api/cicd/settings": (body) => ({ body: { ...delivery, environment: "production", order: 3, ...(body as object) } }),
    });
    render(<EnvironmentSettings workspace="w1" admin />);
    fireEvent.click(await screen.findByRole("radio", { name: "Production" }));
    await waitFor(() => expect(sent.find((s) => s.route === "PUT /api/cicd/settings")?.body).toEqual({ environment: "production" }));
    const lock = await screen.findByLabelText("Changes only by push or promotion");
    fireEvent.click(lock);
    await waitFor(() => expect(sent.filter((s) => s.route === "PUT /api/cicd/settings")[1]?.body).toEqual({ locked: true }));
    expect(await screen.findByText("Saved.")).toBeTruthy();
  });

  it("is only shown to others", async () => {
    stand_in({ "GET /api/cicd/settings": { body: { ...delivery, environment: "test", order: 2, manage: false } } });
    render(<EnvironmentSettings workspace="w1" admin={false} />);
    expect(await screen.findByText("Test")).toBeTruthy();
    expect(screen.queryByRole("radio")).toBeNull();
    expect((screen.getByLabelText("Changes only by push or promotion") as HTMLInputElement).disabled).toBe(true);
  });
});

describe("Git sync", () => {
  it("sets the repository, saves to it, deploys from it and shows the webhook's secret once", async () => {
    const git = { provider: "github", url: "https://github.com/acme/flows", branch: "main", folder: "workflows", credential: "github", changes: "commit", hook: "/hooks/git/w1", secret: false, signing_token: false };
    const sent = stand_in({
      "GET /api/cicd/settings": { body: delivery },
      "GET /api/credentials": { body: { credentials: [{ name: "github", kind: "bearer", fields: ["token"], updated: 1 }, { name: "smtp", kind: "smtp", fields: [], updated: 1 }] } },
      "PUT /api/cicd/settings": { body: { ...delivery, git } },
      "POST /api/cicd/git/save": { body: { saved: 2, unchanged: 0, commit: "1a2b3c4d5e", pull_request: null } },
      "POST /api/cicd/git/deploy": { body: { workflows: [], summary: { create: 1, update: 0, unchanged: 1 }, dry_run: false, at: "main" } },
      "POST /api/cicd/git/secret": { status: 201, body: { secret: "hook-secret-once", hook: "/hooks/git/w1" } },
    });
    render(<GitSync workspace="w1" admin />);
    fireEvent.change(await screen.findByLabelText("Repository address"), { target: { value: "https://github.com/acme/flows" } });
    await waitFor(() => expect(screen.getAllByRole("option", { name: "github" }).length).toBe(1));
    fireEvent.change(screen.getByLabelText("Token credential"), { target: { value: "github" } });
    fireEvent.click(screen.getByRole("button", { name: /Save the repository/ }));
    await waitFor(() => expect(sent.find((s) => s.route === "PUT /api/cicd/settings")).toBeTruthy());
    expect((sent.find((s) => s.route === "PUT /api/cicd/settings")?.body as { git: unknown }).git).toMatchObject({ provider: "github", url: "https://github.com/acme/flows", branch: "main", folder: "workflows", credential: "github", changes: "commit" });
    expect(await screen.findByText(`${location.origin}/hooks/git/w1`)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Save to Git/ }));
    expect(await screen.findByText("2 file(s) committed (1a2b3c4).")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Deploy from Git/ }));
    expect(await screen.findByText("Deployed: 1 new, 0 changed, 1 unchanged.")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Make its secret/ }));
    expect(await screen.findByText("hook-secret-once")).toBeTruthy();
  });
});

describe("promotion", () => {
  it("checks first, then promotes to an environment after this one", async () => {
    const sent = stand_in({
      "GET /api/cicd/environments": {
        body: {
          environments: [
            { id: "w1", name: "Test", environment: "test", order: 1, role: "admin", here: true },
            { id: "w2", name: "Production", environment: "production", order: 2, role: "editor", here: false },
            { id: "w0", name: "Development", environment: "development", order: 0, role: "admin", here: false },
          ],
        },
      },
      "POST /api/cicd/promote": (body) => ({
        body: { workflows: [{ name: "Order alerts", action: (body as { dry_run: boolean }).dry_run ? "create" : "create", to_version: 1, was_on: false, on: false }], summary: {}, dry_run: (body as { dry_run: boolean }).dry_run },
      }),
    });
    render(<Promote name="Order alerts" dirty={false} />);
    fireEvent.click(screen.getByRole("button", { name: /Promote/ }));
    const to = (await screen.findByLabelText("Promote to")) as HTMLSelectElement;
    expect(Array.from(to.options).map((o) => o.value)).toEqual(["w2"]);
    expect((screen.getByRole("button", { name: /Promote it/ }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Check" }));
    expect(await screen.findByText(/needs nothing the target lacks/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Promote it/ }));
    expect(await screen.findByText("Promoted: Order alerts is new there.")).toBeTruthy();
    expect(sent.filter((s) => s.route === "POST /api/cicd/promote").map((s) => s.body)).toEqual([
      { to: "w2", workflows: ["Order alerts"], dry_run: true },
      { to: "w2", workflows: ["Order alerts"], dry_run: false },
    ]);
  });

  it("says what the target lacks", async () => {
    stand_in({
      "GET /api/cicd/environments": { body: { environments: [ { id: "w1", name: "Test", environment: "test", order: 1, role: "admin", here: true }, { id: "w2", name: "Production", environment: "production", order: 2, role: "admin", here: false } ] } },
      "POST /api/cicd/promote": { status: 422, body: { error: "the workspace lacks what the workflows name (Order alerts needs credential shop): nothing was changed" } },
    });
    render(<Promote name="Order alerts" dirty={false} />);
    fireEvent.click(screen.getByRole("button", { name: /Promote/ }));
    fireEvent.click(await screen.findByRole("button", { name: "Check" }));
    expect(await screen.findByText(/needs credential shop/)).toBeTruthy();
  });

  it("names where each version came from", () => {
    render(
      <ul>
        <li>
          <SourceOf source={{ source: "promotion", detail: "from Test v4", token: "Prod deploy" }} />
        </li>
        <li>
          <SourceOf />
        </li>
      </ul>,
    );
    expect(screen.getByText("Promotion from Test v4")).toBeTruthy();
    expect(screen.getByText("(token Prod deploy)")).toBeTruthy();
    expect(screen.getByText("Studio")).toBeTruthy();
  });
});
