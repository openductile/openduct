// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The Apps page ("Add an app"), against a stand-in API: the workspace's own apps listed; one made from a
// description's address, its proposed actions and sign-ins shown and chosen, its quality shown, and saved with what
// was chosen; changed and removed; a viewer only looks.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Apps, idOf } from "./Apps";

type Answer = { status?: number; body: unknown };
type Routes = Record<string, Answer | ((body: unknown) => Answer)>;

function stand_in(routes: Routes) {
  const sent: { route: string; body: unknown }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    const route = `${init.method ?? "GET"} ${path}`;
    const body = init.body ? JSON.parse(String(init.body)) : undefined;
    sent.push({ route, body });
    const found = routes[route];
    const answer = typeof found === "function" ? found(body) : found ?? { status: 404, body: { error: "no such request" } };
    return new Response(JSON.stringify(answer.body), { status: answer.status ?? 200 });
  });
  return sent;
}

const quality = {
  score: 92,
  bar: 80,
  meets: true,
  actions: 2,
  described: 2,
  with_output: 2,
  recorded: 2,
  refusal: true,
  sign_ins: [{ id: "acme_tasks", kind: "a bearer token" }],
  hosts: ["api.tasks.example"],
  missing: ["no output for delete_task"],
};
const preview = {
  saved: false,
  problems: [],
  quality,
  made: {
    id: "acme_tasks",
    name: "Tasks",
    app_yaml: "app: acme_tasks\n",
    readme: "# Tasks",
    tests: [],
    not_done: ["1 operation left out: a file upload (multipart)"],
    sign_ins: [
      { id: "acme_tasks", label: "An access token", kind: "a bearer token", chosen: true, allowed: true },
      { id: "acme_tasks_oauth", label: "Sign in with Tasks", kind: "a sign-in at the vendor (OAuth 2.0)", chosen: false, allowed: false },
    ],
    proposed: [
      { name: "list_tasks", label: "List tasks", method: "GET", path: "/tasks", score: 90, chosen: true, left_out: null },
      { name: "get_task", label: "Look up a task", method: "GET", path: "/tasks/{id}", score: 70, chosen: true, left_out: null },
      { name: "create_upload", label: "Upload a file", method: "POST", path: "/uploads", score: 50, chosen: false, left_out: "a file upload (multipart)" },
    ],
  },
};
const saved = { id: "acme_tasks", name: "Tasks", actions: 1, score: 88, source: "https://tasks.example/openapi.yaml", updated: 1 };

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("apps", () => {
  it("makes an app from a description's address, with the actions and sign-ins chosen, and saves it", async () => {
    let listed: unknown[] = [];
    const sent = stand_in({
      "GET /api/apps/custom": () => ({ body: { apps: listed, most: 200 } }),
      "POST /api/apps/custom": (body) => {
        const b = body as { save: boolean };
        if (!b.save) return { body: preview };
        listed = [saved];
        return { status: 201, body: { saved: true, app: { ...saved, definition: "", readme: "", tests: [], created: 1, quality } } };
      },
    });
    render(<Apps canEdit />);
    expect(await screen.findByText(/No apps of this workspace's own yet/)).toBeTruthy();
    fireEvent.click(screen.getByText("Add an app"));
    fireEvent.change(screen.getByPlaceholderText("https://api.example.com/openapi.json"), { target: { value: "https://tasks.example/openapi.yaml" } });
    fireEvent.change(screen.getByPlaceholderText("my_crm"), { target: { value: "Acme Tasks" } });
    fireEvent.click(screen.getByText("Look"));
    // The proposal: the actions, the best ticked; one not offered, and why; the sign-ins; the quality.
    expect(await screen.findByText("List tasks")).toBeTruthy();
    expect(screen.getByText(/not offered: a file upload/)).toBeTruthy();
    expect(screen.getByText("92/100")).toBeTruthy();
    expect(screen.getByText(/meets the bar of 80/)).toBeTruthy();
    expect(screen.getByText(/needs the installation's own registration/)).toBeTruthy();
    const look = sent.find((s) => s.route === "POST /api/apps/custom")!;
    expect(look.body).toMatchObject({ from: "openapi", url: "https://tasks.example/openapi.yaml", name: "acme_tasks", save: false });
    // Untick an action; the one at the vendor cannot be ticked.
    const boxes = screen.getAllByRole("checkbox") as HTMLInputElement[];
    const oauth = boxes[1]!;
    expect(oauth.disabled).toBe(true);
    fireEvent.click(boxes[3]!); // get_task
    fireEvent.click(screen.getByText("Save the app"));
    await waitFor(() => expect(sent.some((s) => s.route === "POST /api/apps/custom" && (s.body as { save: boolean }).save)).toBe(true));
    const save = sent.filter((s) => s.route === "POST /api/apps/custom").pop()!;
    expect(save.body).toMatchObject({ save: true, chosen: ["list_tasks"], sign_ins: ["acme_tasks"], name: "acme_tasks" });
    expect(await screen.findByText("Tasks")).toBeTruthy();
    expect(screen.getByText("88")).toBeTruthy();
  });

  it("changes and removes an app; a viewer only looks", async () => {
    const app = { ...saved, definition: "app: acme_tasks\nname: Tasks\n", readme: "", tests: [], created: 1, quality };
    const sent = stand_in({
      "GET /api/apps/custom": { body: { apps: [saved], most: 200 } },
      "GET /api/apps/custom/acme_tasks": { body: app },
      "PUT /api/apps/custom/acme_tasks": (body) => ({ body: { app: { ...app, definition: (body as { definition: string }).definition } } }),
      "DELETE /api/apps/custom/acme_tasks": { body: { removed: "acme_tasks" } },
    });
    vi.stubGlobal("confirm", () => true);
    render(<Apps canEdit />);
    expect(await screen.findByText("Tasks")).toBeTruthy();
    fireEvent.click(screen.getByText("Change"));
    const text = (await screen.findByDisplayValue(/name: Tasks/)) as HTMLTextAreaElement;
    fireEvent.change(text, { target: { value: "app: acme_tasks\nname: Acme Tasks\n" } });
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(sent.some((s) => s.route === "PUT /api/apps/custom/acme_tasks")).toBe(true));
    expect(sent.find((s) => s.route === "PUT /api/apps/custom/acme_tasks")!.body).toEqual({ definition: "app: acme_tasks\nname: Acme Tasks\n" });
    fireEvent.click(screen.getByLabelText("Remove Tasks"));
    await waitFor(() => expect(sent.some((s) => s.route === "DELETE /api/apps/custom/acme_tasks")).toBe(true));
    cleanup();
    render(<Apps canEdit={false} />);
    expect(await screen.findByText("Tasks")).toBeTruthy();
    expect(screen.queryByText("Add an app")).toBeNull();
    expect(screen.queryByText("Change")).toBeNull();
    expect(screen.queryByLabelText("Remove Tasks")).toBeNull();
  });

  it("names an app as steps name it", () => {
    expect(idOf("Acme Tasks")).toBe("acme_tasks");
    expect(idOf("2fa-tools")).toBe("app_2fa_tools");
    expect(idOf("  ")).toBe("");
  });
});
