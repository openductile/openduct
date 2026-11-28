// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A schedule's settings in the trigger's window, against a stand-in API: each form and what a person's clicks make
// of the settings, the next runs the studio works out, and its words when something is wrong.
// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { Schedule, shownTime } from "./Schedule";
import { formOf, scheduleLine, startForm, type ScheduleSettings } from "./scheduleWords";
import { triggerSummary } from "./flow/look";

type Answer = { status?: number; body: unknown };

/** A fetch that answers `POST /api/schedule` with `answer`, and keeps what was asked. */
function standIn(answer: (asked: { schedule: ScheduleSettings; count: number }) => Answer) {
  const asked: { schedule: ScheduleSettings; count: number }[] = [];
  vi.stubGlobal("fetch", async (path: string, init: RequestInit = {}) => {
    if (path !== "/api/schedule" || init.method !== "POST") return new Response(JSON.stringify({ error: "no such request" }), { status: 404 });
    const body = JSON.parse(String(init.body)) as { schedule: ScheduleSettings; count: number };
    asked.push(body);
    const a = answer(body);
    return new Response(JSON.stringify(a.body), { status: a.status ?? 200 });
  });
  return asked;
}

/** The schedule's settings as a person changes them, each change kept. */
function Harness(props: { start: ScheduleSettings; seen: ScheduleSettings[] }) {
  const [settings, setSettings] = useState(props.start);
  return (
    <Schedule
      settings={settings}
      onChange={(s) => {
        props.seen.push(s);
        setSettings(s);
      }}
    />
  );
}

const weekdays = {
  says: "every weekday at 08:30 (Europe/Berlin)",
  timezone: "Europe/Berlin",
  next: ["2026-11-30T07:30:00Z", "2026-12-01T07:30:00Z"],
  local: ["2026-11-30T08:30:00+01:00", "2026-12-01T08:30:00+01:00"],
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("a schedule", () => {
  it("runs on weekdays at a time in a time zone, and shows its next runs on that clock", async () => {
    const asked = standIn(() => ({ body: weekdays }));
    const seen: ScheduleSettings[] = [];
    render(<Harness start={{ every: "1h" }} seen={seen} />);
    const how = screen.getByRole("radiogroup", { name: "How often" });
    expect(within(how).getByRole("radio", { name: "Every so often" }).getAttribute("aria-checked")).toBe("true");
    fireEvent.click(within(how).getByRole("radio", { name: "Weekdays" }));
    expect(seen.at(-1)).toEqual({ every: "weekday", at: "09:00" });
    fireEvent.change(screen.getByLabelText("Time"), { target: { value: "08:30" } });
    fireEvent.change(screen.getByLabelText("Time zone"), { target: { value: "Europe/Berlin" } });
    expect(seen.at(-1)).toEqual({ every: "weekday", at: "08:30", timezone: "Europe/Berlin" });
    expect(await screen.findByText("Mon 30 Nov 2026, 08:30")).toBeTruthy();
    expect(screen.getByText("Tue 1 Dec 2026, 08:30")).toBeTruthy();
    expect(screen.getByText(/Runs every weekday at 08:30 \(Europe\/Berlin\)/)).toBeTruthy();
    // The studio is asked about the settings as they are, once they stop changing.
    await waitFor(() => expect(asked.at(-1)).toEqual({ schedule: { every: "weekday", at: "08:30", timezone: "Europe/Berlin" }, count: 5 }));
  });

  it("chooses days of the week, dates of the month and several times", () => {
    standIn(() => ({ body: weekdays }));
    const seen: ScheduleSettings[] = [];
    render(<Harness start={{ every: "week", on: ["monday"], at: "09:00", timezone: "America/New_York" }} seen={seen} />);
    const days = screen.getByRole("group", { name: "Days" });
    expect(within(days).getByRole("checkbox", { name: "monday" }).getAttribute("aria-checked")).toBe("true");
    fireEvent.click(within(days).getByRole("checkbox", { name: "thursday" }));
    expect(seen.at(-1)?.on).toEqual(["monday", "thursday"]);
    fireEvent.click(within(days).getByRole("checkbox", { name: "monday" }));
    expect(seen.at(-1)?.on).toEqual(["thursday"]);
    fireEvent.click(screen.getByText("+ Add a time"));
    expect(seen.at(-1)?.at).toEqual(["09:00", "17:00"]);
    fireEvent.click(screen.getByLabelText("Remove time 1"));
    expect(seen.at(-1)?.at).toBe("17:00");
    // Monthly keeps the time and the time zone, and starts on the 1st.
    fireEvent.click(screen.getByRole("radio", { name: "Monthly" }));
    expect(seen.at(-1)).toEqual({ every: "month", on: [1], at: "17:00", timezone: "America/New_York" });
    const dates = screen.getByRole("group", { name: "Dates" });
    fireEvent.click(within(dates).getByRole("checkbox", { name: "the 15" }));
    fireEvent.click(within(dates).getByRole("checkbox", { name: "the last day" }));
    expect(seen.at(-1)?.on).toEqual([1, 15, "last"]);
  });

  it("runs every hour at a minute past it", () => {
    standIn(() => ({ body: weekdays }));
    const seen: ScheduleSettings[] = [];
    render(<Harness start={{ every: "15m" }} seen={seen} />);
    fireEvent.click(screen.getByRole("radio", { name: "Every hour" }));
    expect(seen.at(-1)).toEqual({ every: "hour", at: ":00" });
    fireEvent.change(screen.getByLabelText("Minutes past the hour"), { target: { value: "5" } });
    expect(seen.at(-1)).toEqual({ every: "hour", at: ":05" });
  });

  it("takes a cron expression, and says what is wrong with one in the studio's words", async () => {
    const wrong = 'schedule.cron: "0 9 * *" has 4 fields: a cron expression has 5 (minute hour day month weekday, as 0 9 * * 1-5) or 6 (with the second first)';
    standIn((asked) => (String(asked.schedule.cron).split(" ").length === 4 ? { status: 422, body: { error: wrong } } : { body: weekdays }));
    const seen: ScheduleSettings[] = [];
    render(<Harness start={{ every: "day", at: "09:00" }} seen={seen} />);
    fireEvent.click(screen.getByRole("radio", { name: "Cron" }));
    expect(seen.at(-1)).toEqual({ cron: "0 9 * * 1-5" });
    fireEvent.change(screen.getByLabelText("Cron expression"), { target: { value: "0 9 * *" } });
    expect(await screen.findByText(wrong)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Cron expression"), { target: { value: "0 9 * * 1-5" } });
    expect(await screen.findByText("Mon 30 Nov 2026, 08:30")).toBeTruthy();
    expect(screen.queryByText(wrong)).toBeNull();
  });

  it("is said in a few words on the canvas", () => {
    expect(formOf({ every: "15m" })).toBe("interval");
    expect(formOf({ every: "Weekly" })).toBe("week");
    expect(formOf({ cron: "* * * * *" })).toBe("cron");
    expect(startForm("interval", { every: "day", at: "09:00", timezone: "Asia/Tokyo" })).toEqual({ every: "1h" });
    expect(startForm("day", { every: "week", on: ["monday"], at: ["07:00", "19:00"], timezone: "Asia/Tokyo" })).toEqual({
      every: "day",
      at: ["07:00", "19:00"],
      timezone: "Asia/Tokyo",
    });
    expect(scheduleLine({ every: "15m" })).toBe("every 15m");
    expect(scheduleLine({ every: "week", on: ["monday", "thu"], at: "08:00" })).toBe("Mon, Thu at 08:00");
    expect(scheduleLine({ every: "month", on: [1, "last"], at: "06:00", timezone: "UTC" })).toBe("monthly on 1, last at 06:00 (UTC)");
    expect(scheduleLine({ cron: "0 9 * * 1-5", timezone: "Europe/Paris" })).toBe("cron 0 9 * * 1-5 (Europe/Paris)");
    expect(triggerSummary({ schedule: { every: "weekday", at: "09:00", timezone: "Europe/Berlin" } }).line).toBe("weekdays at 09:00 (Europe/Berlin)");
    expect(triggerSummary({ schedule: { every: "1h" } }).line).toBe("every 1h");
    expect(shownTime("2026-11-30T09:00:00+01:00")).toBe("Mon 30 Nov 2026, 09:00");
    expect(shownTime("2026-11-30T09:00:30Z")).toBe("Mon 30 Nov 2026, 09:00:30");
  });
});
