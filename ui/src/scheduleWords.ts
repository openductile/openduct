// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A schedule trigger's settings as the studio's page works with them: which form they are (every so often, every
// hour, day, weekday, week or month at set times, or a cron expression), the settings a form starts with, and the
// few words the canvas shows for it. The engine reads them (and says what is wrong); this only shapes them.

export type ScheduleForm = "interval" | "hour" | "day" | "weekday" | "week" | "month" | "cron";
export type ScheduleSettings = Record<string, unknown>;

export const FORMS: { form: ScheduleForm; label: string }[] = [
  { form: "interval", label: "Every so often" },
  { form: "hour", label: "Every hour" },
  { form: "day", label: "Every day" },
  { form: "weekday", label: "Weekdays" },
  { form: "week", label: "Weekly" },
  { form: "month", label: "Monthly" },
  { form: "cron", label: "Cron" },
];

/** Monday first, as a week is read; the engine takes these names. */
export const WEEKDAYS: { day: string; short: string }[] = [
  { day: "monday", short: "Mon" },
  { day: "tuesday", short: "Tue" },
  { day: "wednesday", short: "Wed" },
  { day: "thursday", short: "Thu" },
  { day: "friday", short: "Fri" },
  { day: "saturday", short: "Sat" },
  { day: "sunday", short: "Sun" },
];

const WORDS: Record<string, ScheduleForm> = {
  hour: "hour",
  hourly: "hour",
  day: "day",
  daily: "day",
  weekday: "weekday",
  weekdays: "weekday",
  week: "week",
  weekly: "week",
  month: "month",
  monthly: "month",
};

/** Which form the settings are. */
export function formOf(s: ScheduleSettings): ScheduleForm {
  if (s.cron !== undefined) return "cron";
  return WORDS[String(s.every ?? "").trim().toLowerCase()] ?? "interval";
}

/** A list setting (`at`, `on`, `cron`) as a list, however it was written. */
export function listOf(value: unknown): unknown[] {
  if (Array.isArray(value)) return value;
  return value === undefined || value === null || value === "" ? [] : [value];
}

/** The settings a form starts with, keeping the time zone (an interval has none: it runs on the clock). */
export function startForm(form: ScheduleForm, from: ScheduleSettings): ScheduleSettings {
  const zone = typeof from.timezone === "string" && from.timezone ? { timezone: from.timezone } : {};
  const at = listOf(from.at).filter((t) => typeof t === "string" && t.includes(":") && !t.startsWith(":"));
  const times = at.length > 0 ? at : ["09:00"];
  switch (form) {
    case "interval":
      return { every: "1h" };
    case "hour":
      return { every: "hour", at: ":00", ...zone };
    case "day":
    case "weekday":
      return { every: form, at: times.length === 1 ? times[0] : times, ...zone };
    case "week":
      return { every: "week", on: ["monday"], at: times.length === 1 ? times[0] : times, ...zone };
    case "month":
      return { every: "month", on: [1], at: times.length === 1 ? times[0] : times, ...zone };
    case "cron":
      return { cron: "0 9 * * 1-5", ...zone };
  }
}

/** A list of one written as the one. */
export function oneOrList<T>(items: T[]): T | T[] {
  return items.length === 1 ? items[0]! : items;
}

/** What starts the workflow, in a few words: `weekdays at 09:00 (Europe/Berlin)`. */
export function scheduleLine(s: ScheduleSettings): string {
  const at = listOf(s.at).map(String).join(", ");
  const zone = typeof s.timezone === "string" && s.timezone ? ` (${s.timezone})` : "";
  switch (formOf(s)) {
    case "interval":
      return `every ${String(s.every ?? "?")}`;
    case "cron":
      return `cron ${listOf(s.cron).map(String).join(" and ")}${zone}`;
    case "hour":
      return `every hour at ${at || ":00"}${zone}`;
    case "day":
      return `every day at ${at || "?"}${zone}`;
    case "weekday":
      return `weekdays at ${at || "?"}${zone}`;
    case "week": {
      const days = listOf(s.on).map((d) => WEEKDAYS.find((w) => w.day.startsWith(String(d).toLowerCase().slice(0, 3)))?.short ?? String(d));
      return `${days.join(", ") || "?"} at ${at || "?"}${zone}`;
    }
    case "month":
      return `monthly on ${listOf(s.on).map(String).join(", ") || "?"} at ${at || "?"}${zone}`;
  }
}
