// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A schedule's settings, in the trigger's window: how often (every so often, every hour, every day, weekdays, weekly,
// monthly, or a cron expression), at what times, on which days or dates, in which time zone - and the next times it
// runs, as the studio works them out, or what is wrong, in its words.

import { useEffect, useState } from "react";
import { api, type ScheduleTimes } from "./api";
import { FORMS, WEEKDAYS, formOf, listOf, oneOrList, startForm, type ScheduleSettings } from "./scheduleWords";

const EVERY = ["1m", "5m", "15m", "1h", "6h", "1d"];
/** As many times (or cron expressions) as one schedule takes. */
const MOST = 24;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/** The time zones this browser knows, for the time zone's suggestions. */
function zones(): string[] {
  const known = (Intl as { supportedValuesOf?: (kind: string) => string[] }).supportedValuesOf?.("timeZone") ?? [];
  return ["UTC", ...known.filter((z) => z !== "UTC")];
}

/** This browser's own time zone, when it says. */
function ownZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone ?? "";
  } catch {
    return "";
  }
}

/** A time on the schedule's own clock ("2026-11-30T09:00:00+01:00"), for reading: "Mon 30 Nov 2026, 09:00". */
export function shownTime(local: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})/.exec(local);
  if (!m) return local;
  const [, y, mo, d, h, mi, s] = m.map(String);
  const weekday = DAY_NAMES[new Date(Date.UTC(Number(y), Number(mo) - 1, Number(d))).getUTCDay()];
  return `${weekday} ${Number(d)} ${MONTHS[Number(mo) - 1]} ${y}, ${h}:${mi}${s === "00" ? "" : `:${s}`}`;
}

export function Schedule(props: { settings: ScheduleSettings; onChange: (s: ScheduleSettings) => void }) {
  const s = props.settings;
  const form = formOf(s);
  const patch = (fields: ScheduleSettings) => {
    const next: ScheduleSettings = { ...s, ...fields };
    for (const [k, v] of Object.entries(next)) if (v === undefined) delete next[k];
    props.onChange(next);
  };
  const atTimes = form === "day" || form === "weekday" || form === "week" || form === "month";
  return (
    <div className="tg-section">
      <div className="tg-field">
        <span className="tg-name">How often</span>
        <div className="tg-chips" role="radiogroup" aria-label="How often">
          {FORMS.map((f) => (
            <button
              key={f.form}
              type="button"
              role="radio"
              aria-checked={f.form === form}
              className={f.form === form ? "here" : ""}
              onClick={() => f.form !== form && props.onChange(startForm(f.form, s))}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>
      {form === "interval" && <Interval every={String(s.every ?? "")} onChange={(every) => props.onChange({ every })} />}
      {form === "hour" && <Minute at={s.at} onChange={(at) => patch({ at })} />}
      {form === "week" && <Days on={s.on} onChange={(on) => patch({ on })} />}
      {form === "month" && <Dates on={s.on} onChange={(on) => patch({ on })} />}
      {atTimes && (
        <Texts
          name="At"
          label="Time"
          more="Add a time"
          placeholder="09:30"
          time
          values={listOf(s.at).map(String)}
          onChange={(at) => patch({ at: oneOrList(at) })}
        />
      )}
      {form === "cron" && (
        <>
          <Texts
            name="Cron expression"
            label="Cron expression"
            more="Add an expression"
            placeholder="0 9 * * 1-5"
            values={listOf(s.cron).map(String)}
            onChange={(cron) => patch({ cron: oneOrList(cron) })}
          />
          <p className="tg-hint">Minute, hour, day of the month, month, day of the week: 0 9 * * 1-5 is 09:00, Monday to Friday. Six fields put the second first.</p>
        </>
      )}
      {form !== "interval" && <Zone zone={typeof s.timezone === "string" ? s.timezone : ""} onChange={(z) => patch({ timezone: z || undefined })} />}
      <NextTimes settings={s} />
    </div>
  );
}

function Interval(props: { every: string; onChange: (every: string) => void }) {
  return (
    <>
      <label className="tg-field">
        <span className="tg-name">Every</span>
        <input className="code" value={props.every} placeholder="15m" onChange={(e) => props.onChange(e.target.value)} />
      </label>
      <div className="tg-chips" role="group" aria-label="Usual intervals">
        {EVERY.map((e) => (
          <button key={e} type="button" className={props.every === e ? "here" : ""} onClick={() => props.onChange(e)}>
            {e}
          </button>
        ))}
      </div>
      <p className="tg-hint">Counted from midnight UTC: every 15m runs at :00, :15, :30 and :45. 30s, 15m, 1h and 1d are all written so.</p>
    </>
  );
}

/** Every hour: the minute past it. */
function Minute(props: { at: unknown; onChange: (at: string) => void }) {
  const now = Number(String(listOf(props.at)[0] ?? ":00").replace(":", ""));
  return (
    <label className="tg-field">
      <span className="tg-name">Minutes past the hour</span>
      <input
        type="number"
        min={0}
        max={59}
        value={Number.isFinite(now) ? now : 0}
        onChange={(e) => props.onChange(`:${String(Math.min(59, Math.max(0, Math.trunc(Number(e.target.value) || 0)))).padStart(2, "0")}`)}
      />
    </label>
  );
}

/** Texts of a list - times of day, or cron expressions - each with its own box. */
function Texts(props: { name: string; label: string; more: string; placeholder: string; time?: boolean; values: string[]; onChange: (values: string[]) => void }) {
  const values = props.values.length > 0 ? props.values : [""];
  const put = (i: number, v: string) => props.onChange(values.map((old, j) => (j === i ? v : old)));
  return (
    <div className="tg-field">
      <span className="tg-name">{props.name}</span>
      <div className="tg-times">
        {values.map((v, i) => (
          <span key={i} className="tg-time">
            <input
              className="code"
              type={props.time && /^\d{2}:\d{2}(:\d{2})?$/.test(v) ? "time" : "text"}
              aria-label={values.length > 1 ? `${props.label} ${i + 1}` : props.label}
              value={v}
              placeholder={props.placeholder}
              onChange={(e) => put(i, e.target.value)}
            />
            {values.length > 1 && (
              <button type="button" className="tg-icon" aria-label={`Remove ${props.label.toLowerCase()} ${i + 1}`} title="Remove" onClick={() => props.onChange(values.filter((_, j) => j !== i))}>
                ×
              </button>
            )}
          </span>
        ))}
      </div>
      {values.length < MOST && (
        <div className="tg-chips">
          <button type="button" onClick={() => props.onChange([...values, props.time ? "17:00" : ""])}>
            + {props.more}
          </button>
        </div>
      )}
    </div>
  );
}

/** Weekly: the days it runs on. */
function Days(props: { on: unknown; onChange: (on: string[]) => void }) {
  const chosen = new Set(listOf(props.on).map((d) => String(d).toLowerCase().slice(0, 3)));
  const toggle = (day: string) => {
    const short = day.slice(0, 3);
    props.onChange(WEEKDAYS.map((w) => w.day).filter((d) => (d === day ? !chosen.has(short) : chosen.has(d.slice(0, 3)))));
  };
  return (
    <div className="tg-field">
      <span className="tg-name">On</span>
      <div className="tg-chips" role="group" aria-label="Days">
        {WEEKDAYS.map((w) => {
          const on = chosen.has(w.day.slice(0, 3));
          return (
            <button key={w.day} type="button" role="checkbox" aria-checked={on} aria-label={w.day} className={on ? "here" : ""} onClick={() => toggle(w.day)}>
              {w.short}
            </button>
          );
        })}
      </div>
    </div>
  );
}

/** Monthly: the dates it runs on, and the month's last day. */
function Dates(props: { on: unknown; onChange: (on: (number | string)[]) => void }) {
  const chosen = new Set(listOf(props.on).map((d) => String(d).toLowerCase()));
  const all: (number | string)[] = [...Array.from({ length: 31 }, (_, i) => i + 1), "last"];
  const toggle = (date: number | string) => props.onChange(all.filter((d) => (d === date ? !chosen.has(String(d)) : chosen.has(String(d)))));
  return (
    <div className="tg-field">
      <span className="tg-name">On</span>
      <div className="tg-chips" role="group" aria-label="Dates">
        {all.map((d) => {
          const on = chosen.has(String(d));
          return (
            <button key={d} type="button" role="checkbox" aria-checked={on} aria-label={d === "last" ? "the last day" : `the ${d}`} className={on ? "here" : ""} onClick={() => toggle(d)}>
              {d === "last" ? "Last day" : d}
            </button>
          );
        })}
      </div>
      <p className="tg-hint">A month without the date (the 31st in April) is passed over; Last day is every month's.</p>
    </div>
  );
}

function Zone(props: { zone: string; onChange: (zone: string) => void }) {
  const mine = ownZone();
  return (
    <div className="tg-field">
      <span className="tg-name">
        Time zone <span className="tg-optional">UTC unless it says</span>
      </span>
      <input className="code" list="openduct-zones" aria-label="Time zone" value={props.zone} placeholder="UTC" spellCheck={false} onChange={(e) => props.onChange(e.target.value.trim())} />
      <datalist id="openduct-zones">
        {zones().map((z) => (
          <option key={z} value={z} />
        ))}
      </datalist>
      {mine && mine !== props.zone && (
        <div className="tg-chips">
          <button type="button" onClick={() => props.onChange(mine)}>
            Use mine: {mine}
          </button>
        </div>
      )}
      <p className="tg-hint">When the clocks change, a time skipped runs as the clock jumps, and a time repeated runs the first time.</p>
    </div>
  );
}

/** The next times it runs, as the studio works them out - or what is wrong with the settings. */
function NextTimes(props: { settings: ScheduleSettings }) {
  const key = JSON.stringify(props.settings);
  const [found, setFound] = useState<{ key: string; times?: ScheduleTimes; error?: string } | null>(null);
  useEffect(() => {
    let gone = false;
    const wait = setTimeout(() => {
      api.schedule(JSON.parse(key), 5).then(
        (times) => !gone && setFound({ key, times }),
        (e: unknown) => !gone && setFound({ key, error: e instanceof Error ? e.message : String(e) }),
      );
    }, 300);
    return () => {
      gone = true;
      clearTimeout(wait);
    };
  }, [key]);
  const now = found?.key === key ? found : null;
  return (
    <div className="tg-field" aria-live="polite">
      <span className="tg-name">Next runs</span>
      {!now && <p className="tg-hint">Working out when it runs…</p>}
      {now?.error && <p className="error">{now.error}</p>}
      {now?.times && (
        <>
          <p className="tg-hint">Runs {now.times.says}. In a cluster each time runs once.</p>
          <ol className="tg-next" aria-label="Next runs">
            {now.times.local.map((l, i) => (
              <li key={l} title={now.times?.next[i]}>
                {shownTime(l)}
              </li>
            ))}
          </ol>
        </>
      )}
    </div>
  );
}
