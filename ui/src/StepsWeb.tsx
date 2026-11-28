// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The web step in the builder - fetch a page, pick fields out of it, crawl a site, read a sitemap or a feed - and the
// feed trigger's settings. Each form writes only the settings its action takes, as the engine reads them.

import { Expr, Text } from "./Steps";

type Fields = Record<string, unknown>;
type Put = (name: string, value: unknown) => void;

/** A new web step. */
export function freshWeb(name: string): Record<string, unknown> {
  return { name, web: { action: "fetch", url: "{{ value.url }}" } };
}

const ACTIONS: [string, string][] = [
  ["fetch", "Read a page"],
  ["select", "Pick fields out of a page"],
  ["crawl", "Crawl a site"],
  ["sitemap", "A site's sitemap"],
  ["feed", "A feed's items (RSS, Atom)"],
];

/** The settings each action takes, beside its address and the limits every action has. */
const OWN: Record<string, string[]> = {
  fetch: ["format", "content", "within", "links", "images", "max_chars", "html", "from", "headers", "fail_on_status"],
  select: ["fields", "html", "from", "headers", "fail_on_status"],
  crawl: ["format", "content", "within", "links", "images", "max_chars", "html", "depth", "max_pages", "include", "exclude", "sitemap", "robots", "rate", "time"],
  sitemap: ["max_urls", "headers"],
  feed: ["max_items", "from", "headers"],
};
const COMMON = ["action", "url", "user_agent", "timeout", "max_bytes", "max_redirects"];

/** A body with only what its action takes. */
export function webFor(action: string, fields: Fields): Fields {
  const allowed = new Set([...COMMON, ...(OWN[action] ?? [])]);
  const out: Fields = {};
  for (const [k, v] of Object.entries(fields)) if (allowed.has(k)) out[k] = v;
  out.action = action;
  if (action === "select" && !out.fields) out.fields = { title: "h1" };
  if (out.html === "file" && action === "crawl") delete out.html;
  if (out.url === undefined && out.from === undefined) out.url = "";
  return out;
}

function Check(props: { label: string; on: boolean; set: (v: boolean) => void }) {
  return (
    <label className="check">
      <input type="checkbox" checked={props.on} onChange={(e) => props.set(e.target.checked)} />
      {props.label}
    </label>
  );
}

/** How a page is read: Markdown or text, its main part or all of it, its links and pictures. */
function Reading(props: { fields: Fields; set: Put; crawl: boolean }) {
  const { fields, set } = props;
  const part = fields.within !== undefined ? "within" : String(fields.content ?? "main");
  const choosePart = (p: string) => {
    if (p === "within") {
      set("content", undefined);
      set("within", "article");
      return;
    }
    props.set("within", undefined);
    set("content", p === "main" ? undefined : p);
  };
  return (
    <>
      <div className="row">
        <label className="narrow">
          Text as
          <select value={String(fields.format ?? "markdown")} onChange={(e) => set("format", e.target.value === "markdown" ? undefined : e.target.value)}>
            <option value="markdown">Markdown</option>
            <option value="text">plain text</option>
          </select>
        </label>
        <label className="narrow">
          Of
          <select value={part} onChange={(e) => choosePart(e.target.value)}>
            <option value="main">the main part (no menus, footers ...)</option>
            <option value="all">all the page shows</option>
            <option value="within">what a selector picks</option>
          </select>
        </label>
        {part === "within" && <Text label="Selector" value={String(fields.within ?? "")} onChange={(v) => set("within", v)} placeholder="article, #content" />}
      </div>
      <div className="row">
        <Check label="Its links" on={fields.links === undefined ? !props.crawl : fields.links === true} set={(v) => set("links", v === !props.crawl ? undefined : v)} />
        <Check label="Its pictures" on={fields.images === undefined ? !props.crawl : fields.images === true} set={(v) => set("images", v === !props.crawl ? undefined : v)} />
        <label className="narrow">
          At most (characters)
          <input type="number" min={100} max={1000000} value={fields.max_chars === undefined ? "" : Number(fields.max_chars)} placeholder="all" onChange={(e) => set("max_chars", e.target.value ? Number(e.target.value) : undefined)} />
        </label>
        <label className="narrow">
          Its HTML
          <select value={String(fields.html ?? "no")} onChange={(e) => set("html", e.target.value === "no" ? undefined : e.target.value === "text" ? true : "file")}>
            <option value="no">not given</option>
            <option value="true">as text</option>
            {!props.crawl && <option value="file">as a file</option>}
          </select>
        </label>
      </div>
    </>
  );
}

type FieldSpec = { selector: string; attribute?: string; all?: boolean };

function specOf(v: unknown): FieldSpec {
  if (typeof v === "string") return { selector: v };
  const o = (v ?? {}) as Record<string, unknown>;
  return { selector: String(o.selector ?? ""), attribute: o.attribute as string | undefined, all: o.all === true };
}

/** A field as the engine reads it: a selector alone when it is only that. */
function written(s: FieldSpec, kept: unknown): unknown {
  const extra = typeof kept === "object" && kept ? (kept as Record<string, unknown>) : {};
  if (!s.attribute && !s.all && !extra.fields && !extra.as) return s.selector;
  const out: Record<string, unknown> = { ...extra, selector: s.selector };
  if (s.attribute) out.attribute = s.attribute;
  else delete out.attribute;
  if (s.all) out.all = true;
  else delete out.all;
  return out;
}

/** The fields a select step picks out: each a name, a CSS selector, and what of each element it gives. */
function FieldsEditor(props: { value: Fields; onChange: (v: Fields) => void }) {
  const entries = Object.entries(props.value);
  const put = (i: number, name: string, spec: FieldSpec) => {
    const next: Fields = {};
    entries.forEach(([k, v], j) => {
      if (j === i) next[name] = written(spec, v);
      else next[k] = v;
    });
    props.onChange(next);
  };
  const remove = (i: number) => props.onChange(Object.fromEntries(entries.filter((_, j) => j !== i)));
  const add = () => {
    let n = entries.length + 1;
    while (`field_${n}` in props.value) n += 1;
    props.onChange({ ...props.value, [`field_${n}`]: "" });
  };
  return (
    <div className="pairs">
      {entries.map(([name, v], i) => {
        const s = specOf(v);
        const inner = typeof v === "object" && v && "fields" in (v as object);
        return (
          <div className="row" key={i}>
            <label className="narrow">
              Field
              <input className="code" value={name} onChange={(e) => put(i, e.target.value.replace(/[^A-Za-z0-9_]/g, "_"), s)} />
            </label>
            <label>
              Selector
              <input className="code" value={s.selector} placeholder=".price, h1, a.item" onChange={(e) => put(i, name, { ...s, selector: e.target.value })} />
            </label>
            <label className="narrow">
              Gives
              <input className="code" value={s.attribute ?? ""} placeholder="its text (or an attribute: href)" onChange={(e) => put(i, name, { ...s, attribute: e.target.value || undefined })} />
            </label>
            <Check label="every one" on={s.all === true} set={(all) => put(i, name, { ...s, all })} />
            {inner && <span className="hint">and its own fields (in the workflow's file)</span>}
            <button type="button" className="ghost" onClick={() => remove(i)} aria-label={`Remove ${name}`}>
              ×
            </button>
          </div>
        );
      })}
      <button type="button" className="ghost" onClick={add}>
        + a field
      </button>
    </div>
  );
}

function CrawlSettings(props: { fields: Fields; set: Put }) {
  const { fields, set } = props;
  const list = (v: unknown) => (Array.isArray(v) ? v.join(", ") : String(v ?? ""));
  const toList = (s: string) => {
    const items = s.split(",").map((p) => p.trim()).filter(Boolean);
    return items.length ? items : undefined;
  };
  return (
    <>
      <div className="row">
        <label className="narrow">
          Links deep
          <input type="number" min={0} max={5} value={Number(fields.depth ?? 1)} onChange={(e) => set("depth", Number(e.target.value) === 1 ? undefined : Number(e.target.value))} />
        </label>
        <label className="narrow">
          Pages at most
          <input type="number" min={1} max={200} value={Number(fields.max_pages ?? 10)} onChange={(e) => set("max_pages", Number(e.target.value) === 10 ? undefined : Number(e.target.value))} />
        </label>
        <Text label="A page each" value={String(fields.rate ?? "")} onChange={(v) => set("rate", v || undefined)} placeholder="1s (at least 100ms)" />
        <Text label="For at most" value={String(fields.time ?? "")} onChange={(v) => set("time", v || undefined)} placeholder="2m (at most 10m)" />
      </div>
      <Text label="Only addresses like" value={list(fields.include)} onChange={(v) => set("include", toList(v))} placeholder="the same site (or https://example.com/docs/*, ...)" />
      <Text label="Never addresses like" value={list(fields.exclude)} onChange={(v) => set("exclude", toList(v))} placeholder="*/login*, */cart*" />
      <div className="row">
        <Check label="Start from the site's sitemap too" on={fields.sitemap === true} set={(v) => set("sitemap", v || undefined)} />
        <Check label="Honour the site's robots.txt" on={fields.robots !== false} set={(v) => set("robots", v ? undefined : false)} />
      </div>
      {fields.robots === false && (
        <p className="hint">
          robots.txt is how a site says what crawlers may read. Crawl past it only a site that is yours, or whose owner agreed.
        </p>
      )}
    </>
  );
}

export function WebBody(props: { fields: Fields; set: Put; setBody: (v: unknown) => void }) {
  const { fields, set } = props;
  const action = String(fields.action ?? "fetch");
  const given = fields.from !== undefined;
  const takesFrom = (OWN[action] ?? []).includes("from");
  return (
    <>
      <label>
        What it does
        <select value={action} onChange={(e) => props.setBody(webFor(e.target.value, fields))}>
          {ACTIONS.map(([a, label]) => (
            <option key={a} value={a}>
              {label}
            </option>
          ))}
        </select>
      </label>
      {takesFrom && (
        <label className="narrow">
          The page
          <select
            value={given ? "run" : "web"}
            onChange={(e) => {
              const body = { ...fields };
              if (e.target.value === "run") body.from = "steps.download.body";
              else delete body.from;
              props.setBody(body);
            }}
          >
            <option value="web">fetched from its address</option>
            <option value="run">one the run already has</option>
          </select>
        </label>
      )}
      {given && <Expr label="Its HTML (text or a file)" value={String(fields.from ?? "")} onChange={(v) => set("from", v)} />}
      <Text
        label={given ? "Its address (for its links; optional)" : action === "sitemap" ? "The site, or its sitemap" : action === "crawl" ? "Start at" : "Address"}
        value={String(fields.url ?? "")}
        onChange={(v) => set("url", v || (given ? undefined : ""))}
        placeholder="https://example.com/page - with {{ }} for values"
      />
      {(action === "fetch" || action === "crawl") && <Reading fields={fields} set={set} crawl={action === "crawl"} />}
      {action === "select" && <FieldsEditor value={(fields.fields ?? {}) as Fields} onChange={(v) => set("fields", v)} />}
      {action === "crawl" && <CrawlSettings fields={fields} set={set} />}
      {action === "sitemap" && (
        <label className="narrow">
          Addresses at most
          <input type="number" min={1} max={50000} value={Number(fields.max_urls ?? 1000)} onChange={(e) => set("max_urls", Number(e.target.value) === 1000 ? undefined : Number(e.target.value))} />
        </label>
      )}
      {action === "feed" && (
        <label className="narrow">
          Items at most
          <input type="number" min={1} max={1000} value={Number(fields.max_items ?? 50)} onChange={(e) => set("max_items", Number(e.target.value) === 50 ? undefined : Number(e.target.value))} />
        </label>
      )}
      <details>
        <summary>Limits and how it asks</summary>
        <div className="row">
          <Text label="Each page within" value={String(fields.timeout ?? "")} onChange={(v) => set("timeout", v || undefined)} placeholder="30s (at most 2m)" />
          <Text label="Bytes a page" value={String(fields.max_bytes ?? "")} onChange={(v) => set("max_bytes", v || undefined)} placeholder="5MB (at most 10MB)" />
          <label className="narrow">
            Redirects
            <input type="number" min={0} max={10} value={Number(fields.max_redirects ?? 5)} onChange={(e) => set("max_redirects", Number(e.target.value) === 5 ? undefined : Number(e.target.value))} />
          </label>
        </div>
        <Text label="User-Agent" value={String(fields.user_agent ?? "")} onChange={(v) => set("user_agent", v || undefined)} placeholder="OpenDuct's own (it names OpenDuct)" />
        {(action === "fetch" || action === "select") && (
          <Check label="A page that answers 400 or above fails the step" on={fields.fail_on_status !== false} set={(v) => set("fail_on_status", v ? undefined : false)} />
        )}
      </details>
      <p className="hint">
        {action === "crawl"
          ? "Each page as Read a page gives it, nearest first, one at a time on a site. A test run reads a sample page; a preview fetches for real."
          : action === "select"
            ? "Its result's fields: each the first element's text (or every one's), or the attribute named - addresses in full."
            : "A test run fetches nothing and reads a sample page in its place; a preview fetches for real. Only the web's addresses, never this machine's network."}
      </p>
    </>
  );
}

/** A feed trigger's settings: the feed, and how often it is asked. */
export function FeedTrigger(props: { settings: Fields; onChange: (s: Fields) => void }) {
  const s = props.settings;
  const set = (k: string, v: unknown) => props.onChange({ ...s, [k]: v });
  return (
    <div className="tg-section">
      <Text label="Feed" value={String(s.url ?? "")} onChange={(v) => set("url", v)} placeholder="https://example.com/feed.xml (RSS, Atom or JSON Feed)" />
      <div className="row">
        <Text label="Look every" value={String(s.every ?? "")} onChange={(v) => set("every", v || undefined)} placeholder="15m (1m to 7d)" />
        <label className="narrow">
          New items a look, at most
          <input type="number" min={1} max={500} value={Number(s.max_items ?? 50)} onChange={(e) => set("max_items", Number(e.target.value) === 50 ? undefined : Number(e.target.value))} />
        </label>
      </div>
      <p className="tg-note">
        A run for each new item, oldest first: its title, link, date, summary, content and author. The first look notes what the feed holds; a cluster
        looks once each time, and a restart neither runs an item twice nor passes one by.
      </p>
    </div>
  );
}
