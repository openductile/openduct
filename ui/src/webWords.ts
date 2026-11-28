// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The web step and the feed trigger in a few words, for the canvas. Kept apart from the forms (StepsWeb.tsx) so the
// canvas's look needs nothing more.

type Fields = Record<string, unknown>;

function short(text: unknown, max = 34): string {
  const s = String(text ?? "");
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

/** An address without its scheme, for a line: `example.com/blog`. */
function address(url: unknown): string {
  return short(String(url ?? "").replace(/^https?:\/\//, ""), 30) || "?";
}

/** A web step's line: `fetch example.com/page`, `crawl example.com (depth 2)`. */
export function webSummary(f: Fields): string {
  const action = String(f.action ?? "fetch");
  const from = f.from !== undefined && f.url === undefined ? `the page in ${short(f.from, 24)}` : address(f.url);
  switch (action) {
    case "select": {
      const names = Object.keys((f.fields ?? {}) as Fields);
      return `pick ${names.length ? short(names.join(", "), 22) : "fields"} from ${from}`;
    }
    case "crawl":
      return `crawl ${from} (depth ${String(f.depth ?? 1)}, ${String(f.max_pages ?? 10)} pages)`;
    case "sitemap":
      return `the sitemap of ${from}`;
    case "feed":
      return `the feed ${from}`;
    default:
      return `fetch ${from}`;
  }
}

/** A feed trigger's line: what starts a run. */
export function feedTriggerLine(s: Fields): string {
  return `a new item in the feed ${address(s.url)}`;
}
