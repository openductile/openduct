// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The open tabs across the top: every page and workflow opened from the side stays open as a tab until closed,
// remembered in this browser for each workspace. Workflows is always the first.

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { Glyph } from "./Sidebar";

export type OpenTab = { path: string; title: string; dirty?: boolean };

const HOME: OpenTab = { path: "/dashboard", title: "Dashboard" };

function glyphOf(path: string): string {
  if (path.startsWith("/workflows/")) return "workflow";
  return path.split("/")[1] || "workflows";
}

function read(key: string): OpenTab[] {
  try {
    const kept = JSON.parse(localStorage.getItem(key) ?? "[]") as OpenTab[];
    return Array.isArray(kept) ? kept.filter((t) => typeof t.path === "string" && typeof t.title === "string") : [];
  } catch {
    return [];
  }
}

/** The open tabs of a workspace: Workflows first, then the others in the order they were opened. */
export function useTabs(workspace: string) {
  const key = `openduct.tabs.${workspace}`;
  const fresh = (k: string): OpenTab[] => [HOME, ...read(k).filter((t) => t.path !== HOME.path)];
  // The tabs together with the workspace they are of: another workspace's are never kept under this one's name.
  const [held, setHeld] = useState<{ key: string; tabs: OpenTab[] }>(() => ({ key, tabs: fresh(key) }));
  if (held.key !== key) setHeld({ key, tabs: fresh(key) });
  const tabs = held.key === key ? held.tabs : fresh(key);
  const setTabs = useCallback((change: (now: OpenTab[]) => OpenTab[]) => setHeld((h) => ({ key: h.key, tabs: change(h.tabs) })), []);
  useEffect(() => {
    try {
      localStorage.setItem(held.key, JSON.stringify(held.tabs.filter((t) => t.path !== HOME.path).map(({ path, title }) => ({ path, title }))));
    } catch {
      // A browser that keeps nothing: the tabs last this visit.
    }
  }, [held]);
  /** Opens `path` as a tab, if it is not one already. */
  const open = useCallback((path: string, title: string) => {
    setTabs((now) => {
      if (now.some((t) => t.path === path)) return now;
      // Tabs only grow: one closes when its person closes it, never by itself. The arrows page through them.
      return [...now, { path, title }];
    });
  }, []);
  const close = useCallback((path: string) => setTabs((now) => (path === HOME.path ? now : now.filter((t) => t.path !== path))), []);
  /** A tab's title (a workflow's name as it is written) and whether it has changes not yet saved. */
  const name = useCallback(
    (path: string, title: string, dirty: boolean) =>
      setTabs((now) => (now.some((t) => t.path === path && (t.title !== title || !!t.dirty !== dirty)) ? now.map((t) => (t.path === path ? { ...t, title, dirty } : t)) : now)),
    [],
  );
  return { tabs, open, close, name };
}

export function TabBar(props: { tabs: OpenTab[]; current: string; onGo: (path: string) => void; onClose: (path: string) => void; end?: ReactNode }) {
  const row = useRef<HTMLDivElement>(null);
  // Whether there are tabs out of sight before or after those shown: the arrows page to them.
  const [more, setMore] = useState({ before: false, after: false });
  const measure = useCallback(() => {
    const r = row.current;
    if (!r) return;
    setMore({ before: r.scrollLeft > 1, after: r.scrollLeft + r.clientWidth < r.scrollWidth - 1 });
  }, []);
  useEffect(() => {
    row.current?.querySelector(".open-tab.here")?.scrollIntoView?.({ block: "nearest", inline: "nearest" });
    measure();
  }, [props.current, props.tabs.length, measure]);
  useEffect(() => {
    const r = row.current;
    if (!r || typeof ResizeObserver === "undefined") return;
    const seen = new ResizeObserver(measure);
    seen.observe(r);
    return () => seen.disconnect();
  }, [measure]);
  /** A page of tabs on: the row moves by what it shows, at once. */
  const page = (by: number) => {
    const r = row.current;
    if (!r) return;
    r.scrollLeft += by * r.clientWidth;
    measure();
  };
  return (
    <div className="tabbar">
      {(more.before || more.after) && (
        <button className="tab-arrow" aria-label="Tabs before" title="Tabs before" disabled={!more.before} onClick={() => page(-1)}>
          ‹
        </button>
      )}
      <div className="open-tabs" role="tablist" aria-label="Open tabs" ref={row} onScroll={measure}>
        {props.tabs.map((t) => {
          const here = t.path === props.current;
          const closable = t.path !== HOME.path;
          return (
            <div
              key={t.path}
              className={`open-tab${here ? " here" : ""}${t.dirty ? " dirty" : ""}`}
              role="tab"
              aria-selected={here}
              title={t.title}
              onMouseDown={(e) => {
                // The middle button closes a tab, as in a browser.
                if (e.button === 1 && closable) {
                  e.preventDefault();
                  props.onClose(t.path);
                }
              }}
            >
              <button className="tab-go" onClick={() => props.onGo(t.path)}>
                <Glyph name={glyphOf(t.path)} size={14} />
                <span className="tab-title">{t.title}</span>
              </button>
              {t.dirty && <span className="tab-dot" aria-label="not saved" />}
              {closable && (
                <button className="tab-close" aria-label={`Close ${t.title}`} title="Close" onClick={() => props.onClose(t.path)}>
                  ×
                </button>
              )}
            </div>
          );
        })}
      </div>
      {(more.before || more.after) && (
        <button className="tab-arrow" aria-label="Tabs after" title="Tabs after" disabled={!more.after} onClick={() => page(1)}>
          ›
        </button>
      )}
      {props.end && <div className="tab-end">{props.end}</div>}
    </div>
  );
}
