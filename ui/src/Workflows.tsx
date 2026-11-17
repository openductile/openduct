// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The workflows, on and off, as small tiles; and the templates a new one can start from, on a page of their own:
// each template a card showing the steps (and the services) it uses, by category, found by a search.

import { Sym } from "./Sym";
import { useEffect, useMemo, useState } from "react";
import { api, type Definition, type Step, type Template, type WorkflowSummary } from "./api";
import { Icon } from "./flow/Icon";
import { FAMILY, type Family } from "./flow/look";
import { innerLists, kindOf } from "./Steps";

const BLANK: Definition = { name: "New workflow", trigger: { webhook: {} }, steps: [] };

const TRIGGERS: Record<string, string> = {
  webhook: "Webhook",
  schedule: "Schedule",
  topic: "Topic",
  manual: "By hand",
  failed: "When one fails",
};

/** The colour of a template's category. */
const CATEGORY: Record<string, Family> = {
  AI: "ai",
  Messaging: "message",
  Monitoring: "logic",
  Lookups: "data",
  Databases: "data",
  Streaming: "call",
  Code: "code",
  "E-commerce": "start",
  Basics: "control",
};

/** The kinds of step a workflow uses, in order, each once (branches and loops included). */
function kindsIn(steps: Step[] | undefined, into: string[] = []): string[] {
  for (const step of steps ?? []) {
    const kind = kindOf(step);
    if (!into.includes(kind)) into.push(kind);
    for (const inner of innerLists(step)) kindsIn(inner, into);
  }
  return into;
}

function triggerKind(trigger: unknown): string {
  if (typeof trigger === "string") return trigger;
  if (trigger && typeof trigger === "object") return Object.keys(trigger)[0] ?? "manual";
  return "manual";
}

/** When, in words a person reads at a glance. */
function ago(seconds: number): string {
  const s = Math.max(0, Date.now() / 1000 - seconds);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  if (s < 7 * 86400) return `${Math.floor(s / 86400)} d ago`;
  return new Date(seconds * 1000).toLocaleDateString();
}

/** A webhook whose token a template leaves empty is given one of its own: no two workflows share it. */
export function withOwnToken(definition: Definition): Definition {
  const hook = (definition.trigger as { webhook?: Record<string, unknown> } | undefined)?.webhook;
  if (!hook || hook.token !== "") return definition;
  const bytes = crypto.getRandomValues(new Uint8Array(24));
  // Letters, digits, '-' and '_': what Telegram takes as a secret token.
  const token = btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return { ...definition, trigger: { webhook: { ...hook, token } } };
}

/** Make a workflow from a definition and open it; a failure is the page's to show. */
function useMake(onOpen: (id: string) => void, setTrouble: (t: string) => void) {
  return async (definition: Definition) => {
    try {
      const made = await api.create(withOwnToken(definition));
      onOpen(made.id);
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot make it");
    }
  };
}

export function Workflows(props: { onOpen: (id: string) => void; onTemplates: () => void }) {
  const [workflows, setWorkflows] = useState<WorkflowSummary[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [query, setQuery] = useState("");
  const load = () =>
    api
      .workflows()
      .then(setWorkflows)
      .catch((e: Error) => setTrouble(e.message));
  useEffect(() => {
    load();
  }, []);
  const make = useMake(props.onOpen, setTrouble);
  const toggle = async (w: WorkflowSummary) => {
    try {
      await api.setActive(w.id, !w.active);
      load();
    } catch (e) {
      setTrouble(e instanceof Error ? e.message : "cannot change it");
    }
  };
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  const shown = (workflows ?? []).filter((w) => words.every((word) => `${w.name} ${w.trigger}`.toLowerCase().includes(word)));
  const on = (workflows ?? []).filter((w) => w.active).length;
  return (
    <main className="page list-page workflows-page">
      <div className="page-head">
        <div>
          <h1 className="visually-hidden">Workflows</h1>
          <p className="hint">{workflows === null ? "" : workflows.length === 0 ? "None yet: start from nothing or from a template." : `${workflows.length} in this workspace, ${on} on.`}</p>
        </div>
        <div className="head-tools">
          <input className="search" type="search" placeholder="Search workflows" aria-label="Search workflows" value={query} onChange={(e) => setQuery(e.target.value)} />
          <button className="quiet" onClick={props.onTemplates}>
            <Sym name="template" />
            From a template
          </button>
          <button onClick={() => make(BLANK)}>
            <Sym name="plus" />
            New workflow
          </button>
        </div>
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      {shown.length > 0 && (
        <ul className="workflow-list">
          {shown.map((w) => (
            <li key={w.id} className={w.active ? "on" : ""}>
              <span className="icon family-start">
                <Icon kind={w.trigger} />
              </span>
              <a
                className="wf-main"
                href={`/workflows/${w.id}`}
                title={w.name}
                onClick={(e) => {
                  e.preventDefault();
                  props.onOpen(w.id);
                }}
              >
                <span className="wf-name">{w.name}</span>
                <span className="wf-meta">
                  {TRIGGERS[w.trigger] ?? w.trigger}
                  <span aria-hidden="true"> · </span>
                  <span title={new Date(w.updated * 1000).toLocaleString()}>{ago(w.updated)}</span>
                </span>
              </a>
              <label className="switch small" title={w.active ? "On: click to turn off" : "Off: click to turn on"}>
                <input type="checkbox" checked={w.active} onChange={() => toggle(w)} aria-label={`${w.name} on`} />
                <span />
              </label>
            </li>
          ))}
        </ul>
      )}
      {workflows && workflows.length > 0 && shown.length === 0 && <p className="hint">No workflow matches "{query}".</p>}
      {workflows && workflows.length === 0 && (
        <div className="empty-start">
          <button onClick={() => make(BLANK)}>
            <Sym name="plus" />
            Start from nothing
          </button>
          <button className="quiet" onClick={props.onTemplates}>
            <Sym name="template" />
            Browse the templates
          </button>
        </div>
      )}
    </main>
  );
}

/** The templates a workflow can start from: each a card showing the steps (and services) it uses, by category. */
export function Templates(props: { onOpen: (id: string) => void }) {
  const [templates, setTemplates] = useState<Template[] | null>(null);
  const [trouble, setTrouble] = useState("");
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState("All");
  useEffect(() => {
    api
      .templates()
      .then(setTemplates)
      .catch((e: Error) => setTrouble(e.message));
  }, []);
  const make = useMake(props.onOpen, setTrouble);
  const all = templates ?? [];
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  const categories = useMemo(() => ["All", ...Array.from(new Set(all.map((t) => t.category))).sort()], [templates]);
  const gallery = all.filter(
    (t) =>
      (category === "All" || t.category === category) &&
      words.every((w) => `${t.name} ${t.description} ${t.category} ${kindsIn(t.definition.steps).join(" ")}`.toLowerCase().includes(w)),
  );
  return (
    <main className="page list-page templates-page">
      <div className="page-head">
        <div>
          <h1 className="visually-hidden">Templates</h1>
          <p className="hint">{templates === null ? "" : `${all.length} ready-made workflows: pick one, then make it yours.`}</p>
        </div>
        <div className="head-tools">
          <input className="search" type="search" placeholder="Search templates" aria-label="Search templates" value={query} onChange={(e) => setQuery(e.target.value)} />
        </div>
      </div>
      {trouble && <p className="error banner">{trouble}</p>}
      <div className="filters" role="group" aria-label="Categories">
        {categories.map((c) => (
          <button key={c} className={c === category ? "filter here" : "filter"} onClick={() => setCategory(c)}>
            {c}
          </button>
        ))}
      </div>
      <div className="gallery">
        {gallery.map((t) => {
          const kinds = kindsIn(t.definition.steps);
          const family = CATEGORY[t.category] ?? "control";
          return (
            <button key={t.id} className={`template-card family-${family}`} onClick={() => make(t.definition)}>
              <span className="tc-top">
                <span className="tc-icons">
                  <span className="icon family-start" title={TRIGGERS[triggerKind(t.definition.trigger)]}>
                    <Icon kind={triggerKind(t.definition.trigger)} />
                  </span>
                  {kinds.slice(0, 4).map((k) => (
                    <span key={k} className={`icon family-${FAMILY[k] ?? "data"}`} title={k}>
                      <Icon kind={k} />
                    </span>
                  ))}
                  {kinds.length > 4 && <span className="more">+{kinds.length - 4}</span>}
                </span>
                <span className="tc-category">{t.category}</span>
              </span>
              <strong>{t.name}</strong>
              <span className="tc-text">{t.description}</span>
              <span className="tc-use">Use this template →</span>
            </button>
          );
        })}
      </div>
      {templates && gallery.length === 0 && <p className="hint">No template matches.</p>}
    </main>
  );
}
