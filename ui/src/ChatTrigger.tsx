// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A chat trigger's settings, in the trigger's window: where its chat is (its page, a script tag and a frame for a
// site, the JSON address for a front end of one's own), its words and colours, the sites it may be on, a password,
// how much of a conversation each run is given, its limits - and "Try the chat", which runs the workflow being
// edited on a message as a preview does (the model is asked for real; writes are only shown).

import { type ReactNode, useState } from "react";
import { api, type Definition, type Run, type WorkflowRecord } from "./api";
import { Copy } from "./Trigger";
import "./ChatTrigger.css";

type Settings = Record<string, unknown>;
type Words = Record<string, string>;
type Turn = { role: "user" | "assistant"; content: string };

const TEXTS: { key: string; label: string; hint: string; long?: boolean }[] = [
  { key: "title", label: "Title", hint: "Chat" },
  { key: "welcome", label: "Welcome", hint: "Hi! How can I help?", long: true },
  { key: "placeholder", label: "Box", hint: "Type a message" },
  { key: "send", label: "Button", hint: "Send" },
  { key: "error", label: "When it fails", hint: "Sorry, something went wrong. Please try again." },
];
const COLOURS: { key: string; label: string; standard: string }[] = [
  { key: "accent", label: "Accent", standard: "#0f766e" },
  { key: "background", label: "Background", standard: "#ffffff" },
  { key: "text", label: "Text", standard: "#1f2933" },
];

function record(value: unknown): Settings {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Settings) : {};
}

/** The settings with `key` set in `group` (or at the top), an empty value taken out. */
function changed(settings: Settings, group: string | null, key: string, value: unknown): Settings {
  const empty = value === "" || value === undefined || (Array.isArray(value) && value.length === 0);
  if (!group) {
    const next = { ...settings };
    if (empty) delete next[key];
    else next[key] = value;
    return next;
  }
  const inner = { ...record(settings[group]) };
  if (empty) delete inner[key];
  else inner[key] = value;
  const next = { ...settings };
  if (Object.keys(inner).length === 0) delete next[group];
  else next[group] = inner;
  return next;
}

/** A random password, as a person types it: 20 letters and digits. */
function newPassword(): string {
  const letters = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const bytes = new Uint8Array(20);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => letters[b % letters.length]).join("");
}

function Info(props: { children: ReactNode }) {
  return (
    <p className="tg-note">
      <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" aria-hidden="true">
        <path d="M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20zM12 16v-4M12 8h.01" />
      </svg>
      <span>{props.children}</span>
    </p>
  );
}

function Snippet(props: { label: string; text: string }) {
  return (
    <div className="tg-field">
      <span className="tg-name">{props.label}</span>
      <div className="tg-curl">
        <pre>{props.text}</pre>
        <Copy text={props.text} label={`Copy: ${props.label}`} />
      </div>
    </div>
  );
}

export function ChatTrigger(props: { definition: Definition; record: WorkflowRecord; settings: Settings; onChange: (s: Settings) => void }) {
  const { settings, onChange } = props;
  const texts = record(settings.texts) as Words;
  const theme = record(settings.theme) as Words;
  const limits = record(settings.limits);
  const origins = Array.isArray(settings.origins) ? (settings.origins as string[]) : [];
  const [sites, setSites] = useState(origins.join("\n"));
  const password = typeof settings.password === "string" ? settings.password : "";
  const address = props.record.chat ? `${location.origin}${props.record.chat}` : "";
  const number = (v: string) => (v.trim() === "" ? undefined : Number(v));
  return (
    <>
      <div className="tg-section">
        {address ? (
          <>
            <div className="tg-field">
              <span className="tg-name">The chat's page</span>
              <div className="tg-copyable">
                <code title={address}>{address}</code>
                <a className="tg-icon" href={address} target="_blank" rel="noreferrer" aria-label="Open the chat's page" title="Open">
                  ↗
                </a>
                <Copy text={address} label="Copy the page's address" />
              </div>
            </div>
            <Snippet label="On a site: a chat button" text={`<script src="${address}/embed.js" async></script>`} />
            <Snippet label="On a site: in a frame" text={`<iframe src="${address}" title="Chat" style="width:380px;height:560px;border:0"></iframe>`} />
            <Snippet
              label="From a front end of your own"
              text={`curl -X POST ${address}/messages \\\n  -H 'content-type: application/json'${password ? " \\\n  -H 'authorization: Bearer <the password>'" : ""} \\\n  -d '{"message": "Hello", "session": "a-long-random-conversation-id"}'`}
            />
            {!props.record.active && <Info>Turn the workflow on: until then its page says the chat is closed.</Info>}
          </>
        ) : (
          <Info>Save the workflow to get its chat's address.</Info>
        )}
        <Info>
          Each message is a run: <code>value.message</code>, <code>value.session</code> (the conversation, the same for each of its messages), <code>value.history</code> (its earlier messages, for an
          AI step's Messages) and <code>value.person</code>. The answer is the respond step's, or the last step's text.
        </Info>
      </div>

      <div className="tg-section">
        <span className="tg-name">Words</span>
        {TEXTS.map((t) => (
          <label key={t.key} className="tg-field">
            <span className="tg-name">{t.label}</span>
            {t.long ? (
              <textarea value={texts[t.key] ?? ""} placeholder={t.hint} rows={2} onChange={(e) => onChange(changed(settings, "texts", t.key, e.target.value))} />
            ) : (
              <input value={texts[t.key] ?? ""} placeholder={t.hint} onChange={(e) => onChange(changed(settings, "texts", t.key, e.target.value))} />
            )}
          </label>
        ))}
        <div className="chat-colours">
          {COLOURS.map((c) => (
            <label key={c.key} className="tg-field">
              <span className="tg-name">{c.label}</span>
              <input type="color" value={theme[c.key] ?? c.standard} aria-label={`${c.label} colour`} onChange={(e) => onChange(changed(settings, "theme", c.key, e.target.value))} />
            </label>
          ))}
        </div>
      </div>

      <div className="tg-section">
        <label className="tg-field">
          <span className="tg-name">
            Sites it may be on <span className="tg-optional">one a line</span>
          </span>
          <textarea
            className="code"
            rows={2}
            value={sites}
            placeholder="https://www.example.com"
            aria-label="Sites it may be on"
            onChange={(e) => {
              setSites(e.target.value);
              const list = e.target.value
                .split("\n")
                .map((s) => s.trim())
                .filter(Boolean);
              onChange(changed(settings, null, "origins", list));
            }}
          />
          <span className="tg-hint">Only these sites may show the chat in a frame and send it messages from their pages; * for any. Its own page always may.</span>
        </label>
        <label className="tg-field">
          <span className="tg-name">
            Password <span className="tg-optional">optional</span>
          </span>
          <div className="tg-copyable tg-token">
            <input
              className="code"
              value={password}
              placeholder="none: anyone with the address may chat"
              aria-label="Password"
              autoComplete="off"
              spellCheck={false}
              onChange={(e) => onChange(changed(settings, null, "password", e.target.value))}
            />
            <button type="button" className="tg-icon" aria-label="Make a new password" title="Make a new password" onClick={() => onChange(changed(settings, null, "password", newPassword()))}>
              ⟳
            </button>
          </div>
          <span className="tg-hint">The page asks for it; a front end sends it as a bearer token.</span>
        </label>
      </div>

      <div className="tg-section chat-numbers">
        <label className="tg-field">
          <span className="tg-name">Earlier turns each run is given</span>
          <input type="number" min={0} max={50} value={String(settings.history ?? "")} placeholder="10" onChange={(e) => onChange(changed(settings, null, "history", number(e.target.value)))} />
        </label>
        <label className="tg-field">
          <span className="tg-name">Messages a minute, per conversation</span>
          <input type="number" min={0} max={10000} value={String(limits.per_session ?? "")} placeholder="20" onChange={(e) => onChange(changed(settings, "limits", "per_session", number(e.target.value)))} />
        </label>
        <label className="tg-field">
          <span className="tg-name">Messages a minute, per address</span>
          <input type="number" min={0} max={10000} value={String(limits.per_address ?? "")} placeholder="60" onChange={(e) => onChange(changed(settings, "limits", "per_address", number(e.target.value)))} />
        </label>
        <label className="tg-field">
          <span className="tg-name">Longest message (characters)</span>
          <input type="number" min={1} max={16000} value={String(limits.message ?? "")} placeholder="4000" onChange={(e) => onChange(changed(settings, "limits", "message", number(e.target.value)))} />
        </label>
      </div>

      <TryChat id={props.record.id} definition={props.definition} welcome={texts.welcome || "Hi! How can I help?"} />
    </>
  );
}

/** "Try the chat": a conversation with the workflow being edited, each message run as a preview. */
export function TryChat(props: { id: string; definition: Definition; welcome: string }) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const [run, setRun] = useState<Run | null>(null);
  const send = async () => {
    const message = text.trim();
    if (!message || busy) return;
    setBusy(true);
    setProblem(null);
    setText("");
    const before = turns;
    setTurns([...before, { role: "user", content: message }]);
    try {
      const said = await api.tryChat(props.id, props.definition, message, before);
      setRun(said.run);
      if (said.failed) {
        setProblem(`${said.error_text} (the run failed at ${said.run.at ?? "a step"}: ${said.run.error ?? ""})`);
        setTurns(before);
      } else {
        setTurns([...before, { role: "user", content: message }, { role: "assistant", content: said.answer ?? "" }]);
      }
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
      setTurns(before);
      setText(message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="tg-section chat-try" aria-label="Try the chat">
      <div className="chat-try-head">
        <span className="tg-name">Try the chat</span>
        <button
          type="button"
          className="chat-again"
          onClick={() => {
            setTurns([]);
            setRun(null);
            setProblem(null);
          }}
        >
          Start again
        </button>
      </div>
      <ol className="chat-try-messages" aria-live="polite">
        <li className="bot">{props.welcome}</li>
        {turns.map((t, i) => (
          <li key={i} className={t.role === "user" ? "me" : "bot"}>
            {t.content || <em>(no answer: the run gave no text)</em>}
          </li>
        ))}
        {busy && <li className="bot pending">…</li>}
      </ol>
      {problem && (
        <p className="chat-problem" role="alert">
          {problem}
        </p>
      )}
      <form
        className="chat-try-ask"
        onSubmit={(e) => {
          e.preventDefault();
          void send();
        }}
      >
        <input value={text} placeholder="Type a message" aria-label="A message to try" disabled={busy} onChange={(e) => setText(e.target.value)} />
        <button type="submit" disabled={busy || !text.trim()}>
          Send
        </button>
      </form>
      <p className="tg-hint">Runs the workflow as it is here, saved or not, as Preview does: the model is asked for real; what writes is only shown.</p>
      {run && (
        <details className="chat-run">
          <summary>
            The last run: {run.status}, {run.ms} ms
          </summary>
          <ul>
            {run.steps.map((s) => (
              <li key={s.name}>
                <code>{s.name}</code> {s.kind}, {s.ms} ms{s.error ? `: ${s.error}` : ""}
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}
