// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A text box as tall as what it holds - its lines as they wrap in its width, not only the lines typed - from its
// rows up to `max` pixels, then it scrolls. One that holds a single line (an expression, an address, a subject) grows
// as its text wraps, and Enter adds no line to it.

import { useLayoutEffect, useRef, type TextareaHTMLAttributes } from "react";

type Props = Omit<TextareaHTMLAttributes<HTMLTextAreaElement>, "value" | "onChange"> & {
  value: string;
  onChange: (value: string) => void;
  /** The most it grows to, in pixels, before it scrolls. */
  max?: number;
  /** One line of text, wrapped to be seen whole: Enter adds no line. */
  oneLine?: boolean;
  "data-expr"?: string;
  "data-plain"?: string;
};

export function Grow({ value, onChange, max = 360, oneLine, className, rows, onKeyDown, ...rest }: Props) {
  const box = useRef<HTMLTextAreaElement>(null);
  const fit = () => {
    const el = box.current;
    // Nothing measured (a hidden window, or no layout at all): the rows stand.
    if (!el || el.offsetParent === null) return;
    el.style.height = "auto";
    const want = el.scrollHeight + (el.offsetHeight - el.clientHeight);
    if (want <= 0) return;
    el.style.height = `${Math.min(want, max)}px`;
    el.style.overflowY = want > max ? "auto" : "hidden";
  };
  useLayoutEffect(fit, [value, max]);
  // Its width changes how its text wraps: a window opened or resized.
  useLayoutEffect(() => {
    const el = box.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    let width = el.clientWidth;
    const seen = new ResizeObserver(() => {
      if (el.clientWidth !== width) {
        width = el.clientWidth;
        fit();
      }
    });
    seen.observe(el);
    return () => seen.disconnect();
  }, []);
  return (
    <textarea
      ref={box}
      className={[className, oneLine ? "grow-line" : "grow"].filter(Boolean).join(" ")}
      rows={rows ?? (oneLine ? 1 : 3)}
      value={value}
      spellCheck={false}
      onChange={(e) => onChange(oneLine ? e.target.value.replace(/\r?\n/g, " ") : e.target.value)}
      onKeyDown={(e) => {
        if (oneLine && e.key === "Enter") e.preventDefault();
        onKeyDown?.(e);
      }}
      {...rest}
    />
  );
}
