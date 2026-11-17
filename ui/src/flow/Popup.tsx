// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// A window over the page: Escape, the close button or a click outside it closes it.

import { useEffect, useRef, type ReactNode } from "react";

export function Popup(props: {
  label: string;
  title: ReactNode;
  wide?: boolean;
  /** A class of its own, for a window laid out its own way. */
  className?: string;
  /** What the close button is called, for those who cannot see its symbol: Close by default. */
  closeLabel?: string;
  onClose: () => void;
  /** What sits at its foot: the window's own buttons. */
  footer?: ReactNode;
  children: ReactNode;
}) {
  const box = useRef<HTMLDivElement>(null);
  useEffect(() => box.current?.focus(), []);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && props.onClose()}>
      <div
        ref={box}
        tabIndex={-1}
        className={`modal popup${props.wide ? " wide" : ""}${props.className ? ` ${props.className}` : ""}`}
        role="dialog"
        aria-modal="true"
        aria-label={props.label}
        onKeyDown={(e) => {
          if (e.key === "Escape") props.onClose();
        }}
      >
        <header>
          {props.title}
          <button className="popup-close" onClick={props.onClose} aria-label={props.closeLabel ?? "Close"} title={`${props.closeLabel ?? "Close"} (Esc)`}>
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" aria-hidden="true">
              <path d="M6 6l12 12M18 6 6 18" />
            </svg>
          </button>
        </header>
        <div className="popup-body">{props.children}</div>
        {props.footer && <footer className="popup-foot">{props.footer}</footer>}
      </div>
    </div>
  );
}
