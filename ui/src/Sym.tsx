// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The small symbols beside the studio's action words - add, change, remove, keep, send - one set for every page.

const PATHS: Record<string, string> = {
  plus: "M12 5v14M5 12h14",
  pencil: "M4 20h4L19 9l-4-4L4 16zM14 6l4 4",
  trash: "M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3",
  check: "M5 12l5 5L20 7",
  send: "M22 2 11 13M22 2l-7 20-4-9-9-4z",
  template: "M12 3 3 8l9 5 9-5zM3 12.5l9 5 9-5M3 17l9 5 9-5",
  plug: "M9 2v6M15 2v6M7 8h10v4a5 5 0 0 1-10 0zM12 17v5",
  key: "M15 7a4 4 0 1 1-3.9 4.9L4 19v-3h2v-2h2l1.1-1.1A4 4 0 0 1 15 7zM16 10h.01",
  person: "M15 19v-1a4 4 0 0 0-4-4H7a4 4 0 0 0-4 4v1M9 10a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7zM19 8v6M16 11h6",
  arrow: "M5 12h14M13 6l6 6-6 6",
  close: "M6 6l12 12M18 6 6 18",
};

export function Sym({ name, size = 15 }: { name: keyof typeof PATHS | string; size?: number }) {
  return (
    <svg className="sym" viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={PATHS[name]} />
    </svg>
  );
}
