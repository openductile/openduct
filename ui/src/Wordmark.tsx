// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// OpenDuct's name inside its mark: the red pipe run rising out of the ground on either side of the word, over it
// and down again, its joints flanged and a valve on top. The word takes the page's own colour, so it reads in dark
// and light alike.

import { useId } from "react";

export function Wordmark(props: { height?: number }) {
  const id = useId().replace(/:/g, "");
  const height = props.height ?? 56;
  return (
    <svg className="wordmark" viewBox="0 0 160 64" height={height} width={(height * 160) / 64} role="img" aria-label="OpenDuct">
      <defs>
        <linearGradient id={`run-${id}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stopColor="#F26450" />
          <stop offset="1" stopColor="#C62A1E" />
        </linearGradient>
      </defs>
      {/* The run: up out of the ground, over the word, and down again. */}
      <path d="M10 64V30a16 16 0 0 1 16-16h108a16 16 0 0 1 16 16v34" fill="none" stroke={`url(#run-${id})`} strokeWidth="7.5" />
      {/* Flanged joints on each leg and either side of the valve. */}
      <rect x="4.5" y="44" width="11" height="3.6" rx="1.3" fill="#9E1F15" />
      <rect x="144.5" y="44" width="11" height="3.6" rx="1.3" fill="#9E1F15" />
      <rect x="66" y="8.5" width="3.6" height="11" rx="1.3" fill="#9E1F15" />
      <rect x="90.4" y="8.5" width="3.6" height="11" rx="1.3" fill="#9E1F15" />
      {/* The valve: its stem and handwheel. */}
      <rect x="78.6" y="3.4" width="2.8" height="7" fill="#9E1F15" />
      <rect x="71" y="0.6" width="18" height="3.6" rx="1.8" fill="#9E1F15" />
      <text x="80" y="52" textAnchor="middle" fontSize="23" fontWeight="700" letterSpacing="-0.4" fill="currentColor" fontFamily="inherit">
        OpenDuct
      </text>
    </svg>
  );
}
