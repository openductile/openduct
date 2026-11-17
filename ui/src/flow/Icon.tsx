// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

import { BRANDS, STEP_BRAND } from "./brands";
import { iconOf } from "./look";

/** A brand's logo, in its own colour (a very dark one follows the text, so it shows on a dark page too). */
export function BrandIcon({ slug, size = 18 }: { slug: string; size?: number }) {
  const brand = BRANDS[slug];
  if (!brand) return null;
  const dark = parseInt(brand.hex.slice(0, 2), 16) + parseInt(brand.hex.slice(2, 4), 16) + parseInt(brand.hex.slice(4, 6), 16) < 120;
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill={dark ? "currentColor" : `#${brand.hex}`} aria-hidden="true">
      <title>{brand.title}</title>
      <path d={brand.path} />
    </svg>
  );
}

/** A step's (or a trigger's) icon: its service's logo when it has one, else a plain one in the text's colour. */
export function Icon({ kind, brand }: { kind: string; brand?: string }) {
  const slug = brand ?? STEP_BRAND[kind];
  if (slug && BRANDS[slug]) return <BrandIcon slug={slug} />;
  return (
    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d={iconOf(kind)} />
    </svg>
  );
}

/** Whether a kind of step shows its service's logo (its tile is then plain, not its family's colour). */
export function hasBrand(kind: string): boolean {
  return !!BRANDS[STEP_BRAND[kind] ?? ""];
}
