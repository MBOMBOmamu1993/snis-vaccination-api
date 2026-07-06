// Tailwind config for the design-sync stylesheet build ONLY (never used by
// the Next.js app). Extends the app config with:
//  - the authored preview files as content (previews may use utilities the
//    app itself doesn't), and
//  - a safelist of the full brand color scales, so designs built in
//    claude.ai/design can use any oms/navy/surface/etc. utility even when the
//    app's own pages never referenced that shade (the shipped styles.css is a
//    compiled sheet — unsafelisted, unused utilities simply don't exist in it).
import type { Config } from "tailwindcss";
import base from "../tailwind.config";

const config: Config = {
  ...base,
  content: [...(base.content as string[]), "./.design-sync/previews/**/*.tsx"],
  safelist: [
    { pattern: /^(bg|text|border|ring)-(oms|navy|danger|warn|good|surface)-\d+$/ },
    { pattern: /^(bg|text|border)-navy$/ },
    "shadow-card",
    // @layer components classes are tree-shaken like utilities — keep the whole
    // documented vocabulary available to designs even when the app itself
    // doesn't currently use a class:
    "card", "card-pad", "card-header", "card-title", "card-subtitle",
    "kpi-label", "kpi-value", "kpi-sub",
    "chip", "chip-good", "chip-warn", "chip-bad", "chip-info",
    "btn", "btn-primary", "input", "table-default",
    "section-bar", "badge-appr", "dtable", "dtable-frozen",
  ],
};

export default config;
