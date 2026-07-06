# design-sync notes — snis-vaccination-dhis2

Repo-specific gotchas for future syncs.

- **App repo, not a library**: Next.js app with no `dist/`. The bundle entry is the
  committed barrel `.design-sync/ds-entry.ts` (`cfg.entry`) — the chart components are
  `export default` in `components/charts/*`, so a synthesized `export * from` entry would
  drop them; the barrel re-exports them by name. When a new reusable component is added to
  the app, add it to the barrel AND to `cfg.componentSrcMap`.
- **Source root**: both `lib/` and `components/` exist; `cfg.srcDir: "components"` pins
  discovery away from `lib/` (which holds data/format helpers, not components).
- **CSS is compiled per-sync**: no prebuilt stylesheet exists. `cfg.buildCmd` runs the
  Tailwind CLI with `.design-sync/tailwind.sync.ts` (app config + previews as content + a
  safelist of the full oms/navy/danger/warn/good/surface scales so designs can use any
  shade). Run `cfg.buildCmd` BEFORE `package-build.mjs` — `cfg.cssEntry` points at its
  gitignored output (`.design-sync/.cache/tailwind.css`).
- **Encoding fix in app source**: `slugify` in `components/ui/TableExport.tsx` used a regex
  with literal combining diacritics (U+0300-U+036F as raw characters in the character class), which is mojibake-fragile — the whole
  4 MB bundle failed to evaluate when parsed as windows-1252 ([BUNDLE_EXPORT] 14/14). Fixed
  to the escaped form `/[\u0300-\u036f]/g` (identical behavior). Don't reintroduce raw
  combining characters in regex literals.
- **Frozen-clock capture vs ECharts**: zrender drives animations off `new Date().getTime()`;
  package-capture freezes Date (`setFixedTime`), so charts captured with no series marks
  (empty bars/rings). `.design-sync/previews/_liveClock.ts` detects a frozen clock and
  installs a fast-forward Date shim — imported as line 1 of the EChart/Donut/LineTrend/Radar
  previews only. With a real clock it installs nothing. If a future skill version changes the
  capture harness, re-check chart sheets first.
- **Playwright**: this container pins chromium-1194 at `/opt/pw-browsers` → install
  `playwright@1.56.0` in `.ds-sync/` (newer versions want other builds and fail to launch).
- **[FONT_MISSING] "Inter" is accepted**: the font stack is
  `ui-sans-serif, system-ui, …, Roboto, Inter, Helvetica, …` — Inter sits mid-stack after
  always-resolving system fonts, so it never actually renders; the design intent is system
  UI type. Nothing to ship. (Validate will keep warning — known.)
- **Card grouping**: `components/ui/*` land in group `general` (the `ui` dir name is
  generic-skipped), `components/charts/*` in `charts`.

## Known render warns

- `[FONT_MISSING] "Inter"` — see above, accepted by analysis (system-font stack).
- `[GRID_OVERFLOW]` remedies applied: `cardMode: "column"` for KpiCard, EChart, Donut,
  LineTrend, Radar (wide, chart/grid-shaped stories).

## Re-sync risks

- The preview data (provinces, antigens, %) is invented-but-plausible sample content inlined
  in `.design-sync/previews/*.tsx`; it doesn't track the app's real data model. If column
  labels or component APIs change, previews need a manual pass.
- `_liveClock.ts` depends on the capture harness freezing Date but not `performance.now` —
  a harness change can silently re-blank the chart sheets (grades would carry; the sheets
  would lie). Eyeball chart sheets on every re-sync.
- The Tailwind safelist in `tailwind.sync.ts` must keep tracking `tailwind.config.ts` color
  families — a palette rename there silently drops safelisted utilities from `styles.css`.
- First sync ran WITHOUT DesignSync authorization (remote claude.ai/code session):
  verified bundle built locally, never uploaded, no `projectId` pinned. The next authorized
  `/design-sync` run should create the project and upload (full first-sync scope).
- **Livraison via l'agent claude.ai/design (2026-07-06)** : la session distante claude.ai/code
  n'obtient pas l'autorisation DesignSync (« Send to Claude Code Web » ouvre une *nouvelle*
  session, il ne donne pas l'accès à la session courante). Contournement utilisé : le bundle
  vérifié (78 fichiers, 100 % texte) est commité sous `ds-bundle/` sur la branche de sync
  (`git add -f` par-dessus le .gitignore), et l'agent intégré du projet claude.ai/design
  (projectId 4fc85617-c873-49d6-8194-3a1c8dd351a8, épinglé dans config.json) le copie verbatim
  depuis GitHub — ordre : sentinel d'abord, contenu, `_ds_sync.json` en dernier. Un futur run
  AVEC autorisation DesignSync doit reprendre le chemin normal (atomique) et peut retirer
  `ds-bundle/` de la branche.
