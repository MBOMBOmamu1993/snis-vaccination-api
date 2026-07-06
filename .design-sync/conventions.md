# Conventions — Dashboard PEV de routine (OMS / RDC)

French-language public-health dashboard UI (WHO/EPI identity: navy `#00205c` + WHO cyan `#0093d5`). All user-facing text is **French**. Typography is the system UI stack (no webfont to load). Data domain: DHIS2 routine immunization — provinces of the DRC, antigens (BCG, Penta1/3, VPO, VAR, PCV13), completeness/timeliness/coverage percentages.

## Setup

No provider or theme wrapper is required — import components and render. Charts (`EChart`, `Donut`, `LineTrend`, `Radar`) are self-contained ECharts canvases with the "pev" theme built in; give them a sized container (they fill width; height via the `height` prop).

## Styling idiom

Tailwind utilities + a small set of component classes shipped in `styles.css`. Style your own layout glue with utilities from these brand scales (all shades ship):

| Family | Scale | Use |
|---|---|---|
| `oms-50…900` | WHO cyan (`oms-500` = #0093d5) | brand accents, links, focus rings |
| `navy`, `navy-400…900` | WHO navy (`navy-700` = #00205c) | headers, section bars, table first column |
| `good-50…600` / `warn-50…600` / `danger-50…700` | status green/amber/red | thresholds: ≥90 good, ≥70 warn, else bad |
| `surface-0…900` | slate neutrals | page bg `bg-surface-100`, text `text-surface-900` |

Component classes (from `styles.css` — read it before styling): `card`, `card-pad`, `card-header`, `card-title`, `card-subtitle` · KPI text: `kpi-label`, `kpi-value`, `kpi-sub` · chips: `chip-good`, `chip-warn`, `chip-bad`, `chip-info` · buttons/inputs: `btn`, `btn-primary`, `input` · navy section banner: `section-bar` · dense data table: `table.dtable` (+ `td.name` for the navy first column, `dtable-frozen` for a sticky first column, heatmap cells `hm-good|warn|bad` and `*-soft`) · lighter table: `table-default`.

## Composition rules

- Page skeleton: `bg-surface-100` page → `SectionBar` per section → `Card`s. Inside a card: `CardHeader` (title/subtitle/`icon`+`iconTone`) then the visual.
- KPIs: a grid of `KpiCard` (tones: `neutral|navy|good|warn|bad|brand|violet|teal` — pick by threshold, not decoration; `icon` renders as a corner watermark).
- Tables: `DataTable` with `columns`/`rows` (row = object keyed by column label); pass `heat={(col,v)=>…}` returning `"good"|"warn"|"bad"|null` for threshold coloring; `exportFilename` adds CSV/Excel buttons. Paginate big tables with `usePaged` + `Pager`.
- Charts: `EChart` takes a full ECharts `option` (the "pev" theme applies the palette `PEV_PALETTE`); `menu={false}` hides the export menu. `Donut` (`data: {name,value,color?}[]`, `centerLabel`), `LineTrend` (`months: "YYYY-MM"[]`, `series`, y = %), `Radar` (`indicators`, `entities`, 0–100).
- Icons: `<Icon name="…" className="w-5 h-5 text-navy-700" />` — stroke SVG, inherits `currentColor`. Names include: `home analyse report map pin calendar download refresh syringe child people clinic hospital fridge truck shield clipboard doc database bars layers check alert trophy time down up chevron-down chevron-left chevron-right`.
- Empty data → `EmptyState` (`title`, `message`), never a blank div.

## Idiomatic example

```tsx
import { Card, CardHeader, DataTable, KpiCard, LineTrend, SectionBar } from "snis-vaccination-dhis2";

const months = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"];

export default function Page() {
  return (
    <div className="bg-surface-100 min-h-screen p-4 space-y-3">
      <SectionBar icon="clipboard">Contrôle qualité des données</SectionBar>
      <div className="grid grid-cols-2 gap-3">
        <KpiCard label="Complétude" value="94,2 %" tone="good" icon="clipboard" sub="1 842 / 1 955 rapports" />
        <KpiCard label="Promptitude" value="78,6 %" tone="warn" icon="time" sub="Avant le 5 du mois" />
      </div>
      <Card>
        <CardHeader title="Évolution mensuelle" subtitle="Janv.–Juin 2026" icon="analyse" iconTone="navy" />
        <LineTrend months={months} series={[{ name: "Complétude", data: [88, 90, 91, 93, 92, 94], color: "#0093d5" }]} />
      </Card>
      <Card>
        <CardHeader title="Couverture par province" subtitle="Penta3 · VAR1" icon="map" iconTone="blue" />
        <DataTable
          columns={["Province", "Penta3 (%)", "VAR1 (%)"]}
          rows={[{ "Province": "Kinshasa", "Penta3 (%)": 92.7, "VAR1 (%)": 89.3 }]}
          heat={(c, v) => (c === "Province" || typeof v !== "number" ? null : v >= 90 ? "good" : v >= 70 ? "warn" : "bad")}
        />
      </Card>
    </div>
  );
}
```
