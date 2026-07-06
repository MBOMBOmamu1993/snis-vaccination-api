// Design-sync bundle entry — the curated export surface of this app's reusable
// UI. This repo is a Next.js app (no library dist), so this barrel is what
// esbuild bundles into window.SnisPev for claude.ai/design.
// The chart components are default exports in their source files — they must
// be re-exported by name here (a synthesized `export * from` entry would drop
// them).
export { Card, CardHeader, SectionBar, HEADER_TONE, type HeaderTone } from "@/components/ui/Card";
export { KpiCard, type KpiTone } from "@/components/ui/KpiCard";
export { DataTable, type HeatLevel } from "@/components/ui/DataTable";
export { EmptyState } from "@/components/ui/EmptyState";
export { Icon, type IconName } from "@/components/ui/Icon";
export { Pager, usePaged } from "@/components/ui/Pagination";
export {
  TableExportButtons,
  downloadCsv,
  downloadXlsx,
  slugify,
  tableToData,
  type TableData,
} from "@/components/ui/TableExport";
export { default as EChart, PEV_PALETTE } from "@/components/charts/EChart";
export { default as Donut } from "@/components/charts/Donut";
export { default as LineTrend } from "@/components/charts/LineTrend";
export { default as Radar } from "@/components/charts/Radar";
export { default as ChartMenu, dataFromOption } from "@/components/charts/ChartMenu";
