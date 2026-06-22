"use client";

import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { BarChart, LineChart, PieChart, RadarChart, GaugeChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  RadarComponent,
  MarkLineComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { EChartsCoreOption } from "echarts/core";
import ChartMenu from "./ChartMenu";
import type { TableData } from "@/components/ui/TableExport";

echarts.use([
  BarChart,
  LineChart,
  PieChart,
  RadarChart,
  GaugeChart,
  GridComponent,
  TooltipComponent,
  TitleComponent,
  LegendComponent,
  RadarComponent,
  MarkLineComponent,
  CanvasRenderer,
]);

/* -------------------------------------------------------------------------- */
/* Thème ECharts « pev » — uniformise l'aspect de TOUS les graphiques :       */
/* palette OMS cohérente, axes discrets, gridlines légères, tooltip moderne   */
/* (carte blanche, coins arrondis, ombre douce) et animation fluide.          */
/* Aucune donnée ni calcul n'est touché : seul l'habillage visuel change.     */
/* -------------------------------------------------------------------------- */
const PEV_FONT =
  "ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif";

/** Palette catégorielle alignée sur l'identité OMS / PEV. */
export const PEV_PALETTE = [
  "#0093d5", // OMS cyan
  "#00205c", // navy
  "#1f9d57", // vert
  "#f59e0b", // ambre
  "#7c3aed", // violet
  "#0d9488", // teal
  "#e23636", // rouge
  "#db2777", // rose
];

let themeRegistered = false;
function ensureTheme() {
  if (themeRegistered) return;
  themeRegistered = true;
  echarts.registerTheme("pev", {
    color: PEV_PALETTE,
    textStyle: { fontFamily: PEV_FONT, color: "#475569" },
    title: { textStyle: { color: "#0f172a", fontWeight: 700 }, subtextStyle: { color: "#64748b" } },
    legend: {
      textStyle: { color: "#475569", fontSize: 11 },
      itemWidth: 12,
      itemHeight: 12,
      icon: "roundRect",
    },
    categoryAxis: {
      axisLine: { show: true, lineStyle: { color: "#cbd5e1" } },
      axisTick: { show: false },
      axisLabel: { color: "#475569" },
      splitLine: { show: false, lineStyle: { color: ["#eef2f7"] } },
      splitArea: { show: false },
    },
    valueAxis: {
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#64748b" },
      splitLine: { show: true, lineStyle: { color: ["#eef2f7"], width: 1 } },
      splitArea: { show: false },
    },
    tooltip: {
      backgroundColor: "rgba(255,255,255,0.98)",
      borderColor: "#e2e8f0",
      borderWidth: 1,
      padding: [8, 12],
      textStyle: { color: "#1e293b", fontSize: 12 },
      extraCssText:
        "border-radius:10px;box-shadow:0 12px 30px -10px rgba(0,32,92,.30);backdrop-filter:blur(2px);",
      axisPointer: {
        lineStyle: { color: "#94a3b8", width: 1 },
        crossStyle: { color: "#94a3b8" },
        shadowStyle: { color: "rgba(0,32,92,0.05)" },
      },
    },
  });
}
ensureTheme();

/**
 * Conteneur ECharts commun. Chaque graphique du dashboard expose le menu
 * d'export ≡ (plein écran / impression / PNG / JPEG / PDF / SVG / CSV / XLS /
 * tableau de données — cf. specs feedback TL 01 §2). `exportTitle` nomme les
 * fichiers ; `exportData` force les données CSV/XLS (sinon extraction
 * générique de l'option). `menu={false}` masque le menu (jauges, sparklines).
 */
export default function EChart({
  option,
  height = 300,
  className,
  exportTitle,
  exportData,
  menu = true,
}: {
  option: EChartsCoreOption;
  height?: number;
  className?: string;
  exportTitle?: string;
  exportData?: TableData;
  menu?: boolean;
}) {
  const ref = useRef<HTMLDivElement | null>(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const instRef = useRef<echarts.ECharts | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    const inst = echarts.init(ref.current, "pev", { renderer: "canvas" });
    instRef.current = inst;
    const onResize = () => inst.resize();
    window.addEventListener("resize", onResize);
    document.addEventListener("fullscreenchange", onResize);
    return () => {
      window.removeEventListener("resize", onResize);
      document.removeEventListener("fullscreenchange", onResize);
      inst.dispose();
      instRef.current = null;
    };
  }, []);

  useEffect(() => {
    // Animation fluide (« huilée ») commune à tous les graphiques ; l'option de
    // la page peut toujours surcharger ces réglages.
    instRef.current?.setOption(
      { animationDuration: 650, animationEasing: "cubicOut", animationDurationUpdate: 450, ...option },
      true,
    );
  }, [option]);

  // En plein écran, le conteneur doit rester lisible (fond blanc).
  return (
    <div ref={wrapRef} className="relative w-full bg-white">
      {menu ? (
        <ChartMenu
          getInstance={() => instRef.current}
          getContainer={() => wrapRef.current}
          option={option}
          title={exportTitle}
          exportData={exportData}
        />
      ) : null}
      <div ref={ref} style={{ height }} className={className} />
    </div>
  );
}
