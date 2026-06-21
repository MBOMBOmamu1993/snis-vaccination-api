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
    const inst = echarts.init(ref.current, undefined, { renderer: "canvas" });
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
    instRef.current?.setOption(option, true);
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
