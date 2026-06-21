"use client";

import EChart from "./EChart";
import { fmtMonth } from "@/lib/client/format";

export default function LineTrend({
  series,
  months,
  height = 240,
  exportTitle,
}: {
  series: { name: string; data: (number | null)[]; color?: string }[];
  months: string[];
  height?: number;
  exportTitle?: string;
}) {
  return (
    <EChart
      height={height}
      exportTitle={exportTitle}
      option={{
        color: ["#0093d5", "#22b457", "#7c3aed", "#f29e0b", "#e23636", "#0f766e"],
        grid: { left: 4, right: 12, top: 28, bottom: 4, containLabel: true },
        tooltip: { trigger: "axis", valueFormatter: (v: number | null) => (v === null ? "—" : `${v}%`) },
        legend: { show: series.length > 1, top: 0, type: "scroll", textStyle: { fontSize: 10 } },
        xAxis: { type: "category", data: months.map(fmtMonth), axisLabel: { fontSize: 10 }, boundaryGap: false },
        yAxis: { type: "value", max: 100, axisLabel: { formatter: "{value}%", fontSize: 10 }, splitLine: { lineStyle: { color: "#f1f5f9" } } },
        series: series.map((s) => ({
          name: s.name,
          type: "line",
          data: s.data,
          smooth: true,
          connectNulls: true,
          symbol: "circle",
          symbolSize: 6,
          lineStyle: s.color ? { color: s.color, width: 2.5 } : { width: 2.5 },
          itemStyle: s.color ? { color: s.color } : undefined,
          label: series.length === 1 ? { show: true, formatter: (p: { value: number | null }) => (p.value === null ? "" : `${p.value}%`), fontSize: 10, color: "#475569" } : undefined,
        })),
      }}
    />
  );
}
