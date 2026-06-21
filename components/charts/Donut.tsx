"use client";

import EChart from "./EChart";

export default function Donut({
  data,
  height = 200,
  centerLabel,
  legend = true,
  exportTitle,
}: {
  data: { name: string; value: number; color?: string }[];
  height?: number;
  centerLabel?: string;
  /** Affiche une légende sous l'anneau (activée par défaut). */
  legend?: boolean;
  exportTitle?: string;
}) {
  const total = data.reduce((a, b) => a + b.value, 0);
  return (
    <EChart
      height={height}
      exportTitle={exportTitle}
      option={{
        tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
        title: centerLabel
          ? { text: centerLabel, left: "center", top: legend ? "40%" : "center", textStyle: { fontSize: 11, color: "#64748b" } }
          : undefined,
        legend: legend
          ? {
              show: true,
              type: "scroll",
              bottom: 0,
              icon: "circle",
              itemWidth: 10,
              itemHeight: 10,
              itemGap: 12,
              textStyle: { fontSize: 10.5, color: "#334155", fontWeight: 600 },
              data: data.map((d) => d.name),
            }
          : { show: false },
        series: [
          {
            type: "pie",
            radius: ["52%", "76%"],
            center: ["50%", legend ? "44%" : "50%"],
            avoidLabelOverlap: true,
            itemStyle: { borderRadius: 3, borderColor: "#fff", borderWidth: 2 },
            label: { show: false },
            labelLine: { show: false },
            data: data.map((d) => ({ name: d.name, value: d.value, itemStyle: d.color ? { color: d.color } : undefined })),
          },
        ],
        graphic: total === 0 ? { type: "text", left: "center", top: legend ? "40%" : "center", style: { text: "—", fill: "#cbd5e1", fontSize: 16 } } : undefined,
      }}
    />
  );
}
