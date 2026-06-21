"use client";

import EChart from "./EChart";

export default function Radar({
  indicators,
  entities,
  height = 300,
  exportTitle,
}: {
  indicators: string[];
  entities: { name: string; values: number[] }[];
  height?: number;
  exportTitle?: string;
}) {
  return (
    <EChart
      height={height}
      exportTitle={exportTitle}
      option={{
        color: ["#0093d5", "#22b457", "#7c3aed", "#f29e0b", "#e23636", "#0f766e", "#db2777", "#65a30d"],
        tooltip: { trigger: "item" },
        legend: { type: "scroll", bottom: 0, textStyle: { fontSize: 10 } },
        radar: {
          indicator: indicators.map((name) => ({ name, max: 100 })),
          radius: "62%",
          center: ["50%", "48%"],
          axisName: { fontSize: 9, color: "#475569" },
          splitArea: { areaStyle: { color: ["#fafcff", "#f1f5f9"] } },
        },
        series: [
          {
            type: "radar",
            data: entities.map((e) => ({ name: e.name, value: e.values, areaStyle: { opacity: 0.05 }, lineStyle: { width: 2 }, symbolSize: 3 })),
          },
        ],
      }}
    />
  );
}
