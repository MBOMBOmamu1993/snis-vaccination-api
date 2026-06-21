"use client";

import type { EChartsCoreOption } from "echarts/core";
import EChart from "@/components/charts/EChart";
import { Card, CardHeader } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtPct, fmtNum } from "@/lib/client/format";
import { usePevData } from "@/lib/pev/useData";
import { aggregate, groupBy, cleanName, ymLabel } from "@/lib/pev/calc";
import { ScopeNote, Loading } from "./_shared";

export function QualiteCompletude() {
  const { records, groupKey, level, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection. Choisissez une province et/ou une période." />;

  const total = aggregate(records);
  const groups = groupBy(records, groupKey);
  const names = groups.map((g) => cleanName(g.name));
  const comp = groups.map((g) => +g.agg.comp.toFixed(1));
  const prompt = groups.map((g) => +g.agg.prompt.toFixed(1));

  const bar: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 2, data: ["Complétude", "Promptitude"], textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 24, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "value", max: 100, axisLabel: { formatter: "{value}%" } },
    yAxis: { type: "category", data: names, axisLabel: { fontSize: names.length > 18 ? 8 : 10 } },
    series: [
      { name: "Complétude", type: "bar", data: comp, itemStyle: { color: "#0093d5", borderRadius: [0, 3, 3, 0] } },
      { name: "Promptitude", type: "bar", data: prompt, itemStyle: { color: "#f59e0b", borderRadius: [0, 3, 3, 0] } },
    ],
  };

  const rows = groups.map((g) => ({
    Entité: cleanName(g.name),
    "Structures attendues": g.agg.n,
    "Rapports reçus": g.agg.rap,
    "Complétude %": +g.agg.comp.toFixed(1),
    "Promptitude %": +g.agg.prompt.toFixed(1),
  }));
  const cols = ["Entité", "Structures attendues", "Rapports reçus", "Complétude %", "Promptitude %"];

  return (
    <div className="space-y-4">
      <ScopeNote />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard label="Complétude" value={fmtPct(total.comp, 1)} tone={total.comp >= 80 ? "good" : total.comp >= 50 ? "warn" : "bad"} icon="form" />
        <KpiCard label="Promptitude" value={fmtPct(total.prompt, 1)} tone={total.prompt >= 80 ? "good" : total.prompt >= 50 ? "warn" : "bad"} icon="time" />
        <KpiCard label="Structures attendues" value={fmtNum(total.n)} tone="navy" icon="hospital" />
        <KpiCard label="Rapports reçus" value={fmtNum(total.rap)} tone="brand" icon="doc" />
      </div>
      <Card>
        <CardHeader title="Complétude & promptitude" subtitle={`Par ${level === "province" ? "province" : "zone de santé"} (DHIS2)`} icon="bars" iconTone="blue" />
        <EChart option={bar} height={Math.max(280, names.length * 22)} exportTitle="Completude_promptitude"
          exportData={{ columns: ["Entité", "Complétude %", "Promptitude %"], rows: groups.map((g, i) => [names[i], comp[i], prompt[i]]) }} />
      </Card>
      <Card>
        <CardHeader title="Détail par entité" icon="table" iconTone="navy" />
        <DataTable columns={cols} rows={rows} maxRows={100} exportFilename="completude_promptitude" />
      </Card>
    </div>
  );
}

export function QualiteTendance() {
  const { records, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  const months = Array.from(new Set(records.map((r) => r._YM))).sort();
  const byMonth = months.map((m) => aggregate(records.filter((r) => r._YM === m)));
  const labels = months.map(ymLabel);

  const line: EChartsCoreOption = {
    tooltip: { trigger: "axis" },
    legend: { top: 2, data: ["Complétude", "Promptitude"], textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 16, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: labels, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", max: 100, axisLabel: { formatter: "{value}%" } },
    series: [
      { name: "Complétude", type: "line", smooth: true, data: byMonth.map((a) => +a.comp.toFixed(1)), lineStyle: { width: 3 }, itemStyle: { color: "#0093d5" }, areaStyle: { opacity: 0.06 } },
      { name: "Promptitude", type: "line", smooth: true, data: byMonth.map((a) => +a.prompt.toFixed(1)), lineStyle: { width: 3 }, itemStyle: { color: "#f59e0b" } },
    ],
  };

  return (
    <div className="space-y-4">
      <ScopeNote />
      <Card>
        <CardHeader title="Évolution mensuelle de la qualité des données" subtitle="Complétude & promptitude par mois (DHIS2)" icon="time" iconTone="blue" />
        <EChart option={line} height={360} exportTitle="Tendance_qualite"
          exportData={{ columns: ["Mois", "Complétude %", "Promptitude %"], rows: months.map((m, i) => [labels[i], +byMonth[i].comp.toFixed(1), +byMonth[i].prompt.toFixed(1)]) }} />
      </Card>
    </div>
  );
}
