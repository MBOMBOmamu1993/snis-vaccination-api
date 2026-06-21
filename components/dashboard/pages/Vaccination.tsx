"use client";

import type { EChartsCoreOption } from "echarts/core";
import EChart from "@/components/charts/EChart";
import { Card, CardHeader } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtNum, fmtPct } from "@/lib/client/format";
import { TRACERS } from "@/lib/pev/data";
import { usePevData } from "@/lib/pev/useData";
import { aggregate, groupBy, dropout, cleanName, ymLabel } from "@/lib/pev/calc";
import { ScopeNote, Loading } from "./_shared";

export function VaccinationAntigenes() {
  const { records, groupKey, level, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  const total = aggregate(records);
  const ags = TRACERS;
  const bar: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: ags.map((a) => a.label), axisLabel: { rotate: 45, fontSize: 9 } },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => fmtNum(v) } },
    series: [{ type: "bar", data: ags.map((a) => total.doses[a.key] ?? 0), itemStyle: { color: "#0a3a86", borderRadius: [3, 3, 0, 0] } }],
  };

  const groups = groupBy(records, groupKey);
  const keyAgs = ["BCG", "DTC1", "DTC3", "VAR1"];
  const cols = ["Entité", ...keyAgs.map((k) => TRACERS.find((t) => t.key === k)!.label)];
  const rows = groups.map((g) => {
    const r: Record<string, string | number> = { Entité: cleanName(g.name) };
    for (const k of keyAgs) r[TRACERS.find((t) => t.key === k)!.label] = g.agg.doses[k] ?? 0;
    return r;
  });

  return (
    <div className="space-y-4">
      <ScopeNote />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard label="Doses BCG" value={fmtNum(total.doses.BCG)} tone="navy" icon="syringe" />
        <KpiCard label="Doses DTC1 (Penta1)" value={fmtNum(total.doses.DTC1)} tone="brand" icon="syringe" />
        <KpiCard label="Doses DTC3 (Penta3)" value={fmtNum(total.doses.DTC3)} tone="good" icon="syringe" />
        <KpiCard label="Doses VAR1" value={fmtNum(total.doses.VAR1)} tone="violet" icon="syringe" />
      </div>
      <Card>
        <CardHeader title="Doses administrées par antigène" subtitle="0–11 mois (DHIS2)" icon="bars" iconTone="navy" />
        <EChart option={bar} height={340} exportTitle="Doses_par_antigene"
          exportData={{ columns: ["Antigène", "Doses"], rows: ags.map((a) => [a.label, total.doses[a.key] ?? 0]) }} />
      </Card>
      <Card>
        <CardHeader title={`Doses par ${level === "province" ? "province" : "zone de santé"}`} icon="table" iconTone="navy" />
        <DataTable columns={cols} rows={rows} maxRows={100} exportFilename="doses_antigenes" />
      </Card>
    </div>
  );
}

export function VaccinationAbandon() {
  const { records, groupKey, level, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  const total = aggregate(records);
  const doDtc = dropout(total.doses.DTC1, total.doses.DTC3);
  const doBcgVar = dropout(total.doses.BCG, total.doses.VAR1);

  const groups = groupBy(records, groupKey);
  const names = groups.map((g) => cleanName(g.name));
  const dDtc = groups.map((g) => +dropout(g.agg.doses.DTC1, g.agg.doses.DTC3).toFixed(1));
  const dBcg = groups.map((g) => +dropout(g.agg.doses.BCG, g.agg.doses.VAR1).toFixed(1));

  const bar: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 2, data: ["Abandon DTC1–DTC3", "Abandon BCG–VAR1"], textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 24, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "value", axisLabel: { formatter: "{value}%" } },
    yAxis: { type: "category", data: names, axisLabel: { fontSize: names.length > 18 ? 8 : 10 } },
    series: [
      { name: "Abandon DTC1–DTC3", type: "bar", data: dDtc, itemStyle: { color: "#c81e1e", borderRadius: [0, 3, 3, 0] } },
      { name: "Abandon BCG–VAR1", type: "bar", data: dBcg, itemStyle: { color: "#f59e0b", borderRadius: [0, 3, 3, 0] } },
    ],
  };

  const cols = ["Entité", "Abandon DTC1–DTC3 %", "Abandon BCG–VAR1 %"];
  const rows = groups.map((g, i) => ({ "Entité": names[i], "Abandon DTC1–DTC3 %": dDtc[i], "Abandon BCG–VAR1 %": dBcg[i] }));

  return (
    <div className="space-y-4">
      <ScopeNote />
      <div className="grid grid-cols-2 gap-3">
        <KpiCard label="Taux d'abandon DTC1–DTC3" value={fmtPct(doDtc, 1)} tone={doDtc <= 10 ? "good" : doDtc <= 20 ? "warn" : "bad"} icon="down" />
        <KpiCard label="Taux d'abandon BCG–VAR1" value={fmtPct(doBcgVar, 1)} tone={doBcgVar <= 10 ? "good" : doBcgVar <= 20 ? "warn" : "bad"} icon="down" />
      </div>
      <Card>
        <CardHeader title={`Taux d'abandon par ${level === "province" ? "province" : "zone de santé"}`} subtitle="Plus le taux est bas, mieux c'est (DHIS2)" icon="bars" iconTone="red" />
        <EChart option={bar} height={Math.max(280, names.length * 22)} exportTitle="Taux_abandon"
          exportData={{ columns: cols, rows: groups.map((g, i) => [names[i], dDtc[i], dBcg[i]]) }} />
      </Card>
      <Card>
        <CardHeader title="Détail des taux d'abandon" icon="table" iconTone="navy" />
        <DataTable columns={cols} rows={rows} maxRows={100} exportFilename="taux_abandon" />
      </Card>
    </div>
  );
}

export function VaccinationCourbe() {
  const { records, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  const months = Array.from(new Set(records.map((r) => r._YM))).sort();
  const labels = months.map(ymLabel);
  const series = (["BCG", "DTC1", "DTC3", "VAR1"] as const).map((k, idx) => ({
    name: TRACERS.find((t) => t.key === k)!.label,
    type: "line" as const, smooth: true, lineStyle: { width: 3 },
    itemStyle: { color: ["#00205c", "#0093d5", "#178a44", "#7c3aed"][idx] },
    data: months.map((m) => aggregate(records.filter((r) => r._YM === m)).doses[k] ?? 0),
  }));

  const line: EChartsCoreOption = {
    tooltip: { trigger: "axis" },
    legend: { top: 2, textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 16, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: labels, axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => fmtNum(v) } },
    series,
  };

  return (
    <div className="space-y-4">
      <ScopeNote />
      <Card>
        <CardHeader title="Courbe de suivi des doses administrées" subtitle="Évolution mensuelle par antigène traceur (DHIS2)" icon="time" iconTone="navy" />
        <EChart option={line} height={380} exportTitle="Courbe_suivi"
          exportData={{ columns: ["Mois", ...series.map((s) => s.name)], rows: months.map((m, i) => [labels[i], ...series.map((s) => s.data[i])]) }} />
      </Card>
    </div>
  );
}
