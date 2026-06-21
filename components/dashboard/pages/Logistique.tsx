"use client";

import type { EChartsCoreOption } from "echarts/core";
import EChart from "@/components/charts/EChart";
import { Card, CardHeader, SectionBar } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtPct, fmtNum } from "@/lib/client/format";
import { TRACERS, num } from "@/lib/pev/data";
import { usePevData } from "@/lib/pev/useData";
import { cleanName } from "@/lib/pev/calc";
import { ScopeNote, Loading } from "./_shared";

export function LogistiqueStrategie() {
  const { records, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  // Stratégie fixe vs avancée+mobile : par antigène, doses _fixe et doses totales.
  const ags = TRACERS;
  const fixe: number[] = [], avm: number[] = [];
  let totFixe = 0, totAll = 0;
  for (const a of ags) {
    let tf = 0, tt = 0;
    for (const r of records) { tf += num(r[`${a.field}_fixe`]); tt += num(r[a.field]); }
    fixe.push(tf); avm.push(Math.max(0, tt - tf)); totFixe += tf; totAll += tt;
  }
  const fixePct = totAll ? (totFixe / totAll) * 100 : 0;

  const stack: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 2, data: ["Stratégie fixe", "Avancée + mobile"], textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 16, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: ags.map((a) => a.label), axisLabel: { rotate: 45, fontSize: 9 } },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => fmtNum(v) } },
    series: [
      { name: "Stratégie fixe", type: "bar", stack: "s", data: fixe, itemStyle: { color: "#00205c" } },
      { name: "Avancée + mobile", type: "bar", stack: "s", data: avm, itemStyle: { color: "#0093d5" } },
    ],
  };

  const cols = ["Antigène", "Doses fixe", "Doses avancée + mobile", "Total", "% fixe"];
  const rows = ags.map((a, i) => ({
    "Antigène": a.label, "Doses fixe": fixe[i], "Doses avancée + mobile": avm[i],
    "Total": fixe[i] + avm[i], "% fixe": +((fixe[i] + avm[i]) ? (fixe[i] / (fixe[i] + avm[i])) * 100 : 0).toFixed(1),
  }));

  return (
    <div className="space-y-4">
      <ScopeNote />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-3">
        <KpiCard label="Part stratégie fixe" value={fmtPct(fixePct, 1)} tone="teal" icon="hospital" />
        <KpiCard label="Doses en stratégie fixe" value={fmtNum(totFixe)} tone="navy" icon="syringe" />
        <KpiCard label="Doses avancée + mobile" value={fmtNum(totAll - totFixe)} tone="brand" icon="truck" />
      </div>
      <Card>
        <CardHeader title="Stratégie de vaccination par antigène" subtitle="Répartition fixe vs avancée + mobile (DHIS2)" icon="truck" iconTone="teal" />
        <EChart option={stack} height={360} exportTitle="Strategie_vaccination"
          exportData={{ columns: ["Antigène", "Fixe", "Avancée + mobile"], rows: ags.map((a, i) => [a.label, fixe[i], avm[i]]) }} />
      </Card>
      <Card>
        <CardHeader title="Détail par antigène" icon="table" iconTone="teal" />
        <DataTable columns={cols} rows={rows} maxRows={50} exportFilename="strategie_vaccination" />
      </Card>
      <SectionBar icon="alert">
        Le suivi détaillé des stocks d'intrants (réceptions, utilisations, pertes, ruptures) provient du module logistique DHIS2 et sera affiché ici dès son intégration au pipeline de backfill.
      </SectionBar>
    </div>
  );
}
