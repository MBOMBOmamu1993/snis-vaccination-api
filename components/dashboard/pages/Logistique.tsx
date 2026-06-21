"use client";

import type { EChartsCoreOption } from "echarts/core";
import EChart from "@/components/charts/EChart";
import { Card, CardHeader } from "@/components/ui/Card";
import { KpiCard } from "@/components/ui/KpiCard";
import { DataTable } from "@/components/ui/DataTable";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtPct, fmtNum } from "@/lib/client/format";
import { LOGI_VACCINS, LOGI_FIELDS } from "@/lib/pev/data";
import { usePevData } from "@/lib/pev/useData";
import { sumField } from "@/lib/pev/calc";
import { ScopeNote, Loading } from "./_shared";

export function LogistiqueStrategie() {
  const { records, loading } = usePevData();
  if (loading) return <Loading />;
  if (!records.length) return <EmptyState message="Aucune donnée pour la sélection." />;

  // Mouvements de stock par vaccin (DHIS2 — logistique).
  const data = LOGI_VACCINS.map((v) => {
    const recues = sumField(records, `${v.prefix}_${LOGI_FIELDS.recues}`);
    const utilisees = sumField(records, `${v.prefix}_${LOGI_FIELDS.utilisees}`);
    const pertes = sumField(records, `${v.prefix}_${LOGI_FIELDS.pertes}`);
    const stockFin = sumField(records, `${v.prefix}_${LOGI_FIELDS.stockFin}`);
    const joursRupture = sumField(records, `${v.prefix}_${LOGI_FIELDS.joursRupture}`);
    const tauxPerte = utilisees + pertes ? (pertes / (utilisees + pertes)) * 100 : 0;
    return { ...v, recues, utilisees, pertes, stockFin, joursRupture, tauxPerte: +tauxPerte.toFixed(1) };
  });

  const totUtil = data.reduce((s, d) => s + d.utilisees, 0);
  const totPertes = data.reduce((s, d) => s + d.pertes, 0);
  const totRupture = data.reduce((s, d) => s + d.joursRupture, 0);
  const tauxPerteGlobal = totUtil + totPertes ? (totPertes / (totUtil + totPertes)) * 100 : 0;

  const barPerte: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (v: number) => `${v}%` },
    grid: { left: 8, right: 24, top: 16, bottom: 8, containLabel: true },
    xAxis: { type: "value", axisLabel: { formatter: "{value}%" } },
    yAxis: { type: "category", data: data.map((d) => d.label) },
    series: [{
      type: "bar", data: data.map((d) => d.tauxPerte),
      itemStyle: { color: "#0d9488", borderRadius: [0, 3, 3, 0] },
      markLine: { silent: true, symbol: "none", lineStyle: { color: "#c81e1e", type: "dashed" }, data: [{ xAxis: 5, name: "Seuil 5%" }], label: { formatter: "Seuil 5%", fontSize: 9 } },
    }],
  };

  const barMouv: EChartsCoreOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 2, data: ["Reçues", "Utilisées", "Pertes"], textStyle: { fontSize: 11 } },
    grid: { left: 8, right: 16, top: 34, bottom: 8, containLabel: true },
    xAxis: { type: "category", data: data.map((d) => d.label), axisLabel: { fontSize: 10 } },
    yAxis: { type: "value", axisLabel: { formatter: (v: number) => fmtNum(v) } },
    series: [
      { name: "Reçues", type: "bar", data: data.map((d) => d.recues), itemStyle: { color: "#0093d5" } },
      { name: "Utilisées", type: "bar", data: data.map((d) => d.utilisees), itemStyle: { color: "#00205c" } },
      { name: "Pertes", type: "bar", data: data.map((d) => d.pertes), itemStyle: { color: "#f59e0b" } },
    ],
  };

  const cols = ["Vaccin", "Reçues", "Utilisées", "Pertes", "Taux de perte %", "Stock fin", "Jours de rupture"];
  const rows = data.map((d) => ({
    "Vaccin": d.label, "Reçues": d.recues, "Utilisées": d.utilisees, "Pertes": d.pertes,
    "Taux de perte %": d.tauxPerte, "Stock fin": d.stockFin, "Jours de rupture": d.joursRupture,
  }));

  return (
    <div className="space-y-4">
      <ScopeNote />
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCard label="Taux de perte global" value={fmtPct(tauxPerteGlobal, 1)} tone={tauxPerteGlobal <= 5 ? "good" : tauxPerteGlobal <= 10 ? "warn" : "bad"} icon="alert" />
        <KpiCard label="Doses utilisées" value={fmtNum(totUtil)} tone="navy" icon="syringe" />
        <KpiCard label="Doses perdues" value={fmtNum(totPertes)} tone="warn" icon="down" />
        <KpiCard label="Jours de rupture (cumulés)" value={fmtNum(totRupture)} tone={totRupture === 0 ? "good" : "bad"} icon="calendar" />
      </div>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader title="Taux de perte par vaccin" subtitle="Pertes / (utilisées + pertes) — seuil OMS 5 % (DHIS2)" icon="bars" iconTone="teal" />
          <EChart option={barPerte} height={320} exportTitle="Taux_perte_vaccin"
            exportData={{ columns: ["Vaccin", "Taux de perte %"], rows: data.map((d) => [d.label, d.tauxPerte]) }} />
        </Card>
        <Card>
          <CardHeader title="Mouvements de stock par vaccin" subtitle="Reçues · utilisées · pertes (DHIS2)" icon="truck" iconTone="blue" />
          <EChart option={barMouv} height={320} exportTitle="Mouvements_stock"
            exportData={{ columns: cols.slice(0, 4), rows: data.map((d) => [d.label, d.recues, d.utilisees, d.pertes]) }} />
        </Card>
      </div>
      <Card>
        <CardHeader title="Gestion des intrants vaccinaux" subtitle="Détail par vaccin (DHIS2)" icon="table" iconTone="teal" />
        <DataTable columns={cols} rows={rows} maxRows={50} exportFilename="logistique_intrants" />
      </Card>
    </div>
  );
}
