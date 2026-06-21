"use client";

import { useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { DIcon } from "@/components/dashboard/icons";
import { loadMeta, loadProvince, loadZs, useAsync } from "@/lib/pev/data";
import { useFilters } from "@/lib/pev/store";
import { cleanName, ymLabel } from "@/lib/pev/calc";
import { computeRevueData, generateRevuePptx } from "@/lib/pev/pptx";
import { ScopeNote } from "./_shared";

export function TelechargerCanevas() {
  const f = useFilters();
  const { data: meta } = useAsync(loadMeta, []);
  const { data: prov } = useAsync(loadProvince, []);
  const { data: zs } = useAsync(async () => (f.province ? loadZs() : Promise.resolve(null)), [f.province]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const ready = !!meta && (f.province ? !!zs : !!prov);

  async function onDownload() {
    if (!meta) return;
    setBusy(true); setMsg(null);
    try {
      const base = f.province ? (zs ?? []) : (prov ?? []);
      const data = computeRevueData(base, f, meta.months, !!f.province);
      await generateRevuePptx(data);
      setMsg("Téléchargement lancé — canevas PPTX éditable généré avec les données DHIS2.");
    } catch (e) {
      setMsg(`Erreur : ${String((e as Error)?.message ?? e)}`);
    } finally {
      setBusy(false);
    }
  }

  const periodLabel = f.months.length ? `${ymLabel(f.months[0])} → ${ymLabel(f.months[f.months.length - 1])}` : "Toutes les périodes disponibles";

  return (
    <div className="space-y-4">
      <ScopeNote />
      <Card>
        <CardHeader title="Canevas de revue formative PEV — génération automatique (PPTX)" subtitle="Diapos 10 à 12 renseignées dynamiquement avec les données DHIS2 de la période filtrée" icon="report" iconTone="orange" />
        <div className="grid grid-cols-1 gap-5 md:grid-cols-2">
          <div className="rounded-[14px] border border-surface-200 bg-[#f6f8fb] p-5">
            <h3 className="mb-3 flex items-center gap-2 text-[14px] font-extrabold text-navy-700"><DIcon name="info" style={{ width: 18, height: 18, color: "#0093d5" }} /> Contenu renseigné</h3>
            <ul className="space-y-2 text-[13px] text-surface-700">
              {[
                "Page de garde : province et période filtrées",
                "Diapo 10 : séances de vaccination prévues/réalisées (fixe, avancée, mobile) — DHIS2, période n-1 / période courante",
                "Diapo 11 : complétude, promptitude & doses (DTC3, VAR1) par zone de santé",
                "Diapo 12 : taux d'abandon DTC1–DTC3 et BCG–VAR1 par zone de santé",
              ].map((t) => (
                <li key={t} className="flex items-start gap-2.5">
                  <span className="mt-0.5 flex h-[20px] w-[20px] shrink-0 items-center justify-center rounded-full bg-[#e7f6ec] text-[#178a44]"><DIcon name="checkmark" style={{ width: 12, height: 12 }} strokeWidth={2.6} /></span>
                  <span>{t}</span>
                </li>
              ))}
            </ul>
            <p className="mt-4 text-[11.5px] italic leading-snug text-surface-500">Le fichier conserve la mise en forme du canevas officiel et reste <b>entièrement éditable</b> dans PowerPoint. Toutes les valeurs proviennent du DHIS2 pour la période et le périmètre filtrés.</p>
          </div>

          <div className="flex flex-col items-center justify-center rounded-[14px] border border-surface-200 bg-white p-6 text-center">
            <div className="mb-3 flex h-[72px] w-[72px] items-center justify-center rounded-2xl text-white" style={{ background: "linear-gradient(145deg,#fbbf24,#f08c00)" }}>
              <DIcon name="download" style={{ width: 36, height: 36 }} strokeWidth={2} />
            </div>
            <div className="text-[13px] font-bold text-navy-700">{f.province ? cleanName(f.province) : "Toutes les provinces"}</div>
            <div className="text-[12px] text-surface-600">{periodLabel}</div>
            <button type="button" onClick={onDownload} disabled={busy || !ready}
              className="mt-5 inline-flex items-center gap-2.5 rounded-[12px] px-7 py-3 text-[15px] font-extrabold uppercase tracking-wide text-white transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50"
              style={{ background: "linear-gradient(120deg,#00205c,#15479e)", boxShadow: "0 14px 28px -14px rgba(0,32,92,.7)" }}>
              <DIcon name="download" style={{ width: 18, height: 18 }} strokeWidth={2.2} />
              {busy ? "Génération en cours…" : !ready ? "Chargement des données…" : "Télécharger en PPTX éditable"}
            </button>
            {msg ? <div className="mt-4 text-[12px] font-semibold text-surface-700">{msg}</div> : null}
          </div>
        </div>
      </Card>
    </div>
  );
}
