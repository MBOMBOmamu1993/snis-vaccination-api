import type { AggRecord } from "./types";
import { num, TRACERS } from "./data";

export interface Filters {
  province: string;   // "" = toutes
  antenne: string;
  zs: string;
  months: string[];   // [] = toutes les périodes
}

export function filterRecords(records: AggRecord[], f: Filters): AggRecord[] {
  return records.filter((r) => {
    if (f.province && r._Province !== f.province) return false;
    if (f.antenne && r._Antenne !== f.antenne) return false;
    if (f.zs && r._ZS !== f.zs) return false;
    if (f.months.length && !f.months.includes(r._YM)) return false;
    return true;
  });
}

export interface AggSummary {
  n: number; rap: number; comp: number; prompt: number;
  doses: Record<string, number>;
}

/** Agrège un jeu d'enregistrements. Les % de complétude/promptitude sont
 *  recombinés fidèlement (complétude = Σrap/Σn ; promptitude pondérée par n). */
export function aggregate(records: AggRecord[]): AggSummary {
  let n = 0, rap = 0, promptW = 0;
  const doses: Record<string, number> = {};
  for (const r of records) {
    n += num(r.n); rap += num(r.rap); promptW += num(r.prompt) * num(r.n);
    for (const t of TRACERS) doses[t.key] = (doses[t.key] ?? 0) + num(r[t.field]);
  }
  return {
    n, rap,
    comp: n ? (rap / n) * 100 : 0,
    prompt: n ? promptW / n : 0,
    doses,
  };
}

export const dropout = (a: number, b: number): number => (a > 0 ? ((a - b) / a) * 100 : 0);

/** Somme d'un champ arbitraire sur un jeu d'enregistrements (séances, logistique…). */
export const sumField = (records: AggRecord[], field: string): number => records.reduce((s, r) => s + num(r[field]), 0);

/** Nombre d'entités distinctes (AS/ZS/FOSA/province) ayant field > 0. */
export function countEntitiesWith(records: AggRecord[], field: string): number {
  const m = new Map<string, number>();
  for (const r of records) {
    const k = String(r._AS ?? r._ZS ?? r._Province ?? "—");
    m.set(k, (m.get(k) ?? 0) + num(r[field]));
  }
  return Array.from(m.values()).filter((v) => v > 0).length;
}

/** Regroupe par clé (province / ZS / antenne / mois) et agrège. */
export function groupBy(records: AggRecord[], key: keyof AggRecord): { name: string; agg: AggSummary; records: AggRecord[] }[] {
  const map = new Map<string, AggRecord[]>();
  for (const r of records) {
    const k = String(r[key] ?? "—");
    if (!map.has(k)) map.set(k, []);
    map.get(k)!.push(r);
  }
  return Array.from(map.entries())
    .map(([name, recs]) => ({ name, agg: aggregate(recs), records: recs }))
    .sort((a, b) => a.name.localeCompare(b.name, "fr"));
}

/** "tp Tshopo Province" → "Tshopo" ; "tp Banalia Zone de Santé" → "Banalia". */
export function cleanName(s: string): string {
  return String(s)
    .replace(/^[a-z]{2}\s+/i, "")
    .replace(/\s+(Province|Zone de Santé|Aire de Santé)$/i, "")
    .trim();
}

/** "202503" → "Mar 2025". */
const MFR = ["Jan", "Fév", "Mar", "Avr", "Mai", "Juin", "Juil", "Août", "Sep", "Oct", "Nov", "Déc"];
export function ymLabel(ym: string): string {
  const y = ym.slice(0, 4), m = parseInt(ym.slice(4, 6), 10);
  return `${MFR[m - 1] ?? ym.slice(4)} ${y}`;
}
