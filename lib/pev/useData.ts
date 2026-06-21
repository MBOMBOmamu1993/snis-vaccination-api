"use client";

import { useMemo } from "react";
import { loadProvince, loadZs, useAsync } from "./data";
import { useFilters } from "./store";
import { filterRecords } from "./calc";
import type { AggRecord } from "./types";

/**
 * Données filtrées pour les pages. Tant qu'aucune province n'est sélectionnée,
 * on travaille sur l'agrégat NATIONAL (by_province, léger) → comparaison entre
 * provinces. Dès qu'une province est choisie, on charge by_zs (à la demande) et
 * on descend au niveau Zone de santé. Cette logique « par province et période »
 * évite de charger des milliers d'enregistrements inutilement.
 */
export function usePevData() {
  const f = useFilters();
  const prov = useAsync(loadProvince, []);
  const zs = useAsync(async () => (f.province ? loadZs() : Promise.resolve(null)), [f.province]);

  const level = f.province ? "zs" : "province";
  const groupKey: keyof AggRecord = f.province ? (f.zs ? "_ZS" : "_ZS") : "_Province";

  const base: AggRecord[] = f.province ? (zs.data ?? []) : (prov.data ?? []);
  const records = useMemo(() => filterRecords(base, { province: f.province, antenne: f.antenne, zs: f.zs, months: f.months }), [base, f.province, f.antenne, f.zs, f.months]);

  return {
    records,
    level,
    groupKey,
    loading: f.province ? zs.loading || prov.loading : prov.loading,
    error: prov.error ?? zs.error,
    filters: f,
  };
}
