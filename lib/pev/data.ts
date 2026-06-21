"use client";

import { useEffect, useState } from "react";
import type { AggRecord, Meta } from "./types";

/* Les données pré-agrégées (DHIS2 → backfill → aggregate) sont servies en
   STATIQUE depuis /data (copie de docs/data au build, cf. scripts/prepare-data).
   On les décompresse DANS LE NAVIGATEUR (DecompressionStream « gzip ») — exactement
   comme l'ancien dashboard avec pako : aucune charge serveur, affichage par
   province et période, des milliers d'enregistrements sans planter le serveur. */
const BASE = "/data/dashboard";

async function gunzipJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { cache: "force-cache" });
  if (!res.ok) throw new Error(`HTTP ${res.status} — ${url}`);
  // Fichiers .json simples (meta) : pas de décompression.
  if (url.endsWith(".json")) return (await res.json()) as T;
  const ds = new DecompressionStream("gzip");
  const stream = (res.body as ReadableStream<Uint8Array>).pipeThrough(ds);
  const text = await new Response(stream).text();
  return JSON.parse(text) as T;
}

/* Cache module : chaque fichier n'est téléchargé / décompressé qu'une fois. */
const cache = new Map<string, Promise<unknown>>();
function once<T>(key: string, loader: () => Promise<T>): Promise<T> {
  if (!cache.has(key)) cache.set(key, loader());
  return cache.get(key) as Promise<T>;
}

export const loadMeta = () => once<Meta>("meta", () => gunzipJson<Meta>(`${BASE}/meta.json`));
export const loadProvince = () => once<AggRecord[]>("prov", () => gunzipJson<AggRecord[]>(`${BASE}/by_province.json.gz`));
/** by_zs.json.gz : niveau Zone de santé + Antenne (chargé à la demande, mis en
 *  cache) — utilisé pour la cascade Antenne/ZS et les vues par ZS. */
export const loadZs = () => once<AggRecord[]>("zs", () => gunzipJson<AggRecord[]>(`${BASE}/by_zs.json.gz`));

/* ---- Hook générique de chargement ---- */
export function useAsync<T>(loader: () => Promise<T>, deps: unknown[] = []): { data: T | null; error: string | null; loading: boolean } {
  const [state, setState] = useState<{ data: T | null; error: string | null; loading: boolean }>({ data: null, error: null, loading: true });
  useEffect(() => {
    let alive = true;
    setState((s) => ({ ...s, loading: true }));
    loader()
      .then((data) => { if (alive) setState({ data, error: null, loading: false }); })
      .catch((e) => { if (alive) setState({ data: null, error: String(e?.message ?? e), loading: false }); });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return state;
}

/* ---- Antigènes traceurs (libellés issus de l'ancien dashboard) ---- */
export interface AntigenDef { key: string; field: string; label: string; }
export const TRACERS: AntigenDef[] = [
  { key: "BCG", field: "BCG_0_11", label: "BCG" },
  { key: "VPO0", field: "VPO0_0_11", label: "VPO0" },
  { key: "DTC1", field: "DTC1_0_11", label: "DTC1 (Penta1)" },
  { key: "DTC2", field: "DTC2_0_11", label: "DTC2 (Penta2)" },
  { key: "DTC3", field: "DTC3_0_11", label: "DTC3 (Penta3)" },
  { key: "VPO1", field: "VPO1_0_11", label: "VPO1" },
  { key: "VPO2", field: "VPO2_0_11", label: "VPO2" },
  { key: "VPO3", field: "VPO3_0_11", label: "VPO3" },
  { key: "VPI1", field: "VPI1_0_11", label: "VPI1" },
  { key: "VPI2", field: "VPI2_0_11", label: "VPI2" },
  { key: "ROTA1", field: "ROTA1_0_11", label: "ROTA1" },
  { key: "ROTA2", field: "ROTA2_0_11", label: "ROTA2" },
  { key: "ROTA3", field: "ROTA3_0_11", label: "ROTA3" },
  { key: "PCV13_1", field: "PCV13_1_0_11", label: "PCV13-1" },
  { key: "PCV13_2", field: "PCV13_2_0_11", label: "PCV13-2" },
  { key: "PCV13_3", field: "PCV13_3_0_11", label: "PCV13-3" },
  { key: "VAR1", field: "VAR1_0_11", label: "VAR1" },
  { key: "VAR2", field: "VAR2_12_23", label: "VAR2" },
  { key: "VAA", field: "VAA_0_11", label: "VAA" },
];

export const num = (v: string | number | undefined): number => {
  const n = typeof v === "number" ? v : v == null ? 0 : parseFloat(v);
  return Number.isFinite(n) ? n : 0;
};
