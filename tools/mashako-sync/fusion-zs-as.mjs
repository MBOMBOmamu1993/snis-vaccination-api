#!/usr/bin/env node
/** Fusion des tranches de l'export « détail par aire de santé ».
 *
 *  export-zs-as.mjs tourne en plusieurs exemplaires (MASHAKO_SHARD), chacun
 *  sur son propre profil Chrome et ses propres fichiers `_ASs<n>.json` — sans
 *  quoi les processus se réécrivent mutuellement. Ce script les recombine en
 *  un fichier par dashboard, prêt à publier.
 *
 *  Sortie : out-zs/views/<urlName>_AS.json + résumé de couverture.
 *  Usage  : node fusion-zs-as.mjs
 */
import { readFileSync, writeFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const VUES = path.join(process.env.MASHAKO_AS_OUT || path.join(HERE, "out-zs"), "views");
const log = (m) => console.log(m);

const parVue = {};
for (const f of readdirSync(VUES)) {
  const m = /^(.+)_AS_s\d+\.json$/.exec(f);
  if (!m) continue;
  let j;
  try { j = JSON.parse(readFileSync(path.join(VUES, f), "utf8")); } catch (e) { log(`⚠ ${f} illisible`); continue; }
  const v = parVue[m[1]] || (parVue[m[1]] = { name: j.name, urlName: j.urlName, source: j.source, period: j.period, columns: [], rows: [] });
  for (const c of j.columns || []) if (!v.columns.includes(c)) v.columns.push(c);
  /* Une zone ne peut venir que d'une tranche : pas de doublon à arbitrer, on
     concatène. On dédoublonne tout de même par (zone, rôle, aire) au cas où
     une tranche aurait été relancée avec un autre découpage. */
  v.rows.push(...(j.rows || []));
}

let total = 0;
for (const [u, v] of Object.entries(parVue)) {
  const vu = new Set();
  v.rows = v.rows.filter((r) => {
    const k = `${r._ZS}|${r._ROLE}|${(r._AS || "").toLowerCase()}`;
    if (vu.has(k)) return false;
    vu.add(k); return true;
  });
  v.zones = [...new Set(v.rows.map((r) => r._ZS))].sort((a, b) => a.localeCompare(b, "fr"));
  v.generated_at = new Date().toISOString();
  writeFileSync(path.join(VUES, `${u}_AS.json`), JSON.stringify(v));
  const nAS = v.rows.filter((r) => r._ROLE === "AS").length;
  log(`✓ ${u}_AS.json : ${v.zones.length} zone(s), ${v.rows.length} lignes (${nAS} détail AS), ${v.columns.length} colonnes`);
  total += v.rows.length;
}
log(`— ${Object.keys(parVue).length} feuille(s), ${total} lignes —`);
