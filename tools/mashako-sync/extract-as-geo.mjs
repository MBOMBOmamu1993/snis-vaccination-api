#!/usr/bin/env node
/** Extraction des points (centroïdes) des aires de santé par zone de santé.
 *  Source : C:/Users/felly/Downloads/RDC_aires_de_sante.geojson (59,8 Mo,
 *  MultiPolygones, props id=UID DHIS2 + na) — fichier partagé par Felly.
 *  Jointure hiérarchique : docs/data_as/ou_map_as.json (UID → Org3 = ZS
 *  complète « hu Aba Zone de Santé », Org4 = AS complète) — le préfixe
 *  2 lettres n'est PAS unique (31 préfixes), d'où la jointure par UID.
 *  Sortie : docs/data/mashako/geo_as_points.json = { "Aba": [["Ataki",lon,lat],…] }
 *  (noms courts Mashako, coordonnées arrondies à 4 décimales).
 *  Usage : node extract-as-geo.mjs  (depuis mashako-sync/)
 */
import { readFileSync, writeFileSync } from "node:fs";

const GEO = "C:/Users/felly/Downloads/RDC_aires_de_sante.geojson";
const OUMAP = "C:/Users/felly/snis-vaccination-api/docs/data_as/ou_map_as.json";
const OUT = "C:/Users/felly/snis-vaccination-api/docs/data/mashako/geo_as_points.json";

/** « hu Aba Zone de Santé » → « Aba » ; « bu Aboso Aire de Santé » → « Aboso » */
function nomCourt(full) {
  let s = String(full || "").trim();
  s = s.replace(/^\S{2}\s+/, "");                       // préfixe code 2 lettres
  s = s.replace(/\s+(Zone de Sant[ée]|Aire de Sant[ée])\s*$/i, "");
  return s.trim();
}

/** centroïde du plus grand anneau extérieur (MultiPolygon) — formule de l'aire */
function centroide(geom) {
  if (!geom) return null;
  const polys = geom.type === "MultiPolygon" ? geom.coordinates
    : geom.type === "Polygon" ? [geom.coordinates] : null;
  if (!polys) return null;
  let best = null, bestA = -1;
  for (const poly of polys) {
    const ring = poly && poly[0];
    if (!ring || ring.length < 3) continue;
    let a = 0, cx = 0, cy = 0;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [x0, y0] = ring[j], [x1, y1] = ring[i];
      const w = x0 * y1 - x1 * y0;
      a += w; cx += (x0 + x1) * w; cy += (y0 + y1) * w;
    }
    if (Math.abs(a) > bestA) { bestA = Math.abs(a); best = a ? [cx / (3 * a), cy / (3 * a)] : ring[0]; }
  }
  return best;
}

const ou = JSON.parse(readFileSync(OUMAP, "utf8"));
const gj = JSON.parse(readFileSync(GEO, "utf8"));
const parZS = {};
let sansOu = 0, sansParent = 0;
for (const f of gj.features) {
  const id = f.properties && f.properties.id;
  const meta = id ? ou[id] : null;
  if (!meta) { sansOu++; continue; }
  const zs = nomCourt(meta.Org3);
  const as = nomCourt(meta.Org4 || (f.properties && f.properties.na));
  if (!zs || !as) { sansParent++; continue; }
  const c = centroide(f.geometry);
  if (!c) continue;
  (parZS[zs] = parZS[zs] || []).push([as, Math.round(c[0] * 1e4) / 1e4, Math.round(c[1] * 1e4) / 1e4]);
}
for (const zs of Object.keys(parZS)) parZS[zs].sort((a, b) => a[0].localeCompare(b[0], "fr"));
writeFileSync(OUT, JSON.stringify(parZS));

const total = Object.values(parZS).reduce((n, l) => n + l.length, 0);
console.log("ZS couvertes :", Object.keys(parZS).length, "| points AS :", total,
  "| sans correspondance ou_map :", sansOu, "| sans parent :", sansParent);
for (const z of ["Aketi", "Aba", "Buta"]) {
  const l = parZS[z] || [];
  console.log(`${z} : ${l.length} AS — ${l.slice(0, 5).map((p) => p[0]).join(", ")}…`);
}
console.log("écrit :", OUT);
