#!/usr/bin/env node
/** Valeurs EXACTES du filtre _SELECTED_location_level du classeur ZS.
 *
 *  ⚠ Le filtre n'accepte PAS le nom court de la zone de santé (« Aketi ») mais
 *  la chaîne composée « bu Aketi Zone de Santé », où le préfixe est le code
 *  province (bu = Bas-Uele, nk = Nord-Kivu, tp = Tshopo…). Avec une valeur
 *  invalide le dashboard ne rend rien et le dialogue crosstab répond 200 avec
 *  une liste de feuilles VIDE — c'est ce qui a fait croire pendant deux jours
 *  que la voie crosstab était morte sur ce classeur (28/07).
 *
 *  Source : le topojson des zones de santé déjà utilisé par le dashboard
 *  (docs/index.html, MK_TOPO_URL) — il porte les 519 noms complets, donc les
 *  codes de toutes les provinces. Les feuilles FILTER_VALUES ne conviennent
 *  pas : FILTER_VALUES_3 renvoie une copie de FILTER_VALUES (en .csv comme en
 *  tableau croisé), ce qui laisse 172 zones sans valeur.
 *
 *  Sortie : zs_filter_values.json { "Aketi": "bu Aketi Zone de Santé", … }
 */
import { writeFileSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const TOPO = process.env.MK_TOPO_URL ||
  "https://gist.githubusercontent.com/MBOMBOmamu1993/1297c206c046ee018d5ed6c392d6c20f/raw/24ce95b2935d2b4cc4ef71701138218ca870ff01/rdc_zs.topojson";
const FEATURE = "Zone de SantéRDC";
const log = (m) => console.log(`[${new Date().toISOString().slice(11, 19)}] ${m}`);

const topo = await (await fetch(TOPO)).json();
const geoms = (topo.objects?.[FEATURE]?.geometries) || [];
if (!geoms.length) { log(`✗ topojson illisible (objet « ${FEATURE} » absent)`); process.exit(1); }

const table = {};
for (const g of geoms) {
  const complet = g.properties?.name;
  if (!complet) continue;
  /* « bu Aketi Zone de Santé » → « Aketi » : le libellé court est celui que
     publie le dashboard et sur lequel le rendu filtrera les lignes. */
  const court = String(complet).replace(/^[a-z]{2,3}\s+/, "").replace(/\s*zones?\s+de\s+sant[ée]\s*$/i, "").trim();
  if (court) table[court] = complet;
}
const codes = [...new Set(Object.values(table).map((v) => v.split(" ")[0]))].sort();
log(`✓ ${Object.keys(table).length} zones de santé, ${codes.length} codes province : ${codes.join(" ")}`);

/* Contrôle : toutes les zones publiées doivent avoir une valeur de filtre. */
try {
  const meta = JSON.parse(readFileSync(path.join(HERE, "out-zs", "meta.json"), "utf8"));
  const manquants = (meta.antennes?.values || []).filter((z) => !table[z]);
  if (manquants.length) log(`⚠ ${manquants.length} zone(s) publiée(s) sans valeur : ${manquants.slice(0, 10).join(", ")}${manquants.length > 10 ? "…" : ""}`);
  else log("✓ toutes les zones publiées ont leur valeur de filtre");
} catch (e) { }

writeFileSync(path.join(HERE, "zs_filter_values.json"), JSON.stringify(table, null, 1));
log("→ zs_filter_values.json");
