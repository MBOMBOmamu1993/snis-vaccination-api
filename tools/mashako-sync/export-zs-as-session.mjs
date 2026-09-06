#!/usr/bin/env node
/** DÉTAIL PAR AIRE DE SANTÉ du classeur ZS — version « sessions VizQL » (06/09/2026).
 *
 *  Même sortie que export-zs-as.mjs (fichiers <urlName>_AS<SUF>.json, lignes _ZS/_ROLE,
 *  colonnes « b<n>·… » par bloc du tableau croisé, consolidation par aire), mais au lieu
 *  d'une navigation Chrome par (dashboard, zone) — ~50-80 s de calcul serveur à chaque
 *  session neuve — on ouvre UNE session par dashboard et par travailleur, puis on change
 *  la zone par `tabdoc/categorical-filter` (~10-15 s) avant l'export croisé (~2 s).
 *  Mesuré le 06/09 : 56 s/export en navigation contre ~15 s ici, × MASHAKO_SESSIONS.
 *
 *  Garde-fou anti attribution croisée : les aires exportées pour une zone sont comparées
 *  à celles de l'archive de synthèse publiée (Vaccine_expiration_HZ_P1 de la période,
 *  colonne « Aire de Santé ») ; aucun recoupement alors qu'on en attend → la session est
 *  rouverte sur la zone et l'export rejoué ; désaccord persistant → ⚠ au journal, lignes
 *  écartées (jamais publiées sous une mauvaise zone).
 *
 *  Usage : node export-zs-as-session.mjs [Mois] [Année]
 *  Env   : MASHAKO_AS_OUT (dossier, déf. out-zs), MASHAKO_SESSIONS (déf. 6), MASHAKO_MINUTES
 *          (déf. 600), MASHAKO_ONLY (urlNames), MASHAKO_ZS (zones), MASHAKO_PROFILE
 *          (déf. browser-profile-as-session), MASHAKO_SUF (déf. _s0), MASHAKO_REPRISE=0.
 *  Les journaux zs_as_ledger_s*.json du même dossier sont lus : les couples déjà exportés
 *  par les tranches classiques ne sont pas refaits ; fusion-zs-as.mjs recombine le tout.
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readXlsx } from "./xlsx-lite.mjs";
import { surveiller, bailAutre } from "./cloud/lease.mjs";
import { launch, openSession, setZone, crosstabSheets } from "./vizql-lib.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const OUT = process.env.MASHAKO_AS_OUT || path.join(HERE, "out-zs");
const PROFILE = process.env.MASHAKO_PROFILE || path.join(HERE, "browser-profile-as-session");
const WB = "Mashako3_0RapportdelaZone";
const MOIS_FR = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];
const now = new Date();
const MONTH = process.argv[2] || process.env.MASHAKO_MONTH || MOIS_FR[now.getMonth()];
const YEAR = process.argv[3] || process.env.MASHAKO_YEAR || String(now.getFullYear());
const PER = `_PARAM_month=${encodeURIComponent(MONTH)}&_PARAM_year=${encodeURIComponent(YEAR)}`;
const KEY = `${YEAR}-${String(MOIS_FR.indexOf(MONTH) + 1).padStart(2, "0")}`;
const MAX_MINUTES = Number(process.env.MASHAKO_MINUTES || 600);
const P = Math.max(1, Math.min(Number(process.env.MASHAKO_SESSIONS || 6), 12)); // 8 → trop d'erreurs 400 sous charge (06/09)
const ONLY = (process.env.MASHAKO_ONLY || "").split(",").map((s) => s.trim()).filter(Boolean);
const ZS_ONLY = (process.env.MASHAKO_ZS || "").split(",").map((s) => s.trim()).filter(Boolean);
const SUF = process.env.MASHAKO_SUF || "_s0";
const REPRISE = process.env.MASHAKO_REPRISE !== "0";
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const IGNORE = /^(Cover Page|Contacts Page|FILTER_VALUES|KPI Manuel|Configuration|Performance |Carte de Supervision)/;
const EST_CIBLE = (n) => /aire\s*de\s*sant|airesante|_TABLE|_HZ_total/i.test(n);
const EST_TOTAL = (n) => /_HZ_total|_total$|_TABLE/i.test(n);
const EST_COL_AS = (c) => /·\s*aire de sant/i.test(c);
const urlnames = JSON.parse(readFileSync(path.join(HERE, "urlnames-zs.json"), "utf8"));
const THUMBS = readFileSync(path.join(HERE, "thumb-uris-zs.json"), "utf8");
let VUES = Object.entries(urlnames).filter(([k]) => !IGNORE.test(k));
if (ONLY.length) VUES = VUES.filter(([k, u]) => ONLY.includes(u) || ONLY.includes(k));
/* ordre : P1 avant P2 (le saut des P2 dépend du remplissage de la page 1), Supervision_HZ_P3 en dernier */
const rang = ([, u]) => [/^Supervision_HZ_P3$/i.test(u) ? 1 : 0, u.replace(/_P\d$|_NF$/i, ""), (/_P(\d)$/.exec(u) || [null, "0"])[1]];
VUES.sort((a, b) => { const x = rang(a), y = rang(b); return x[0] - y[0] || x[1].localeCompare(y[1]) || Number(x[2]) - Number(y[2]); });
const FILTRES = JSON.parse(readFileSync(path.join(HERE, "zs_filter_values.json"), "utf8"));
let ZONES = ZS_ONLY.length ? ZS_ONLY.filter((z) => FILTRES[z]) : Object.keys(FILTRES).sort((a, b) => a.localeCompare(b, "fr"));
if (!ZONES.length) { log("✗ aucune zone exploitable"); process.exit(1); }

mkdirSync(path.join(OUT, "views"), { recursive: true });
const LEDGER = path.join(OUT, `zs_as_ledger${SUF}.json`);
const cle = (u, z) => `${u}|${z}|${MONTH}-${YEAR}`;
const fait = {};
if (REPRISE) {
  for (const f of readdirSync(OUT).filter((f) => /^zs_as_ledger.*\.json$/.test(f))) {
    try { Object.assign(fait, JSON.parse(readFileSync(path.join(OUT, f), "utf8"))); } catch (e) { /* journal illisible */ }
  }
}
const faitIci = existsSync(LEDGER) && REPRISE ? JSON.parse(readFileSync(LEDGER, "utf8")) : {};
/* reprise : nos propres lignes + celles des tranches (pour le comptage d'aires des P1) */
const acc = {}; const autresRows = {};
for (const [label, urlName] of VUES) {
  for (const f of readdirSync(path.join(OUT, "views")).filter((f) => f.startsWith(`${urlName}_AS`) && f.endsWith(".json"))) {
    try {
      const j = JSON.parse(readFileSync(path.join(OUT, "views", f), "utf8"));
      if (j.period !== `${MONTH} ${YEAR}`) continue;
      if (f === `${urlName}_AS${SUF}.json`) acc[urlName] = { name: label, columns: j.columns || [], rows: j.rows || [] };
      else (autresRows[urlName] ||= []).push(...(j.rows || []));
    } catch (e) { /* fichier illisible */ }
  }
}
const nomAireBrut = (r) => {
  if (r._AS) return String(r._AS).trim();
  for (const [c, v] of Object.entries(r)) if (EST_COL_AS(c) && String(v ?? "").trim()) return String(v).trim();
  return "";
};
const nomAire = (r) => nomAireBrut(r).toLowerCase();
const airesDe = (u, zone) => new Set([...(acc[u]?.rows || []), ...(autresRows[u] || [])].filter((r) => r._ZS === zone && r._ROLE === "AS").map(nomAire).filter(Boolean));

/* Référence anti attribution croisée : aires par zone d'après l'archive de synthèse publiée. */
const court = (z) => String(z).replace(/^[a-z]{2}\s+/, "").replace(/\s+Zone de Sant[ée]$/i, "").trim().toLowerCase();
let attendues = {}; // zone courte → Set(aires) — référence GLOBALE (union des familles)
const attenduesFam = {}; // famille (CDF_HZ, Vaccine_dispo_HZ…) → zone → Set(aires)
const famille = (u) => u.replace(/_P\d$|_NF$/i, "");
try {
  const RAW = `https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/mashako-data/zs/periods/${KEY}/views`;
  for (const f of ["Vaccine_expiration_HZ_P1", "Vaccine_dispo_HZ_P1", "CDF_HZ_NF", "CDF_HZ_P1", "Infirmier_HZ_P1", "Livraison_HZ_P1", "Supervision_HZ_P3"]) {
    const r = await fetch(`${RAW}/${f}.json?_r=${Date.now()}`); if (!r.ok) continue;
    const d = await r.json();
    const rows = d.format === "compact" ? d.data.map((a) => Object.fromEntries(d.columns.map((c, i) => [c, a[i]]))) : d.rows;
    const fam = attenduesFam[famille(f)] ||= {};
    for (const row of rows) {
      const z = court(row["Zone de santé"] || row["Zone de Santé"] || row.Antenne);
      const a = row["Aire de Santé"] || row["Aire de santé"] || row["Aire de santé supervisée"];
      if (z && a) { (fam[z] ||= new Set()).add(String(a).trim().toLowerCase()); (attendues[z] ||= new Set()).add(String(a).trim().toLowerCase()); }
    }
  }
  log(`Référence d'aires : ${Object.keys(attendues).length} zone(s), familles ${Object.keys(attenduesFam).join(", ")} (archive ${KEY}).`);
} catch (e) { log(`⚠ référence d'aires indisponible (${String(e.message).slice(0, 80)}) — contrôle de cohérence désactivé.`); }
/* référence de la feuille courante : sa famille si connue, sinon la globale */
let attenduesCour = attendues;

function consolider(v) {
  const groupes = new Map(); const ordre = [];
  for (const r of v.rows) {
    const nom = nomAireBrut(r);
    const k = r._ROLE === "AS" && nom ? `AS|${r._ZS}|${nom.toLowerCase()}` : `${r._ROLE}|${r._ZS}`;
    let g = groupes.get(k);
    if (!g) { g = { _ZS: r._ZS, _ROLE: r._ROLE, _AS: nom }; groupes.set(k, g); ordre.push(g); }
    for (const [c, val] of Object.entries(r)) {
      if (c === "_SHEET" || c === "_ZS" || c === "_ROLE" || c === "_BLOC" || c === "_AS") continue;
      const s = String(val ?? "").trim();
      if (s !== "" && (g[c] === undefined || g[c] === "")) g[c] = s;
    }
  }
  return ordre;
}
const sauver = () => {
  for (const [u, v] of Object.entries(acc)) {
    const rows = consolider(v);
    writeFileSync(path.join(OUT, "views", `${u}_AS${SUF}.json`), JSON.stringify({
      name: `${v.name} — détail par aire de santé`, urlName: `${u}_AS`, source: u, period: `${MONTH} ${YEAR}`, generated_at: new Date().toISOString(),
      zones: [...new Set(rows.map((r) => r._ZS))].sort((a, b) => a.localeCompare(b, "fr")), columns: v.columns, rows,
    }));
  }
  writeFileSync(LEDGER, JSON.stringify(faitIci));
};

/* Export croisé XLSX d'une feuille de la session (même commande que export-zs-as.mjs). */
async function crosstabXlsx(s, sheetdocId) {
  let x = null, key = null;
  /* HTTP 400 (TableauException) juste après un changement de zone = feuille pas encore
     recalculée : on réessaie dans la MÊME session avant de la rouvrir (06/09 : 11 zones
     perdues en 5 min faute de ce délai). */
  for (let e = 1; e <= 4 && !key; e++) {
    x = await s.post("tabsrv/export-crosstab-to-excel-server", { sheetdocId, useTabs: "true", sendNotifications: "true" });
    key = (/"resultKey"\s*:\s*"?([^",}]+)/.exec(x.txt || "") || [])[1];
    if (!key) { if (x.st === 410 || x.st === 503) break; await sleep(4000 * e); }
  }
  if (!key) throw new Error(`export-crosstab HTTP ${x.st} : ${(x.txt || x.err || "").replace(/\s+/g, " ").slice(0, 140)}`);
  for (let e = 0; e < 8; e++) {
    const g = await s.getB64(`${s.VZ}/tempfile/sessions/${s.SID}?key=${key}&keepfile=yes&attachment=yes`);
    if (g.st === 200 && g.b64) return Buffer.from(g.b64, "base64");
    if (g.st === 410 || g.st === 503) throw new Error(`session perdue (tempfile ${g.st})`);
    await sleep(1500);
  }
  throw new Error("tempfile indisponible");
}
/* Lignes brutes d'une zone : tous les tableaux croisés cibles du dashboard. */
async function exporterZone(s, zone, cibles) {
  const rows = []; const cols = [];
  for (const cible of cibles) {
    const buf = await crosstabXlsx(s, cible.id);
    const role = EST_TOTAL(cible.name) ? "HZ" : "AS";
    let bloc = 0;
    for (const sh of readXlsx(buf)) {
      if (sh.rows.length < 2) continue;
      bloc++;
      const hdr = sh.rows[0].map((h) => String(h || "").trim());
      for (const rr of sh.rows.slice(1)) {
        if (!rr.some((v) => String(v || "").trim())) continue;
        const o = { _ZS: zone, _ROLE: role, _SHEET: cible.name, _BLOC: bloc };
        hdr.forEach((c, i) => { if (c) o[`b${bloc}·${c}`] = rr[i] ?? ""; });
        rows.push(o);
      }
      for (const c of hdr) if (c && !cols.includes(`b${bloc}·${c}`)) cols.push(`b${bloc}·${c}`);
    }
  }
  return { rows, cols };
}
const coherent = (zone, rows) => {
  const ref = attenduesCour[court(zone)] || attendues[court(zone)]; if (!ref || !ref.size) return true;
  const vues = new Set(rows.filter((r) => r._ROLE === "AS").map(nomAire).filter(Boolean));
  if (!vues.size) return true; // rien exporté : pas un désaccord
  for (const a of vues) if (ref.has(a)) return true;
  return false;
};

const t0 = Date.now();
const ctx = await launch({ profile: PROFILE, cookies: path.join(HERE, "cookies-tableau.json") });
const autre = bailAutre("as");
if (autre) { log(`⏭ ${autre.titulaire} collecte déjà le détail AS (battement il y a ${autre.age_min} min) — abandon.`); await ctx.close(); process.exit(0); }
const bail = surveiller("as", { note: `export AS sessions ${KEY}`, tache: "sync" });
if (!bail) log("⚠ Bail « as » non posé (GitHub muet) — on continue (fail-open).");

let nOk = 0, nVide = 0, nEchec = 0, nSaut = 0, coupe = false;
/* Bridage Tableau (06/09 15:40 : cloud 6 sessions + synchro ZS locale + AS → tous les
   tableaux de bord répondent avec un dialogue vide et un bootstrap muet, y compris sur des
   zones qui passaient 10 min avant). Au lieu de brûler les zones 3 essais chacune, on
   marque une pause globale de MASHAKO_PAUSE_MIN (déf. 10) après 6 « dialogue vide »
   consécutifs, et la zone est remise dans la file. */
let videsConsecutifs = 0, pauseJusqua = 0;
const PAUSE_MIN = Number(process.env.MASHAKO_PAUSE_MIN || 10);
const budgetEpuise = () => (Date.now() - t0) / 60000 > MAX_MINUTES;
try {
  for (const [label, urlName] of VUES) {
    if (budgetEpuise()) { coupe = true; break; }
    const p1 = /_P2$/.test(urlName) ? urlName.replace(/_P2$/, "_P1") : null;
    const todo = ZONES.filter((z) => {
      if (fait[cle(urlName, z)] || faitIci[cle(urlName, z)]) return false;
      if (p1 && (fait[cle(p1, z)] || faitIci[cle(p1, z)]) && airesDe(p1, z).size < 20) { faitIci[cle(urlName, z)] = "page1-suffit"; nSaut++; return false; }
      return true;
    });
    attenduesCour = attenduesFam[famille(urlName)] || attendues;
    if (!todo.length) { log(`= ${label} : rien à faire`); continue; }
    log(`▶ ${label} (${urlName}) : ${todo.length} zone(s), ${Math.min(P, todo.length)} session(s)`);
    const a = acc[urlName] || (acc[urlName] = { name: label, columns: [], rows: [] });
    let idx = 0; const prochaine = () => (idx < todo.length && !budgetEpuise() ? todo[idx++] : null);
    let reouverture = false;
    const ouvrir = async (zone) => {
      const s = await openSession(ctx, WB, urlName, `${PER}&_SELECTED_location_level=${encodeURIComponent(FILTRES[zone])}`);
      try {
        let sh = [];
        const attendDetail = (attenduesCour[court(zone)] || new Set()).size > 0;
        for (let e = 1; e <= 15; e++) {
          sh = await crosstabSheets(s, THUMBS);
          const complet = sh.some((x) => x.id && EST_CIBLE(x.name));
          if (complet || (sh.length && !attendDetail && e >= 3)) break;
          await sleep(4000);
        }
        if (!sh.length) throw new Error("dialogue croisé vide");
        return { s, courante: zone, cibles: sh.filter((x) => x.id && EST_CIBLE(x.name)), toutes: sh.map((x) => x.name) };
      } catch (e) { await s.close(); throw e; }
    };
    async function travailleur(n) {
      if (n > 1) await sleep((n - 1) * 8000);
      let w = null, zone = prochaine(), essais = 0;
      while (zone) {
        const tz = Date.now();
        try {
          if (!w) w = await ouvrir(zone);
          else if (w.courante !== zone) {
            if (reouverture) { await w.s.close(); w = await ouvrir(zone); }
            else { await setZone(w.s, FILTRES[zone]); w.courante = zone; }
          }
          if (!w.cibles.length && (attenduesCour[court(zone)] || new Set()).size && essais < 3) {
            /* dialogue partiel (feuilles pas encore chargées) alors que l'archive connaît des
               aires pour cette zone : on relit la liste avant de conclure « pas de détail ». */
            essais++; await sleep(4000);
            const sh = await crosstabSheets(w.s, THUMBS);
            if (sh.length) { w.cibles = sh.filter((x) => x.id && EST_CIBLE(x.name)); w.toutes = sh.map((x) => x.name); }
            if (!w.cibles.length) { log(`  ⟳ ${zone} · ${label} : dialogue sans feuille de détail alors que l'archive a des aires — relecture ${essais}/3`); if (essais === 3) { await w.s.close(); w = null; } continue; }
          }
          if (!w.cibles.length) {
            log(`· ${zone} · ${label} : pas de détail par aire (${w.toutes.join(" | ").slice(0, 80)})`);
            faitIci[cle(urlName, zone)] = "vide"; nVide++; zone = prochaine(); essais = 0; continue;
          }
          const { rows, cols } = await exporterZone(w.s, zone, w.cibles);
          if (!coherent(zone, rows)) {
            essais++;
            if (essais <= 2) {
              log(`  ↺ ${zone} · ${label} : aires en désaccord avec l'archive (filtre non rafraîchi ?) — session rouverte sur la zone (essai ${essais})`);
              if (essais === 2) reouverture = true;
              await w.s.close(); w = null;
              continue;
            }
            log(`  ⚠ ${zone} · ${label} : désaccord persistant — lignes écartées, zone en échec`);
            nEchec++; zone = prochaine(); essais = 0; continue;
          }
          if (!rows.length) { log(`✗ ${zone} · ${label} : ${w.cibles.length} feuille(s) annoncée(s), 0 ligne — sera repris`); nEchec++; zone = prochaine(); essais = 0; continue; }
          a.rows = a.rows.filter((r) => r._ZS !== zone); a.rows.push(...rows);
          for (const c of cols) if (!a.columns.includes(c)) a.columns.push(c);
          faitIci[cle(urlName, zone)] = new Date().toISOString(); nOk++; essais = 0; videsConsecutifs = 0;
          log(`✓ ${zone} · ${label} : ${w.cibles.length} feuille(s), ${rows.length} lignes (${Math.round((Date.now() - tz) / 1000)} s) [${nOk} ok / ${nVide} vides / ${nEchec} échecs]`);
          if (nOk % 5 === 0) sauver();
          zone = prochaine();
        } catch (e) {
          const msg = String(e.message || e);
          if (w) { await w.s.close(); w = null; }
          if (/dialogue croisé vide|session VizQL non capturée|bootstrap muet/i.test(msg)) {
            videsConsecutifs++;
            if (videsConsecutifs >= 6) {
              if (Date.now() >= pauseJusqua) {
                pauseJusqua = Date.now() + PAUSE_MIN * 60000;
                log(`  ⏸ Tableau ne rend plus les tableaux de bord (${videsConsecutifs} dialogues vides d'affilée — bridage probable) : pause ${PAUSE_MIN} min, zones remises en file.`);
              }
              todo.push(zone); zone = null; // remise en file, ce travailleur attend la fin de la pause
              while (Date.now() < pauseJusqua && !budgetEpuise()) await sleep(5000);
              videsConsecutifs = 0; essais = 0; zone = prochaine();
              continue;
            }
          } else videsConsecutifs = 0;
          essais++;
          log(`  ⟳ ${zone} · ${label} : ${msg.slice(0, 120)} (essai ${essais})`);
          if (essais >= 3) { nEchec++; essais = 0; zone = prochaine(); }
          await sleep(3000 * Math.min(essais + 1, 4));
        }
      }
      if (w) await w.s.close();
    }
    await Promise.all(Array.from({ length: Math.min(P, todo.length) }, (_, i) => travailleur(i + 1)));
    sauver();
    if (budgetEpuise()) { coupe = true; break; }
  }
} finally {
  sauver();
  await ctx.close().catch(() => { /* déjà fermé */ });
}
if (coupe) log(`⚠ Garde-fou ${MAX_MINUTES} min — arrêt (la progression est au journal).`);
log("— Bilan —");
for (const [u, v] of Object.entries(acc)) log(`   ${u}_AS${SUF}.json : ${new Set(v.rows.map((r) => r._ZS)).size} zone(s), ${v.rows.length} lignes brutes`);
log(`— ${nOk} export(s), ${nVide} sans détail, ${nSaut} page(s) 2 sautée(s), ${nEchec} échec(s) en ${Math.round((Date.now() - t0) / 60000)} min —`);
