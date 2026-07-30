#!/usr/bin/env node
/** Validation ZS : pour chaque feuille de données du classeur ZS, compare
 *  l'export GROUPÉ (paquet de 5 ZS) à la somme des exports UNITAIRES (5 ZS).
 *  Décide quelles feuilles acceptent le multi-valeurs sans perte (liste
 *  blanche) et lesquelles collapsent (→ unitaire dans sync.mjs).
 *  Durée : ~20-25 min (22 feuilles × 6 requêtes). Aucune publication.
 */
import { chromium } from "playwright";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = path.join(HERE, "browser-profile");
const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
const WB = "Mashako3_0RapportdelaZone";
const PER = `_PARAM_month=${encodeURIComponent(process.env.MASHAKO_MONTH || "Juillet")}&_PARAM_year=${process.env.MASHAKO_YEAR || "2026"}`;
const NZS = Number(process.env.VZS || 5);
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);
function parseCsv(text) {
  const rows = []; let row = [], field = "", inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) { if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else inQ = false; } else field += c; }
    else if (c === '"') inQ = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); field = "";
      if (row.length > 1 || row[0] !== "") rows.push(row);
      row = [];
    } else field += c;
  }
  if (field !== "" || row.length) { row.push(field); rows.push(row); }
  return rows;
}

const ctx = await chromium.launchPersistentContext(PROFILE, {
  channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
  viewport: { width: 1500, height: 950 }, args: ["--no-first-run", "--no-default-browser-check"],
});
const page = ctx.pages()[0] || await ctx.newPage();
try {
  /* ⚠ 27/07 07:21 : navigation vers PerformanceRsum_HZ (lourde) — l'iframe de
     la viz n'était pas bootstrapée au bout de 240 s → fetchBin sans frame →
     « FILTER_VALUES indisponible ». On passe par la feuille LÉGÈRE
     FILTER_VALUES (constaté : iframe prête en ~15 s) et on attend la frame
     explicitement. */
  await page.goto(`${SERVER}/#/site/${SITE}/views/${WB}/FILTER_VALUES`, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
  let frame = null;
  const dlF = Date.now() + 150000;
  while (Date.now() < dlF) {
    await page.waitForTimeout(3000);
    frame = page.frames().find((fr) => fr.url().includes(`/t/${SITE}/views/`));
    if (frame) break;
  }
  if (!frame) { log(`✗ iframe viz introuvable après 150 s — url: ${page.url()}`); process.exit(1); }
  /* ⚠ 27/07 ~07:22 : un fetch .csv est resté PENDANT >60 min (Vaccine_dispo_HZ_P2)
     — sans garde-temps, frame.evaluate ne rend jamais la main et le script gèle.
     AbortController côté page (90 s) + course côté Node (120 s). */
  const fetchBin = (url) => {
    const f = page.frames().find((fr) => fr.url().includes(`/t/${SITE}/views/`));
    if (!f) return Promise.resolve(null);
    return Promise.race([
      f.evaluate(async (u) => {
        try {
          const ac = new AbortController();
          const timer = setTimeout(() => ac.abort(), 90000);
          const r = await fetch(u, { credentials: "include", signal: ac.signal });
          clearTimeout(timer);
          const ct = (r.headers.get("content-type") || "").toLowerCase();
          if (!r.ok || ct.includes("html")) return null;
          return await r.text();
        } catch (e) { return null; }
      }, url).catch(() => null),
      new Promise((res) => setTimeout(() => res(null), 120000)),
    ]);
  };
  /* fetch avec rejeu + diagnostic : un null peut être transitoire (iframe
     détachée, refus ponctuel) — on réessaie avant de déclarer forfait. */
  const fetchBinRetry = async (url, essais = 3) => {
    for (let i = 0; i < essais; i++) {
      const r = await fetchBin(url);
      if (r) return r;
      await page.waitForTimeout(4000 * (i + 1));
    }
    return null;
  };
  const exportUrl = (urlName, params) => `${SERVER}/t/${SITE}/views/${WB}/${encodeURIComponent(urlName)}.csv?${params}`;

  /* 5 ZS composites avec données (Aba, Adja, Alimbongo, Ango, Bagira — mixtes) */
  const fvR = await fetchBinRetry(exportUrl("FILTER_VALUES", `:refresh=yes`));
  if (!fvR) { log(`✗ FILTER_VALUES indisponible après rejeux — url: ${page.url()} — frames: ${page.frames().map((f) => f.url().slice(0, 80)).join(" | ")}`); process.exit(1); }
  const fvRows = parseCsv(fvR);
  const si = fvRows[0].findIndex((c) => /SELECTED_location_level/i.test(c));
  const tous = fvRows.slice(1).map((r) => (r[si] || "").trim()).filter(Boolean);
  const zss = tous.slice(0, NZS);
  log(`✓ ${tous.length} ZS — échantillon : ${zss.join(" | ")}`);

  const urlCache = JSON.parse(readFileSync(path.join(HERE, "urlnames-zs.json"), "utf8"));
  const SKIP = /^(Cover ?Page|Contacts ?Page|FILTER|Configuration|Ranking|KPI )/i;
  const feuilles = Object.entries(urlCache).filter(([l]) => !SKIP.test(l));
  /* Reprise incrémentale : les verdicts déjà établis (fichier ou run gelé)
     sont conservés ; écriture APRÈS chaque feuille pour ne rien reperdre. */
  const VFICH = path.join(HERE, "zs_batch_verdicts.json");
  let verdicts = {};
  try { verdicts = JSON.parse(readFileSync(VFICH, "utf8")).verdicts || {}; } catch (e) { }
  const sauver = () => writeFileSync(VFICH, JSON.stringify({ at: new Date().toISOString(), n: zss.length, verdicts }, null, 1));
  const reste = feuilles.filter(([l]) => !verdicts[l]);
  if (reste.length < feuilles.length) log(`↩ reprise : ${feuilles.length - reste.length} feuille(s) déjà verdictée(s), ${reste.length} restante(s).`);
  for (const [label, urlName] of reste) {
    const g = await fetchBin(exportUrl(urlName, `${PER}&_SELECTED_location_level=${zss.map(encodeURIComponent).join(",")}&:refresh=yes`));
    const gRows = g ? parseCsv(g) : null;
    let u = 0, uOk = 0;
    for (const zs of zss) {
      const r = await fetchBin(exportUrl(urlName, `${PER}&_SELECTED_location_level=${encodeURIComponent(zs)}&:refresh=yes`));
      if (r) { const pr = parseCsv(r); if (pr.length > 1) { u += pr.length - 1; uOk++; } }
    }
    const gn = gRows && gRows.length > 1 ? gRows.length - 1 : 0;
    /* verdict : groupé OK s'il ramène au moins ~80% des lignes unitaires */
    const ok = gn >= Math.max(1, Math.floor(u * 0.8));
    verdicts[label] = { groupe: gn, unitaire: u, ok };
    sauver();
    log(`${ok ? "✓" : "✗ COLLAPSE"} ${label} : groupé ${gn} | unitaire ${u}`);
  }
  sauver();
  const okList = Object.keys(verdicts).filter((k) => verdicts[k].ok);
  log(`\n— ${okList.length}/${feuilles.length} feuilles compatibles groupé —`);
  log(okList.join(" | "));
} finally { await ctx.close(); }
