#!/usr/bin/env node
/** Détail par ZONE DE SANTÉ de Dispo_vaccins_ANT et Vaccine_expiration_ANT_P1.
 *
 *  Le CSV de ces feuilles ne donne que la synthèse antenne (1 ligne). Le détail
 *  ZS vit dans les feuilles masquées _TABLE_… du tableau de bord, accessibles
 *  UNIQUEMENT par la chaîne crosstab Excel (① dialogue → ② export → ③ xlsx).
 *  UNE session filtrée sur les 51 antennes (multi-valeurs) ramène les ~514 ZS
 *  d'un coup (validé le 27/07 — 514 lignes pour Dispo).
 *
 *  Sortie : out/views/Dispo_vaccins_ZS.json et out/views/Vaccine_expiration_ZS.json
 *  (une ligne par ZS, une colonne par antigène), prêts à publier.
 *
 *  Usage autonome : node export-ant-zs-detail.mjs [Mois] [Année] (déf. Juin 2026)
 *  Intégré : sync.mjs appelle exportAntZsDetail(page, { month, year }) à chaque
 *  synchro ANT quotidienne (29/07) — les *_ZS.json ne datent plus.
 */
import { chromium } from "playwright";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readXlsx } from "./xlsx-lite.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = path.join(HERE, "browser-profile");
const OUT = path.join(HERE, "out");
const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
const WB = "Mashako3_0RapportdelAntenne";
const LOG0 = (m) => console.log(`[${new Date().toISOString()}] ${m}`);
const T = (t) => `${Math.round((Date.now() - t) / 1000)} s`;

const DASHBOARDS = [
  { view: "Dispo_vaccins_ANT", table: /_TABLE_vaccine_av/i, out: "Dispo_vaccins_ZS", label: "Disponibilité des vaccins — détail par zone de santé", kind: "stock" },
  { view: "Vaccine_expiration_ANT_P1", table: /_TABLE_vaccine_expiry/i, out: "Vaccine_expiration_ZS", label: "Expiration des vaccins — détail par zone de santé", kind: "expiry" },
];
/* antigènes : nom court → libellé affiché (ordre du visuel original) */
const AG_STOCK = [["BCG", "BCG"], ["Penta", "Penta"], ["PCV", "PCV"], ["VPO", "VPO"], ["Rota", "Rota"], ["VPI", "VPI"], ["VAR", "VAR-RR"], ["VAA", "VAA"], ["TD", "TD"], ["SAB_005ml", "SAB 0,05ml"], ["SAB_05ml", "SAB 0,5ml"], ["Sdil2ml", "Sdil 2ml"], ["Sdil5ml", "Sdil 5ml"], ["BS", "Box de Sécurité"]];
const AG_EXPIRY = [["BCG", "BCG"], ["Penta", "Penta"], ["PCV", "PCV"], ["vpo", "VPO"], ["rota", "Rota"], ["vpi", "VPI"], ["var", "VAR-RR"], ["td", "TD"]];

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

/** Cœur réutilisable : page Playwright DÉJÀ connectée au Tableau (fournie par
 *  sync.mjs, ou par un navigateur dédié en usage autonome). Écrit
 *  out/views/*_ZS.json (+ diag-sheets/*.xlsx) et retourne
 *  { urlName: { label, rows, avec, file } }. Lance une Error si la session est
 *  invalide ; les échecs par feuille sont loggés et ignorés (continue). */
export async function exportAntZsDetail(page, { month = "Juin", year = "2026", log = LOG0 } = {}) {
  const PER = `_PARAM_month=${encodeURIComponent(month)}&_PARAM_year=${encodeURIComponent(year)}`;
  let SID = null, GSH = null, XSRF = null, TVER = null;
  const onResp = (resp) => {
    try {
      const u = resp.url();
      if (!u.includes(`/t/${SITE}/`)) return;
      const h = resp.headers();
      if (h["x-session-id"]) SID = h["x-session-id"];
      if (h["global-session-header"]) GSH = h["global-session-header"];
    } catch (e) { }
  };
  const onReq = (r) => {
    try {
      const m = /\/sessions\/([0-9A-F]+-\d+:\d+)/i.exec(r.url());
      if (m && !SID) SID = m[1];
      const h = r.headers();
      if (!XSRF && h["x-xsrf-token"]) XSRF = h["x-xsrf-token"];
      if (!TVER && h["x-tableau-version"]) TVER = h["x-tableau-version"];
    } catch (e) { }
  };
  page.on("response", onResp);
  page.on("request", onReq);
  const mkHeaders = (a) => ({
    accept: a || "text/javascript", "global-session-header": GSH,
    "x-xsrf-token": XSRF, "x-tableau-version": TVER || "2026.2", "x-requested-with": "XMLHttpRequest",
  });
  const post = (u, fields) => page.evaluate(async (a) => {
    const fd = new FormData();
    for (const [k, v] of Object.entries(a.fields)) fd.append(k, v);
    try {
      const r = await fetch(a.u, { method: "POST", body: fd, credentials: "include", headers: a.headers });
      return { st: r.status, txt: (await r.text()).slice(0, 200000) };
    } catch (e) { return { st: 0, err: String(e).slice(0, 120) }; }
  }, { u, fields, headers: mkHeaders() });

  const resultats = {};
  try {
    /* ── session + liste complète des antennes ── */
    const t0 = Date.now();
    await page.goto(`${SERVER}/#/site/${SITE}/views/${WB}/FILTER_VALUES_ANT`, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
    let dl = Date.now() + 120000;
    while (Date.now() < dl) { await page.waitForTimeout(3000); if (page.frames().find((f) => f.url().includes(`/t/${SITE}/views/`))) break; }
    const fvTxt = await page.evaluate(async (u) => {
      const r = await fetch(u, { credentials: "include" });
      return r.ok ? await r.text() : "";
    }, `${SERVER}/t/${SITE}/views/${WB}/FILTER_VALUES_ANT.csv?${PER}&:refresh=yes`);
    const fvRows = parseCsv(fvTxt || "");
    if (fvRows.length < 2) throw new Error("FILTER_VALUES_ANT indisponible");
    const si = fvRows[0].findIndex((c) => /SELECTED_location_level/i.test(c));
    const antennes = fvRows.slice(1).map((r) => (r[si] || "").trim()).filter(Boolean);
    log(`✓ ${antennes.length} antennes (${T(t0)})`);
    /* carte ZS→antenne (cache du jour) */
    let zsToAnt = {};
    try { zsToAnt = JSON.parse(readFileSync(path.join(HERE, "zs_ant_map.json"), "utf8")).map || {}; } catch (e) { }
    log(`  carte ZS→antenne : ${Object.keys(zsToAnt).length} ZS`);

    for (const dash of DASHBOARDS) {
      const td = Date.now();
      SID = null; GSH = null;
      await page.goto("about:blank").catch(() => { });
      await page.goto(`${SERVER}/#/site/${SITE}/views/${WB}/${dash.view}?${PER}&_SELECTED_location_level=${antennes.map(encodeURIComponent).join(",")}`, { waitUntil: "domcontentloaded", timeout: 150000 }).catch(() => { });
      dl = Date.now() + 300000;
      while (Date.now() < dl) { await page.waitForTimeout(4000); if (SID && GSH && XSRF) break; }
      if (!(SID && GSH && XSRF)) { log(`✗ ${dash.view} : session non capturée`); continue; }
      const BASE = `${SERVER}/vizql/t/${SITE}/w/${WB}/v/${dash.view}/sessions/${SID}/commands/tabsrv`;
      const d = await post(`${BASE}/export-crosstab-server-dialog`, { thumbnailUris: "{}" });
      if (d.st !== 200) { log(`✗ ${dash.view} : dialogue ${d.st}`); continue; }
      const items = [...d.txt.matchAll(/"sheetName"\s*:\s*"([^"]+)"\s*,\s*"sheetdocId"\s*:\s*"([^"]+)"/g)]
        .map((m) => ({ name: m[1], id: m[2] }));
      const target = items.find((s) => dash.table.test(s.name));
      if (!target) { log(`✗ ${dash.view} : feuille _TABLE introuvable (${items.map((x) => x.name).join(" | ")})`); continue; }
      const x = await post(`${BASE}/export-crosstab-to-excel-server`, { sheetdocId: target.id, useTabs: "true", sendNotifications: "true" });
      const key = (/"resultKey"\s*:\s*"?([^",}]+)/.exec(x.txt || "") || /key=(\d+)/.exec(x.txt || "") || [])[1];
      if (!key) { log(`✗ ${dash.view} : export ${x.st} — ${(x.txt || "").slice(0, 150)}`); continue; }
      const b64 = await page.evaluate(async (a) => {
        const r = await fetch(a.u, { credentials: "include", headers: a.headers });
        if (!r.ok) return null;
        const b = new Uint8Array(await r.arrayBuffer());
        let s = ""; const C = 0x8000;
        for (let i = 0; i < b.length; i += C) s += String.fromCharCode.apply(null, b.subarray(i, i + C));
        return btoa(s);
      }, { u: `${SERVER}/vizql/t/${SITE}/w/${WB}/v/${dash.view}/tempfile/sessions/${SID}?key=${key}&keepfile=yes&attachment=yes`, headers: mkHeaders("*/*") });
      if (!b64) { log(`✗ ${dash.view} : téléchargement`); continue; }
      const buf = Buffer.from(b64, "base64");
      mkdirSync(path.join(HERE, "diag-sheets"), { recursive: true });
      writeFileSync(path.join(HERE, "diag-sheets", `${dash.out}_${month}_${year}.xlsx`), buf);
      const sheets = readXlsx(buf);
      log(`✓ ${dash.view} : xlsx ${buf.length} octets, ${sheets.length} feuilles (${T(td)})`);

      /* ── parsing → une ligne par ZS ── */
      const AG = dash.kind === "stock" ? AG_STOCK : AG_EXPIRY;
      const parZs = {}; // zs → { Antenne, "Zone de santé", ... }
      const zsRow = (zs) => {
        if (!parZs[zs]) {
          parZs[zs] = { Antenne: zsToAnt[zs] || "", "Zone de santé": zs };
        }
        return parZs[zs];
      };
      for (const sh of sheets) {
        if (!sh.rows.length) continue;
        const hdr = sh.rows[0].map((h) => String(h || ""));
        const zsi = hdr.findIndex((h) => /zone de sant/i.test(h));
        if (zsi < 0) continue;
        if (dash.kind === "stock") {
          const fi = hdr.findIndex((h) => /_([A-Za-z0-9_]+)_avail_HZS_noagg_COLOR/i.test(h));
          if (fi < 0) continue;
          const agKey = (/_([A-Za-z0-9_]+)_avail_HZS_noagg_COLOR/i.exec(hdr[fi]) || [])[1];
          const ag = AG.find(([k]) => k.toLowerCase() === String(agKey || "").toLowerCase());
          if (!ag) continue;
          /* colonne semaines : celle dont l'en-tête n'est ni ZS, ni flag, ni SUPERVISION, ni MIN(1) */
          const vi = hdr.findIndex((h, i) => i !== zsi && i !== fi && !/SUPERVISION|MIN\(1\)|^\s*$/.test(h));
          for (const rr of sh.rows.slice(1)) {
            const zs = String(rr[zsi] || "").trim();
            if (!zs) continue;
            const o = zsRow(zs);
            o[`${ag[0]}_avail`] = String(rr[fi] || "").trim() === "Vrai" ? "Vrai" : "Faux";
            const v = vi >= 0 ? String(rr[vi] ?? "").trim() : "";
            o[ag[0]] = v === "" || v === "0" ? (o[`${ag[0]}_avail`] === "Vrai" ? v : "") : v;
          }
        } else {
          const fi = hdr.findIndex((h) => /_PERC_HA_expiry_alert_([A-Za-z]+)_color/i.test(h));
          if (fi < 0) continue;
          const agKey = (/_PERC_HA_expiry_alert_([A-Za-z]+)_color/i.exec(hdr[fi]) || [])[1];
          const ag = AG.find(([k]) => k.toLowerCase() === String(agKey || "").toLowerCase());
          if (!ag) continue;
          const vi = hdr.findIndex((h) => new RegExp(`_PERC_HA_expiry_alert_${agKey}\\]`, "i").test(h) || new RegExp(`^_PERC_HA_expiry_alert_${agKey}$`, "i").test(h));
          for (const rr of sh.rows.slice(1)) {
            const zs = String(rr[zsi] || "").trim();
            if (!zs) continue;
            const o = zsRow(zs);
            const n = vi >= 0 ? Number(String(rr[vi] ?? "").replace(",", ".")) : NaN;
            o[`_perc_HA_expiry_alert_${ag[0]}`] = isFinite(n) ? `${Math.round(n * 100)}%` : "";
            o[`_perc_HA_expiry_alert_${ag[0]}_COLOR`] = String(rr[fi] || "").trim().toLowerCase();
          }
        }
      }
      /* colonnes dans l'ordre du visuel */
      let columns;
      if (dash.kind === "stock") {
        columns = ["Antenne", "Zone de santé"];
        for (const [k, l] of AG) { columns.push(k, `${k}_avail`); }
      } else {
        columns = ["Antenne", "Zone de santé"];
        for (const [k, l] of AG) { columns.push(`_perc_HA_expiry_alert_${k}`, `_perc_HA_expiry_alert_${k}_COLOR`); }
      }
      const rows = Object.values(parZs).sort((a, b) => String(a.Antenne).localeCompare(String(b.Antenne), "fr") || String(a["Zone de santé"]).localeCompare(String(b["Zone de santé"]), "fr"));
      const avec = rows.filter((r) => AG.some(([k]) => dash.kind === "stock" ? r[`${k}_avail`] === "Vrai" : r[`_perc_HA_expiry_alert_${k}`])).length;
      mkdirSync(path.join(OUT, "views"), { recursive: true });
      const rel = path.join(OUT, "views", `${dash.out}.json`);
      writeFileSync(rel, JSON.stringify({ name: dash.label, urlName: dash.out, columns, rows }, null, 0));
      resultats[dash.out] = { label: dash.label, rows: rows.length, avec, file: `views/${dash.out}.json` };
      log(`✓ ${dash.out}.json : ${rows.length} ZS (${avec} avec données) → ${rel}`);
    }
    log("— Export terminé —");
    return resultats;
  } finally {
    page.off("response", onResp);
    page.off("request", onReq);
  }
}

/* Point d'entrée autonome : lance son propre navigateur sur le profil dédié. */
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const MONTH = process.argv[2] || process.env.MASHAKO_MONTH || "Juin";
  const YEAR = process.argv[3] || process.env.MASHAKO_YEAR || "2026";
  const ctx = await chromium.launchPersistentContext(PROFILE, {
    channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
    viewport: { width: 1500, height: 950 }, args: ["--no-first-run", "--no-default-browser-check"],
  });
  try {
    const page = ctx.pages()[0] || await ctx.newPage();
    const r = await exportAntZsDetail(page, { month: MONTH, year: YEAR });
    console.log(JSON.stringify(r));
  } finally { await ctx.close(); }
}
