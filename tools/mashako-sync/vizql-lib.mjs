/** Bibliothèque VizQL (02/09/2026) — UNE session Tableau par (vue, période),
 *  puis changement de zone par commande et export dans la même session.
 *
 *  Pourquoi : un export .csv par URL ouvre une session VizQL neuve et rend tout
 *  le dashboard (~33 s côté serveur, 120 s sous charge). Dans une session déjà
 *  ouverte, categorical-filter → zone coûte ~10-17 s et un export ~1-2 s.
 *
 *  Le navigateur (profil Chrome connecté) ne sert qu'à OUVRIR la session et à
 *  porter les cookies : toutes les commandes passent par fetch() dans la page.
 */
import { chromium } from "playwright";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const SERVER = "https://eu-west-1a.online.tableau.com";
export const SITE = "axdata";
export const telemetryId = () => `${Date.now().toString(36)}$${Math.random().toString(36).slice(2, 8)}`;
export const dec16 = (b64) => { const b = Buffer.from(b64, "base64"); return (b[0] === 0xff && b[1] === 0xfe) ? b.slice(2).toString("utf16le") : b.toString("utf8"); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function launch(opts = {}) {
  const profile = opts.profile || path.join(HERE, "browser-profile");
  let ctx = null;
  for (let essai = 1; essai <= 3 && !ctx; essai++) {
    try {
      ctx = await chromium.launchPersistentContext(profile, {
        channel: "chrome", headless: opts.headless !== false, ignoreDefaultArgs: ["--enable-automation"],
        viewport: { width: 1500, height: 950 }, args: ["--no-first-run", "--no-default-browser-check"],
      });
    } catch (e) { if (essai === 3) throw e; await sleep(20000); }
  }
  return ctx;
}

/** Ouvre une session VizQL sur `view` (urlName) avec les paramètres d'URL
 *  donnés (ex. `_PARAM_month=Juin&_PARAM_year=2026&_SELECTED_location_level=…`). */
export async function openSession(ctx, wb, view, query, opts = {}) {
  const page = await ctx.newPage();
  const s = { page, wb, view, SID: null, GSH: null, XSRF: null, TVER: null, BOOT: null };
  const onResp = async (r) => {
    try {
      const u = r.url(); if (!u.includes(`/t/${SITE}/`) || !u.includes(`/v/${view}/`) && !u.includes(`/views/${wb}/${view}`)) return;
      const h = r.headers();
      if (h["x-session-id"]) s.SID = h["x-session-id"];
      if (h["global-session-header"]) s.GSH = h["global-session-header"];
      if (u.includes("/bootstrapSession/")) s.BOOT = await r.text().catch(() => null);
    } catch (e) { }
  };
  const onReq = (r) => {
    try {
      const u = r.url(); const m = /\/sessions\/([0-9A-F]+-\d+:\d+)/i.exec(u);
      if (m && !s.SID && u.includes(`/v/${view}/`)) s.SID = m[1];
      const h = r.headers();
      if (!s.XSRF && h["x-xsrf-token"]) s.XSRF = h["x-xsrf-token"];
      if (!s.TVER && h["x-tableau-version"]) s.TVER = h["x-tableau-version"];
    } catch (e) { }
  };
  page.on("response", onResp); page.on("request", onReq);
  const t0 = Date.now();
  await page.goto(`${SERVER}/#/site/${SITE}/views/${wb}/${view}?${query}`, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
  const dl = Date.now() + (opts.timeout || 150000);
  while (Date.now() < dl && !(s.SID && s.XSRF && s.BOOT)) await sleep(500);
  if (!(s.SID && s.XSRF && s.BOOT)) { await page.close().catch(() => { }); throw new Error(`session VizQL non capturée pour ${view} (SID=${s.SID}, XSRF=${!!s.XSRF}, boot=${!!s.BOOT})`); }
  s.ms = Date.now() - t0;
  s.VZ = `${SERVER}/vizql/t/${SITE}/w/${wb}/v/${view}`;
  s.CMD = `${s.VZ}/sessions/${s.SID}/commands`;
  const headers = (accept) => ({
    accept: accept || "text/javascript", "global-session-header": s.GSH, "x-xsrf-token": s.XSRF,
    "x-tableau-version": s.TVER || "2026.2", "x-requested-with": "XMLHttpRequest", "x-tsi-active-tab": encodeURIComponent(view),
  });
  s.post = (cmd, fields) => page.evaluate(async (a) => {
    const fd = new FormData();
    for (const [k, v] of Object.entries(a.fields)) fd.append(k, v);
    try { const r = await fetch(a.u, { method: "POST", body: fd, credentials: "include", headers: a.headers }); return { st: r.status, txt: (await r.text()).slice(0, 4000000) }; }
    catch (e) { return { st: 0, err: String(e).slice(0, 160) }; }
  }, { u: `${s.CMD}/${cmd}`, fields: { ...fields, telemetryCommandId: telemetryId() }, headers: headers() });
  s.getB64 = (u) => page.evaluate(async (a) => {
    try {
      const r = await fetch(a.u, { credentials: "include", headers: a.headers });
      const b = new Uint8Array(await r.arrayBuffer()); let out = ""; const C = 0x8000;
      for (let i = 0; i < b.length; i += C) out += String.fromCharCode.apply(null, b.subarray(i, i + C));
      return { st: r.status, ct: r.headers.get("content-type"), b64: btoa(out) };
    } catch (e) { return { st: 0, err: String(e).slice(0, 160) }; }
  }, { u, headers: headers("*/*") });
  /* le filtre de localisation et ses feuilles, lus dans le bootstrap */
  const evt = /"genFilterChangeEventPresModel":\{"fieldCaption": "_SELECTED_location_level","fn": "([^"]+)","globalFieldName": "([^"]+)","visualIdPresModel":\{"worksheet": "([^"]+)"/.exec(s.BOOT);
  s.filterField = evt ? evt[2] : null;
  s.filterWorksheet = evt ? evt[3] : null;
  s.dashboard = (/"dashboardPresModel":\{"sheetPath":\{"sheetName": "([^"]+)","isDashboard": true/.exec(s.BOOT) || /"active_tab":\s*"([^"]+)"/.exec(s.BOOT) || [])[1] || view;
  /* toutes les feuilles portant le filtre de localisation (61 dans le classeur ZS) */
  s.filterSheets = [...new Set([...s.BOOT.matchAll(/"genFilterChangeEventPresModel":\{"fieldCaption": "_SELECTED_location_level"[^}]*"worksheet": "([^"]+)"/g)].map((m) => m[1]))];
  s.sheets = null; // renseigné par crosstabSheets()
  s.params = Object.fromEntries([...s.BOOT.matchAll(/"fn": "(\[Parameters\]\.\[[^"]+\])","fieldCaption": "([^"]+)"/g)].map((m) => [m[2], m[1]]));
  s.tempfile = async (key, tries = 8) => {
    for (let e = 0; e < tries; e++) {
      const g = await s.getB64(`${s.VZ}/tempfile/sessions/${s.SID}?key=${key}&keepfile=yes&attachment=yes`);
      if (g.st === 200) return dec16(g.b64);
      await sleep(1200);
    }
    return null;
  };
  s.close = () => page.close().catch(() => { });
  return s;
}

/** Applique la zone (valeur composée « bu Aketi Zone de Santé ») au filtre de localisation. */
export async function setZone(s, zone, worksheet) {
  if (!s.filterField) throw new Error("filtre _SELECTED_location_level introuvable dans le bootstrap");
  /* ⚠ la feuille visée doit appartenir AU dashboard courant : filtrer une feuille
     d'un autre dashboard répond 200 en 0,3 s… sans rien changer à l'écran. */
  const dans = (w) => !s.sheets || s.sheets.some((x) => x.name === w);
  const cible = worksheet || (s.sheets ? s.filterSheets.find((w) => dans(w)) : null) || s.filterWorksheet;
  const r = await s.post("tabdoc/categorical-filter", {
    visualIdPresModel: JSON.stringify({ worksheet: cible, dashboard: s.dashboard }),
    globalFieldName: s.filterField, membershipTarget: "filter", filterUpdateType: "filter-replace",
    filterValues: JSON.stringify(Array.isArray(zone) ? zone : [zone]), heuristicCommandReinterpretation: "false",
  });
  const txt = r.txt || r.err || "";
  if (r.st !== 200 || /TableauException|"errorCode"/.test(txt.slice(0, 800))) throw new Error(`categorical-filter HTTP ${r.st} : ${txt.replace(/\s+/g, " ").slice(0, 200)}`);
  return r;
}

/** Change un paramètre (ex. _PARAM_month = Juillet). */
export async function setParam(s, caption, value) {
  const fn = s.params[caption];
  if (!fn) throw new Error(`paramètre ${caption} inconnu (${Object.keys(s.params).join(", ")})`);
  const r = await s.post("tabdoc/set-parameter-value", { globalFieldName: fn, valueString: String(value), useUsLocale: "false" });
  const txt = r.txt || r.err || "";
  if (r.st !== 200 || /TableauException|"errorCode"/.test(txt.slice(0, 800))) throw new Error(`set-parameter-value HTTP ${r.st} : ${txt.replace(/\s+/g, " ").slice(0, 200)}`);
  return r;
}

/** Liste des feuilles du dashboard exportables en tableau croisé. */
export async function crosstabSheets(s, thumbs) {
  const d = await s.post("tabsrv/export-crosstab-server-dialog", { thumbnailUris: thumbs || "[]" });
  s.sheets = [...(d.txt || "").matchAll(/"sheetName"\s*:\s*"([^"]+)"\s*,\s*"sheetdocId"\s*:\s*"([^"]+)"/g)].map((m) => ({ name: m[1], id: m[2] }));
  return s.sheets;
}

/** Export tableau croisé CSV (UTF-16 → texte, tabulations). */
export async function crosstabCsv(s, sheetdocId) {
  const x = await s.post("tabsrv/export-crosstab-to-csvserver", { sheetdocId, useTabs: "true", sendNotifications: "true" });
  const key = (/"resultKey"\s*:\s*"?([^",}]+)/.exec(x.txt || "") || [])[1];
  if (!key) throw new Error(`export-crosstab HTTP ${x.st} : ${(x.txt || x.err || "").replace(/\s+/g, " ").slice(0, 160)}`);
  const csv = await s.tempfile(key);
  if (csv == null) throw new Error("tempfile indisponible");
  return csv;
}

/** Fichier tabulé → lignes (tableau de tableaux). */
export function parseTsv(text) {
  return text.split(/\r?\n/).filter((l) => l.length).map((l) => l.split("\t"));
}

/* ── Données résumé (équivalent EXACT de l'export .csv par URL) ─────────────
   Protocole capturé le 02/09/2026 sur la fenêtre « Télécharger → Données » :
   ① tabdoc/get-view-data-dialog-tab-pres-model (isSummaryTable=true) → colonnes
   ② tabsrv/export-view-data-summary-to-csv-server (columns=[fn…]) → resultKey
   ③ GET tempfile/sessions/<sid>?key=… → CSV (UTF-8, séparateur « ; »). */
export function datasourceOf(s) {
  return (/^\[([^\]]+)\]\./.exec(s.filterField || "") || [])[1] || (/"datasource"\s*:\s*"([^"]+)"/.exec(s.BOOT) || [])[1] || "";
}
export async function viewDataColumns(s, worksheet, opts = {}) {
  const ds = opts.datasource || datasourceOf(s);
  const r = await s.post("tabdoc/get-view-data-dialog-tab-pres-model", {
    dataProviderType: opts.dataProviderType || "selection", viewDataTableId: "", isSummaryTable: "true",
    datasource: ds, connectionName: ds, sqlQuery: "", tableName: "",
    visualIdPresModel: JSON.stringify({ worksheet, dashboard: s.dashboard }), topN: String(opts.topN || 100000),
  });
  const txt = r.txt || r.err || "";
  if (r.st !== 200 || /TableauException|"valid": false/.test(txt.slice(0, 1500))) throw new Error(`view-data dialog HTTP ${r.st} : ${txt.replace(/\s+/g, " ").slice(0, 200)}`);
  return { txt, columns: [...new Set([...txt.matchAll(/"fn"\s*:\s*"(\[[^"]+\])"/g)].map((m) => m[1]))], captions: [...new Set([...txt.matchAll(/"fieldCaption"\s*:\s*"([^"]+)"/g)].map((m) => m[1]))] };
}
export async function summaryCsv(s, worksheet, columns, opts = {}) {
  /* showAliases=false : « True » comme l'export .csv par URL (avec alias : « Vrai ») */
  const x = await s.post("tabsrv/export-view-data-summary-to-csv-server", {
    visualIdPresModel: JSON.stringify({ worksheet, dashboard: s.dashboard }), showAliases: opts.showAliases ? "true" : "false",
    columns: JSON.stringify(columns), listSeparatorValue: ",", unicodeEncodingValue: "UTF-8",
  });
  const txt = x.txt || x.err || "";
  const key = (/"resultKey"\s*:\s*"?([^",}]+)/.exec(txt) || [])[1];
  if (!key) throw new Error(`export résumé HTTP ${x.st} : ${txt.replace(/\s+/g, " ").slice(0, 200)}`);
  const csv = await s.tempfile(key);
  if (csv == null) throw new Error("tempfile indisponible (export résumé)");
  return csv.replace(/^﻿/, "");
}
