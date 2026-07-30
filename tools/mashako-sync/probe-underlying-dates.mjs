#!/usr/bin/env node
/** Sonde : les DATES « Expiration la plus proche » sont-elles dans
 *  tabdoc/get-underlying-data ? (demande Felly 27/07, piste §D de la mémoire)
 *
 *  Le crosstab Excel de _TABLE_vaccine_expiry_ANT_P1 ne porte que % + couleur ;
 *  les dates s'affichent à l'écran mais n'ont jamais été extraites. Piste
 *  tableauscraper (api.py) : POST …/sessions/{sid}/commands/tabdoc/
 *  get-underlying-data avec maxRows, includeAllColumns=true et
 *  visualIdPresModel={worksheet,dashboard,flipboardZoneId:0,storyPointId:0}.
 *
 *  Leçons du 27/07 appliquées :
 *   – SID pris sur l'URL /sessions/ (AVEC suffixe « -0:0 »), jamais l'en-tête ;
 *   – GSH cueilli sur les RÉPONSES /vizql/ (adopté aussi des réponses 410) ;
 *   – RAZ SID/GSH avant la navigation ; session filtrée (2 antennes suffisent
 *     pour vérifier la PRÉSENCE des colonnes date).
 *
 *  Profil SÉPARÉ (browser-profile-dates) : ne touche pas à la synchro en cours.
 *  Sortie : diag-sheets/underlying-dates-probe.json + verdict en console.
 */
import { chromium } from "playwright";
import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DIAG = path.join(HERE, "diag-sheets");
mkdirSync(DIAG, { recursive: true });
const PROFILE = path.join(HERE, "browser-profile-dates");
const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
const WB = "Mashako3_0RapportdelAntenne";
const VIEW = "Vaccine_expiration_ANT_P1";
const MONTH = process.env.MASHAKO_MONTH || "Juillet";
const YEAR = process.env.MASHAKO_YEAR || "2026";
const PER = `_PARAM_month=${encodeURIComponent(MONTH)}&_PARAM_year=${YEAR}`;
const log = (m) => console.log(`[${new Date().toISOString()}] ${m}`);

const ctx = await chromium.launchPersistentContext(PROFILE, {
  channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
  viewport: { width: 1500, height: 950 }, args: ["--no-first-run", "--no-default-browser-check"],
});
const page = ctx.pages()[0] || await ctx.newPage();
/* Le profil dates n'a pas de session vivante : on réensemence avec les cookies
   récoltés sur le profil principal (harvest-cookies.mjs, frais du jour). */
try {
  const raw = JSON.parse(readFileSync(path.join(HERE, "cookies-tableau.json"), "utf8"));
  const arr = Array.isArray(raw) ? raw : (raw.cookies || []);
  const ck = arr.map((k) => {
    const o = { name: k.name, value: k.value, domain: k.domain, path: k.path || "/", secure: !!k.secure, httpOnly: !!k.httpOnly };
    if (k.expires && k.expires > 0) o.expires = k.expires;
    if (k.sameSite) o.sameSite = /^(Lax|Strict|None)$/i.test(k.sameSite) ? k.sameSite[0].toUpperCase() + k.sameSite.slice(1).toLowerCase() : "Lax";
    return o;
  });
  await ctx.addCookies(ck);
  log(`✓ ${ck.length} cookies injectés (récolte cookies-tableau.json)`);
} catch (e) { log(`⚠ injection cookies impossible : ${String(e.message || e).slice(0, 100)}`); }
let SID = null, GSH = null, XSRF = null, TVER = null;
page.on("response", (resp) => {
  try {
    const u = resp.url();
    if (!u.includes(`/t/${SITE}/`)) return;
    const h = resp.headers();
    if (h["global-session-header"]) GSH = h["global-session-header"]; // adopté même sur 410
  } catch (e) { }
});
page.on("request", (r) => {
  try {
    const m = /\/sessions\/([0-9A-F]+-\d+:\d+)/i.exec(r.url());
    if (m) SID = m[1]; // l'URL porte le suffixe, l'en-tête x-session-id non — on ne lit QUE l'URL
    const h = r.headers();
    if (!XSRF && h["x-xsrf-token"]) XSRF = h["x-xsrf-token"];
    if (!TVER && h["x-tableau-version"]) TVER = h["x-tableau-version"];
  } catch (e) { }
});
const mkHeaders = (a) => ({
  accept: a || "text/javascript", "global-session-header": GSH,
  "x-xsrf-token": XSRF, "x-tableau-version": TVER || "2026.2", "x-requested-with": "XMLHttpRequest",
});
const post = (u, fields) => page.evaluate(async (a) => {
  const fd = new FormData();
  for (const [k, v] of Object.entries(a.fields)) fd.append(k, v);
  try {
    const r = await fetch(a.u, { method: "POST", body: fd, credentials: "include", headers: a.headers });
    return { st: r.status, txt: (await r.text()).slice(0, 500000) };
  } catch (e) { return { st: 0, err: String(e).slice(0, 120) }; }
}, { u, fields, headers: mkHeaders() });

try {
  /* 1) session filtrée sur 2 antennes (la présence des colonnes ne dépend pas
        du filtre, mais « All » retombe sur la localisation par défaut) */
  log(`→ ${VIEW} (${MONTH} ${YEAR}), filtre 2 antennes…`);
  await page.goto("about:blank").catch(() => { });
  await page.goto(`${SERVER}/#/site/${SITE}/views/${WB}/${VIEW}?${PER}&_SELECTED_location_level=${encodeURIComponent("Aketi")},${encodeURIComponent("Buta")}`, { waitUntil: "domcontentloaded", timeout: 150000 }).catch(() => { });
  const dl = Date.now() + 300000;
  while (Date.now() < dl) { await page.waitForTimeout(4000); if (SID && GSH && XSRF) break; }
  if (!(SID && GSH && XSRF)) { log(`✗ session non capturée (SID=${!!SID} GSH=${!!GSH} XSRF=${!!XSRF}) — profil browser-profile-dates sans session valide ?`); process.exit(2); }
  log(`✓ session ${SID.slice(0, 12)}… (GSH ${String(GSH).slice(0, 8)}…)`);

  /* 2) nom exact de la feuille masquée _TABLE_ via le dialogue crosstab */
  const BASE = `${SERVER}/vizql/t/${SITE}/w/${WB}/v/${VIEW}/sessions/${SID}/commands`;
  let d = await post(`${BASE}/tabsrv/export-crosstab-server-dialog`, { thumbnailUris: "{}" });
  if (d.st === 410) { log("↻ 410 au dialogue — GSH adopté, rejeu…"); d = await post(`${BASE}/tabsrv/export-crosstab-server-dialog`, { thumbnailUris: "{}" }); }
  if (d.st !== 200 || !d.txt) { log(`✗ dialogue ${d.st} — canal commandes instable (rafale de 410 ?)`); process.exit(3); }
  const items = [...d.txt.matchAll(/"sheetName"\s*:\s*"([^"]+)"\s*,\s*"sheetdocId"\s*:\s*"([^"]+)"/g)].map((m) => ({ name: m[1], id: m[2] }));
  const target = items.find((s) => /_TABLE_vaccine_expiry/i.test(s.name));
  if (!target) { log(`✗ feuille _TABLE_vaccine_expiry introuvable (${items.map((x) => x.name).join(" | ")})`); process.exit(4); }
  log(`✓ feuille masquée : ${target.name}`);

  /* 3) get-underlying-data (toutes colonnes) */
  const vipm = JSON.stringify({ worksheet: target.name, dashboard: VIEW, flipboardZoneId: 0, storyPointId: 0 });
  let u = await post(`${BASE}/tabdoc/get-underlying-data`, { maxRows: "50", includeAllColumns: "true", visualIdPresModel: vipm });
  if (u.st === 410) { log("↻ 410 underlying — GSH adopté, rejeu…"); u = await post(`${BASE}/tabdoc/get-underlying-data`, { maxRows: "50", includeAllColumns: "true", visualIdPresModel: vipm }); }
  log(`→ underlying-data : HTTP ${u.st}, ${(u.txt || "").length} car.`);
  writeFileSync(path.join(DIAG, "underlying-dates-probe.json"), JSON.stringify({ st: u.st, err: u.err || null, txt: u.txt || "" }, null, 0));
  if (u.st !== 200 || !u.txt) { log(`✗ underlying-data ${u.st} — voir diag-sheets/underlying-dates-probe.json`); process.exit(5); }

  /* 4) verdict : y a-t-il des colonnes de DATE ? */
  let j = null; try { j = JSON.parse(u.txt); } catch (e) { }
  const cles = j ? Object.keys(j) : [];
  log(`  clés racine : ${cles.join(", ") || "(JSON illisible — extrait sauvegardé)"}`);
  const txt = u.txt;
  const motifsDate = [...new Set([...txt.matchAll(/"[^"]*(date|expir|échéance|echeance)[^"]*"/gi)].map((m) => m[0]))].slice(0, 30);
  log(`  champs *date/expir* : ${motifsDate.length ? motifsDate.join(" | ") : "AUCUN"}`);
  const echantillonsDates = [...new Set([...txt.matchAll(/"\d{4}-\d{2}-\d{2}([T ][^"]*)?"|\d{1,2}\/\d{1,2}\/\d{4}/g)].map((m) => m[0]))].slice(0, 15);
  log(`  valeurs ressemblant à des dates : ${echantillonsDates.length ? echantillonsDates.join(" | ") : "AUCUNE"}`);
  log(motifsDate.length || echantillonsDates.length ? "✓ PISTE CONFIRMÉE — les dates sont extractibles par underlying-data." : "✗ Pas de date dans underlying-data — voir le JSON sauvegardé.");
} finally { await ctx.close(); }
