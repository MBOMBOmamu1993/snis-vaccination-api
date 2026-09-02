#!/usr/bin/env node
/**
 * Récupération des périodes PASSÉES du classeur Mashako 3.0 (rapport mensuel).
 *
 * Le classeur expose _PARAM_month/_PARAM_year en paramètres d'URL (validé le
 * 25/07/2026 : les CSV changent réellement par période). Ce script parcourt les
 * périodes manquantes de periods/index.json — de la plus récente à la plus
 * ancienne (2026-05 → 2025-07) — et pour chacune :
 *   feuilles × 51 antennes en CSV (+ PNG par défaut par feuille), meta.json,
 *   publication sous periods/<AAAA-MM>/ (base_tree : ne touche à rien d'autre).
 *
 * Conçu pour tourner DES HEURES en détaché (Start-Process). Journal : backfill.log.
 * S'arrête proprement si Tableau bride (pré-contrôle + taux de réussite par période).
 */
import { chromium } from "playwright";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync, appendFileSync, rmSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PROFILE = path.join(HERE, "browser-profile");
/* ── DEUX CLASSEURS : ANT (défaut) et ZS (env MASHAKO_CFG=zs), publiés
   respectivement sous periods/ et zs/periods/ de la même branche. ── */
const IS_ZS = process.env.MASHAKO_CFG === "zs";
const OUT = path.join(HERE, (IS_ZS ? "out-backfill-zs" : "out-backfill") + (process.env.MASHAKO_NOLOCK === "1" ? "-test" : "")); // tests : dossier séparé (le 02/09 un test a effacé les fichiers du run en cours)
const LOG = path.join(HERE, IS_ZS ? "backfill-zs.log" : "backfill.log");
const LOCK = path.join(HERE, "out", ".sync.lock"); // verrou PARTAGÉ avec sync.mjs (ANT + ZS)

const SERVER = "https://eu-west-1a.online.tableau.com";
const SITE = "axdata";
/* Classeur ZS = jumeau du rapport Antenne (voir sync.mjs) — corrigé 25/07/2026 */
const WORKBOOK = IS_ZS ? "Mashako3_0RapportdelaZone" : "Mashako3_0RapportdelAntenne";
const MAIN_VIEW = IS_ZS ? "PerformanceRsum_HZ" : "HZScores_ANT";
const PFX = IS_ZS ? "zs/" : "";
const URLCACHE_FILE = path.join(HERE, IS_ZS ? "urlnames-zs.json" : "urlnames.json");
const UI_URL = `${SERVER}/#/site/${SITE}/views/${WORKBOOK}/${MAIN_VIEW}`;
const REPO = "repos/MBOMBOmamu1993/snis-vaccination-api";
const DATA_BRANCH = "mashako-data";
const RAW = "https://raw.githubusercontent.com/MBOMBOmamu1993/snis-vaccination-api/mashako-data";

const MOIS = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];
/* Périodes cibles : Juillet 2025 → Mai 2026 (Juin 2026 = rapport courant, déjà archivé par sync.mjs) */
const TARGETS = [];
for (let y = 2026, m = 5; !(y === 2025 && m === 6); m--) { if (m === 0) { m = 12; y--; } TARGETS.push({ key: `${y}-${String(m).padStart(2, "0")}`, month: MOIS[m - 1], year: String(y) }); }

/* MASHAKO_REDO=2026-07[,2026-06…] : ré-archiver des périodes DÉJÀ publiées.
   Utile quand une archive a été figée depuis un snapshot partiel — cas de
   juillet 2026 : session morte le 30/07, archive posée le 06/08 avec les
   supervisions saisies après le 30/07 absentes (confusions antenne Luiza). */
const REDO = (process.env.MASHAKO_REDO || "").split(",").map((s) => s.trim()).filter(Boolean)
  .map((k) => {
    const [y, m] = k.split("-").map(Number);
    return y >= 2024 && m >= 1 && m <= 12
      ? { key: `${y}-${String(m).padStart(2, "0")}`, month: MOIS[m - 1], year: String(y) }
      : null;
  })
  .filter(Boolean);

/* MASHAKO_ONLY="HZ Scores_ANT[,Infirmier_ANT…]" (avec MASHAKO_REDO=<période>) :
   RÉPARATION CIBLÉE d'une archive déjà publiée — seules ces feuilles sont
   ré-exportées puis FUSIONNÉES dans l'archive (meta.json conservé, vues
   remplacées). Ajouté le 02/09/2026 pour HZ Scores_ANT de janvier 2026. */
const ONLY = (process.env.MASHAKO_ONLY || "").split(",").map((s) => s.trim()).filter(Boolean);
const onlyMatch = (label) => !ONLY.length || ONLY.some((o) => o.toLowerCase() === label.toLowerCase() || slug(o) === slug(label));

/* MODE DIRECT (02/09/2026) : les exports CSV/PNG partent en HTTPS avec les
   cookies de session (cookies-tableau.json, ou TABLEAU_COOKIES dans le cloud),
   SANS Chrome : pas de profil à verrouiller, pas de rendu de vue, parallélisme
   6 au lieu de 3. Repli automatique sur Playwright si la session directe est
   refusée. MASHAKO_DIRECT=0 pour imposer le navigateur. */
const DIRECT = process.env.MASHAKO_DIRECT !== "0";
const COOKIE_FILE = path.join(HERE, "cookies-tableau.json");
function cookieHeader() {
  try {
    /* PC : cookies-tableau.json (reconnecter.mjs) ; cloud : cookies-cache.json
       (session-cache.mjs --charger) puis le secret TABLEAU_COOKIES. */
    let raw = null;
    for (const f of [COOKIE_FILE, path.join(HERE, "cookies-cache.json")]) {
      try { raw = JSON.parse(readFileSync(f, "utf8")); break; } catch (e) { }
    }
    if (!raw && process.env.TABLEAU_COOKIES) raw = JSON.parse(process.env.TABLEAU_COOKIES);
    if (!raw) return null;
    const list = Array.isArray(raw) ? raw : (raw.cookies || []);
    return list.filter((c) => c && c.name && c.value !== undefined).map((c) => `${c.name}=${c.value}`).join("; ") || null;
  } catch (e) { return null; }
}
const PAR = Number(process.env.MASHAKO_PAR || 0); // 0 = auto : 6 en direct, 3 via Chrome

/* MASHAKO_PHASE=groupe|unitaire (02/09/2026). Mesuré côté ZS : un export
   unitaire coûte ~33 s de calcul serveur (session VizQL neuve par requête),
   ×519 zones ×7 feuilles ≈ 5 h 30 par période ; les 14 feuilles GROUPÉES
   (paquets de 100 zones) tiennent en ~10-15 min. On archive donc d'abord le
   groupé pour toutes les périodes, puis on complète l'unitaire période par
   période (fusion dans le meta.json publié). MASHAKO_MERGE=0 : ne pas fusionner
   avec l'archive existante (repartir d'un meta neuf — cas des archives fausses). */
const PHASE = (process.env.MASHAKO_PHASE || "").toLowerCase();
if (PHASE && !["groupe", "unitaire"].includes(PHASE)) { console.error(`MASHAKO_PHASE=${PHASE} inconnu (groupe|unitaire)`); process.exit(2); }
const partial = () => ONLY.length > 0 || !!PHASE;
const MERGE = process.env.MASHAKO_MERGE !== "0";

/* Feuilles hors périmètre données (pages fixes / jamais exportables en CSV) —
   générique ANT+ZS ; côté ZS on ignore aussi OVM/surveillance/annexe (Felly). */
const SKIP_DATA = IS_ZS
  ? /^(Cover ?Page|Contacts ?Page|FILTER|Configuration|Ranking|KPI )/i
  : /^(Cover ?Page|Contacts ?Page|FILTER|Configuration|Ranking|KPI Manuel_ANT_P\d)/i;
const IMG_ALSO = /^KPI /i; // pas de CSV mais l'image mensuelle a de la valeur

function log(msg) {
  const line = `[${new Date().toISOString()}]${IS_ZS ? " [ZS]" : ""} ${msg}`;
  console.log(line);
  try { appendFileSync(LOG, line + "\n"); } catch (e) { }
}
function gh(args, inputFile) {
  const a = ["api", ...args];
  if (inputFile) a.push("--input", inputFile);
  return execFileSync("gh", a, { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
}
function slug(label) {
  return label.normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 60) || "feuille";
}
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
/* Un CSV réduit à la seule colonne « _SELECTED_location_level_LABEL » = la
   feuille n'a AUCUNE donnée pour la période (Tableau n'exporte que le libellé
   du filtre) — cas de Vaccine_expiration_ANT_P2 sur plusieurs mois. */
const hasData = (rows) => rows.length > 1 && !(rows[0].length === 1 && /_LABEL$/i.test(String(rows[0][0] || "")));
const vizFrame = (page) => page.frames().find((f) => f.url().includes(`/t/${SITE}/views/`)) || null;
async function runBatch(items, size, fn) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(...await Promise.all(items.slice(i, i + size).map(fn)));
  return out;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  mkdirSync(OUT, { recursive: true });
  mkdirSync(path.dirname(LOCK), { recursive: true });
  /* verrou partagé : ne pas démarrer si une synchro tourne (MASHAKO_DRY=1 : lecture seule, verrou ignoré) */
  const DRY = process.env.MASHAKO_DRY === "1";
  /* Tests du moteur VizQL (02/09/2026) : MASHAKO_NOLOCK=1 (verrou ignoré),
     MASHAKO_ZS_LIMIT=n (n zones seulement), MASHAKO_NO_PUBLISH=1 (rien poussé). */
  const NOLOCK = process.env.MASHAKO_NOLOCK === "1";
  if (!DRY && !NOLOCK) {
    try {
      const st = statSync(LOCK);
      if (Date.now() - st.mtimeMs < 2 * 3600 * 1000) { log("⏭ Verrou récent (synchro en cours ?) — abandon, relancer plus tard."); return 3; }
    } catch (e) { }
    writeFileSync(LOCK, new Date().toISOString() + " backfill pid=" + process.pid);
  }

  /* ZS : le mois COURANT d'abord. Avec 519 zones de santé, une passe complète
     prend plusieurs nuits (registre zs_ledger.json, couverture cumulative) ; on
     ne lance pas le backfill des mois passés tant que le mois courant n'est pas
     couvert, sinon les deux jobs se disputent la même session Tableau et les
     archives seraient très partielles. Lever avec MASHAKO_FORCE_BACKFILL=1. */
  if (IS_ZS && process.env.MASHAKO_FORCE_BACKFILL !== "1") {
    /* Source de vérité = l'archive PUBLIÉE du mois courant (zs/meta.json) ; le
       registre local zs_ledger.json ne compte que ce que CETTE machine a
       synchronisé (20 zones sur le PC depuis que le cloud est titulaire). */
    let n = 0;
    try { n = (((await (await fetch(`${RAW}/zs/meta.json?_r=${Date.now()}`)).json()).antennes || {}).values || []).length; } catch (e) { }
    if (n < 500) {
      let led = {};
      try { led = JSON.parse(readFileSync(path.join(HERE, "zs_ledger.json"), "utf8")); } catch (e) { }
      n = Math.max(n, Object.keys(led).length);
    }
    if (n < 500) {
      log(`⏭ Mois courant pas encore couvert (${n}/519 zones de santé synchronisées) — backfill des mois passés en attente.`);
      rmSync(LOCK, { force: true });
      return 2;
    }
  }

  /* périodes déjà archivées */
  let have = [];
  try { have = (await (await fetch(`${RAW}/${PFX}periods/index.json?_r=${Date.now()}`)).json()).periods || []; } catch (e) { }
  let todo = [...REDO, ...TARGETS.filter((t) => !have.includes(t.key) && !REDO.some((r) => r.key === t.key))];

  let urlCache;
  try { urlCache = JSON.parse(readFileSync(URLCACHE_FILE, "utf8")); }
  catch (e) {
    log(`⏭ ${URLCACHE_FILE} absent — lancer d'abord une synchro du mois courant (elle construit le cache des noms d'URL).`);
    rmSync(LOCK, { force: true });
    return 4;
  }
  const labels = Object.keys(urlCache);

  /* ── AUTO-COMPLÉMENT (02/09/2026) : toutes les périodes archivées ? On cherche
     alors les archives INCOMPLÈTES — feuilles de données attendues sans fichier
     et non marquées vides (typiquement les feuilles unitaires laissées de côté
     par la phase « groupe », ~33 s de calcul serveur par zone) — et on en
     complète UNE par run, la plus récente d'abord, par lots de
     MASHAKO_COMPLEMENT_MAX feuilles (défaut 3 ≈ 2 h 30 côté ZS, sous les 6 h
     d'un job GitHub). Le mois courant (réécrit par la synchro) est exclu.
     MASHAKO_COMPLEMENT=0 pour désactiver. Le complément est FUSIONNÉ. */
  if (!todo.length && !ONLY.length && !PHASE && process.env.MASHAKO_COMPLEMENT !== "0") {
    const attendues = labels.filter((l) => !SKIP_DATA.test(l));
    let cur = null;
    try { cur = (await (await fetch(`${RAW}/${PFX}periods/index.json?_r=${Date.now()}`)).json()).current || null; } catch (e) { }
    for (const key of [...have].sort().reverse()) {
      if (key === cur) continue;
      let m = null;
      try { m = await (await fetch(`${RAW}/${PFX}periods/${key}/meta.json?_r=${Date.now()}`)).json(); } catch (e) { }
      if (!m || !Array.isArray(m.views)) continue;
      const manquantes = attendues.filter((l) => {
        const v = m.views.find((x) => x.name === l || x.urlName === slug(l));
        return !(v && (v.file || (v.empty && v.mode === "unitaire")));
      });
      if (!manquantes.length) continue;
      const [y, mo] = key.split("-").map(Number);
      const lot = manquantes.slice(0, Number(process.env.MASHAKO_COMPLEMENT_MAX || 3));
      todo = [{ key, month: MOIS[mo - 1], year: String(y) }];
      ONLY.push(...lot);
      log(`— Auto-complément : ${PFX}${key} incomplète (${manquantes.length} feuille(s) sans fichier : ${manquantes.join(", ")}) → ce run : ${lot.join(", ")} —`);
      break;
    }
  }
  if (!todo.length) { log("✓ Toutes les périodes 2025-07 → 2026-05 sont archivées et complètes — rien à faire."); if (!DRY) rmSync(LOCK, { force: true }); return 2; }
  if (DRY) { log(`[dry] périodes : ${todo.map((t) => t.key).join(", ")}${ONLY.length ? " — feuilles : " + ONLY.join(", ") : ""}${PHASE ? " — phase " + PHASE : ""} — rien lancé.`); return 0; }
  /* UNE période par exécution (défaut) : le run de 20h archive un mois par
     soir (~20 min avec le groupé multi-valeurs), verrou libéré avant 23h30 —
     fini les verrous de 27 h et les morts silencieuses à mi-parcours
     (constaté 25-26/07). MASHAKO_BACKFILL_ALL=1 pour tout faire d'un coup. */
  if (process.env.MASHAKO_BACKFILL_ALL !== "1") todo = todo.slice(0, 1);
  log(`— Backfill périodes : ${todo.length} ce run (${TARGETS.filter((t) => !have.includes(t.key)).length} restantes : ${TARGETS.filter((t) => !have.includes(t.key)).map((t) => t.key).join(", ")}) —`);

  /* Le profil Chrome peut être occupé par une synchro en cours (sync.mjs
     tourne parfois ~10 h ; son verrou est désormais rafraîchi par heartbeat,
     mais la fenêtre de 2 h laissait passer les runs longs → crash au
     lancement les 27-29/07). 3 essais espacés de 30 s (profil en cours de
     fermeture), puis abandon PROPRE — le verrou est libéré et le backfill
     retentera au prochain créneau — au lieu du crash non intercepté. */
  const exportUrl = (urlName, ext, params) =>
    `${SERVER}/t/${SITE}/views/${WORKBOOK}/${encodeURIComponent(urlName)}.${ext}` + (params ? "?" + params : "");
  let ctx = null, page = null, frame = null, mode = "chrome";
  let fetchBin = null;
  if (DIRECT) {
    let ck = cookieHeader();
    if (ck) {
      const direct = async (url, timeout) => {
        for (let essai = 0; essai < 2; essai++) {
          const ctrl = new AbortController();
          const to = setTimeout(() => ctrl.abort(), timeout);
          try {
            const r = await fetch(url, { headers: { Cookie: ck, Accept: "*/*" }, redirect: "manual", signal: ctrl.signal });
            clearTimeout(to);
            const ct = (r.headers.get("content-type") || "").toLowerCase();
            if (r.ok && !ct.includes("html")) return { b64: Buffer.from(await r.arrayBuffer()).toString("base64"), ct };
            if (r.status === 429 || r.status >= 500) return null; // bridage / panne serveur
            /* HTML ou redirection SSO = session refusée ; reconnecter.mjs a
               peut-être renouvelé les cookies entre-temps -> on relit UNE fois. */
            const ck2 = cookieHeader();
            if (ck2 && ck2 !== ck) { ck = ck2; continue; }
            return null;
          } catch (e) { clearTimeout(to); return null; }
        }
        return null;
      };
      const test = await direct(exportUrl(IS_ZS ? "FILTER_VALUES" : "FILTER_VALUES_ANT", "csv", ":refresh=yes"), 60000);
      if (test && test.ct.includes("csv")) { fetchBin = direct; mode = "direct"; log("✓ Session valide (mode direct, sans navigateur)."); }
      else log("⚠ Session directe refusée (cookies périmés ?) — repli sur Chrome.");
    } else log("⚠ Aucun cookie de session (cookies-tableau.json / TABLEAU_COOKIES) — repli sur Chrome.");
  }
  if (mode === "chrome") {
  let launchErr = null;
  for (let essai = 1; essai <= 3 && !ctx; essai++) {
    try {
      ctx = await chromium.launchPersistentContext(PROFILE, {
        channel: "chrome", headless: true, ignoreDefaultArgs: ["--enable-automation"],
        viewport: { width: 1600, height: 950 },
        args: ["--no-first-run", "--no-default-browser-check"],
      });
    } catch (e) {
      launchErr = e;
      log(`  ⟳ Lancement de Chrome impossible (essai ${essai}/3 : ${String(e.message || e).slice(0, 80)})${essai < 3 ? " — nouvel essai dans 30 s…" : ""}`);
      if (essai < 3) await sleep(30000);
    }
  }
  if (!ctx) {
    log(`⏭ Profil Chrome indisponible (synchro en cours ?) — abandon propre, retente au prochain créneau.`);
    rmSync(LOCK, { force: true });
    return 3;
  }
  page = ctx.pages()[0] || await ctx.newPage();
  }
  const PARALLEL = PAR || (mode === "direct" ? 6 : 3);
  log(`  mode ${mode}, parallélisme ${PARALLEL}${ONLY.length ? `, feuilles ciblées : ${ONLY.join(", ")}` : ""}${PHASE ? `, phase ${PHASE}` : ""}${partial() && !MERGE ? ", sans fusion" : ""}.`);
  let exitCode = 0;
  try {
    if (mode === "chrome") {
    await page.goto(UI_URL, { waitUntil: "domcontentloaded", timeout: 120000 }).catch(() => { });
    const dl = Date.now() + 90000;
    while (Date.now() < dl) { await sleep(3000); frame = vizFrame(page); if (frame) break; }
    if (!frame) throw new Error("Vue Tableau introuvable (session ?).");
    log("✓ Session valide.");

    fetchBin = async function (url, timeout) {
      frame = vizFrame(page) || frame;
      const evalP = frame.evaluate(async (args) => {
        const ctrl = new AbortController();
        const to = setTimeout(() => ctrl.abort(), args.timeout);
        try {
          const r = await fetch(args.url, { credentials: "include", signal: ctrl.signal });
          clearTimeout(to);
          const ct = (r.headers.get("content-type") || "").toLowerCase();
          if (!r.ok || ct.includes("html")) return null;
          const buf = new Uint8Array(await r.arrayBuffer());
          let bin = ""; const CH = 0x8000;
          for (let i = 0; i < buf.length; i += CH) bin += String.fromCharCode.apply(null, buf.subarray(i, i + CH));
          return { b64: btoa(bin), ct };
        } catch (e) { clearTimeout(to); return null; }
      }, { url, timeout }).catch(() => null);
      const guard = new Promise((res) => setTimeout(() => res(null), timeout + 20000));
      return await Promise.race([evalP, guard]);
    };
    }
    const perParams = (t, extra) =>
      `_PARAM_month=${encodeURIComponent(t.month)}&_PARAM_year=${encodeURIComponent(t.year)}` + (extra ? "&" + extra : "") + "&:refresh=yes";

    const shortLoc = (v) => {
      let s = String(v || "").trim().replace(/\s*zones?\s+de\s+sant[eé]\s*$/i, "").trim();
      return s.replace(/^[a-z]{2,3}\s+/, "").trim() || String(v || "").trim();
    };
    /* Localisations : ANT = FILTER_VALUES_ANT (51 antennes, libellé « Antenne En »).
       ZS = les TROIS feuilles FILTER_VALUES, _2, _3 se partagent les 519 zones
       (172-175 chacune) -> cumul obligatoire, puis complément par le référentiel
       zs_filter_values.json (topojson). ⚠ Côté ZS la colonne « Antenne En » est
       l'antenne de RATTACHEMENT : l'archive ZS de juin 2026 avait 172 zones
       toutes libellées « Buta »… Le libellé ZS est le nom court de la zone. */
    const fvLabels = labels.filter((l) => /^FILTER/i.test(l)).sort();
    if (!fvLabels.length) fvLabels.push(IS_ZS ? "FILTER_VALUES" : "FILTER_VALUES_ANT");
    let antField = null;
    const antLabel = {};
    const antennes = [];
    for (const fvLabel of fvLabels) {
      const fvName = urlCache[fvLabel] || fvLabel;
      const fvR = await fetchBin(exportUrl(fvName, "csv", ":refresh=yes"), 150000);
      if (!fvR) { log(`  ⚠ ${fvLabel} : CSV indisponible.`); continue; }
      const fvRows = parseCsv(Buffer.from(fvR.b64, "base64").toString("utf8"));
      if (fvRows.length < 2) continue;
      const fvCols = fvRows[0];
      const si = fvCols.findIndex((c) => /SELECTED_location_level/i.test(c));
      if (si < 0) { log(`  ⚠ ${fvLabel} : colonne _SELECTED_location_level absente.`); continue; }
      const ci = IS_ZS ? -1 : fvCols.findIndex((c) => /antenne/i.test(c));
      antField = antField || fvCols[si];
      let n = 0;
      for (const rr of fvRows.slice(1)) {
        const comp = (rr[si] || "").trim();
        if (!comp || antennes.includes(comp)) continue;
        const court = ci >= 0 ? (rr[ci] || "").trim() : "";
        antennes.push(comp); antLabel[comp] = court || shortLoc(comp); n++;
      }
      log(`  ✓ ${fvLabel} : ${fvRows.length - 1} lignes (+${n}, cumul ${antennes.length}).`);
    }
    if (IS_ZS) {
      try {
        const ref = JSON.parse(readFileSync(path.join(HERE, "zs_filter_values.json"), "utf8"));
        let added = 0;
        for (const [court, comp] of Object.entries(ref)) {
          if (comp && !antennes.includes(comp)) { antennes.push(comp); antLabel[comp] = court; added++; }
        }
        if (added) log(`  + ${added} zones de santé ajoutées depuis zs_filter_values.json (référentiel topojson).`);
      } catch (e) { }
    }
    if (!antField) throw new Error("FILTER_VALUES illisible (throttle ?).");
    if (!antennes.length) throw new Error("Aucune localisation lue dans FILTER_VALUES.");
    log(`✓ ${antennes.length} ${IS_ZS ? "zones de santé" : "antennes"}.`);

    let dataLabels = labels.filter((l) => !SKIP_DATA.test(l) && onlyMatch(l));
    let imgLabels = labels.filter((l) => (!SKIP_DATA.test(l) || IMG_ALSO.test(l)) && onlyMatch(l));
    if (ONLY.length && !dataLabels.length && !imgLabels.length) throw new Error(`MASHAKO_ONLY : aucune feuille ne correspond à « ${ONLY.join(", ")} ».`);
    /* ── LISTE BLANCHE multi-valeurs (même règle que sync.mjs, validée 27/07) :
       ces feuilles gardent toutes leurs lignes en export groupé ; les autres
       (pivots « Noms de mesures », classements, cartes) collapsent → unitaire. */
    const BATCH_OK = IS_ZS
      ? /^(Supervision_HZ_P\d|CDF_HZ_P\d|CDF_HZ_NF|S.+ances_HZ_P\d|Taux d.abandon_HZ_P\d|Infirmier_HZ_P\d|Vaccine_expiration_HZ_P\d)$/i
      : /^(HZ Scores_ANT|Supervision_Quality_ANT|Supervision_ANT_P1|R.+union_ANT|CDF_ANT|S.+ances_ANT|Taux d.abandon_ANT|Infirmier_ANT|Livraison_ANT_P2)$/i;
    const ZS_PACK = Number(process.env.MASHAKO_ZS_PACK || 100);
    if (PHASE === "groupe") { dataLabels = dataLabels.filter((l) => BATCH_OK.test(l)); imgLabels = imgLabels.filter((l) => BATCH_OK.test(l)); }
    else if (PHASE === "unitaire") { dataLabels = dataLabels.filter((l) => !BATCH_OK.test(l)); imgLabels = imgLabels.filter((l) => !BATCH_OK.test(l)); }
    if (PHASE) log(`  phase « ${PHASE} » : ${dataLabels.length} feuilles de données, ${imgLabels.length} images.`);
    /* carte ZS (nom court) → antenne, pour l'attribution des exports groupés
       ANT (les lignes « Zone de sante » n'ont pas de colonne antenne). */
    let zsToAnt = {};
    if (!IS_ZS) {
      try { zsToAnt = JSON.parse(readFileSync(path.join(HERE, "zs_ant_map.json"), "utf8")).map || {}; } catch (e) { }
      log(`  carte ZS→antenne : ${Object.keys(zsToAnt).length} ZS (cache).`);
    }

    for (const t of todo) {
      writeFileSync(LOCK, new Date().toISOString() + " backfill " + t.key);
      log(`━━ Période ${t.month} ${t.year} (${t.key}) : ${dataLabels.length} feuilles × ${antennes.length} ${IS_ZS ? "zones de santé" : "antennes"} ━━`);
      /* pré-contrôle throttling */
      const probeSheet = (!IS_ZS && urlCache["HZ Scores_ANT"]) || urlCache[dataLabels[0]] || MAIN_VIEW;
      const probe = await fetchBin(exportUrl(probeSheet, "csv",
        perParams(t, `${encodeURIComponent(antField)}=${encodeURIComponent(antennes[0])}`)), 150000);
      if (!probe) {
        log("⛔ Pré-contrôle en échec — Tableau bride probablement. Pause 60 min puis nouvel essai…");
        await sleep(60 * 60 * 1000);
        const retry = await fetchBin(exportUrl(probeSheet, "csv",
          perParams(t, `${encodeURIComponent(antField)}=${encodeURIComponent(antennes[0])}`)), 150000);
        if (!retry) { log("⛔ Toujours bridé — arrêt du backfill (reprendre plus tard)."); exitCode = 1; break; }
      }
      rmSync(path.join(OUT, "views"), { recursive: true, force: true });
      mkdirSync(path.join(OUT, "views"), { recursive: true });
      const metaViews = [];
      /* complément / ciblé : le meta publié dit déjà quelles feuilles n'ont pas
         de colonne de localisation en groupé → unitaire d'emblée (on économise
         les 6 paquets + relances, ~1 min 30 par feuille). */
      let dejaDifferees = new Set();
      if (partial()) {
        try {
          const old = await (await fetch(`${RAW}/${PFX}periods/${t.key}/meta.json?_r=${Date.now()}`)).json();
          for (const v of old.views || []) if (v.deferred || (v.empty && v.mode !== "unitaire")) dejaDifferees.add(v.name);
        } catch (e) { }
      }
      let okSheets = 0;
      for (const label of dataLabels) {
        const urlName = urlCache[label], s = slug(label);
        let ok = [];
        let unitaire = !BATCH_OK.test(label) || dejaDifferees.has(label); // vrai si l'export a été (ou sera) fait zone par zone
        let differe = false;
        if (!unitaire) {
          /* groupé multi-valeurs : paquets de 51 antennes (ANT) ou 100 ZS (ZS) */
          const packs = [];
          for (let i = 0; i < antennes.length; i += IS_ZS ? ZS_PACK : 51) packs.push(antennes.slice(i, i + (IS_ZS ? ZS_PACK : 51)));
          const results = await runBatch(packs, PARALLEL, async (pack) => {
            /* Un paquet de 100 zones qui revient vide n'est pas « sans donnée »,
               c'est un serveur saturé (constaté 02/09 : 1 octet après 42 s) → on
               réessaie, sinon 100 zones disparaissent en silence de l'archive. */
            for (let essai = 1; essai <= 3; essai++) {
              const r = await fetchBin(exportUrl(urlName, "csv",
                perParams(t, `${encodeURIComponent(antField)}=${pack.map(encodeURIComponent).join(",")}`)), 150000);
              if (r) {
                const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
                if (hasData(rows)) return rows;
              }
              if (essai < 3) { log(`  ⟳ ${label} : paquet de ${pack.length} vide ou refusé (essai ${essai}/3) — pause 20 s…`); await sleep(20000); }
            }
            return null;
          });
          /* Même règle que sync.mjs : un export groupé SANS colonne de
             localisation (« Zone de santé » côté ZS, « Antenne » côté ANT) ne
             peut pas être attribué → ce n'est PAS « aucune donnée ». En phase
             « groupe » on diffère la feuille (le complément la reprendra en
             unitaire) ; en ciblé/complément on bascule tout de suite. */
          const avecDonnees = results.filter(Boolean);
          const attribuable = avecDonnees.some((rows) => rows[0].some((c) => IS_ZS ? /zone de sant/i.test(c) : (/^Antenne( En)?$/i.test(String(c).trim()) || /zone de sant/i.test(c))));
          if (avecDonnees.length && !attribuable) {
            if (ONLY.length) { log(`  ↪ ${label} : pas de colonne de localisation dans l'export groupé — repli unitaire (${antennes.length} requêtes).`); unitaire = true; }
            else { log(`  ⏳ ${label} : pas de colonne de localisation dans l'export groupé — différée (complément unitaire).`); differe = true; }
          }
          if (!differe && !unitaire) for (const rows of avecDonnees) {
            const hdr = rows[0];
            const zi = hdr.findIndex((c) => /zone de sant/i.test(c));
            const ai = hdr.findIndex((c) => /^Antenne( En)?$/i.test(String(c).trim()));
            for (const rr of rows.slice(1)) {
              let ant = "";
              if (IS_ZS) ant = zi >= 0 ? shortLoc(rr[zi]) : "";
              else if (ai >= 0) ant = String(rr[ai] || "").trim();
              else if (zi >= 0) ant = zsToAnt[String(rr[zi] || "").trim()] || "";
              if (!ant) continue;
              ok.push({ ant, rows: [hdr, rr] });
            }
          }
          /* groupé sans aucune donnée (paquets vides / libellé seul) : en
             phase groupe on diffère aussi — seul l'unitaire fait foi pour
             déclarer une feuille vide. */
          if (!differe && !unitaire && !ok.length && !avecDonnees.length && !ONLY.length) { log(`  ⏳ ${label} : export groupé vide — différée (complément unitaire).`); differe = true; }
          if (!differe && !unitaire && !ok.length && !avecDonnees.length && ONLY.length) unitaire = true;
        }
        if (differe) {
          metaViews.push({ name: label, urlName: s, rows: 0, file: null, image: null, antImages: null, deferred: true });
          continue;
        }
        let viaSession = false;
        if (unitaire && IS_ZS && process.env.MASHAKO_SESSION !== "0") {
          /* ── MOTEUR VizQL (02/09/2026) : une session par tranche de zones,
             categorical-filter (~10-15 s) + export résumé (~0,7 s) par zone, au
             lieu d'un export .csv par URL (~33 s, session neuve à chaque fois).
             Même CSV que l'URL (vérifié). Repli URL si le moteur échoue. ── */
          try {
            const { exportParSessions } = await import("./vizql-export.mjs");
            const r = await exportParSessions({
              wb: WORKBOOK, urlName, slug: s, label, month: t.month, year: t.year, zones: antennes, antLabel, log,
              limit: Number(process.env.MASHAKO_ZS_LIMIT || 0) || undefined,
            });
            const traitees = r.zonesOk + r.zonesVides;
            const attendu = Number(process.env.MASHAKO_ZS_LIMIT || 0) || antennes.length;
            if (traitees >= attendu * 0.9) {
              const cols = r.columns.filter((c) => c !== "Antenne");
              for (const rec of r.records) ok.push({ ant: rec.Antenne, rows: [cols, cols.map((c) => rec[c] ?? "")] });
              viaSession = true;
            } else log(`  ⚠ ${label} : moteur VizQL incomplet (${traitees}/${attendu} zones) — repli export par URL.`);
          } catch (e) { log(`  ⚠ ${label} : moteur VizQL indisponible (${String(e.message || e).slice(0, 120)}) — repli export par URL.`); }
        }
        if (unitaire && !viaSession) {
          const results = await runBatch(antennes, PARALLEL, async (ant) => {
            let r = await fetchBin(exportUrl(urlName, "csv",
              perParams(t, `${encodeURIComponent(antField)}=${encodeURIComponent(ant)}`)), 150000);
            if (!r) { await sleep(10000); r = await fetchBin(exportUrl(urlName, "csv",
              perParams(t, `${encodeURIComponent(antField)}=${encodeURIComponent(ant)}`)), 150000); }
            if (!r) return null;
            const rows = parseCsv(Buffer.from(r.b64, "base64").toString("utf8"));
            return hasData(rows) ? { ant, rows } : null;
          });
          ok = results.filter(Boolean);
        }
        if (!ok.length) {
          log(`  ✗ ${label} : aucune donnée (${unitaire ? "vérifié zone par zone" : "groupé"})`);
          metaViews.push({ name: label, urlName: s, rows: 0, file: null, image: null, antImages: null, empty: true, mode: unitaire ? "unitaire" : "groupe", checked_at: new Date().toISOString() });
          continue;
        }
        const colSet = [];
        for (const { rows } of ok) for (const c of rows[0]) if (colSet.indexOf(c) < 0) colSet.push(c);
        const columns = ["Antenne", ...colSet];
        const records = [];
        for (const { ant, rows } of ok) {
          const hdr = rows[0];
          for (const rr of rows.slice(1)) {
            const o = { Antenne: antLabel[ant] || ant };
            hdr.forEach((c, i) => { o[c] = rr[i] ?? ""; });
            records.push(o);
          }
        }
        const rel = `views/${s}.json`;
        writeFileSync(path.join(OUT, rel), JSON.stringify({ name: label, urlName: s, columns, rows: records }));
        metaViews.push({ name: label, urlName: s, rows: records.length, file: rel, image: null, antImages: null, ...(viaSession ? { engine: "vizql-session" } : {}) });
        okSheets++;
        log(`  ✓ ${label} : ${records.length} lignes (${viaSession ? "sessions VizQL" : !unitaire ? "groupé" : `${ok.length}/${antennes.length} ${IS_ZS ? "ZS" : "antennes"}`})`);
      }
      if (partial() && !okSheets && !imgLabels.length) { log(`⚠ ${t.key} : aucune donnée obtenue pour ${ONLY.join(", ") || PHASE} — archive inchangée.`); continue; }
      if (!partial() && okSheets < Math.ceil(dataLabels.length * 0.6)) {
        log(`⛔ Période ${t.key} trop incomplète (${okSheets}/${dataLabels.length} feuilles) — non publiée, arrêt (throttle probable).`);
        exitCode = 1; break;
      }
      /* images par défaut (période appliquée) — best effort */
      for (const label of imgLabels) {
        const urlName = urlCache[label], s = slug(label);
        const r = await fetchBin(exportUrl(urlName, "png", perParams(t)), 150000);
        if (r && r.ct.includes("image")) {
          const rel = `views/${s}.png`;
          writeFileSync(path.join(OUT, rel), Buffer.from(r.b64, "base64"));
          const mv = metaViews.find((v) => v.name === label);
          if (mv) mv.image = rel;
          else metaViews.push({ name: label, urlName: s, rows: 0, file: null, image: rel, antImages: null });
        }
      }
      let meta = {
        generated_at: new Date().toISOString(),
        server: SERVER.replace("https://", ""), site: SITE,
        workbook: { name: IS_ZS ? "Mashako 3.0 — Rapport de Zone de Santé" : "Mashako 3.0 — Rapport de l'Antenne", contentUrl: WORKBOOK },
        main_view: MAIN_VIEW, original_url: UI_URL, sync_mode: "backfill-period",
        period: { key: t.key, month: t.month, year: t.year },
        antennes: { field: antField, values: antennes.map((a) => antLabel[a] || a) },
        views: metaViews,
      };
      if (partial() && MERGE) {
        /* réparation / complément : on repart du meta.json PUBLIÉ et on ne remplace que les vues ré-exportées */
        let old = null;
        try { old = await (await fetch(`${RAW}/${PFX}periods/${t.key}/meta.json?_r=${Date.now()}`)).json(); } catch (e) { }
        if (old && Array.isArray(old.views)) {
          const views = old.views.slice();
          for (const v of metaViews) {
            const k = views.findIndex((x) => x.name === v.name || x.urlName === v.urlName);
            if (k >= 0) { if (!(v.deferred && views[k].file)) views[k] = { ...views[k], ...v }; } else views.push(v);
          }
          meta = { ...old, antennes: meta.antennes, views, repaired_at: new Date().toISOString(), repaired_views: [...(old.repaired_views || []), ...metaViews.map((v) => v.name)] };
          log(`  ↻ meta.json fusionné (${metaViews.length} vue(s) remplacée(s) sur ${views.length}).`);
        }
      }
      writeFileSync(path.join(OUT, "meta.json"), JSON.stringify(meta, null, 2));
      /* ── publication : base_tree (ne touche qu'à periods/<key>/ et l'index) ── */
      if (process.env.MASHAKO_NO_PUBLISH === "1") { log(`[test] ${t.key} : ${metaViews.length} vue(s) prêtes dans ${OUT} — publication SAUTÉE (MASHAKO_NO_PUBLISH).`); continue; }
      log(`→ Publication de l'archive ${t.key}…`);
      const oldRef = JSON.parse(gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`]));
      const oldTree = JSON.parse(gh([`${REPO}/git/commits/${oldRef.object.sha}`])).tree.sha;
      const blob = (buf) => {
        const p = path.join(OUT, "_payload.json");
        writeFileSync(p, JSON.stringify({ encoding: "base64", content: buf.toString("base64") }));
        return JSON.parse(gh([`${REPO}/git/blobs`, "-X", "POST"], p)).sha;
      };
      const entries = [{ path: `${PFX}periods/${t.key}/meta.json`, mode: "100644", type: "blob", sha: blob(Buffer.from(JSON.stringify(meta, null, 2))) }];
      for (const v of metaViews) {
        if (v.file) entries.push({ path: `${PFX}periods/${t.key}/${v.file}`, mode: "100644", type: "blob", sha: blob(readFileSync(path.join(OUT, v.file))) });
        if (v.image) entries.push({ path: `${PFX}periods/${t.key}/${v.image}`, mode: "100644", type: "blob", sha: blob(readFileSync(path.join(OUT, v.image))) });
      }
      let curIdx = { periods: [] };
      try { curIdx = await (await fetch(`${RAW}/${PFX}periods/index.json?_r=${Date.now()}`)).json(); } catch (e) { }
      const keys = [...new Set([...(curIdx.periods || []), t.key])].sort();
      entries.push({ path: `${PFX}periods/index.json`, mode: "100644", type: "blob", sha: blob(Buffer.from(JSON.stringify({ periods: keys, current: curIdx.current || null, updated_at: new Date().toISOString() }, null, 2))) });
      const tp = path.join(OUT, "_tree.json");
      writeFileSync(tp, JSON.stringify({ base_tree: oldTree, tree: entries }));
      const newTree = JSON.parse(gh([`${REPO}/git/trees`, "-X", "POST"], tp)).sha;
      const cp = path.join(OUT, "_commit.json");
      writeFileSync(cp, JSON.stringify({ message: partial() ? `auto: archive Mashako ${PFX}${t.key} — ${PHASE || "réparation"} (${okSheets} feuilles${ONLY.length ? " : " + metaViews.map((v) => v.name).join(", ") : ""})` : `auto: archive Mashako ${PFX}${t.key} (${okSheets} feuilles, ${antennes.length} ${IS_ZS ? "zones de santé" : "antennes"})`, tree: newTree, parents: [] }));
      const commit = JSON.parse(gh([`${REPO}/git/commits`, "-X", "POST"], cp)).sha;
      gh([`${REPO}/git/refs/heads/${DATA_BRANCH}`, "-X", "PATCH", "-f", `sha=${commit}`, "-F", "force=true"]);
      log(`✓ Archive ${t.key} publiée (${commit.slice(0, 9)}) — ${okSheets} feuilles de données.`);

      /* ── Détail par aire de santé de la période archivée ───────────────────
         Hors flux par défaut, et c'est délibéré : le tableau croisé du détail
         AS est paginé à 20 lignes sans colonne « zone de santé », donc une
         session Tableau PAR ZONE — environ 16 h pour les 517 zones d'UN mois.
         Rejouer les 12 mois d'archives coûterait des jours d'export continu.
         Le détail est donc tenu à jour pour le mois courant (tâche « Mashako
         3.0 ZS Detail aire de sante », 01h00) ; pour reconstituer un mois
         passé, lancer ce backfill avec MASHAKO_BACKFILL_AS=1 — le journal de
         reprise fait qu'on peut l'étaler sur plusieurs nuits. */
      if (IS_ZS && process.env.MASHAKO_BACKFILL_AS === "1") {
        log(`→ Détail par aire de santé de ${t.key} (reprenable, budget ${process.env.MASHAKO_AS_MINUTES || 240} min)…`);
        try {
          execFileSync(process.execPath, [path.join(HERE, "export-zs-as.mjs"), t.month, t.year], {
            stdio: "inherit",
            env: { ...process.env, MASHAKO_MINUTES: process.env.MASHAKO_AS_MINUTES || "240" },
          });
          execFileSync(process.execPath, [path.join(HERE, "publish-zs-as.mjs"), "--fusion"], { stdio: "inherit" });
          log(`✓ Détail par aire de santé de ${t.key} publié.`);
        } catch (e) {
          /* Un détail AS incomplet ne doit jamais compromettre l'archive du
             mois, qui vient d'être publiée avec succès. */
          log(`⚠ Détail par aire de santé de ${t.key} interrompu (${String(e.message).slice(0, 90)}) — repris au prochain run.`);
        }
      }
      if (t !== todo[todo.length - 1]) await sleep(3 * 60 * 1000); // pause entre périodes (ménage le serveur)
    }
    if (exitCode === 0) log("— Backfill terminé : toutes les périodes demandées sont archivées —");
  } catch (e) {
    log(`✖ ÉCHEC backfill : ${e.message}`);
    exitCode = 1;
  } finally {
    if (ctx) await ctx.close().catch(() => { });
    if (!NOLOCK) rmSync(LOCK, { force: true });
  }
  return exitCode;
}

process.exitCode = await main();
